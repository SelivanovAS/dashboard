"""Crash and conflict proofs for the durable coordinator, without host mutations."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

SPEC = importlib.util.spec_from_file_location("program_maintenance", Path(__file__).parents[1] / "program_maintenance.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
SHA = "a" * 40
TARGET = "b" * 40
SOURCE = "c" * 40
RELEASE = "d" * 64
NONCE = "e" * 32


class Hooks:
    def __init__(self):
        self.calls = []
        self.held = None
        self.marker = None
        self.fail = None
        self.snapshot_value = {r: {"head": SHA, "release_id": "f" * 64, "managed_sha256": "1" * 64} for r in m.REGIONS}
        self.safe_journals = []

    def call(self, name):
        self.calls.append(name)
        if name == self.fail:
            raise m.MaintenanceError("injected " + name)

    def check_guard(self, marker):
        self.call("check_guard")
        assert marker.is_file()
        self.marker = marker

    def drain(self):
        self.call("drain")

    def acquire_run_locks(self, nonce):
        self.call("acquire")
        assert self.held is None
        self.held = nonce

    def assert_run_locks(self, nonce):
        self.call("assert_locks")
        assert self.held == nonce

    def release_run_locks(self, nonce):
        self.call("release")
        assert self.held == nonce
        assert self.marker.exists(), "run locks must be released BEFORE marker"
        self.held = None

    def snapshot(self):
        self.call("snapshot")
        return copy.deepcopy(self.snapshot_value)

    def validate_snapshot(self, snapshot):
        self.call("validate_snapshot")
        if snapshot != self.snapshot_value:
            raise m.MaintenanceError("changed baseline")

    def verify_installed(self, journal):
        self.call("verify_installed")
        assert self.held == journal["nonce"]
        return {"installed_sha": journal["attempts"][-1]["target_sha"], "release_id": journal["release_id"], "evidence_sha256": "2" * 64}

    def verify_safe(self, journal):
        self.call("verify_safe")
        self.safe_journals.append(copy.deepcopy(journal))
        return {"covered_attempt_ids": [a["attempt_id"] for a in journal["attempts"]], "evidence_sha256": "3" * 64}


@pytest.fixture
def state_dir(tmp_path):
    return tmp_path / "maintenance"


def reserve(lease, nonce=NONCE):
    return lease.reserve(region="hmao", release_id=RELEASE, source_commit=SOURCE, nonce=nonce)


def advance(lease, phase):
    reserve(lease)
    if phase == "reserved": return
    lease.prepare(NONCE)
    if phase == "ready": return
    lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
    if phase == "publish_intent": return
    lease.begin_apply(NONCE, 1)
    if phase == "applying": return
    lease.mark_verified(NONCE)
    if phase == "verified": return
    lease.finish(NONCE)


def test_normal_flow_durable_order_and_idempotent_finish(state_dir, monkeypatch):
    hooks = Hooks()
    writes = []
    original = m.MaintenanceLease._write
    def tracked(self, name, document):
        writes.append((name, document.get("phase")))
        if name == "journal.json":
            assert (state_dir / "blocked").exists()
        return original(self, name, document)
    monkeypatch.setattr(m.MaintenanceLease, "_write", tracked)
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "complete")
        assert lease.inspect()["blocked"] is False
        assert lease.finish(NONCE)["phase"] == "complete"
    assert writes.index(("blocked", None)) < writes.index(("journal.json", "ready"))
    assert [p for n, p in writes if n == "journal.json"] == ["reserved", "ready", "publish_intent", "applying", "verified", "safe_to_resume", "complete"]
    assert hooks.calls.count("acquire") == hooks.calls.count("release") == 1
    assert len(hooks.safe_journals) == 2
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in state_dir.iterdir())


@pytest.mark.parametrize("phase", ["reserved", "ready", "publish_intent", "applying", "verified"])
def test_eof_at_each_phase_keeps_gate_closed_and_recovery_does_not_unblock(state_dir, phase):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, phase)
    assert (state_dir / "blocked").exists()
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks, boot_id="new-boot") as lease:
        assert lease.recover(NONCE)["phase"] == phase
        assert lease.inspect()["blocked"]
        if phase not in ("reserved", "ready"):
            with pytest.raises(m.MaintenanceError, match="недопустима"):
                lease.cancel_before_publish(NONCE)
    assert (state_dir / "blocked").exists()


def test_global_flock_excludes_other_coordinator_and_inode_persists(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as first:
        original_inode = (state_dir / "coordinator.lock").stat().st_ino
        with pytest.raises(m.MaintenanceError, match="глобальный замок"):
            with m.MaintenanceLease(state_dir, Hooks()): pass
        reserve(first)
    with m.MaintenanceLease(state_dir, Hooks()):
        assert (state_dir / "coordinator.lock").stat().st_ino == original_inode


def test_replaced_global_inode_fails_before_permission(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        reserve(lease)
        lease.prepare(NONCE)
        (state_dir / "coordinator.lock").unlink()
        (state_dir / "coordinator.lock").touch(mode=0o600)
        with pytest.raises(m.MaintenanceError, match="Inode|Небезопасный файл"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
    assert json.loads((state_dir / "journal.json").read_text())["attempts"] == []


def test_second_reservation_refuses_unknown_existing_window(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        reserve(lease)
        with pytest.raises(m.MaintenanceError, match="восстановить"):
            reserve(lease, "1" * 32)


def test_reused_nonce_refused_even_after_intervening_release(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        reserve(lease)
        lease.cancel_before_publish(NONCE)
        reserve(lease, "1" * 32)
        lease.cancel_before_publish("1" * 32)
        with pytest.raises(m.MaintenanceError, match="уже использован"):
            reserve(lease)


def test_all_intents_preserved_and_stale_attempt_cannot_apply(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "publish_intent")
        permit = lease.publish_intent(NONCE, base_sha="4" * 40, target_sha="5" * 40)
        assert permit["attempt_id"] == 2
        assert len(lease.inspect()["journal"]["attempts"]) == 2
        with pytest.raises(m.MaintenanceError, match="Устаревшее"):
            lease.begin_apply(NONCE, 1)
        lease.begin_apply(NONCE, 2)
        lease.mark_verified(NONCE)
        lease.finish(NONCE)


def test_verify_safe_must_cover_every_intent_not_only_latest(state_dir):
    hooks = Hooks()
    hooks.verify_safe = lambda doc: {"covered_attempt_ids": [2], "evidence_sha256": "3" * 64}
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "publish_intent")
        lease.publish_intent(NONCE, base_sha="4" * 40, target_sha="5" * 40)
        lease.begin_apply(NONCE, 2)
        lease.mark_verified(NONCE)
        with pytest.raises(m.MaintenanceError, match="все разрешённые"):
            lease.finish(NONCE)
    assert (state_dir / "blocked").exists()


def test_ambiguous_remote_ancestor_can_never_be_cancelled(state_dir):
    hooks = Hooks()
    hooks.fail = "verify_safe"
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "verified")
        with pytest.raises(m.MaintenanceError, match="verify_safe"):
            lease.finish(NONCE)
        with pytest.raises(m.MaintenanceError):
            lease.cancel_before_publish(NONCE)
    assert (state_dir / "blocked").exists()


@pytest.mark.parametrize("method", ["prepare", "recover", "mark_verified", "finish", "cancel_before_publish"])
def test_wrong_nonce_never_advances_or_clears(state_dir, method):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        reserve(lease)
        with pytest.raises(m.MaintenanceError, match="nonce"):
            getattr(lease, method)("f" * 32)
        assert lease.inspect()["journal"]["phase"] == "reserved"


def test_lost_live_run_locks_refuses_publish(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "ready")
        hooks.fail = "assert_locks"
        with pytest.raises(m.MaintenanceError, match="assert_locks"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
    assert json.loads((state_dir / "journal.json").read_text())["attempts"] == []


def test_recovered_session_must_explicitly_reacquire_before_new_permission(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "ready")
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        with pytest.raises(m.MaintenanceError, match="не удерживаются"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
        lease.recover(NONCE)
        assert lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)["attempt_id"] == 1


def test_changed_baseline_refuses_publish_and_cancel(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "ready")
        hooks.snapshot_value["tyumen"]["head"] = "4" * 40
        with pytest.raises(m.MaintenanceError, match="baseline"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
        with pytest.raises(m.MaintenanceError, match="baseline"):
            lease.cancel_before_publish(NONCE)
    assert (state_dir / "blocked").exists()


def test_failed_atomic_acquire_is_not_blindly_released(state_dir):
    hooks = Hooks()
    hooks.fail = "acquire"
    with m.MaintenanceLease(state_dir, hooks) as lease:
        reserve(lease)
        with pytest.raises(m.MaintenanceError, match="acquire"):
            lease.prepare(NONCE)
    assert "release" not in hooks.calls
    assert (state_dir / "blocked").exists()


def test_failed_release_is_not_retried_by_context_exit(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "verified")
        hooks.fail = "release"
        with pytest.raises(m.MaintenanceError, match="release"):
            lease.finish(NONCE)
    assert hooks.calls.count("release") == 1
    assert (state_dir / "blocked").exists()
    assert json.loads((state_dir / "journal.json").read_text())["phase"] == "safe_to_resume"


def test_failure_after_run_lock_release_still_blocks_and_rechecks_on_recovery(state_dir):
    hooks = Hooks()
    original = hooks.verify_safe
    def checking(doc):
        if hooks.held is None:
            raise m.MaintenanceError("late pending job")
        return original(doc)
    hooks.verify_safe = checking
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "verified")
        with pytest.raises(m.MaintenanceError, match="pending job"):
            lease.finish(NONCE)
    assert json.loads((state_dir / "journal.json").read_text())["phase"] == "safe_to_resume"
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        lease.recover(NONCE)
        lease.finish(NONCE)
        assert not lease.inspect()["blocked"]


@pytest.mark.parametrize("point", ["before_reserved_journal", "before_intent_journal", "after_intent_rename", "before_marker_unlink"])
def test_crash_write_boundaries_never_open_ambiguous_gate(state_dir, monkeypatch, point):
    original_write = m.MaintenanceLease._write
    def broken(self, name, doc):
        phase = doc.get("phase")
        if name == "journal.json" and ((point == "before_reserved_journal" and phase == "reserved")
                or (point == "before_intent_journal" and phase == "publish_intent")):
            raise OSError("disk full")
        original_write(self, name, doc)
        if point == "after_intent_rename" and phase == "publish_intent":
            raise OSError("lost acknowledgment")
        if point == "before_marker_unlink" and phase == "complete":
            raise OSError("crash after durable complete")
    monkeypatch.setattr(m.MaintenanceLease, "_write", broken)
    with pytest.raises(OSError):
        with m.MaintenanceLease(state_dir, Hooks()) as lease:
            advance(lease, "complete")
    assert (state_dir / "blocked").exists()
    monkeypatch.setattr(m.MaintenanceLease, "_write", original_write)
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        if point == "before_reserved_journal":
            with pytest.raises(m.MaintenanceError, match="не соответствует"):
                lease.inspect()
        else:
            state = lease.inspect()
            assert state["blocked"]
            if point == "before_marker_unlink":
                assert state["journal"]["phase"] == "complete"
                lease.finish(NONCE)
                assert not lease.inspect()["blocked"]
            elif point == "after_intent_rename":
                assert len(state["journal"]["attempts"]) == 1
                with pytest.raises(m.MaintenanceError): lease.cancel_before_publish(NONCE)
            else:
                assert state["journal"]["attempts"] == []


def test_crash_after_marker_unlink_has_durable_complete_receipt(state_dir, monkeypatch):
    original_unlink = m.os.unlink
    def unlink(name, **kwargs):
        original_unlink(name, **kwargs)
        if name == "blocked": raise OSError("crash before final directory fsync")
    monkeypatch.setattr(m.os, "unlink", unlink)
    with pytest.raises(OSError):
        with m.MaintenanceLease(state_dir, Hooks()) as lease:
            advance(lease, "complete")
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        assert lease.finish(NONCE)["phase"] == "complete"
        assert not lease.inspect()["blocked"]


def test_real_abrupt_process_exit_releases_flock_but_not_gate(state_dir):
    # Real process exit bypasses __exit__, destructors and Python exception cleanup.
    test_path = Path(__file__).resolve()
    code = f'''import importlib.util, os
spec = importlib.util.spec_from_file_location("fixture", {str(test_path)!r})
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
with f.m.MaintenanceLease({str(state_dir)!r}, f.Hooks()) as lease:
    f.advance(lease, "publish_intent")
    os._exit(73)
'''
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 73, result.stderr
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        assert lease.recover(NONCE)["phase"] == "publish_intent"
        assert lease.inspect()["blocked"]


@pytest.mark.parametrize("name", ["coordinator.lock", "journal.json", "blocked"])
@pytest.mark.parametrize("kind", ["symlink", "hardlink"])
def test_unsafe_state_files_refused_without_touching_target(state_dir, tmp_path, name, kind):
    state_dir.mkdir(mode=0o700)
    target = tmp_path / "unrelated"
    target.write_text('{"unchanged":true}')
    target.chmod(0o600)
    if kind == "symlink": (state_dir / name).symlink_to(target)
    else: os.link(target, state_dir / name)
    with pytest.raises((OSError, m.MaintenanceError)):
        with m.MaintenanceLease(state_dir, Hooks()) as lease:
            reserve(lease)
    assert target.read_text() == '{"unchanged":true}'


def test_symlink_ancestor_refused(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    with pytest.raises(OSError):
        with m.MaintenanceLease(tmp_path / "alias" / "lease", Hooks()): pass
    assert not (real / "lease").exists()


def test_world_readable_state_directory_refused(state_dir):
    state_dir.mkdir(mode=0o755)
    with pytest.raises(m.MaintenanceError, match="0700"):
        with m.MaintenanceLease(state_dir, Hooks()): pass


@pytest.mark.parametrize("mutation", ["missing_marker", "duplicate_json_key", "unknown_phase", "wrong_marker_nonce", "skipped_attempt_id"])
def test_corrupt_or_inconsistent_state_stays_blocked(state_dir, mutation):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "publish_intent")
    path = state_dir / "journal.json"
    doc = json.loads(path.read_text())
    if mutation == "missing_marker":
        (state_dir / "blocked").unlink()
    elif mutation == "duplicate_json_key":
        path.write_text(path.read_text().replace('"phase":"publish_intent"', '"phase":"ready","phase":"publish_intent"'))
    elif mutation == "unknown_phase":
        doc["phase"] = "expired_safe"; path.write_text(json.dumps(doc))
    elif mutation == "wrong_marker_nonce":
        (state_dir / "blocked").write_text(json.dumps({"schema_version": 1, "nonce": "0" * 32}))
    elif mutation == "skipped_attempt_id":
        doc["attempts"][0]["attempt_id"] = 2; path.write_text(json.dumps(doc))
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        with pytest.raises(m.MaintenanceError): lease.recover(NONCE)
        with pytest.raises(m.MaintenanceError): reserve(lease, "2" * 32)
    assert (state_dir / "blocked").exists() or mutation == "missing_marker"


def test_missing_verification_hook_has_no_default_approval(state_dir):
    hooks = Hooks()
    hooks.verify_safe = None
    with pytest.raises(m.MaintenanceError, match="verify_safe"):
        m.MaintenanceLease(state_dir, hooks)


@pytest.mark.parametrize("field,value", [("nonce", "../secret"), ("region", "ural"), ("release_id", "a"), ("source_commit", "HEAD")])
def test_invalid_reservation_never_creates_gate(state_dir, field, value):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        args = dict(region="hmao", release_id=RELEASE, source_commit=SOURCE, nonce=NONCE)
        args[field] = value
        with pytest.raises(m.MaintenanceError): lease.reserve(**args)
    assert not (state_dir / "blocked").exists()


def test_fsync_failure_does_not_return_publication_permission(state_dir, monkeypatch):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "ready")
        original = m.os.fsync
        calls = 0
        def fail_on_directory(fd):
            nonlocal calls
            calls += 1
            if calls == 2: raise OSError("directory fsync failed")
            return original(fd)
        monkeypatch.setattr(m.os, "fsync", fail_on_directory)
        with pytest.raises(OSError, match="fsync failed"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
        assert calls == 2
        assert (state_dir / "blocked").exists()
        assert lease.inspect()["journal"]["phase"] == "publish_intent"


@pytest.mark.parametrize("name", ["coordinator.lock", "journal.json", "blocked"])
def test_fifo_state_file_is_rejected_without_waiting_for_writer(state_dir, name):
    state_dir.mkdir(mode=0o700)
    os.mkfifo(state_dir / name, 0o600)
    # Bound this regression in a child: removing O_NONBLOCK must fail a test,
    # never hang the whole test suite while waiting for a FIFO writer.
    code = f"""import importlib.util
spec = importlib.util.spec_from_file_location("fixture", {str(Path(__file__).resolve())!r})
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
try:
    with f.m.MaintenanceLease({str(state_dir)!r}, f.Hooks()) as lease:
        f.reserve(lease)
except f.m.MaintenanceError as exc:
    assert "Небезопасный файл" in str(exc), str(exc)
else:
    raise SystemExit("FIFO was accepted")
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, result.stderr


def test_state_permissions_changed_mid_lease_fail_closed(state_dir):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "ready")
        state_dir.chmod(0o755)
        with pytest.raises(m.MaintenanceError, match="Каталог окна"):
            lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
    assert (state_dir / "blocked").exists()
