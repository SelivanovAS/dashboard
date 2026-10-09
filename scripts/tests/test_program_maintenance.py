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
MANIFEST = "6" * 64


class Hooks:
    def __init__(self):
        self.calls = []
        self.held = None
        self.marker = None
        self.fail = None
        self.snapshot_value = {r: {"head": SHA, "release_id": "f" * 64, "managed_sha256": "1" * 64,
                                   "inventory_sha256": "7" * 64} for r in m.REGIONS}
        self.safe_journals = []
        self.target_journals = []

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

    def validate_target(self, journal):
        self.call("validate_target")
        self.target_journals.append(copy.deepcopy(journal))
        for region in m.REGIONS:
            if journal["snapshot"][region]["inventory_sha256"] != self.snapshot_value[region]["inventory_sha256"]:
                raise m.MaintenanceError("changed inventory")

    def check_exit(self, journal):
        self.call("check_exit")
        assert journal["phase"] in ("verified", "safe_to_resume", "complete")
        assert self.marker.exists()

    def apply_target(self, journal):
        self.call("apply_target")
        assert self.held == journal["nonce"]
        assert self.marker.is_file()
        persisted = json.loads((self.marker.parent / "journal.json").read_text())
        assert persisted["phase"] == "applying"
        assert persisted == journal

    def verify_installed(self, journal):
        self.call("verify_installed")
        assert self.held == journal["nonce"]
        return {"target_id": journal["active_target_id"], "manifest_sha256": journal["manifest_sha256"],
                "installed_sha": journal["attempts"][-1]["target_sha"], "release_id": journal["release_id"], "evidence_sha256": "2" * 64}

    def verify_safe(self, journal):
        self.call("verify_safe")
        self.safe_journals.append(copy.deepcopy(journal))
        return {"covered_attempt_ids": [a["attempt_id"] for a in journal["attempts"]], "evidence_sha256": "3" * 64}


@pytest.fixture
def state_dir(tmp_path):
    return tmp_path / "maintenance"


def reserve(lease, nonce=NONCE):
    return lease.reserve(region="hmao", release_id=RELEASE, source_commit=SOURCE,
                         manifest_sha256=MANIFEST, nonce=nonce)


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


def test_apply_records_phase_before_host_write_and_requires_separate_verification(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "publish_intent")
        applied = lease.apply(NONCE, 1)
        assert applied["phase"] == "applying" and applied["verification"] is None
        assert hooks.calls.count("apply_target") == 1
        assert lease.inspect()["blocked"]
        with pytest.raises(m.MaintenanceError):
            lease.finish(NONCE)
        lease.mark_verified(NONCE)
        lease.finish(NONCE)


@pytest.mark.parametrize("fault", ["apply_target", "assert_locks", "drain"])
def test_apply_failure_preserves_gate_and_intent(state_dir, fault):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "publish_intent")
        hooks.fail = fault
        with pytest.raises(m.MaintenanceError):
            lease.apply(NONCE, 1)
        assert lease.inspect()["blocked"]
        assert len(lease.inspect()["journal"]["attempts"]) == 1
        if fault != "apply_target":
            assert "apply_target" not in hooks.calls
        hooks.fail = None


def test_apply_cannot_write_before_intent_or_for_stale_attempt(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "ready")
        with pytest.raises(m.MaintenanceError):
            lease.apply(NONCE, 1)
        lease.publish_intent(NONCE, base_sha=SHA, target_sha=TARGET)
        with pytest.raises(m.MaintenanceError):
            lease.apply(NONCE, 2)
        assert "apply_target" not in hooks.calls


@pytest.mark.parametrize("field,value", [("nonce", "../secret"), ("region", "ural"), ("release_id", "a"), ("source_commit", "HEAD")])
def test_invalid_reservation_never_creates_gate(state_dir, field, value):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        args = dict(region="hmao", release_id=RELEASE, source_commit=SOURCE,
                    manifest_sha256=MANIFEST, nonce=NONCE)
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


def recovery_target(lease, *, reason="rollback"):
    return lease.select_recovery_target(NONCE, release_id="8" * 64,
                                        source_commit="9" * 40,
                                        manifest_sha256="0" * 64, reason=reason)


def test_v2_recovery_keeps_original_intents_and_inventories_across_real_crash(state_dir):
    # Crash after durable selection, before sending a recovery publication.
    code = f'''import importlib.util, os
spec = importlib.util.spec_from_file_location("fixture", {str(Path(__file__).resolve())!r})
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
with f.m.MaintenanceLease({str(state_dir)!r}, f.Hooks()) as lease:
    f.advance(lease, "applying")
    f.recovery_target(lease)
    os._exit(73)
'''
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
    assert result.returncode == 73, result.stderr
    persisted = json.loads((state_dir / "journal.json").read_text())
    assert persisted["phase"] == "recovery_ready"
    assert persisted["active_attempt_id"] is None
    assert persisted["attempts"] == [{"attempt_id": 1, "target_id": 1, "base_sha": SHA, "target_sha": TARGET}]
    hooks = Hooks()
    # Recovery is allowed over a partial checkout; protected inventory is still
    # the original four-region snapshot. Equality to old HEAD is inappropriate.
    hooks.snapshot_value["hmao"]["head"] = "4" * 40
    with m.MaintenanceLease(state_dir, hooks, boot_id="after-reboot") as lease:
        lease.recover(NONCE)
        with pytest.raises(m.MaintenanceError):
            lease.cancel_before_publish(NONCE)
        permission = lease.publish_intent(NONCE, base_sha=TARGET, target_sha="5" * 40)
        assert permission["attempt_id"] == permission["target_id"] == 2
        with pytest.raises(m.MaintenanceError, match="Устаревшее"):
            lease.begin_apply(NONCE, 1)
        lease.begin_apply(NONCE, 2)
        lease.mark_verified(NONCE)
        result = lease.finish(NONCE)
        assert not lease.inspect()["blocked"]
    assert result["targets"][0]["release_id"] == RELEASE
    assert result["targets"][1]["reason"] == "rollback"
    assert result["snapshot"] == persisted["snapshot"]
    assert result["verification"]["target_id"] == 2
    assert result["safe_evidence"]["covered_attempt_ids"] == [1, 2]
    assert hooks.target_journals[-1]["attempts"] == persisted["attempts"]
    assert all(len(doc["attempts"]) == 2 for doc in hooks.safe_journals)


@pytest.mark.parametrize("phase", ["publish_intent", "applying", "verified"])
def test_recovery_selection_never_cancels_or_reopens_postintent_lease(state_dir, phase):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, phase)
        before = lease.inspect()["journal"]
        selected = recovery_target(lease, reason="repair")
        assert selected["snapshot"] == before["snapshot"]
        assert selected["attempts"] == before["attempts"]
        assert selected["verification"] is selected["safe_evidence"] is None
        assert selected["targets"][-1]["reason"] == "repair"
        for action in (lease.cancel_before_publish, lease.finish):
            with pytest.raises(m.MaintenanceError): action(NONCE)
        assert lease.inspect()["blocked"]


def test_recovery_finish_refuses_safe_evidence_covering_only_new_target(state_dir):
    hooks = Hooks()
    hooks.verify_safe = lambda doc: {"covered_attempt_ids": [2], "evidence_sha256": "3" * 64}
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "applying")
        recovery_target(lease)
        lease.publish_intent(NONCE, base_sha=TARGET, target_sha="5" * 40)
        lease.begin_apply(NONCE, 2)
        lease.mark_verified(NONCE)
        with pytest.raises(m.MaintenanceError, match="все разрешённые"):
            lease.finish(NONCE)
    assert (state_dir / "blocked").exists()


@pytest.mark.parametrize("field,value", [("target_id", 1), ("release_id", RELEASE), ("manifest_sha256", MANIFEST)])
def test_recovery_verification_must_match_current_target_and_exact_manifest(state_dir, field, value):
    hooks = Hooks()
    original = hooks.verify_installed
    def wrong(doc):
        result = original(doc)
        result[field] = value
        return result
    hooks.verify_installed = wrong
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "applying")
        recovery_target(lease)
        lease.publish_intent(NONCE, base_sha=TARGET, target_sha="5" * 40)
        lease.begin_apply(NONCE, 2)
        with pytest.raises(m.MaintenanceError, match="другой пакет"):
            lease.mark_verified(NONCE)
        assert lease.inspect()["journal"]["phase"] == "applying"
    assert (state_dir / "blocked").exists()


@pytest.mark.parametrize("region", m.REGIONS)
def test_original_protected_inventory_is_bound_through_recovery_intents(state_dir, region):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "applying")
        recovery_target(lease)
        hooks.snapshot_value[region]["inventory_sha256"] = "8" * 64
        with pytest.raises(m.MaintenanceError, match="inventory"):
            lease.publish_intent(NONCE, base_sha=TARGET, target_sha="5" * 40)
        assert len(lease.inspect()["journal"]["attempts"]) == 1
    assert (state_dir / "blocked").exists()


@pytest.mark.parametrize("bad", [None, "hash", 1])
def test_snapshot_requires_full_inventory_digest_before_first_permission(state_dir, bad):
    hooks = Hooks()
    hooks.snapshot_value["tyumen"]["inventory_sha256"] = bad
    with m.MaintenanceLease(state_dir, hooks) as lease:
        reserve(lease)
        with pytest.raises(m.MaintenanceError, match="inventory_sha256"):
            lease.prepare(NONCE)
        assert lease.inspect()["journal"]["snapshot"] is None
    assert (state_dir / "blocked").exists()


def test_missing_inventory_is_not_silently_filled_or_recomputed(state_dir):
    hooks = Hooks()
    del hooks.snapshot_value["tyumen"]["inventory_sha256"]
    with m.MaintenanceLease(state_dir, hooks) as lease:
        reserve(lease)
        with pytest.raises(m.MaintenanceError, match="снимок территории"):
            lease.prepare(NONCE)


@pytest.mark.parametrize("phase", ["publish_intent", "complete"])
def test_v1_journal_is_rejected_without_migration_or_marker_removal(state_dir, phase):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, phase)
    path = state_dir / "journal.json"
    legacy = json.loads(path.read_text())
    legacy["schema_version"], legacy["protocol"] = 1, "court-program-maintenance/1"
    for key in ("targets", "active_target_id", "manifest_sha256"):
        del legacy[key]
    for record in legacy["snapshot"].values(): del record["inventory_sha256"]
    for attempt in legacy["attempts"]: del attempt["target_id"]
    if legacy["verification"]:
        del legacy["verification"]["target_id"]
        del legacy["verification"]["manifest_sha256"]
    path.write_text(json.dumps(legacy))
    marker = state_dir / "blocked"
    if phase != "complete": marker.write_text(json.dumps({"schema_version": 1, "nonce": NONCE}))
    before = {p.name: p.read_bytes() for p in state_dir.iterdir()}
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        for action in (lease.recover, lease.finish):
            with pytest.raises(m.MaintenanceError): action(NONCE)
        with pytest.raises(m.MaintenanceError): reserve(lease, "4" * 32)
    assert before == {p.name: p.read_bytes() for p in state_dir.iterdir()}


@pytest.mark.parametrize("mutation", ["unknown_target", "old_active_target", "current_manifest", "invalid_reason", "backwards_attempt"])
def test_inconsistent_v2_target_history_fails_closed(state_dir, mutation):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "applying")
        recovery_target(lease)
        lease.publish_intent(NONCE, base_sha=TARGET, target_sha="5" * 40)
    path = state_dir / "journal.json"
    doc = json.loads(path.read_text())
    if mutation == "unknown_target": doc["attempts"][-1]["target_id"] = 3
    elif mutation == "old_active_target": doc["active_target_id"] = 1
    elif mutation == "current_manifest": doc["manifest_sha256"] = MANIFEST
    elif mutation == "invalid_reason": doc["targets"][-1]["reason"] = "abort"
    else:
        doc["attempts"][0]["target_id"] = 2
        doc["attempts"][-1]["target_id"] = 1
    path.write_text(json.dumps(doc))
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        with pytest.raises(m.MaintenanceError): lease.recover(NONCE)
    assert (state_dir / "blocked").exists()


def test_recovery_target_fsync_ack_loss_preserves_both_targets(state_dir, monkeypatch):
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        advance(lease, "applying")
        original = m.os.fsync
        calls = 0
        def fail_directory(fd):
            nonlocal calls
            calls += 1
            if calls == 2: raise OSError("lost target acknowledgment")
            return original(fd)
        monkeypatch.setattr(m.os, "fsync", fail_directory)
        with pytest.raises(OSError): recovery_target(lease)
        monkeypatch.setattr(m.os, "fsync", original)
        state = lease.inspect()
        assert state["blocked"] and state["journal"]["phase"] == "recovery_ready"
        assert len(state["journal"]["targets"]) == 2
        assert len(state["journal"]["attempts"]) == 1
    with m.MaintenanceLease(state_dir, Hooks()) as lease:
        assert lease.recover(NONCE)["active_target_id"] == 2


def test_finite_exit_requires_explicit_mode_and_mandatory_live_check(state_dir):
    hooks = Hooks()
    hooks.check_exit = None
    with pytest.raises(m.MaintenanceError, match="check_exit"):
        m.MaintenanceLease(state_dir, hooks, allow_verified_pending_jobs=True)
    with m.MaintenanceLease(state_dir, hooks) as lease:
        advance(lease, "verified")
        hooks.fail = "drain"
        with pytest.raises(m.MaintenanceError, match="drain"):
            lease.finish(NONCE)
    assert (state_dir / "blocked").exists()


def test_explicit_verified_exit_permits_natural_pending_jobs_without_another_drain(state_dir):
    hooks = Hooks()
    with m.MaintenanceLease(state_dir, hooks, allow_verified_pending_jobs=True) as lease:
        advance(lease, "verified")
        hooks.calls.clear()
        hooks.fail = "drain"  # A natural pending job persists beyond verified.
        lease.finish(NONCE)
        assert not lease.inspect()["blocked"]
    assert "drain" not in hooks.calls
    assert hooks.calls.count("check_exit") == 3  # Reassert locks, resume, after unlock.
    assert hooks.calls.count("verify_safe") == 2


def test_failed_final_exit_check_keeps_gate_and_recovers_without_clearing_evidence(state_dir):
    hooks = Hooks()
    original = hooks.check_exit
    def check(doc):
        original(doc)
        if hooks.held is None: raise m.MaintenanceError("writer still active")
    hooks.check_exit = check
    with m.MaintenanceLease(state_dir, hooks, allow_verified_pending_jobs=True) as lease:
        advance(lease, "verified")
        with pytest.raises(m.MaintenanceError, match="writer still active"):
            lease.finish(NONCE)
    assert (state_dir / "blocked").exists()
    hooks = Hooks()
    hooks.fail = "drain"
    with m.MaintenanceLease(state_dir, hooks, allow_verified_pending_jobs=True) as lease:
        recovered = lease.recover(NONCE)
        assert recovered["phase"] == "safe_to_resume"
        lease.finish(NONCE)
        assert not lease.inspect()["blocked"]
