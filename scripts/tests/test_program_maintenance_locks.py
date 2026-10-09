"""Real Linux atomic-lock publication; no production services or admission proof."""
import importlib.util
import hashlib
import errno
import types
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys

import pytest
import program_maintenance_locks as locks

NONCE = "a" * 32
linux = pytest.mark.skipif(sys.platform != "linux", reason="Actual Linux renameat2 is mandatory, never emulated")
ROOT = Path(__file__).resolve().parents[2]


def fixture(root, *, state_name="state"):
    paths = {r: root / r / "ops/mac-local-run/.run.lock" for r in locks.REGIONS}
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    state = root / state_name
    state.mkdir(mode=0o700, exist_ok=True)
    marker = root / "blocked"
    if not marker.exists():
        marker.write_text(json.dumps({"schema_version": 2, "nonce": NONCE}))
        marker.chmod(0o600)
    return paths, state, marker


def adapter(root, **kwargs):
    paths, state, marker = fixture(root, state_name=kwargs.pop("state_name", "state"))
    return locks.AtomicRunLocks(paths, state, marker, NONCE, check_admission=lambda: None, **kwargs)


def load_helper():
    spec = importlib.util.spec_from_file_location("existing_run_lock", ROOT / "ops/mac-local-run/run_lock.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper



def load_legacy():
    path = ROOT / "scripts/tests/fixtures/maintenance/legacy_run_lock_42065f6.txt"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "d4e439ae9f2c2551d130236a4863c2e3302876d33917345faf014b054e748b58"
    module = types.ModuleType("frozen_legacy_run_lock")
    exec(compile(raw, str(path), "exec"), module.__dict__)
    return module


def test_frozen_legacy_fixture_matches_exact_source_blob():
    assert callable(load_legacy().acquire)


@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EINVAL, errno.EXDEV, errno.EOPNOTSUPP])
def test_unsupported_syscall_or_filesystem_never_falls_back(monkeypatch, error):
    class Rename:
        def __call__(self, *args):
            locks.ctypes.set_errno(error)
            return -1
    monkeypatch.setattr(locks.sys, "platform", "linux")
    monkeypatch.setattr(locks.ctypes, "CDLL", lambda *a, **k: types.SimpleNamespace(renameat2=Rename()))
    with pytest.raises(locks.AtomicLockError, match="no fallback"):
        locks.rename_noreplace(0, "old", 0, "new")


def test_unsupported_platform_has_no_rename_fallback(monkeypatch):
    monkeypatch.setattr(locks.sys, "platform", "darwin")
    with pytest.raises(locks.AtomicLockError, match="Linux renameat2"):
        locks.rename_noreplace(0, "old", 0, "new")


def test_legacy_locale_mismatch_is_fail_closed(monkeypatch):
    replies = iter((subprocess.CompletedProcess([], 0, "localized start\n", ""),
                    subprocess.CompletedProcess([], 0, "C start\n", "")))
    monkeypatch.setattr(locks.subprocess, "run", lambda *a, **k: next(replies))
    with pytest.raises(locks.AtomicLockError, match="locale"):
        locks.process_start(os.getpid())


@linux
def test_complete_owner_is_visible_at_atomic_publication_and_legacy_busy(tmp_path):
    events = []
    def observe(event, region):
        receipt = json.loads((tmp_path / "state" / NONCE / "receipt.json").read_text())
        record = receipt["locks"][region]
        if event == "receipt":
            assert not Path(record["target"]).exists()
        elif event == "published":
            path = Path(record["target"])
            assert sorted(p.name for p in path.iterdir()) == ["owner.json"]
            assert json.loads((path / "owner.json").read_text()) == record["owner"]
            assert [path.stat().st_dev, path.stat().st_ino] == record["inode"]
        events.append(event)
    with adapter(tmp_path, checkpoint=observe) as current:
        receipt = current.acquire()
        assert receipt["phase"] == "held" and events.count("published") == 4
        current.assert_owned()
        for path in current.paths.values():
            # The old importer simply mkdirs; a complete coordinator blocks it.
            with pytest.raises(FileExistsError):
                path.mkdir()
            # Legacy owner format is the real ps string recognized by helpers.
            owner = json.loads((path / "owner.json").read_text())
            assert owner["version"] == 1 and owner["process_start"] == locks.process_start(owner["pid"])
            assert load_helper().acquire(str(path), os.getpid()) == 1
            assert load_legacy().acquire(str(path), os.getpid()) == 1
        guard_inodes = {r: Path(str(p) + ".guard").stat().st_ino for r, p in current.paths.items()}
        assert current.release()["phase"] == "released"
        assert not any(path.exists() for path in current.paths.values())
        assert guard_inodes == {r: Path(str(p) + ".guard").stat().st_ino for r, p in current.paths.items()}


@linux
@pytest.mark.parametrize("kind", ["empty", "live", "dead", "file", "symlink"])
def test_existing_last_lock_preserved_and_first_three_compensated(tmp_path, kind):
    paths, _, _ = fixture(tmp_path)
    path = paths["tyumen"]
    if kind in ("empty", "live", "dead"):
        path.mkdir()
        if kind != "empty":
            (path / "owner.json").write_text(json.dumps({"version": 1,
                "pid": os.getpid() if kind == "live" else 99999999,
                "process_start": locks.process_start(os.getpid()) if kind == "live" else "old"}))
    elif kind == "file":
        path.write_text("foreign")
    else:
        path.symlink_to(tmp_path / "not-present")
    inode = path.lstat().st_ino
    with adapter(tmp_path) as current:
        with pytest.raises(locks.AtomicLockError, match="Existing lock"):
            current.acquire()
        assert current.receipt()["phase"] == "acquire_failed"
        assert not any(paths[r].exists() for r in locks.REGIONS[:-1])
        assert path.lstat().st_ino == inode
        if kind == "empty":
            assert list(path.iterdir()) == []


@linux
@pytest.mark.parametrize("tamper", ["owner", "inode", "foreign-file", "guard", "marker", "receipt"])
def test_unknown_ownership_blocks_release_and_preserves_all_files(tmp_path, tamper):
    with adapter(tmp_path) as current:
        current.acquire()
        path = current.paths["hmao"]
        if tamper == "owner":
            (path / "owner.json").write_text("foreign owner")
        elif tamper == "inode":
            path.rename(path.with_name("foreign-preserved"))
            path.mkdir()
        elif tamper == "foreign-file":
            (path / "extra").write_text("do not delete")
        elif tamper == "guard":
            guard = Path(str(path) + ".guard")
            guard.rename(guard.with_name("old-guard"))
            guard.touch(mode=0o600)
        elif tamper == "marker":
            current.marker.unlink()
        else:
            (current.state / NONCE / "receipt.json").write_text("tampered receipt")
        with pytest.raises((locks.AtomicLockError, FileNotFoundError)):
            current.release()
        assert all(p.exists() for p in current.paths.values())
        if tamper == "foreign-file":
            assert (path / "extra").read_text() == "do not delete"


def contender(root, state_name, barrier, results, finish):
    with adapter(Path(root), state_name=state_name) as current:
        barrier.wait(20)
        try:
            current.acquire()
        except locks.AtomicLockError:
            results.put((os.getpid(), False))
            return
        results.put((os.getpid(), True))
        if not finish.wait(20):
            raise AssertionError("fixture handshake expired")
        current.release()


@linux
def test_real_competing_coordinators_have_one_complete_owner(tmp_path):
    fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    barrier, queue, finish = context.Barrier(4), context.Queue(), context.Event()
    processes = [context.Process(target=contender, args=(str(tmp_path), f"state-{i}", barrier, queue, finish)) for i in range(3)]
    for process in processes:
        process.start()
    try:
        barrier.wait(20)
        observed = [queue.get(timeout=20) for _ in processes]
        winners = [pid for pid, won in observed if won]
        assert len(winners) == 1
        for region in locks.REGIONS:
            owner = json.loads((tmp_path / region / "ops/mac-local-run/.run.lock/owner.json").read_text())
            assert owner["pid"] == winners[0]
    finally:
        finish.set()
        for process in processes:
            process.join(10)
            if process.is_alive():
                process.kill()
                process.join(5)
            assert process.exitcode == 0


def crash_after_publish(root):
    def checkpoint(event, region):
        if event == "published":
            os._exit(73)
    current = adapter(Path(root), checkpoint=checkpoint)
    current.acquire()


@linux
def test_crash_receipt_retains_published_inode_and_new_acquire_never_reclaims(tmp_path):
    fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=crash_after_publish, args=(str(tmp_path),))
    process.start()
    process.join(20)
    assert process.exitcode == 73
    receipt = json.loads((tmp_path / "state" / NONCE / "receipt.json").read_text())
    path = tmp_path / "hmao/ops/mac-local-run/.run.lock"
    original = path.stat().st_ino
    assert receipt["locks"]["hmao"]["inode"][1] == original
    assert json.loads((path / "owner.json").read_text())["pid"] == process.pid
    with adapter(tmp_path, state_name="new-state") as next_owner:
        with pytest.raises(locks.AtomicLockError, match="Existing lock"):
            next_owner.acquire()
    assert path.stat().st_ino == original
    assert (tmp_path / "blocked").exists()


def plain_importer(path, barrier, results, finish):
    path = Path(path)
    barrier.wait(20)
    try:
        path.mkdir()
    except FileExistsError:
        results.put(("import", False))
        return
    results.put(("import", True))
    if not finish.wait(20):
        raise AssertionError("fixture handshake expired")
    path.rmdir()


@linux
def test_plain_legacy_mkdir_and_coordinator_cannot_both_win(tmp_path):
    paths, _, _ = fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    barrier, queue, finish = context.Barrier(3), context.Queue(), context.Event()
    processes = [
        context.Process(target=contender, args=(str(tmp_path), "coordinator-state", barrier, queue, finish)),
        context.Process(target=plain_importer, args=(str(paths["tyumen"]), barrier, queue, finish)),
    ]
    for process in processes:
        process.start()
    try:
        barrier.wait(20)
        results = [queue.get(timeout=20) for _ in processes]
        assert sum(won for _, won in results) == 1
        winner = next(actor for actor, won in results if won)
        if winner == "import":
            assert list(paths["tyumen"].iterdir()) == []
            assert not any(paths[r].exists() for r in locks.REGIONS[:-1])
        else:
            assert all(json.loads((path / "owner.json").read_text())["pid"] == winner
                       for path in paths.values())
    finally:
        finish.set()
        for process in processes:
            process.join(10)
            if process.is_alive():
                process.kill()
                process.join(5)
            assert process.exitcode == 0


def old_parser(path, barrier, results, finish):
    helper = load_legacy()
    barrier.wait(20)
    rc = helper.acquire(path, os.getpid())
    results.put(("legacy", rc == 0))
    if rc:
        return
    if not finish.wait(20):
        raise AssertionError("fixture handshake expired")
    assert helper.release(path, os.getpid()) == 0


@linux
def test_exact_old_parser_and_coordinator_cannot_both_win(tmp_path):
    paths, _, _ = fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    barrier, queue, finish = context.Barrier(3), context.Queue(), context.Event()
    processes = [
        context.Process(target=contender, args=(str(tmp_path), "coordinator-state", barrier, queue, finish)),
        context.Process(target=old_parser, args=(str(paths["tyumen"]), barrier, queue, finish)),
    ]
    for process in processes:
        process.start()
    try:
        barrier.wait(20)
        results = [queue.get(timeout=20) for _ in processes]
        assert sum(won for _, won in results) == 1
        winner = next(actor for actor, won in results if won)
        if winner == "legacy":
            assert json.loads((paths["tyumen"] / "owner.json").read_text())["pid"] == processes[1].pid
            assert not any(paths[r].exists() for r in locks.REGIONS[:-1])
        else:
            assert all(json.loads((path / "owner.json").read_text())["pid"] == winner
                       for path in paths.values())
    finally:
        finish.set()
        for process in processes:
            process.join(10)
            if process.is_alive():
                process.kill()
                process.join(5)
            assert process.exitcode == 0


def crash_at_checkpoint(root, event, region):
    def checkpoint(actual_event, actual_region):
        if (actual_event, actual_region) == (event, region):
            os._exit(74)
    current = adapter(Path(root), checkpoint=checkpoint)
    current.acquire()
    current.release()


@linux
@pytest.mark.parametrize("event", ["receipt", "published", "retired"])
@pytest.mark.parametrize("region", locks.REGIONS)
def test_real_crash_at_every_lock_boundary_retains_exact_inode_receipt(tmp_path, event, region):
    fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=crash_at_checkpoint, args=(str(tmp_path), event, region))
    process.start()
    process.join(20)
    if process.is_alive():
        process.kill()
        process.join(5)
    assert process.exitcode == 74
    folder = tmp_path / "state" / NONCE
    receipt = json.loads((folder / "receipt.json").read_text())
    assert receipt["pid"] == process.pid and receipt["nonce"] == NONCE
    assert (tmp_path / "blocked").exists()
    # Crash can precede the status update. Recorded inode and predetermined
    # old/new path pair still identify exactly one retained directory.
    for record in receipt["locks"].values():
        choices = [Path(record["target"]), folder / record["prepared"], folder / record["retired"]]
        found = [path for path in choices if path.exists()]
        assert len(found) == 1
        assert [found[0].stat().st_dev, found[0].stat().st_ino] == record["inode"]
        assert json.loads((found[0] / "owner.json").read_text()) == record["owner"]
