"""Real Linux atomic-lock publication; no production services or admission proof."""
import importlib.util
import copy
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
        if region is None:
            return  # Separate tests exercise receipt file rename boundaries.
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


def test_recovery_liveness_refuses_live_and_permission_unknown(monkeypatch):
    epoch = {"pid": 42, "process_start": "saved", "boot_id": "same"}
    monkeypatch.setattr(locks.os, "kill", lambda *args: None)
    monkeypatch.setattr(locks, "process_start", lambda pid: "saved")
    with pytest.raises(locks.AtomicLockError, match="still alive"):
        locks.AtomicRunLocks._prove_dead(epoch, "same")
    def denied(*args):
        raise PermissionError("cannot read process")
    monkeypatch.setattr(locks.os, "kill", denied)
    with pytest.raises(locks.AtomicLockError, match="liveness is unknown"):
        locks.AtomicRunLocks._prove_dead(epoch, "same")


def test_recovery_failed_ps_does_not_prove_death(monkeypatch):
    epoch = {"pid": 42, "process_start": "saved", "boot_id": "same"}
    monkeypatch.setattr(locks.os, "kill", lambda *args: None)
    def failed(pid):
        raise locks.AtomicLockError("ps failed")
    monkeypatch.setattr(locks, "process_start", failed)
    with pytest.raises(locks.AtomicLockError, match="start identity is unknown"):
        locks.AtomicRunLocks._prove_dead(epoch, "same")


def test_recovery_accepts_only_esrch_prior_boot_or_proven_pid_reuse(monkeypatch):
    epoch = {"pid": 42, "process_start": "saved", "boot_id": "before"}
    def forbidden(*args):
        raise AssertionError("old boot cannot contain a live process")
    monkeypatch.setattr(locks.os, "kill", forbidden)
    locks.AtomicRunLocks._prove_dead(epoch, "after")
    monkeypatch.setattr(locks.os, "kill", lambda *args: None)
    monkeypatch.setattr(locks, "process_start", lambda pid: "different process")
    locks.AtomicRunLocks._prove_dead(epoch, "before")
    def absent(*args):
        raise ProcessLookupError("gone")
    monkeypatch.setattr(locks.os, "kill", absent)
    locks.AtomicRunLocks._prove_dead(epoch, "before")


def run_crash(root, event, region, *, recovery=False):
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=crash_during_recovery if recovery else crash_at_checkpoint,
                              args=(str(root), event, region))
    process.start()
    process.join(30)
    if process.is_alive():
        process.kill()
        process.join(5)
    assert process.exitcode == 74
    return process.pid


def assert_recovered(current, previous_pid, generation):
    result = current.recover()
    assert result["phase"] == "held" and result["generation"] == generation
    assert result["nonce"] == NONCE and result["pid"] == os.getpid()
    assert result["pid"] != previous_pid and len(result["history"]) == generation - 1
    assert current.marker.exists()
    current.assert_owned()
    for record in result["locks"].values():
        path = Path(record["target"])
        assert json.loads((path / "owner.json").read_text()) == current.owner
        assert [path.stat().st_dev, path.stat().st_ino] == record["inode"]
    current.release()
    assert current.marker.exists(), "Lock recovery/release must never open admission"
    return result


@linux
@pytest.mark.parametrize("event", ["receipt", "published", "retired"])
@pytest.mark.parametrize("region", locks.REGIONS)
def test_explicit_recovery_of_each_original_crash_boundary(tmp_path, event, region):
    fixture(tmp_path)
    pid = run_crash(tmp_path, event, region)
    with adapter(tmp_path) as current:
        result = assert_recovered(current, pid, 2)
        assert result["history"][0]["pid"] == pid


def crash_during_recovery(root, event, region):
    def checkpoint(actual_event, actual_region):
        if (actual_event, actual_region) == (event, region):
            os._exit(74)
    current = adapter(Path(root), checkpoint=checkpoint)
    current.recover()


@linux
@pytest.mark.parametrize("event,region", [("recovery_begin", None)] +
    [(event, region) for event in ("recovery_retired", "recovery_receipt", "recovery_published") for region in locks.REGIONS])
def test_second_crash_during_recovery_keeps_all_generations_recoverable(tmp_path, event, region):
    fixture(tmp_path)
    first_pid = run_crash(tmp_path, "published", "tyumen")
    second_pid = run_crash(tmp_path, event, region, recovery=True)
    with adapter(tmp_path) as current:
        result = assert_recovered(current, second_pid, 3)
        assert [epoch["pid"] for epoch in result["history"]] == [first_pid, second_pid]
        assert len({epoch["session_id"] for epoch in [*result["history"], result]}) == 3


@linux
def test_recovery_refuses_an_alive_predecessor_including_same_pid(tmp_path):
    with adapter(tmp_path) as owner:
        owner.acquire()
        before = owner.receipt()
        with adapter(tmp_path) as contender:
            with pytest.raises(locks.AtomicLockError, match="still alive"):
                contender.recover()
        assert owner.receipt() == before
        owner.assert_owned()
        owner.release()


def acquired_then_released(root):
    with adapter(Path(root)) as current:
        current.acquire()
        current.release()
    os._exit(75)


@linux
def test_dead_owner_released_locks_recover_without_new_nonce(tmp_path):
    fixture(tmp_path)
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=acquired_then_released, args=(str(tmp_path),))
    process.start()
    process.join(30)
    assert process.exitcode == 75
    with adapter(tmp_path) as current:
        assert_recovered(current, process.pid, 2)


@linux
@pytest.mark.parametrize("change", ["owner", "foreign-content", "foreign-target", "receipt-next", "guard"])
def test_recovery_preserves_unknown_state_instead_of_reclaiming(tmp_path, change):
    paths, state, marker = fixture(tmp_path)
    run_crash(tmp_path, "published", "hmao")
    path = paths["hmao"]
    if change == "owner":
        (path / "owner.json").write_text("unknown owner")
    elif change == "foreign-content":
        (path / "extra").write_text("preserve")
    elif change == "foreign-target":
        paths["tyumen"].mkdir()
    elif change == "receipt-next":
        (state / NONCE / "receipt.next").write_text("unexplained partial write")
    else:
        guard = Path(str(path) + ".guard")
        guard.rename(guard.with_name("old.guard"))
        guard.touch(mode=0o600)
    before = (state / NONCE / "receipt.json").read_bytes()
    current_inode = path.stat().st_ino
    with adapter(tmp_path) as current:
        with pytest.raises(locks.AtomicLockError):
            current.recover()
    assert marker.exists()
    assert (state / NONCE / "receipt.json").read_bytes() == before
    assert path.stat().st_ino == current_inode
    if change == "foreign-content":
        assert (path / "extra").read_text() == "preserve"


def crash_receipt_write(root, boundary, number, recovery):
    seen = 0
    def checkpoint(event, region):
        nonlocal seen
        if event == boundary:
            seen += 1
            if seen == number:
                os._exit(74)
    with adapter(Path(root), checkpoint=checkpoint) as current:
        if recovery:
            current.recover()
        else:
            current.acquire()
            current.release()
    os._exit(93)  # Requested boundary was not reached.


def receipt_write_crash(root, boundary, number, *, recovery=False):
    context = multiprocessing.get_context('spawn')
    process = context.Process(target=crash_receipt_write, args=(str(root), boundary, number, recovery))
    process.start()
    process.join(30)
    if process.is_alive():
        process.kill()
        process.join(5)
    assert process.exitcode == 74
    return process.pid


@linux
@pytest.mark.parametrize('boundary', ['receipt_next', 'receipt_renamed'])
@pytest.mark.parametrize('number', range(1, 16))
def test_every_acquire_release_receipt_write_crash_is_recoverable(tmp_path, boundary, number):
    fixture(tmp_path)
    pid = receipt_write_crash(tmp_path, boundary, number)
    folder = tmp_path / 'state' / NONCE
    if boundary == 'receipt_next':
        assert (folder / 'receipt.next').is_file()
        if number == 1:
            assert not (folder / 'receipt.json').exists()
    with adapter(tmp_path) as current:
        result = assert_recovered(current, pid, 2)
        assert result['history'][0]['pid'] == pid
    assert not (folder / 'receipt.next').exists()


@linux
@pytest.mark.parametrize('boundary', ['receipt_next', 'receipt_renamed'])
@pytest.mark.parametrize('number', range(1, 15))
def test_every_recovery_receipt_write_crash_keeps_old_generations(tmp_path, boundary, number):
    fixture(tmp_path)
    first = run_crash(tmp_path, 'published', 'tyumen')
    second = receipt_write_crash(tmp_path, boundary, number, recovery=True)
    with adapter(tmp_path) as current:
        result = assert_recovered(current, second, 3)
        assert [epoch['pid'] for epoch in result['history']] == [first, second]


@linux
def test_crash_after_candidate_reconciliation_still_recovers_same_nonce(tmp_path):
    fixture(tmp_path)
    first = receipt_write_crash(tmp_path, 'receipt_next', 4)
    run_crash(tmp_path, 'receipt_reconciled', None, recovery=True)
    with adapter(tmp_path) as current:
        result = assert_recovered(current, first, 2)
        assert result['history'][0]['pid'] == first


def successor_fixture():
    """Pure transition fixture; filesystem/liveness are independently checked."""
    current = locks.AtomicRunLocks.__new__(locks.AtomicRunLocks)
    doc = {'schema_version': 2, 'generation': 1, 'history': [], 'nonce': NONCE,
           'strategy': 'linux-renameat2-noreplace/1', 'legacy_ps_locale': 'C-compatible',
           'pid': 100, 'process_start': 'recorded owner', 'boot_id': 'a' * 36,
           'session_id': 'b' * 32, 'phase': 'preparing',
           'guard_inodes': {region: [1, number + 10] for number, region in enumerate(locks.REGIONS)},
           'locks': {locks.REGIONS[0]: {'status': 'prepared', 'inode': [1, 100], 'owner_inode': [1, 101]}}}
    return current, doc


def test_complete_initial_candidate_does_not_require_existing_receipt():
    current, doc = successor_fixture()
    current._validate_successor(None, doc)
    doc['phase'] = 'held'
    with pytest.raises(locks.AtomicLockError, match='initial'):
        current._validate_successor(None, doc)


@pytest.mark.parametrize('change', ['nonce', 'guard', 'owner', 'session', 'generation',
                                   'new_inode', 'skip_region', 'skip_status', 'history'])
def test_unknown_complete_candidates_are_not_treated_as_successors(change):
    current, previous = successor_fixture()
    candidate = copy.deepcopy(previous)
    candidate['locks'][locks.REGIONS[0]]['status'] = 'installed'
    if change == 'nonce': candidate['nonce'] = 'c' * 32
    elif change == 'guard': candidate['guard_inodes']['hmao'] = [2, 34]
    elif change == 'owner': candidate['pid'] += 1
    elif change == 'session': candidate['session_id'] = 'c' * 32
    elif change == 'generation': candidate['generation'] = 3
    elif change == 'new_inode': candidate['locks']['hmao']['inode'] = [1, 99]
    elif change == 'skip_region': candidate['locks']['tyumen'] = {'status': 'prepared'}
    elif change == 'skip_status': candidate['locks']['hmao']['status'] = 'retired'
    elif change == 'history': candidate['history'] = [copy.deepcopy(previous)]
    with pytest.raises(locks.AtomicLockError):
        current._validate_successor(previous, candidate)


def test_next_generation_candidate_must_retain_exact_predecessor():
    current, previous = successor_fixture()
    candidate = copy.deepcopy(previous)
    candidate.update(generation=2, history=[current._epoch(previous)], phase='recovering',
                     locks={}, pid=200, process_start='new recorded owner', session_id='c' * 32)
    current._validate_successor(previous, candidate)
    candidate['history'][0]['locks']['hmao']['inode'] = [1, 99]
    with pytest.raises(locks.AtomicLockError, match='next-generation'):
        current._validate_successor(previous, candidate)


def test_historical_retirement_candidate_cannot_change_owner_or_inode():
    current, first = successor_fixture()
    previous = copy.deepcopy(first)
    previous.update(generation=2, history=[current._epoch(first)], phase='recovering',
                    locks={}, pid=200, process_start='new owner', session_id='c' * 32)
    candidate = copy.deepcopy(previous)
    candidate['history'][0]['locks']['hmao']['status'] = 'retired'
    current._validate_successor(previous, candidate)
    candidate['history'][0]['pid'] = 999
    with pytest.raises(locks.AtomicLockError, match='Historical identity'):
        current._validate_successor(previous, candidate)


def test_exact_durable_successors_cover_acquire_release_recovery_and_compensation():
    current, previous = successor_fixture()

    def advance(candidate):
        nonlocal previous
        current._validate_successor(previous, candidate)
        previous = copy.deepcopy(candidate)

    for number, region in enumerate(locks.REGIONS[1:], 1):
        candidate = copy.deepcopy(previous)
        candidate['locks'][region] = {'status': 'prepared', 'inode': [1, 100 + 2 * number],
                                     'owner_inode': [1, 101 + 2 * number]}
        advance(candidate)
    for region in locks.REGIONS:
        candidate = copy.deepcopy(previous)
        candidate['locks'][region]['status'] = 'installed'
        advance(candidate)
    advance(dict(copy.deepcopy(previous), phase='held'))
    held = copy.deepcopy(previous)
    advance(dict(copy.deepcopy(previous), phase='releasing'))
    for region in reversed(locks.REGIONS):
        candidate = copy.deepcopy(previous)
        candidate['locks'][region]['status'] = 'retired'
        advance(candidate)
    advance(dict(copy.deepcopy(previous), phase='released'))

    # Recovery starts with a new durable owner, retaining the complete earlier
    # generation, before retiring any visible predecessor directory.
    previous = held
    candidate = dict(copy.deepcopy(previous), generation=2, history=[current._epoch(previous)],
                     phase='recovering', locks={}, pid=200, process_start='new owner', session_id='c' * 32)
    advance(candidate)
    for region in reversed(locks.REGIONS):
        candidate = copy.deepcopy(previous)
        candidate['history'][0]['locks'][region]['status'] = 'retired'
        advance(candidate)
    for number, region in enumerate(locks.REGIONS):
        candidate = copy.deepcopy(previous)
        candidate['locks'][region] = {'status': 'prepared', 'inode': [1, 200 + 2 * number],
                                     'owner_inode': [1, 201 + 2 * number]}
        advance(candidate)
    for region in locks.REGIONS:
        candidate = copy.deepcopy(previous)
        candidate['locks'][region]['status'] = 'installed'
        advance(candidate)
    advance(dict(copy.deepcopy(previous), phase='held'))

    # Failed acquisition retires only the already published lock; unexposed
    # prepared directories remain as receipt-bound evidence.
    _, previous = successor_fixture()
    previous['locks']['hmao']['status'] = 'installed'
    previous['locks'][locks.REGIONS[1]] = {'status': 'prepared', 'inode': [1, 110], 'owner_inode': [1, 111]}
    advance(dict(copy.deepcopy(previous), phase='compensating'))
    candidate = copy.deepcopy(previous)
    candidate['locks']['hmao']['status'] = 'retired'
    advance(candidate)
    advance(dict(copy.deepcopy(previous), phase='acquire_failed'))
