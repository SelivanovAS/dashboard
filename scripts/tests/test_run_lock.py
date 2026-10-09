#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run-lock owner safety and real competing process regressions."""

from __future__ import annotations

import importlib.util
import json
import multiprocessing
import os
from pathlib import Path

import pytest


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOL = os.path.join(REPO, "ops", "mac-local-run", "run_lock.py")
SPEC = importlib.util.spec_from_file_location("run_lock", TOOL)
run_lock = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(run_lock)


@pytest.fixture(autouse=True)
def _stable_process_identity(monkeypatch):
    # Unit cases replace only the OS start-time source. Multiprocess cases use
    # actual live/dead processes and explicit handshakes, not sleep windows.
    current = os.getpid()
    monkeypatch.setattr(run_lock, "_process_start", lambda pid: f"start:{pid}" if pid == current else "")


def _owner(lock, **values):
    lock.mkdir()
    (lock / run_lock.OWNER_FILE).write_text(json.dumps(values), encoding="utf-8")


def test_live_owner_blocks_second_slot(tmp_path):
    lock = str(tmp_path / ".run.lock")
    pid = os.getpid()
    assert run_lock.acquire(lock, pid) == 0
    assert run_lock.acquire(lock, pid) == 1
    assert run_lock.release(lock, pid) == 0
    assert not os.path.exists(lock)


def test_dead_owner_is_reclaimed(tmp_path):
    lock = tmp_path / ".run.lock"
    _owner(lock, pid=99999999, process_start="Mon Jan  1 00:00:00 1990")
    assert run_lock.acquire(str(lock), os.getpid()) == 0
    owner = json.loads((lock / run_lock.OWNER_FILE).read_text())
    assert owner["pid"] == os.getpid()
    assert run_lock.release(str(lock), os.getpid()) == 0


def test_reused_pid_with_different_start_is_reclaimed(tmp_path):
    lock = tmp_path / ".run.lock"
    _owner(lock, pid=os.getpid(), process_start="start:previous-process")
    assert run_lock.acquire(str(lock), os.getpid()) == 0
    owner = json.loads((lock / run_lock.OWNER_FILE).read_text())
    assert owner["process_start"] == f"start:{os.getpid()}"
    assert run_lock.release(str(lock), os.getpid()) == 0


def test_empty_legacy_lock_requires_investigation_not_reclaim(tmp_path):
    # An old helper may be paused after mkdir. A crash is indistinguishable:
    # neither its age nor the absent owner permits another writer to enter.
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    inode = lock.stat().st_ino
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.acquire(str(lock), os.getpid())
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.release(str(lock), os.getpid())
    assert lock.stat().st_ino == inode
    assert not list(lock.iterdir())


@pytest.mark.parametrize("content", ["{}", "[]", "not json", '{"pid":0,"process_start":"x"}',
    '{"pid":true,"process_start":"x"}', '{"pid":"99999999","process_start":"x"}',
    '{"pid":99999999}', '{"pid":99999999,"process_start":"x","version":99}'])
def test_unknown_owner_is_never_reclaimed(tmp_path, content):
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    path = lock / run_lock.OWNER_FILE
    path.write_text(content)
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.acquire(str(lock), os.getpid())
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.release(str(lock), os.getpid())
    assert path.read_text() == content


def test_failed_ps_for_existing_process_does_not_authorize_reclaim(tmp_path, monkeypatch):
    lock = tmp_path / ".run.lock"
    # Real current PID is alive even when its start-time could not be read.
    _owner(lock, pid=os.getpid(), process_start="saved identity")
    before = (lock / run_lock.OWNER_FILE).read_bytes()
    fake_requester = 98765432
    monkeypatch.setattr(run_lock, "_process_start", lambda pid: "requester" if pid == fake_requester else "")
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.acquire(str(lock), fake_requester)
    assert (lock / run_lock.OWNER_FILE).read_bytes() == before


def test_permission_denied_process_probe_does_not_authorize_reclaim(tmp_path, monkeypatch):
    lock = tmp_path / ".run.lock"
    _owner(lock, pid=98765432, process_start="saved identity")
    def denied(pid, signal):
        raise PermissionError("not permitted")
    monkeypatch.setattr(run_lock.os, "kill", denied)
    with pytest.raises(RuntimeError, match="неизвестный владелец"):
        run_lock.acquire(str(lock), os.getpid())


def test_foreign_owner_cannot_release_lock(tmp_path):
    lock = str(tmp_path / ".run.lock")
    assert run_lock.acquire(lock, os.getpid()) == 0
    assert run_lock.release(lock, os.getpid() + 1) == 1
    assert os.path.isdir(lock)
    assert run_lock.release(lock, os.getpid()) == 0


def test_reused_pid_cannot_release_previous_owner(tmp_path):
    lock = tmp_path / ".run.lock"
    _owner(lock, pid=os.getpid(), process_start="previous identity")
    before = (lock / run_lock.OWNER_FILE).read_bytes()
    assert run_lock.release(str(lock), os.getpid()) == 1
    assert (lock / run_lock.OWNER_FILE).read_bytes() == before


def test_guard_inode_survives_repeated_acquire_and_release(tmp_path):
    lock = str(tmp_path / ".run.lock")
    assert run_lock.acquire(lock, os.getpid()) == 0
    guard = Path(lock + run_lock.GUARD_SUFFIX)
    inode = guard.stat().st_ino
    assert run_lock.release(lock, os.getpid()) == 0
    assert run_lock.release(lock, os.getpid()) == 0
    assert run_lock.acquire(lock, os.getpid()) == 0
    assert guard.stat().st_ino == inode
    assert run_lock.release(lock, os.getpid()) == 0
    assert guard.stat().st_ino == inode


@pytest.mark.parametrize("action", ["acquire", "release"])
@pytest.mark.parametrize("link_kind", ["lock", "owner", "guard"])
def test_symlinks_are_refused_without_touching_target(tmp_path, action, link_kind):
    lock = tmp_path / ".run.lock"
    target = tmp_path / "do-not-touch"
    target.mkdir()
    record = json.dumps({"pid": os.getpid(), "process_start": f"start:{os.getpid()}"})
    (target / "owner.json").write_text(record)
    if link_kind == "lock":
        lock.symlink_to(target, target_is_directory=True)
    elif link_kind == "owner":
        lock.mkdir()
        (lock / "owner.json").symlink_to(target / "owner.json")
    else:
        Path(str(lock) + run_lock.GUARD_SUFFIX).symlink_to(target / "owner.json")
    assert run_lock.main([action, str(lock), str(os.getpid())]) == 2
    assert (target / "owner.json").read_text() == record
    assert len(list(target.iterdir())) == 1


@pytest.mark.parametrize("kind", ["guard", "owner"])
def test_hardlinked_identity_files_are_refused(tmp_path, kind):
    lock = tmp_path / ".run.lock"
    original = tmp_path / "original"
    original.write_text(json.dumps({"pid": os.getpid(), "process_start": f"start:{os.getpid()}"}))
    if kind == "owner":
        lock.mkdir()
        path = lock / "owner.json"
    else:
        path = Path(str(lock) + run_lock.GUARD_SUFFIX)
    os.link(original, path)
    assert run_lock.main(["acquire", str(lock), str(os.getpid())]) == 2
    assert original.stat().st_nlink == 2


@pytest.mark.parametrize("operation", ["acquire", "release"])
def test_unexpected_files_in_lock_are_preserved(tmp_path, operation):
    lock = tmp_path / ".run.lock"
    pid = 99999999 if operation == "acquire" else os.getpid()
    _owner(lock, pid=pid, process_start=f"start:{pid}")
    extra = lock / "unrecognized-file"
    extra.write_text("preserve")
    before = (lock / "owner.json").read_bytes()
    with pytest.raises(RuntimeError, match="постороннее содержимое"):
        getattr(run_lock, operation)(str(lock), os.getpid())
    assert extra.read_text() == "preserve"
    assert (lock / "owner.json").read_bytes() == before


def _live_start_for_process_test(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return ""
    return f"live:{pid}"


def _process_contender(lock, start, results, finish, paused=None, resume=None):
    # spawn imports a fresh module: the parent's fixture does not patch this.
    run_lock._process_start = _live_start_for_process_test
    original_write = run_lock._write_owner
    if paused is not None:
        def pause_before_write(lock, pid, started):
            paused.set()
            if not resume.wait(15):
                raise RuntimeError("test handshake timed out")
            original_write(lock, pid, started)
        run_lock._write_owner = pause_before_write
    start.wait(15)
    rc = run_lock.acquire(lock, os.getpid())
    results.put((os.getpid(), rc))
    if not finish.wait(15):
        raise RuntimeError("test shutdown timed out")
    if rc == 0:
        if run_lock.release(lock, os.getpid()) != 0:
            raise RuntimeError("owner could not release lock")


def _join_processes(processes, finish):
    finish.set()
    for process in processes:
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(5)
        assert process.exitcode == 0


def test_real_processes_have_exactly_one_owner(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    lock = str(tmp_path / ".run.lock")
    barrier = ctx.Barrier(7)
    results, finish = ctx.Queue(), ctx.Event()
    processes = [ctx.Process(target=_process_contender, args=(lock, barrier, results, finish)) for _ in range(6)]
    for process in processes:
        process.start()
    try:
        barrier.wait(15)
        observed = [results.get(timeout=15) for _ in processes]
        winners = [pid for pid, rc in observed if rc == 0]
        assert len(winners) == 1, observed
        assert all(rc in (0, 1) for _, rc in observed)
        assert json.loads(Path(lock, "owner.json").read_text())["pid"] == winners[0]
    finally:
        _join_processes(processes, finish)
    assert not Path(lock).exists()


def test_contender_cannot_reclaim_owner_paused_between_mkdir_and_write(tmp_path, monkeypatch):
    monkeypatch.setattr(run_lock, "_process_start", _live_start_for_process_test)
    ctx = multiprocessing.get_context("spawn")
    lock = str(tmp_path / ".run.lock")
    barrier = ctx.Barrier(2)
    results, finish, paused, resume = ctx.Queue(), ctx.Event(), ctx.Event(), ctx.Event()
    process = ctx.Process(target=_process_contender, args=(lock, barrier, results, finish, paused, resume))
    process.start()
    try:
        barrier.wait(15)
        assert paused.wait(15)
        assert Path(lock).is_dir() and not Path(lock, "owner.json").exists()
        inode = Path(lock).stat().st_ino
        assert run_lock.acquire(lock, os.getpid()) == 1
        assert run_lock.release(lock, os.getpid()) == 1
        assert Path(lock).stat().st_ino == inode
        resume.set()
        assert results.get(timeout=15) == (process.pid, 0)
        assert run_lock.acquire(lock, os.getpid()) == 1
        assert json.loads(Path(lock, "owner.json").read_text())["pid"] == process.pid
    finally:
        resume.set()
        _join_processes([process], finish)
    assert not Path(lock).exists()


def test_process_crash_during_owner_write_leaves_unknown_lock_closed(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    lock = str(tmp_path / ".run.lock")
    barrier = ctx.Barrier(2)
    results, finish, paused, resume = ctx.Queue(), ctx.Event(), ctx.Event(), ctx.Event()
    process = ctx.Process(target=_process_contender, args=(lock, barrier, results, finish, paused, resume))
    process.start()
    try:
        barrier.wait(15)
        assert paused.wait(15)
        process.kill()
        process.join(5)
        assert not process.is_alive()
        # The kernel released flock, but missing identity remains unknown.
        with pytest.raises(RuntimeError, match="неизвестный владелец"):
            run_lock.acquire(lock, os.getpid())
        assert Path(lock).is_dir() and not Path(lock, "owner.json").exists()
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)


def test_real_dead_owner_is_reclaimed_after_process_exit(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    lock = str(tmp_path / ".run.lock")
    barrier = ctx.Barrier(2)
    results, finish = ctx.Queue(), ctx.Event()
    process = ctx.Process(target=_process_contender, args=(lock, barrier, results, finish))
    process.start()
    try:
        barrier.wait(15)
        assert results.get(timeout=15) == (process.pid, 0)
        process.kill()
        process.join(5)
        assert not process.is_alive()
        assert run_lock.acquire(lock, os.getpid()) == 0
        assert json.loads(Path(lock, "owner.json").read_text())["pid"] == os.getpid()
        assert run_lock.release(lock, os.getpid()) == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)


@pytest.mark.parametrize("owner_contents", [None, "{broken json"])
def test_cli_unknown_owner_reports_error_instead_of_quiet_skip(tmp_path, capsys, owner_contents):
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    if owner_contents is not None:
        (lock / "owner.json").write_text(owner_contents)
    inode = lock.stat().st_ino
    assert run_lock.main(["acquire", str(lock), str(os.getpid())]) == 2
    captured = capsys.readouterr()
    assert "неизвестный владелец lock" in captured.err
    assert "проверка оператора" in captured.err
    assert captured.out == ""
    assert lock.stat().st_ino == inode
    if owner_contents is not None:
        assert (lock / "owner.json").read_text() == owner_contents
    else:
        assert not list(lock.iterdir())


def test_cli_live_owner_remains_a_quiet_busy_skip(tmp_path, capsys):
    lock = str(tmp_path / ".run.lock")
    assert run_lock.main(["acquire", lock, str(os.getpid())]) == 0
    try:
        assert run_lock.main(["acquire", lock, str(os.getpid())]) == 1
        captured = capsys.readouterr()
        assert captured.out == captured.err == ""
    finally:
        assert run_lock.release(lock, os.getpid()) == 0
