#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SIGKILL-safe Mac wrapper lock."""

from __future__ import annotations

import importlib.util
import json
import os

import pytest


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOL = os.path.join(REPO, "ops", "mac-local-run", "run_lock.py")
SPEC = importlib.util.spec_from_file_location("run_lock", TOOL)
run_lock = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(run_lock)


@pytest.fixture(autouse=True)
def _stable_process_identity(monkeypatch):
    """Unit-тест не зависит от sandbox-доступа к macOS ``ps``.

    Сам механизм лока отдельно проверяет PID + start-token; здесь подменяем
    только ОС-источник этого токена, а не решение о живом владельце.
    """
    current = os.getpid()
    monkeypatch.setattr(
        run_lock, "_process_start",
        lambda pid: f"start:{pid}" if pid == current else "",
    )


def test_live_owner_blocks_second_slot(tmp_path):
    lock = str(tmp_path / ".run.lock")
    pid = os.getpid()
    assert run_lock.acquire(lock, pid) == 0
    assert run_lock.acquire(lock, pid) == 1
    assert run_lock.release(lock, pid) == 0
    assert not os.path.exists(lock)


def test_dead_owner_is_reclaimed(tmp_path):
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    (lock / run_lock.OWNER_FILE).write_text(
        json.dumps({
            "pid": 99999999,
            "process_start": "Mon Jan  1 00:00:00 1990",
        }),
        encoding="utf-8",
    )
    assert run_lock.acquire(str(lock), os.getpid()) == 0
    owner = json.loads((lock / run_lock.OWNER_FILE).read_text())
    assert owner["pid"] == os.getpid()
    assert run_lock.release(str(lock), os.getpid()) == 0


def test_reused_pid_with_different_start_is_reclaimed(tmp_path):
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    (lock / run_lock.OWNER_FILE).write_text(
        json.dumps({
            "pid": os.getpid(),
            "process_start": "start:previous-process",
        }),
        encoding="utf-8",
    )
    assert run_lock.acquire(str(lock), os.getpid()) == 0
    owner = json.loads((lock / run_lock.OWNER_FILE).read_text())
    assert owner["process_start"] == f"start:{os.getpid()}"
    assert run_lock.release(str(lock), os.getpid()) == 0


def test_crash_between_mkdir_and_owner_write_is_reclaimed(tmp_path):
    lock = tmp_path / ".run.lock"
    lock.mkdir()
    assert run_lock.acquire(str(lock), os.getpid()) == 0
    assert run_lock.release(str(lock), os.getpid()) == 0


def test_foreign_owner_cannot_release_lock(tmp_path):
    lock = str(tmp_path / ".run.lock")
    assert run_lock.acquire(lock, os.getpid()) == 0
    assert run_lock.release(lock, os.getpid() + 1) == 1
    assert os.path.isdir(lock)
    assert run_lock.release(lock, os.getpid()) == 0
