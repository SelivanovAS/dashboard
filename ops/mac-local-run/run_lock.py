#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recoverable directory lock for the Mac parser wrapper.

A plain ``mkdir`` lock survives SIGKILL and power loss forever.  The owner
record stores both PID and the OS process start string, so a later process
reusing the same PID is not mistaken for the old parser.  Reclaim is an atomic
rename of the stale directory before a new owner is created.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime


OWNER_FILE = "owner.json"


def _process_start(pid: int) -> str:
    proc = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _owner_path(lock: str) -> str:
    return os.path.join(lock, OWNER_FILE)


def _write_owner(lock: str, pid: int, started: str) -> None:
    path = _owner_path(lock)
    fd, tmp = tempfile.mkstemp(prefix=".owner.", suffix=".tmp", dir=lock)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "pid": pid,
                    "process_start": started,
                    "acquired_at": datetime.now().astimezone().isoformat(
                        timespec="seconds"
                    ),
                },
                f,
                ensure_ascii=False,
            )
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = ""
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass


def _read_owner(lock: str) -> dict | None:
    try:
        with open(_owner_path(lock), encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _owner_alive(owner: dict | None) -> bool:
    if not owner:
        return False
    try:
        pid = int(owner.get("pid"))
    except (TypeError, ValueError):
        return False
    saved_start = str(owner.get("process_start") or "")
    return bool(saved_start and _process_start(pid) == saved_start)


def acquire(lock: str, pid: int) -> int:
    lock = os.path.abspath(lock)
    parent = os.path.dirname(lock) or "."
    os.makedirs(parent, exist_ok=True)
    started = _process_start(pid)
    if not started:
        raise RuntimeError(f"не удалось прочитать start-time PID {pid}")

    try:
        os.mkdir(lock, 0o700)
    except FileExistsError:
        if _owner_alive(_read_owner(lock)):
            return 1
        stale = f"{lock}.stale.{pid}.{uuid.uuid4().hex}"
        try:
            os.rename(lock, stale)
        except FileNotFoundError:
            # The previous owner or another reclaimer won the race.
            return acquire(lock, pid)
        try:
            os.mkdir(lock, 0o700)
        except FileExistsError:
            shutil.rmtree(stale, ignore_errors=True)
            return 1
        shutil.rmtree(stale, ignore_errors=True)
    _write_owner(lock, pid, started)
    return 0


def release(lock: str, pid: int) -> int:
    lock = os.path.abspath(lock)
    owner = _read_owner(lock)
    if owner is None:
        return 0 if not os.path.exists(lock) else 1
    try:
        owner_pid = int(owner.get("pid"))
    except (TypeError, ValueError):
        return 1
    if owner_pid != pid:
        return 1
    try:
        os.unlink(_owner_path(lock))
        os.rmdir(lock)
    except FileNotFoundError:
        return 0
    return 0


def main(argv: list[str]) -> int:
    try:
        if len(argv) != 3 or argv[0] not in ("acquire", "release"):
            raise ValueError("usage")
        pid = int(argv[2])
        if pid <= 0:
            raise ValueError("bad pid")
        return acquire(argv[1], pid) if argv[0] == "acquire" else release(argv[1], pid)
    except (OSError, RuntimeError, ValueError) as exc:
        if str(exc) == "usage":
            print("usage: run_lock.py acquire|release LOCK PID", file=sys.stderr)
        else:
            print(f"run_lock: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
