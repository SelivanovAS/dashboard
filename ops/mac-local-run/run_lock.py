#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Directory lock shared by Mac/Linux wrappers and release maintenance.

Every acquire/release/reclaim transition holds an adjacent ``.guard`` flock.
That regular file is permanent: unlinking it would create independent mutexes.
A valid legacy owner (PID + OS start string) remains compatible. A missing or
unreadable owner is *unknown*, never proof of a dead process: an older helper
may still be writing it. Such a lock returns an error requiring operator investigation (not a busy skip).

Bootstrap must fence new wrapper starts and drain helpers before replacing the
old implementation. A concurrently executing old helper does not use flock and
cannot participate in this protocol. Do not advertise mixed helpers as safe.
The lock's parent directory must be controlled by the same trusted operator.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import os
import stat
import subprocess
import sys
import tempfile
import uuid


OWNER_FILE = "owner.json"
GUARD_SUFFIX = ".guard"


class _Busy(Exception):
    pass


def _process_start(pid: int) -> str:
    proc = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _canonical_lock(lock: str) -> str:
    lock = os.path.abspath(lock)
    # /tmp and /var are normal symlink aliases on macOS. Canonicalize the
    # trusted parent so aliases share one guard; never follow the lock itself.
    parent = os.path.realpath(os.path.dirname(lock))
    os.makedirs(parent, exist_ok=True)
    return os.path.join(parent, os.path.basename(lock))


@contextmanager
def _guard(lock: str):
    path = lock + GUARD_SUFFIX
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        identity = os.fstat(fd)
        if not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1:
            raise RuntimeError("guard должен быть обычным файлом без hardlink")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise _Busy from exc
        current = os.stat(path, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise RuntimeError("guard заменён во время захвата")
        yield
    finally:
        os.close(fd)


def _directory_identity(lock: str):
    try:
        current = os.stat(lock, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISDIR(current.st_mode):
        raise RuntimeError("lock должен быть каталогом, не ссылкой")
    return current.st_dev, current.st_ino


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
                    "acquired_at": datetime.now().astimezone().isoformat(timespec="seconds"),
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
        fd = os.open(_owner_path(lock), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        current = os.fstat(fd)
        if not stat.S_ISREG(current.st_mode) or current.st_nlink != 1:
            raise RuntimeError("owner должен быть обычным файлом без hardlink")
        # A damaged/unexpected file is not evidence authorizing recovery.
        if current.st_size > 64 * 1024:
            return None
        with os.fdopen(fd, encoding="utf-8", closefd=False) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (UnicodeError, ValueError):
        return None
    finally:
        os.close(fd)


def _owner_identity(owner: dict | None):
    if not owner:
        return None
    pid = owner.get("pid")
    started = owner.get("process_start")
    if type(pid) is not int or pid <= 0 or not isinstance(started, str) or not started:
        return None
    if owner.get("version", 1) != 1:
        return None
    return pid, started


def _owner_state(owner: dict | None) -> str:
    identity = _owner_identity(owner)
    if identity is None:
        return "unknown"
    pid, saved_start = identity
    current_start = _process_start(pid)
    if current_start:
        return "alive" if current_start == saved_start else "dead"
    # ps can fail because it is sandboxed/unavailable. An empty output alone
    # must never authorize deleting the directory of a live parser.
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "dead"
    except (PermissionError, OSError):
        return "unknown"
    return "unknown"


def _discard_owned_directory(lock: str, identity, pid: int) -> None:
    if _directory_identity(lock) != identity:
        raise RuntimeError("lock заменён во время проверки")
    if set(os.listdir(lock)) != {OWNER_FILE}:
        raise RuntimeError("постороннее содержимое lock; требуется проверка оператора")
    retired = f"{lock}.stale.{pid}.{uuid.uuid4().hex}"
    # Remove the public directory atomically; do not expose a half-released
    # owner to a contender. Never recursively remove unexpected contents.
    os.rename(lock, retired)
    os.unlink(_owner_path(retired))
    os.rmdir(retired)


def acquire(lock: str, pid: int) -> int:
    lock = _canonical_lock(lock)
    if type(pid) is not int or pid <= 0:
        raise ValueError("bad pid")
    started = _process_start(pid)
    if not started:
        raise RuntimeError(f"не удалось прочитать start-time PID {pid}")
    try:
        with _guard(lock):
            identity = _directory_identity(lock)
            if identity is not None:
                state = _owner_state(_read_owner(lock))
                if state == "alive":
                    return 1
                if state == "unknown":
                    raise RuntimeError("неизвестный владелец lock; требуется проверка оператора")
                _discard_owned_directory(lock, identity, pid)
            os.mkdir(lock, 0o700)
            _write_owner(lock, pid, started)
            return 0
    except _Busy:
        return 1


def release(lock: str, pid: int) -> int:
    lock = _canonical_lock(lock)
    if type(pid) is not int or pid <= 0:
        raise ValueError("bad pid")
    try:
        with _guard(lock):
            identity = _directory_identity(lock)
            if identity is None:
                return 0
            owner = _owner_identity(_read_owner(lock))
            if owner is None:
                raise RuntimeError("неизвестный владелец lock; требуется проверка оператора")
            if owner != (pid, _process_start(pid)):
                return 1
            _discard_owned_directory(lock, identity, pid)
            return 0
    except _Busy:
        return 1


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
