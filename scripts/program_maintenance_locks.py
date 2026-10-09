#!/usr/bin/env python3
"""Atomic coordinator locks; source-only, no production CLI or stale adoption.

Caller must close permanent service admission and drain legacy writers BEFORE
using this adapter. check_admission repeats that proof before every mutation.
New helpers cooperate via permanent guard flocks. Old helpers do not: arbitrary
manual/root mutation and old helpers running despite admission are outside this
contract. Their unsafe stale-reclaim behavior is not repaired by this module.

The full owner v1 appears atomically through Linux RENAME_NOREPLACE, never the
legacy mkdir-to-owner gap. Fresh acquire never reclaims ANY existing lock.
Durable receipts support future explicit recovery, but this version deliberately
cannot adopt a previous/dead coordinator. After such a crash admission stays
closed; do not delete locks or create a new nonce to bypass that boundary.
"""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
import errno
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid

REGIONS = ("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen")


class AtomicLockError(RuntimeError):
    pass


def _sync(fd):
    os.fsync(fd)


def _identity(info):
    return (info.st_dev, info.st_ino)


def _no_links(path):
    if not path.is_absolute() or ".." in path.parts:
        raise AtomicLockError("A normalized absolute path is required")
    for item in (*reversed(path.parents), path):
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise AtomicLockError("Linked lock/receipt path")
        if item != path and not stat.S_ISDIR(info.st_mode):
            raise AtomicLockError("Non-directory ancestor")


def process_start(pid):
    """Use the exact legacy ps field, refusing a non-C ambient rendering.

    Bootstrap must also prove the supported legacy service environments use this
    same C-compatible field. A caller's environment cannot prove other launchers.
    """
    command = ["ps", "-p", str(pid), "-o", "lstart="]
    ambient = subprocess.run(command, capture_output=True, text=True, timeout=10)
    canonical = subprocess.run(command, capture_output=True, text=True, timeout=10,
                               env={**os.environ, "LC_ALL": "C"})
    value = canonical.stdout.strip()
    if ambient.returncode or canonical.returncode or not value or ambient.stdout.strip() != value:
        raise AtomicLockError("Cannot prove legacy-compatible process start/locale")
    return value


def rename_noreplace(source_fd, source, target_fd, target):
    if sys.platform != "linux":
        raise AtomicLockError("Linux renameat2 RENAME_NOREPLACE is required")
    try:
        method = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError as exc:
        raise AtomicLockError("renameat2 is unavailable; no rename fallback") from exc
    method.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    method.restype = ctypes.c_int
    if method(source_fd, os.fsencode(source), target_fd, os.fsencode(target), 1):
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise AtomicLockError("Existing lock/destination must not be replaced")
        raise AtomicLockError(f"RENAME_NOREPLACE failed ({errno.errorcode.get(code, code)}); no fallback")


class AtomicRunLocks:
    """Exactly four real clone locks with durable per-nonce inode receipts.

    state_dir is private, on the same filesystem, outside ALL four checkouts.
    No method reclaims stale/empty/foreign locks. On an ordinary acquisition
    conflict all already acquired own locks are removed. If identity/admission
    becomes unknown during compensation, the error preserves the remaining locks
    and receipt for explicit recovery; a partially acquired set is never READY.
    """
    def __init__(self, paths, state_dir, marker, nonce, *, check_admission, checkpoint=None):
        if set(paths) != set(REGIONS) or not re.fullmatch(r"[0-9a-f]{32}", nonce):
            raise AtomicLockError("Need four regional paths and an exact nonce")
        if not callable(check_admission):
            raise AtomicLockError("Admission/drain verification is mandatory")
        self.paths = {region: Path(paths[region]) for region in REGIONS}
        self.state = Path(state_dir)
        self.marker = Path(marker)
        self.nonce = nonce
        self.check_admission = check_admission
        self.checkpoint = checkpoint
        self.pid = os.getpid()
        self.started = process_start(self.pid)
        self.session = uuid.uuid4().hex
        self.parents = {}
        self.state_fd = None
        self.folder_fd = None
        self.records = {}
        self.guard_inodes = {}
        self.receipt_bytes = None
        self.acquired = []
        self.phase = "new"
        self.owner = {"version": 1, "pid": self.pid, "process_start": self.started}
        self.owner_bytes = (json.dumps(self.owner, sort_keys=True) + "\n").encode()

    def _admission(self):
        if os.getpid() != self.pid or process_start(self.pid) != self.started:
            raise AtomicLockError("Coordinator process identity changed")
        _no_links(self.marker)
        info = self.marker.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise AtomicLockError("Unsafe maintenance marker")
        if json.loads(self.marker.read_bytes()) != {"schema_version": 2, "nonce": self.nonce}:
            raise AtomicLockError("Marker belongs to another lease")
        self.check_admission()

    def _parent(self, region):
        path, fd, identity = self.parents[region]
        _no_links(path)
        if _identity(path.stat()) != identity or _identity(os.fstat(fd)) != identity:
            raise AtomicLockError("Clone lock parent was replaced")
        return fd

    def _state_identity(self):
        _no_links(self.state)
        if _identity(self.state.stat()) != _identity(os.fstat(self.state_fd)):
            raise AtomicLockError("Receipt parent was replaced")
        folder = self.state / self.nonce
        _no_links(folder)
        if _identity(folder.stat()) != _identity(os.fstat(self.folder_fd)):
            raise AtomicLockError("Receipt directory was replaced")

    def _save(self):
        self._state_identity()
        record = {"schema_version": 1, "strategy": "linux-renameat2-noreplace/1", "nonce": self.nonce,
            "pid": self.pid, "process_start": self.started, "boot_id": self.boot_id,
            "session_id": self.session, "legacy_ps_locale": "C-compatible", "phase": self.phase,
            "locks": self.records, "guard_inodes": self.guard_inodes}
        self._check_receipt()
        fd = os.open("receipt.next", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.folder_fd)
        try:
            raw = (json.dumps(record, sort_keys=True) + "\n").encode()
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(raw)
                stream.flush()
            _sync(fd)
        finally:
            os.close(fd)
        os.replace("receipt.next", "receipt.json", src_dir_fd=self.folder_fd, dst_dir_fd=self.folder_fd)
        _sync(self.folder_fd)
        self.receipt_bytes = raw

    def _check_receipt(self):
        if self.receipt_bytes is None:
            return
        fd = os.open("receipt.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.folder_fd)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size != len(self.receipt_bytes)
                    or os.read(fd, len(self.receipt_bytes) + 1) != self.receipt_bytes):
                raise AtomicLockError("Durable lock receipt changed")
        finally:
            os.close(fd)

    def _step(self, event, region):
        if self.checkpoint:
            self.checkpoint(event, region)

    @contextmanager
    def _guards(self):
        opened = []
        try:
            for region in REGIONS:
                parent = self._parent(region)
                name = self.paths[region].name + ".guard"
                fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
                opened.append((region, name, fd))
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
                    raise AtomicLockError("Unsafe permanent guard")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise AtomicLockError("Another lock transition owns the guard") from exc
                if _identity(info) != _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)):
                    raise AtomicLockError("Guard inode replaced")
                identity = list(_identity(info))
                if region in self.guard_inodes and self.guard_inodes[region] != identity:
                    raise AtomicLockError("Permanent guard inode changed")
                self.guard_inodes[region] = identity
                _sync(fd)
                _sync(parent)
            yield
            for region, name, fd in opened:
                if _identity(os.fstat(fd)) != _identity(os.stat(name, dir_fd=self._parent(region), follow_symlinks=False)):
                    raise AtomicLockError("Guard inode replaced during transition")
        finally:
            for _, _, fd in reversed(opened):
                os.close(fd)

    def _open(self):
        if self.phase != "new":
            raise AtomicLockError("Fresh acquire cannot reuse an existing receipt")
        if sys.platform != "linux":
            raise AtomicLockError("Atomic bootstrap requires Linux")
        self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if not re.fullmatch(r"[0-9a-f-]{36}", self.boot_id):
            raise AtomicLockError("Cannot read host boot identity")
        _no_links(self.state)
        info = self.state.stat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
            raise AtomicLockError("Private existing state directory required")
        self.state_fd = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for region, lock in self.paths.items():
            if lock.parts[-3:] != ("ops", "mac-local-run", ".run.lock"):
                raise AtomicLockError("Unknown regional lock route")
            checkout = lock.parents[2]
            if self.state == checkout or checkout in self.state.parents:
                raise AtomicLockError("Receipts must be outside every checkout")
            _no_links(lock.parent)
            fd = os.open(lock.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            identity = _identity(os.fstat(fd))
            self.parents[region] = (lock.parent, fd, identity)
            if identity[0] != info.st_dev:
                raise AtomicLockError("Locks and private staging must share a filesystem")
        if len({identity for _, _, identity in self.parents.values()}) != 4:
            raise AtomicLockError("Four distinct regional lock parents required")
        os.mkdir(self.nonce, 0o700, dir_fd=self.state_fd)
        _sync(self.state_fd)
        self.folder_fd = os.open(self.nonce, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.state_fd)
        self.phase = "preparing"

    def _check_directory(self, region, parent, name):
        record = self.records[region]
        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (not stat.S_ISDIR(info.st_mode) or list(_identity(info)) != record["inode"]
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise AtomicLockError("Lock inode/permissions changed")
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            if sorted(os.listdir(fd)) != ["owner.json"]:
                raise AtomicLockError("Foreign contents in lock; preserve for investigation")
            owner_fd = os.open("owner.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                owner_info = os.fstat(owner_fd)
                if (list(_identity(owner_info)) != record["owner_inode"]
                        or not stat.S_ISREG(owner_info.st_mode) or owner_info.st_nlink != 1
                        or owner_info.st_uid != os.geteuid() or stat.S_IMODE(owner_info.st_mode) != 0o600
                        or owner_info.st_size != len(self.owner_bytes)
                        or os.read(owner_fd, len(self.owner_bytes) + 1) != self.owner_bytes):
                    raise AtomicLockError("Lock owner changed")
            finally:
                os.close(owner_fd)
        finally:
            os.close(fd)

    def acquire(self):
        self._admission()
        try:
            self._open()
            with self._guards():
                for region in REGIONS:
                    self._admission()
                    name = "prepared-" + region
                    os.mkdir(name, 0o700, dir_fd=self.folder_fd)
                    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.folder_fd)
                    try:
                        owner_fd = os.open("owner.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
                        try:
                            with os.fdopen(owner_fd, "wb", closefd=False) as stream:
                                stream.write(self.owner_bytes)
                                stream.flush()
                            _sync(owner_fd)
                            owner_inode = list(_identity(os.fstat(owner_fd)))
                        finally:
                            os.close(owner_fd)
                        _sync(fd)
                        inode = list(_identity(os.fstat(fd)))
                    finally:
                        os.close(fd)
                    _sync(self.folder_fd)
                    self.records[region] = {"target": str(self.paths[region]), "inode": inode,
                        "owner": self.owner, "owner_inode": owner_inode, "prepared": name, "retired": "retired-" + region,
                        "parent_inode": list(self.parents[region][2]), "status": "prepared"}
                    self._save()  # Durable inode/owner receipt BEFORE the visible name.
                    self._step("receipt", region)
                try:
                    for region in REGIONS:
                        self._admission()
                        record = self.records[region]
                        self._check_directory(region, self.folder_fd, record["prepared"])
                        rename_noreplace(self.folder_fd, record["prepared"], self._parent(region), self.paths[region].name)
                        self.acquired.append(region)
                        _sync(self._parent(region))
                        _sync(self.folder_fd)
                        self._step("published", region)
                        self._check_directory(region, self._parent(region), self.paths[region].name)
                        record["status"] = "installed"
                        self._save()
                except BaseException:
                    self.phase = "compensating"
                    self._save()
                    self._release_locked()
                    self.phase = "acquire_failed"
                    self._save()
                    raise
                self.phase = "held"
                self._save()
                self._assert_locked()
            return self.receipt()
        except BaseException:
            # Preserved private staging/receipts are intentional crash evidence.
            # Unknown ownership/admission must never trigger blind cleanup.
            raise

    def _assert_locked(self):
        self._admission()
        self._state_identity()
        self._check_receipt()
        if self.phase != "held" or self.acquired != list(REGIONS):
            raise AtomicLockError("Not the complete four-lock set")
        for region in REGIONS:
            self._check_directory(region, self._parent(region), self.paths[region].name)

    def assert_owned(self):
        with self._guards():
            self._assert_locked()

    def _release_locked(self):
        # Validate every owned directory BEFORE releasing the first one.
        self._admission()
        for region in self.acquired:
            self._check_directory(region, self._parent(region), self.paths[region].name)
        for region in reversed(self.acquired.copy()):
            self._admission()
            record = self.records[region]
            self._check_directory(region, self._parent(region), self.paths[region].name)
            rename_noreplace(self._parent(region), self.paths[region].name, self.folder_fd, record["retired"])
            _sync(self._parent(region))
            _sync(self.folder_fd)
            self._step("retired", region)
            self._check_directory(region, self.folder_fd, record["retired"])
            record["status"] = "retired"
            self.acquired.remove(region)
            self._save()
            # Retain the tiny owned directory as evidence. It is outside every
            # checkout and cannot block any ordinary run; no recursive deletion.

    def release(self):
        with self._guards():
            self._assert_locked()
            self.phase = "releasing"
            self._save()
            self._release_locked()
            self.phase = "released"
            self._save()
        return self.receipt()

    def receipt(self):
        self._state_identity()
        self._check_receipt()
        return json.loads(self.receipt_bytes)

    def close(self):
        """Close only local handles; never implicitly release ownership/admission."""
        for _, fd, _ in self.parents.values():
            os.close(fd)
        self.parents = {}
        if self.folder_fd is not None:
            os.close(self.folder_fd)
            self.folder_fd = None
        if self.state_fd is not None:
            os.close(self.state_fd)
            self.state_fd = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
