#!/usr/bin/env python3
"""Atomic coordinator locks; source-only, no production CLI or foreign stale adoption.

Caller must close permanent service admission and drain legacy writers BEFORE
using this adapter. check_admission repeats that proof before every mutation.
New helpers cooperate via permanent guard flocks. Old helpers do not: arbitrary
manual/root mutation and old helpers running despite admission are outside this
contract. Their unsafe stale-reclaim behavior is not repaired by this module.

The full owner v1 appears atomically through Linux RENAME_NOREPLACE, never the
legacy mkdir-to-owner gap. Fresh acquire never reclaims ANY existing lock.
Explicit recover() reuses the SAME nonce only after proving the recorded owner
has died. Every old inode remains in durable generation history; old directories
are retired, never recursively deleted. Unknown state remains closed.
"""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
import copy
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
        self.history = []
        self.generation = 1
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
        record = {"schema_version": 2, "generation": self.generation, "history": self.history, "strategy": "linux-renameat2-noreplace/1", "nonce": self.nonce,
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
        self._step("receipt_next", None)
        os.replace("receipt.next", "receipt.json", src_dir_fd=self.folder_fd, dst_dir_fd=self.folder_fd)
        self._step("receipt_renamed", None)
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

    def _open(self, *, existing=False):
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
        if not existing:
            os.mkdir(self.nonce, 0o700, dir_fd=self.state_fd)
            _sync(self.state_fd)
        self.folder_fd = os.open(self.nonce, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.state_fd)
        folder_info = os.fstat(self.folder_fd)
        if folder_info.st_uid != os.geteuid() or stat.S_IMODE(folder_info.st_mode) != 0o700:
            raise AtomicLockError("Unsafe receipt directory")
        self.phase = "preparing"

    def _check_directory(self, region, parent, name):
        self._check_record_directory(self.records[region], parent, name)

    def _check_record_directory(self, record, parent, name):
        owner_bytes = (json.dumps(record["owner"], sort_keys=True) + "\n").encode()
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
                        or owner_info.st_size != len(owner_bytes)
                        or os.read(owner_fd, len(owner_bytes) + 1) != owner_bytes):
                    raise AtomicLockError("Lock owner changed")
            finally:
                os.close(owner_fd)
        finally:
            os.close(fd)

    def _name(self, kind, region):
        prefix = "" if self.generation == 1 else str(self.generation) + "-"
        return kind + "-" + prefix + region

    @staticmethod
    def _prove_dead(epoch, current_boot):
        if epoch["boot_id"] != current_boot:
            return  # A process from another kernel boot cannot still own locks.
        pid = epoch["pid"]
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except OSError as exc:
            raise AtomicLockError("Previous coordinator liveness is unknown") from exc
        try:
            started = process_start(pid)
        except (AtomicLockError, OSError, subprocess.SubprocessError) as exc:
            # Exit between kill(0) and ps is safe ONLY after ESRCH, never on an
            # empty/failed ps result alone (the old helper's unsafe behavior).
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            except OSError:
                pass
            raise AtomicLockError("Previous coordinator start identity is unknown") from exc
        if started == epoch["process_start"]:
            raise AtomicLockError("Previous coordinator is still alive")
        # PID exists with another OS start string: the recorded process is dead.

    def _read_document(self, name, *, optional=False):
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.folder_fd)
        except FileNotFoundError:
            if optional:
                return None, None
            raise
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or not 0 < info.st_size <= 1024 * 1024):
                raise AtomicLockError("Unsafe recovery receipt")
            raw = os.read(fd, info.st_size + 1)
            if len(raw) != info.st_size:
                raise AtomicLockError("Recovery receipt changed during read")
        finally:
            os.close(fd)
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise AtomicLockError("Duplicate recovery receipt field")
                value[key] = item
            return value
        try:
            doc = json.loads(raw, object_pairs_hook=unique)
        except (ValueError, UnicodeError) as exc:
            raise AtomicLockError("Invalid recovery receipt") from exc
        self._validate_receipt(doc)
        return doc, raw

    @staticmethod
    def _epoch(doc):
        return copy.deepcopy({key: doc.get(key, 1) for key in
                ("generation", "pid", "process_start", "boot_id", "session_id", "locks")})

    def _validate_successor(self, previous, candidate):
        """Recognize exactly one _save transition, never a general journal edit.

        The candidate's record/schema/route validation and inode/liveness proof
        are separate requirements. Unknown private staging remains an error.
        """
        if candidate["schema_version"] != 2:
            raise AtomicLockError("Recovery candidate is not a current receipt")
        if previous is None:
            if (candidate["generation"] != 1 or candidate["history"] or candidate["phase"] != "preparing"
                    or set(candidate["locks"]) != {REGIONS[0]}
                    or candidate["locks"][REGIONS[0]]["status"] != "prepared"):
                raise AtomicLockError("Unknown initial receipt candidate")
            return
        if any(previous[key] != candidate[key] for key in ("strategy", "nonce", "legacy_ps_locale", "guard_inodes")):
            raise AtomicLockError("Recovery candidate changed lease/guard identity")
        old_generation = previous.get("generation", 1)
        if candidate["generation"] == old_generation + 1:
            expected_history = [*previous.get("history", []), self._epoch(previous)]
            if (candidate["history"] != expected_history or candidate["phase"] != "recovering"
                    or candidate["locks"] or candidate["session_id"] == previous["session_id"]):
                raise AtomicLockError("Unknown next-generation receipt candidate")
            return
        if (candidate["generation"] != old_generation or previous["schema_version"] != 2
                or any(previous[key] != candidate[key] for key in
                       ("pid", "process_start", "boot_id", "session_id"))):
            raise AtomicLockError("Recovery candidate changed current owner")
        if candidate == previous:
            return  # A fully identical durable rewrite adds no new permission.
        old_locks, new_locks = previous["locks"], candidate["locks"]
        if candidate["history"] != previous["history"]:
            # Retiring a visible historical inode changes one status only.
            old = copy.deepcopy(previous["history"])
            changes = 0
            if (candidate["phase"] != previous["phase"] or candidate["phase"] != "recovering"
                    or old_locks != new_locks):
                raise AtomicLockError("Unknown historical receipt transition")
            for left, right in zip(old, candidate["history"]):
                if set(left["locks"]) != set(right["locks"]):
                    raise AtomicLockError("Historical receipt records changed")
                for region, record in left["locks"].items():
                    after = right["locks"][region]
                    if record != after:
                        if record["status"] not in ("prepared", "installed") or after["status"] != "retired":
                            raise AtomicLockError("Unknown historical retirement")
                        record["status"] = "retired"
                        changes += 1
            if changes != 1 or old != candidate["history"]:
                raise AtomicLockError("Historical identity changed")
            return
        if candidate["phase"] != previous["phase"]:
            phases = (previous["phase"], candidate["phase"])
            allowed = {("preparing", "held"), ("preparing", "compensating"),
                       ("compensating", "acquire_failed"), ("held", "releasing"),
                       ("releasing", "released"), ("recovering", "held")}
            if old_locks != new_locks or phases not in allowed:
                raise AtomicLockError("Unknown receipt phase transition")
            statuses = [record["status"] for record in new_locks.values()]
            if candidate["phase"] in ("held", "releasing") and (set(new_locks) != set(REGIONS) or set(statuses) != {"installed"}):
                raise AtomicLockError("Incomplete held receipt candidate")
            if candidate["phase"] == "released" and (set(new_locks) != set(REGIONS) or set(statuses) != {"retired"}):
                raise AtomicLockError("Incomplete released receipt candidate")
            if candidate["phase"] == "acquire_failed" and "installed" in statuses:
                raise AtomicLockError("Unreleased acquisition candidate")
            return
        if set(old_locks) != set(new_locks):
            next_region = REGIONS[len(old_locks)] if len(old_locks) < len(REGIONS) else None
            if (candidate["phase"] not in ("preparing", "recovering")
                    or set(old_locks) != set(REGIONS[:len(old_locks)])
                    or set(new_locks) != set(old_locks) | {next_region}
                    or any(new_locks[region] != record for region, record in old_locks.items())
                    or new_locks[next_region]["status"] != "prepared"):
                raise AtomicLockError("Unknown prepared receipt candidate")
            return
        updated = copy.deepcopy(old_locks)
        changes = 0
        for region, record in updated.items():
            after = new_locks[region]
            if record != after:
                transition = (record["status"], after["status"])
                allowed = ((candidate["phase"] in ("preparing", "recovering") and transition == ("prepared", "installed"))
                           or (candidate["phase"] in ("compensating", "releasing")
                               and transition in (("prepared", "retired"), ("installed", "retired"))))
                if not allowed:
                    raise AtomicLockError("Unknown visible lock receipt transition")
                record["status"] = after["status"]
                changes += 1
        if changes != 1 or updated != new_locks:
            raise AtomicLockError("Lock identity changed in receipt candidate")

    def _validate_receipt(self, doc):
        base = {"schema_version", "strategy", "nonce", "pid", "process_start", "boot_id", "session_id",
                "legacy_ps_locale", "phase", "locks", "guard_inodes"}
        if (not isinstance(doc, dict) or type(doc.get("schema_version")) is not int
                or doc["schema_version"] not in (1, 2)
                or set(doc) != base | ({"generation", "history"} if doc["schema_version"] == 2 else set())
                or doc["nonce"] != self.nonce or doc["strategy"] != "linux-renameat2-noreplace/1"
                or doc["legacy_ps_locale"] != "C-compatible"
                or doc["phase"] not in ("preparing", "held", "compensating", "acquire_failed", "releasing", "released", "recovering")):
            raise AtomicLockError("Unknown/foreign recovery receipt")
        history = doc.get("history", [])
        generation = doc.get("generation", 1)
        if (type(generation) is not int or not 1 <= generation <= 1000 or not isinstance(history, list)
                or len(history) != generation - 1):
            raise AtomicLockError("Broken recovery generation history")
        def inode(value):
            return isinstance(value, list) and len(value) == 2 and all(type(i) is int and i >= 0 for i in value)
        if (not isinstance(doc["guard_inodes"], dict) or set(doc["guard_inodes"]) != set(REGIONS)
                or not all(inode(value) for value in doc["guard_inodes"].values())):
            raise AtomicLockError("Unknown permanent guard identities")
        epoch_fields = {"generation", "pid", "process_start", "boot_id", "session_id", "locks"}
        current = {key: doc.get(key, 1) for key in epoch_fields}
        for number, epoch in enumerate([*history, current], 1):
            if (not isinstance(epoch, dict) or set(epoch) != epoch_fields or type(epoch["generation"]) is not int or epoch["generation"] != number
                    or type(epoch["pid"]) is not int or epoch["pid"] <= 0
                    or not isinstance(epoch["process_start"], str) or not 0 < len(epoch["process_start"]) <= 256
                    or not isinstance(epoch["boot_id"], str) or not re.fullmatch(r"[0-9a-f-]{36}", epoch["boot_id"])
                    or not isinstance(epoch["session_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", epoch["session_id"])
                    or not isinstance(epoch["locks"], dict) or not set(epoch["locks"]) <= set(REGIONS)):
                raise AtomicLockError("Invalid previous coordinator identity")
            prefix = "" if number == 1 else str(number) + "-"
            for region, record in epoch["locks"].items():
                if (not isinstance(record, dict) or set(record) != {"target", "inode", "owner", "owner_inode", "prepared", "retired", "parent_inode", "status"}
                        or record["target"] != str(self.paths[region])
                        or record["prepared"] != "prepared-" + prefix + region
                        or record["retired"] != "retired-" + prefix + region
                        or not isinstance(record["owner"], dict) or type(record["owner"].get("version")) is not int
                        or record["owner"] != {"version": 1, "pid": epoch["pid"], "process_start": epoch["process_start"]}
                        or not all(inode(record[key]) for key in ("inode", "owner_inode", "parent_inode"))
                        or record["parent_inode"] != list(self.parents[region][2])
                        or record["status"] not in ("prepared", "installed", "retired")):
                    raise AtomicLockError("Unknown historical lock identity/route")

    def _locations(self, epochs, *, candidate=False):
        visible = {}
        allowed_private = {"receipt.json", "receipt.next"} if candidate else {"receipt.json"}
        for epoch in epochs:
            for region, record in epoch["locks"].items():
                found = []
                for name in (record["prepared"], record["retired"]):
                    allowed_private.add(name)
                    try:
                        os.stat(name, dir_fd=self.folder_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    self._check_record_directory(record, self.folder_fd, name)
                    found.append((self.folder_fd, name))
                parent, name = self._parent(region), self.paths[region].name
                try:
                    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    info = None
                if info is not None and list(_identity(info)) == record["inode"]:
                    self._check_record_directory(record, parent, name)
                    if region in visible:
                        raise AtomicLockError("Duplicate historical visible lock")
                    visible[region] = record
                    found.append((parent, name))
                if len(found) != 1:
                    raise AtomicLockError("Historical lock missing/ambiguous; preserve recovery state")
        for region in REGIONS:
            try:
                os.stat(self.paths[region].name, dir_fd=self._parent(region), follow_symlinks=False)
            except FileNotFoundError:
                continue
            if region not in visible:
                raise AtomicLockError("Foreign lock blocks recovery")
        if set(os.listdir(self.folder_fd)) - allowed_private:
            raise AtomicLockError("Unexplained private recovery files; preserve for investigation")
        return visible

    def recover(self):
        """Reacquire four locks for this nonce after a PROVEN dead predecessor.

        No automatic call from acquire. New owner/generation is durable BEFORE
        retiring any old visible directory. Every old and new inode remains in
        the same receipt, making receipt/publish/retire crashes recoverable.
        A complete receipt.next is reconciled only as an exact valid successor;
        unexpected files or incomplete private writes fail closed, never cleaned.
        """
        self._admission()
        self._open(existing=True)
        previous, previous_raw = self._read_document("receipt.json", optional=True)
        candidate, candidate_raw = self._read_document("receipt.next", optional=True)
        if previous is None and candidate is None:
            raise AtomicLockError("No durable recovery receipt")
        self.receipt_bytes = previous_raw
        if candidate is not None:
            self._validate_successor(previous, candidate)
        selected = candidate if candidate is not None else previous
        self.guard_inodes = copy.deepcopy(selected["guard_inodes"])
        epochs = copy.deepcopy([*selected.get("history", []), self._epoch(selected)])
        with self._guards():
            self._admission()
            # Bind both exact byte strings while all permanent guard flocks are
            # held. A concurrent recovery cannot silently change our predecessor.
            if (self._read_document("receipt.json", optional=True)[1] != previous_raw
                    or self._read_document("receipt.next", optional=True)[1] != candidate_raw):
                raise AtomicLockError("Recovery receipt changed before reconciliation")
            for epoch in epochs:
                self._prove_dead(epoch, self.boot_id)
            self._locations(epochs, candidate=candidate is not None)
            if candidate is not None:
                self._state_identity()
                fd = os.open("receipt.next", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.folder_fd)
                try:
                    _sync(fd)
                finally:
                    os.close(fd)
                os.replace("receipt.next", "receipt.json", src_dir_fd=self.folder_fd, dst_dir_fd=self.folder_fd)
                _sync(self.folder_fd)
                self.receipt_bytes = candidate_raw
                self._step("receipt_reconciled", None)
            self.history = epochs
            self.generation = len(epochs) + 1
            if self.generation > 1000:
                raise AtomicLockError("Recovery history limit reached")
            self.records, self.acquired, self.phase = {}, [], "recovering"
            self._save()
            self._step("recovery_begin", None)
            visible = self._locations(self.history)
            for region in reversed(REGIONS):
                self._admission()
                record = visible.get(region)
                if record is None:
                    continue
                self._check_record_directory(record, self._parent(region), self.paths[region].name)
                rename_noreplace(self._parent(region), self.paths[region].name, self.folder_fd, record["retired"])
                _sync(self._parent(region))
                _sync(self.folder_fd)
                self._step("recovery_retired", region)
                self._check_record_directory(record, self.folder_fd, record["retired"])
                record["status"] = "retired"
                self._save()
            self._prepare_recovery_records()
            for region in REGIONS:
                self._admission()
                record = self.records[region]
                self._check_directory(region, self.folder_fd, record["prepared"])
                rename_noreplace(self.folder_fd, record["prepared"], self._parent(region), self.paths[region].name)
                self.acquired.append(region)
                _sync(self._parent(region))
                _sync(self.folder_fd)
                self._step("recovery_published", region)
                self._check_directory(region, self._parent(region), self.paths[region].name)
                record["status"] = "installed"
                self._save()
            self.phase = "held"
            self._save()
            self._assert_locked()
        return self.receipt()

    def _prepare_recovery_records(self):
        for region in REGIONS:
            self._admission()
            name = self._name("prepared", region)
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
                "owner": self.owner, "owner_inode": owner_inode, "prepared": name,
                "retired": self._name("retired", region), "parent_inode": list(self.parents[region][2]), "status": "prepared"}
            self._save()
            self._step("recovery_receipt", region)

    def acquire(self):
        self._admission()
        try:
            self._open()
            with self._guards():
                for region in REGIONS:
                    self._admission()
                    name = self._name("prepared", region)
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
                        "owner": self.owner, "owner_inode": owner_inode, "prepared": name, "retired": self._name("retired", region),
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
