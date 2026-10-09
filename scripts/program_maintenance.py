#!/usr/bin/env python3
"""Долговечный журнал окна выпуска. Сам по себе не разрешает production-деплой.

Host adapter MUST implement every hook. In particular verify_safe must inspect ALL
issued intents: a remote ref which is merely an ancestor of an issued target does
not fence a late push. EOF, expiry and loss of the original process prove nothing.
The adapter must audit permanent service guards, drain jobs/cgroups, and hold all
four clone locks. Hooks must never stop/start timers or kill existing workers.
This module does not execute Git, SSH, systemd or any delivery operation.
"""
from __future__ import annotations

import copy
import errno
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Protocol

SCHEMA_VERSION = 2
PROTOCOL = "court-program-maintenance/2"
REGIONS = ("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen")
PHASES = ("reserved", "ready", "recovery_ready", "publish_intent", "applying", "verified", "safe_to_resume", "complete")
MAX_DOCUMENT_BYTES = 1024 * 1024


class MaintenanceError(RuntimeError):
    """Неоднозначное состояние остаётся закрытым до проверки оператором."""


class MaintenanceHooks(Protocol):
    """No defaults: omitted verification is a programming error, never approval.

    acquire_run_locks is all-or-nothing; on failure it MUST release any partial
    acquisition itself. release_run_locks is called only after acquisition has
    returned successfully. It must prove release or raise; no blind second retry.
    snapshot returns four region records: head, release_id, managed_sha256 and
    inventory_sha256. The last digest binds a durable external inventory of
    protected/unmanaged files; the adapter must retain it across recovery.
    validate_snapshot proves the current trees equal that snapshot (data rules
    belong to the host adapter). assert_run_locks proves live ownership.
    validate_target proves the current target's manifest and the original
    protected inventories, including known partial writes on recovery. It must
    reject unknown managed/unmanaged differences before any publish permission.
    verify_installed checks hashes, modes, source, effective region and protected
    data; returns target_id, manifest_sha256, installed_sha, release_id,
    evidence_sha256 for the CURRENT target, including an emergency rollback.
    verify_safe checks the entire journal, all attempted target/base pairs, remote
    ref fences, queued jobs and installed integrity; returns covered_attempt_ids
    and evidence_sha256. With no intents it proves the baseline is still safe.
    The digest is audit evidence, not a replacement for those live checks.
    """
    def check_guard(self, marker: Path) -> None: ...
    def drain(self) -> None: ...
    def acquire_run_locks(self, nonce: str) -> None: ...
    def assert_run_locks(self, nonce: str) -> None: ...
    def release_run_locks(self, nonce: str) -> None: ...
    def snapshot(self) -> dict: ...
    def validate_snapshot(self, snapshot: dict) -> None: ...
    def validate_target(self, journal: dict) -> None: ...
    def apply_target(self, journal: dict) -> None: ...
    def verify_installed(self, journal: dict) -> dict: ...
    def verify_safe(self, journal: dict) -> dict: ...


def _hex(value, length, label):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{%d}" % length, value) is None:
        raise MaintenanceError(f"Некорректный {label}")
    return value


def _sha(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", value) is None:
        raise MaintenanceError("Нужен полный SHA коммита")
    return value


def _nonce(value):
    return _hex(value, 32, "nonce")


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise MaintenanceError("Повторный ключ JSON")
        result[key] = value
    return result


def _bytes(document):
    try:
        result = (json.dumps(document, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False) + "\n").encode()
    except (TypeError, ValueError) as exc:
        raise MaintenanceError("Документ не является корректным JSON") from exc
    if len(result) > MAX_DOCUMENT_BYTES:
        raise MaintenanceError("Слишком большой журнал")
    return result


def _snapshot(document):
    if not isinstance(document, dict) or set(document) != set(REGIONS):
        raise MaintenanceError("Нужен снимок четырёх территорий")
    for record in document.values():
        if not isinstance(record, dict) or set(record) != {"head", "release_id", "managed_sha256", "inventory_sha256"}:
            raise MaintenanceError("Некорректный снимок территории")
        _sha(record["head"])
        if record["release_id"] is not None:
            _hex(record["release_id"], 64, "release_id")
        _hex(record["managed_sha256"], 64, "managed_sha256")
        _hex(record["inventory_sha256"], 64, "inventory_sha256")
    return copy.deepcopy(document)


def _target(document, index):
    if (not isinstance(document, dict) or set(document) != {
            "target_id", "release_id", "source_commit", "manifest_sha256", "reason"}
            or type(document["target_id"]) is not int or document["target_id"] != index):
        raise MaintenanceError("Нарушен порядок целей установки")
    _hex(document["release_id"], 64, "release_id")
    _sha(document["source_commit"])
    _hex(document["manifest_sha256"], 64, "manifest_sha256")
    if document["reason"] not in (("release",) if index == 1 else ("rollback", "repair")):
        raise MaintenanceError("Неизвестное основание цели установки")
    return document


def _journal(document):
    required = {"schema_version", "protocol", "nonce", "region", "release_id", "source_commit",
                "manifest_sha256", "targets", "active_target_id",
                "phase", "revision", "owner", "snapshot", "attempts", "active_attempt_id",
                "verification", "safe_evidence", "outcome"}
    if not isinstance(document, dict) or set(document) != required:
        raise MaintenanceError("Неизвестный формат журнала")
    if (type(document["schema_version"]) is not int or document["schema_version"] != SCHEMA_VERSION
            or document["protocol"] != PROTOCOL):
        raise MaintenanceError("Несовместимый протокол окна")
    _nonce(document["nonce"])
    if document["region"] not in REGIONS or document["phase"] not in PHASES:
        raise MaintenanceError("Неизвестная территория или фаза")
    _hex(document["release_id"], 64, "release_id")
    _sha(document["source_commit"])
    _hex(document["manifest_sha256"], 64, "manifest_sha256")
    targets = document["targets"]
    if not isinstance(targets, list) or not targets:
        raise MaintenanceError("Потеряны цели установки")
    for index, target in enumerate(targets, 1):
        _target(target, index)
    if (type(document["active_target_id"]) is not int
            or document["active_target_id"] != len(targets)):
        raise MaintenanceError("Некорректная активная цель")
    current = targets[-1]
    if any(document[key] != current[key] for key in ("release_id", "source_commit", "manifest_sha256")):
        raise MaintenanceError("Активная цель не соответствует паспорту окна")
    if type(document["revision"]) is not int or document["revision"] < 1:
        raise MaintenanceError("Некорректная ревизия журнала")
    owner = document["owner"]
    if (not isinstance(owner, dict) or set(owner) != {"pid", "session_id", "boot_id"}
            or type(owner["pid"]) is not int or owner["pid"] <= 0):
        raise MaintenanceError("Некорректный владелец журнала")
    _nonce(owner["session_id"])
    if owner["boot_id"] is not None and (not isinstance(owner["boot_id"], str)
            or re.fullmatch(r"[a-zA-Z0-9-]{1,80}", owner["boot_id"]) is None):
        raise MaintenanceError("Некорректный boot_id")
    if document["snapshot"] is not None:
        _snapshot(document["snapshot"])
    if not isinstance(document["attempts"], list):
        raise MaintenanceError("Некорректные намерения публикации")
    previous_target = 1
    for index, attempt in enumerate(document["attempts"], 1):
        if (not isinstance(attempt, dict) or set(attempt) != {"attempt_id", "target_id", "base_sha", "target_sha"}
                or type(attempt["attempt_id"]) is not int or attempt["attempt_id"] != index):
            raise MaintenanceError("Нарушен порядок намерений публикации")
        if (type(attempt["target_id"]) is not int
                or not previous_target <= attempt["target_id"] <= len(targets)
                or (index == 1 and attempt["target_id"] != 1)):
            raise MaintenanceError("Намерение относится к неизвестной или прежней цели")
        previous_target = attempt["target_id"]
        _sha(attempt["base_sha"])
        _sha(attempt["target_sha"])
        if len(attempt["base_sha"]) != len(attempt["target_sha"]):
            raise MaintenanceError("Разные форматы SHA в намерении")
        if attempt["base_sha"] == attempt["target_sha"]:
            raise MaintenanceError("Публикация не меняет коммит")
    active = document["active_attempt_id"]
    if active is not None and (type(active) is not int or not document["attempts"]
                              or active != len(document["attempts"])):
        raise MaintenanceError("Некорректное активное намерение")
    if active is not None and document["attempts"][-1]["target_id"] != current["target_id"]:
        raise MaintenanceError("Активное намерение относится к прежней цели")
    phase = document["phase"]
    if phase in ("reserved", "ready") and (document["attempts"] or active is not None):
        raise MaintenanceError("Намерение предшествует разрешённой фазе")
    if phase in ("reserved", "ready") and len(targets) != 1:
        raise MaintenanceError("Цель восстановления предшествует разрешению публикации")
    if phase == "recovery_ready" and (len(targets) < 2 or not document["attempts"] or active is not None):
        raise MaintenanceError("Некорректное состояние цели восстановления")
    if (phase == "recovery_ready"
            and document["attempts"][-1]["target_id"] >= current["target_id"]):
        raise MaintenanceError("Восстановление содержит уже разрешённое активное намерение")
    if len(targets) > 1 and not document["attempts"]:
        raise MaintenanceError("Восстановление не содержит прежних намерений")
    if phase != "reserved" and document["snapshot"] is None:
        # A reservation may be cancelled before it obtains a baseline snapshot.
        if not (phase == "complete" and document["outcome"] == "cancelled"):
            raise MaintenanceError("Потерян исходный снимок")
    if phase in ("publish_intent", "applying", "verified", "safe_to_resume") and (not document["attempts"] or active is None):
        raise MaintenanceError("Потеряно намерение публикации")
    if document["outcome"] not in (None, "installed", "cancelled"):
        raise MaintenanceError("Неизвестный исход окна")
    if phase == "complete" and document["outcome"] is None:
        raise MaintenanceError("Неизвестный исход завершённого окна")
    if phase != "complete" and document["outcome"] is not None:
        raise MaintenanceError("Исход записан до завершения")
    if document["outcome"] == "installed" and (not document["attempts"] or active is None):
        raise MaintenanceError("Потеряно намерение завершённой установки")
    if document["outcome"] == "cancelled" and document["attempts"]:
        raise MaintenanceError("Отмена после разрешения публикации запрещена")
    verified = phase in ("verified", "safe_to_resume") or document["outcome"] == "installed"
    if verified:
        _verification(document["verification"], current)
    elif document["verification"] is not None:
        raise MaintenanceError("Проверка записана до установки")
    if phase == "safe_to_resume" or phase == "complete":
        _safe_evidence(document["safe_evidence"], document["attempts"])
    elif document["safe_evidence"] is not None:
        raise MaintenanceError("Разрешение запуска записано раньше проверки")
    _bytes(document)
    return document


def _verification(document, target):
    if not isinstance(document, dict) or set(document) != {"target_id", "manifest_sha256", "installed_sha", "release_id", "evidence_sha256"}:
        raise MaintenanceError("Нет полной проверки установленного пакета")
    _sha(document["installed_sha"])
    if (document["release_id"] != target["release_id"]
            or type(document["target_id"]) is not int or document["target_id"] != target["target_id"]
            or document["manifest_sha256"] != target["manifest_sha256"]):
        raise MaintenanceError("Проверен другой пакет")
    _hex(document["evidence_sha256"], 64, "evidence_sha256")
    return copy.deepcopy(document)


def _safe_evidence(document, attempts):
    if (not isinstance(document, dict) or set(document) != {"covered_attempt_ids", "evidence_sha256"}
            or document["covered_attempt_ids"] != [a["attempt_id"] for a in attempts]
            or any(type(a) is not int for a in document["covered_attempt_ids"])):
        raise MaintenanceError("Не проверены все разрешённые попытки публикации")
    _hex(document["evidence_sha256"], 64, "evidence_sha256")
    return copy.deepcopy(document)


class MaintenanceLease:
    """One live coordinator; context exit releases locks, NEVER removes marker.

    state_dir must have an existing parent and is created 0700. All ancestors
    must be real directories. Its lock inode persists forever. No TTL exists.
    A dead coordinator is recovered explicitly with recover(nonce); it never
    gains an automatic right to clear an ambiguous post-intent marker.
    """
    def __init__(self, state_dir, hooks: MaintenanceHooks, *, boot_id=None,
                 allow_verified_pending_jobs=False):
        self.path = Path(state_dir)
        if not self.path.is_absolute() or ".." in self.path.parts:
            raise MaintenanceError("Нужен абсолютный путь каталога окна")
        self.marker = self.path / "blocked"
        self.hooks = hooks
        for name in MaintenanceHooks.__dict__:
            if not name.startswith("_") and not callable(getattr(hooks, name, None)):
                raise MaintenanceError(f"Не реализована обязательная проверка {name}")
        if type(allow_verified_pending_jobs) is not bool:
            raise MaintenanceError("Режим проверки выхода должен быть явным bool")
        self.allow_verified_pending_jobs = allow_verified_pending_jobs
        if allow_verified_pending_jobs and not callable(getattr(hooks, "check_exit", None)):
            raise MaintenanceError("Для обычных ожидающих jobs нужна проверка check_exit")
        self.owner = {"pid": os.getpid(), "session_id": secrets.token_hex(16), "boot_id": boot_id}
        self.dirfd = self.lockfd = None
        self.held_nonce = None

    def __enter__(self):
        if self.dirfd is not None:
            raise MaintenanceError("Окно уже открыто этим объектом")
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for index, part in enumerate(self.path.parts[1:]):
                if index == len(self.path.parts) - 2:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                nextfd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = nextfd
            state = os.fstat(fd)
            if state.st_uid != os.geteuid() or stat.S_IMODE(state.st_mode) != 0o700:
                raise MaintenanceError("Каталог окна должен принадлежать исполнителю и иметь права 0700")
            self.dirfd = fd
            fd = None
            self.lockfd = os.open("coordinator.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.dirfd)
            self._check_file(self.lockfd)
            os.fsync(self.lockfd)
            os.fsync(self.dirfd)
            try:
                fcntl.flock(self.lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN):
                    raise MaintenanceError("Другое окно уже удерживает глобальный замок") from exc
                raise
            self._identity()
            return self
        except BaseException:
            if fd is not None:
                os.close(fd)
            self._close()
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            self._release_locks()
        finally:
            self._close()

    def _close(self):
        for attr in ("lockfd", "dirfd"):
            fd = getattr(self, attr)
            if fd is not None:
                os.close(fd)
                setattr(self, attr, None)

    def _check_file(self, fd):
        state = os.fstat(fd)
        if (not stat.S_ISREG(state.st_mode) or state.st_nlink != 1
                or state.st_uid != os.geteuid() or stat.S_IMODE(state.st_mode) != 0o600):
            raise MaintenanceError("Небезопасный файл состояния: нужны обычный файл, один link и права 0600")

    def _identity(self):
        if self.dirfd is None or self.lockfd is None:
            raise MaintenanceError("Глобальный замок не удерживается")
        path_state = self.path.lstat()
        held = os.fstat(self.dirfd)
        if (not stat.S_ISDIR(path_state.st_mode) or path_state.st_uid != os.geteuid()
                or stat.S_IMODE(path_state.st_mode) != 0o700
                or (path_state.st_dev, path_state.st_ino) != (held.st_dev, held.st_ino)):
            raise MaintenanceError("Каталог окна был заменён")
        current = os.stat("coordinator.lock", dir_fd=self.dirfd, follow_symlinks=False)
        locked = os.fstat(self.lockfd)
        self._check_file(self.lockfd)
        if (current.st_dev, current.st_ino) != (locked.st_dev, locked.st_ino):
            raise MaintenanceError("Inode глобального замка был заменён")

    def _read(self, name):
        self._identity()
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.dirfd)
        except FileNotFoundError:
            return None
        try:
            self._check_file(fd)
            if os.fstat(fd).st_size > MAX_DOCUMENT_BYTES:
                raise MaintenanceError("Слишком большой файл состояния")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                return json.loads(stream.read(MAX_DOCUMENT_BYTES + 1), object_pairs_hook=_unique_pairs)
        except (ValueError, UnicodeError) as exc:
            raise MaintenanceError("Повреждённый JSON состояния") from exc
        finally:
            os.close(fd)

    def _write(self, name, document):
        self._identity()
        self._read(name)  # Refuse symlink, hardlink, corrupt or foreign destination.
        content = _bytes(document)
        temporary = f".tmp-{secrets.token_hex(16)}"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.dirfd)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(content)
                stream.flush()
                os.fsync(fd)
            os.replace(temporary, name, src_dir_fd=self.dirfd, dst_dir_fd=self.dirfd)
            os.fsync(self.dirfd)
        finally:
            os.close(fd)
            try:
                os.unlink(temporary, dir_fd=self.dirfd)
            except FileNotFoundError:
                pass

    def _save(self, document):
        document = copy.deepcopy(document)
        document["revision"] += 1
        self._write("journal.json", _journal(document))
        return copy.deepcopy(document)

    def inspect(self):
        """Read under flock. Unknown state raises; never attempts cleanup."""
        marker = self._read("blocked")
        journal = self._read("journal.json")
        if journal is not None:
            _journal(journal)
        if marker is not None:
            if not isinstance(marker, dict) or set(marker) != {"schema_version", "nonce"} or type(marker["schema_version"]) is not int or marker["schema_version"] != SCHEMA_VERSION:
                raise MaintenanceError("Повреждённый marker окна")
            _nonce(marker["nonce"])
            if journal is None or marker["nonce"] != journal["nonce"]:
                raise MaintenanceError("Marker не соответствует журналу; защита сохранена")
        elif journal is not None and journal["phase"] != "complete":
            raise MaintenanceError("Защита незавершённого окна отсутствует")
        return {"blocked": marker is not None, "journal": copy.deepcopy(journal)}

    def _owned(self, nonce, *, phases=None, allow_unblocked=False):
        _nonce(nonce)
        state = self.inspect()
        journal = state["journal"]
        if journal is None or journal["nonce"] != nonce:
            raise MaintenanceError("Чужой или неизвестный nonce")
        if not state["blocked"] and not allow_unblocked:
            raise MaintenanceError("Marker окна отсутствует")
        if phases is not None and journal["phase"] not in phases:
            raise MaintenanceError(f"Операция недопустима в фазе {journal['phase']}")
        return journal

    def _check_quiescence(self, exit_journal=None):
        if self.allow_verified_pending_jobs and exit_journal is not None:
            # An explicit adapter proves no remaining writes/active workers and
            # effective admission guards. Natural pending jobs may remain; this
            # hook MUST NOT create, cancel or start any job. Only a verified
            # target may use this finite exit contract, including crash recovery.
            if exit_journal["phase"] not in ("verified", "safe_to_resume", "complete"):
                raise MaintenanceError("Ослабление drain до проверки запрещено")
            self.hooks.check_exit(copy.deepcopy(exit_journal))
        else:
            self.hooks.drain()

    def _live(self, nonce, *, exit_journal=None):
        self._identity()
        if self.held_nonce != nonce:
            raise MaintenanceError("Четыре региональных замка не удерживаются")
        self.hooks.check_guard(self.marker)
        self.hooks.assert_run_locks(nonce)
        self._check_quiescence(exit_journal)
        self.hooks.assert_run_locks(nonce)

    def _acquire(self, nonce, *, exit_journal=None):
        if self.held_nonce is not None:
            if self.held_nonce != nonce:
                raise MaintenanceError("Объект удерживает замки другого окна")
            self._live(nonce, exit_journal=exit_journal)
            return
        self.hooks.check_guard(self.marker)
        self._check_quiescence(exit_journal)
        self.hooks.acquire_run_locks(nonce)  # Adapter cleans its own partial failure.
        self.held_nonce = nonce
        self._live(nonce, exit_journal=exit_journal)

    def _release_locks(self):
        if self.held_nonce is not None:
            nonce, self.held_nonce = self.held_nonce, None
            self.hooks.release_run_locks(nonce)

    def reserve(self, *, region, release_id, source_commit, manifest_sha256, nonce=None):
        nonce = _nonce(nonce or secrets.token_hex(16))
        state = self.inspect()
        if state["blocked"] or (state["journal"] is not None and state["journal"]["phase"] != "complete"):
            raise MaintenanceError("Нужно явно восстановить прежнее окно")
        journal = _journal({"schema_version": SCHEMA_VERSION, "protocol": PROTOCOL, "nonce": nonce,
            "region": region, "release_id": release_id, "source_commit": source_commit,
            "manifest_sha256": manifest_sha256, "active_target_id": 1,
            "targets": [{"target_id": 1, "release_id": release_id, "source_commit": source_commit,
                         "manifest_sha256": manifest_sha256, "reason": "release"}],
            "phase": "reserved", "revision": 1, "owner": self.owner, "snapshot": None,
            "attempts": [], "active_attempt_id": None, "verification": None,
            "safe_evidence": None, "outcome": None})
        if self._read(f"nonce-{nonce}.json") is not None:
            raise MaintenanceError("Nonce уже использован прежним окном")
        self._write(f"nonce-{nonce}.json", {"nonce": nonce, "release_id": release_id})
        # Marker is durable BEFORE any READY or publish permission can be returned.
        self._write("blocked", {"schema_version": SCHEMA_VERSION, "nonce": nonce})
        self._write("journal.json", journal)
        self.hooks.check_guard(self.marker)
        return copy.deepcopy(journal)

    def prepare(self, nonce):
        journal = self._owned(nonce, phases=("reserved", "ready"))
        self._acquire(nonce)
        if journal["snapshot"] is None:
            journal["snapshot"] = _snapshot(self.hooks.snapshot())
        self.hooks.validate_snapshot(copy.deepcopy(journal["snapshot"]))
        journal["phase"] = "ready"
        return self._save(journal)

    def recover(self, nonce):
        """Reacquire checks/locks only. Does not cancel intent or remove marker."""
        journal = self._owned(nonce, allow_unblocked=True)
        if journal["phase"] == "complete" and not self.inspect()["blocked"]:
            return journal
        final = journal if (journal["phase"] in ("verified", "safe_to_resume")
                            or journal["outcome"] == "installed") else None
        self._acquire(nonce, exit_journal=final)
        if journal["phase"] in ("reserved", "ready") and journal["snapshot"] is not None:
            self.hooks.validate_snapshot(copy.deepcopy(journal["snapshot"]))
        return journal

    def select_recovery_target(self, nonce, *, release_id, source_commit,
                               manifest_sha256, reason):
        """Retain every earlier late-push hazard inside the SAME closed lease.

        Selection is not permission to publish/apply. validate_target must prove
        the chosen manifest and original inventories before a new intent is ACKed.
        v1 journals are rejected by inspect, never silently migrated or replaced.
        """
        journal = self._owned(nonce, phases=("publish_intent", "applying", "verified",
                                            "safe_to_resume", "recovery_ready", "complete"))
        if not journal["attempts"]:
            raise MaintenanceError("Нет прежней публикации для восстановления")
        self._live(nonce)
        target = _target({"target_id": len(journal["targets"]) + 1,
                          "release_id": release_id, "source_commit": source_commit,
                          "manifest_sha256": manifest_sha256, "reason": reason},
                         len(journal["targets"]) + 1)
        journal["targets"].append(target)
        journal["active_target_id"] = target["target_id"]
        for key in ("release_id", "source_commit", "manifest_sha256"):
            journal[key] = target[key]
        journal.update(phase="recovery_ready", active_attempt_id=None,
                       verification=None, safe_evidence=None, outcome=None)
        return self._save(journal)

    def publish_intent(self, nonce, *, base_sha, target_sha):
        journal = self._owned(nonce, phases=("ready", "recovery_ready", "publish_intent"))
        self._live(nonce)
        if journal["active_target_id"] == 1:
            self.hooks.validate_snapshot(copy.deepcopy(journal["snapshot"]))
        self.hooks.validate_target(copy.deepcopy(journal))
        attempt = {"attempt_id": len(journal["attempts"]) + 1,
                   "target_id": journal["active_target_id"],
                   "base_sha": _sha(base_sha), "target_sha": _sha(target_sha)}
        journal["attempts"].append(attempt)
        journal["active_attempt_id"] = attempt["attempt_id"]
        journal["phase"] = "publish_intent"
        self._save(journal)  # Do not ACK before fsync(file) AND fsync(directory).
        return {"protocol": PROTOCOL, "nonce": nonce, **attempt}

    def begin_apply(self, nonce, attempt_id):
        journal = self._owned(nonce, phases=("publish_intent", "applying"))
        if type(attempt_id) is not int or attempt_id != journal["active_attempt_id"]:
            raise MaintenanceError("Устаревшее разрешение публикации")
        self._live(nonce)
        journal["phase"] = "applying"
        return self._save(journal)

    def mark_verified(self, nonce):
        journal = self._owned(nonce, phases=("applying", "verified"))
        self._live(nonce)
        journal["verification"] = _verification(self.hooks.verify_installed(copy.deepcopy(journal)), journal["targets"][-1])
        journal["phase"] = "verified"
        return self._save(journal)

    def apply(self, nonce, attempt_id):
        """Explicit host write boundary; never infer installation from an ACK.

        The host must bind its resumable Git journal to this exact target and
        independently validate the fetched commit and preserved inventory. It
        must not accept client commands or run a parser. Any failure leaves
        durable `applying` and the marker; repeating uses the same transaction.
        Verification/opening remain separate operations after this returns.
        """
        journal = self.begin_apply(nonce, attempt_id)
        self.hooks.apply_target(copy.deepcopy(journal))
        self._live(nonce)
        return self._owned(nonce, phases=("applying",))

    def _resume(self, journal, *, outcome):
        nonce = journal["nonce"]
        self._live(nonce, exit_journal=journal if outcome == "installed" else None)
        if outcome == "installed":
            journal["verification"] = _verification(self.hooks.verify_installed(copy.deepcopy(journal)), journal["targets"][-1])
        elif journal["snapshot"] is not None:
            self.hooks.validate_snapshot(copy.deepcopy(journal["snapshot"]))
        journal["safe_evidence"] = _safe_evidence(self.hooks.verify_safe(copy.deepcopy(journal)), journal["attempts"])
        if outcome == "installed":
            journal["outcome"] = None
            journal["phase"] = "safe_to_resume"
            journal = self._save(journal)
        # All run locks are released while the durable marker still excludes starts.
        self._release_locks()
        self.hooks.check_guard(self.marker)
        self._check_quiescence(journal if outcome == "installed" else None)
        # Adapter must close queued-job races and late-push races here as well.
        journal["safe_evidence"] = _safe_evidence(self.hooks.verify_safe(copy.deepcopy(journal)), journal["attempts"])
        journal["phase"], journal["outcome"] = "complete", outcome
        journal = self._save(journal)
        # COMPLETE is durable before marker removal: no missing marker with an
        # ambiguous journal after a crash. A complete-but-blocked retry rechecks.
        self._owned(nonce, phases=("complete",))
        os.unlink("blocked", dir_fd=self.dirfd)
        os.fsync(self.dirfd)
        return journal

    def finish(self, nonce):
        journal = self._owned(nonce, phases=("verified", "safe_to_resume", "complete"), allow_unblocked=True)
        if journal["phase"] == "complete" and not self.inspect()["blocked"]:
            return journal
        if journal["phase"] == "complete" and journal["outcome"] == "cancelled":
            self._acquire(nonce)
            return self._resume(journal, outcome="cancelled")
        self._acquire(nonce, exit_journal=journal)
        return self._resume(journal, outcome="installed")

    def cancel_before_publish(self, nonce):
        journal = self._owned(nonce, phases=("reserved", "ready"))
        self._acquire(nonce)
        if journal["snapshot"] is None:
            journal["snapshot"] = _snapshot(self.hooks.snapshot())
        return self._resume(journal, outcome="cancelled")
