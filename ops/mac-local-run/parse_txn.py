#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Crash-safe snapshot for one Mac parser invocation.

The parser mutates durable data and one-shot ``*_emitted`` markers before it
writes the daily digest context.  A crash in that window would make the next
run see no event to emit.  This helper snapshots every machine-generated data
path (except the digest context itself) before Python starts.

``save_digest_context`` writes an ACK carrying the same transaction id only
after the context is durable.  Recovery therefore has two safe outcomes:

* matching ACK: keep parser data and discard the snapshot;
* no ACK: restore the snapshot, while retaining any atomically written digest
  context as a write-ahead record for the next run.

The manifest is published last, so a crash while preparing the snapshot can
never make the wrapper start the parser with an incomplete recovery point.
"""

from __future__ import annotations

import glob
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
import uuid
from datetime import datetime


VERSION = 1


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_json(path: str, payload: dict) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.{os.getpid()}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = ""
        _fsync_dir(directory)
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass


def _load_json(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    if not isinstance(data, dict):
        raise RuntimeError(f"неверный JSON-объект: {path}")
    return data


def _safe_rel(repo: str, value: str) -> tuple[str, str]:
    rel = os.path.normpath(value.strip())
    if not rel or rel == "." or os.path.isabs(rel):
        raise RuntimeError(f"недопустимый путь snapshot: {value!r}")
    if rel == ".." or rel.startswith(".." + os.sep):
        raise RuntimeError(f"путь выходит за репозиторий: {value!r}")
    target = os.path.abspath(os.path.join(repo, rel))
    if os.path.commonpath([repo, target]) != repo:
        raise RuntimeError(f"путь выходит за репозиторий: {value!r}")
    return rel, target


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_durable(source: str, target: str, mode: int | None = None) -> None:
    directory = os.path.dirname(target) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{os.path.basename(target)}.{os.getpid()}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with open(source, "rb") as src, os.fdopen(fd, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, target)
        tmp = ""
        _fsync_dir(directory)
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass


def _snapshot_dir_for(journal: str, txn_id: str) -> str:
    return f"{journal}.files.{txn_id}"


def _validate_snapshot_dir(journal: str, snapshot_dir: str) -> str:
    journal_abs = os.path.abspath(journal)
    snapshot_abs = os.path.abspath(snapshot_dir)
    expected_prefix = journal_abs + ".files."
    if not snapshot_abs.startswith(expected_prefix):
        raise RuntimeError("каталог snapshot не привязан к journal")
    return snapshot_abs


def _remove_ack(ack_file: str, txn_id: str | None = None) -> None:
    try:
        ack = _load_json(ack_file)
    except (OSError, ValueError, RuntimeError):
        ack = None
    if txn_id and ack and str(ack.get("txn_id") or "") != txn_id:
        return
    try:
        os.unlink(ack_file)
    except FileNotFoundError:
        return
    _fsync_dir(os.path.dirname(ack_file) or ".")


def prepare(
    journal: str,
    ack_file: str,
    repo: str,
    excluded_rel: str,
    patterns: list[str],
) -> str:
    repo = os.path.abspath(repo)
    if not os.path.isdir(repo):
        raise RuntimeError(f"репозиторий не найден: {repo}")
    if os.path.exists(journal):
        raise RuntimeError("незавершённый parse snapshot уже существует")
    excluded, _ = _safe_rel(repo, excluded_rel)
    txn_id = uuid.uuid4().hex
    snapshot_dir = _snapshot_dir_for(journal, txn_id)
    os.makedirs(snapshot_dir, mode=0o700, exist_ok=False)
    _remove_ack(ack_file)

    normalized_patterns: list[str] = []
    entries: dict[str, dict] = {}
    absent_exact: list[str] = []
    try:
        for raw in patterns:
            if not raw.strip():
                continue
            pattern, absolute_pattern = _safe_rel(repo, raw)
            if pattern == excluded:
                continue
            if pattern not in normalized_patterns:
                normalized_patterns.append(pattern)
            has_magic = glob.has_magic(pattern)
            matches = glob.glob(absolute_pattern) if has_magic else [absolute_pattern]
            found = False
            for absolute in matches:
                if not os.path.isfile(absolute):
                    continue
                rel = os.path.relpath(absolute, repo)
                rel, absolute = _safe_rel(repo, rel)
                if rel == excluded or rel in entries:
                    continue
                found = True
                st = os.stat(absolute)
                backup = os.path.join(snapshot_dir, rel)
                _copy_durable(absolute, backup, stat.S_IMODE(st.st_mode))
                entries[rel] = {
                    "sha256": _sha256(absolute),
                    "mode": stat.S_IMODE(st.st_mode),
                }
            if not has_magic and not found:
                absent_exact.append(pattern)

        manifest = {
            "version": VERSION,
            "status": "prepared",
            "txn_id": txn_id,
            "repo": repo,
            "excluded": excluded,
            "snapshot_dir": snapshot_dir,
            "patterns": normalized_patterns,
            "entries": entries,
            "absent_exact": absent_exact,
            "created_at": _now(),
        }
        _atomic_json(journal, manifest)
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise
    return txn_id


def _ack_matches(ack_file: str, txn_id: str) -> bool:
    try:
        ack = _load_json(ack_file)
    except (OSError, ValueError, RuntimeError):
        return False
    return bool(ack and str(ack.get("txn_id") or "") == txn_id)


def _current_matches(manifest: dict) -> tuple[int, set[str]]:
    repo = os.path.abspath(str(manifest["repo"]))
    entries = manifest.get("entries") or {}
    changed = 0
    known = set(entries)
    for rel, meta in entries.items():
        _, target = _safe_rel(repo, rel)
        if not os.path.isfile(target) or _sha256(target) != meta.get("sha256"):
            changed += 1
    for rel in manifest.get("absent_exact") or []:
        _, target = _safe_rel(repo, rel)
        if os.path.exists(target):
            changed += 1
    for pattern in manifest.get("patterns") or []:
        if not glob.has_magic(pattern):
            continue
        _, absolute_pattern = _safe_rel(repo, pattern)
        for target in glob.glob(absolute_pattern):
            if not os.path.isfile(target):
                continue
            rel = os.path.relpath(target, repo)
            if rel not in known:
                changed += 1
    return changed, known


def _clear(journal: str, ack_file: str, manifest: dict) -> None:
    txn_id = str(manifest.get("txn_id") or "")
    snapshot_dir = _validate_snapshot_dir(
        journal, str(manifest.get("snapshot_dir") or "")
    )
    # Journal disappears first.  If power is lost while deleting backups,
    # recovery is no longer needed and an orphan directory is harmless.
    os.unlink(journal)
    _fsync_dir(os.path.dirname(journal) or ".")
    _remove_ack(ack_file, txn_id)
    shutil.rmtree(snapshot_dir, ignore_errors=True)


def rollback(journal: str, ack_file: str, manifest: dict) -> int:
    repo = os.path.abspath(str(manifest["repo"]))
    snapshot_dir = _validate_snapshot_dir(
        journal, str(manifest.get("snapshot_dir") or "")
    )
    changed, known = _current_matches(manifest)

    # Remove only newly-created files matched by the manifest patterns.
    for pattern in manifest.get("patterns") or []:
        if not glob.has_magic(pattern):
            continue
        _, absolute_pattern = _safe_rel(repo, pattern)
        for target in glob.glob(absolute_pattern):
            if not os.path.isfile(target):
                continue
            rel = os.path.relpath(target, repo)
            if rel not in known:
                os.unlink(target)
                _fsync_dir(os.path.dirname(target) or ".")

    for rel in manifest.get("absent_exact") or []:
        _, target = _safe_rel(repo, rel)
        if os.path.isfile(target) or os.path.islink(target):
            os.unlink(target)
            _fsync_dir(os.path.dirname(target) or ".")
        elif os.path.exists(target):
            raise RuntimeError(f"отказ удалять не-файл при rollback: {rel}")

    for rel, meta in (manifest.get("entries") or {}).items():
        _, target = _safe_rel(repo, rel)
        backup = os.path.join(snapshot_dir, rel)
        if not os.path.isfile(backup):
            raise RuntimeError(f"в snapshot нет файла: {rel}")
        _copy_durable(backup, target, int(meta.get("mode", 0o600)))

    _clear(journal, ack_file, manifest)
    return changed


def recover(journal: str, ack_file: str) -> tuple[str, int]:
    manifest = _load_json(journal)
    if manifest is None:
        return "absent", 1
    txn_id = str(manifest.get("txn_id") or "")
    if not txn_id:
        raise RuntimeError("parse snapshot без txn_id")
    if _ack_matches(ack_file, txn_id):
        _clear(journal, ack_file, manifest)
        return "wal_committed", 0
    changed = rollback(journal, ack_file, manifest)
    return f"rolled_back:{changed}", 0


def finish(journal: str, ack_file: str, txn_id: str) -> tuple[str, int]:
    manifest = _load_json(journal)
    if manifest is None:
        raise RuntimeError("parse snapshot исчез до finish")
    if str(manifest.get("txn_id") or "") != txn_id:
        raise RuntimeError("finish не вправе закрыть чужой parse snapshot")
    if _ack_matches(ack_file, txn_id):
        _clear(journal, ack_file, manifest)
        return "wal_committed", 0
    changed = rollback(journal, ack_file, manifest)
    if changed:
        return f"rolled_back_without_wal:{changed}", 3
    return "clean_without_wal", 0


def main(argv: list[str]) -> int:
    try:
        command = argv[0] if argv else ""
        if command == "prepare" and len(argv) == 5:
            txn_id = prepare(
                argv[1], argv[2], argv[3], argv[4],
                [line.rstrip("\n") for line in sys.stdin],
            )
            print(txn_id)
            return 0
        if command == "recover" and len(argv) == 3:
            result, rc = recover(argv[1], argv[2])
            print(result)
            return rc
        if command == "finish" and len(argv) == 4:
            result, rc = finish(argv[1], argv[2], argv[3])
            print(result)
            return rc
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"parse_txn: {exc}", file=sys.stderr)
        return 2
    print(
        "usage: parse_txn.py prepare JOURNAL ACK REPO EXCLUDED < paths | "
        "recover JOURNAL ACK | finish JOURNAL ACK TXN_ID",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
