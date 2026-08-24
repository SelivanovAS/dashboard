#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Локальный журнал транзакции Mac -> GitHub для дневного дайджеста.

Журнал создаётся ДО ``delivered_at`` и переживает SIGKILL/сон/обрыв сети.
Он не является данными суда и живёт в ``ops/mac-local-run/.runtime``.

CLI намеренно мал: shell-обёртка владеет уведомлениями и компенсацией,
этот модуль — атомарным состоянием и трёхзначной проверкой remote:

* accepted (0): marker SHA уже main либо его предок;
* absent (1): remote прочитан, marker SHA там нет;
* unknown (2): remote проверить нельзя — откатывать штамп опасно.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_journal(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return {"status": "corrupt"}
    return data if isinstance(data, dict) else {"status": "corrupt"}


def _atomic_write(path: str, data: dict) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            # Данные уже атомарно заменены; fsync каталога на отдельных FS
            # недоступен и не должен превращать успешную запись в отказ.
            pass
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def prepare(path: str, delivery_id: str, pre_sha: str) -> None:
    current = load_journal(path)
    if current:
        raise RuntimeError(
            f"незавершённая delivery-транзакция уже существует: "
            f"{current.get('status', '?')} {current.get('delivery_id', '?')}"
        )
    now = _now()
    _atomic_write(path, {
        "version": 1,
        "status": "prepared",
        "delivery_id": delivery_id,
        "pre_sha": pre_sha,
        "marker_sha": "",
        "created_at": now,
        "updated_at": now,
    })


def mark_committed(path: str, delivery_id: str, marker_sha: str) -> None:
    current = load_journal(path)
    if not current or current.get("status") == "corrupt":
        raise RuntimeError("delivery journal отсутствует или повреждён")
    if current.get("delivery_id") != delivery_id:
        raise RuntimeError("delivery_id журнала не совпадает")
    current["status"] = "committed"
    current["marker_sha"] = marker_sha
    current["updated_at"] = _now()
    _atomic_write(path, current)


def clear(path: str, delivery_id: str) -> None:
    current = load_journal(path)
    if current is None:
        return
    if current.get("status") == "corrupt":
        raise RuntimeError("повреждённый delivery journal нельзя молча удалить")
    if current.get("delivery_id") != delivery_id:
        raise RuntimeError("отказ удалить journal чужой delivery-транзакции")
    os.unlink(path)
    # Удаление — такой же durable state transition, как atomic replace:
    # после внезапного питания старый journal не должен «воскреснуть».
    directory = os.path.dirname(path) or "."
    try:
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def read_line(path: str) -> str | None:
    current = load_journal(path)
    if current is None:
        return None
    if current.get("status") == "corrupt":
        raise RuntimeError("delivery journal повреждён")
    # `read` в системном Bash 3.2 схлопывает соседние whitespace-разделители:
    # у prepared marker_sha пуст, и TSV сдвинул бы pre_sha в третье поле.
    # Вертикальная черта ни в SHA, ни в REGION:issue_key не встречается.
    return "|".join([
        str(current.get("status") or ""),
        str(current.get("delivery_id") or ""),
        str(current.get("marker_sha") or ""),
        str(current.get("pre_sha") or ""),
    ])


def remote_contains(repo: str, remote: str, marker_sha: str) -> int:
    """Вернуть 0 accepted, 1 definitely absent, 2 unknown."""
    fetched = subprocess.run(
        ["git", "fetch", "--quiet", remote, "refs/heads/main"],
        cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if fetched.returncode != 0:
        return 2
    # marker_sha должен существовать локально; отсутствие объекта означает
    # повреждённый journal, а не доказательство отсутствия на remote.
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{marker_sha}^{{commit}}"], cwd=repo,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if exists.returncode != 0:
        return 2
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", marker_sha, "FETCH_HEAD"],
        cwd=repo, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if ancestor.returncode == 0:
        return 0
    if ancestor.returncode == 1:
        return 1
    return 2


def main(argv: list[str]) -> int:
    try:
        command = argv[0]
        if command == "prepare" and len(argv) == 4:
            prepare(argv[1], argv[2], argv[3])
            return 0
        if command == "committed" and len(argv) == 4:
            mark_committed(argv[1], argv[2], argv[3])
            return 0
        if command == "clear" and len(argv) == 3:
            clear(argv[1], argv[2])
            return 0
        if command == "read" and len(argv) == 2:
            line = read_line(argv[1])
            if line is None:
                return 1
            print(line)
            return 0
        if command == "remote-state" and len(argv) == 4:
            rc = remote_contains(argv[1], argv[2], argv[3])
            print(("accepted", "absent", "unknown")[rc])
            return rc
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"delivery_txn: {exc}", file=sys.stderr)
        return 2
    print(
        "usage: delivery_txn.py prepare JOURNAL ID PRE_SHA | "
        "committed JOURNAL ID SHA | clear JOURNAL ID | read JOURNAL | "
        "remote-state REPO REMOTE SHA",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
