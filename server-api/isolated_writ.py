#!/usr/bin/env python3
"""Настоящая логика ИЛ на закрытой синтетической копии, без Git и отправок.

Это репетиция будущего местного исполнителя; рабочие каталоги здесь запрещены.
Общая региональная блокировка удерживается штатным run_lock, без удаления locks.
"""
from __future__ import annotations
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

SOURCE = Path(__file__).resolve().parents[1]
FILES = ("cases.json", "cases_archive.json", "cases_bank.json", "cases_bank_archive.json",
         "cases_bank_events.json", "cases_bank_archive_events.json")


def atomic(path, payload):
    temp = path.with_suffix(path.suffix + ".tmp")
    with open(temp, "w", encoding="utf-8") as out:
        os.chmod(temp, 0o600)
        json.dump(payload, out, ensure_ascii=False, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fingerprints(directory):
    result = {}
    for name in FILES:
        path = directory / name
        if path.is_symlink():
            raise ValueError("Недопустимая ссылка")
        data = path.read_bytes()
        json.loads(data)
        result[name] = hashlib.sha256(data).hexdigest()
    return result


def digest(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main():
    command, root_arg, region, job_id = sys.argv[1:]
    import re
    if command not in ("apply", "inspect", "publish") or not re.fullmatch(r"[a-f0-9-]{36}", job_id):
        raise ValueError("Неверная операция")
    root = Path(root_arg).resolve()
    if not Path(root_arg).is_absolute() or str(root).startswith(("/opt/", "/srv/")):
        raise ValueError("Нужен отдельный изолированный каталог")
    marker = json.loads((root / ".sbersud-isolated.json").read_text())
    if marker != {"region": region, "synthetic": True}:
        raise ValueError("Нет явного признака синтетической копии")
    if any(p.is_symlink() for p in root.rglob("*")):
        raise ValueError("В изолированной копии недопустимы ссылки")
    spec = importlib.util.spec_from_file_location("sbersud_run_lock", SOURCE / "ops/mac-local-run/run_lock.py")
    lock = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lock)
    lock_path = str(root / ".run.lock")
    if lock.acquire(lock_path, os.getpid()) != 0:
        raise RuntimeError("Изолированная картотека занята")
    try:
        candidate = root / "snapshots" / job_id
        receipt_path = candidate / "receipt.json"
        current_path = root / "current.json"
        current = json.loads(current_path.read_text())
        if not re.fullmatch(r"[a-zA-Z0-9-]+", current["version"]):
            raise ValueError("Неверный снимок")
        if command == "apply" and not receipt_path.exists():
            source = root / "snapshots" / current["version"] / "data"
            base_hash = digest(fingerprints(source))
            candidate.mkdir(parents=True, exist_ok=True, mode=0o700)
            target = candidate / "data"
            target.mkdir(exist_ok=True, mode=0o700)
            for name in FILES:
                shutil.copy2(source / name, target / name)
            env = {"PATH": os.environ.get("PATH", ""), "REGION": region,
                   "BANK_TRACK": "1", "PYTHONDONTWRITEBYTECODE": "1",
                   "JSON_PATH": str(target / "cases.json"),
                   "IMPORT_SUMMARY_PATH": str(candidate / "summary.json")}
            # JSON_* neighbours derive from JSON_PATH in the common config.
            result = subprocess.run([sys.executable, str(SOURCE / "scripts/mark_writ_waived.py"),
                                     "--job", str(root / "jobs" / (job_id + ".json"))],
                                    cwd=candidate, env=env, capture_output=True)
            if result.returncode:
                raise RuntimeError("Обработка ИЛ завершилась ошибкой")
            files = fingerprints(target)
            for name in FILES:
                with open(target / name, "rb") as f:
                    os.fsync(f.fileno())
            summary = json.loads((candidate / "summary.json").read_text())
            receipt = {"region": region, "job_id": job_id, "sha256": digest(files), "files": files,
                       "base_version": current["version"], "base_sha256": base_hash, "summary": summary}
            atomic(receipt_path, receipt)
        if not receipt_path.exists():
            print("null")
            return
        receipt = json.loads(receipt_path.read_text())
        if receipt["region"] != region or receipt["job_id"] != job_id or digest(fingerprints(candidate / "data")) != receipt["sha256"]:
            raise ValueError("Результат применения повреждён")
        if command == "publish":
            if current["version"] != job_id:
                if current["version"] != receipt["base_version"] or digest(fingerprints(root / "snapshots" / current["version"] / "data")) != receipt["base_sha256"]:
                    raise ValueError("После подготовки появились новые данные")
                atomic(current_path, {"version": job_id, "sha256": receipt["sha256"]})
            published = json.loads(current_path.read_text())
            if published.get("sha256") != receipt["sha256"]:
                raise ValueError("Опубликован другой снимок")
            receipt["verified"] = True
        print(json.dumps(receipt, ensure_ascii=False))
    finally:
        lock.release(lock_path, os.getpid())


if __name__ == "__main__":
    main()
