#!/usr/bin/env python3
"""Импорт и публикация на помеченной синтетической копии. Сеть запрещена."""
from __future__ import annotations
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
from isolated_writ import atomic

SOURCE = Path(__file__).resolve().parents[1]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def files(directory):
    result = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() or path.is_file() and path.stat().st_nlink != 1:
            raise ValueError("Ссылки не допускаются")
        if path.is_file():
            raw = path.read_bytes()
            if path.suffix == ".json":
                json.loads(raw)
            result[path.relative_to(directory).as_posix()] = hashlib.sha256(raw).hexdigest()
        elif not path.is_dir():
            raise ValueError("Необычный файл")
    return result


def sync_tree(directory):
    for path in directory.rglob("*"):
        if path.is_file():
            with path.open("rb") as f:
                os.fsync(f.fileno())
    for path in [*sorted((p for p in directory.rglob("*") if p.is_dir()), reverse=True), directory]:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def canonical(url):
    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(sorted(parse_qsl(p.query, keep_blank_values=True))), ""))


def execute(job, candidate, root, region):
    """Only HTTP transport is substituted; parsing, intake and saves stay real."""
    sys.path.insert(0, str(SOURCE / "scripts"))
    os.environ.update(REGION=region, BANK_TRACK="1", JSON_PATH=str(candidate / "data/cases.json"),
                      CSV_PATH=str(candidate / "data/sberbank_cases.csv"),
                      IMPORT_SUMMARY_PATH=str(candidate / "summary.json"), FETCH_MAX_RETRIES="1",
                      CARD_BREAKER_MODE="count", CARD_BREAKER_THRESHOLD="5", CARD_BREAKER_PROBE_EVERY="3")
    os.chdir(candidate)
    # Same declared data paths as production staging. Preserve the entire data
    # tree as well, including cold archives, deletions and unrelated sidecars.
    staged = subprocess.check_output(["bash", str(SOURCE / "ops/stage_data_files.sh"), "--list"],
                                    cwd=SOURCE, text=True)
    for name in staged.splitlines():
        Path(name).resolve().relative_to(candidate / "data")
    import requests
    from court_monitor import config
    from court_monitor.regions import get_region
    region_config = get_region(region)
    allowed = {c.domain for c in (*region_config.first_instance_courts, *region_config.appeal_courts,
                                  *region_config.presidium_courts, region_config.cassation_court)}
    config.REQUEST_DELAY = (0, 0)  # No real court is contacted in this replay.
    recording = json.loads((root / "responses.json").read_text())
    if recording.get("region") != region or recording.get("synthetic") is not True:
        raise ValueError("Нет синтетических сетевых ответов")
    responses = {canonical(k): v for k, v in recording["responses"].items()}
    calls = []

    def request(session, method, url, **kwargs):
        if method.upper() != "GET" or urlsplit(url).hostname not in allowed:
            raise RuntimeError("Запрос вне реестра")
        key = canonical(url)
        calls.append(key)
        item = responses.get(key)
        if item is None or item.get("error"):
            raise requests.ConnectionError("Изолированная недоступная карточка")
        response = requests.Response()
        response.status_code = item.get("status", 200)
        response.url = url
        response._content = item.get("body", "").encode("cp1251")
        return response

    requests.Session.request = request
    # Stop all accidental sockets, OCR subprocesses and shell calls during the
    # actual importer; only the above recording transport is available.
    restricted = [True]
    def audit(event, args):
        if restricted[0] and (event.startswith("socket.") or event in ("subprocess.Popen", "os.system", "os.posix_spawn")):
            raise RuntimeError("Внешние действия запрещены в изолированном импорте")
    sys.addaudithook(audit)
    try:
        if job["workflow"] in ("add_cases.yml", "mark_writ.yml"):
            import importlib
            importer = importlib.import_module("add_cases_targeted" if job["workflow"] == "add_cases.yml" else "mark_writ_waived")
            input_path = candidate / "input.json"
            input_path.write_text(job["body"], encoding="utf-8")
            rc = importer.main(["--job", str(input_path)])
        elif job["workflow"] == "import_cases.yml":
            import import_search_dump
            input_path = candidate / "input.html"
            input_path.write_text(job["body"], encoding="utf-8")
            args = job["inputs"]
            rc = import_search_dump.main([str(input_path), "--court-domain", args.get("court_domain") or "",
                 "--delo-id", args.get("delo_id") or "", "--section", args.get("section") or "",
                 "--operator", args.get("operator") or ""])
        else:
            raise ValueError("Неизвестный импортёр")
    finally:
        restricted[0] = False
    summary = json.loads((candidate / "summary.json").read_text())
    transient = rc == 4 and job["workflow"] == "add_cases.yml"
    pending = any(summary.get(k, 0) > 0 for k in ("fetch_error", "fetch_fail", "card_failed", "bank_capped"))
    outcome = "failed" if rc and not transient else "retry" if transient or pending else "done"
    if outcome == "failed":
        summary["error"] = (summary.get("error") or "Входные данные отклонены") + "; повтор не поможет — исправьте входные данные."
    atomic(candidate / "transport.json", {"requests": calls, "external_requests": 0})
    return summary, outcome


def main():
    command, root_arg, region, job_id, attempt_arg, node = sys.argv[1:]
    if command not in ("apply", "inspect", "publish", "initialize") or not re.fullmatch(r"[a-f0-9-]{36}", job_id):
        raise ValueError("Неверная операция")
    attempt = int(attempt_arg)
    if not 1 <= attempt <= 100000:
        raise ValueError("Неверная попытка")
    root = Path(root_arg).resolve()
    if str(root) != root_arg or re.match(r"^/(opt|srv)(/|$)", str(root)):
        raise ValueError("Нужен изолированный каталог")
    if json.loads((root / ".sbersud-isolated.json").read_text()) != {"region": region, "synthetic": True}:
        raise ValueError("Нет признака синтетической копии")
    if any(p.is_symlink() or p.is_file() and p.stat().st_nlink != 1 for p in root.rglob("*")):
        raise ValueError("Ссылки не допускаются")
    spec = importlib.util.spec_from_file_location("run_lock", SOURCE / "ops/mac-local-run/run_lock.py")
    lock = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lock)
    lock_path = str(root / ".run.lock")
    if lock.acquire(lock_path, os.getpid()) != 0:
        raise RuntimeError("Копия занята")
    try:
        version = f"{job_id}-{attempt}"
        candidate = root / "snapshots" / version
        receipt_path = candidate / "receipt.json"
        current = json.loads((root / "current.json").read_text())
        if not re.fullmatch(r"[a-zA-Z0-9-]+", current["version"]):
            raise ValueError("Неверный указатель")
        source = root / "snapshots" / current["version"] / "data"
        if command == "initialize":
            if current["version"] != "initial" or current.get("sha256"):
                raise ValueError("Первоначальная публикация уже создана")
            initial = source.parent
            before = files(source)
            subprocess.run([node, str(SOURCE / "server-api/data-index.mjs"), str(source),
                            str(initial / "api-index.json"), region], check=True, capture_output=True)
            if files(source) != before:
                raise ValueError("Исходная копия изменилась")
            payload = {"files": before, "index_sha256": hashlib.sha256((initial / "api-index.json").read_bytes()).hexdigest()}
            receipt = {"region": region, "job_id": None, "version": "initial", "outcome": "done",
                       **payload, "sha256": digest(payload)}
            sync_tree(initial)
            atomic(initial / "receipt.json", receipt)
            atomic(root / "current.json", {"version": "initial", "sha256": receipt["sha256"]})
            print(json.dumps({"initialized": True, "sha256": receipt["sha256"]}))
            return
        if command == "apply" and not receipt_path.exists():
            if current["version"] == version:
                raise ValueError("У опубликованного снимка потеряна квитанция")
            if candidate.exists():
                shutil.rmtree(candidate)  # only unpublished incomplete private candidate
            candidate.mkdir(mode=0o700)
            base_hash = digest(files(source))
            for name in ("cases.json", "cases_bank.json"):
                doc = json.loads((source / name).read_text())
                if name == "cases.json" and doc.get("region", {}).get("code") != region or doc.get("region") and doc["region"]["code"] != region:
                    raise ValueError("Чужая картотека")
            shutil.copytree(source, candidate / "data")
            job = json.loads((root / "jobs" / f"{version}.json").read_text())
            if job["id"] != job_id or job["attempt"] != attempt:
                raise ValueError("Другое задание")
            summary, outcome = execute(job, candidate, root, region)
            if outcome != "failed":
                subprocess.run([node, str(SOURCE / "server-api/data-index.mjs"), str(candidate / "data"),
                                str(candidate / "api-index.json"), region], check=True, capture_output=True)
                index_hash = hashlib.sha256((candidate / "api-index.json").read_bytes()).hexdigest()
            else:
                index_hash = None
            payload = {"files": files(candidate / "data"), "index_sha256": index_hash}
            receipt = {"region": region, "job_id": job_id, "attempt": attempt, "version": version,
                       **payload, "sha256": digest(payload), "base_version": current["version"],
                       "base_sha256": base_hash, "summary": summary, "outcome": outcome}
            sync_tree(candidate)
            atomic(receipt_path, receipt)
            sync_tree(candidate.parent)
        if not receipt_path.exists():
            print("null")
            return
        receipt = json.loads(receipt_path.read_text())
        payload = {"files": files(candidate / "data"), "index_sha256": None}
        if receipt["outcome"] != "failed":
            payload["index_sha256"] = hashlib.sha256((candidate / "api-index.json").read_bytes()).hexdigest()
        if (receipt["region"] != region or receipt["job_id"] != job_id or receipt["attempt"] != attempt
                or payload != {"files": receipt["files"], "index_sha256": receipt["index_sha256"]}
                or digest(payload) != receipt["sha256"]):
            raise ValueError("Сохранённый результат повреждён")
        if command == "publish":
            if receipt["outcome"] == "failed":
                raise ValueError("Отказ не публикуется")
            if current["version"] != version:
                if current["version"] != receipt["base_version"] or digest(files(source)) != receipt["base_sha256"]:
                    raise ValueError("Исходные данные изменились")
                atomic(root / "current.json", {"version": version, "sha256": receipt["sha256"]})
            if json.loads((root / "current.json").read_text()).get("sha256") != receipt["sha256"]:
                raise ValueError("Публикация не подтверждена")
            receipt["verified"] = True
        print(json.dumps(receipt, ensure_ascii=False))
    finally:
        lock.release(lock_path, os.getpid())


if __name__ == "__main__":
    main()
