#!/usr/bin/env python3
"""Импорт на отдельной копии; закрытый пилот допускает ограниченное чтение судов."""
from __future__ import annotations
import hashlib
import gzip
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
PUBLIC_FILE=re.compile(r'^(?:cases(?:_bank)?(?:_archive(?:_[0-9]{4})?)?(?:_events)?\.json|sberbank_cases(?:_archive)?\.csv)$')


def encode_publication(candidate,manifest):
    directory=candidate/'encoded'
    directory.mkdir(exist_ok=True,mode=0o700)
    result={}
    for name in manifest:
        if not PUBLIC_FILE.fullmatch(name):continue
        path=directory/(name+'.gz')
        with (candidate/'data'/name).open('rb') as src,path.open('wb') as output:
            with gzip.GzipFile(filename='',mode='wb',fileobj=output,compresslevel=4,mtime=0) as compressed:
                shutil.copyfileobj(src,compressed,1024*1024)
        result[name]=hashlib.sha256(path.read_bytes()).hexdigest()
    return result


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


def execute(job, candidate, root, region, pilot=False):
    """Only HTTP transport is substituted; parsing, intake and saves stay real."""
    sys.path.insert(0, str(SOURCE / "scripts"))
    os.environ.update(REGION=region, BANK_TRACK="1", JSON_PATH=str(candidate / "data/cases.json"),
                      CSV_PATH=str(candidate / "data/sberbank_cases.csv"),
                      IMPORT_SUMMARY_PATH=str(candidate / "summary.json"), FETCH_MAX_RETRIES="1",
                      CARD_BREAKER_MODE="count", CARD_BREAKER_THRESHOLD="5", CARD_BREAKER_PROBE_EVERY="3")
    parsing=job['workflow']=='update_cases.yml'
    if parsing:
        options=json.loads(job['body'])
        os.environ.update(CM_SUMMARY_GENERATION='0',RUN_DEADLINE_SECONDS='600',CARD_BREAKER_MODE='time',
            SKIP_CHECKED_TODAY='1',SKIP_NON_WORKING_DAYS='1' if options.get('smart_skip',True) else '0',
            IGNORE_NON_WORKING_DAY='1' if options.get('ignore_calendar') else '0',
            CM_RETRY_ONLY='1' if options.get('retry_only') else '0',
            PARSE_TELEMETRY_FILE=str(candidate/'parse-telemetry.jsonl'))
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
    if pilot:
        from court_transport import CourtTransport
        network=json.loads((root / "court-network.json").read_text())
        if network.get("region")!=region or not set(network.get("allowed_hosts",[]))<=allowed:
            raise ValueError("Сетевая конфигурация другой территории")
        transport=CourtTransport(network["allowed_hosts"],network.get("max_requests",12),network.get("ca_file"))
    else:
        config.REQUEST_DELAY = (0, 0)  # No real court is contacted in this replay.
        recording = json.loads((root / "responses.json").read_text())
        if recording.get("region") != region or recording.get("synthetic") is not True:
            raise ValueError("Нет синтетических сетевых ответов")
        responses = {canonical(k): v for k, v in recording["responses"].items()}
    calls = []
    network_active=[False]
    (candidate/'tmp').mkdir(exist_ok=True)
    import tempfile
    tempfile.tempdir=str(candidate/'tmp')

    def inside(path):
        if isinstance(path,(str,bytes,os.PathLike)):
            Path(os.fsdecode(path)).resolve().relative_to(candidate)

    def request(session, method, url, **kwargs):
        if pilot:
            network_active[0]=True
            try:return transport.request(session,method,url,**kwargs)
            finally:network_active[0]=False
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
    # During parsing allow only the selected recording or guarded court transport.
    # OCR subprocesses, shell calls and unrelated network access stay disabled.
    restricted = [True]
    def audit(event, args):
        if restricted[0] and (event.startswith("socket.") and not network_active[0]
                or event in ("subprocess.Popen", "os.system", "os.posix_spawn")):
            raise RuntimeError("Внешние действия запрещены в изолированном импорте")
        if restricted[0]:
            if event=='open' and len(args)>2 and args[2] & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):inside(args[0])
            elif event in ('os.remove','os.rmdir','os.mkdir','os.chmod','os.utime'):inside(args[0])
            elif event in ('os.rename','os.link','os.symlink'):inside(args[0]);inside(args[1])
    sys.addaudithook(audit)
    try:
        if parsing:
            from court_monitor import runs
            # Same no-delivery entry point as ops/mac-local-run/run_parse.py,
            # retaining the candidate cwd and all original parser rules.
            runs.validate_environment=lambda *args,**kwargs:None
            runs.main_json()
            from court_monitor.netutil import run_deadline_reached
            summary={'parse_completed':True,'deadline_reached':run_deadline_reached(),
                     'metrics':dict(config.METRICS),'delivery_enabled':False,'models_enabled':False}
            atomic(candidate/'summary.json',summary)
            rc=0
        elif job["workflow"] in ("add_cases.yml", "mark_writ.yml"):
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
    atomic(candidate / "transport.json", {"requests": transport.calls if pilot else calls,
          "external_requests":len(transport.calls) if pilot else 0,"mode":"closed-pilot" if pilot else "isolated"})
    return summary, outcome


def main():
    command, root_arg, region, job_id, attempt_arg, node = sys.argv[1:]
    if command not in ("apply", "inspect", "publish", "initialize") or not re.fullmatch(r"[a-f0-9-]{36}", job_id):
        raise ValueError("Неверная операция")
    attempt = int(attempt_arg)
    if not 1 <= attempt <= 100000:
        raise ValueError("Неверная попытка")
    root = Path(root_arg).resolve()
    if str(root) != root_arg:raise ValueError("Нужен канонический каталог")
    marker=json.loads((root / ".sbersud-isolated.json").read_text())
    pilot=marker=={"region":"hmao","mode":"closed-pilot"} and region=="hmao" and str(root)=="/srv/projects/sbersud/pilot/hmao/state/publication"
    if not pilot and (re.match(r"^/(opt|srv)(/|$)", str(root)) or marker!={"region":region,"synthetic":True}):
        raise ValueError("Нет изолированной копии или закрытого пилота")
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
            payload = {"files": before, "index_sha256": hashlib.sha256((initial / "api-index.json").read_bytes()).hexdigest(),
                       "gzip_files":encode_publication(initial,before)}
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
            summary, outcome = execute(job, candidate, root, region, pilot)
            if outcome != "failed":
                subprocess.run([node, str(SOURCE / "server-api/data-index.mjs"), str(candidate / "data"),
                                str(candidate / "api-index.json"), region], check=True, capture_output=True)
                index_hash = hashlib.sha256((candidate / "api-index.json").read_bytes()).hexdigest()
            else:
                index_hash = None
            manifest=files(candidate/'data')
            payload = {"files": manifest, "index_sha256": index_hash,
                       "gzip_files":encode_publication(candidate,manifest) if outcome!='failed' else {}}
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
        if 'gzip_files' in receipt:
            if any(not PUBLIC_FILE.fullmatch(name) for name in receipt['gzip_files']):raise ValueError('Неверное имя сжатого файла')
            payload['gzip_files']={name:hashlib.sha256((candidate/'encoded'/(name+'.gz')).read_bytes()).hexdigest()
                                   for name in receipt['gzip_files']}
        if receipt["outcome"] != "failed":
            payload["index_sha256"] = hashlib.sha256((candidate / "api-index.json").read_bytes()).hexdigest()
        if (receipt["region"] != region or receipt["job_id"] != job_id or receipt["attempt"] != attempt
                or payload != {key:receipt[key] for key in payload}
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
