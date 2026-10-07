#!/usr/bin/env python3
"""Установка опубликованного выпуска; без парсинга, доставки и смены расписаний."""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid

REPOSITORIES = {"hmao": "dashboard", "sverdlovsk_yanao": "dashboard-ural",
                "bashkortostan": "dashboard-bashkortostan", "tyumen": "dashboard-tyumen"}
SERVICES = ("court-parse", "court-import", "court-import-poll", "court-delivery", "court-retry")
STAMP = ".program-release.json"
FORBIDDEN = {"data", "runtime", "logs", ".git", ".venv", "venv", "node_modules",
             ".wrangler", ".claude", ".aws", ".codex", ".agents", "secrets"}

class InstallError(RuntimeError):
    pass

def run(args, *, cwd=None, check=True, text=True, input=None):
    try:
        p = subprocess.run(args, cwd=cwd, text=text, input=input,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    except subprocess.TimeoutExpired as exc:
        raise InstallError(f"Истекло время команды {args[0]}") from exc
    if check and p.returncode:
        error = p.stderr if text else p.stderr.decode("utf-8", "replace")
        raise InstallError(f"Команда {args[0]} завершилась с кодом {p.returncode}: {error.strip()[:1500]}")
    return p

def git(repo, *args):
    return run(["git", *args], cwd=repo).stdout.strip()

def valid_sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", value):
        raise InstallError("Нужен полный SHA коммита")
    return value

def safe_name(name):
    if not isinstance(name, str) or not name or "\\" in name or any(ord(c) < 32 for c in name):
        raise InstallError("Небезопасный путь в паспорте выпуска")
    path = PurePosixPath(name)
    if path.is_absolute() or any(p in ("", ".", "..") for p in name.split("/")):
        raise InstallError("Небезопасный путь в паспорте выпуска")
    if (path.parts[0] in FORBIDDEN or name == STAMP
            or any(p.startswith((".env", ".dev.vars")) for p in path.parts)
            or any(p in (".git", ".run.lock", ".runtime", ".ssh", ".secrets") for p in path.parts)
            or path.name.lower() in ("id_rsa", "id_ed25519", "credentials", "credentials.json")
            or path.name.lower().endswith((".pem", ".key", ".p12", ".pfx", ".log"))
            or (path.parts[0] == "ops" and len(path.parts) > 1 and path.parts[1] in
                ("bank_registry", "court_probe", "region_probe", "writ_probe"))):
        raise InstallError(f"Защищённый путь в паспорте выпуска: {name}")
    return path

def validate_document(document):
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise InstallError("Неизвестный формат паспорта выпуска")
    region = document.get("region")
    if region not in REPOSITORIES or document.get("repository") != "SelivanovAS/" + REPOSITORIES[region]:
        raise InstallError("Некорректная территория или репозиторий выпуска")
    source_repo = document.get("source_repo")
    expected_repo = document["repository"] if document.get("kind") == "baseline" else "SelivanovAS/dashboard"
    if document.get("kind") not in ("program", "baseline") or source_repo != expected_repo:
        raise InstallError("Неизвестный источник программы")
    valid_sha(document.get("source_commit"))
    for key in ("profile_sha256", "baseline_sha256", "release_id"):
        if not isinstance(document.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", document[key]):
            raise InstallError(f"Некорректный {key}")
    files = document.get("files")
    if not isinstance(files, dict) or not files:
        raise InstallError("В паспорте нет файлов программы")
    for name, record in files.items():
        path = safe_name(name)
        if any(parent.as_posix() in files for parent in path.parents):
            raise InstallError("Файл одновременно используется как каталог")
        if (not isinstance(record, dict) or set(record) != {"sha256", "mode"}
                or record.get("mode") not in ("100644", "100755")
                or not isinstance(record.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", record["sha256"])):
            raise InstallError(f"Некорректная запись файла: {name}")
    canonical = (json.dumps({k: v for k, v in document.items() if k != "release_id"},
                            ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    if hashlib.sha256(canonical).hexdigest() != document["release_id"]:
        raise InstallError("Не совпадает контрольная сумма паспорта выпуска")
    return document

def lock_document(repo, revision):
    def unique_pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InstallError("Повторный ключ в паспорте выпуска")
            result[key] = value
        return result
    try:
        value = json.loads(git(repo, "show", f"{revision}:{STAMP}"), object_pairs_hook=unique_pairs)
        return validate_document(value)
    except (ValueError, InstallError) as exc:
        raise InstallError(f"Нет корректного паспорта выпуска в {revision}: {exc}") from exc

def verify_files(repo, document):
    for name, record in document["files"].items():
        path = safe_name(name)
        target = repo
        for part in path.parts:
            target = target / part
            if target.is_symlink():
                raise InstallError(f"Символическая ссылка вместо файла выпуска: {name}")
        if not target.is_file():
            raise InstallError(f"Отсутствует обычный файл выпуска: {name}")
        if hashlib.sha256(target.read_bytes()).hexdigest() != record["sha256"]:
            raise InstallError(f"Не совпадает хеш установленного файла: {name}")
        actual = "100755" if target.stat().st_mode & 0o100 else "100644"
        if record["mode"] != actual:
            raise InstallError(f"Не совпадают права установленного файла: {name}")

def preflight_revision(repo, revision, document, region):
    """Проверяем Git-объекты и config вне рабочего клона, без боевого env и data."""
    entries = {}
    for record in run(["git", "ls-tree", "-r", "-z", "--full-tree", revision], cwd=repo).stdout.split("\0"):
        if record:
            metadata, name = record.split("\t", 1)
            entries[name] = metadata.split()
    selected = []
    for name, record in document["files"].items():
        mode, kind, oid = entries.get(name, (None, None, None))
        if kind != "blob" or mode != record["mode"]:
            raise InstallError(f"Не совпадает тип/права Git-файла: {name}")
        selected.append((name, oid))
    raw = run(["git", "cat-file", "--batch"], cwd=repo, text=False,
              input="".join(oid + "\n" for _, oid in selected).encode()).stdout
    with tempfile.TemporaryDirectory(prefix="court-program-preflight-") as directory:
        stage = Path(directory)
        offset = 0
        for name, expected_oid in selected:
            end = raw.index(b"\n", offset)
            oid, kind, size = raw[offset:end].decode().split()
            size = int(size)
            if oid != expected_oid or kind != "blob":
                raise InstallError("Неожиданный ответ Git при проверке выпуска")
            content = raw[end + 1:end + 1 + size]
            offset = end + 2 + size
            path = stage / safe_name(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            path.chmod(0o755 if document["files"][name]["mode"] == "100755" else 0o644)
        verify_files(stage, document)
        region_file = stage / "REGION"
        if region_file.is_file():
            if region_file.read_text().strip() != region:
                raise InstallError("Не совпадает файл REGION выпуска")
        elif not (document.get("kind") == "baseline" and region == "hmao"):
            raise InstallError("Не совпадает файл REGION выпуска")
        # До первого общего выпуска ХМАО использовал штатный default hmao
        # без файла REGION. Только такой исходный снимок допускает отсутствие
        # файла; эффективный регион всё равно подтверждается ниже процессом
        # без env и боевых данных. Новые program-пакеты требуют явного REGION.
        # Не задаём REGION через env: иначе неверный регион в файле маскируется.
        code = "import sys; sys.path.insert(0, 'scripts'); from court_monitor import config; print(config.REGION)"
        try:
            p = subprocess.run([sys.executable, "-I", "-B", "-c", code], cwd=stage,
                               env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
                               capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired as exc:
            raise InstallError("Проверка конфига превысила время") from exc
        if p.returncode or p.stdout.strip() != region:
            raise InstallError("В изолированной проверке не подтверждён регион запуска")

@contextmanager
def installation_guard(root):
    path = root / ".program-install.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise InstallError("На VPS уже выполняется установка программы") from exc
        yield path
    finally:
        os.close(fd)

# Запускается systemd после обрыва installer. flock не позволяет оживить
# таймеры, пока установка ещё работает. Незавершённая запись требует разбора.
RECOVERY_CODE = """import json, subprocess, sys
from pathlib import Path
state = json.loads(Path(sys.argv[1]).read_text())
if state.get('id') != sys.argv[2] or state.get('phase') not in ('waiting', 'verified'):
    raise SystemExit('Незавершенная установка: таймеры оставлены остановленными')
raise SystemExit(subprocess.run(['/bin/systemctl', 'start', *state['timers']]).returncode)
"""

def remote_install(region, commit, expected_source, *, root=Path("/opt/court-monitor"), timeout=300):
    valid_sha(commit)
    valid_sha(expected_source)
    root = Path(root).resolve()
    with installation_guard(root) as guard:
        return _remote_install(region, commit, expected_source, root, timeout, guard)

def busy_services():
    busy = []
    for name in SERVICES:
        unit = name + ".service"
        state = run(["systemctl", "show", unit, "--property=ActiveState", "--value"]).stdout.strip()
        job = run(["systemctl", "show", unit, "--property=Job", "--value"]).stdout.strip()
        if state in ("active", "activating", "deactivating", "reloading") or (job and job.split()[0] != "0"):
            busy.append(name)
    return busy


def interrupted_states(root, region):
    states = []
    allowed = {name + ".timer" for name in SERVICES}
    for path in sorted(root.glob(".program-install-*.json")):
        try:
            if path.is_symlink():
                raise ValueError("symlink")
            state = json.loads(path.read_text())
            if (not isinstance(state, dict) or not re.fullmatch(r"[0-9a-f]{32}", state.get("id", ""))
                    or path.name != ".program-install-" + state["id"] + ".json"
                    or state.get("phase") not in ("waiting", "applying", "verified")
                    or state.get("region") != region
                    or not isinstance(state.get("timers"), list)
                    or any(timer not in allowed for timer in state["timers"])
                    or len(set(state["timers"])) != len(state["timers"])):
                raise ValueError("state")
        except (ValueError, OSError, TypeError) as exc:
            raise InstallError(f"Нужно разобрать состояние прежней установки: {path.name}") from exc
        states.append((path, state))
    if states and any(set(state["timers"]) != set(states[0][1]["timers"]) for _, state in states):
        raise InstallError("Неоднозначное исходное состояние таймеров прежней установки")
    return states


def _remote_install(region, commit, expected_source, root, timeout, guard):
    repo = root / REPOSITORIES[region]
    if git(repo, "branch", "--show-current") != "main":
        raise InstallError("Рабочая копия VPS должна оставаться в main")
    url = f"ssh://git@ssh.github.com:443/SelivanovAS/{REPOSITORIES[region]}.git"
    git(repo, "fetch", url, "refs/heads/main:refs/remotes/origin/main")
    git(repo, "merge-base", "--is-ancestor", commit, "origin/main")
    target_lock = lock_document(repo, commit)
    if target_lock["region"] != region or target_lock["source_commit"] != expected_source:
        raise InstallError("Коммит относится к другой территории или версии программы")
    latest = git(repo, "rev-parse", "origin/main")
    if lock_document(repo, latest) != target_lock:
        raise InstallError("В GitHub уже опубликована другая версия программы")
    changed = git(repo, "diff-tree", "--no-commit-id", "--name-only", "-r", commit).splitlines()
    if any(n.startswith("data/") for n in changed):
        raise InstallError("Релиз-коммит меняет рабочие данные")
    preflight_revision(repo, latest, target_lock, region)
    previous_states = interrupted_states(root, region)
    active_timers = [name + ".timer" for name in SERVICES
                     if run(["systemctl", "is-active", "--quiet", name + ".timer"], check=False).returncode == 0]
    if previous_states:
        original = previous_states[0][1]["timers"]
        if not set(active_timers).issubset(original):
            raise InstallError("Таймеры изменены после прерванной установки; требуется сверка состояния")
        active_timers = original
    locked = []
    nonce = uuid.uuid4().hex
    rescue = "court-program-recover-" + nonce
    state_file = root / (".program-install-" + nonce + ".json")
    rescue_armed = False
    timers_stopped = False
    timers_restored = False
    applying = any(state["phase"] == "applying" for _, state in previous_states)
    old = None
    def phase(value):
        # Атомарная замена: rescue не увидит наполовину записанный JSON.
        temporary = state_file.with_suffix(".tmp")
        temporary.write_text(json.dumps({"id": nonce, "region": region, "phase": value, "timers": active_timers}))
        os.replace(temporary, state_file)
    def interrupted(signum, frame):
        raise InstallError(f"Установка прервана сигналом {signum}")
    handlers = {s: signal.signal(s, interrupted) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        phase("applying" if applying else "waiting")
        if active_timers:
            run(["systemd-run", "--quiet", "--collect", "--unit=" + rescue,
                 "--on-active=10min", "/usr/bin/flock", "--exclusive", str(guard),
                 sys.executable, "-c", RECOVERY_CODE, str(state_file), nonce])
            rescue_armed = True
            # Частичный отказ stop тоже требует вернуть исходный набор.
            timers_stopped = True
            run(["systemctl", "stop", *active_timers])
        deadline = time.monotonic() + timeout
        while True:
            busy = busy_services()
            if not busy:
                break
            if time.monotonic() >= deadline:
                raise InstallError("Службы заняты; работающий парсер не прерывался")
            time.sleep(2)
        # Lock-tool копируем: его старая версия освобождает захваченный lock
        # даже при обновлении run_lock.py в устанавливаемом пакете.
        with tempfile.TemporaryDirectory(prefix="court-program-locks-") as lock_dir:
            for name in REPOSITORIES.values():
                clone = root / name
                tool = Path(lock_dir) / (name + ".py")
                tool.write_bytes((clone / "ops/mac-local-run/run_lock.py").read_bytes())
                lock = clone / "ops/mac-local-run/.run.lock"
                if run([sys.executable, str(tool), "acquire", str(lock), str(os.getpid())], check=False).returncode:
                    raise InstallError("Занята штатная блокировка " + name)
                locked.append((tool, lock))
                runtime = clone / "ops/mac-local-run/.runtime"
                if any((runtime / n).exists() for n in ("parse_txn.json", "delivery_txn.json")):
                    raise InstallError("Незавершённая транзакция " + name + "; её завершит штатная служба")
            try:
                if busy_services():
                    raise InstallError("Служба или задание появились во время захвата блокировок")
                if git(repo, "status", "--porcelain", "--untracked-files=no"):
                    raise InstallError("В рабочей копии есть незакоммиченные изменения после ожидания")
                if git(repo, "branch", "--show-current") != "main":
                    raise InstallError("Ветка рабочей копии изменилась во время ожидания")
                old = git(repo, "rev-parse", "HEAD")
                git(repo, "fetch", url, "refs/heads/main:refs/remotes/origin/main")
                current = git(repo, "rev-parse", "origin/main")
                if lock_document(repo, current) != target_lock:
                    raise InstallError("Версия программы изменилась во время ожидания")
                if current != latest:
                    preflight_revision(repo, current, target_lock, region)
                latest = current
                git(repo, "merge-base", "--is-ancestor", "HEAD", latest)
                phase("applying")
                applying = True
                git(repo, "merge", "--ff-only", latest)
                verify_files(repo, target_lock)
                if git(repo, "status", "--porcelain", "--untracked-files=no"):
                    raise InstallError("После установки изменились tracked-файлы")
                phase("verified")
                applying = False
                return {"region": region, "from": old, "head": latest, "source_commit": expected_source,
                        "installed": True, "normal_cycle_verified": False, "worker_deployed": False}
            finally:
                for tool, lock in reversed(locked):
                    run([sys.executable, str(tool), "release", str(lock), str(os.getpid())], check=False)
                locked.clear()
    finally:
        # При отказе до входа во внутренний try lock-tools могли удалиться;
        # владелец — этот PID, но лучше освободить их до возврата таймеров.
        for tool, lock in reversed(locked):
            # Этот путь только для отказа во время захвата: исходники не менялись.
            original = lock.parent / "run_lock.py"
            run([sys.executable, str(original), "release", str(lock), str(os.getpid())], check=False)
        try:
            if applying:
                # Ошибка checkout/проверки не разрешает запуск неизвестного кода.
                # Откат делают опубликованным пакетом поверх текущих data.
                raise InstallError("Установка не подтверждена; таймеры оставлены остановленными. Нужна проверка и повторная установка или опубликованный откат")
            if timers_stopped:
                run(["systemctl", "start", *active_timers])
            timers_restored = True
        finally:
            if rescue_armed and timers_restored:
                run(["systemctl", "stop", rescue + ".timer", rescue + ".service"], check=False)
            if timers_restored:
                state_file.unlink(missing_ok=True)
                for path, previous in previous_states:
                    unit = "court-program-recover-" + previous["id"]
                    run(["systemctl", "stop", unit + ".timer", unit + ".service"], check=False)
                    path.unlink(missing_ok=True)
            for sig, handler in handlers.items():
                signal.signal(sig, handler)

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--host")
    parser.add_argument("--identity", "--ssh-key", dest="identity")
    parser.add_argument("--region", choices=REPOSITORIES, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--expected-source", required=True)
    args = parser.parse_args(argv)
    try:
        valid_sha(args.commit)
        valid_sha(args.expected_source)
        if args.remote:
            result = remote_install(args.region, args.commit, args.expected_source)
            print(json.dumps(result, ensure_ascii=False))
            return 0
        if not args.host or not args.identity or args.host.startswith("-"):
            raise InstallError("Нужны --host и --identity")
        remote = shlex.join(["python3", "-", "--remote", "--region", args.region,
                             "--commit", args.commit, "--expected-source", args.expected_source])
        proc = subprocess.run(["ssh", "-i", args.identity, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                               "-o", "ConnectTimeout=15", args.host, remote], input=Path(__file__).read_text(), text=True)
        return proc.returncode
    except (InstallError, OSError) as exc:
        print(json.dumps({"installed": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
