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


# This deliberately narrow contract is audited against the first common-source
# release. New runtime code never qualifies merely because it has mode 100644.
ROUTING_HASHES = {
    'ops/vps-run/parse_all.sh': 'ea16170c97c3cb1334df16b2b6fc85dfe74500b492cb94c5a4ce65d27b5f1034',
    'ops/vps-run/import_all.sh': '476d7488f201dc4534cfc63a7181d587286d0464365479acdf1f3dbf28a749bd',
    'ops/vps-run/delivery_all.sh': 'f8dfa823c9aba803377e31922c2f65f6ae6f2a17704e90cf5fa3389725020e2b',
    'ops/vps-run/import_poll.sh': 'caeba6d782ac2b02fc2afe8d2d7cd5a5f5de73ed85e88f18fc2617ef3f099613',
    'ops/vps-run/vps_env.sh': '008eff4cdac42629b389b9f83b2c9884e6c7d2da433dd2f09791a7fb20ab742b',
    'ops/vps-run/shims/netstat': 'd5e5dca03bb8f69d193b541c9ef6ab1c3c1857c9bad5ccff03184f77d84ae496',
    'ops/mac-local-run/parse_all.sh': '3f6683b1e04d99aeb703194b89d1126d112a103f6cb954951b2eecd51002d082',
    'ops/mac-local-run/import_all.sh': 'fbcda8f88369099f9364ef4c9eb904207b8cd927172ff880f46941e708d1dfae',
    'ops/mac-local-run/delivery_all.sh': 'e3f398227b6d581faeccb7bb8134d957b19cafad4cb016aff713ed894bde1a4c',
    'ops/mac-local-run/lib_sber_net.sh': '53a62d24d990559387dc851bf805a299e6721c7779e848ca124ff0142db3d135',
}
DORMANT_DISPATCHERS = {
    'ops/mac-local-run/parse_all.sh', 'ops/mac-local-run/import_all.sh',
    'ops/mac-local-run/delivery_all.sh', 'ops/vps-run/import_poll.sh',
}
NON_RUNTIME_FILES = {
    'AGENTS.md', 'README.md', 'scripts/program_release.py', 'scripts/program_install_vps.py',
    '.github/workflows/tests.yml', '.github/workflows/collect_bank_claims.yml',
    '.github/workflows/probe_region_registry.yml',
}
ENV_KEYS = {
    'DASHBOARD_URL', 'BANK_TRACK', 'BANK_AUTO_INTAKE', 'BANK_INTAKE_DRY_RUN',
    'BANK_INTAKE_MAX_PER_RUN', 'BANK_INTAKE_MAX_CARDS_PER_COURT',
    'BANK_INTAKE_DIGEST_FOLD', 'BANK_FORCE_DIGEST_FOLD',
    'DIGEST_PARTIES_MAX_LEN', 'DIGEST_PARTIES_KEEP', 'REGION',
}


def systemd_version():
    value = run(['systemctl', '--version']).stdout
    match = re.match(r'^systemd ([0-9]+)(?:\s|$)', value)
    if not match:
        raise InstallError('Не удалось однозначно определить версию systemd')
    return int(match.group(1))


def current_file_records(repo, names):
    """Hash Git objects in one batch; never interpret the working tree as code."""
    entries = {}
    for line in run(['git', 'ls-tree', '-r', '-z', '--full-tree', 'HEAD'], cwd=repo).stdout.split('\0'):
        if line:
            meta, name = line.split('\t', 1)
            if name in names:
                mode, kind, oid = meta.split()
                if kind != 'blob' or mode not in ('100644', '100755'):
                    raise InstallError('Небезопасный тип установленного файла: ' + name)
                entries[name] = (mode, oid)
    raw = run(['git', 'cat-file', '--batch'], cwd=repo, text=False,
              input=''.join(oid + '\n' for _, oid in entries.values()).encode()).stdout
    result, offset = {}, 0
    for name, (mode, expected) in entries.items():
        end = raw.index(b'\n', offset)
        oid, kind, size = raw[offset:end].decode().split()
        size = int(size)
        if oid != expected or kind != 'blob':
            raise InstallError('Неожиданный ответ Git при проверке установленной программы')
        result[name] = {'mode': mode, 'sha256': hashlib.sha256(raw[end + 1:end + 1 + size]).hexdigest()}
        offset = end + size + 2
    return result


def actual_changes(repo, document):
    """Compare both tracked bytes and the working tree before classifying a path."""
    installed = None
    stamp = repo / STAMP
    if stamp.exists() or stamp.is_symlink():
        if stamp.is_symlink():
            raise InstallError('Символическая ссылка вместо паспорта VPS')
        installed = validate_document(json.loads(stamp.read_text()))
        if installed['region'] != document['region']:
            raise InstallError('Паспорт VPS относится к другой территории')
        if installed != lock_document(repo, 'HEAD'):
            raise InstallError('Паспорт VPS изменён вне Git')
        verify_files(repo, installed)
    names = set(document['files']) | set((installed or {}).get('files', {}))
    records = current_file_records(repo, names)
    verify_files(repo, {'files': records})
    for name in names - set(records):
        target = repo
        parts = safe_name(name).parts
        for index, part in enumerate(parts):
            target = target / part
            if target.is_symlink():
                raise InstallError('Символическая ссылка на новом пути выпуска: ' + name)
            if index < len(parts) - 1 and target.exists() and not target.is_dir():
                raise InstallError('Посторонний файл занимает каталог выпуска: ' + name)
        if target.exists() or target.is_symlink():
            raise InstallError('Посторонний файл занимает путь выпуска: ' + name)
    changed = sorted(name for name in names if records.get(name) != document['files'].get(name))
    return changed, records, installed == document


def narrow_path_allowed(name, before, after, region):
    if name in DORMANT_DISPATCHERS:
        return before is not None and after is not None
    # The standalone Tyumen relay is not a court-* service. Only its initial
    # addition in another regional clone is dormant, never an existing service edit.
    if name in ('ops/vps-run/tyumen_upload_relay.py', 'scripts/court_monitor/regions/tyumen.py',
                'docs/regions/tyumen_courts.json'):
        return region != 'tyumen' and before is None and after is not None and after['mode'] == '100644'
    if after is not None and after['mode'] != '100644':
        return False
    if before is not None and before['mode'] != '100644':
        return False
    return (name in NON_RUNTIME_FILES
            or (name.startswith('docs/') and name.endswith('.md'))
            or (name.startswith('scripts/tests/test_') and name.endswith('.py')))


def read_properties(unit, properties):
    value = run(['systemctl', 'show', unit, '--property=' + ','.join(properties)]).stdout
    return dict(line.split('=', 1) for line in value.splitlines() if '=' in line)


def reject_old_recovery(root):
    if list(root.glob('.program-install-*.json')):
        raise InstallError('Есть запись прежней установки; требуется разбор до узкой установки')
    for command in ('list-units', 'list-unit-files'):
        result = run(['systemctl', command, '--all', '--plain', '--no-legend', 'court-program-recover-*'], check=False)
        # systemd 255 uses exit 1 for list-unit-files with no matching glob.
        no_matches = command == 'list-unit-files' and result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip()
        if result.returncode and not no_matches:
            raise InstallError('Не удалось проверить прежние задания восстановления таймеров')
        if result.stdout.strip():
            raise InstallError('Есть прежнее задание восстановления таймеров; требуется разбор')


def validate_narrow_routing(root, region):
    """Read loaded systemd state and plain configuration; never source an env file."""
    shared = root / REPOSITORIES['hmao']
    verify_files(shared, {'files': {name: {'sha256': digest,
                  'mode': '100644' if name in ('ops/vps-run/vps_env.sh', 'ops/mac-local-run/lib_sber_net.sh') else '100755'}
                  for name, digest in ROUTING_HASHES.items()}})
    manager = run(['systemctl', 'show-environment']).stdout
    for line in manager.splitlines():
        key, sep, value = line.partition('=')
        if not sep or key not in ('LANG', 'PATH'):
            raise InstallError('Неизвестное окружение менеджера systemd; значения скрыты')
        if key == 'PATH' and any(p not in ('/usr/local/sbin', '/usr/local/bin', '/usr/sbin', '/usr/bin', '/sbin', '/bin') for p in value.split(':')):
            raise InstallError('Неожиданный PATH менеджера systemd')
    launchers = {'court-parse': 'parse_all.sh', 'court-import': 'import_all.sh',
                 'court-import-poll': 'import_poll.sh', 'court-delivery': 'delivery_all.sh',
                 'court-retry': 'parse_all.sh --retry-only'}
    empty = ('ExecStartPre', 'ExecStartPost', 'ExecCondition', 'ExecStop', 'ExecStopPost', 'ExecReload',
             'EnvironmentFiles', 'PassEnvironment', 'UnsetEnvironment', 'RootDirectory', 'RootImage',
             'WorkingDirectory', 'BindPaths', 'BindReadOnlyPaths')
    props = (*empty, 'ExecStart', 'Environment', 'LoadState', 'NeedDaemonReload', 'User')
    for service, launcher in launchers.items():
        state = read_properties(service + '.service', props)
        if (state.get('LoadState') != 'loaded' or state.get('NeedDaemonReload') != 'no'
                or state.get('User') != 'root' or any(state.get(name) for name in empty)):
            raise InstallError('Неожиданная загруженная конфигурация службы ' + service)
        expected = '/bin/bash ' + str(shared / 'ops/vps-run') + '/' + launcher
        starts = re.findall(r'\{ path=([^;]+) ; argv\[\]=([^;]+) ;', state.get('ExecStart', ''))
        if (starts != [('/bin/bash', expected)] or state.get('ExecStart', '').count('{') != 1
                or state.get('ExecStart', '').count('}') != 1):
            raise InstallError('Не подтверждён путь запуска службы ' + service)
        try:
            environment = shlex.split(state.get('Environment', ''))
        except ValueError:
            raise InstallError('Нераспознанное окружение службы ' + service) from None
        allowed = {'CM_PARALLEL_STAGGER_SECONDS', 'CM_PARALLEL_START_DELAYS'} if service in ('court-parse', 'court-retry') else set()
        for assignment in environment:
            key, sep, value = assignment.partition('=')
            if not sep or key not in allowed:
                raise InstallError('Неожиданное окружение службы ' + service + '; значения скрыты')
            if key == 'CM_PARALLEL_STAGGER_SECONDS' and not value.isdigit():
                raise InstallError('Нераспознанная задержка службы ' + service)
            if key == 'CM_PARALLEL_START_DELAYS' and any(not re.fullmatch(r'(hmao|sverdlovsk_yanao|bashkortostan|tyumen)=[0-9]+', item) for item in value.split()):
                raise InstallError('Нераспознанные задержки территорий службы ' + service)
    target = root / REPOSITORIES[region]
    registry = 'scripts/court_monitor/regions/__init__.py'
    verify_files(target, {'files': {registry: {'mode': '100644',
                  'sha256': 'b63f3af935ce04c71af13bffe314d6a5e7e328dc3346582ceffc372f6f9a89d8'}}})
    # Other loaded services may not directly execute this regional clone. Read
    # their parsed commands too; do not print command/environment values.
    units = run(['systemctl', 'list-units', '--all', '--type=service', '--plain', '--no-legend', '--no-pager']).stdout
    others = []
    for line in units.splitlines():
        unit = line.split()[0]
        if not re.fullmatch(r'[A-Za-z0-9_.@\\:-]+\.service', unit):
            raise InstallError('Нераспознанный список загруженных служб')
        if unit not in {name + '.service' for name in SERVICES}:
            others.append(unit)
    if others:
        commands = run(['systemctl', 'show', *others,
                        '--property=ExecStart,ExecStartPre,ExecStartPost,ExecCondition,ExecReload,ExecStop,ExecStopPost,Environment,EnvironmentFiles,WorkingDirectory,RootDirectory,RootImage,BindPaths,BindReadOnlyPaths']).stdout
        if str(target) in commands or REPOSITORIES[region] in commands:
            raise InstallError('Другая загруженная служба использует региональный клон')
    config = Path.home() / '.config/court-monitor'
    listing = config / 'territories'
    if not listing.is_file() or listing.is_symlink():
        raise InstallError('Не подтверждён файл маршрутов территорий')
    territories = [line.strip() for line in listing.read_text().splitlines()
                   if line.strip() and not line.lstrip().startswith('#')]
    if len(territories) != len(REPOSITORIES) or set(territories) != {str(root/name) for name in REPOSITORIES.values()}:
        raise InstallError('Неожиданные маршруты территорий')
    for code in REPOSITORIES:
        env_file = config / ('env.' + code)
        if not env_file.exists():
            continue
        if env_file.is_symlink() or not env_file.is_file():
            raise InstallError('Необычный файл окружения территории ' + code)
        for line in env_file.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            # Config is normally shell syntax. Only literal single assignments
            # are acceptable for the narrow route; expansions are never evaluated.
            if any(char in stripped for char in '$`;&|<>\\'):
                raise InstallError('Окружение требует ручной проверки: ' + code + '; значения скрыты')
            try:
                parts = shlex.split(stripped, comments=True)
            except ValueError:
                raise InstallError('Нераспознанное окружение территории ' + code) from None
            if parts and parts[0] == 'export':
                parts = parts[1:]
            if len(parts) != 1:
                raise InstallError('Окружение требует ручной проверки: ' + code)
            key, sep, value = parts[0].partition('=')
            if not sep or key not in ENV_KEYS or (key == 'REGION' and value != code):
                raise InstallError('Неожиданное окружение территории ' + code + '; значения скрыты')
    if (root / REPOSITORIES[region] / 'REGION').read_text().strip() != region:
        raise InstallError('Не подтверждён фактический регион VPS')


def check_idle_target(repo):
    if busy_services():
        raise InstallError('Службы заняты; узкая установка отложена без остановки таймеров')
    runtime = repo / 'ops/mac-local-run/.runtime'
    if any((runtime / name).exists() for name in ('parse_txn.json', 'delivery_txn.json')):
        raise InstallError('Незавершённая транзакция; её завершит штатная служба')


def installation_mode(root, document, *, readonly=False):
    region = document['region']
    repo = root / REPOSITORIES[region]
    if git(repo, 'branch', '--show-current') != 'main':
        raise InstallError('Рабочая копия VPS должна оставаться в main')
    changed, before, identical = actual_changes(repo, document)
    if identical and not changed:
        reject_old_recovery(root)
        return {'mode': 'noop', 'changed_paths': [], 'head': git(repo, 'rev-parse', 'HEAD')}
    if git(repo, 'status', '--porcelain', '--untracked-files=no'):
        raise InstallError('В рабочей копии есть незакоммиченные изменения до публикации')
    version = systemd_version()
    eligible = region != 'hmao' and all(narrow_path_allowed(name, before.get(name), document['files'].get(name), region) for name in changed)
    if eligible:
        reject_old_recovery(root)
        validate_narrow_routing(root, region)
        check_idle_target(repo)
        if readonly and (repo / 'ops/mac-local-run/.run.lock').exists():
            raise InstallError('Занята штатная блокировка; предварительная проверка отложена')
        mode = 'narrow'
    elif version >= 259:
        raise InstallError('Обновление исполняемой программы требует отдельной процедуры: резервирование VPS до публикации не реализовано')
    else:
        raise InstallError('Нужна общая остановка таймеров, небезопасная на systemd < 259; установка заблокирована до изменения VPS')
    return {'mode': mode, 'systemd_version': version, 'changed_paths': changed,
            'head': git(repo, 'rev-parse', 'HEAD')}


def remote_preflight(document, *, root=Path('/opt/court-monitor')):
    document = validate_document(document)
    result = installation_mode(Path(root).resolve(), document, readonly=True)
    return dict(result, preflight_passed=True, region=document['region'],
                source_commit=document['source_commit'], release_id=document['release_id'])


def local_preflight(document, host, identity):
    document = validate_document(document)
    if not host or host.startswith('-') or not identity:
        raise InstallError('Нужны --host и --identity')
    # The checked manifest travels inside stdin with the installer, never as a
    # shell expression or an argument longer than the platform's argv limit.
    source = Path(__file__).read_text().rsplit('\nif __name__', 1)[0]
    source += '\nPREFLIGHT_DOCUMENT = ' + repr(document) + "\nraise SystemExit(main(['--remote-preflight']))\n"
    proc = subprocess.run(['ssh', '-i', identity, '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes',
                           '-o', 'ConnectTimeout=15', host, 'python3 -I -B -'],
                          input=source, text=True, capture_output=True, timeout=120)
    if proc.returncode:
        # Remote diagnostics are controlled JSON. Never reflect arbitrary ssh/
        # shell stderr that could contain values from an unexpected environment.
        try:
            error = json.loads(proc.stderr.strip()).get('error', 'VPS preflight failed')
        except ValueError:
            error = 'Не удалось выполнить предварительную проверку VPS'
        raise InstallError(error)
    try:
        result = json.loads(proc.stdout)
    except ValueError:
        raise InstallError('Некорректный результат предварительной проверки VPS') from None
    if (result.get('preflight_passed') is not True or any(result.get(key) != document[key]
            for key in ('region', 'source_commit', 'release_id'))):
        raise InstallError('Предварительная проверка VPS относится к другому выпуску')
    return result


def narrow_install(repo, region, target_lock, latest, url, root, expected_source):
    """No systemctl mutation, no timer receipt/rescue. Only dormant file changes."""
    lock = repo / 'ops/mac-local-run/.run.lock'
    with tempfile.TemporaryDirectory(prefix='court-program-target-lock-') as directory:
        tool = Path(directory) / 'run_lock.py'
        tool.write_bytes((lock.parent / 'run_lock.py').read_bytes())
        expected_tool = target_lock['files'].get('ops/mac-local-run/run_lock.py')
        if not expected_tool or hashlib.sha256(tool.read_bytes()).hexdigest() != expected_tool['sha256']:
            raise InstallError('Не подтверждён скрипт штатной блокировки')
        if run([sys.executable, str(tool), 'acquire', str(lock), str(os.getpid())], check=False).returncode:
            raise InstallError('Занята штатная блокировка ' + REPOSITORIES[region])
        try:
            check_idle_target(repo)
            if git(repo, 'status', '--porcelain', '--untracked-files=no'):
                raise InstallError('В рабочей копии есть незакоммиченные изменения')
            old = git(repo, 'rev-parse', 'HEAD')
            git(repo, 'fetch', url, 'refs/heads/main:refs/remotes/origin/main')
            current = git(repo, 'rev-parse', 'origin/main')
            if lock_document(repo, current) != target_lock:
                raise InstallError('Версия программы изменилась во время ожидания')
            if current != latest:
                names = git(repo, 'diff', '--name-only', latest, current).splitlines()
                if any(not name.startswith('data/') for name in names):
                    raise InstallError('После выпуска появились изменения вне данных')
                preflight_revision(repo, current, target_lock, region)
            # Repeat the actual filesystem and loaded-routing checks after the
            # lock and fetch: only data advancement may change the outcome.
            mode = installation_mode(root, target_lock)
            if mode['mode'] != 'narrow':
                raise InstallError('Класс установки изменился после блокировки')
            git(repo, 'merge-base', '--is-ancestor', 'HEAD', current)
            changes = git(repo, 'diff', '--name-only', 'HEAD', current).splitlines()
            allowed = set(mode['changed_paths']) | {STAMP}
            if any(name not in allowed and not name.startswith('data/') for name in changes):
                raise InstallError('GitHub меняет посторонний файл вне пакета')
            git(repo, 'merge', '--ff-only', current)
            verify_files(repo, target_lock)
            if git(repo, 'status', '--porcelain', '--untracked-files=no'):
                raise InstallError('После установки изменились tracked-файлы')
            return {'region': region, 'from': old, 'head': current, 'source_commit': expected_source,
                    'installed': True, 'mode': 'narrow', 'timers_changed': False,
                    'normal_cycle_verified': False, 'worker_deployed': False}
        finally:
            run([sys.executable, str(tool), 'release', str(lock), str(os.getpid())], check=False)


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
    mode = installation_mode(root, target_lock)
    if mode['mode'] == 'noop':
        return dict(mode, region=region, source_commit=expected_source, installed=True,
                    unchanged=True, timers_changed=False, normal_cycle_verified=False,
                    worker_deployed=False)
    if mode['mode'] == 'narrow':
        return narrow_install(repo, region, target_lock, latest, url, root, expected_source)
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
    parser.add_argument("--remote-preflight", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--preflight-manifest")
    parser.add_argument("--host")
    parser.add_argument("--identity", "--ssh-key", dest="identity")
    parser.add_argument("--region", choices=REPOSITORIES)
    parser.add_argument("--commit")
    parser.add_argument("--expected-source")
    args = parser.parse_args(argv)
    try:
        if args.remote_preflight:
            result = remote_preflight(PREFLIGHT_DOCUMENT)
            print(json.dumps(result, ensure_ascii=False))
            return 0
        if args.preflight_manifest:
            result = local_preflight(json.loads(Path(args.preflight_manifest).read_text()), args.host, args.identity)
            print(json.dumps(result, ensure_ascii=False))
            return 0
        if args.region not in REPOSITORIES:
            raise InstallError('Нужен --region')
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
    except (InstallError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"installed": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
