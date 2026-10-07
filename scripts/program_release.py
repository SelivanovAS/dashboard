#!/usr/bin/env python3
"""Сборка и публикация общей программы без переноса рабочих данных.

build читает только Git-объекты полного SHA. promote делает коммит в отдельной
копии; без --push сохраняет refs/program-release/prepared/... в --repo, с
--push публикует обычным SSH push. Рабочее дерево --repo всегда сохраняется.
VPS устанавливается отдельным helper при --vps-host; Worker — отдельно.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from urllib.parse import urlparse

SCHEMA = 1
LOCK = ".program-release.json"
REGIONS = ("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
MODES = {"100644", "100755"}
REPORT_DIRS = {"bank_registry", "court_probe", "region_probe", "writ_probe", "initial_import"}
MANUAL_WORKFLOWS = {".github/workflows/collect_bank_claims.yml", ".github/workflows/probe_region_registry.yml", ".github/workflows/probe_writ_section.yml"}
FORBIDDEN_ROOTS = {"data", "runtime", "logs", "outputs", "exports", ".git", ".venv", "venv", "node_modules",
                   ".wrangler", ".claude", ".aws", ".codex", ".agents", "secrets"}


class ReleaseError(RuntimeError):
    """Safety check failed; no force/recovery is attempted."""


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def git(repo, *args, check=True, input_bytes=None):
    cp = subprocess.run(["git", "-C", str(repo), *args], input=input_bytes,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    if check and cp.returncode:
        raise ReleaseError(f"Git {args[0]} завершился с кодом {cp.returncode}: "
                           + cp.stderr.decode("utf-8", "replace")[-1500:])
    return cp


def git_text(repo, *args):
    return git(repo, *args).stdout.decode().strip()


def full_commit(repo, value):
    if not isinstance(value, str) or not COMMIT.fullmatch(value):
        raise ReleaseError("Нужен полный SHA коммита, а не ветка, тег или сокращение")
    actual = git_text(repo, "rev-parse", "--verify", value + "^{commit}")
    if actual != value:
        raise ReleaseError("SHA должен ссылаться непосредственно на коммит")
    return actual


def validate_region(region):
    if region not in REGIONS:
        raise ReleaseError(f"Неизвестная территория: {region}")


def safe_path(value, *, managed=False):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ReleaseError(f"Недопустимый путь: {value!r}")
    path = PurePosixPath(value)
    if (path.is_absolute() or path.as_posix() != value
            or any(p in {"", ".", ".."} for p in value.split("/"))
            or any(ord(c) < 32 for c in value)):
        raise ReleaseError(f"Небезопасный путь: {value!r}")
    parts, leaf = path.parts, path.name.lower()
    if managed and (parts[0] in FORBIDDEN_ROOTS or value == LOCK
                    or any(p.startswith((".env", ".dev.vars")) for p in parts)
                    or any(p in {".git", ".run.lock", ".runtime", ".ssh", ".secrets", "__pycache__", ".pytest_cache"} for p in parts)
                    or leaf in {"id_rsa", "id_ed25519", "credentials", "credentials.json"}
                    or leaf.endswith((".pem", ".key", ".p12", ".pfx", ".log"))
                    or (parts[0] == "ops" and len(parts) > 1 and parts[1] in REPORT_DIRS)):
        raise ReleaseError(f"Защищённый файл не может входить в программу: {value}")
    return value


def check_path_set(paths):
    paths = set(paths)
    for name in paths:
        safe_path(name, managed=True)
        for parent in PurePosixPath(name).parents:
            if parent.as_posix() in paths:
                raise ReleaseError(f"Файл одновременно используется как каталог: {parent}")


def check_protected_prefixes(paths, prefixes):
    if not isinstance(prefixes, list) or not all(isinstance(p, str) and p for p in prefixes):
        raise ReleaseError("protected_prefixes должен быть списком путей")
    for prefix in prefixes:
        safe_path(prefix.rstrip("/"))
    for path in paths:
        if any(path.startswith(prefix) or path == prefix.rstrip("/") for prefix in prefixes):
            raise ReleaseError(f"Путь защищён manifest: {path}")


def fs_path(root, name):
    safe_path(name)
    current = Path(root)
    for component in PurePosixPath(name).parts:
        current = current / component
        if current.is_symlink():
            raise ReleaseError(f"Символическая ссылка запрещена: {name}")
        if current.exists() and current != Path(root) / name and not current.is_dir():
            raise ReleaseError(f"Родитель пути не является каталогом: {name}")
    if current.exists() and not current.is_file():
        raise ReleaseError(f"Ожидался обычный файл: {name}")
    return current


def read_json(raw, label):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ReleaseError(f"Повторный ключ JSON в {label}: {key}")
            out[key] = value
        return out
    try:
        value = json.loads(raw, object_pairs_hook=pairs)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ReleaseError(f"Некорректный JSON: {label}") from exc
    if not isinstance(value, dict):
        raise ReleaseError(f"Ожидался объект JSON: {label}")
    return value


def tree_entries(repo, commit):
    result = {}
    for record in git(repo, "ls-tree", "-r", "-z", "--full-tree", commit).stdout.split(b"\0"):
        if record:
            metadata, path = record.split(b"\t", 1)
            result[path.decode()] = tuple(metadata.decode("ascii").split())
    return result


def read_git_files(repo, commit, paths, *, entries=None):
    entries = tree_entries(repo, commit) if entries is None else entries
    present = []
    for name in sorted(set(paths)):
        safe_path(name)
        if name in entries:
            mode, kind, oid = entries[name]
            if mode not in MODES or kind != "blob":
                raise ReleaseError(f"В исходнике не обычный файл: {name}")
            present.append((name, mode, oid))
    if not present:
        return {}
    raw = git(repo, "cat-file", "--batch", input_bytes="".join(x[2] + "\n" for x in present).encode()).stdout
    offset, result = 0, {}
    for name, mode, expected_oid in present:
        end = raw.index(b"\n", offset)
        oid, kind, size = raw[offset:end].decode().split()
        size = int(size)
        if oid != expected_oid or kind != "blob":
            raise ReleaseError(f"Ошибка Git-объекта: {name}")
        content = raw[end + 1:end + 1 + size]
        if len(content) != size:
            raise ReleaseError("Обрезанный Git-объект")
        result[name] = {"content": content, "mode": mode}
        offset = end + size + 2
    return result


def record_of(item):
    return None if item is None else {"sha256": digest(item["content"]), "mode": item["mode"]}


def validate_records(records, *, nullable=False):
    if not isinstance(records, dict):
        raise ReleaseError("files должен быть объектом с точными путями")
    check_path_set(records)
    for name, record in records.items():
        if record is None and nullable:
            continue
        if (not isinstance(record, dict) or set(record) != {"sha256", "mode"}
                or not isinstance(record.get("sha256"), str)
                or not HEX64.fullmatch(record["sha256"]) or record.get("mode") not in MODES):
            raise ReleaseError(f"Некорректные хеш или права файла: {name}")


def validate_baseline(baseline, region):
    if baseline.get("schema_version") != SCHEMA or baseline.get("region") != region:
        raise ReleaseError("Опись другой территории или неизвестного формата")
    if not isinstance(baseline.get("source_commit"), str) or not COMMIT.fullmatch(baseline["source_commit"]):
        raise ReleaseError("В описи отсутствует точный коммит")
    validate_records(baseline.get("files"), nullable=True)


def lock_from_package(package):
    return {key: value for key, value in package.items() if key != "baseline"}


def release_id(lock):
    return digest(canonical({key: value for key, value in lock.items() if key != "release_id"}))


def validate_lock(lock, region=None):
    if lock.get("schema_version") != SCHEMA:
        raise ReleaseError("Неизвестный формат выпуска")
    validate_region(lock.get("region"))
    if region is not None and lock["region"] != region:
        raise ReleaseError("Выпуск относится к другой территории")
    if not isinstance(lock.get("source_commit"), str) or not COMMIT.fullmatch(lock["source_commit"]):
        raise ReleaseError("Некорректный source_commit")
    for key in ("profile_sha256", "baseline_sha256", "release_id"):
        if not isinstance(lock.get(key), str) or not HEX64.fullmatch(lock[key]):
            raise ReleaseError(f"Некорректный {key}")
    if not isinstance(lock.get("source_repo"), str) or not isinstance(lock.get("repository"), str):
        raise ReleaseError("Не задан исходный или целевой репозиторий")
    validate_records(lock.get("files"))
    check_protected_prefixes(lock["files"], lock.get("protected_prefixes", []))
    if release_id(lock) != lock["release_id"]:
        raise ReleaseError("Контрольная сумма описания выпуска не совпадает")


def load_package(directory):
    directory = Path(directory).resolve()
    package = read_json(fs_path(directory, "package.json").read_bytes(), "package.json")
    lock = lock_from_package(package)
    validate_lock(lock)
    baseline = package.get("baseline")
    if not isinstance(baseline, dict):
        raise ReleaseError("В пакете отсутствует начальная опись")
    validate_baseline(baseline, lock["region"])
    if digest(canonical(baseline)) != lock["baseline_sha256"]:
        raise ReleaseError("Хеш начальной описи не совпадает")
    files_root = directory / "files"
    if files_root.is_symlink() or not files_root.is_dir():
        raise ReleaseError("В пакете отсутствует обычный каталог files")
    observed = set()
    for path in files_root.rglob("*"):
        if path.is_symlink():
            raise ReleaseError("Символические ссылки в пакете запрещены")
        if path.is_file():
            observed.add(path.relative_to(files_root).as_posix())
        elif not path.is_dir():
            raise ReleaseError("Специальный файл в пакете запрещён")
    if observed != set(lock["files"]):
        raise ReleaseError("Фактический перечень файлов отличается от package.json")
    for name, expected in lock["files"].items():
        path = fs_path(files_root, name)
        mode = "100755" if path.stat().st_mode & stat.S_IXUSR else "100644"
        if {"sha256": digest(path.read_bytes()), "mode": mode} != expected:
            raise ReleaseError(f"Повреждён файл пакета: {name}")
    region_file = files_root / "REGION"
    if "REGION" in lock["files"]:
        if region_file.read_text(encoding="utf-8").strip() != lock["region"]:
            raise ReleaseError("Файл REGION не соответствует территории пакета")
    elif lock.get("kind") != "baseline" or lock["region"] != "hmao":
        raise ReleaseError("В программе должен быть явный файл REGION")
    return package


def build_release(source_repo, source_sha, out, regions=None, baseline_repo=None):
    source_repo, out = Path(source_repo).resolve(), Path(out).resolve()
    full_commit(source_repo, source_sha)
    regions = list(REGIONS if regions is None else regions)
    if not regions or len(set(regions)) != len(regions):
        raise ReleaseError("Укажите неповторяющиеся территории")
    for region in regions:
        validate_region(region)
    if baseline_repo is not None and len(regions) != 1:
        raise ReleaseError("--baseline-repo требует ровно одну территорию")
    entries = tree_entries(source_repo, source_sha)
    paths = ["deployment/manifest.json"]
    for region in regions:
        paths += [f"deployment/regions/{region}/profile.json", f"deployment/baselines/{region}.json"]
    configs = read_git_files(source_repo, source_sha, paths, entries=entries)
    if set(configs) != set(paths):
        raise ReleaseError("В выбранном коммите нет manifest/profile/baseline")
    manifest = read_json(configs["deployment/manifest.json"]["content"], "manifest")
    common = manifest.get("common_files")
    if manifest.get("schema_version") != SCHEMA or not isinstance(common, list) or not all(isinstance(p, str) for p in common):
        raise ReleaseError("Ожидался manifest schema_version=1, common_files=[точные пути]")
    if len(set(common)) != len(common):
        raise ReleaseError("Повторный путь в common_files")
    check_path_set(common)
    prefixes = manifest.get("protected_prefixes", [])
    check_protected_prefixes(common, prefixes)
    prepared = []
    for region in regions:
        profile_path = f"deployment/regions/{region}/profile.json"
        profile_raw = configs[profile_path]["content"]
        profile = read_json(profile_raw, profile_path)
        if profile.get("region") != region or not isinstance(profile.get("repository"), str):
            raise ReleaseError("Некорректный региональный профиль")
        mapping = {p: p for p in common}
        overrides = profile.get("files")
        if not isinstance(overrides, dict):
            raise ReleaseError("profile.files должен отображать целевой путь в исходный")
        for target, source in overrides.items():
            safe_path(target, managed=True)
            safe_path(source, managed=True)
            mapping[target] = source
        check_path_set(mapping)
        check_protected_prefixes(mapping, prefixes)
        baseline = read_json(configs[f"deployment/baselines/{region}.json"]["content"], "baseline")
        validate_baseline(baseline, region)
        policy_overlays = []
        if baseline_repo is None:
            sources = read_git_files(source_repo, source_sha, mapping.values(), entries=entries)
            if set(sources) != set(mapping.values()):
                raise ReleaseError(f"Файлы manifest отсутствуют в коммите: {sorted(set(mapping.values()) - set(sources))[:10]}")
            contents = {target: sources[source] for target, source in mapping.items()}
            actual_sha, source_name, kind = source_sha, manifest.get("source_repository", "SelivanovAS/dashboard"), "program"
        else:
            actual_sha = full_commit(baseline_repo, baseline["source_commit"])
            contents = read_git_files(baseline_repo, actual_sha, baseline["files"])
            if {name: record_of(contents.get(name)) for name in baseline["files"]} != baseline["files"]:
                raise ReleaseError("Начальный снимок не соответствует проверенной описи")
            source_name, kind = profile["repository"], "baseline"
            # A first rollback restores old program bytes but must not revive
            # automatic court probes/collectors triggered by that rollback push.
            policy_overlays = sorted(MANUAL_WORKFLOWS & set(contents))
            if policy_overlays:
                if any(path not in mapping for path in policy_overlays):
                    raise ReleaseError("Нет безопасного workflow для первого отката")
                templates = read_git_files(source_repo, source_sha, [mapping[path] for path in policy_overlays], entries=entries)
                for path in policy_overlays:
                    template = templates.get(mapping[path])
                    if template is None or re.search(rb"(?m)^\s+push\s*:", template["content"]):
                        raise ReleaseError("Откат не может включить автоматические пробы")
                    contents[path] = template
        lock = {"schema_version": SCHEMA, "kind": kind, "source_repo": source_name,
                "source_commit": actual_sha, "region": region, "repository": profile["repository"],
                "site_url": profile.get("site_url"), "vps_path": profile.get("vps_path"),
                "profile_sha256": digest(profile_raw), "baseline_sha256": digest(canonical(baseline)),
                "protected_prefixes": prefixes,
                "files": {name: record_of(item) for name, item in contents.items()}}
        if kind == "baseline":
            lock.update(policy_source_commit=source_sha, retained_workflow_policy=policy_overlays)
        lock["release_id"] = release_id(lock)
        package = {**lock, "baseline": baseline}
        validate_lock(lock)
        destination = out / region
        if destination.exists() or destination.is_symlink():
            raise ReleaseError(f"Каталог пакета уже существует: {destination}")
        prepared.append((destination, package, contents))
    out.mkdir(parents=True, exist_ok=True)
    results = []
    for destination, package, contents in prepared:
        with tempfile.TemporaryDirectory(prefix=".program-build-", dir=out) as tmp:
            stage = Path(tmp) / "package"
            (stage / "files").mkdir(parents=True)
            for name, item in contents.items():
                path = stage / "files" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(item["content"])
                path.chmod(0o755 if item["mode"] == "100755" else 0o644)
            (stage / "package.json").write_bytes(canonical(package))
            load_package(stage)
            stage.rename(destination)
        results.append({"region": package["region"], "package": str(destination),
                        "release_id": package["release_id"], "source_commit": package["source_commit"]})
    return results


def read_installed(repo, commit, region):
    items = read_git_files(repo, commit, [LOCK])
    if LOCK not in items:
        return None
    installed = read_json(items[LOCK]["content"], LOCK)
    validate_lock(installed, region)
    return installed


def check_worktree(repo, names):
    """Only touched paths matter; unrelated dirty/untracked data stay untouched."""
    names = set(names) | {LOCK}
    for name in names:
        fs_path(repo, name)
    raw = git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--", *sorted(names)).stdout
    if raw:
        raise ReleaseError("Управляемые файлы имеют локальные/неотслеживаемые изменения; checkout не тронут")
    entries = tree_entries(repo, "HEAD")
    tracked = read_git_files(repo, "HEAD", names, entries=entries)
    for name in names:
        path = fs_path(repo, name)
        if name not in entries and path.exists():
            raise ReleaseError(f"Посторонний файл занимает управляемый путь: {name}")
        # git status can hide edits under assume-unchanged/skip-worktree and
        # core.filemode=false. The installed bytes, including the lock itself,
        # are the evidence of installation; index flags cannot substitute it.
        actual = None
        if path.exists():
            actual = {"sha256": digest(path.read_bytes()),
                      "mode": "100755" if path.stat().st_mode & stat.S_IXUSR else "100644"}
        if actual != record_of(tracked.get(name)):
            raise ReleaseError(f"Рабочий файл отличается от Git независимо от флагов индекса: {name}")


def _plan(package, repo, commit, *, check_checkout=False):
    installed = read_installed(repo, commit, package["region"])
    expected = installed["files"] if installed else package["baseline"]["files"]
    desired = package["files"]
    names = set(expected) | set(desired)
    if check_checkout:
        check_worktree(repo, names)
    entries = tree_entries(repo, commit)
    for name in names | {LOCK}:
        for parent in PurePosixPath(name).parents:
            if parent.as_posix() in entries:
                raise ReleaseError(f"Родитель управляемого пути занят файлом: {parent}")
    items = read_git_files(repo, commit, names, entries=entries)
    actual = {name: record_of(items.get(name)) for name in names}
    drift = [name for name in names if actual[name] != expected.get(name)]
    if drift:
        raise ReleaseError("Неизвестное изменение программы: " + ", ".join(sorted(drift)[:20]))
    added = sorted(name for name in desired if actual[name] is None)
    modified = sorted(name for name in desired if actual[name] is not None and actual[name] != desired[name])
    deleted = sorted(name for name in expected if expected[name] is not None and name not in desired)
    changed = added + modified + deleted
    # A new protected prefix also applies to files owned by an older release.
    # Removing their entry from the new manifest must never authorize deletion.
    check_protected_prefixes(changed, package.get("protected_prefixes", []))
    worker_required = any(p.startswith("cloudflare-worker/") and not p.endswith("README.md") for p in changed)
    if installed == lock_from_package(package) and not changed:
        # A retry after publish must not erase an outstanding Worker deployment.
        # Its introduction diff is still observable even when local HEAD is old.
        introduced = git_text(repo, "log", "-1", "--format=%H", commit, "--", LOCK)
        parent = git(repo, "rev-parse", "--verify", introduced + "^", check=False) if introduced else None
        if parent is not None and parent.returncode == 0:
            worker_changes = git(repo, "diff", "--name-only", "-z", parent.stdout.decode().strip(), introduced,
                                 "--", "cloudflare-worker").stdout.decode().split("\0")
            worker_required = any(path and not path.endswith("README.md") for path in worker_changes)
        else:
            worker_required = any(path.startswith("cloudflare-worker/") for path in desired)
    return {"region": package["region"], "base_commit": commit, "release_id": package["release_id"],
            "source_commit": package["source_commit"], "bootstrap": installed is None,
            "added": added, "modified": modified, "deleted": deleted,
            "unchanged": installed == lock_from_package(package) and not changed,
            "worker_deploy_required": worker_required, "worker": "deploy_required" if worker_required else "unchanged"}


def plan_release(package_dir, repo, region=None, target_commit=None, check_checkout=True):
    package = load_package(package_dir)
    if region is not None:
        validate_region(region)
        if package["region"] != region:
            raise ReleaseError("Пакет другой территории")
    commit = full_commit(repo, target_commit) if target_commit else git_text(repo, "rev-parse", "HEAD")
    return _plan(package, repo, commit, check_checkout=check_checkout)


def _apply(package, package_dir, clone, plan):
    for name in plan["deleted"]:
        fs_path(clone, name).unlink()
    for name in plan["added"] + plan["modified"]:
        target = fs_path(clone, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(fs_path(Path(package_dir) / "files", name), target)
        target.chmod(0o755 if package["files"][name]["mode"] == "100755" else 0o644)
    fs_path(clone, LOCK).write_bytes(canonical(lock_from_package(package)))
    paths = plan["added"] + plan["modified"] + plan["deleted"] + [LOCK]
    git(clone, "add", "--", *paths)
    for name in plan["added"] + plan["modified"]:
        git(clone, "update-index", "--chmod=" + ("+x" if package["files"][name]["mode"] == "100755" else "-x"), "--", name)
    changed = git(clone, "diff", "--cached", "--name-only", "-z").stdout.decode().split("\0")
    if not set(filter(None, changed)).issubset(paths):
        raise ReleaseError("Индекс содержит изменения вне пакета")
    if git(clone, "diff", "--cached", "--quiet", check=False).returncode == 0:
        return plan["base_commit"]
    git(clone, "-c", "user.name=Court Monitor Release", "-c", "user.email=court-monitor-release@users.noreply.github.com",
        "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "commit", "-m",
        f"Выпуск общей программы: {package['region']} {package['source_commit'][:12]}")
    commit = git_text(clone, "rev-parse", "HEAD")
    before, after = tree_entries(clone, plan["base_commit"]), tree_entries(clone, commit)
    owned = set(paths)
    if {p: v for p, v in before.items() if p not in owned} != {p: v for p, v in after.items() if p not in owned}:
        raise ReleaseError("Коммит изменил защищённую часть дерева")
    actual = read_git_files(clone, commit, package["files"])
    if {name: record_of(actual.get(name)) for name in package["files"]} != package["files"]:
        raise ReleaseError("Git-фильтры изменили содержимое пакета; публикация остановлена")
    if read_installed(clone, commit, package["region"]) != lock_from_package(package):
        raise ReleaseError("Git изменил опись установленной программы")
    return commit


def repository_name(value, *, require_ssh=False):
    if not isinstance(value, str) or any(c.isspace() for c in value):
        raise ReleaseError("Некорректный адрес репозитория")
    match = re.fullmatch(r"git@([^:]+):(.+)", value)
    if match:
        host, name = match.groups()
    elif "://" in value:
        parsed = urlparse(value)
        if require_ssh and (parsed.scheme != "ssh" or parsed.username != "git" or parsed.password):
            raise ReleaseError("Публикация разрешена только через SSH Git")
        host, name = parsed.hostname, parsed.path.lstrip("/")
    else:
        if require_ssh:
            raise ReleaseError("Для --push нужен явный SSH URL")
        host, name = "github.com", value
    if host not in {"github.com", "ssh.github.com"}:
        raise ReleaseError("Ожидался существующий репозиторий GitHub")
    name = name.removesuffix(".git")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name):
        raise ReleaseError("Некорректное имя репозитория GitHub")
    return name.lower()


def data_only_path(name):
    parts = PurePosixPath(name).parts
    return bool(parts and (parts[0] == "data" or
                (parts[0] == "ops" and len(parts) > 1 and parts[1] in REPORT_DIRS)))


def check_remote_advance(clone, initial, remote_head, package):
    if initial == remote_head:
        return
    installed = read_installed(clone, remote_head, package["region"])
    if installed == lock_from_package(package):
        _plan(package, clone, remote_head)
        return
    if git(clone, "merge-base", "--is-ancestor", initial, remote_head, check=False).returncode:
        raise ReleaseError("Удалённая история разошлась; выпуск остановлен")
    changed = list(filter(None, git(clone, "diff", "--name-only", "-z", initial, remote_head).stdout.decode().split("\0")))
    if any(not data_only_path(path) for path in changed):
        raise ReleaseError("На сервере появились изменения программы/настроек; обновите checkout")


def install_vps(result, package, host, identity):
    if host is None:
        result.update(install_pending=True, status="published_not_installed" if result["published"] else "prepared")
        return
    helper = Path(__file__).with_name("program_install_vps.py")
    if not helper.is_file():
        result.update(status="published_not_installed", install_pending=True, install_error="Отсутствует program_install_vps.py")
        return
    command = [sys.executable, str(helper), "--host", host, "--region", package["region"],
               "--commit", result["commit"], "--expected-source", package["source_commit"]]
    if identity:
        command += ["--identity", str(identity)]
    cp = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        report = json.loads(cp.stdout)
    except ValueError:
        report = {"error": "Установщик не вернул JSON", "exit_code": cp.returncode}
    result["vps"] = report
    success = cp.returncode == 0 and report.get("installed") is True
    result["install_pending"] = not success
    result["status"] = ("published_not_installed" if not success else
                        "worker_deploy_pending" if result["worker_deploy_required"] else "installed")
    if not success:
        result["install_error"] = "Установка VPS не подтверждена; опубликованный коммит сохранён"


def promote_release(package_dir, repo, region, *, push=False, remote=None, branch="main", vps_host=None, ssh_key=None, rollback=False):
    package_dir, repo = Path(package_dir).resolve(), Path(repo).resolve()
    package = load_package(package_dir)
    validate_region(region)
    if package["region"] != region:
        raise ReleaseError("Пакет другой территории")
    if branch != "main":
        raise ReleaseError("Рабочая ветка выпуска — main")
    if vps_host and not push:
        raise ReleaseError("Установка VPS требует опубликованного коммита (--push)")
    if push and repository_name(remote, require_ssh=True) != repository_name(package["repository"]):
        raise ReleaseError("SSH URL не соответствует территории пакета")
    initial = git_text(repo, "rev-parse", "HEAD")
    local_installed = read_installed(repo, initial, region)
    if rollback and local_installed is None and not push:
        raise ReleaseError("Откат требует установленной программы")
    check_worktree(repo, set(package["files"]) |
                   set(local_installed["files"] if local_installed else package["baseline"]["files"]))
    with tempfile.TemporaryDirectory(prefix="court-program-promote-") as tmp:
        clone = Path(tmp) / "repo"
        git(repo, "clone", "--no-local", "--no-checkout", str(repo), str(clone))
        base = initial
        if push:
            git(clone, "fetch", "--no-tags", remote, "refs/heads/main")
            base = git_text(clone, "rev-parse", "FETCH_HEAD")
            check_remote_advance(clone, initial, base, package)
        if rollback and read_installed(clone, base, region) is None:
            raise ReleaseError("Откат требует установленной программы")
        for attempt in range(3):
            git(clone, "-c", "core.hooksPath=/dev/null", "checkout", "--detach", base)
            plan = _plan(package, clone, base)
            target = base if plan["unchanged"] else _apply(package, package_dir, clone, plan)
            if not push:
                reference = None
                if target != base:
                    # Include the target base: a new data commit prepares a new safe candidate.
                    reference = f"refs/program-release/prepared/{region}/{package['release_id']}/{base}"
                    git(repo, "fetch", "--no-tags", str(clone), target)
                    previous = git(repo, "rev-parse", "--verify", reference, check=False)
                    if previous.returncode == 0:
                        previous_sha = previous.stdout.decode().strip()
                        if git_text(repo, "rev-parse", previous_sha + "^{tree}") != git_text(repo, "rev-parse", target + "^{tree}"):
                            raise ReleaseError("Ранее подготовленный выпуск отличается")
                        target = previous_sha
                    else:
                        git(repo, "update-ref", reference, target, "0" * len(target))
                result = {**plan, "commit": target, "prepared_ref": reference,
                          "published": False, "operation": "rollback" if rollback else "promote",
                          "checkout_unchanged": True}
                install_vps(result, package, None, None)
                return result
            if target == base:
                # main may already contain newer parser data after publication.
                # Give the installer the code-only introduction commit; it will
                # independently verify ancestry/same lock and install latest data.
                introduced = git_text(clone, "log", "-1", "--format=%H", base, "--", LOCK)
                if not introduced:
                    raise ReleaseError("Не найден коммит публикации установленного выпуска")
                result = {**plan, "commit": introduced, "remote_commit": base,
                          "published": True, "checkout_unchanged": True,
                          "operation": "rollback" if rollback else "promote"}
                install_vps(result, package, vps_host, ssh_key)
                return result
            pushed = git(clone, "push", remote, target + ":refs/heads/main", check=False)
            # Verify remote even after timeout: push may have reached GitHub.
            fetched = git(clone, "fetch", "--no-tags", remote, "refs/heads/main", check=False)
            if fetched.returncode:
                raise ReleaseError(f"Результат push не подтверждён; проверьте удалённый коммит {target} перед повтором")
            newest = git_text(clone, "rev-parse", "FETCH_HEAD")
            if git(clone, "merge-base", "--is-ancestor", target, newest, check=False).returncode == 0:
                _plan(package, clone, newest)
                result = {**plan, "commit": target, "remote_commit": newest, "published": True,
                          "checkout_unchanged": True, "operation": "rollback" if rollback else "promote"}
                install_vps(result, package, vps_host, ssh_key)
                return result
            check_remote_advance(clone, base, newest, package)
            if newest == base:
                raise ReleaseError(f"SSH push отклонён (код {pushed.returncode}); удалённая ветка не изменена")
            base = newest
        raise ReleaseError("Данные изменились во всех трёх попытках публикации; повторите позднее")


def verify_release(repo, region=None, package_dir=None):
    repo = Path(repo).resolve()
    commit = git_text(repo, "rev-parse", "HEAD")
    items = read_git_files(repo, commit, [LOCK])
    if LOCK not in items:
        raise ReleaseError("В checkout нет установленного выпуска")
    installed = read_json(items[LOCK]["content"], LOCK)
    validate_lock(installed, region)
    check_worktree(repo, installed["files"])
    actual = read_git_files(repo, commit, installed["files"])
    if {name: record_of(actual.get(name)) for name in installed["files"]} != installed["files"]:
        raise ReleaseError("Установленные файлы отличаются от описи выпуска")
    if package_dir is not None and lock_from_package(load_package(package_dir)) != installed:
        raise ReleaseError("Установлен другой пакет")
    return {"status": "verified_checkout", "region": installed["region"], "commit": commit,
            "source_commit": installed["source_commit"], "release_id": installed["release_id"],
            "files_verified": len(installed["files"]), "online_verified": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="Точный коммит; --baseline-repo готовит первый откат")
    build.add_argument("--source-sha", required=True)
    build.add_argument("--source-repo", type=Path, default=Path(__file__).resolve().parents[1])
    build.add_argument("--out", type=Path, required=True)
    build.add_argument("--regions", help="Коды через запятую; по умолчанию четыре территории")
    build.add_argument("--baseline-repo", type=Path)
    for name in ("plan", "promote", "rollback", "verify"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--repo", required=True, type=Path)
        cmd.add_argument("--region", choices=REGIONS, required=name != "verify")
        cmd.add_argument("--package", required=name != "verify", type=Path)
        if name in {"promote", "rollback"}:
            cmd.add_argument("--push", action="store_true")
            cmd.add_argument("--remote", help="Явный SSH URL соответствующего репозитория")
            cmd.add_argument("--vps-host", help="После push вызвать отдельный установщик VPS")
            cmd.add_argument("--ssh-key", type=Path, help="SSH identity для установщика VPS")
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            result = build_release(args.source_repo, args.source_sha, args.out,
                                   args.regions.split(",") if args.regions else None, args.baseline_repo)
        elif args.command == "plan":
            result = plan_release(args.package, args.repo, args.region)
        elif args.command == "verify":
            result = verify_release(args.repo, args.region, args.package)
        else:
            result = promote_release(args.package, args.repo, args.region, push=args.push,
                                     remote=args.remote, vps_host=args.vps_host, ssh_key=args.ssh_key,
                                     rollback=args.command == "rollback")
        print(canonical(result).decode(), end="")
        return 2 if isinstance(result, dict) and result.get("install_error") else 0
    except (ReleaseError, OSError) as exc:
        print(canonical({"status": "blocked", "error": str(exc)}).decode(), end="", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
