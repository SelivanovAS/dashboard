#!/usr/bin/env python3
"""Public release orchestration over a held, pinned maintenance coordinator.

Local operation files survive SSH loss. No public push exists outside the exact
publish permit callback. Ambiguous operations leave admission closed for recover.
"""
from __future__ import annotations

import base64
import copy
import json
import os
from pathlib import Path
import secrets
import shutil
import stat

import program_release as release
import program_maintenance_bundle as bundles
import program_maintenance_git as git_proof
from program_maintenance_protocol import LeaseClient, MaintenanceProtocolError


class ClientError(release.ReleaseError):
    pass


def _directory(path, *, create=False):
    path = Path(path).absolute()
    if ".." in path.parts or any(ord(c) < 32 or ord(c) == 127 for c in str(path)):
        raise ClientError("Нужен безопасный абсолютный каталог операции")
    git_proof._no_symlinks(path)
    if create:
        path.mkdir(mode=0o700)
        git_proof._sync_dir(path.parent)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ClientError("Каталог операции должен принадлежать пользователю и иметь права 0700")
    return path


def _read(path):
    git_proof._no_symlinks(path)
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 32 * 1024 * 1024):
        raise ClientError("Неизвестный файл операции")
    return release.read_json(path.read_bytes(), "журнал локальной операции")


def _save(path, value, *, immutable=False):
    git_proof._no_symlinks(path)
    if path.exists() or path.is_symlink():
        previous = _read(path)
        if immutable:
            if previous != value:
                raise ClientError("Сохранённая операция имеет другие параметры")
            return
    temporary = path.parent / (".write-" + secrets.token_hex(16))
    git_proof._write_exclusive(temporary, release.canonical(value))
    os.replace(temporary, path)
    git_proof._sync_dir(path.parent)


def _copy_package(package_dir, operation, config):
    package = release.load_package(package_dir)
    lock = release.lock_from_package(package)
    if config["manifests"].get(package["release_id"]) != lock:
        raise ClientError("Пакет отсутствует в закреплённой карте разрешённых выпусков")
    root = operation / "packages"
    if not root.exists():
        _directory(root, create=True)
    _directory(root)
    folder = root / package["release_id"]
    if folder.exists() or folder.is_symlink():
        _directory(folder)
        if release.load_package(folder) != package:
            raise ClientError("Сохранённый пакет отличается")
        return package, folder
    _directory(folder, create=True)
    files = folder / "files"
    files.mkdir(mode=0o700)
    for name, record in package["files"].items():
        destination = release.fs_path(files, name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        raw = release.fs_path(Path(package_dir) / "files", name).read_bytes()
        with destination.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        destination.chmod(0o755 if record["mode"] == "100755" else 0o644)
    for directory in sorted([p for p in files.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
        git_proof._sync_dir(directory)
    git_proof._sync_dir(files)
    _save(folder / "package.json", package, immutable=True)
    git_proof._sync_dir(folder)
    release.load_package(folder)
    return package, folder


def remote_git(repo, remote, operation, revision=None):
    """Only pinned SSH remote and non-force fetch/push, with no Git rewrites."""
    release.repository_name(remote, require_ssh=True)
    env = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
           "GIT_SSH_COMMAND": "ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=2 -p 443 -o HostName=ssh.github.com"}
    rewrites = git_proof._git(repo, "config", "--get-regexp", r"^url\..*\.(insteadof|pushinsteadof)$", env=env, check=False)
    if rewrites.returncode not in (0, 1) or rewrites.returncode == 0:
        raise ClientError("Git URL rewrite не допускается для публикации")
    prefix = ("-c", "protocol.allow=never", "-c", "protocol.ssh.allow=always", "-c", "gc.auto=0", "-c", "maintenance.auto=false")
    if operation == "fetch":
        cp = git_proof._git(repo, *prefix, "fetch", "--no-tags", "--no-recurse-submodules", remote,
                           "refs/heads/main", env=env, check=False)
        if cp.returncode:
            raise ClientError("Удалённый результат не подтверждён; подробности SSH скрыты")
        git_proof._assert_complete_history(repo)
        return git_proof._text(repo, "rev-parse", "FETCH_HEAD")
    if operation != "push" or not release.COMMIT.fullmatch(str(revision)):
        raise ClientError("Неизвестная операция публикации")
    return git_proof._git(repo, *prefix, "push", remote, revision + ":refs/heads/main", env=env, check=False).returncode


def _config(path, package):
    value = release.read_json(Path(path).read_bytes(), "конфигурация окна")
    if (set(value) != bundles.CONFIG_KEYS or value["schema_version"] != 1
            or set(value["profiles"]) != set(release.REGIONS)
            or value["manifests"].get(package["release_id"]) != release.lock_from_package(package)):
        raise ClientError("Нужна полная конфигурация четырёх территорий с точным паспортом пакета")
    for region, profile in value["profiles"].items():
        release.validate_region(region)
        if set(profile) != {"path", "repository", "remote", "baseline"}:
            raise ClientError("Неполный профиль VPS")
        for field in ("state_dir", "config_dir"):
            path = Path(value[field])
            if not path.is_absolute() or ".." in path.parts or str(path) != value[field]:
                raise ClientError("Нужен абсолютный путь VPS")
        if not Path(profile["path"]).is_absolute() or ".." in Path(profile["path"]).parts:
            raise ClientError("Нужен абсолютный путь регионального клона VPS")
    return value


def _connect(operation, spec, config, *, bootstrap=False):
    bundle = _read(operation / "bundle.json")
    if bundle["bundle_sha256"] != spec["bundle_sha256"] or bundle["source_commit"] != spec["coordinator_sha"]:
        raise ClientError("Изменился закреплённый координатор операции")
    source = bundles.bootstrap_source(bundle, config, bootstrap_guard=bootstrap)
    return LeaseClient.ssh(source, host=spec["vps_host"], identity=spec["ssh_key"], timeout=900)


def _candidate_repo(operation):
    clone = _directory(operation / "candidate")
    git_proof._repo(clone)
    expected = _read(operation / "candidate-identity.json")
    actual = {name: {"dev": (clone / name).stat().st_dev, "ino": (clone / name).stat().st_ino}
              for name in (".", ".git")}
    if expected != actual:
        raise ClientError("Каталог подготовленного Git-клона был заменён")
    git_proof._assert_complete_history(clone)
    return clone


def _open(operation, spec, config, bootstrap):
    client = _connect(operation, spec, config, bootstrap=False)
    try:
        state = client.request("inspect")
        journal = state["journal"]
        eligible = (journal is None or (journal["phase"] == "complete" and not state["blocked"] and journal["nonce"] != spec["nonce"])
                    or (journal["nonce"] == spec["nonce"] and journal["phase"] == "reserved"
                        and journal["snapshot"] is None and not journal["attempts"]))
        if bootstrap and eligible:
            client.close()
            client = _connect(operation, spec, config, bootstrap=True)
        return client
    except BaseException:
        client.close()
        raise


def _checkpoint(operation, journal):
    if journal is None:
        return
    old_path = operation / "remote-journal.json"
    if old_path.exists():
        old = _read(old_path)
        if old["nonce"] != journal["nonce"] or journal["attempts"][:len(old["attempts"])] != old["attempts"]:
            raise ClientError("Сервер потерял ранее выданные намерения публикации")
    _save(old_path, journal)


def _known_journal(spec, config, journal):
    if (not isinstance(journal, dict) or journal.get("nonce") != spec["nonce"]
            or journal.get("region") != spec["region"] or journal.get("protocol") != "court-program-maintenance/2"):
        raise ClientError("VPS удерживает другую операцию; её журнал нельзя заменять")
    for target in journal["targets"]:
        manifest = config["manifests"].get(target["release_id"])
        if (manifest is None or manifest["source_commit"] != target["source_commit"]
                or release.digest(release.canonical(manifest)) != target["manifest_sha256"]):
            raise ClientError("Серверная цель не входит в сохранённую карту пакетов")


def _result(operation, spec, journal, *, status, error=None):
    result = {"status": status, "region": spec["region"], "operation": spec["operation"],
              "operation_dir": str(operation), "nonce": spec["nonce"], "checkout_unchanged": True,
              "published": status == "installed",
              "publication_uncertain": status == "recovery_required",
              "install_pending": status not in ("installed", "cancelled"),
              "recovery_command": ["python3", "scripts/program_release.py", "recover", "--operation-dir", str(operation)]}
    if journal:
        result.update(phase=journal["phase"], attempts=journal["attempts"], release_id=journal["release_id"],
                      source_commit=journal["source_commit"], verification=journal.get("verification"),
                      safe_evidence=journal.get("safe_evidence"))
        if journal.get("verification"):
            result["commit"] = journal["verification"]["installed_sha"]
    if error:
        result["install_error"] = error
    _save(operation / "result.json", result)
    return result


def _finish(client, operation, spec, journal):
    if journal["phase"] in ("publish_intent", "applying"):
        journal = client.request("apply", nonce=spec["nonce"], attempt_id=journal["active_attempt_id"])
        _checkpoint(operation, journal)
    if journal["phase"] == "applying":
        journal = client.request("mark_verified", nonce=spec["nonce"])
        _checkpoint(operation, journal)
    if journal["phase"] in ("verified", "safe_to_resume", "complete"):
        journal = client.request("finish", nonce=spec["nonce"])
        _checkpoint(operation, journal)
    if journal["phase"] != "complete" or journal["outcome"] != "installed":
        raise ClientError("Установка и безопасный выход не подтверждены")
    return _result(operation, spec, journal, status="installed")


def _publish(client, operation, spec, config, journal, package, folder):
    clone = _candidate_repo(operation)
    initial = journal["snapshot"][spec["region"]]["head"]
    active = release.lock_from_package(package)
    for _ in range(3):
        newest = remote_git(clone, spec["remote"], "fetch")
        # A prior uncertain push may already be published. It must be one of our
        # durable intents, never inferred from an identical program alone.
        if journal["attempts"] and journal["attempts"][-1]["target_id"] == journal["active_target_id"]:
            last = journal["attempts"][-1]
            if git_proof._ancestor(clone, last["target_sha"], newest):
                if release.read_installed(clone, newest, spec["region"]) != active:
                    raise ClientError("После публикации изменилась программа удалённой ветки")
                return _finish(client, operation, spec, journal)
            release.check_remote_advance(clone, last["base_sha"], newest, package)
        else:
            # Recovery target may follow a prior approved program; _plan checks
            # its actual manifest and the host independently validates ancestry.
            if journal["active_target_id"] == 1:
                release.check_remote_advance(clone, initial, newest, package)
        git_proof._git(clone, "checkout", "--detach", newest)
        plan = release._plan(package, clone, newest)
        if plan["unchanged"]:
            if not journal["attempts"]:
                raise ClientError("Пакет уже опубликован без соответствующего намерения; нужен разбор исходной операции")
            # An older permitted push may win while a newer permitted sibling
            # loses. A single empty confirmation commit above the actual main
            # fences every sibling without rewinding history or touching data.
            git_proof._git(clone, "-c", "user.name=Court Monitor Release", "-c", "user.email=court-monitor-release@users.noreply.github.com",
                           "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "Подтверждение проверенного выпуска общей программы")
            target = git_proof._text(clone, "rev-parse", "HEAD")
        else:
            target = release._apply(package, folder, clone, plan)
        candidate = {"target_id": journal["active_target_id"], "base_sha": newest, "target_sha": target,
                     "release_id": package["release_id"]}
        root = operation / "candidates"
        if not root.exists(): _directory(root, create=True)
        _save(root / (target + ".json"), candidate, immutable=True)
        bundle = bundles.build_target_bundle(clone, newest, target)
        client.register_target(nonce=spec["nonce"], base_sha=newest, target_sha=target,
                               bundle=base64.b64decode(bundle["bundle_base64"], validate=True))
        # Only LeaseClient.publish can reach this callback, after exact ACK.
        def permitted_push(sha):
            # Persist the acknowledged intent locally before starting Git. A
            # lost push reply must not leave only a pre-publication checkpoint.
            permitted = client.request("inspect")["journal"]
            _known_journal(spec, config, permitted)
            if permitted["attempts"][-1]["target_sha"] != sha:
                raise ClientError("Намерение публикации изменилось до push")
            _checkpoint(operation, permitted)
            return remote_git(clone, spec["remote"], "push", sha)
        client.publish(nonce=spec["nonce"], base_sha=newest, target_sha=target, push=permitted_push)
        observed = client.request("inspect")["journal"]
        _known_journal(spec, config, observed)
        journal = observed
        _checkpoint(operation, journal)
        remote = remote_git(clone, spec["remote"], "fetch")
        if git_proof._ancestor(clone, target, remote):
            if release.read_installed(clone, remote, spec["region"]) != active:
                raise ClientError("Удалённая программа изменилась после разрешённого push")
            return _finish(client, operation, spec, journal)
        release.check_remote_advance(clone, newest, remote, package)
        if remote == newest:
            raise ClientError("Push отклонён; разрешённая цель сохранена для recover")
    raise ClientError("Три попытки встретили новые данные; продолжайте через recover с тем же журналом")


def _run(operation, spec, config, *, bootstrap=False, rollback_package=None, cancel=False):
    journal = None
    try:
        with _open(operation, spec, config, bootstrap) as client:
            observed = client.request("inspect")
            journal = observed["journal"]
            if journal is None or (journal["phase"] == "complete" and not observed["blocked"] and journal["nonce"] != spec["nonce"]):
                if (operation / "remote-journal.json").exists():
                    raise ClientError("Серверный журнал завершён другой операцией; повторная публикация запрещена")
                lock = config["manifests"][spec["release_id"]]
                journal = client.request("reserve", nonce=spec["nonce"], region=spec["region"],
                    release_id=lock["release_id"], source_commit=lock["source_commit"], manifest_sha256=release.digest(release.canonical(lock)))
                _checkpoint(operation, journal)
                journal = client.request("prepare", nonce=spec["nonce"])
            else:
                _known_journal(spec, config, journal)
                journal = client.request("recover", nonce=spec["nonce"])
                if journal["phase"] == "reserved":
                    journal = client.request("prepare", nonce=spec["nonce"])
            _known_journal(spec, config, journal)
            _checkpoint(operation, journal)
            if journal["phase"] == "complete" and journal["outcome"] == "cancelled":
                return _result(operation, spec, journal, status="cancelled")
            if cancel:
                if journal["attempts"]:
                    raise ClientError("После разрешения push отмена запрещена; завершите установку или выберите пакет отката")
                journal = client.request("cancel_before_publish", nonce=spec["nonce"])
                _checkpoint(operation, journal)
                return _result(operation, spec, journal, status="cancelled")
            if rollback_package is not None:
                package, folder = _copy_package(rollback_package, operation, config)
                if package["region"] != spec["region"]:
                    raise ClientError("Пакет отката другой территории")
                if not journal["attempts"]:
                    raise ClientError("До первого намерения следует отменить окно и начать отдельный выпуск")
                if journal["release_id"] != package["release_id"]:
                    lock = release.lock_from_package(package)
                    journal = client.request("select_recovery_target", nonce=spec["nonce"], release_id=lock["release_id"],
                        source_commit=lock["source_commit"], manifest_sha256=release.digest(release.canonical(lock)), reason="rollback")
                    _checkpoint(operation, journal)
            folder = operation / "packages" / journal["release_id"]
            _directory(operation / "packages")
            _directory(folder)
            package = release.load_package(folder)
            if journal["phase"] in ("applying", "verified", "safe_to_resume", "complete"):
                return _finish(client, operation, spec, journal)
            return _publish(client, operation, spec, config, journal, package, folder)
    except (release.ReleaseError, git_proof.MaintenanceGitError, MaintenanceProtocolError, OSError, ValueError) as exc:
        # No cancel/finish in exception handlers: an ACK/push may already exist.
        # Raw subprocess/server diagnostics may contain secrets; omit them.
        if (operation / "remote-journal.json").exists():
            saved = _read(operation / "remote-journal.json")
            _known_journal(spec, config, saved)
            journal = saved
        return _result(operation, spec, journal, status="recovery_required",
                       error="Операция не завершена. Защита VPS могла остаться закрытой; используйте recover с этим каталогом.")


def _verify_no_publication(operation, spec, config, newest=None):
    # This operation never reserved a lease or issued a publication intent.
    # Repeating it remains a fresh read-only verification, including recover.
    if (operation / "remote-journal.json").exists():
        raise ClientError("Операция уже содержит серверный журнал")
    clone = _candidate_repo(operation)
    newest = newest or remote_git(clone, spec["remote"], "fetch")
    lock = config["manifests"][spec["release_id"]]
    if release.read_installed(clone, newest, spec["region"]) != lock:
        raise ClientError("После проверки без публикации установленная программа изменилась")
    release.verify_git_snapshot(clone, newest, lock)
    proof = release.verify_vps(lock, spec["vps_host"], spec["ssh_key"])
    _save(operation / "no-publication.json", {"release_id": spec["release_id"], "nonce": spec["nonce"]}, immutable=True)
    result = {"status": "installed", "unchanged": True, "published": True, "install_pending": False,
              "region": spec["region"], "release_id": spec["release_id"], "commit": newest,
              "operation_dir": str(operation), "checkout_unchanged": True, "vps": proof}
    _save(operation / "result.json", result)
    return result


def promote(package_dir, repo, region, *, remote, vps_host, ssh_key, maintenance_config,
            coordinator_sha, operation_dir, bootstrap_guard=False, rollback=False, source_repo=None):
    package = release.load_package(package_dir)
    release.validate_region(region)
    if package["region"] != region or release.repository_name(remote, require_ssh=True) != release.repository_name(package["repository"]):
        raise ClientError("Территория, пакет и SSH remote должны совпадать")
    repo = Path(repo).resolve()
    git_proof._assert_complete_history(repo)
    if not vps_host or not ssh_key or not Path(ssh_key).is_file():
        raise ClientError("Для общего выпуска нужны существующие VPS host и SSH key")
    config = _config(maintenance_config, package)
    if config["profiles"][region]["remote"] != remote:
        raise ClientError("Remote отличается от закреплённого профиля VPS")
    operation = Path(operation_dir).absolute()
    if operation.exists():
        saved = _read(_directory(operation) / "operation.json")
        if (saved["region"] != region or saved["release_id"] != package["release_id"]
                or saved["remote"] != remote or saved["vps_host"] != vps_host
                or saved["coordinator_sha"] != coordinator_sha or _read(operation / "config.json") != config):
            raise ClientError("Каталог уже принадлежит другому выпуску")
        return recover(operation)
    source_repo = Path(source_repo or Path(__file__).resolve().parents[1])
    bundle = bundles.build_bundle(source_repo, coordinator_sha)
    initial = git_proof._text(repo, "rev-parse", "HEAD")
    installed = release.read_installed(repo, initial, region)
    if installed is None:
        raise ClientError("Общее окно требует уже управляемой установки; первый переход использует прежний ограниченный путь")
    release.check_worktree(repo, set(installed["files"]) | set(package["files"]))
    _directory(operation, create=True)
    spec = {"schema_version": 1, "nonce": secrets.token_hex(16), "region": region,
            "release_id": package["release_id"], "remote": remote, "vps_host": vps_host,
            "ssh_key": str(Path(ssh_key).resolve()), "coordinator_sha": coordinator_sha,
            "bundle_sha256": bundle["bundle_sha256"], "operation": "rollback" if rollback else "promote",
            "initial_sha": initial, "bootstrap_guard": bool(bootstrap_guard)}
    _save(operation / "operation.json", spec, immutable=True)
    _save(operation / "bundle.json", bundle, immutable=True)
    _save(operation / "config.json", config, immutable=True)
    _copy_package(package_dir, operation, config)
    clone = operation / "candidate"
    git_proof._git(repo, "clone", "--no-local", "--no-checkout", str(repo), str(clone))
    clone.chmod(0o700)
    _save(operation / "candidate-identity.json", {name: {"dev": (clone / name).stat().st_dev, "ino": (clone / name).stat().st_ino}
                                              for name in (".", ".git")}, immutable=True)
    _candidate_repo(operation)
    newest = remote_git(clone, remote, "fetch")
    release.check_remote_advance(clone, initial, newest, package)
    if release.read_installed(clone, newest, region) == release.lock_from_package(package):
        return _verify_no_publication(operation, spec, config, newest)
    return _run(operation, spec, config, bootstrap=bootstrap_guard)


def recover(operation_dir, *, rollback_package=None, cancel=False):
    operation = _directory(operation_dir)
    spec, config = _read(operation / "operation.json"), _read(operation / "config.json")
    if spec.get("schema_version") != 1 or set(config) != bundles.CONFIG_KEYS:
        raise ClientError("Неизвестный формат сохранённой операции")
    if rollback_package is not None and cancel:
        raise ClientError("Отмена и откат несовместимы")
    if (operation / "no-publication.json").exists():
        if _read(operation / "no-publication.json") != {"release_id": spec["release_id"], "nonce": spec["nonce"]}:
            raise ClientError("Изменилась запись проверки без публикации")
        if rollback_package is not None or cancel:
            raise ClientError("Проверка без публикации не создаёт окна для отмены/отката")
        return _verify_no_publication(operation, spec, config)
    return _run(operation, spec, config, bootstrap=spec["bootstrap_guard"],
                rollback_package=rollback_package, cancel=cancel)
