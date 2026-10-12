#!/usr/bin/env python3
"""Точный независимый bundle координатора. Сам по себе не включает promote.

Only committed, explicitly enumerated Python modules are transferred over the
held protocol stdin. The remote interpreter never imports the regional clone.
Transfer writes only the private coordinator store. Permanent guard bootstrap is
explicit; regional writes require a separate recorded protocol operation.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import zlib

import program_release as release
from program_maintenance_protocol import MAX_BOOTSTRAP, PROTOCOL


MODULES = (
    "program_release.py", "program_install_vps.py", "program_maintenance.py",
    "program_maintenance_protocol.py", "program_maintenance_git.py",
    "program_maintenance_systemd.py", "program_maintenance_locks.py",
    "program_maintenance_host.py",
)
CONFIG_KEYS = {"schema_version", "state_dir", "config_dir", "profiles", "manifests"}
MAX_TARGET_BUNDLE = 512 * 1024


class MaintenanceBundleError(RuntimeError):
    pass


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode()


def build_target_bundle(repo, base_sha, target_sha):
    """Transfer every candidate before its permit, including rejected retries.

    A release creates exactly one commit over the latest data. The small thin
    bundle retains that otherwise unreachable commit on the coordinator even
    when a racing data commit causes its ordinary push to be rejected.
    Candidates above the explicit transport bound stop BEFORE publication.
    """
    import program_maintenance_git as maintenance_git
    maintenance_git._assert_complete_history(repo)
    maintenance_git._commit(repo, base_sha)
    maintenance_git._commit(repo, target_sha)
    parents = maintenance_git._text(repo, "rev-list", "--parents", "-n", "1", target_sha).split()
    if parents != [target_sha, base_sha]:
        raise MaintenanceBundleError("Нужен один коммит выпуска поверх точного base")
    # Git bundle requires an advertised named ref. Keep a deterministic owned
    # ref so the same candidate has the same header and remains reachable even
    # after the local release branch advances or its push is rejected.
    reference = "refs/program-maintenance/targets/" + target_sha
    previous = maintenance_git._git(repo, "rev-parse", "--verify", reference, check=False)
    if previous.returncode:
        maintenance_git._git(repo, "update-ref", reference, target_sha, "0" * len(target_sha))
    elif previous.stdout.decode().strip() != target_sha:
        raise MaintenanceBundleError("Ссылка сохранённого кандидата была изменена")
    with tempfile.TemporaryDirectory(prefix="court-target-bundle-") as temporary:
        path = Path(temporary) / "target.bundle"
        maintenance_git._git(repo, "bundle", "create", str(path), reference, "^" + base_sha)
        raw = path.read_bytes()
    if not raw or len(raw) > MAX_TARGET_BUNDLE:
        raise MaintenanceBundleError("Пакет Git превышает 512 KiB; публикация не разрешена")
    return {"base_sha": base_sha, "target_sha": target_sha,
            "bundle_sha256": hashlib.sha256(raw).hexdigest(),
            "bundle_base64": base64.b64encode(raw).decode("ascii")}


def build_bundle(repo, source_commit):
    """Read only exact Git blobs; dirty worktree files cannot change the bundle."""
    release.full_commit(repo, source_commit)
    paths = ["scripts/" + name for name in MODULES]
    entries = release.tree_entries(repo, source_commit)
    files = {}
    for name, path in zip(MODULES, paths):
        entry = entries.get(path)
        if entry is None or entry[0] not in release.MODES:
            raise MaintenanceBundleError("В точном коммите отсутствует обычный модуль координатора")
        raw = release.git(repo, "show", source_commit + ":" + path).stdout
        if not raw or len(raw) > MAX_BOOTSTRAP:
            raise MaintenanceBundleError("Некорректный размер модуля координатора")
        try:
            compile(raw, name, "exec")
        except (SyntaxError, ValueError) as exc:
            raise MaintenanceBundleError("Некорректный Python-модуль координатора") from exc
        files[name] = {"sha256": hashlib.sha256(raw).hexdigest(),
                       "content": base64.b64encode(raw).decode("ascii")}
    hashes = {name: record["sha256"] for name, record in files.items()}
    return {"schema_version": 1, "source_commit": source_commit,
            "bundle_sha256": hashlib.sha256(canonical(hashes)).hexdigest(), "files": files}


def bootstrap_source(bundle, config, *, bootstrap_guard=False):
    """Produce constant-loader source; configuration is data, never shell text."""
    if type(bootstrap_guard) is not bool:
        raise MaintenanceBundleError("Нужен явный режим bootstrap")
    if not isinstance(config, dict) or set(config) != CONFIG_KEYS:
        raise MaintenanceBundleError("Неизвестная конфигурация bundle")
    config = copy.deepcopy(config)
    state = config["state_dir"]
    if (not isinstance(state, str) or not state.startswith("/")
            or str(Path(state)) != state or any(part in {".", ".."} for part in state.split("/"))
            or any(ord(char) < 32 for char in state)):
        raise MaintenanceBundleError("Нужен абсолютный безопасный каталог состояния")
    if not isinstance(bundle, dict) or set(bundle) != {"schema_version", "source_commit", "bundle_sha256", "files"}:
        raise MaintenanceBundleError("Неизвестный формат bundle")
    if bundle["schema_version"] != 1 or not release.COMMIT.fullmatch(bundle["source_commit"]):
        raise MaintenanceBundleError("Неизвестный исходник bundle")
    files = bundle["files"]
    if not isinstance(files, dict) or set(files) != set(MODULES):
        raise MaintenanceBundleError("Неполный или посторонний модуль bundle")
    hashes = {}
    for name, record in files.items():
        if not isinstance(record, dict) or set(record) != {"sha256", "content"}:
            raise MaintenanceBundleError("Неизвестная запись bundle")
        try:
            raw = base64.b64decode(record["content"], validate=True)
        except (ValueError, TypeError) as exc:
            raise MaintenanceBundleError("Повреждённый модуль bundle") from exc
        if not raw or hashlib.sha256(raw).hexdigest() != record["sha256"]:
            raise MaintenanceBundleError("Хеш модуля bundle не совпал")
        hashes[name] = record["sha256"]
    if hashlib.sha256(canonical(hashes)).hexdigest() != bundle["bundle_sha256"]:
        raise MaintenanceBundleError("Хеш bundle не совпал")
    folder = Path(state) / "bundles" / bundle["bundle_sha256"]
    config["capability"] = {"schema_version": 1, "protocol": PROTOCOL,
        "source_commit": bundle["source_commit"], "bundle_sha256": bundle["bundle_sha256"],
        "bundle_files": {str(folder / name): digest for name, digest in hashes.items()}}
    payload = canonical({"bundle": bundle, "config": config, "bootstrap_guard": bootstrap_guard})
    if len(payload) > 8 * MAX_BOOTSTRAP:
        raise MaintenanceBundleError("Слишком большой bundle с конфигурацией")
    encoded = base64.b64encode(zlib.compress(payload, level=9)).decode("ascii")
    result = ("PAYLOAD = " + repr(encoded) + "\n" + _REMOTE).encode()
    if len(result) > MAX_BOOTSTRAP:
        raise MaintenanceBundleError("Bundle превышает размер SSH bootstrap")
    return result


_REMOTE = r'''
import base64, hashlib, json, os, stat, sys, zlib
from pathlib import Path
sys.dont_write_bytecode = True
def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode()
def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)
def no_links(path):
    for part in (path, *path.parents):
        try: info = part.lstat()
        except FileNotFoundError: continue
        if stat.S_ISLNK(info.st_mode): raise RuntimeError("unsafe bundle path")
def private_dir(path):
    no_links(path)
    try:
        path.mkdir(mode=0o700)
        sync_dir(path.parent)
    except FileExistsError: pass
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeError("unsafe bundle directory")
def exact_file(path, raw):
    no_links(path)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size != len(raw):
                raise RuntimeError("unsafe bundle file")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                if stream.read(len(raw) + 1) != raw: raise RuntimeError("changed bundle file")
        finally: os.close(fd)
        return
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    sync_dir(path.parent)
def main():
    data = json.loads(zlib.decompress(base64.b64decode(PAYLOAD)))
    bundle, config = data["bundle"], data["config"]
    hashes = {name: record["sha256"] for name, record in bundle["files"].items()}
    if hashlib.sha256(canonical(hashes)).hexdigest() != bundle["bundle_sha256"]:
        raise RuntimeError("bundle mismatch")
    decoded = {}
    for name, record in bundle["files"].items():
        if "/" in name or "\\" in name or not name.endswith(".py"):
            raise RuntimeError("unsafe module name")
        raw = base64.b64decode(record["content"], validate=True)
        if hashlib.sha256(raw).hexdigest() != record["sha256"]:
            raise RuntimeError("module mismatch")
        decoded[name] = raw
    state = Path(config["state_dir"])
    for profile in config["profiles"].values():
        repo = Path(profile["path"])
        if state == repo or repo in state.parents:
            raise RuntimeError("bundle must be outside regional clones")
    private_dir(state)
    private_dir(state / "bundles")
    folder = state / "bundles" / bundle["bundle_sha256"]
    private_dir(folder)
    if set(path.name for path in folder.iterdir()) - set(decoded):
        raise RuntimeError("unknown bundle file")
    for name, raw in decoded.items(): exact_file(folder / name, raw)
    # The isolated interpreter has no cwd/PYTHONPATH imports. Only this verified
    # independent folder is added; region paths are never added to sys.path.
    sys.path.insert(0, str(folder))
    from program_maintenance_host import HostHooks
    from program_maintenance import MaintenanceLease
    from program_maintenance_protocol import serve
    class Hooks(HostHooks):
        def check_guard(self, marker):
            if self.initial_bootstrap:
                journal = self.lease.inspect()["journal"]
                if journal is None or journal["phase"] != "reserved" or journal["snapshot"] is not None or journal["attempts"]:
                    raise RuntimeError("bootstrap is restricted to unpublished reservation")
                self.bootstrap_guard()
                self.initial_bootstrap = False
            return super().check_guard(marker)
    hooks = Hooks(config)
    hooks.initial_bootstrap = data["bootstrap_guard"]
    lease = MaintenanceLease(state, hooks, allow_verified_pending_jobs=True)
    hooks.lease = lease
    serve(lease)
try: main()
except Exception:
    # Files, environment and command errors may contain secrets. Their values
    # must never cross the transport or appear in ordinary installer output.
    sys.stderr.write("Independent maintenance bundle refused; admission state is preserved.\n")
    raise SystemExit(1)
'''
