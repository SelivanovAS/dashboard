#!/usr/bin/env python3
"""Concrete VPS hooks. No CLI, no mutable-clone imports, no delivery operations.

Caller executes this module from a pinned private bundle and holds MaintenanceLease.
Routine checks are read-only; bootstrap_guard is an explicit one-time operation.
Tests inject only the system boundary (systemd runner/gate/locks/fetch), while Git
and filesystem transaction proofs remain real. Production uses the default boundary.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess

import program_maintenance_git as mg
import program_maintenance_locks as ml
import program_maintenance_systemd as ms
import program_release as release

REGIONS = ml.REGIONS
REPOSITORIES = {"hmao": "dashboard", "sverdlovsk_yanao": "dashboard-ural",
                "bashkortostan": "dashboard-bashkortostan", "tyumen": "dashboard-tyumen"}
LAUNCHERS = {"court-parse.service": "parse_all.sh", "court-retry.service": "parse_all.sh --retry-only",
             "court-import.service": "import_all.sh", "court-import-poll.service": "import_poll.sh",
             "court-delivery.service": "delivery_all.sh"}
ROUTING = ("ops/vps-run/parse_all.sh", "ops/vps-run/import_all.sh", "ops/vps-run/delivery_all.sh",
           "ops/vps-run/import_poll.sh", "ops/vps-run/vps_env.sh", "ops/vps-run/shims/netstat",
           "ops/mac-local-run/parse_all.sh", "ops/mac-local-run/import_all.sh",
           "ops/mac-local-run/delivery_all.sh", "ops/mac-local-run/lib_sber_net.sh")
EMPTY_PROPERTIES = ("ExecStartPre", "ExecStartPost", "ExecCondition", "ExecStop", "ExecStopPost", "ExecReload",
                    "EnvironmentFiles", "PassEnvironment", "UnsetEnvironment", "RootDirectory", "RootImage",
                    "WorkingDirectory", "BindPaths", "BindReadOnlyPaths")
TRANSPORT_ENV = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
BUNDLE_MODULES = {"program_release.py", "program_install_vps.py", "program_maintenance.py",
                  "program_maintenance_protocol.py", "program_maintenance_git.py",
                  "program_maintenance_systemd.py", "program_maintenance_locks.py", "program_maintenance_host.py"}
ENV_KEYS = {"DASHBOARD_URL", "BANK_TRACK", "BANK_AUTO_INTAKE", "BANK_INTAKE_DRY_RUN",
            "BANK_INTAKE_MAX_PER_RUN", "BANK_INTAKE_MAX_CARDS_PER_COURT", "BANK_INTAKE_DIGEST_FOLD",
            "BANK_FORCE_DIGEST_FOLD", "DIGEST_PARTIES_MAX_LEN", "DIGEST_PARTIES_KEEP", "REGION"}


class HostError(RuntimeError):
    pass


def digest(value):
    return release.digest(release.canonical(value))


def _path(value, *, directory=False, private=False):
    path = Path(value)
    if (not path.is_absolute() or ".." in path.parts or str(path) != str(value)
            or any(ord(char) < 32 or ord(char) == 127 for char in str(value))):
        raise HostError("Canonical absolute host path required")
    mg._no_symlinks(path)
    if path.exists():
        st = path.stat()
        if st.st_uid != os.geteuid() or st.st_mode & 0o022:
            raise HostError("Untrusted ownership/permissions of host path")
        if directory and not path.is_dir():
            raise HostError("Host directory required")
        if private and stat.S_IMODE(st.st_mode) != 0o700:
            raise HostError("Host state directory must be 0700")
    return path


def _mkdir(path):
    _path(path)
    if not path.exists():
        path.mkdir(mode=0o700)
        mg._sync_dir(path.parent)
    _path(path, directory=True, private=True)
    return path


def _read(path):
    _path(path)
    if (not path.is_file() or path.stat().st_nlink != 1
            or stat.S_IMODE(path.stat().st_mode) != 0o600 or path.stat().st_size > 64 * 1024 * 1024):
        raise HostError("Unsafe/missing host document")
    return release.read_json(path.read_bytes(), "private host document")


def _immutable(path, document):
    raw = release.canonical(document)
    if path.exists() or path.is_symlink():
        if _read(path) != document:
            raise HostError("Persisted host document differs from this operation")
    else:
        mg._write_exclusive(path, raw)
    return document


def validate_capability(config):
    """Prove trusted bundle bytes independently of every regional working tree."""
    capability = config.get("capability", {})
    if (set(capability) != {"schema_version", "protocol", "source_commit", "bundle_sha256", "bundle_files"}
            or capability["schema_version"] != 1 or capability["protocol"] != "court-program-maintenance/2"
            or not release.COMMIT.fullmatch(str(capability["source_commit"]))
            or not release.HEX64.fullmatch(str(capability["bundle_sha256"]))
            or not isinstance(capability["bundle_files"], dict) or not capability["bundle_files"]):
        raise HostError("Unknown or incomplete bundle capability")
    roots = [Path(p["path"]) for p in config["profiles"].values()]
    files, parents = {}, set()
    for name, expected in capability["bundle_files"].items():
        path = _path(name)
        _path(path.parent, directory=True, private=True)
        parents.add(path.parent)
        if any(path == root or root in path.parents for root in roots):
            raise HostError("Coordinator bundle may not live in a mutable clone")
        if (not path.is_file() or path.stat().st_nlink != 1 or not release.HEX64.fullmatch(str(expected))
                or release.digest(path.read_bytes()) != expected):
            raise HostError("Coordinator bundle bytes changed")
        if path.name in files:
            raise HostError("Duplicate coordinator module basename")
        files[path.name] = expected
    encoded = (json.dumps(files, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode()
    if (len(parents) != 1 or set(files) != BUNDLE_MODULES
            or release.digest(encoded) != capability["bundle_sha256"]):
        raise HostError("Bundle capability hash mismatch")
    return {"bundle_sha256": capability["bundle_sha256"], "source_commit": capability["source_commit"],
            "files_verified": len(files), "protocol": capability["protocol"]}


def load_recovery_config(state_dir, nonce):
    if not re.fullmatch(r"[0-9a-f]{32}", str(nonce)):
        raise HostError("Invalid recovery nonce")
    path = _path(state_dir, directory=True, private=True) / "host/operations" / nonce / "config.json"
    value = _read(path)
    validate_capability(value)
    return value


class HostHooks:
    def __init__(self, config, *, gate=None, locks_factory=ml.AtomicRunLocks, runner=ms.command,
                 fetcher=None, checkpoint=None):
        self.config = copy.deepcopy(config)
        config = self.config
        if (set(config) != {"schema_version", "state_dir", "config_dir", "profiles", "manifests", "capability"}
                or config["schema_version"] != 1 or set(config["profiles"]) != set(REGIONS)):
            raise HostError("Explicit four-region host configuration required")
        self.state = _path(config["state_dir"], directory=True, private=True)
        if not self.state.is_dir():
            raise HostError("Bootstrap must create private maintenance state first")
        self.marker = self.state / "blocked"
        self.config_dir = _path(config["config_dir"], directory=True)
        self.profiles = config["profiles"]
        self.repos = {}
        for region, profile in self.profiles.items():
            if set(profile) != {"path", "repository", "remote", "baseline"}:
                raise HostError("Unknown profile fields")
            expected_repo = "SelivanovAS/" + REPOSITORIES[region]
            expected_remote = "ssh://git@ssh.github.com:443/" + expected_repo + ".git"
            if profile["repository"] != expected_repo or profile["remote"] != expected_remote:
                raise HostError("Remote must be this region's pinned SSH GitHub repository")
            path = _path(profile["path"], directory=True)
            mg._repo(path)
            if self.state == path or path in self.state.parents:
                raise HostError("State may not be inside a regional clone")
            self.repos[region] = path
            baseline = profile["baseline"]
            if "release_id" in baseline:
                release.validate_lock(baseline, region)
                if baseline["repository"] != expected_repo:
                    raise HostError("Baseline repository mismatch")
            else:
                release.validate_baseline(baseline, region)
        if len(set(self.repos.values())) != 4:
            raise HostError("Four distinct clones required")
        self.manifests = config["manifests"]
        if not isinstance(self.manifests, dict):
            raise HostError("Manifest map required")
        for key, manifest in self.manifests.items():
            release.validate_lock(manifest)
            if (key != manifest["release_id"]
                    or manifest["repository"] != self.profiles[manifest["region"]]["repository"]):
                raise HostError("Manifest map identity mismatch")
        validate_capability(config)
        self.runner = runner
        self.gate = gate or ms.SystemdMaintenanceGate(self.marker, tuple(LAUNCHERS),
                              timers=tuple(n.replace(".service", ".timer") for n in LAUNCHERS))
        if self.gate.marker != self.marker:
            raise HostError("Gate marker differs from lease marker")
        self.locks_factory = locks_factory
        self.fetcher = fetcher
        self.checkpoint = checkpoint
        self.locks = None
        self.nonce = None
        self.relaxed_exit = False
        self.applying = False
        self.host_dir = _mkdir(self.state / "host")
        self.operations = _mkdir(self.host_dir / "operations")
        self.lock_state = _mkdir(self.host_dir / "locks")
        manifests_dir = _mkdir(self.host_dir / "manifests")
        for key, manifest in self.manifests.items():
            _immutable(manifests_dir / (key + ".json"), manifest)
        capabilities = _mkdir(self.host_dir / "capabilities")
        _immutable(capabilities / (config["capability"]["bundle_sha256"] + ".json"), config["capability"])

    def _operation(self, nonce=None):
        nonce = nonce or _read(self.marker).get("nonce")
        if not re.fullmatch(r"[0-9a-f]{32}", str(nonce)):
            raise HostError("Unknown operation nonce")
        folder = _mkdir(self.operations / nonce)
        _immutable(folder / "config.json", self.config)
        return folder

    def _properties(self, unit, names):
        raw = self.runner(["systemctl", "show", unit, "--all", "--property=" + ",".join(names)])
        out = {}
        for line in raw.splitlines():
            key, sep, value = line.partition("=")
            if not sep or key not in names or key in out:
                raise HostError("Ambiguous loaded unit configuration")
            out[key] = value
        if set(out) != set(names):
            raise HostError("Incomplete loaded unit configuration")
        return out

    def _approved(self, region):
        return [self.profiles[region]["baseline"], *[v for v in self.manifests.values() if v["region"] == region]]

    def _routing_record(self, path):
        st = _path(path).stat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise HostError("Routing/configuration must be a regular private-owned file")
        return {"sha256": release.digest(path.read_bytes()), "mode": stat.S_IMODE(st.st_mode)}

    def _audit_routing(self, *, allow_reload=False):
        validate_capability(self.config)
        shared = self.repos["hmao"]
        for name in ROUTING:
            actual = self._routing_record(shared / name)
            allowed = [v["files"].get(name) for v in self._approved("hmao")]
            if not any(record and actual == {"sha256": record["sha256"],
                       "mode": 0o755 if record["mode"] == "100755" else 0o644} for record in allowed):
                raise HostError("Shared dispatcher differs from approved package: " + name)
        manager = self.runner(["systemctl", "show-environment"])
        manager_values = {}
        for line in manager.splitlines():
            key, sep, value = line.partition("=")
            if not sep or key not in ("LANG", "PATH") or key in manager_values:
                raise HostError("Unknown manager environment; values suppressed")
            if key == "PATH" and any(v not in ("/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin") for v in value.split(":")):
                raise HostError("Unknown manager executable search path")
            manager_values[key] = value
        services, unit_files = {}, {}
        names = (*EMPTY_PROPERTIES, "ExecStart", "Environment", "LoadState", "NeedDaemonReload", "User",
                 "FragmentPath", "DropInPaths", "OnSuccess", "OnFailure")
        for unit, launcher in LAUNCHERS.items():
            props = self._properties(unit, names)
            if (props["LoadState"] != "loaded" or props["User"] != "root"
                    or (not allow_reload and props["NeedDaemonReload"] != "no")
                    or any(props[k] for k in EMPTY_PROPERTIES)):
                raise HostError("Unexpected loaded service routing: " + unit)
            expected = "/bin/bash " + str(shared / "ops/vps-run") + "/" + launcher
            starts = re.findall(r"\{ path=([^;]+) ; argv\[\]=([^;]+) ;", props["ExecStart"])
            if starts != [("/bin/bash", expected)] or props["ExecStart"].count("{") != 1 or props["ExecStart"].count("}") != 1:
                raise HostError("Unexpected executable service route: " + unit)
            chain = {"court-parse.service": ("court-import.service", "court-import.service"),
                     "court-import.service": ("court-delivery.service", "court-delivery.service"),
                     "court-import-poll.service": ("", "court-delivery.service")}.get(unit, ("", ""))
            if (props["OnSuccess"], props["OnFailure"]) != chain:
                raise HostError("Unknown service continuation chain")
            allowed_keys = {"CM_PARALLEL_STAGGER_SECONDS", "CM_PARALLEL_START_DELAYS"} if unit in ("court-parse.service", "court-retry.service") else set()
            for assignment in shlex.split(props["Environment"]):
                key, sep, value = assignment.partition("=")
                if not sep or key not in allowed_keys:
                    raise HostError("Unexpected service environment; values suppressed")
                if (key == "CM_PARALLEL_STAGGER_SECONDS" and not value.isdigit()) or (key == "CM_PARALLEL_START_DELAYS" and any(not re.fullmatch(r"(hmao|sverdlovsk_yanao|bashkortostan|tyumen)=[0-9]+", x) for x in value.split())):
                    raise HostError("Invalid service delay configuration")
            for filename in [props["FragmentPath"], *props["DropInPaths"].split()]:
                if not filename:
                    raise HostError("Missing unit fragment path")
                unit_files[filename] = self._routing_record(Path(filename))
            services[unit] = {k: v for k, v in props.items() if k not in ("NeedDaemonReload", "DropInPaths", "ExecStart")}
            services[unit]["ExecStart"] = {"path": "/bin/bash", "argv": expected}
        units = self.runner(["systemctl", "list-units", "--all", "--type=service", "--plain", "--no-legend", "--no-pager"])
        for line in units.splitlines():
            unit = line.split()[0]
            if not re.fullmatch(r"[A-Za-z0-9_.@\\:-]+\.service", unit):
                raise HostError("Unknown loaded service name")
            if unit in LAUNCHERS:
                continue
            other = self._properties(unit, ("ExecStart", "Environment", *EMPTY_PROPERTIES))
            text = release.canonical(other).decode()
            if any(str(repo) in text or REPOSITORIES[region] in text for region, repo in self.repos.items()):
                raise HostError("Another service references a regional clone")
        listing = self.config_dir / "territories"
        _path(listing)
        entries = [s.strip() for s in listing.read_text().splitlines() if s.strip() and not s.lstrip().startswith("#")]
        if len(entries) != 4 or set(entries) != {str(p) for p in self.repos.values()}:
            raise HostError("Territory routes differ from the four explicit clones")
        for region in REGIONS:
            env_path = self.config_dir / ("env." + region)
            if not env_path.exists():
                continue
            _path(env_path)
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if any(char in line for char in "$`;&|<>\\"):
                    raise HostError("Nonliteral territory environment; values suppressed")
                parts = shlex.split(line, comments=True)
                if parts and parts[0] == "export":
                    parts = parts[1:]
                if len(parts) != 1:
                    raise HostError("Ambiguous territory environment; values suppressed")
                key, sep, value = parts[0].partition("=")
                if not sep or key not in ENV_KEYS or (key == "REGION" and value != region):
                    raise HostError("Unknown territory environment; values suppressed")
        config_files = {}
        for path in sorted(self.config_dir.rglob("*")):
            _path(path)
            if path.is_dir():
                continue
            config_files[path.relative_to(self.config_dir).as_posix()] = self._routing_record(path)
        timers = {}
        for service in LAUNCHERS:
            timer = service.replace(".service", ".timer")
            props = self._properties(timer, ("LoadState", "ActiveState", "UnitFileState", "FragmentPath", "DropInPaths", "Persistent"))
            if props["LoadState"] != "loaded":
                raise HostError("Timer is not loaded")
            timers[timer] = props
            for filename in [props["FragmentPath"], *props["DropInPaths"].split()]:
                unit_files[filename] = self._routing_record(Path(filename))
        # Never persist raw Environment or contents of configuration/secret files.
        return {"manager_sha256": digest(manager_values), "services_sha256": digest(services),
                "config_files": config_files, "unit_files": unit_files, "timers_sha256": digest(timers)}

    def bootstrap_guard(self):
        journal = _read(self.state / "journal.json")
        if journal["phase"] != "reserved" or journal["snapshot"] is not None or journal["attempts"]:
            raise HostError("Guard bootstrap is permitted only before the first publish intent")
        folder = self._operation(journal["nonce"])
        current = self._audit_routing(allow_reload=True)
        saved = folder / "bootstrap-before.json"
        before = _read(saved) if saved.exists() else _immutable(saved, current)
        self._bootstrap_delta(before, current)
        self.gate.bootstrap()
        self.gate.check_guard()
        after = self._audit_routing()
        self._bootstrap_delta(before, after)
        _immutable(folder / "bootstrap-after.json", after)
        return HostHooks.check_guard(self, self.marker)

    def _bootstrap_delta(self, before, after):
        for key in ("manager_sha256", "services_sha256", "config_files", "timers_sha256"):
            if before[key] != after[key]:
                raise HostError("Bootstrap changed routing/configuration/timer state")
        for name, record in before["unit_files"].items():
            if after["unit_files"].get(name) != record:
                raise HostError("Bootstrap changed an existing unit file")
        added = set(after["unit_files"]) - set(before["unit_files"])
        allowed = {str(self.gate.unit_directory / (unit + ".d") / self.gate.DROPIN) for unit in LAUNCHERS}
        if not added <= allowed:
            raise HostError("Bootstrap added unknown unit files")
        for name in added:
            if Path(name).read_bytes() != self.gate.content:
                raise HostError("Bootstrap guard addition has unknown bytes")

    def check_guard(self, marker):
        if Path(marker) != self.marker or _read(self.marker).get("schema_version") != 2:
            raise HostError("Incorrect lease marker")
        self.gate.check_guard()
        audit = self._audit_routing()
        _immutable(self._operation() / "routing-audit.json", audit)
        return audit

    def drain(self):
        self.relaxed_exit = False
        self.gate.drain()

    def check_exit(self, journal):
        if journal["phase"] not in ("applying", "verified", "safe_to_resume", "complete") or self.applying:
            raise HostError("Writes have not finished; exit is forbidden")
        self.relaxed_exit = True
        return self.gate.check_exit()

    def _admission(self):
        self.check_guard(self.marker)
        if self.relaxed_exit:
            self.gate.check_exit()
        else:
            self.gate.drain()

    def acquire_run_locks(self, nonce):
        self._operation(nonce)
        if self.locks is not None:
            raise HostError("Lock adapter already exists in this session")
        self.nonce = nonce
        paths = {region: repo / "ops/mac-local-run/.run.lock" for region, repo in self.repos.items()}
        self.locks = self.locks_factory(paths, self.lock_state, self.marker, nonce, check_admission=self._admission)
        try:
            if (self.lock_state / nonce).exists():
                self.locks.recover()
            else:
                self.locks.acquire()
        except BaseException:
            self.locks.close()
            self.locks = None
            raise

    def assert_run_locks(self, nonce):
        if self.nonce != nonce or self.locks is None:
            raise HostError("This coordinator does not own the four locks")
        self.locks.assert_owned()

    def release_run_locks(self, nonce):
        self.assert_run_locks(nonce)
        self.locks.release()
        self.locks.close()
        self.locks = None

    def _program(self, region, sha):
        repo = self.repos[region]
        mg._assert_complete_history(repo)
        entries = mg._tree_entries(repo, sha)
        if release.LOCK in entries:
            manifest = mg._installed(repo, sha)
            if manifest not in self._approved(region):
                raise HostError("Unknown installed/remote program")
            return manifest
        baseline = self.profiles[region]["baseline"]
        if "release_id" in baseline:
            raise HostError("Installed release manifest disappeared")
        records, _ = mg._tree(repo, sha)
        for name, wanted in baseline["files"].items():
            expected = None if wanted is None else {"type": "file", "sha256": wanted["sha256"], "mode": 0o755 if wanted["mode"] == "100755" else 0o644}
            if records.get(name) != expected:
                raise HostError("Legacy program differs from explicit baseline")
        return baseline

    def _actual_program(self, region):
        repo = self.repos[region]
        sha = mg._text(repo, "rev-parse", "HEAD")
        program = self._program(region, sha)
        observed = mg.inventory(repo)
        for name, wanted in program["files"].items():
            expected = None if wanted is None else {"type": "file", "sha256": wanted["sha256"], "mode": 0o755 if wanted["mode"] == "100755" else 0o644}
            if observed.get(name) != expected:
                raise HostError("Actual managed program differs from its approved version")
        if "release_id" in program:
            if release.read_json((repo / release.LOCK).read_bytes(), "installed manifest") != program:
                raise HostError("Actual manifest differs from Git")
        region_file = repo / "REGION"
        actual_region = region_file.read_text().strip() if region_file.exists() else "hmao"
        if actual_region != region:
            raise HostError("Effective regional setting changed")
        return sha, program, observed

    def snapshot(self):
        self.assert_run_locks(self.nonce)
        folder = _mkdir(self._operation() / "inventories")
        snapshot = {}
        for region in REGIONS:
            sha, program, observed = self._actual_program(region)
            _immutable(folder / (region + ".json"), observed)
            snapshot[region] = {"head": sha, "release_id": program.get("release_id"),
                "managed_sha256": digest(program["files"]), "inventory_sha256": digest(observed)}
        _immutable(self._operation() / "snapshot.json", snapshot)
        return snapshot

    def _baseline(self, journal, region):
        folder = self._operation(journal["nonce"])
        expected = _read(folder / "snapshot.json")
        if expected != journal["snapshot"]:
            raise HostError("Core snapshot does not match durable host snapshot")
        result = _read(folder / "inventories" / (region + ".json"))
        if digest(result) != expected[region]["inventory_sha256"]:
            raise HostError("Protected inventory checksum mismatch")
        return result

    def validate_snapshot(self, snapshot):
        folder = self._operation()
        if _read(folder / "snapshot.json") != snapshot:
            raise HostError("Unknown snapshot")
        for region in REGIONS:
            sha, program, observed = self._actual_program(region)
            saved = _read(folder / "inventories" / (region + ".json"))
            if (sha != snapshot[region]["head"] or digest(saved) != snapshot[region]["inventory_sha256"]
                    or observed != saved or digest(program["files"]) != snapshot[region]["managed_sha256"]):
                raise HostError("Working files changed after reservation")

    def _target(self, journal, target_id=None):
        target = journal["targets"][(target_id or journal["active_target_id"]) - 1]
        manifest = self.manifests.get(target["release_id"])
        if (manifest is None or digest(manifest) != target["manifest_sha256"]
                or manifest["source_commit"] != target["source_commit"] or manifest["region"] != journal["region"]):
            raise HostError("Target manifest is not in this pinned bundle's approved map")
        stored = _read(self.host_dir / "manifests" / (target["release_id"] + ".json"))
        if stored != manifest:
            raise HostError("Stored target manifest changed")
        return manifest

    def _fetch(self, region):
        profile, repo = self.profiles[region], self.repos[region]
        mg._assert_complete_history(repo)
        rewrites = mg._git(repo, "config", "--get-regexp", r"^url\..*\.(insteadof|pushinsteadof)$",
                           env=TRANSPORT_ENV, check=False)
        if rewrites.returncode not in (0, 1) or rewrites.returncode == 0:
            raise HostError("Git URL rewrites cannot redirect the trusted SSH read")
        if self.fetcher is not None:
            sha = self.fetcher(region, repo, profile["remote"])
        else:
            ref = "refs/program-maintenance/remote-main"
            command = ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
                       "-c", "protocol.allow=never", "-c", "protocol.ssh.allow=always",
                       "-c", "gc.auto=0", "-c", "maintenance.auto=false", "-C", str(repo),
                       "fetch", "--no-tags", "--no-recurse-submodules", profile["remote"], "refs/heads/main:" + ref]
            env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
            env.update(TRANSPORT_ENV)
            env.update(GIT_TERMINAL_PROMPT="0", GIT_NO_REPLACE_OBJECTS="1", GIT_OPTIONAL_LOCKS="0",
                       GIT_SSH_COMMAND="ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=2 -p 443 -o HostName=ssh.github.com")
            try:
                cp = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=120)
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise HostError("Remote Git read failed; details suppressed") from exc
            if cp.returncode:
                raise HostError("Remote Git read failed; details suppressed")
            sha = mg._text(repo, "rev-parse", ref)
        mg._assert_complete_history(repo)
        return mg._commit(repo, sha)

    def _data_advance(self, region, before, after):
        repo = self.repos[region]
        if not mg._ancestor(repo, before, after):
            raise HostError("Remote history rewound or diverged")
        changed = mg._git(repo, "diff", "--name-only", "-z", before, after).stdout.decode().split("\0")
        if any(name and not release.data_only_path(name) for name in changed):
            raise HostError("Remote advanced outside protected data paths")

    def _bundle_header(self, raw, base_sha, target_sha):
        # SHA-1 ordinary clone bundles generated as: git bundle create FILE T ^B.
        # One commit candidate only: exactly B prerequisite and exactly T tip.
        if not isinstance(raw, bytes) or not 0 < len(raw) <= 512 * 1024:
            raise HostError("Target bundle exceeds the bounded registration limit")
        header, separator, pack = raw.partition(b"\n\n")
        lines = header.splitlines()
        if (not separator or not pack.startswith(b"PACK") or not lines or lines[0] != b"# v2 git bundle"
                or len(lines) != 3):
            raise HostError("Only a bounded v2 single-candidate Git bundle is supported")
        prerequisite = lines[1].split(b" ", 1)[0]
        advertised = lines[2].split(b" ", 1)
        if prerequisite != ("-" + base_sha).encode() or len(advertised) != 2 or advertised[0] != target_sha.encode():
            raise HostError("Bundle prerequisite/tip differs from the registered candidate")
        if advertised[1] != ("refs/program-maintenance/targets/" + target_sha).encode():
            raise HostError("Unknown bundle tip label")

    def _candidate(self, journal, base_sha, target_sha):
        repo, region = self.repos[journal["region"]], journal["region"]
        mg._assert_complete_history(repo)
        mg._commit(repo, base_sha)
        mg._commit(repo, target_sha)
        if mg._text(repo, "rev-list", "--parents", "-n", "1", target_sha).split() != [target_sha, base_sha]:
            raise HostError("Prepared target must be one ordinary commit above its exact base")
        before = self._program(region, base_sha)
        manifest = self._target(journal)
        mg._installed(repo, target_sha, manifest)
        target_paths = mg._tree_entries(repo, target_sha)
        if any(name in target_paths for name in set(before["files"]) - set(manifest["files"])):
            raise HostError("Target retains a previously managed file omitted from its manifest")
        allowed = set(before["files"]) | set(manifest["files"]) | {release.LOCK}
        changed = mg._git(repo, "diff", "--name-only", "-z", base_sha, target_sha).stdout.decode().split("\0")
        if any(name and name not in allowed for name in changed):
            raise HostError("Prepared target changes data or files outside its approved program")
        # Ensure existing clone can reach B without history/data rewind. Any
        # intervening package must itself have an approved final stamp; no
        # unknown tracked file can appear outside known program/data paths.
        original = journal["snapshot"][region]["head"]
        if not mg._ancestor(repo, original, base_sha):
            raise HostError("Candidate base rewinds/diverges from the reserved checkout")
        known = {release.LOCK}
        for program in self._approved(region):
            known.update(program["files"])
        advanced = mg._git(repo, "diff", "--name-only", "-z", original, base_sha).stdout.decode().split("\0")
        if any(name and name not in known and not release.data_only_path(name) for name in advanced):
            raise HostError("Candidate base contains unknown tracked changes")
        original_tree, _ = mg._tree(repo, original)
        future_tree, _ = mg._tree(repo, target_sha)
        inventory = self._baseline(journal, region)
        for name in future_tree:
            if name not in original_tree and name in inventory:
                raise HostError("Candidate collides with a protected/unmanaged object")
            for parent in Path(name).parents:
                if str(parent) != "." and parent.as_posix() in inventory and inventory[parent.as_posix()]["type"] != "directory":
                    raise HostError("Candidate parent collides with a protected object")
        return manifest

    def _registration_path(self, journal, base_sha, target_sha):
        if not all(release.COMMIT.fullmatch(str(value)) for value in (base_sha, target_sha)):
            raise HostError("Registration requires exact full commit identities")
        folder = _mkdir(self._operation(journal["nonce"]) / "registrations")
        return folder / (str(journal["active_target_id"]) + "-" + base_sha + "-" + target_sha + ".json")

    def _sync_registered_objects(self, repo, reference):
        # A durable bundle receipt alone is insufficient after power loss. Make
        # imported ODB bytes and all parent entries durable before any ACK.
        objects = repo / ".git/objects"
        directories = [objects]
        for path in sorted(objects.rglob("*")):
            mg._no_symlinks(path)
            if path.is_dir():
                directories.append(path)
                continue
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise HostError("Unknown object-store entry prevents durable registration")
                os.fsync(fd)
            finally:
                os.close(fd)
        for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
            mg._sync_dir(directory)
        path = repo / ".git" / reference
        if not path.exists():
            path = repo / ".git/packed-refs"
        mg._no_symlinks(path)
        with path.open("rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise HostError("Unsafe registered Git reference")
            os.fsync(stream.fileno())
        for directory in path.parents:
            mg._sync_dir(directory)
            if directory == repo / ".git":
                break

    def register_target(self, journal, *, base_sha, target_sha, bundle_sha256, bundle_bytes):
        self.validate_target(journal)
        receipt_path = self._registration_path(journal, base_sha, target_sha)
        if not release.HEX64.fullmatch(str(bundle_sha256)) or release.digest(bundle_bytes) != bundle_sha256:
            raise HostError("Target bundle checksum mismatch")
        self._bundle_header(bundle_bytes, base_sha, target_sha)
        region, repo = journal["region"], self.repos[journal["region"]]
        self._fetch(region)  # Read only trusted SSH main; supplies current prerequisites.
        mg._commit(repo, base_sha)
        bundle_path = receipt_path.parent / (bundle_sha256 + ".bundle")
        if bundle_path.exists():
            _path(bundle_path)
            if (not bundle_path.is_file() or bundle_path.stat().st_nlink != 1
                    or stat.S_IMODE(bundle_path.stat().st_mode) != 0o600 or bundle_path.read_bytes() != bundle_bytes):
                raise HostError("Existing registered bundle differs from its checksum")
        else:
            mg._write_exclusive(bundle_path, bundle_bytes)
        for operation in ("verify", "unbundle"):
            if mg._git(repo, "-c", "protocol.allow=never", "-c", "protocol.file.allow=always",
                       "-c", "core.fsync=all", "-c", "core.fsyncMethod=fsync",
                       "-c", "gc.auto=0", "-c", "maintenance.auto=false", "bundle", operation, str(bundle_path), env=TRANSPORT_ENV, check=False).returncode:
                raise HostError("Git refused registered candidate objects; details suppressed")
        manifest = self._candidate(journal, base_sha, target_sha)
        ref = "refs/program-maintenance/registered/" + journal["nonce"] + "/" + target_sha
        present = mg._git(repo, "rev-parse", "--verify", ref, check=False)
        if present.returncode == 0:
            if present.stdout.decode().strip() != target_sha:
                raise HostError("Registered target ref was changed")
        elif mg._git(repo, "update-ref", ref, target_sha, "0" * 40, check=False).returncode:
            raise HostError("Cannot retain candidate objects before publication")
        self._sync_registered_objects(repo, ref)
        receipt = {"schema_version": 1, "nonce": journal["nonce"], "region": region,
                   "target_id": journal["active_target_id"], "manifest_sha256": digest(manifest),
                   "base_sha": base_sha, "target_sha": target_sha, "bundle_sha256": bundle_sha256}
        _immutable(receipt_path, receipt)
        return {"evidence_sha256": digest(receipt)}

    def validate_publication(self, journal, attempt):
        self.assert_run_locks(journal["nonce"])
        if attempt["target_id"] != journal["active_target_id"]:
            raise HostError("Publication candidate belongs to another target")
        path = self._registration_path(journal, attempt["base_sha"], attempt["target_sha"])
        receipt = _read(path)
        manifest = self._candidate(journal, attempt["base_sha"], attempt["target_sha"])
        reference = "refs/program-maintenance/registered/" + journal["nonce"] + "/" + attempt["target_sha"]
        if mg._text(self.repos[journal["region"]], "rev-parse", "--verify", reference) != attempt["target_sha"]:
            raise HostError("Registered target ref changed before publication")
        expected = {"schema_version": 1, "nonce": journal["nonce"], "region": journal["region"],
                    "target_id": journal["active_target_id"], "manifest_sha256": digest(manifest),
                    "base_sha": attempt["base_sha"], "target_sha": attempt["target_sha"],
                    "bundle_sha256": receipt.get("bundle_sha256")}
        if receipt != expected or not release.HEX64.fullmatch(str(receipt.get("bundle_sha256"))):
            raise HostError("Missing or changed durable candidate registration")
        bundle_path = path.parent / (receipt["bundle_sha256"] + ".bundle")
        _path(bundle_path)
        if (not bundle_path.is_file() or bundle_path.stat().st_nlink != 1
                or stat.S_IMODE(bundle_path.stat().st_mode) != 0o600):
            raise HostError("Unsafe registered candidate bundle")
        raw = bundle_path.read_bytes()
        if release.digest(raw) != receipt["bundle_sha256"]:
            raise HostError("Registered candidate bundle changed")
        self._bundle_header(raw, attempt["base_sha"], attempt["target_sha"])

    def _attempt_dir(self, journal, attempt_id):
        return _mkdir(_mkdir(self._operation(journal["nonce"]) / "installs") / ("attempt-" + str(attempt_id)))

    def _binding(self, journal, attempt):
        folder = self._attempt_dir(journal, attempt["attempt_id"])
        path = folder / "binding.json"
        if not path.exists():
            return None, None
        binding = _read(path)
        manifest = self._target(journal, attempt["target_id"])
        expected = {"nonce": journal["nonce"], "region": journal["region"], **attempt, "manifest_sha256": digest(manifest)}
        if {k: v for k, v in binding.items() if k != "install_sha"} != expected:
            raise HostError("Git transaction is bound to a different publication intent")
        mg._commit(self.repos[journal["region"]], binding["install_sha"])
        docs = list(folder.glob("tx-*/journal.json"))
        if len(docs) > 1:
            raise HostError("Ambiguous installation journals")
        return binding, docs[0] if docs else None

    def _chain(self, journal):
        region, repo = journal["region"], self.repos[journal["region"]]
        expected = self._baseline(journal, region)
        head = journal["snapshot"][region]["head"]
        last = None
        for attempt in journal["attempts"]:
            binding, path = self._binding(journal, attempt)
            if path is None:
                continue
            _, _, document = mg._load(repo, path)
            manifest = self._target(journal, attempt["target_id"])
            if (document["inventory"] != expected or document["old_sha"] != head
                    or document["target_sha"] != binding["install_sha"] or document["target_lock"] != manifest):
                raise HostError("Installation journal breaks the original protected inventory chain")
            self._data_advance(region, attempt["target_sha"], binding["install_sha"])
            if mg._installed(repo, attempt["target_sha"]) != manifest:
                raise HostError("Published target does not contain the approved manifest")
            expected = copy.deepcopy(expected)
            expected.update(document["new_directories"])
            for name, change in document["writes"].items():
                if change["new"] is None:
                    expected.pop(name, None)
                else:
                    expected[name] = change["new"]
            head, last = document["target_sha"], path
        if last is None:
            if mg.inventory(repo) != expected or mg._text(repo, "rev-parse", "HEAD") != head:
                raise HostError("Target working copy changed outside this release")
        else:
            _, _, document = mg._load(repo, last)
            mg._validate_partial(repo, document)
            index_lock = repo / ".git/index.lock"
            if index_lock.exists() or index_lock.is_symlink():
                mg._check_index_lock(repo, last, document, require_owned=True)
        return last

    def _untouched(self, journal):
        for region, repo in self.repos.items():
            if region == journal["region"]:
                continue
            expected = self._baseline(journal, region)
            if mg.inventory(repo) != expected or mg._text(repo, "rev-parse", "HEAD") != journal["snapshot"][region]["head"]:
                raise HostError("Untouched territory changed during maintenance")
            self._actual_program(region)

    def _no_runtime_transactions(self):
        for repo in self.repos.values():
            runtime = repo / "ops/mac-local-run/.runtime"
            if any((runtime / name).exists() or (runtime / name).is_symlink() for name in ("parse_txn.json", "delivery_txn.json")):
                raise HostError("An ordinary parse/delivery transaction must finish before publication")

    def _clean_checkout(self, region):
        repo = self.repos[region]
        head = mg._text(repo, "rev-parse", "HEAD")
        if (repo / ".git/index.lock").exists() or (repo / ".git/index.lock").is_symlink():
            raise HostError("Foreign Git index lock prevents publication")
        expected, _ = mg._tree(repo, head)
        actual = mg.inventory(repo)
        if not mg._index_matches(repo, head) or any(actual.get(name) != value for name, value in expected.items()):
            raise HostError("Tracked checkout/index must be clean before publication")

    def validate_target(self, journal):
        self.assert_run_locks(journal["nonce"])
        self._target(journal)
        self._no_runtime_transactions()
        self._untouched(journal)
        previous = self._chain(journal)
        for region in REGIONS:
            if region != journal["region"] or previous is None:
                self._clean_checkout(region)

    def apply_target(self, journal):
        self.assert_run_locks(journal["nonce"])
        self.validate_target(journal)
        attempt = journal["attempts"][-1]
        if (journal["phase"] != "applying" or attempt["attempt_id"] != journal["active_attempt_id"]
                or attempt["target_id"] != journal["active_target_id"]):
            raise HostError("Only the active applying publication may write files")
        region, repo = journal["region"], self.repos[journal["region"]]
        manifest = self._target(journal)
        self.applying = True
        try:
            prior = self._chain(journal)
            if prior is not None:
                mg.resume_install(repo, prior, checkpoint=self.checkpoint)
            self._untouched(journal)
            remote = self._fetch(region)
            if mg._installed(repo, remote) != manifest or mg._installed(repo, attempt["target_sha"]) != manifest:
                raise HostError("Published remote is not the approved active target")
            self._data_advance(region, attempt["target_sha"], remote)
            binding, path = self._binding(journal, attempt)
            folder = self._attempt_dir(journal, attempt["attempt_id"])
            if binding is None:
                binding = {"nonce": journal["nonce"], "region": region, **attempt,
                           "manifest_sha256": digest(manifest), "install_sha": remote}
                _immutable(folder / "binding.json", binding)
            else:
                self._data_advance(region, binding["install_sha"], remote)
            if path is None:
                # A cut during blob preparation mutates no checkout files. Keep
                # its private evidence and use another bounded staging generation.
                for number in range(1, 17):
                    candidate = folder / ("tx-%04d" % number)
                    if not candidate.exists():
                        path = candidate / "journal.json"
                        break
                else:
                    raise HostError("Too many interrupted preparation generations")
                mg.begin_install(repo, binding["install_sha"], path, manifest)
            mg.resume_install(repo, path, checkpoint=self.checkpoint)
        finally:
            self.applying = False

    def verify_installed(self, journal):
        self._untouched(journal)
        path = self._chain(journal)
        attempt = journal["attempts"][-1]
        binding, expected_path = self._binding(journal, attempt)
        if path is None or path != expected_path or binding is None:
            raise HostError("Active target has no completed installation journal")
        proof = mg.verify_install(self.repos[journal["region"]], path)
        manifest = self._target(journal)
        if proof["installed_sha"] != binding["install_sha"] or proof["release_id"] != manifest["release_id"]:
            raise HostError("A different release was installed")
        return {"target_id": journal["active_target_id"], "manifest_sha256": journal["manifest_sha256"],
                "installed_sha": proof["installed_sha"], "release_id": proof["release_id"], "evidence_sha256": proof["evidence_sha256"]}

    def verify_safe(self, journal):
        if journal["attempts"]:
            verification = self.verify_installed(journal)
        else:
            self.validate_snapshot(journal["snapshot"])
            verification = None
        remote_records = {}
        for region in REGIONS:
            remote = self._fetch(region)
            program = self._program(region, remote)
            if journal["attempts"] and region == journal["region"]:
                if program != self._target(journal):
                    raise HostError("Remote target program changed before admission reopened")
                self._data_advance(region, verification["installed_sha"], remote)
                fence = mg.publication_fences(self.repos[region], remote, journal["attempts"])
            else:
                before = journal["snapshot"][region]["head"]
                self._data_advance(region, before, remote)
                before_program = self._program(region, before)
                if program != before_program:
                    raise HostError("Untouched remote program changed")
            remote_records[region] = {"head": remote, "managed_sha256": digest(program["files"])}
        evidence = {"verification": verification, "remotes": remote_records,
                    "attempts": journal["attempts"], "bundle_sha256": self.config["capability"]["bundle_sha256"]}
        proof_hash = digest(evidence)
        evidence_dir = _mkdir(self._operation(journal["nonce"]) / "evidence")
        _immutable(evidence_dir / (proof_hash + ".json"), evidence)
        return {"covered_attempt_ids": [a["attempt_id"] for a in journal["attempts"]], "evidence_sha256": proof_hash}


def make_hooks(config):
    return HostHooks(config)
