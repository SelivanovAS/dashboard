"""Real lease+Git host integration; only service/lock/SSH boundaries are fake.

No production I/O or parser execution. Atomic Linux ownership and real systemd
admission have their own tests; this suite proves their host-level composition.
"""
import base64
import copy
import json
from pathlib import Path
import subprocess

import pytest
import program_maintenance as core
import program_maintenance_bundle as bundle
import program_maintenance_git as mg
import program_maintenance_host as host
import program_release as release


def git(repo, *args, check=True):
    cp = subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True)
    if check and cp.returncode:
        raise AssertionError(cp.stderr)
    return cp.stdout.strip() if check else cp


def write(repo, name, raw, mode=0o644):
    path = repo / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


def commit(repo, message):
    git(repo, "add", ".")
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def manifest(repo, region, source, managed):
    value = {"schema_version": 1, "region": region, "source_commit": source,
        "source_repo": "SelivanovAS/dashboard", "repository": "SelivanovAS/" + host.REPOSITORIES[region],
        "profile_sha256": "b" * 64, "baseline_sha256": "c" * 64, "protected_prefixes": ["data/"],
        "files": {name: {"sha256": release.digest((repo / name).read_bytes()),
                        "mode": "100755" if (repo / name).stat().st_mode & 0o111 else "100644"}
                  for name in managed}}
    value["release_id"] = release.release_id(value)
    write(repo, release.LOCK, release.canonical(value))
    return value


class FakeLocks:
    def __init__(self, paths, state, marker, nonce, *, check_admission):
        self.paths, self.marker, self.nonce, self.check = paths, marker, nonce, check_admission
        self.held = False

    def acquire(self):
        self.check()
        for path in self.paths.values():
            path.mkdir()
            write(path, "owner.json", self.nonce.encode())
        self.held = True

    def assert_owned(self):
        self.check()
        assert self.held and self.marker.exists()
        for path in self.paths.values():
            assert (path / "owner.json").read_text() == self.nonce

    def release(self):
        self.assert_owned()
        for path in self.paths.values():
            (path / "owner.json").unlink()
            path.rmdir()
        self.held = False

    def close(self):
        pass


class Boundary:
    DROPIN = "90-program-maintenance.conf"
    def __init__(self, config, root):
        self.marker = Path(config["state_dir"]) / "blocked"
        self.unit_directory = root / "units"
        self.unit_directory.mkdir()
        self.shared = Path(config["profiles"]["hmao"]["path"])
        self.guarded = True
        self.busy = False
        self.pending = False
        self.runtime = "pid=0 ; code=(null) ; status=0/0"
        self.environment = ""
        self.extra_files = {}
        self.calls = []
        for unit in host.LAUNCHERS:
            write(self.unit_directory, unit, b"[Service]\nType=oneshot\n")
            write(self.unit_directory, unit.replace(".service", ".timer"), b"[Timer]\nOnCalendar=*-*-* 06:00:00\n")

    @property
    def content(self):
        return ("[Unit]\nConditionPathExists=!" + str(self.marker) + "\n").encode()

    def check_guard(self):
        assert self.marker.exists() and self.guarded

    def drain(self):
        self.check_guard()
        if self.busy or self.pending:
            raise host.HostError("busy")

    def check_exit(self):
        self.check_guard()
        if self.busy:
            raise host.HostError("busy")
        return {"no_active_workers": True}

    def bootstrap(self):
        for unit in host.LAUNCHERS:
            write(self.unit_directory, unit + ".d/" + self.DROPIN, self.content)
        self.guarded = True

    def run(self, args):
        self.calls.append(args)
        if args[1] == "show-environment":
            return "LANG=C.UTF-8\nPATH=/usr/sbin:/usr/bin:/sbin:/bin\n"
        if args[1] == "list-units":
            return "\n".join(unit + " loaded inactive dead fixture" for unit in host.LAUNCHERS)
        assert args[1] == "show"
        unit = args[2]
        names = args[-1].split("=", 1)[1].split(",")
        props = {name: "" for name in names}
        props.update(LoadState="loaded", NeedDaemonReload="no", User="root", ActiveState="active",
                     UnitFileState="enabled", Persistent="yes", FragmentPath=str(self.unit_directory / unit))
        props["DropInPaths"] = " ".join(str(p) for p in sorted((self.unit_directory / (unit + ".d")).glob("*")))
        if unit in host.LAUNCHERS:
            expected = "/bin/bash " + str(self.shared / "ops/vps-run") + "/" + host.LAUNCHERS[unit]
            props["ExecStart"] = "{ path=/bin/bash ; argv[]=" + expected + " ; " + self.runtime + " ; }"
            chains = {"court-parse.service": ("court-import.service", "court-import.service"),
                      "court-import.service": ("court-delivery.service", "court-delivery.service"),
                      "court-import-poll.service": ("", "court-delivery.service")}
            props["OnSuccess"], props["OnFailure"] = chains.get(unit, ("", ""))
            props["Environment"] = self.environment
        return "\n".join(key + "=" + props[key] for key in names) + "\n"


@pytest.fixture
def fixture(tmp_path):
    root = tmp_path.resolve()
    state = root / "state"
    state.mkdir(mode=0o700)
    config_dir = root / "config"
    config_dir.mkdir()
    profiles, old, authors, remotes, initial, managed = {}, {}, {}, {}, {}, {}
    for region in host.REGIONS:
        origin, author, installed = root / (region + ".git"), root / (region + "-author"), root / region
        origin.mkdir()
        git(origin, "init", "--bare", "--initial-branch=main")
        git(root, "clone", str(origin), str(author))
        git(author, "config", "user.name", "fixture")
        git(author, "config", "user.email", "fixture@example.invalid")
        write(author, "REGION", (region + "\n").encode())
        write(author, "script.py", b"old program\n", 0o755)
        write(author, "data/cases.json", b'{"cases": [1]}\n')
        write(author, ".gitignore", b".runtime/\nops/mac-local-run/.runtime/\n")
        managed[region] = ["REGION", "script.py"]
        if region == "hmao":
            for name in host.ROUTING:
                write(author, name, b"#!/bin/bash\nexit 0\n", 0o755)
            managed[region] += list(host.ROUTING)
        old[region] = manifest(author, region, "a" * 40, managed[region])
        initial[region] = commit(author, "initial")
        git(author, "push", "origin", "main")
        git(root, "clone", str(origin), str(installed))
        write(installed, "ops/mac-local-run/.runtime/queue.json", b"secret queue unchanged\n")
        write(installed, ".runtime/outbox.json", b"root runtime unchanged\n")
        write(installed, "untracked.json", b"preserved\n")
        repository = "SelivanovAS/" + host.REPOSITORIES[region]
        profiles[region] = {"path": str(installed), "repository": repository,
                           "remote": "ssh://git@ssh.github.com:443/" + repository + ".git", "baseline": old[region]}
        authors[region], remotes[region] = author, origin
        write(config_dir, "env." + region, ("REGION=" + region + "\nBANK_TRACK=1\n").encode(), 0o600)
    write(config_dir, "territories", ("\n".join(p["path"] for p in profiles.values()) + "\n").encode(), 0o600)
    author = authors["hmao"]
    write(author, "script.py", b"new program\n", 0o644)
    new = manifest(author, "hmao", "d" * 40, managed["hmao"])
    target = commit(author, "prepared release")
    private_bundle = state / "bundle"
    private_bundle.mkdir(mode=0o700)
    paths = {}
    for name in host.BUNDLE_MODULES:
        write(private_bundle, name, b"# fixture trusted module\n", 0o600)
        paths[str(private_bundle / name)] = release.digest((private_bundle / name).read_bytes())
    bundle_hash = release.digest(bundle.canonical({Path(name).name: value for name, value in paths.items()}))
    config = {"schema_version": 1, "state_dir": str(state), "config_dir": str(config_dir), "profiles": profiles,
              "manifests": {value["release_id"]: value for value in [*old.values(), new]},
              "capability": {"schema_version": 1, "protocol": core.PROTOCOL, "source_commit": "e" * 40,
                             "bundle_sha256": bundle_hash, "bundle_files": paths}}
    boundary = Boundary(config, root)
    def fetch(region, repo, remote):
        assert remote == profiles[region]["remote"]
        git(repo, "fetch", str(remotes[region]), "refs/heads/main:refs/test/main")
        return git(repo, "rev-parse", "refs/test/main")
    hooks = host.HostHooks(config, gate=boundary, runner=boundary.run, locks_factory=FakeLocks, fetcher=fetch)
    return dict(root=root, state=state, config=config, old=old, new=new, target=target, hooks=hooks,
                initial=initial, authors=authors, remotes=remotes, boundary=boundary, managed=managed)


def reserve(lease, fixture):
    new = fixture["new"]
    journal = lease.reserve(region="hmao", release_id=new["release_id"], source_commit=new["source_commit"],
                            manifest_sha256=host.digest(new))
    lease.prepare(journal["nonce"])
    return journal["nonce"]


def register(lease, fixture, nonce, base=None, target=None):
    base = base or fixture["initial"]["hmao"]
    target = target or fixture["target"]
    return lease.register_target(nonce, **bundle.build_target_bundle(fixture["authors"]["hmao"], base, target))


def publish(lease, fixture, nonce):
    register(lease, fixture, nonce)
    token = lease.publish_intent(nonce, base_sha=fixture["initial"]["hmao"], target_sha=fixture["target"])
    git(fixture["authors"]["hmao"], "push", "origin", "main")
    return token


def test_real_four_clone_install_preserves_runtime_and_exits_with_pending_job(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"], allow_verified_pending_jobs=True) as lease:
        nonce = reserve(lease, f)
        token = publish(lease, f, nonce)
        lease.apply(nonce, token["attempt_id"])
        f["boundary"].pending = True
        lease.mark_verified(nonce)
        result = lease.finish(nonce)
        assert result["phase"] == "complete" and result["outcome"] == "installed"
        assert result["safe_evidence"]["covered_attempt_ids"] == [1]
        assert not f["hooks"].marker.exists()
    for region, repo in f["hooks"].repos.items():
        assert (repo / "ops/mac-local-run/.runtime/queue.json").read_bytes() == b"secret queue unchanged\n"
        assert (repo / ".runtime/outbox.json").read_bytes() == b"root runtime unchanged\n"
        assert (repo / "untracked.json").read_bytes() == b"preserved\n"
        assert git(repo, "rev-parse", "HEAD") == (f["target"] if region == "hmao" else f["initial"][region])
    assert host.load_recovery_config(f["state"], nonce) == f["config"]
    assert all(args[1] not in ("start", "stop", "restart") for args in f["boundary"].calls)


def test_runtime_exec_metadata_is_not_routing_identity(fixture):
    f = fixture
    before = f["hooks"]._audit_routing()
    f["boundary"].runtime = "start_time=Sat 2026-10-10 ; stop_time=Sat 2026-10-10 ; pid=943 ; code=exited ; status=0"
    assert f["hooks"]._audit_routing() == before


@pytest.mark.parametrize("flag", ["0", "1"])
def test_hmao_captcha_literals_are_audited_without_exposing_credential(fixture, flag):
    f = fixture
    key = "fixture-token.only+opaque/value=="
    path = Path(f["config"]["config_dir"]) / "env.hmao"
    write(path.parent, path.name, ("REGION='hmao'\nHMAO_APPEAL_CAPTCHA_ENABLED=" + flag +
                                  "\nexport CLOUDRU_API_KEY='" + key + "'\n").encode(), 0o600)
    audit = f["hooks"]._audit_routing()
    encoded = release.canonical(audit)
    assert key.encode() not in encoded
    assert audit["config_files"]["env.hmao"]["sha256"] == release.digest(path.read_bytes())


@pytest.mark.parametrize("region", ["sverdlovsk_yanao", "bashkortostan", "tyumen"])
@pytest.mark.parametrize("assignment", ["HMAO_APPEAL_CAPTCHA_ENABLED=1", "CLOUDRU_API_KEY=fixture-credential"])
def test_hmao_captcha_environment_is_rejected_in_other_territories(fixture, region, assignment):
    f = fixture
    path = Path(f["config"]["config_dir"]) / ("env." + region)
    write(path.parent, path.name, ("REGION=" + region + "\n" + assignment + "\n").encode(), 0o600)
    with pytest.raises(host.HostError, match="values suppressed") as error:
        f["hooks"]._audit_routing()
    assert assignment not in str(error.value)


@pytest.mark.parametrize("assignment", [
    "HMAO_APPEAL_CAPTCHA_ENABLED=true", "HMAO_APPEAL_CAPTCHA_ENABLED=2",
    "HMAO_APPEAL_CAPTCHA_ENABLED=", "CLOUDRU_API_KEY=", "CLOUDRU_API_KEY=' '",
    "CLOUDRU_API_KEY='fixture credential'", "CLOUDRU_API_KEY=one two",
    "CLOUDRU_API_KEY=$(cat /invalid)", "CLOUDRU_API_KEY=`invalid`",
    "CLOUDRU_API_KEY=one;invalid", "CLOUDRU_API_KEY=first\\ second",
])
def test_invalid_captcha_literal_never_leaks_value(fixture, assignment):
    f = fixture
    path = Path(f["config"]["config_dir"]) / "env.hmao"
    write(path.parent, path.name, ("REGION=hmao\n" + assignment + "\n").encode(), 0o600)
    with pytest.raises(host.HostError, match="values suppressed") as error:
        f["hooks"]._audit_routing()
    assert assignment not in str(error.value)


def test_captcha_enablement_during_open_lease_remains_forbidden(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        path = Path(f["config"]["config_dir"]) / "env.hmao"
        before = path.read_bytes()
        write(path.parent, path.name, before + b"HMAO_APPEAL_CAPTCHA_ENABLED=1\n", 0o600)
        try:
            with pytest.raises(host.HostError, match="Persisted host document"):
                f["hooks"].check_guard(f["hooks"].marker)
            assert lease.inspect()["blocked"]
        finally:
            # Restore fixture bytes so context-manager lock release remains valid.
            write(path.parent, path.name, before, 0o600)
        lease.cancel_before_publish(nonce)


@pytest.mark.parametrize("kind", ["queue", "index", "staged", "dirty_data", "transaction", "env", "bundle"])
def test_unknown_mutations_block_publication(fixture, kind):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        repo = f["hooks"].repos["hmao"]
        if kind == "queue": write(repo, ".runtime/outbox.json", b"changed")
        if kind == "index": write(repo, ".git/index.lock", b"foreign")
        if kind == "staged":
            write(repo, "data/cases.json", b"changed")
            git(repo, "add", "data/cases.json")
        if kind == "dirty_data": write(repo, "data/cases.json", b"changed")
        if kind == "transaction": write(repo, "ops/mac-local-run/.runtime/parse_txn.json", b"{}")
        if kind == "env": f["boundary"].environment = "GIT_SSH_COMMAND=secret-do-not-display"
        if kind == "bundle": Path(next(iter(f["config"]["capability"]["bundle_files"]))).write_bytes(b"changed")
        try:
            with pytest.raises((host.HostError, mg.MaintenanceGitError)) as exc:
                lease.publish_intent(nonce, base_sha=f["initial"]["hmao"], target_sha=f["target"])
            assert "secret-do-not-display" not in str(exc.value)
            assert lease.inspect()["journal"]["attempts"] == [] and f["hooks"].marker.exists()
        finally:
            # Fake release still calls admission; restore changed boundary only,
            # leaving actual unknown checkout files in place for assertions.
            f["boundary"].environment = ""
            if kind == "bundle": Path(next(iter(f["config"]["capability"]["bundle_files"]))).write_bytes(b"# fixture trusted module\n")


def test_data_advanced_remote_is_preserved_during_apply(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        token = publish(lease, f, nonce)
        write(f["authors"]["hmao"], "data/cases.json", b'{"cases": [1, 2]}\n')
        fresh = commit(f["authors"]["hmao"], "fresh data")
        git(f["authors"]["hmao"], "push", "origin", "main")
        lease.apply(nonce, token["attempt_id"])
        lease.mark_verified(nonce)
        lease.finish(nonce)
        assert git(f["hooks"].repos["hmao"], "rev-parse", "HEAD") == fresh
        assert (f["hooks"].repos["hmao"] / "data/cases.json").read_bytes() == b'{"cases": [1, 2]}\n'


def test_partial_checkout_repeats_same_binding_then_rollback_above_fresh_data(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        token = publish(lease, f, nonce)
        def crash(event, name):
            if event == "file": raise RuntimeError("simulated cut")
        f["hooks"].checkpoint = crash
        with pytest.raises(RuntimeError, match="simulated cut"):
            lease.apply(nonce, token["attempt_id"])
        assert f["hooks"].marker.exists() and lease.inspect()["journal"]["phase"] == "applying"
        f["hooks"].checkpoint = None
        lease.apply(nonce, token["attempt_id"])
        old = f["old"]["hmao"]
        lease.select_recovery_target(nonce, release_id=old["release_id"], source_commit=old["source_commit"],
                                     manifest_sha256=host.digest(old), reason="rollback")
        author = f["authors"]["hmao"]
        write(author, "data/cases.json", b'{"cases": [1, 2, 3]}\n')
        data_sha = commit(author, "data after first install")
        git(author, "push", "origin", "main")
        write(author, "script.py", b"old program\n", 0o755)
        write(author, release.LOCK, release.canonical(old))
        rollback = commit(author, "rollback program over latest data")
        register(lease, f, nonce, data_sha, rollback)
        token2 = lease.publish_intent(nonce, base_sha=data_sha, target_sha=rollback)
        git(author, "push", "origin", "main")
        lease.apply(nonce, token2["attempt_id"])
        lease.mark_verified(nonce)
        result = lease.finish(nonce)
        assert result["safe_evidence"]["covered_attempt_ids"] == [1, 2]
        assert result["verification"]["release_id"] == old["release_id"]
        repo = f["hooks"].repos["hmao"]
        assert (repo / "data/cases.json").read_bytes() == b'{"cases": [1, 2, 3]}\n'
        assert git(repo, "rev-parse", "HEAD") == rollback
        assert git(repo, "merge-base", "--is-ancestor", f["target"], rollback) == ""


def test_unknown_untouched_remote_program_keeps_gate_closed(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        token = publish(lease, f, nonce)
        lease.apply(nonce, token["attempt_id"])
        lease.mark_verified(nonce)
        author = f["authors"]["tyumen"]
        write(author, "script.py", b"unaudited")
        commit(author, "unknown program")
        git(author, "push", "origin", "main")
        with pytest.raises((host.HostError, mg.MaintenanceGitError)):
            lease.finish(nonce)
        assert f["hooks"].marker.exists()


def test_partial_bootstrap_reuses_original_audit(fixture):
    f = fixture
    boundary, hooks = f["boundary"], f["hooks"]
    original_bootstrap = boundary.bootstrap
    class Wrapper(host.HostHooks):
        def check_guard(self, marker):
            if getattr(self, "initial_bootstrap", False):
                self.bootstrap_guard()
                self.initial_bootstrap = False
            return super().check_guard(marker)
    hooks.__class__ = Wrapper
    hooks.initial_bootstrap = True
    def crash_after_first():
        first = next(iter(host.LAUNCHERS))
        write(boundary.unit_directory, first + ".d/" + boundary.DROPIN, boundary.content)
        raise RuntimeError("interrupted explicit bootstrap")
    boundary.bootstrap = crash_after_first
    with core.MaintenanceLease(f["state"], hooks) as lease:
        new = f["new"]
        with pytest.raises(RuntimeError, match="interrupted explicit bootstrap"):
            lease.reserve(region="hmao", release_id=new["release_id"], source_commit=new["source_commit"],
                          manifest_sha256=host.digest(new))
        journal = lease.inspect()["journal"]
        folder = hooks._operation(journal["nonce"])
        assert not (folder / "routing-audit.json").exists()
        boundary.bootstrap = original_bootstrap
        hooks.check_guard(hooks.marker)
        assert (folder / "bootstrap-after.json").exists()
        assert all((boundary.unit_directory / (unit + ".d") / boundary.DROPIN).exists() for unit in host.LAUNCHERS)
        lease.cancel_before_publish(journal["nonce"])


def test_rejected_unpublished_target_then_rebase_has_all_objects_and_fences(fixture):
    f = fixture
    author, installed = f["authors"]["hmao"], f["hooks"].repos["hmao"]
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        register(lease, f, nonce)
        first = lease.publish_intent(nonce, base_sha=f["initial"]["hmao"], target_sha=f["target"])
        git(author, "checkout", "-b", "data", f["initial"]["hmao"])
        write(author, "data/cases.json", b'{"cases": [1, 2, 3, 4]}\n')
        fresh = commit(author, "data races old target")
        git(author, "push", "origin", fresh + ":main")
        assert git(author, "push", "origin", f["target"] + ":main", check=False).returncode != 0
        git(author, "cherry-pick", f["target"])
        retry = git(author, "rev-parse", "HEAD")
        register(lease, f, nonce, fresh, retry)
        second = lease.publish_intent(nonce, base_sha=fresh, target_sha=retry)
        git(author, "push", "origin", retry + ":main")
        lease.apply(nonce, second["attempt_id"])
        lease.mark_verified(nonce)
        result = lease.finish(nonce)
        assert result["safe_evidence"]["covered_attempt_ids"] == [1, 2]
        assert git(installed, "rev-parse", "refs/program-maintenance/registered/" + nonce + "/" + first["target_sha"]) == first["target_sha"]
        assert git(author, "push", "origin", first["target_sha"] + ":main", check=False).returncode != 0
        assert (installed / "data/cases.json").read_bytes() == b'{"cases": [1, 2, 3, 4]}\n'


def test_unregistered_target_never_receives_publish_ack(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        with pytest.raises(host.HostError, match="host document"):
            lease.publish_intent(nonce, base_sha=f["initial"]["hmao"], target_sha=f["target"])
        assert lease.inspect()["journal"]["attempts"] == []
        lease.cancel_before_publish(nonce)


@pytest.mark.parametrize("kind", ["foreign_tip", "wrong_prerequisite", "data_change", "extra_ref"])
def test_registration_refuses_unknown_bundle_or_candidate(fixture, kind):
    f = fixture
    author = f["authors"]["hmao"]
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        path = f["root"] / "invalid.bundle"
        raw = base64.b64decode(bundle.build_target_bundle(author, f["initial"]["hmao"], f["target"])["bundle_base64"])
        if kind == "foreign_tip": raw = raw.replace(f["target"].encode(), b"f" * 40, 1)
        if kind == "wrong_prerequisite": raw = raw.replace(("-" + f["initial"]["hmao"]).encode(), b"-" + b"f" * 40, 1)
        if kind == "extra_ref": raw = raw.replace(b"\n\nPACK", ("\n" + f["target"] + " other\n\nPACK").encode(), 1)
        target = f["target"]
        if kind == "data_change":
            git(author, "checkout", "-b", "bad-data", f["initial"]["hmao"])
            write(author, "script.py", b"new program\n", 0o644)
            write(author, release.LOCK, release.canonical(f["new"]))
            write(author, "data/cases.json", b"unexpected bundled data\n")
            target = commit(author, "candidate secretly edits data")
            raw = base64.b64decode(bundle.build_target_bundle(author, f["initial"]["hmao"], target)["bundle_base64"])
        with pytest.raises(host.HostError):
            lease.register_target(nonce, base_sha=f["initial"]["hmao"], target_sha=target,
                bundle_sha256=release.digest(raw), bundle_base64=base64.b64encode(raw).decode())
        assert lease.inspect()["journal"]["attempts"] == []
        lease.cancel_before_publish(nonce)


def test_hmao_can_install_while_two_untouched_territories_use_explicit_legacy_baselines(fixture):
    f = fixture
    for region in ("bashkortostan", "tyumen"):
        author, repo = f["authors"][region], f["hooks"].repos[region]
        (author / release.LOCK).unlink()
        legacy_sha = commit(author, "fixture legacy territory")
        git(author, "push", "origin", "main")
        git(repo, "pull", "--ff-only", "origin", "main")
        f["initial"][region] = legacy_sha
        f["config"]["profiles"][region]["baseline"] = {
            "schema_version": 1, "region": region, "source_commit": legacy_sha,
            "files": f["old"][region]["files"]}
    previous = f["hooks"]
    f["hooks"] = host.HostHooks(f["config"], gate=f["boundary"], runner=f["boundary"].run,
                                locks_factory=FakeLocks, fetcher=previous.fetcher)
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        snapshot = lease.inspect()["journal"]["snapshot"]
        assert snapshot["bashkortostan"]["release_id"] is None
        assert snapshot["tyumen"]["release_id"] is None
        token = publish(lease, f, nonce)
        lease.apply(nonce, token["attempt_id"])
        lease.mark_verified(nonce)
        lease.finish(nonce)
    for region in ("bashkortostan", "tyumen"):
        assert not (f["hooks"].repos[region] / release.LOCK).exists()
        assert git(f["hooks"].repos[region], "rev-parse", "HEAD") == f["initial"][region]


@pytest.mark.parametrize("path", ["relative", "/tmp/a/../b", "/tmp/a\nb", "/tmp/a\x7fb", "/tmp/a/./b"])
def test_host_paths_are_canonical_absolute_without_controls(path):
    with pytest.raises(host.HostError):
        host._path(path)


def test_git_url_rewrite_cannot_redirect_pinned_remote(fixture):
    hooks = fixture["hooks"]
    git(hooks.repos["hmao"], "config", "url.file:///untrusted.insteadOf", "ssh://git@ssh.github.com:443/")
    with pytest.raises(host.HostError, match="URL rewrites"):
        hooks._fetch("hmao")


def test_production_fetch_environment_ignores_global_git_and_denies_other_protocols(fixture, monkeypatch):
    hooks = fixture["hooks"]
    hooks.fetcher = None
    original_run = subprocess.run
    observed = []
    def intercept(args, **kwargs):
        if "fetch" in args:
            observed.append((args, kwargs["env"]))
            return subprocess.CompletedProcess(args, 1, stdout=b"", stderr=b"private detail")
        return original_run(args, **kwargs)
    monkeypatch.setattr(subprocess, "run", intercept)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/untrusted/global-config")
    with pytest.raises(host.HostError, match="details suppressed"):
        hooks._fetch("hmao")
    assert len(observed) == 1
    command, environment = observed[0]
    assert environment["GIT_CONFIG_NOSYSTEM"] == "1" and environment["GIT_CONFIG_GLOBAL"] == "/dev/null"
    assert "protocol.allow=never" in command and "protocol.ssh.allow=always" in command
    assert "HostName=ssh.github.com" in environment["GIT_SSH_COMMAND"]


def test_capability_requires_one_private_module_directory(fixture):
    config = fixture["config"]
    assert host.validate_capability(config)["files_verified"] == 8
    directory = Path(next(iter(config["capability"]["bundle_files"]))).parent
    directory.chmod(0o755)
    try:
        with pytest.raises(host.HostError, match="0700"):
            host.validate_capability(config)
    finally:
        directory.chmod(0o700)


def test_registration_rejects_managed_manifest_omission_without_deletion(fixture):
    f = fixture
    author = f["authors"]["hmao"]
    git(author, "checkout", "-b", "omission", f["initial"]["hmao"])
    incomplete = manifest(author, "hmao", "d" * 40, [name for name in f["managed"]["hmao"] if name != "script.py"])
    target = commit(author, "malformed managed-file omission")
    f["config"]["manifests"][incomplete["release_id"]] = incomplete
    previous = f["hooks"]
    f["hooks"] = host.HostHooks(f["config"], gate=f["boundary"], runner=f["boundary"].run,
                                locks_factory=FakeLocks, fetcher=previous.fetcher)
    f["new"], f["target"] = incomplete, target
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        with pytest.raises(host.HostError, match="omitted"):
            register(lease, f, nonce)
        assert lease.inspect()["journal"]["attempts"] == []
        assert git(f["hooks"].repos["hmao"], "rev-parse", "HEAD") == f["initial"]["hmao"]
        lease.cancel_before_publish(nonce)


def test_registration_receipt_waits_for_object_fsync_and_can_repeat_after_cut(fixture, monkeypatch):
    f = fixture
    hooks = f["hooks"]
    original = hooks._sync_registered_objects
    with core.MaintenanceLease(f["state"], hooks) as lease:
        nonce = reserve(lease, f)
        def interrupted(*args):
            raise RuntimeError("power loss before object sync")
        monkeypatch.setattr(hooks, "_sync_registered_objects", interrupted)
        with pytest.raises(RuntimeError, match="power loss"):
            register(lease, f, nonce)
        folder = hooks._operation(nonce) / "registrations"
        assert not list(folder.glob("*.json"))
        assert lease.inspect()["journal"]["attempts"] == []
        monkeypatch.setattr(hooks, "_sync_registered_objects", original)
        first = register(lease, f, nonce)
        second = register(lease, f, nonce)
        assert first == second
        lease.cancel_before_publish(nonce)


def test_missing_registered_pin_blocks_publish_ack(fixture):
    f = fixture
    with core.MaintenanceLease(f["state"], f["hooks"]) as lease:
        nonce = reserve(lease, f)
        register(lease, f, nonce)
        git(f["hooks"].repos["hmao"], "update-ref", "-d", "refs/program-maintenance/registered/" + nonce + "/" + f["target"])
        with pytest.raises((host.HostError, mg.MaintenanceGitError)):
            lease.publish_intent(nonce, base_sha=f["initial"]["hmao"], target_sha=f["target"])
        assert lease.inspect()["journal"]["attempts"] == []
        lease.cancel_before_publish(nonce)
