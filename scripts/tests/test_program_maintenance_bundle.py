"""Independent committed coordinator bootstrap; no systemd or production I/O."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess

import pytest

import program_maintenance_bundle as bundle
from program_maintenance_protocol import LeaseClient, MaintenanceProtocolError


HOST = '''
from pathlib import Path
class HostHooks:
    def __init__(self, config):
        self.config = config
        self.guard = Path(config["state_dir"]) / "fixture-guard"
    def bootstrap_guard(self):
        self.guard.write_text("guard")
    def check_guard(self, marker):
        assert self.guard.read_text() == "guard"
    def drain(self): pass
    def acquire_run_locks(self, nonce): pass
    def assert_run_locks(self, nonce): pass
    def release_run_locks(self, nonce): pass
    def snapshot(self):
        return {code: {"head":"1"*40,"release_id":None,"managed_sha256":"2"*64,"inventory_sha256":"3"*64}
                for code in ("hmao","sverdlovsk_yanao","bashkortostan","tyumen")}
    def validate_snapshot(self, snapshot): pass
    def validate_target(self, journal): pass
    def validate_publication(self, journal, attempt): pass
    def apply_target(self, journal): pass
    def verify_installed(self, journal): raise RuntimeError("not a host fixture installation")
    def verify_safe(self, journal): raise RuntimeError("not a host fixture installation")
    def check_exit(self, journal): pass
'''


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE).decode().strip()


@pytest.fixture
def committed(tmp_path):
    root = tmp_path.resolve()
    repo = root / "source"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "fixture@example.invalid")
    git(repo, "config", "user.name", "Fixture")
    scripts = repo / "scripts"
    scripts.mkdir()
    for name in bundle.MODULES:
        src = Path(bundle.__file__).with_name(name)
        content = HOST if name == "program_maintenance_host.py" else src.read_text()
        (scripts / name).write_text(content)
    git(repo, "add", "scripts")
    git(repo, "commit", "-qm", "fixture coordinator")
    source = git(repo, "rev-parse", "HEAD")
    config = {"schema_version": 1, "state_dir": str(root / "state"),
              "config_dir": str(root / "config"),
              "profiles": {code: {"path": str(root / code)} for code in
                           ("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen")}, "manifests": {}}
    return repo, source, config


def reserve(client):
    return client.request("reserve", region="hmao", release_id="1" * 64,
                          source_commit="2" * 40, manifest_sha256="3" * 64)


def test_bundle_uses_exact_commit_not_dirty_worktree(committed):
    repo, source, _ = committed
    before = bundle.build_bundle(repo, source)
    (repo / "scripts/program_maintenance.py").write_text("invalid Python!\n")
    assert bundle.build_bundle(repo, source) == before
    with pytest.raises(Exception):
        bundle.build_bundle(repo, "HEAD")


def test_target_bundle_retains_candidate_unreachable_from_newer_data(committed, tmp_path):
    repo, base, _ = committed
    clone = tmp_path / "proof"
    subprocess.run(["git", "clone", "-q", "--no-local", str(repo), str(clone)], check=True)
    (repo / "program.txt").write_text("new program")
    git(repo, "add", "program.txt")
    git(repo, "commit", "-qm", "candidate")
    target = git(repo, "rev-parse", "HEAD")
    candidate = bundle.build_target_bundle(repo, base, target)
    path = tmp_path / "target.bundle"
    path.write_bytes(base64.b64decode(candidate["bundle_base64"]))
    assert git(clone, "bundle", "list-heads", str(path)).split() == [target, "refs/program-maintenance/targets/" + target]
    git(clone, "bundle", "verify", str(path))
    git(clone, "fetch", str(path), target)
    assert git(clone, "show", target + ":program.txt") == "new program"


def test_target_bundle_rejects_wrong_base_noop_and_oversize(committed):
    repo, base, _ = committed
    with pytest.raises(bundle.MaintenanceBundleError): bundle.build_target_bundle(repo, base, base)
    (repo / "program.txt").write_text("first")
    git(repo, "add", "program.txt")
    git(repo, "commit", "-qm", "first")
    intermediate = git(repo, "rev-parse", "HEAD")
    (repo / "program.txt").write_bytes(os.urandom(700 * 1024))
    git(repo, "add", "program.txt")
    git(repo, "commit", "-qm", "second")
    target = git(repo, "rev-parse", "HEAD")
    with pytest.raises(bundle.MaintenanceBundleError, match="один коммит"):
        bundle.build_target_bundle(repo, base, target)
    with pytest.raises(bundle.MaintenanceBundleError, match="512 KiB"):
        bundle.build_target_bundle(repo, intermediate, target)


def test_bootstrap_imports_committed_private_bundle_and_can_repeat(committed):
    repo, source, config = committed
    fixed = bundle.build_bundle(repo, source)
    script = bundle.bootstrap_source(fixed, config)
    (repo / "scripts/program_maintenance.py").write_text("raise RuntimeError('dirty clone')")
    for _ in range(2):
        with LeaseClient.local(script) as client:
            assert client.request("inspect") == {"blocked": False, "journal": None}
    folder = Path(config["state_dir"]) / "bundles" / fixed["bundle_sha256"]
    assert set(path.name for path in folder.iterdir()) == set(bundle.MODULES)
    assert folder.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in folder.iterdir())


@pytest.mark.parametrize("mutation", ["changed", "symlink", "hardlink", "permissions", "unknown"])
def test_existing_bundle_tampering_is_not_overwritten(committed, mutation):
    repo, source, config = committed
    fixed = bundle.build_bundle(repo, source)
    script = bundle.bootstrap_source(fixed, config)
    with LeaseClient.local(script): pass
    folder = Path(config["state_dir"]) / "bundles" / fixed["bundle_sha256"]
    target = folder / "program_release.py"
    if mutation == "changed": target.write_bytes(b"changed")
    elif mutation == "symlink":
        target.unlink()
        target.symlink_to(repo / "scripts/program_release.py")
    elif mutation == "hardlink": os.link(target, folder.parent / "foreign-hardlink")
    elif mutation == "permissions": target.chmod(0o644)
    else: (folder / "unknown.py").write_text("unknown")
    with pytest.raises(MaintenanceProtocolError): LeaseClient.local(script)
    if mutation == "changed": assert target.read_bytes() == b"changed"
    if mutation == "symlink": assert target.is_symlink()


def test_bootstrap_guard_is_explicit_and_marker_precedes_it(committed):
    repo, source, config = committed
    fixed = bundle.build_bundle(repo, source)
    with LeaseClient.local(bundle.bootstrap_source(fixed, config)) as client:
        with pytest.raises(MaintenanceProtocolError): reserve(client)
    state = Path(config["state_dir"])
    assert (state / "blocked").exists()
    assert not (state / "fixture-guard").exists()
    nonce = json.loads((state / "journal.json").read_text())["nonce"]
    with LeaseClient.local(bundle.bootstrap_source(fixed, config, bootstrap_guard=True)) as client:
        journal = client.request("recover", nonce=nonce)
        assert journal["phase"] == "reserved"
        assert (state / "fixture-guard").read_text() == "guard"
        client.request("prepare", nonce=nonce)
    # Explicit bootstrap is not a fallback for later installation phases.
    with LeaseClient.local(bundle.bootstrap_source(fixed, config, bootstrap_guard=True)) as client:
        with pytest.raises(MaintenanceProtocolError): client.request("recover", nonce=nonce)
    assert (state / "blocked").exists()


@pytest.mark.parametrize("mutation", ["content", "digest", "extra", "missing", "config", "relative", "inside_clone"])
def test_rejects_corrupt_or_unsafe_bundle_before_serving(committed, mutation):
    repo, source, config = committed
    fixed = copy.deepcopy(bundle.build_bundle(repo, source))
    if mutation == "content": fixed["files"]["program_release.py"]["content"] = base64.b64encode(b"other").decode()
    elif mutation == "digest": fixed["bundle_sha256"] = "0" * 64
    elif mutation == "extra": fixed["files"]["unknown.py"] = fixed["files"]["program_release.py"]
    elif mutation == "missing": del fixed["files"]["program_release.py"]
    elif mutation == "config": config["untrusted_extra"] = "must not be embedded"
    elif mutation == "relative": config["state_dir"] = "relative/state"
    else:
        config["profiles"]["hmao"]["path"] = config["state_dir"]
        script = bundle.bootstrap_source(fixed, config)
        with pytest.raises(MaintenanceProtocolError): LeaseClient.local(script)
        assert not Path(config["state_dir"]).exists()
        return
    with pytest.raises(bundle.MaintenanceBundleError): bundle.bootstrap_source(fixed, config)
