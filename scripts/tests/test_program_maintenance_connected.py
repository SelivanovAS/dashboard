"""Connected local-process/Git proof, explicitly NOT production SSH/systemd.

Real MaintenanceLease v2, framed LeaseClient subprocess, four real run_lock
owners, four Git clones and bare remotes, and resumable Git installs are joined
by fixture-only host hooks. The marker represents admission; no systemd guard,
service drain, privileged bootstrap, remote SSH or production CLI is exercised.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import program_maintenance as lease_module
import program_maintenance_git as git_module
import program_maintenance_protocol as protocol
import program_release as release

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("connected_run_lock", ROOT / "ops/mac-local-run/run_lock.py")
run_lock = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_lock)
REGIONS = lease_module.REGIONS


def git(repo, *args):
    cp = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(repo), *args],
                        text=True, capture_output=True, check=True)
    return cp.stdout.strip()


def write(repo, path, content, mode=0o644):
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    target.chmod(mode)


def commit(repo, message):
    git(repo, "add", ".")
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def stamp(repo, region, source):
    result = {"schema_version": 1, "region": region, "source_commit": source,
        "source_repo": "SelivanovAS/dashboard", "repository": "SelivanovAS/dashboard",
        "profile_sha256": "b" * 64, "baseline_sha256": "c" * 64, "protected_prefixes": ["data/"],
        "files": {name: {"sha256": release.digest((repo / name).read_bytes()),
                         "mode": "100755" if (repo / name).stat().st_mode & 0o111 else "100644"}
                  for name in ("REGION", "script-a.py", "script-b.py")}}
    result["release_id"] = release.release_id(result)
    write(repo, release.LOCK, release.canonical(result))
    return result


def durable(path, document):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        stream.write(release.canonical(document))
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FixtureHooks:
    """Local test adapter. It does not assert real systemd quiescence."""
    def __init__(self, root):
        self.root = Path(root)
        self.state = self.root / "lease"
        self.marker = self.state / "blocked"
        self.held = None
        self.owned = []

    def repo(self, region):
        return self.root / region / "installed"

    def lock(self, region):
        return self.repo(region) / "ops/mac-local-run/.run.lock"

    def check_guard(self, marker):
        assert marker == self.marker and marker.is_file()

    def drain(self):
        # There are no service processes in this fixture. Real production drain
        # is intentionally outside this connected transport/Git proof.
        assert self.marker.is_file()

    def acquire_run_locks(self, nonce):
        assert self.held is None
        try:
            for region in REGIONS:
                path = self.lock(region)
                assert run_lock.acquire(str(path), os.getpid()) == 0
                self.owned.append(path)
            self.held = nonce
        except BaseException:
            for path in reversed(self.owned):
                assert run_lock.release(str(path), os.getpid()) == 0
            self.owned = []
            raise

    def assert_run_locks(self, nonce):
        assert self.held == nonce and len(self.owned) == 4
        for path in self.owned:
            owner = json.loads((path / "owner.json").read_text())
            assert owner["pid"] == os.getpid()
            assert owner["process_start"] == run_lock._process_start(os.getpid())

    def release_run_locks(self, nonce):
        self.assert_run_locks(nonce)
        assert self.marker.is_file()
        for path in reversed(self.owned):
            assert run_lock.release(str(path), os.getpid()) == 0
        self.owned, self.held = [], None

    def snapshot(self):
        result = {}
        for region in REGIONS:
            repo = self.repo(region)
            lock = json.loads((repo / release.LOCK).read_text())
            inventory = git_module.inventory(repo)
            durable(self.root / "inventories" / (region + ".json"), inventory)
            result[region] = {"head": git(repo, "rev-parse", "HEAD"), "release_id": lock["release_id"],
                "managed_sha256": release.digest(release.canonical(lock["files"])),
                "inventory_sha256": release.digest(release.canonical(inventory))}
        return result

    def baseline(self, snapshot, region):
        raw = (self.root / "inventories" / (region + ".json")).read_bytes()
        assert release.digest(raw) == snapshot[region]["inventory_sha256"]
        return json.loads(raw)

    def validate_snapshot(self, snapshot):
        for region in REGIONS:
            assert git(self.repo(region), "rev-parse", "HEAD") == snapshot[region]["head"]
            assert git_module.inventory(self.repo(region)) == self.baseline(snapshot, region)

    def target(self, journal):
        manifest = json.loads((self.root / "manifests" / (journal["release_id"] + ".json")).read_text())
        assert release.digest(release.canonical(manifest)) == journal["manifest_sha256"]
        assert manifest["source_commit"] == journal["source_commit"]
        assert manifest["release_id"] == journal["release_id"]
        assert manifest["region"] == journal["region"]
        return manifest

    def tx(self, target_id):
        return self.root / ("transaction-" + str(target_id)) / "journal.json"

    def validate_target(self, journal):
        self.target(journal)
        for region in REGIONS:
            original = self.baseline(journal["snapshot"], region)
            if region != journal["region"]:
                assert git_module.inventory(self.repo(region)) == original
            elif journal["active_target_id"] == 1:
                assert git_module.inventory(self.repo(region)) == original
            else:
                # Recovery first completes the old recorded transition behind
                # the closed gate, then chooses a new rollback transaction.
                git_module.verify_install(self.repo(region), self.tx(journal["active_target_id"] - 1))

    def registration(self, journal, base_sha, target_sha):
        return self.root / "registrations" / journal["nonce"] / str(journal["active_target_id"]) / (base_sha + "-" + target_sha)

    def register_target(self, journal, *, base_sha, target_sha, bundle_sha256, bundle_bytes):
        """Actual Git import for this local fixture; no production host claim."""
        self.assert_run_locks(journal["nonce"])
        repo = self.repo(journal["region"])
        manifest = self.target(journal)
        folder = self.registration(journal, base_sha, target_sha)
        folder.mkdir(parents=True, exist_ok=True)
        bundle = folder / "candidate.bundle"
        if bundle.exists():
            assert bundle.read_bytes() == bundle_bytes
        else:
            with bundle.open("xb") as stream:
                stream.write(bundle_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        assert release.digest(bundle.read_bytes()) == bundle_sha256
        git(repo, "fetch", "origin", "main")  # Obtain a newer data-only base, if any.
        git(repo, "bundle", "verify", str(bundle))
        heads = git(repo, "bundle", "list-heads", str(bundle)).splitlines()
        assert len(heads) == 1 and heads[0].split()[0] == target_sha
        git(repo, "bundle", "unbundle", str(bundle))  # Imports objects, no checkout.
        assert git(repo, "rev-parse", target_sha + "^") == base_sha
        assert json.loads(git(repo, "show", target_sha + ":" + release.LOCK)) == manifest
        baseline = json.loads(git(repo, "show", base_sha + ":" + release.LOCK))
        allowed = set(baseline["files"]) | set(manifest["files"]) | {release.LOCK}
        assert set(git(repo, "diff", "--name-only", base_sha, target_sha).splitlines()) <= allowed
        receipt = {"nonce": journal["nonce"], "target_id": journal["active_target_id"],
                   "manifest_sha256": journal["manifest_sha256"], "base_sha": base_sha,
                   "target_sha": target_sha, "bundle_sha256": bundle_sha256}
        path = folder / "receipt.json"
        if path.exists():
            assert json.loads(path.read_text()) == receipt
        else:
            durable(path, receipt)
        return {"evidence_sha256": release.digest(release.canonical(receipt))}

    def validate_publication(self, journal, attempt):
        self.assert_run_locks(journal["nonce"])
        folder = self.registration(journal, attempt["base_sha"], attempt["target_sha"])
        receipt = json.loads((folder / "receipt.json").read_text())
        assert receipt == {"nonce": journal["nonce"], "target_id": attempt["target_id"],
            "manifest_sha256": journal["manifest_sha256"], "base_sha": attempt["base_sha"],
            "target_sha": attempt["target_sha"], "bundle_sha256": release.digest((folder / "candidate.bundle").read_bytes())}
        repo = self.repo(journal["region"])
        assert git(repo, "cat-file", "-t", attempt["target_sha"]) == "commit"
        assert git(repo, "rev-parse", attempt["target_sha"] + "^") == attempt["base_sha"]

    def verify_installed(self, journal):
        manifest = self.target(journal)
        proof = git_module.verify_install(self.repo(journal["region"]), self.tx(journal["active_target_id"]))
        assert proof["source_commit"] == manifest["source_commit"]
        assert proof["release_id"] == manifest["release_id"]
        assert proof["installed_sha"] == journal["attempts"][-1]["target_sha"]
        for region in REGIONS:
            if region != journal["region"]:
                assert git_module.inventory(self.repo(region)) == self.baseline(journal["snapshot"], region)
        return {"target_id": journal["active_target_id"], "manifest_sha256": journal["manifest_sha256"],
            "installed_sha": proof["installed_sha"], "release_id": proof["release_id"],
            "evidence_sha256": proof["evidence_sha256"]}

    def apply_target(self, journal):
        self.assert_run_locks(journal["nonce"])
        assert self.marker.is_file() and journal["phase"] == "applying"
        manifest = self.target(journal)
        repo = self.repo(journal["region"])
        git(repo, "fetch", "origin", "main")
        transaction = self.tx(journal["active_target_id"])
        if not transaction.exists():
            self.validate_target(journal)
            git_module.begin_install(repo, journal["attempts"][-1]["target_sha"], transaction, manifest)
        def checkpoint(phase, path):
            fault = self.root / "crash-next-apply"
            if phase == "file" and fault.exists():
                fault.unlink()
                # The coordinator itself dies while it owns all four locks.
                # No parent-side installer can finish the partial checkout.
                os._exit(41)
        git_module.resume_install(repo, transaction, checkpoint=checkpoint)

    def verify_safe(self, journal):
        self.verify_installed(journal)
        repo = self.repo(journal["region"])
        git(repo, "fetch", "origin", "main")
        remote = git(repo, "rev-parse", "refs/remotes/origin/main")
        assert remote == git(repo, "rev-parse", "HEAD")
        proof = git_module.publication_fences(repo, remote, journal["attempts"])
        return {key: proof[key] for key in ("covered_attempt_ids", "evidence_sha256")}


def start_client(root):
    bootstrap = ("import sys,runpy\n"
        f"sys.path.insert(0, {str(ROOT / 'scripts')!r})\n"
        f"ns=runpy.run_path({str(Path(__file__).resolve())!r})\n"
        f"hooks=ns['FixtureHooks']({str(root)!r})\n"
        "from program_maintenance import MaintenanceLease\n"
        "from program_maintenance_protocol import serve\n"
        "serve(MaintenanceLease(hooks.state, hooks, boot_id='local-subprocess-fixture'))\n").encode()
    return protocol.LeaseClient.local(bootstrap, timeout=20)


@pytest.fixture
def connected(tmp_path):
    root = tmp_path.resolve()
    records = {}
    for region in REGIONS:
        area = root / region
        area.mkdir()
        remote, author, installed = (area / name for name in ("remote.git", "author", "installed"))
        remote.mkdir()
        git(remote, "init", "--bare", "--initial-branch=main")
        git(area, "clone", str(remote), str(author))
        git(author, "config", "user.name", "Connected fixture")
        git(author, "config", "user.email", "fixture@example.invalid")
        write(author, "REGION", (region + "\n").encode())
        write(author, "script-a.py", b"old a\n", 0o755)
        write(author, "script-b.py", b"old b\n")
        write(author, "data/cases.json", b'["original"]\n')
        write(author, ".gitignore", b".runtime/\nops/mac-local-run/.runtime/\n")
        initial_lock = stamp(author, region, "a" * 40)
        base = commit(author, "initial")
        git(author, "push", "origin", "main")
        git(area, "clone", str(remote), str(installed))
        write(installed, ".runtime/queue.json", b'["pending queue"]\n')
        write(installed, "ops/mac-local-run/.runtime/delivery.json", b'{"delivered": 4}\n')
        write(installed, "unmanaged.txt", b"operator file\n")
        (installed / "empty-unmanaged").mkdir()
        durable(root / "manifests" / (initial_lock["release_id"] + ".json"), initial_lock)
        records[region] = {"author": author, "installed": installed, "base": base, "lock": initial_lock}
    return root, records


def prepare_release(root, records):
    author = records["hmao"]["author"]
    write(author, "script-a.py", b"new a\n", 0o644)
    write(author, "script-b.py", b"new b\n", 0o755)
    manifest = stamp(author, "hmao", "d" * 40)
    target = commit(author, "prepared program")
    durable(root / "manifests" / (manifest["release_id"] + ".json"), manifest)
    return target, manifest


def reserve(client, manifest):
    value = client.request("reserve", region="hmao", release_id=manifest["release_id"],
        source_commit=manifest["source_commit"], manifest_sha256=release.digest(release.canonical(manifest)))
    client.request("prepare", nonce=value["nonce"])
    return value["nonce"]


def push_after_durable_intent(client, root, author, nonce, base, target):
    register_candidate(client, root, author, nonce, base, target)
    def push(allowed):
        disk = json.loads((root / "lease/journal.json").read_text())
        assert disk["phase"] == "publish_intent"
        assert disk["attempts"][-1]["target_sha"] == allowed == target
        assert git(root / "hmao/installed", "cat-file", "-t", target) == "commit"
        assert (root / "lease/blocked").is_file()
        assert all((root / region / "installed/ops/mac-local-run/.run.lock/owner.json").is_file()
                   for region in REGIONS)
        git(author, "push", "origin", allowed + ":refs/heads/main")
        return "accepted"
    return client.publish(nonce=nonce, base_sha=base, target_sha=target, push=push)[0]


def register_candidate(client, root, author, nonce, base, target):
    bundle = root / (target + ".bundle")
    ref = "refs/fixture-targets/" + target
    git(author, "update-ref", ref, target)
    git(author, "bundle", "create", "--version=2", str(bundle), ref, "^" + base)
    return client.register_target(nonce=nonce, base_sha=base, target_sha=target, bundle=bundle.read_bytes())


def assert_preserved(root, records, *, data):
    for region, record in records.items():
        repo = record["installed"]
        assert (repo / ".runtime/queue.json").read_bytes() == b'["pending queue"]\n'
        assert (repo / "ops/mac-local-run/.runtime/delivery.json").read_bytes() == b'{"delivered": 4}\n'
        assert (repo / "unmanaged.txt").read_bytes() == b"operator file\n"
        assert (repo / "empty-unmanaged").is_dir()
        assert (repo / "data/cases.json").read_bytes() == (data if region == "hmao" else b'["original"]\n')
        if region != "hmao":
            assert git(repo, "rev-parse", "HEAD") == record["base"]
            assert (repo / "script-a.py").read_bytes() == b"old a\n"
        assert not (repo / "ops/mac-local-run/.run.lock").exists()


def test_connected_publish_permit_real_push_install_and_finish(connected):
    root, records = connected
    target, manifest = prepare_release(root, records)
    with start_client(root) as client:
        nonce = reserve(client, manifest)
        for region in REGIONS:
            assert run_lock.acquire(str(root / region / "installed/ops/mac-local-run/.run.lock"), os.getpid()) == 1
        permit = push_after_durable_intent(client, root, records["hmao"]["author"], nonce,
                                          records["hmao"]["base"], target)
        client.request("apply", nonce=nonce, attempt_id=permit["attempt_id"])
        client.request("mark_verified", nonce=nonce)
        complete = client.request("finish", nonce=nonce)
        assert complete["safe_evidence"]["covered_attempt_ids"] == [1]
        assert complete["outcome"] == "installed"
        assert not client.request("inspect")["blocked"]
    assert_preserved(root, records, data=b'["original"]\n')
    assert (records["hmao"]["installed"] / "script-a.py").read_bytes() == b"new a\n"


def test_connected_unregistered_git_target_has_no_publish_permission(connected):
    root, records = connected
    target, manifest = prepare_release(root, records)
    called = []
    with start_client(root) as client:
        nonce = reserve(client, manifest)
        with pytest.raises(protocol.MaintenanceProtocolError):
            client.publish(nonce=nonce, base_sha=records["hmao"]["base"], target_sha=target,
                           push=called.append)
    assert called == [] and (root / "lease/blocked").is_file()
    assert json.loads((root / "lease/journal.json").read_text())["attempts"] == []
    assert git(records["hmao"]["author"], "ls-remote", "origin", "refs/heads/main").split()[0] == records["hmao"]["base"]


def test_connected_unpushed_target_objects_survive_rebased_second_intent(connected):
    root, records = connected
    target, manifest = prepare_release(root, records)
    author, installed, base = (records["hmao"][key] for key in ("author", "installed", "base"))
    with start_client(root) as client:
        nonce = reserve(client, manifest)
        register_candidate(client, root, author, nonce, base, target)
        first = client.request("publish_intent", nonce=nonce, base_sha=base, target_sha=target)
        assert first["attempt_id"] == 1
        assert git(installed, "cat-file", "-t", target) == "commit"
        # A data commit won the remote race. The permitted first target was never
        # published, but its objects must remain available for late-push fencing.
        git(author, "reset", "--hard", base)
        write(author, "data/cases.json", b'["original", "newer record"]\n')
        fresh = commit(author, "data won race before candidate push")
        git(author, "push", "origin", "main")
        write(author, "script-a.py", b"new a\n", 0o644)
        write(author, "script-b.py", b"new b\n", 0o755)
        write(author, release.LOCK, release.canonical(manifest))
        rebased = commit(author, "same approved program over newer data")
        permit = push_after_durable_intent(client, root, author, nonce, fresh, rebased)
        assert permit["attempt_id"] == 2 and permit["target_id"] == 1
        client.request("apply", nonce=nonce, attempt_id=2)
        client.request("mark_verified", nonce=nonce)
        complete = client.request("finish", nonce=nonce)
        assert complete["safe_evidence"]["covered_attempt_ids"] == [1, 2]
        assert complete["attempts"][0]["target_sha"] == target
        assert git(installed, "cat-file", "-t", target) == "commit"
    assert_preserved(root, records, data=b'["original", "newer record"]\n')


@pytest.mark.parametrize("disconnect_phase", ["after-push", "partial-checkout"])
def test_connected_disconnect_recovers_and_rolls_back_in_same_lease(connected, disconnect_phase):
    root, records = connected
    target, manifest = prepare_release(root, records)
    client = start_client(root)
    try:
        nonce = reserve(client, manifest)
        permit = push_after_durable_intent(client, root, records["hmao"]["author"], nonce,
                                          records["hmao"]["base"], target)
        if disconnect_phase == "partial-checkout":
            (root / "crash-next-apply").touch()
            with pytest.raises(protocol.MaintenanceProtocolError):
                client.request("apply", nonce=nonce, attempt_id=permit["attempt_id"])
        # SIGKILL the actual coordinator process: no context-manager release and
        # no EOF callback. The persistent marker and stale real lock owners remain.
        if client.process is not None:
            client.process.kill()
            client.process.wait(timeout=5)
    finally:
        client.close()
    assert (root / "lease/blocked").exists()
    assert json.loads((root / "lease/journal.json").read_text())["nonce"] == nonce
    with start_client(root) as recovered:
        recovered.request("recover", nonce=nonce)
        assert (root / "lease/blocked").exists()
        recovered.request("apply", nonce=nonce, attempt_id=1)
        # Fresh remote data arrives without modifying the running clone. Rollback
        # must be a descendant of this newer state, retaining the new records.
        author = records["hmao"]["author"]
        write(author, "data/cases.json", b'["original", "newer record"]\n')
        fresh = commit(author, "new data during recovery")
        git(author, "push", "origin", "main")
        old_manifest = records["hmao"]["lock"]
        write(author, "script-a.py", b"old a\n", 0o755)
        write(author, "script-b.py", b"old b\n")
        write(author, release.LOCK, release.canonical(old_manifest))
        rollback = commit(author, "rollback old program over current data")
        recovered.request("select_recovery_target", nonce=nonce, release_id=old_manifest["release_id"],
            source_commit=old_manifest["source_commit"], manifest_sha256=release.digest(release.canonical(old_manifest)),
            reason="rollback")
        rollback_permit = push_after_durable_intent(recovered, root, author, nonce, fresh, rollback)
        assert rollback_permit["target_id"] == 2
        recovered.request("apply", nonce=nonce, attempt_id=2)
        recovered.request("mark_verified", nonce=nonce)
        final = recovered.request("finish", nonce=nonce)
        assert final["nonce"] == nonce and len(final["targets"]) == 2
        assert [attempt["target_id"] for attempt in final["attempts"]] == [1, 2]
        assert final["safe_evidence"]["covered_attempt_ids"] == [1, 2]
        assert final["verification"]["target_id"] == 2
        assert final["verification"]["release_id"] == old_manifest["release_id"]
        assert not recovered.request("inspect")["blocked"]
    repo = records["hmao"]["installed"]
    assert git(repo, "rev-parse", "HEAD") == rollback
    git(repo, "merge-base", "--is-ancestor", fresh, rollback)
    assert (repo / "script-a.py").read_bytes() == b"old a\n"
    assert_preserved(root, records, data=b'["original", "newer record"]\n')
