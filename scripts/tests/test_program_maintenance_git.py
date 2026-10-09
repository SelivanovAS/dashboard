"""Real temporary Git repositories; no SSH, production services or delivery."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest
import program_maintenance_git as m
import program_release as r


def git(repo, *args, check=True):
    cp = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if check and cp.returncode:
        raise AssertionError(cp.stderr)
    return cp.stdout.strip() if check else cp


def write(repo, name, raw, mode=0o644):
    p = repo / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(raw)
    p.chmod(mode)


def stamp(repo, source="a" * 40, managed=("REGION", "script.py")):
    lock = {"schema_version": 1, "region": "hmao", "source_commit": source,
            "source_repo": "SelivanovAS/dashboard", "repository": "SelivanovAS/dashboard",
            "profile_sha256": "b" * 64, "baseline_sha256": "c" * 64, "protected_prefixes": ["data/"],
            "files": {name: {"sha256": r.digest((repo / name).read_bytes()),
                             "mode": "100755" if (repo / name).stat().st_mode & 0o111 else "100644"}
                      for name in managed}}
    lock["release_id"] = r.release_id(lock)
    write(repo, r.LOCK, r.canonical(lock))
    return lock


def commit(repo, msg):
    git(repo, "add", ".")
    git(repo, "commit", "-m", msg)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repos(tmp_path):
    origin = tmp_path / "remote.git"
    origin.mkdir()
    git(origin, "init", "--bare", "--initial-branch=main")
    author = tmp_path / "author"
    git(tmp_path, "clone", str(origin), str(author))
    git(author, "config", "user.name", "fixture")
    git(author, "config", "user.email", "fixture@example.invalid")
    write(author, "REGION", b"hmao\n")
    write(author, "script.py", b"old program\n", 0o755)
    write(author, "data/cases.json", b'{"cases": [1]}\n')
    write(author, ".gitignore", b".runtime/\nops/mac-local-run/.runtime/\n")
    old_lock = stamp(author)
    base = commit(author, "initial")
    git(author, "push", "origin", "main")
    installed = tmp_path / "installed"
    git(tmp_path, "clone", str(origin), str(installed))
    write(installed, ".runtime/queue.json", b'"protected runtime"\n')
    write(installed, "ops/mac-local-run/.runtime/delivery.json", b'"protected delivery"\n')
    write(installed, "ops/mac-local-run/.run.lock/owner.json", b'"ephemeral"\n')
    write(installed, "ops/mac-local-run/.run.lock.guard", b"ephemeral")
    write(installed, "untracked-queue.json", b'"untouched queue"\n')
    (installed / "empty-untracked").mkdir()
    (installed / "outside-link").symlink_to("/missing/secret")
    return origin, author, installed, base, old_lock


def target(repos, *, data=False, nested=False, remove=False):
    origin, author, installed, base, old_lock = repos
    write(author, "script.py", b"new program\n", 0o644)
    managed = ["REGION", "script.py"]
    if nested:
        write(author, "new/path/module.py", b"nested program\n")
        managed.append("new/path/module.py")
    if remove:
        (author / "script.py").unlink()
        managed.remove("script.py")
    if data:
        write(author, "data/cases.json", b'{"cases": [1, 2]}\n')
    lock = stamp(author, "d" * 40, managed)
    sha = commit(author, "prepared release")
    git(author, "push", "origin", "main")
    git(installed, "fetch", "origin", "main")
    return sha, lock


def journal(tmp_path, name="install"):
    return tmp_path / name / "journal.json"


def test_install_preserves_all_runtime_and_untracked_files(repos, tmp_path):
    _, _, installed, _, _ = repos
    sha, lock = target(repos, data=True, nested=True)
    before = m.inventory(installed)
    assert ".runtime/queue.json" in before
    assert "ops/mac-local-run/.runtime/delivery.json" in before
    assert "ops/mac-local-run/.run.lock/owner.json" not in before
    path = journal(tmp_path)
    doc = m.begin_install(installed, sha, path, lock)
    assert doc["inventory"] == before
    result = m.resume_install(installed, path)
    assert result["installed_sha"] == sha
    assert result["release_id"] == lock["release_id"]
    after = m.inventory(installed)
    for name in before.keys() - doc["writes"].keys():
        assert after[name] == before[name]
    assert (installed / "data/cases.json").read_bytes() == b'{"cases": [1, 2]}\n'
    assert git(installed, "diff", "--name-only", "HEAD") == ""
    assert m.resume_install(installed, path) == result


@pytest.mark.parametrize("phase", ["file", "index", "head"])
def test_crash_resume_at_each_phase(repos, tmp_path, phase):
    _, _, installed, _, _ = repos
    sha, lock = target(repos, data=True, nested=True)
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    def crash(point, name):
        if point == phase:
            raise RuntimeError("power cut")
    with pytest.raises(RuntimeError, match="power cut"):
        m.resume_install(installed, path, checkpoint=crash)
    assert (installed / ".git/index.lock").exists()
    assert m.resume_install(installed, path)["installed_sha"] == sha
    assert not (installed / ".git/index.lock").exists()


def test_resume_after_every_file_boundary(repos, tmp_path):
    _, _, installed, _, _ = repos
    sha, lock = target(repos, data=True, nested=True)
    path = journal(tmp_path)
    doc = m.begin_install(installed, sha, path, lock)
    changed = []
    def crash(point, name):
        if point == "file":
            changed.append(name)
            raise RuntimeError("power cut")
    for _ in doc["writes"]:
        with pytest.raises(RuntimeError):
            m.resume_install(installed, path, checkpoint=crash)
    assert set(changed) == set(doc["writes"])
    assert m.resume_install(installed, path)["installed_sha"] == sha


@pytest.mark.parametrize("name", [".runtime/queue.json", "ops/mac-local-run/.runtime/delivery.json",
                                 "untracked-queue.json", "script.py", "data/cases.json"])
def test_unknown_edits_fail_before_any_mutation(repos, tmp_path, name):
    _, _, installed, base, _ = repos
    sha, lock = target(repos)
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    write(installed, name, b"external edit")
    snapshot = m.inventory(installed)
    with pytest.raises(m.MaintenanceGitError):
        m.resume_install(installed, path)
    assert m.inventory(installed) == snapshot
    assert git(installed, "rev-parse", "HEAD") == base


@pytest.mark.parametrize("fault", ["new-file", "deleted-empty-directory", "changed-directory-mode", "changed-symlink", "foreign-index-lock", "symlink-index-lock"])
def test_unmanaged_inventory_and_index_lock_conflicts(repos, tmp_path, fault):
    _, _, installed, _, _ = repos
    sha, lock = target(repos)
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    if fault == "new-file": write(installed, "external", b"new")
    if fault == "deleted-empty-directory": (installed / "empty-untracked").rmdir()
    if fault == "changed-directory-mode": (installed / "empty-untracked").chmod(0o700)
    if fault == "changed-symlink":
        (installed / "outside-link").unlink()
        (installed / "outside-link").symlink_to("/other")
    if fault == "foreign-index-lock": write(installed, ".git/index.lock", b"another git")
    if fault == "symlink-index-lock": (installed / ".git/index.lock").symlink_to("/missing")
    with pytest.raises(m.MaintenanceGitError): m.resume_install(installed, path)


@pytest.mark.parametrize("fault", ["dirty", "staged", "assume-unchanged", "symlink-parent", "untracked-collision", "unknown-program"])
def test_begin_rejects_unknown_changes(repos, tmp_path, fault):
    origin, author, installed, _, _ = repos
    sha, lock = target(repos, nested=True)
    if fault == "dirty": write(installed, "script.py", b"dirty")
    if fault == "staged":
        write(installed, "script.py", b"dirty")
        git(installed, "add", "script.py")
    if fault == "assume-unchanged": git(installed, "update-index", "--assume-unchanged", "script.py")
    if fault == "symlink-parent": (installed / "new").symlink_to(tmp_path)
    if fault == "untracked-collision": write(installed, "new/path/module.py", b"other")
    if fault == "unknown-program":
        write(author, ".gitignore", b"new unknown configuration\n")
        sha = commit(author, "unmanaged program change")
        git(author, "push", "origin", "main")
        git(installed, "fetch", "origin", "main")
    before = m.inventory(installed)
    with pytest.raises(m.MaintenanceGitError):
        m.begin_install(installed, sha, journal(tmp_path), lock)
    assert m.inventory(installed) == before


def test_managed_deletion(repos, tmp_path):
    _, _, installed, _, _ = repos
    sha, lock = target(repos, remove=True)
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    m.resume_install(installed, path)
    assert not (installed / "script.py").exists()
    assert (installed / "untracked-queue.json").exists()


def test_rollback_is_new_commit_over_fresh_data(repos, tmp_path):
    origin, author, installed, _, old_lock = repos
    sha, lock = target(repos, data=True)
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    m.resume_install(installed, path)
    write(author, "data/cases.json", b'{"cases": [1, 2, 3]}\n')
    write(author, "script.py", b"old program\n", 0o755)
    stamp(author)
    rollback = commit(author, "rollback program over current data")
    git(author, "push", "origin", "main")
    git(installed, "fetch", "origin", "main")
    path2 = journal(tmp_path, "rollback")
    m.begin_install(installed, rollback, path2, old_lock)
    m.resume_install(installed, path2)
    assert (installed / "data/cases.json").read_bytes() == b'{"cases": [1, 2, 3]}\n'
    assert git(installed, "merge-base", "--is-ancestor", sha, rollback) == ""


def intent(base, sha, number=1):
    return {"attempt_id": number, "base_sha": base, "target_sha": sha}


def test_all_intents_require_fencing_and_data_descendants_are_safe(repos):
    origin, author, installed, base, _ = repos
    sha, lock = target(repos)
    with pytest.raises(m.MaintenanceGitError, match="Late push"):
        m.publication_fences(installed, base, [intent(base, sha)])
    assert m.publication_fences(installed, sha, [intent(base, sha)])["attempts"][0]["relationship"] == "reached"
    write(author, "data/cases.json", b"later data")
    data = commit(author, "data")
    git(author, "push", "origin", "main")
    git(installed, "fetch", "origin", "main")
    assert m.publication_fences(installed, data, [intent(base, sha)])["covered_attempt_ids"] == [1]
    with pytest.raises(m.MaintenanceGitError, match="Late push 2"):
        m.publication_fences(installed, sha, [intent(base, sha), intent(sha, data, 2)])


def wait_for(path, timeout=8):
    until = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > until: raise AssertionError("Git hook did not arrive")
        time.sleep(0.01)


def hook(origin, target_sha, hookname, reached, unblock):
    code = f'''#!{sys.executable}
import pathlib, sys, time
for line in sys.stdin:
    old, new, ref = line.split()
    if new == {target_sha!r}:
        pathlib.Path({str(reached)!r}).touch()
        deadline = time.monotonic() + 12
        while not pathlib.Path({str(unblock)!r}).exists():
            if time.monotonic() > deadline: sys.exit(1)
            time.sleep(.01)
'''
    write(origin, "hooks/" + hookname, code.encode(), 0o755)


@pytest.mark.parametrize("old_wins", [True, False])
def test_delayed_push_after_advertisement_both_race_orders(repos, tmp_path, old_wins):
    origin, author, installed, base, old_lock = repos
    # Prepare two descendants of one base, without publishing either.
    write(author, "script.py", b"candidate")
    stamp(author, "d" * 40)
    candidate = commit(author, "candidate")
    git(author, "checkout", "-b", "safe-fence", base)
    git(author, "commit", "--allow-empty", "-m", "safe fence")
    fence = git(author, "rev-parse", "HEAD")
    arrived, go = tmp_path / "arrived", tmp_path / "go"
    hook(origin, candidate, "pre-receive", arrived, go)
    delayed = subprocess.Popen(["git", "-C", str(author), "push", "origin", candidate + ":refs/heads/main"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        wait_for(arrived)
        if old_wins:
            go.touch()
            assert delayed.wait(timeout=10) == 0
            assert git(author, "push", "origin", fence + ":refs/heads/main", check=False).returncode != 0
            # Recovery builds a new safe-program commit on the actual new remote.
            git(author, "checkout", "-b", "recovery", candidate)
            write(author, "script.py", b"old program\n", 0o755)
            stamp(author)
            fence = commit(author, "safe rollback after late target won")
            git(author, "push", "origin", fence + ":refs/heads/main")
        else:
            git(author, "push", "origin", fence + ":refs/heads/main")
            go.touch()
            assert delayed.wait(timeout=10) != 0
        git(installed, "fetch", "origin", "main")
        # Candidate objects needed to prove topology even when its ref never won.
        git(installed, "fetch", str(author), candidate)
        actual = git(installed, "rev-parse", "origin/main")
        proof = m.publication_fences(installed, actual, [intent(base, candidate)])
        assert proof["attempts"][0]["relationship"] == ("reached" if old_wins else "diverged")
        assert m._installed(installed, actual) == old_lock
    finally:
        go.touch()
        delayed.communicate(timeout=10)


def test_push_ack_lost_after_remote_ref_update(repos, tmp_path):
    origin, author, installed, base, _ = repos
    write(author, "script.py", b"candidate")
    stamp(author, "d" * 40)
    candidate = commit(author, "candidate")
    arrived, go = tmp_path / "arrived", tmp_path / "go"
    hook(origin, candidate, "post-receive", arrived, go)
    delayed = subprocess.Popen(["git", "-C", str(author), "push", "origin", candidate + ":refs/heads/main"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_for(arrived)
        assert git(origin, "rev-parse", "main") == candidate
        os.killpg(delayed.pid, signal.SIGKILL)
        delayed.wait(timeout=10)
        git(installed, "fetch", "origin", "main")
        actual = git(installed, "rev-parse", "origin/main")
        assert m.publication_fences(installed, actual, [intent(base, candidate)])["attempts"][0]["relationship"] == "reached"
    finally:
        go.touch()
        delayed.communicate(timeout=10)


def test_repository_hooks_and_redirecting_environment_cannot_execute(repos, tmp_path, monkeypatch):
    _, _, installed, _, _ = repos
    sha, lock = target(repos)
    sentinel = tmp_path / "hook-executed"
    write(installed, ".git/hooks/reference-transaction", ("#!/bin/sh\ntouch '" + str(sentinel) + "'\n").encode(), 0o755)
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "wrong-git-dir"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "wrong-index"))
    path = journal(tmp_path)
    m.begin_install(installed, sha, path, lock)
    m.resume_install(installed, path)
    assert not sentinel.exists()
    assert not (tmp_path / "wrong-index").exists()


def test_corrupted_journal_and_staged_blob_fail_closed(repos, tmp_path):
    _, _, installed, base, _ = repos
    sha, lock = target(repos)
    path = journal(tmp_path)
    doc = m.begin_install(installed, sha, path, lock)
    original = path.read_bytes()
    path.write_bytes(original.replace(b'"schema_version": 1', b'"schema_version": 2'))
    with pytest.raises(m.MaintenanceGitError): m.resume_install(installed, path)
    path.write_bytes(original)
    first = next(change for change in doc["writes"].values() if change["new"] is not None)
    (path.parent / first["blob"]).write_bytes(b"corrupt")
    with pytest.raises(m.MaintenanceGitError, match="Staged blob changed"):
        m.resume_install(installed, path)
    assert git(installed, "rev-parse", "HEAD") == base


def test_delayed_push_before_connection_is_rejected_after_safe_fence(repos):
    origin, author, installed, base, _ = repos
    write(author, "script.py", b"candidate")
    stamp(author, "d" * 40)
    candidate = commit(author, "candidate")
    git(author, "checkout", "-b", "safe-fence", base)
    git(author, "commit", "--allow-empty", "-m", "safe fence")
    fence = git(author, "rev-parse", "HEAD")
    git(author, "push", "origin", fence + ":refs/heads/main")
    git(installed, "fetch", "origin", "main")
    git(installed, "fetch", str(author), candidate)
    assert m.publication_fences(installed, fence, [intent(base, candidate)])["attempts"][0]["relationship"] == "diverged"
    assert git(author, "push", "origin", candidate + ":refs/heads/main", check=False).returncode != 0
    assert git(origin, "rev-parse", "main") == fence
