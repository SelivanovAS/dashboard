"""Offline release rehearsals use real Git objects and preserve data/history."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

import program_release as release


def git(repo, *args):
    cp = subprocess.run(["git", "-C", str(repo), *args], check=True, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, text=True,
                        env={**os.environ, "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
                             "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid"})
    return cp.stdout.strip()


def write(repo, name, content, executable=False):
    path = repo / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if isinstance(content, bytes) else content.encode())
    path.chmod(0o755 if executable else 0o644)


def commit(repo, message="fixture"):
    git(repo, "add", "--all")
    git(repo, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def init(path):
    path.mkdir()
    git(path, "init", "-b", "main")
    return path


def record(content, executable=False):
    return {"sha256": release.digest(content.encode()), "mode": "100755" if executable else "100644"}


@pytest.fixture
def world(tmp_path):
    target = init(tmp_path / "target")
    write(target, "app.js", "old\n")
    write(target, "obsolete.sh", "old script\n", executable=True)
    write(target, "data/cases.json", '{"case":"live"}\n')
    write(target, "operator-notes.md", "unmanaged\n")
    target_sha = commit(target)
    source = init(tmp_path / "source")
    write(source, "app.js", "new\n")
    write(source, "bin/run.sh", "#!/bin/sh\nexit 0\n", executable=True)
    write(source, "deployment/regions/hmao/files/REGION", "hmao\n")
    profile = {"region": "hmao", "repository": "SelivanovAS/dashboard", "site_url": "https://example.invalid",
               "vps_path": "/opt/court-monitor/dashboard", "files": {"REGION": "deployment/regions/hmao/files/REGION"}}
    baseline = {"schema_version": 1, "region": "hmao", "source_commit": target_sha,
                "files": {"app.js": record("old\n"), "obsolete.sh": record("old script\n", True),
                          "bin/run.sh": None, "REGION": None}}
    manifest = {"schema_version": 1, "common_files": ["app.js", "bin/run.sh"]}
    write(source, "deployment/regions/hmao/profile.json", release.canonical(profile))
    write(source, "deployment/baselines/hmao.json", release.canonical(baseline))
    write(source, "deployment/manifest.json", release.canonical(manifest))
    sha = commit(source)
    out = tmp_path / "packages"
    release.build_release(source, sha, out, ["hmao"])
    return {"source": source, "target": target, "sha": sha, "package": out / "hmao", "tmp": tmp_path}


def install_prepared(world, package=None, rollback=False):
    target = world["target"]
    result = release.promote_release(package or world["package"], target, "hmao", rollback=rollback)
    git(target, "merge", "--ff-only", result["commit"])
    return result


def test_build_pinned_and_deterministic_despite_dirty_source(world):
    write(world["source"], "app.js", "dirty not in package\n")
    next_out = world["tmp"] / "again"
    release.build_release(world["source"], world["sha"], next_out, ["hmao"])
    first, second = world["package"], next_out / "hmao"
    assert (first / "package.json").read_bytes() == (second / "package.json").read_bytes()
    assert (second / "files/app.js").read_text() == "new\n"
    assert os.access(second / "files/bin/run.sh", os.X_OK)
    with pytest.raises(release.ReleaseError, match="полный SHA"):
        release.build_release(world["source"], "main", world["tmp"] / "bad", ["hmao"])


def test_rehearsal_protects_unmanaged_data_checkout_and_idempotence(world):
    target = world["target"]
    write(target, "data/cases.json", '{"case":"dirty newer"}\n')
    write(target, "private-note.txt", "untracked\n")
    old_head = git(target, "rev-parse", "HEAD")
    status = git(target, "status", "--porcelain")
    result = release.promote_release(world["package"], target, "hmao")
    assert result["status"] == "prepared"
    assert result["install_pending"] is True and result["published"] is False
    assert git(target, "rev-parse", "HEAD") == old_head
    assert git(target, "status", "--porcelain") == status
    assert git(target, "show", result["commit"] + ":data/cases.json") == '{"case":"live"}'
    assert git(target, "show", result["commit"] + ":operator-notes.md") == "unmanaged"
    git(target, "merge", "--ff-only", result["commit"])
    assert (target / "data/cases.json").read_text() == '{"case":"dirty newer"}\n'
    assert not (target / "obsolete.sh").exists()
    assert release.verify_release(target, "hmao", world["package"])["online_verified"] is False
    again = release.promote_release(world["package"], target, "hmao")
    assert again["unchanged"] is True
    assert again["commit"] == result["commit"]
    assert again["prepared_ref"] is None


def test_first_rollback_restores_old_code_but_keeps_new_data(world):
    baseline_out = world["tmp"] / "rollback"
    release.build_release(world["source"], world["sha"], baseline_out, ["hmao"], world["target"])
    install_prepared(world)
    write(world["target"], "data/cases.json", '{"case":"newest"}\n')
    data_sha = commit(world["target"], "new data")
    result = install_prepared(world, baseline_out / "hmao", rollback=True)
    assert result["operation"] == "rollback"
    assert git(world["target"], "rev-parse", "HEAD^") == data_sha
    assert (world["target"] / "app.js").read_text() == "old\n"
    assert (world["target"] / "obsolete.sh").read_text() == "old script\n"
    assert not (world["target"] / "bin/run.sh").exists()
    assert (world["target"] / "data/cases.json").read_text() == '{"case":"newest"}\n'
    release.verify_release(world["target"], "hmao", baseline_out / "hmao")


@pytest.mark.parametrize("tracked", [False, True])
def test_unknown_code_drift_fails_before_commit(world, tracked):
    write(world["target"], "app.js", "operator change\n")
    if tracked:
        commit(world["target"], "operator code")
    before = git(world["target"], "rev-parse", "HEAD")
    with pytest.raises(release.ReleaseError):
        release.promote_release(world["package"], world["target"], "hmao")
    assert git(world["target"], "rev-parse", "HEAD") == before


def test_untracked_and_ignored_collision_are_rejected(world):
    write(world["target"], ".gitignore", "REGION\n")
    commit(world["target"], "ignore")
    write(world["target"], "REGION", "operator file\n")
    with pytest.raises(release.ReleaseError, match="Посторонний"):
        release.plan_release(world["package"], world["target"], "hmao")


def test_parent_symlink_in_checkout_and_package_rejected(world):
    external = world["tmp"] / "external"
    external.mkdir()
    (world["target"] / "bin").symlink_to(external, target_is_directory=True)
    with pytest.raises(release.ReleaseError, match="Символическая"):
        release.plan_release(world["package"], world["target"], "hmao")
    (world["target"] / "bin").unlink()
    package_file = world["package"] / "files/app.js"
    package_file.unlink()
    package_file.symlink_to(world["source"] / "app.js")
    with pytest.raises(release.ReleaseError, match="Символические"):
        release.load_package(world["package"])


def test_git_symlink_parent_rejected_even_without_checkout(world):
    (world["target"] / "bin").symlink_to("somewhere")
    commit(world["target"], "symlink")
    with pytest.raises(release.ReleaseError, match="Родитель"):
        release.plan_release(world["package"], world["target"], check_checkout=False)


@pytest.mark.parametrize("path", ["../outside", "/absolute", "a/../../escape", "a//b", "data/cases.json",
                                  ".env.local", "cloudflare-worker/.dev.vars", ".git/config", "ops/court_probe/report.txt"])
def test_path_and_protected_data_rejection(path):
    with pytest.raises(release.ReleaseError):
        release.validate_records({path: record("anything")})


def test_manifest_cannot_include_runtime_files(world):
    source = world["source"]
    manifest = {"schema_version": 1, "common_files": ["data/cases.json"]}
    write(source, "deployment/manifest.json", release.canonical(manifest))
    sha = commit(source)
    with pytest.raises(release.ReleaseError, match="Защищённый"):
        release.build_release(source, sha, world["tmp"] / "bad", ["hmao"])


def test_tampered_package_and_bad_hash_fail_closed(world):
    package_path = world["package"] / "package.json"
    payload = json.loads(package_path.read_text())
    payload["files"]["app.js"]["sha256"] = "incorrect"
    payload["release_id"] = release.release_id(release.lock_from_package(payload))
    package_path.write_bytes(release.canonical(payload))
    with pytest.raises(release.ReleaseError, match="хеш"):
        release.load_package(world["package"])


def test_extra_package_file_and_duplicate_json_rejected(world):
    write(world["package"], "files/unlisted.txt", "extra")
    with pytest.raises(release.ReleaseError, match="перечень"):
        release.load_package(world["package"])
    with pytest.raises(release.ReleaseError, match="Повторный"):
        release.read_json('{"a": 1, "a": 2}', "fixture")


def test_only_data_remote_advance_accepted(world):
    target = world["target"]
    initial = git(target, "rev-parse", "HEAD")
    write(target, "data/cases.json", "new data")
    after = commit(target, "data")
    package = release.load_package(world["package"])
    release.check_remote_advance(target, initial, after, package)
    write(target, "app.js", "remote code drift")
    after_code = commit(target, "code")
    with pytest.raises(release.ReleaseError, match="программы/настроек"):
        release.check_remote_advance(target, after, after_code, package)


def test_managed_deletion_fails_if_changed_after_install(world):
    install_prepared(world)
    baseline_out = world["tmp"] / "rollback"
    release.build_release(world["source"], world["sha"], baseline_out, ["hmao"], world["target"])
    write(world["target"], "bin/run.sh", "operator owns this now\n", executable=True)
    commit(world["target"], "changed managed script")
    with pytest.raises(release.ReleaseError, match="Неизвестное"):
        release.promote_release(baseline_out / "hmao", world["target"], "hmao", rollback=True)


def test_wrong_region_or_remote_never_publishes(world):
    with pytest.raises(release.ReleaseError, match="другой территории"):
        release.promote_release(world["package"], world["target"], "tyumen")
    with pytest.raises(release.ReleaseError, match="не соответствует"):
        release.promote_release(world["package"], world["target"], "hmao", push=True,
                                remote="git@github.com:SelivanovAS/dashboard-tyumen.git")
    with pytest.raises(release.ReleaseError, match="SSH"):
        release.promote_release(world["package"], world["target"], "hmao", push=True,
                                remote="https://github.com/SelivanovAS/dashboard.git")


def test_unconfirmed_vps_preserves_published_status(monkeypatch):
    result = {"published": True, "commit": "a" * 40, "worker_deploy_required": False}
    monkeypatch.setattr(release.Path, "is_file", lambda _: True)
    monkeypatch.setattr(release.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 1, '{"error":"busy"}', ""))
    release.install_vps(result, {"region": "hmao", "source_commit": "b" * 40}, "server", None)
    assert result["status"] == "published_not_installed"
    assert result["install_pending"] is True
    assert result["commit"] == "a" * 40


def test_idempotent_remote_release_can_retry_install_from_old_local(world):
    target = world["target"]
    initial = git(target, "rev-parse", "HEAD")
    result = install_prepared(world)
    release.check_remote_advance(target, initial, result["commit"], release.load_package(world["package"]))


@pytest.mark.parametrize("path", ["ops/mac-local-run/.runtime/delivery_txn.json", "outputs/report.json",
                                  "exports/cases.csv", "ops/initial_import/report.json", "scripts/__pycache__/x.pyc"])
def test_nested_runtime_and_report_directories_hard_blocked(path):
    with pytest.raises(release.ReleaseError, match="Защищённый"):
        release.validate_records({path: record("private")})


def test_declared_extra_protected_prefix_enforced_at_build_and_load(world):
    manifest_path = world["source"] / "deployment/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["protected_prefixes"] = ["app.js"]
    manifest_path.write_bytes(release.canonical(manifest))
    sha = commit(world["source"], "protect")
    with pytest.raises(release.ReleaseError, match="защищён manifest"):
        release.build_release(world["source"], sha, world["tmp"] / "protected", ["hmao"])
    payload_path = world["package"] / "package.json"
    payload = json.loads(payload_path.read_text())
    payload["protected_prefixes"] = ["app.js"]
    payload["release_id"] = release.release_id(release.lock_from_package(payload))
    payload_path.write_bytes(release.canonical(payload))
    with pytest.raises(release.ReleaseError, match="защищён manifest"):
        release.load_package(world["package"])


def test_later_release_adds_new_owned_file_without_rewriting_initial_baseline(world):
    install_prepared(world)
    source = world["source"]
    write(source, "new-module.py", "answer = 42\n")
    path = source / "deployment/manifest.json"
    manifest = json.loads(path.read_text())
    manifest["common_files"].append("new-module.py")
    path.write_bytes(release.canonical(manifest))
    sha = commit(source, "new module")
    out = world["tmp"] / "next"
    release.build_release(source, sha, out, ["hmao"])
    result = install_prepared(world, out / "hmao")
    assert result["added"] == ["new-module.py"]
    assert (world["target"] / "new-module.py").read_text() == "answer = 42\n"


def test_first_rollback_retains_safe_workflow_trigger_policy(world):
    source, target = world["source"], world["target"]
    workflow = ".github/workflows/probe_writ_section.yml"
    old = "name: Probe\non:\n  push:\n    paths: ['scripts/**']\njobs: {}\n"
    safe = "name: Probe\non:\n  workflow_dispatch:\njobs: {}\n"
    write(target, workflow, old)
    target_sha = commit(target, "old automatic probe")
    baseline_path = source / "deployment/baselines/hmao.json"
    baseline = json.loads(baseline_path.read_text())
    baseline["files"][workflow] = record(old)
    baseline["source_commit"] = target_sha
    baseline_path.write_bytes(release.canonical(baseline))
    profile_path = source / "deployment/regions/hmao/profile.json"
    profile = json.loads(profile_path.read_text())
    profile["files"][workflow] = "deployment/workflows/probe_writ_section.yml"
    profile_path.write_bytes(release.canonical(profile))
    write(source, "deployment/workflows/probe_writ_section.yml", safe)
    sha = commit(source, "manual-only policy")
    out = world["tmp"] / "safe-baseline"
    release.build_release(source, sha, out, ["hmao"], target)
    payload = release.load_package(out / "hmao")
    assert payload["policy_source_commit"] == sha
    assert payload["retained_workflow_policy"] == [workflow]
    assert (out / "hmao/files" / workflow).read_text() == safe
    assert payload["baseline"]["files"][workflow] == record(old)


def local_remote(world, monkeypatch, before_push=None, lost_response=False):
    """Keep the public SSH validation; redirect only Git transport to a local bare repo."""
    bare = world["tmp"] / "remote.git"
    git(world["tmp"], "clone", "--bare", str(world["target"]), str(bare))
    original_git = release.git
    remote = "ssh://git@ssh.github.com:443/SelivanovAS/dashboard.git"
    pushes = []

    def transport(repo, *args, **kwargs):
        if args[0] in {"fetch", "push"} and remote in args:
            if args[0] == "push":
                pushes.append(args)
                if len(pushes) == 1 and before_push:
                    before_push(bare)
            rewritten = tuple(str(bare) if arg == remote else arg for arg in args)
            cp = original_git(repo, *rewritten, **kwargs)
            if args[0] == "push" and lost_response and cp.returncode == 0:
                return subprocess.CompletedProcess(cp.args, 1, b"", b"lost response")
            return cp
        return original_git(repo, *args, **kwargs)

    monkeypatch.setattr(release, "git", transport)
    # These tests exercise real Git transport and simulate the separate VPS.
    monkeypatch.setattr(release, "preflight_vps", lambda *args: {"preflight_passed": True})
    monkeypatch.setattr(release, "install_vps", lambda result, *args:
                        result.update(status="published_not_installed", install_pending=True))
    return bare, remote, pushes


def test_remote_racing_data_commit_preserved_by_normal_push_retry(world, monkeypatch):
    def race(bare):
        writer = world["tmp"] / "writer"
        git(world["tmp"], "clone", str(bare), str(writer))
        write(writer, "data/cases.json", '{"case":"racing"}\n')
        commit(writer, "normal parser data")
        git(writer, "push", "origin", "main")
    bare, remote, pushes = local_remote(world, monkeypatch, before_push=race)
    original_head = git(world["target"], "rev-parse", "HEAD")
    result = release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    assert result["published"] is True
    assert result["install_pending"] is True
    assert len(pushes) == 2
    assert all(not any("force" in arg for arg in args) for args in pushes)
    assert git(bare, "show", "main:data/cases.json") == '{"case":"racing"}'
    assert git(bare, "show", "main:app.js") == "new"
    assert git(world["target"], "rev-parse", "HEAD") == original_head


def test_successful_push_with_lost_response_verified_without_duplicate_commit(world, monkeypatch):
    bare, remote, pushes = local_remote(world, monkeypatch, lost_response=True)
    result = release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    assert result["published"] is True
    assert len(pushes) == 1
    repeat = release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    assert repeat["unchanged"] is True
    assert repeat["commit"] == result["commit"]
    assert len(pushes) == 1
    assert git(bare, "rev-parse", "main") == result["commit"]


def test_remote_racing_code_commit_fails_without_overwriting_it(world, monkeypatch):
    def race(bare):
        writer = world["tmp"] / "writer"
        git(world["tmp"], "clone", str(bare), str(writer))
        write(writer, "app.js", "operator remote code\n")
        commit(writer, "operator code")
        git(writer, "push", "origin", "main")
    bare, remote, pushes = local_remote(world, monkeypatch, before_push=race)
    with pytest.raises(release.ReleaseError, match="программы/настроек"):
        release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    assert len(pushes) == 1
    assert git(bare, "show", "main:app.js") == "operator remote code"


def test_repeated_publish_retries_failed_vps_install(world, monkeypatch):
    bare, remote, pushes = local_remote(world, monkeypatch)
    installs = []
    def install(result, package, host, identity):
        installs.append((result["commit"], host))
        result.update(status="published_not_installed", install_pending=True)
    monkeypatch.setattr(release, "install_vps", install)
    monkeypatch.setattr(release, "preflight_vps", lambda *args: {"preflight_passed": True})
    first = release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    second = release.promote_release(world["package"], world["target"], "hmao", push=True, remote=remote, vps_host="host", ssh_key="identity")
    assert len(installs) == 2
    assert installs[0] == installs[1] == (first["commit"], "host")
    assert second["unchanged"] is True and len(pushes) == 1


def test_worker_change_remains_pending_on_idempotent_retry(world):
    source = world["source"]
    write(source, "cloudflare-worker/worker.js", "export default {};\n")
    path = source / "deployment/manifest.json"
    manifest = json.loads(path.read_text())
    manifest["common_files"].append("cloudflare-worker/worker.js")
    path.write_bytes(release.canonical(manifest))
    sha = commit(source, "worker change")
    out = world["tmp"] / "worker"
    release.build_release(source, sha, out, ["hmao"])
    first = install_prepared(world, out / "hmao")
    assert first["worker_deploy_required"] is True
    again = release.promote_release(out / "hmao", world["target"], "hmao")
    assert again["unchanged"] is True
    assert again["worker_deploy_required"] is True


def test_package_region_file_must_match_region_even_with_valid_hashes(world):
    package = world["package"]
    write(package, "files/REGION", "tyumen\n")
    path = package / "package.json"
    payload = json.loads(path.read_text())
    payload["files"]["REGION"] = record("tyumen\n")
    payload["release_id"] = release.release_id(release.lock_from_package(payload))
    path.write_bytes(release.canonical(payload))
    with pytest.raises(release.ReleaseError, match="REGION не соответствует"):
        release.load_package(package)


@pytest.mark.parametrize("index_flag", ["--assume-unchanged", "--skip-worktree"])
def test_verify_and_promote_hash_real_files_despite_hidden_index_flags(world, index_flag):
    install_prepared(world)
    target = world["target"]
    git(target, "update-index", index_flag, "app.js")
    write(target, "app.js", "hidden local edit\n")
    assert git(target, "status", "--porcelain") == ""
    with pytest.raises(release.ReleaseError, match="независимо от флагов"):
        release.verify_release(target, "hmao", world["package"])
    with pytest.raises(release.ReleaseError, match="независимо от флагов"):
        release.promote_release(world["package"], target, "hmao")


def test_hidden_installed_stamp_edit_and_hidden_mode_change_rejected(world):
    install_prepared(world)
    target = world["target"]
    git(target, "update-index", "--assume-unchanged", release.LOCK)
    original = (target / release.LOCK).read_bytes()
    write(target, release.LOCK, original + b" ")
    with pytest.raises(release.ReleaseError, match="независимо от флагов"):
        release.verify_release(target, "hmao")
    write(target, release.LOCK, original)
    git(target, "config", "core.filemode", "false")
    (target / "app.js").chmod(0o755)
    assert git(target, "status", "--porcelain") == ""
    with pytest.raises(release.ReleaseError, match="независимо от флагов"):
        release.verify_release(target, "hmao")


@pytest.mark.parametrize("installed", [False, True])
def test_newly_protected_former_managed_file_is_never_deleted(world, installed):
    if installed:
        install_prepared(world)
    source, target = world["source"], world["target"]
    manifest_path = source / "deployment/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["common_files"].remove("app.js")
    manifest["protected_prefixes"] = ["app.js"]
    manifest_path.write_bytes(release.canonical(manifest))
    sha = commit(source, "protect formerly managed code")
    out = world["tmp"] / "protected-next"
    release.build_release(source, sha, out, ["hmao"])
    initial_head = git(target, "rev-parse", "HEAD")
    initial_contents = (target / "app.js").read_bytes()
    with pytest.raises(release.ReleaseError, match="защищён manifest"):
        release.plan_release(out / "hmao", target, "hmao")
    with pytest.raises(release.ReleaseError, match="защищён manifest"):
        release.promote_release(out / "hmao", target, "hmao")
    assert git(target, "rev-parse", "HEAD") == initial_head
    assert (target / "app.js").read_bytes() == initial_contents


def test_repeated_publish_after_new_data_retries_original_code_commit(world, monkeypatch):
    bare, remote, pushes = local_remote(world, monkeypatch)
    installs = []
    def install(result, package, host, identity):
        installs.append(result['commit'])
        result.update(status='published_not_installed', install_pending=True)
    monkeypatch.setattr(release, 'install_vps', install)
    monkeypatch.setattr(release, 'preflight_vps', lambda *args: {'preflight_passed': True})
    first = release.promote_release(world['package'], world['target'], 'hmao', push=True,
                                    remote=remote, vps_host='host', ssh_key='identity')
    writer = world['tmp'] / 'normal-data-writer'
    git(world['tmp'], 'clone', str(bare), str(writer))
    write(writer, 'data/cases.json', '{"case":"after published release"}\n')
    data_commit = commit(writer, 'normal data after code publish')
    git(writer, 'push', 'origin', 'main')
    second = release.promote_release(world['package'], world['target'], 'hmao', push=True,
                                     remote=remote, vps_host='host', ssh_key='identity')
    assert second['unchanged'] is True and len(pushes) == 1
    assert second['commit'] == first['commit'] != data_commit
    assert second['remote_commit'] == data_commit
    assert installs == [first['commit'], first['commit']]
    assert git(bare, 'show', 'main:data/cases.json') == '{"case":"after published release"}'
    release_diff = git(bare, 'diff-tree', '--no-commit-id', '--name-only', '-r', second['commit']).splitlines()
    assert release.LOCK in release_diff
    assert not any(name.startswith('data/') for name in release_diff)


def mock_site_reader(world, expected, *, stale_stamp=False, wrong_asset=False):
    def read(url):
        path = release.urlparse(url).path
        if path.endswith('/' + release.LOCK):
            stamp = dict(expected)
            if stale_stamp:
                stamp['source_commit'] = 'f' * 40
                stamp['release_id'] = release.release_id(stamp)
            return release.canonical(stamp)
        name = path.rsplit('/', 1)[-1]
        if wrong_asset:
            return b'old cached asset'
        return (world['package'] / 'files' / name).read_bytes()
    return read


def vps_report(expected, **changes):
    return {'verified': True, 'head': 'a' * 40, 'branch': 'main', 'region': expected['region'],
            'effective_region': expected['region'], 'source_commit': expected['source_commit'],
            'release_id': expected['release_id'], 'files_verified': len(expected['files']), **changes}


def test_online_github_accepts_later_data_without_touching_local_refs(world, monkeypatch):
    install_prepared(world)
    bare, remote, pushes = local_remote(world, monkeypatch)
    writer = world['tmp'] / 'online-data-writer'
    git(world['tmp'], 'clone', str(bare), str(writer))
    write(writer, 'data/cases.json', '{"case":"online newer"}\n')
    data_head = commit(writer, 'normal data')
    git(writer, 'push', 'origin', 'main')
    refs_before = git(world['target'], 'show-ref')
    head_before = git(world['target'], 'rev-parse', 'HEAD')
    result = release.verify_release(world['target'], 'hmao', remote=remote)
    assert result['requested_checks_passed'] is True
    assert result['online_verified'] is False  # Pages/VPS were not requested.
    assert result['boundaries']['github']['commit'] == data_head
    assert result['boundaries']['github']['source_commit'] == world['sha']
    assert git(world['target'], 'show-ref') == refs_before
    assert git(world['target'], 'rev-parse', 'HEAD') == head_before
    assert pushes == []
    assert git(bare, 'show', 'main:data/cases.json') == '{"case":"online newer"}'


def test_pages_checks_stamp_and_actual_browser_assets(world, monkeypatch):
    expected = release.lock_from_package(release.load_package(world['package']))
    reader = mock_site_reader(world, expected)
    checked = []
    def read(url):
        checked.append(url)
        return reader(url)
    monkeypatch.setattr(release, 'read_public_bytes', read)
    result = release.verify_site(expected, expected['site_url'])
    assert result['verified'] is True and result['files_verified'] == ['app.js']
    assert len(checked) == 2
    assert any(release.LOCK in url for url in checked)
    assert all('data/' not in url and 'cloudflare-worker' not in url for url in checked)


@pytest.mark.parametrize('stale_stamp,wrong_asset', [(True, False), (False, True)])
def test_pages_stale_stamp_or_asset_fails(world, monkeypatch, stale_stamp, wrong_asset):
    expected = release.lock_from_package(release.load_package(world['package']))
    monkeypatch.setattr(release, 'read_public_bytes', mock_site_reader(world, expected,
                       stale_stamp=stale_stamp, wrong_asset=wrong_asset))
    with pytest.raises(release.ReleaseError):
        release.verify_site(expected, expected['site_url'])


def test_site_url_normalization_and_territory_mismatch(world, monkeypatch):
    assert release.site_base('https://site.invalid/region/sberbank_dashboard.html') == 'https://site.invalid/region/'
    assert release.site_base('https://site.invalid/region') == 'https://site.invalid/region/'
    expected = release.lock_from_package(release.load_package(world['package']))
    monkeypatch.setattr(release, 'read_public_bytes', lambda _: pytest.fail('Must reject before HTTP'))
    with pytest.raises(release.ReleaseError, match='территории'):
        release.verify_site(expected, 'https://other.invalid/')
    with pytest.raises(release.ReleaseError, match='HTTPS'):
        release.site_base('http://site.invalid/')


def test_online_reports_all_requested_boundaries_even_when_one_fails(world, monkeypatch):
    install_prepared(world)
    expected = release.lock_from_package(release.load_package(world['package']))
    calls = []
    def github(*args):
        calls.append('github')
        raise release.ReleaseError('GitHub version differs')
    def site(*args):
        calls.append('pages')
        return {'verified': True}
    def vps(*args):
        calls.append('vps')
        return vps_report(expected)
    monkeypatch.setattr(release, 'verify_github', github)
    monkeypatch.setattr(release, 'verify_site', site)
    monkeypatch.setattr(release, 'verify_vps', vps)
    result = release.verify_release(world['target'], remote='git@github.com:SelivanovAS/dashboard.git',
                                   site_url=expected['site_url'], vps_host='host', ssh_key='identity')
    assert calls == ['github', 'pages', 'vps']
    assert result['status'] == 'verification_failed'
    assert result['online_verified'] is False
    assert result['boundaries']['local']['verified'] is True
    assert result['boundaries']['github']['verified'] is False
    assert result['boundaries']['pages']['verified'] is True
    assert result['boundaries']['vps']['verified'] is True


def test_online_flag_true_only_after_all_three_checks_and_local_pass(world, monkeypatch):
    install_prepared(world)
    expected = release.lock_from_package(release.load_package(world['package']))
    monkeypatch.setattr(release, 'verify_github', lambda *a: {'verified': True})
    monkeypatch.setattr(release, 'verify_site', lambda *a: {'verified': True})
    monkeypatch.setattr(release, 'verify_vps', lambda *a: vps_report(expected))
    kwargs = dict(remote='git@github.com:SelivanovAS/dashboard.git', site_url=expected['site_url'],
                  vps_host='host', ssh_key='identity')
    result = release.verify_release(world['target'], **kwargs)
    assert result['status'] == 'verified_requested_boundaries' and result['online_verified'] is True
    git(world['target'], 'update-index', '--assume-unchanged', 'app.js')
    write(world['target'], 'app.js', 'hidden local defect\n')
    bad = release.verify_release(world['target'], **kwargs)
    assert bad['online_verified'] is False
    assert bad['boundaries']['local']['verified'] is False
    assert bad['boundaries']['vps']['verified'] is True


def test_vps_uses_read_only_ssh_script_and_validates_reply(world, monkeypatch):
    expected = release.lock_from_package(release.load_package(world['package']))
    commands = []
    def run(args, **kwargs):
        commands.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, json.dumps(vps_report(expected)), '')
    monkeypatch.setattr(release.subprocess, 'run', run)
    result = release.verify_vps(expected, 'operator@server.invalid', '/private/key')
    assert result['verified'] is True
    args, kwargs = commands[0]
    assert args[0] == 'ssh' and args[-1] == 'python3 -B -'
    assert 'IdentitiesOnly=yes' in args and 'BatchMode=yes' in args
    script = kwargs['input']
    assert 'systemctl' not in script and "'fetch'" not in script and "'merge'" not in script
    assert '.run.lock' not in script and 'data/cases.json' not in script
    assert 'hashlib.sha256(path.read_bytes())' in script
    monkeypatch.setattr(release.subprocess, 'run', lambda args, **kw: subprocess.CompletedProcess(
        args, 0, json.dumps(vps_report(expected, source_commit='f' * 40)), ''))
    with pytest.raises(release.ReleaseError, match='другую версию'):
        release.verify_vps(expected, 'operator@server.invalid', '/private/key')


def test_vps_read_only_script_checks_actual_bytes_with_hidden_index_changes(world):
    install_prepared(world)
    target = world['target']
    config = 'scripts/court_monitor/config.py'
    write(target, config, "from pathlib import Path\nREGION=Path('REGION').read_text().strip()\n")
    expected = release.lock_from_package(release.load_package(world['package']))
    expected['files'][config] = record((target / config).read_text())
    expected['release_id'] = release.release_id(expected)
    write(target, release.LOCK, release.canonical(expected))
    commit(target, 'VPS fixture config')
    root = world['tmp'] / 'vps-root'
    root.mkdir()
    vps = root / 'dashboard'
    git(root, 'clone', str(target), str(vps))
    code = release.VPS_VERIFY_CODE.replace("Path('/opt/court-monitor')", f'Path({str(root)!r})')
    script = 'import json\nexpected=json.loads(' + repr(release.canonical(expected).decode()) + ')\n' + code
    def check():
        return subprocess.run([os.sys.executable, '-B', '-'], input=script, capture_output=True, text=True)
    before = git(vps, 'rev-parse', 'HEAD')
    result = check()
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)['verified'] is True
    git(vps, 'update-index', '--assume-unchanged', 'app.js')
    write(vps, 'app.js', 'hidden VPS modification')
    assert git(vps, 'status', '--porcelain') == ''
    failed = check()
    assert failed.returncode == 1
    assert json.loads(failed.stdout)['verified'] is False
    assert git(vps, 'rev-parse', 'HEAD') == before
    assert (vps / 'data/cases.json').read_text() == '{"case":"live"}\n'


def test_verify_cli_returns_failure_for_requested_online_mismatch(world, monkeypatch, capsys):
    install_prepared(world)
    def fail(*args):
        raise release.ReleaseError('Old Pages asset')
    monkeypatch.setattr(release, 'verify_site', fail)
    code = release.main(['verify', '--repo', str(world['target']), '--site-url', 'https://example.invalid'])
    assert code == 1
    output = json.loads(capsys.readouterr().out)
    assert output['status'] == 'verification_failed'
    assert output['boundaries']['local']['verified'] is True
    assert output['boundaries']['pages']['verified'] is False


def test_pages_verifies_html_js_css_region_manifest_and_service_worker(world, monkeypatch):
    expected = release.lock_from_package(release.load_package(world['package']))
    assets = {name: (name + '\n').encode() for name in (
        'sberbank_dashboard.html', 'app.js', 'styles.css', 'region_front.js', 'manifest.json', 'service-worker.js')}
    for name, content in assets.items():
        expected['files'][name] = {'sha256': release.digest(content), 'mode': '100644'}
    expected['release_id'] = release.release_id(expected)
    def read(url):
        name = release.urlparse(url).path.rsplit('/', 1)[-1]
        return release.canonical(expected) if name == release.LOCK else assets[name]
    monkeypatch.setattr(release, 'read_public_bytes', read)
    result = release.verify_site(expected, expected['site_url'])
    assert set(result['files_verified']) == set(assets)


def test_failed_vps_preflight_never_publishes_or_installs(world, monkeypatch):
    bare, remote, pushes = local_remote(world, monkeypatch)
    before = git(bare, 'rev-parse', 'main')
    installs = []
    def reject(package, host, identity):
        assert host == 'server' and identity == 'identity'
        assert package['release_id'] == release.load_package(world['package'])['release_id']
        raise release.ReleaseError('Небезопасное восстановление таймера')
    monkeypatch.setattr(release, 'preflight_vps', reject)
    monkeypatch.setattr(release, 'install_vps', lambda *a: installs.append(a))
    with pytest.raises(release.ReleaseError, match='Небезопасное восстановление'):
        release.promote_release(world['package'], world['target'], 'hmao', push=True,
                                remote=remote, vps_host='server', ssh_key='identity')
    assert pushes == [] and installs == []
    assert git(bare, 'rev-parse', 'main') == before
    assert git(bare, 'show', 'main:app.js') == 'old'


def test_preflight_repeated_before_each_racing_push(world, monkeypatch):
    def race(bare):
        writer = world['tmp'] / 'preflight-racer'
        git(world['tmp'], 'clone', str(bare), str(writer))
        write(writer, 'data/cases.json', '{"case":"racing-data"}\n')
        commit(writer, 'new data')
        git(writer, 'push', 'origin', 'main')
    bare, remote, pushes = local_remote(world, monkeypatch, before_push=race)
    checks = []
    def preflight(package, host, identity):
        checks.append(len(pushes))
        return {'preflight_passed': True, 'check_number': len(checks)}
    monkeypatch.setattr(release, 'preflight_vps', preflight)
    monkeypatch.setattr(release, 'install_vps', lambda result, *a: result.update(status='installed'))
    result = release.promote_release(world['package'], world['target'], 'hmao', push=True,
                                     remote=remote, vps_host='server', ssh_key='identity')
    assert checks == [0, 1]
    assert result['vps_preflight']['check_number'] == 2
    assert git(bare, 'show', 'main:data/cases.json') == '{"case":"racing-data"}'


def test_vps_preflight_sends_exact_manifest_and_validates_reply(world, monkeypatch):
    package = release.load_package(world['package'])
    expected = release.lock_from_package(package)
    seen = []
    def runner(args, **kwargs):
        seen.append(args)
        path = Path(args[args.index('--preflight-manifest') + 1])
        assert json.loads(path.read_text()) == expected
        assert 'baseline' not in json.loads(path.read_text())
        assert kwargs['timeout'] == 120
        assert args[-4:] == ['--host', 'server', '--identity', 'identity']
        return subprocess.CompletedProcess(args, 0, json.dumps({
            'preflight_passed': True, 'head': 'a' * 40,
            **{k: expected[k] for k in ('region', 'source_commit', 'release_id')}
        }), '')
    monkeypatch.setattr(release.subprocess, 'run', runner)
    assert release.preflight_vps(package, 'server', 'identity')['preflight_passed']
    assert len(seen) == 1


@pytest.mark.parametrize('failure', ['denied', 'bad-json', 'wrong-release', 'missing-head', 'timeout'])
def test_vps_preflight_fails_closed(world, monkeypatch, failure):
    package = release.load_package(world['package'])
    report = {'preflight_passed': True, 'head': 'a' * 40,
              **{k: package[k] for k in ('region', 'source_commit', 'release_id')}}
    if failure == 'wrong-release': report['release_id'] = 'f' * 64
    if failure == 'missing-head': report.pop('head')
    def runner(args, **kwargs):
        if failure == 'timeout': raise subprocess.TimeoutExpired(args, 120)
        return subprocess.CompletedProcess(args, 1 if failure == 'denied' else 0,
            'not-json' if failure == 'bad-json' else json.dumps(report),
            json.dumps({'error': 'systemd needs safe installation'}))
    monkeypatch.setattr(release.subprocess, 'run', runner)
    with pytest.raises(release.ReleaseError):
        release.preflight_vps(package, 'server', 'identity')


def test_no_vps_preflight_for_local_package_preparation(monkeypatch):
    monkeypatch.setattr(release.subprocess, 'run', lambda *a, **kw: pytest.fail('unexpected remote check'))
    assert release.preflight_vps({}, None, None) is None


@pytest.mark.parametrize('host,identity', [(None, None), ('server', None), (None, 'identity')])
def test_prod_publish_cannot_skip_vps_preflight(world, monkeypatch, host, identity):
    bare, remote, pushes = local_remote(world, monkeypatch)
    before = git(bare, 'rev-parse', 'main')
    monkeypatch.setattr(release, 'preflight_vps', lambda *a: pytest.fail('must reject missing connection first'))
    with pytest.raises(release.ReleaseError, match='требует --vps-host и --ssh-key'):
        release.promote_release(world['package'], world['target'], 'hmao', push=True,
                                remote=remote, vps_host=host, ssh_key=identity)
    assert pushes == [] and git(bare, 'rev-parse', 'main') == before
