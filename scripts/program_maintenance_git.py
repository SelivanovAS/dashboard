#!/usr/bin/env python3
"""Local Git proofs and resumable checkout; NEVER opens admission or publishes.

The caller must hold the durable maintenance guard and all run locks throughout.
A transaction requires an already managed, ordinary Git clone. Its private journal
and blobs live outside that clone on the same filesystem. No reset/clean/checkout
is used: every changed file must still equal one of its recorded old/new states.
Remote ref retrieval and independent approval of its program belong to the caller.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import subprocess

import program_release as release

SCHEMA = 1
EXCLUDED = {".git", "ops/mac-local-run/.run.lock", "ops/mac-local-run/.run.lock.guard"}


class MaintenanceGitError(RuntimeError):
    pass


def _git(repo, *args, env=None, input_bytes=None, check=True):
    ambient = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    cp = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
                         "-C", str(repo), *args], input=input_bytes,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        env={**ambient, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0",
                             "GIT_NO_REPLACE_OBJECTS": "1", **(env or {})})
    if check and cp.returncode:
        raise MaintenanceGitError(f"Git {args[0]} failed ({cp.returncode}): " + cp.stderr.decode(errors="replace")[-1000:])
    return cp


def _text(repo, *args):
    return _git(repo, *args).stdout.decode().strip()


def _assert_complete_history(repo):
    # A shallow/grafted graph can falsely report "diverged" while the actual
    # remote still accepts a late fast-forward. Never fetch missing history as
    # an implicit side effect of a safety proof.
    if _text(repo, "rev-parse", "--is-shallow-repository") != "false":
        raise MaintenanceGitError("Incomplete/shallow Git history cannot prove safety")
    for item in ("shallow", "info/grafts"):
        path = Path(_text(repo, "rev-parse", "--git-path", item))
        if not path.is_absolute():
            path = Path(repo) / path
        if path.exists() or path.is_symlink():
            raise MaintenanceGitError("Shallow/grafted Git history cannot prove safety")
    config = _git(repo, "config", "--get-regexp",
                  r"^(extensions\.partialclone|remote\..*\.promisor|remote\..*\.partialclonefilter)$", check=False)
    if config.returncode not in (0, 1):
        raise MaintenanceGitError("Cannot inspect partial-clone configuration")
    if config.returncode == 0:
        raise MaintenanceGitError("Partial/promisor clone cannot prove safety")
    objects = Path(_text(repo, "rev-parse", "--git-path", "objects"))
    if not objects.is_absolute():
        objects = Path(repo) / objects
    if any((objects / "pack").glob("*.promisor")):
        raise MaintenanceGitError("Promisor objects cannot prove safety")
    # External object stores could themselves hide shallow/promisor metadata.
    # Generic release checkouts must be self-contained ordinary full clones.
    for item in (objects / "info/alternates", objects / "info/http-alternates"):
        if item.exists() or item.is_symlink():
            raise MaintenanceGitError("Alternate object stores cannot prove safety")


def _commit(repo, sha):
    if not isinstance(sha, str) or not release.COMMIT.fullmatch(sha):
        raise MaintenanceGitError("A full commit SHA is required")
    if _text(repo, "rev-parse", "--verify", sha + "^{commit}") != sha:
        raise MaintenanceGitError("Object is not the exact commit")
    return sha


def _ancestor(repo, before, after):
    cp = _git(repo, "merge-base", "--is-ancestor", before, after, check=False)
    if cp.returncode not in (0, 1):
        raise MaintenanceGitError("Cannot prove commit ancestry")
    return cp.returncode == 0


def publication_fences(repo, remote_sha, attempts):
    """Prove ALL previously authorized non-force pushes cannot advance remote.

    This is only a topology proof. Caller independently verifies the remote and
    installed program, obtains remote_sha by its own fetch, and forbids force push.
    A diverged remote is fenced, but is not thereby a safe program.
    """
    _assert_complete_history(repo)
    _commit(repo, remote_sha)
    if not isinstance(attempts, list):
        raise MaintenanceGitError("Invalid publication intents")
    evidence = []
    v2 = bool(attempts and isinstance(attempts[0], dict) and "target_id" in attempts[0])
    keys = {"attempt_id", "base_sha", "target_sha"} | ({"target_id"} if v2 else set())
    previous_target = 1
    for number, attempt in enumerate(attempts, 1):
        if (not isinstance(attempt, dict) or set(attempt) != keys
                or type(attempt["attempt_id"]) is not int or attempt["attempt_id"] != number):
            raise MaintenanceGitError("Publication intent sequence is invalid")
        if v2:
            target_id = attempt["target_id"]
            if (type(target_id) is not int or target_id < previous_target
                    or (number == 1 and target_id != 1)):
                raise MaintenanceGitError("Invalid v2 target sequence")
            previous_target = target_id
        base, target = (_commit(repo, attempt[key]) for key in ("base_sha", "target_sha"))
        if base == target or not _ancestor(repo, base, target):
            raise MaintenanceGitError("Intent was not a strict fast-forward")
        if remote_sha != target and _ancestor(repo, remote_sha, target):
            raise MaintenanceGitError(f"Late push {number} can still advance remote")
        relationship = "reached" if _ancestor(repo, target, remote_sha) else "diverged"
        evidence.append({**attempt, "relationship": relationship})
    return {"remote_sha": remote_sha, "covered_attempt_ids": list(range(1, len(attempts) + 1)),
            "evidence_sha256": _digest(release.canonical({"remote_sha": remote_sha, "attempts": evidence})),
            "attempts": evidence}


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_exclusive(path, raw, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(raw)
            stream.flush()
        os.fchmod(fd, mode)
        os.fsync(fd)
    finally:
        os.close(fd)
    _sync_dir(path.parent)


def _no_symlinks(path):
    if not path.is_absolute() or ".." in path.parts:
        raise MaintenanceGitError("Absolute normalized path required")
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise MaintenanceGitError("Symlink in filesystem path")


def _repo(repo):
    repo = Path(repo)
    _no_symlinks(repo)
    if not repo.is_dir() or not (repo / ".git").is_dir() or (repo / ".git").is_symlink():
        raise MaintenanceGitError("An ordinary clone with a real .git directory is required")
    if Path(_text(repo, "rev-parse", "--show-toplevel")) != repo:
        raise MaintenanceGitError("Path is not the clone root")
    return repo


def _record(path):
    st = path.lstat()
    mode = stat.S_IMODE(st.st_mode)
    if stat.S_ISREG(st.st_mode):
        if st.st_nlink != 1:
            raise MaintenanceGitError(f"Hardlinked file is not supported: {path.name}")
        return {"type": "file", "mode": mode, "sha256": _digest(path.read_bytes())}
    if stat.S_ISDIR(st.st_mode):
        return {"type": "directory", "mode": mode}
    if stat.S_ISLNK(st.st_mode):
        # Hash the link itself; do not read a possibly secret external target.
        return {"type": "symlink", "mode": mode, "sha256": _digest(os.fsencode(os.readlink(path)))}
    raise MaintenanceGitError(f"Unsupported filesystem object: {path.name}")


def inventory(repo):
    """Hash every worktree object, including ignored queues and empty directories."""
    repo = _repo(repo)
    result = {}
    def visit(directory):
        for path in sorted(directory.iterdir()):
            name = path.relative_to(repo).as_posix()
            if name in EXCLUDED:
                continue
            release.safe_path(name)
            result[name] = _record(path)
            if result[name]["type"] == "directory":
                visit(path)
    visit(repo)
    return result


def inventory_digest(repo):
    return _digest(release.canonical(inventory(repo)))


def _tree_entries(repo, sha):
    entries = {}
    for raw in _git(repo, "ls-tree", "-r", "-z", "--full-tree", sha).stdout.split(b"\0"):
        if raw:
            metadata, name = raw.split(b"\t", 1)
            entries[name.decode()] = tuple(metadata.decode("ascii").split())
    return entries


def _tree(repo, sha):
    entries = _tree_entries(repo, sha)
    if any(e[0] not in release.MODES for e in entries.values()):
        raise MaintenanceGitError("Tracked symlinks/submodules are not supported")
    contents = {}
    raw = _git(repo, "cat-file", "--batch", input_bytes="".join(e[2] + "\n" for e in entries.values()).encode()).stdout
    offset = 0
    for name, (mode, kind, expected) in entries.items():
        release.safe_path(name)
        end = raw.index(b"\n", offset)
        oid, actual_kind, size = raw[offset:end].decode().split()
        size = int(size)
        data = raw[end + 1:end + 1 + size]
        if oid != expected or kind != "blob" or actual_kind != "blob" or len(data) != size:
            raise MaintenanceGitError("Invalid Git blob")
        contents[name] = {"mode": mode, "content": data}
        offset = end + size + 2
    return {name: {"type": "file", "mode": 0o755 if item["mode"] == "100755" else 0o644,
                   "sha256": _digest(item["content"])} for name, item in contents.items()}, contents


def _installed(repo, sha, expected=None):
    try:
        raw = _git(repo, "show", sha + ":" + release.LOCK).stdout
        lock = release.read_json(raw, release.LOCK)
        release.validate_lock(lock)
        records, _ = _tree(repo, sha)
        for name, record in lock["files"].items():
            wanted = {"type": "file", "sha256": record["sha256"],
                      "mode": 0o755 if record["mode"] == "100755" else 0o644}
            if records.get(name) != wanted:
                raise MaintenanceGitError("Git program does not match its manifest")
        region_raw = _git(repo, "show", sha + ":REGION").stdout
        if region_raw.strip().decode() != lock["region"]:
            raise MaintenanceGitError("Effective region differs from manifest")
        if expected is not None and lock != expected:
            raise MaintenanceGitError("Target contains a different package")
        return lock
    except release.ReleaseError as exc:
        raise MaintenanceGitError(str(exc)) from exc


def _index_entries(repo):
    flags = _git(repo, "ls-files", "-v", "-z").stdout.split(b"\0")
    if any(item and not item.startswith(b"H ") for item in flags):
        raise MaintenanceGitError("Hidden index flags are not supported")
    entries = {}
    for raw in _git(repo, "ls-files", "--stage", "-z").stdout.split(b"\0"):
        if not raw:
            continue
        metadata, name = raw.split(b"\t", 1)
        mode, oid, stage = metadata.decode().split()
        if stage != "0":
            raise MaintenanceGitError("Unmerged index entries")
        entries[name.decode()] = (mode, "blob", oid)
    return entries


def _index_matches(repo, sha):
    # git status/diff may refresh stat-cache bytes without changing indexed files.
    return _index_entries(repo) == _tree_entries(repo, sha)


def _index_tree(repo, path=None):
    env = {"GIT_INDEX_FILE": str(path)} if path else None
    return _git(repo, "write-tree", env=env).stdout.decode().strip()


def begin_install(repo, target_sha, journal_path, expected_lock):
    """Write immutable inventory/blob journal BEFORE mutating any clone files.

    Target must be an ff descendant, with only managed paths and committed data
    advances differing from HEAD. Unknown tracked or untracked content is refused.
    A crash while preparing a journal changes no checkout file; incomplete private
    staging can be retained for inspection and a new journal path can be used.
    """
    repo = _repo(repo)
    _assert_complete_history(repo)
    _commit(repo, target_sha)
    old = _text(repo, "rev-parse", "HEAD")
    branch = _text(repo, "symbolic-ref", "HEAD")
    if not branch.startswith("refs/heads/"):
        raise MaintenanceGitError("A local branch is required")
    if not _ancestor(repo, old, target_sha):
        raise MaintenanceGitError("Installation may not rewind or diverge history")
    before_lock = _installed(repo, old)
    target_lock = _installed(repo, target_sha, expected_lock)
    if target_lock["region"] != before_lock["region"] or target_lock["repository"] != before_lock["repository"]:
        raise MaintenanceGitError("Region/repository changed")
    old_tree, _ = _tree(repo, old)
    new_tree, new_contents = _tree(repo, target_sha)
    if _index_tree(repo) != _text(repo, "rev-parse", old + "^{tree}"):
        raise MaintenanceGitError("Index has staged changes")
    flags = _git(repo, "ls-files", "-v", "-z").stdout.split(b"\0")
    if any(item and not item.startswith(b"H ") for item in flags):
        raise MaintenanceGitError("Hidden index flags are not supported")
    before = inventory(repo)
    if any(before.get(name) != record for name, record in old_tree.items()):
        raise MaintenanceGitError("Tracked filesystem bytes/modes differ from HEAD")
    names = set(old_tree) | set(new_tree)
    managed = set(before_lock["files"]) | set(target_lock["files"]) | {release.LOCK}
    writes = {}
    directories = {}
    for name in sorted(names):
        if old_tree.get(name) == new_tree.get(name):
            continue
        if name not in managed and not release.data_only_path(name):
            raise MaintenanceGitError(f"Unknown program change: {name}")
        if name not in old_tree and name in before:
            raise MaintenanceGitError(f"New tracked path collides with unmanaged object: {name}")
        for parent in Path(name).parents:
            if str(parent) == ".":
                continue
            key = parent.as_posix()
            if key in before and before[key]["type"] != "directory":
                raise MaintenanceGitError(f"Parent is not a directory: {key}")
            if key not in before:
                directories[key] = {"type": "directory", "mode": 0o755}
        writes[name] = {"old": old_tree.get(name), "new": new_tree.get(name),
                        "kind": "managed" if name in managed else "data"}
    journal_path = Path(journal_path)
    _no_symlinks(journal_path)
    if journal_path == repo or repo in journal_path.parents:
        raise MaintenanceGitError("Journal must be outside the checkout")
    if journal_path.parent.exists():
        if not journal_path.parent.is_dir() or stat.S_IMODE(journal_path.parent.stat().st_mode) != 0o700:
            raise MaintenanceGitError("Journal directory must be private (0700)")
        if any(journal_path.parent.iterdir()):
            raise MaintenanceGitError("Use a fresh journal directory")
    else:
        journal_path.parent.mkdir(mode=0o700)
        _sync_dir(journal_path.parent.parent)
    if journal_path.parent.stat().st_dev != repo.stat().st_dev or (repo / ".git").stat().st_dev != repo.stat().st_dev:
        raise MaintenanceGitError("Journal/clone/index must share a filesystem")
    txid = secrets.token_hex(16)
    for number, (name, change) in enumerate(writes.items()):
        change["blob"] = f"blob-{number}"
        if change["new"] is not None:
            _write_exclusive(journal_path.parent / change["blob"], new_contents[name]["content"], change["new"]["mode"])
    next_index = journal_path.parent / "index.next"
    _git(repo, "read-tree", target_sha, env={"GIT_INDEX_FILE": str(next_index)})
    next_index.chmod(0o600)
    with next_index.open("rb") as stream:
        os.fsync(stream.fileno())
    _sync_dir(next_index.parent)
    owner = ("program-maintenance-index/1 " + txid + "\n").encode()
    _write_exclusive(journal_path.parent / "index.owner", owner)
    doc = {"schema_version": SCHEMA, "transaction_id": txid, "repo": str(repo), "branch": branch,
           "old_sha": old, "target_sha": target_sha, "target_lock": target_lock, "inventory": before,
           "new_directories": directories, "writes": writes,
           "index_old_sha256": _digest((repo / ".git/index").read_bytes()),
           "index_new_sha256": _digest(next_index.read_bytes())}
    _write_exclusive(journal_path, release.canonical({"document": doc, "sha256": _digest(release.canonical(doc))}))
    return doc


def _load(repo, journal_path):
    repo = _repo(repo)
    _assert_complete_history(repo)
    journal_path = Path(journal_path)
    _no_symlinks(journal_path)
    if (stat.S_IMODE(journal_path.parent.stat().st_mode) != 0o700
            or journal_path.parent.stat().st_uid != os.geteuid()
            or not journal_path.is_file() or journal_path.stat().st_nlink != 1
            or journal_path.stat().st_uid != os.geteuid()
            or stat.S_IMODE(journal_path.stat().st_mode) != 0o600):
        raise MaintenanceGitError("Journal permissions changed")
    if journal_path.stat().st_size > 64 * 1024 * 1024:
        raise MaintenanceGitError("Journal too large")
    wrapped = release.read_json(journal_path.read_bytes(), "installation journal")
    doc = wrapped.get("document")
    if (set(wrapped) != {"document", "sha256"} or not isinstance(doc, dict)
            or wrapped["sha256"] != _digest(release.canonical(doc)) or doc.get("schema_version") != SCHEMA
            or doc.get("repo") != str(repo)):
        raise MaintenanceGitError("Corrupt or foreign installation journal")
    for name in set(doc["inventory"]) | set(doc["writes"]) | set(doc["new_directories"]):
        release.safe_path(name)
        if any(name == prefix or name.startswith(prefix + "/") for prefix in EXCLUDED):
            raise MaintenanceGitError("Journal targets excluded paths")
    _installed(repo, doc["target_sha"], doc["target_lock"])
    return repo, journal_path, doc


def _validate_partial(repo, doc):
    actual = inventory(repo)
    allowed = set(doc["inventory"]) | set(doc["writes"]) | set(doc["new_directories"])
    if set(actual) - allowed:
        raise MaintenanceGitError("Unknown filesystem object appeared during installation")
    for name in allowed:
        current = actual.get(name)
        if name in doc["writes"]:
            change = doc["writes"][name]
            if current != change["old"] and current != change["new"]:
                raise MaintenanceGitError(f"Changed file is neither recorded old nor new: {name}")
        elif name in doc["new_directories"]:
            if current is not None and current != doc["new_directories"][name]:
                raise MaintenanceGitError(f"New directory changed: {name}")
        elif current != doc["inventory"][name]:
            raise MaintenanceGitError(f"Protected/unmanaged object changed: {name}")
    if _text(repo, "symbolic-ref", "HEAD") != doc["branch"]:
        raise MaintenanceGitError("Checkout branch changed")
    if _text(repo, "rev-parse", "HEAD") not in (doc["old_sha"], doc["target_sha"]):
        raise MaintenanceGitError("Checkout HEAD changed")
    if not _index_matches(repo, doc["old_sha"]) and not _index_matches(repo, doc["target_sha"]):
        raise MaintenanceGitError("Index changed outside installation")


def _check_index_lock(repo, journal_path, doc, *, require_owned):
    lock = repo / ".git/index.lock"
    present = lock.exists() or lock.is_symlink()
    if not require_owned:
        if present:
            raise MaintenanceGitError("Git index lock must be absent for public verification")
        return
    if not present or lock.is_symlink() or not lock.is_file():
        raise MaintenanceGitError("Owned Git index lock is missing or invalid")
    owner_file = journal_path.parent / "index.owner"
    if owner_file.is_symlink() or not owner_file.is_file():
        raise MaintenanceGitError("Index lock owner is invalid")
    held, original = lock.stat(), owner_file.stat()
    owner = ("program-maintenance-index/1 " + doc["transaction_id"] + "\n").encode()
    if ((held.st_dev, held.st_ino) != (original.st_dev, original.st_ino)
            or stat.S_IMODE(held.st_mode) != 0o600 or held.st_uid != os.geteuid()
            or lock.read_bytes() != owner):
        raise MaintenanceGitError("Foreign Git index lock")


def resume_install(repo, journal_path, *, checkpoint=None):
    """Finish only the recorded transition. Fault callback is for isolated tests.

    Raises on unexplained files, modes, index/ref or protected-data changes. Caller
    must keep admission closed after ANY exception. Repeated success is idempotent.
    """
    repo, journal_path, doc = _load(repo, journal_path)
    _validate_partial(repo, doc)
    lock = repo / ".git/index.lock"
    if lock.exists() or lock.is_symlink():
        _check_index_lock(repo, journal_path, doc, require_owned=True)
    else:
        os.link(journal_path.parent / "index.owner", lock)
        _sync_dir(lock.parent)
    try:
        for name in sorted(doc["new_directories"], key=lambda n: (len(Path(n).parts), n)):
            path = repo / name
            if not path.exists():
                path.mkdir(mode=0o755)
                path.chmod(0o755)
                _sync_dir(path.parent)
        for name, change in doc["writes"].items():
            path = repo / name
            current = _record(path) if path.exists() or path.is_symlink() else None
            if current == change["new"]:
                continue
            if current != change["old"]:
                raise MaintenanceGitError(f"File changed during installation: {name}")
            if change["new"] is None:
                path.unlink()
            else:
                staged = journal_path.parent / change["blob"]
                if _record(staged) != change["new"]:
                    raise MaintenanceGitError(f"Staged blob changed: {name}")
                os.replace(staged, path)
                _sync_dir(journal_path.parent)
            _sync_dir(path.parent)
            if checkpoint:
                checkpoint("file", name)
        _validate_partial(repo, doc)
        index = repo / ".git/index"
        if not _index_matches(repo, doc["target_sha"]):
            prepared = journal_path.parent / "index.next"
            if _digest(prepared.read_bytes()) != doc["index_new_sha256"]:
                raise MaintenanceGitError("Prepared index changed")
            os.replace(prepared, index)
            _sync_dir(index.parent)
            _sync_dir(journal_path.parent)
        if checkpoint:
            checkpoint("index", None)
        if _text(repo, "rev-parse", "HEAD") != doc["target_sha"]:
            _git(repo, "update-ref", "-m", "program maintenance installation", doc["branch"], doc["target_sha"], doc["old_sha"])
            refpath = repo / ".git" / doc["branch"]
            # Git uses atomic ref rename; fsync the loose ref and its directory.
            with refpath.open("rb") as stream:
                os.fsync(stream.fileno())
            _sync_dir(refpath.parent)
        if checkpoint:
            checkpoint("head", None)
        _verify_install(repo, journal_path, allow_owned_index_lock=True)
        _check_index_lock(repo, journal_path, doc, require_owned=True)
        lock.unlink()
        _sync_dir(lock.parent)
        return verify_install(repo, journal_path)
    except BaseException:
        # The owned index lock deliberately survives ambiguity for explicit resume.
        raise


def verify_install(repo, journal_path):
    """Public completion proof: no remaining Git index lock is acceptable."""
    return _verify_install(repo, journal_path, allow_owned_index_lock=False)


def _verify_install(repo, journal_path, *, allow_owned_index_lock):
    repo, journal_path, doc = _load(repo, journal_path)
    _check_index_lock(repo, journal_path, doc, require_owned=allow_owned_index_lock)
    _validate_partial(repo, doc)
    actual = inventory(repo)
    for name, change in doc["writes"].items():
        if actual.get(name) != change["new"]:
            raise MaintenanceGitError(f"Installation incomplete: {name}")
    if _text(repo, "rev-parse", "HEAD") != doc["target_sha"]:
        raise MaintenanceGitError("HEAD installation incomplete")
    if not _index_matches(repo, doc["target_sha"]):
        raise MaintenanceGitError("Index installation incomplete")
    proof = {"installed_sha": doc["target_sha"], "release_id": doc["target_lock"]["release_id"],
             "source_commit": doc["target_lock"]["source_commit"], "region": doc["target_lock"]["region"],
             "inventory_sha256": _digest(release.canonical(actual)), "journal_sha256": _digest(journal_path.read_bytes())}
    proof["evidence_sha256"] = _digest(release.canonical(proof))
    return proof
