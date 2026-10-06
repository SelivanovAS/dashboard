#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Failure-injection для локальной транзакции доставочного marker-коммита."""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ops/mac-local-run/delivery_txn.py"
SPEC = importlib.util.spec_from_file_location("delivery_txn", MODULE)
delivery_txn = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(delivery_txn)


def _git(path: Path, *args: str) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "t@x",
           "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "t@x"}
    cp = subprocess.run(
        ["git", *args], cwd=path, env=env, check=True,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return cp.stdout.strip()


def test_journal_lifecycle_is_atomic_and_conditional(tmp_path):
    journal = tmp_path / "runtime/delivery.json"
    delivery_txn.prepare(str(journal), "hmao:issue", "abc")
    assert not list(journal.parent.glob("*.tmp.*"))
    assert delivery_txn.read_line(str(journal)) == (
        "prepared|hmao:issue||abc"
    )
    with pytest.raises(RuntimeError):
        delivery_txn.prepare(str(journal), "hmao:other", "def")
    delivery_txn.mark_committed(str(journal), "hmao:issue", "deadbeef")
    data = json.loads(journal.read_text(encoding="utf-8"))
    assert data["status"] == "committed"
    with pytest.raises(RuntimeError):
        delivery_txn.clear(str(journal), "hmao:other")
    delivery_txn.clear(str(journal), "hmao:issue")
    assert not journal.exists()


def test_prepared_line_is_parsed_by_the_same_bash_ifs(tmp_path):
    """Пустой marker_sha не должен сдвигать pre_sha в его поле.

    TSV для этого не годится: Bash 3.2 схлопывает соседние
    whitespace-разделители. Гоняем реальный `IFS='|' read`, как
    в parse_and_push.sh.
    """
    journal = tmp_path / "delivery.json"
    delivery_txn.prepare(str(journal), "hmao:issue", "abc123")
    line = delivery_txn.read_line(str(journal))
    cp = subprocess.run(
        [
            "bash", "-c",
            "IFS='|' read -r status delivery marker pre <<< \"$1\"; "
            "printf '%s\\n%s\\n%s\\n%s\\n' \"$status\" \"$delivery\" \"$marker\" \"$pre\"",
            "bash", line,
        ],
        check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert cp.stdout.splitlines() == ["prepared", "hmao:issue", "", "abc123"]


def test_corrupt_journal_fails_closed(tmp_path):
    journal = tmp_path / "delivery.json"
    journal.write_text("{", encoding="utf-8")
    with pytest.raises(RuntimeError):
        delivery_txn.read_line(str(journal))
    with pytest.raises(RuntimeError):
        delivery_txn.clear(str(journal), "hmao:x")


def test_remote_state_accepts_exact_sha_and_descendant(tmp_path):
    remote = tmp_path / "remote.git"
    local = tmp_path / "local"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run(["git", "init", "-b", "main", str(local)], check=True,
                   stdout=subprocess.DEVNULL)
    (local / "a").write_text("one", encoding="utf-8")
    _git(local, "add", "a")
    _git(local, "commit", "-m", "marker")
    marker = _git(local, "rev-parse", "HEAD")
    _git(local, "push", str(remote), "HEAD:main")
    assert delivery_txn.remote_contains(str(local), str(remote), marker) == 0

    (local / "a").write_text("two", encoding="utf-8")
    _git(local, "commit", "-am", "descendant")
    _git(local, "push", str(remote), "HEAD:main")
    assert delivery_txn.remote_contains(str(local), str(remote), marker) == 0


def test_remote_state_distinguishes_absent_and_unknown(tmp_path):
    remote = tmp_path / "remote.git"
    local = tmp_path / "local"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run(["git", "init", "-b", "main", str(local)], check=True,
                   stdout=subprocess.DEVNULL)
    (local / "a").write_text("one", encoding="utf-8")
    _git(local, "add", "a")
    _git(local, "commit", "-m", "remote base")
    _git(local, "push", str(remote), "HEAD:main")
    (local / "b").write_text("local only", encoding="utf-8")
    _git(local, "add", "b")
    _git(local, "commit", "-m", "marker not pushed")
    marker = _git(local, "rev-parse", "HEAD")
    assert delivery_txn.remote_contains(str(local), str(remote), marker) == 1
    assert delivery_txn.remote_contains(
        str(local), str(tmp_path / "missing.git"), marker
    ) == 2


def test_rollback_only_commit_does_not_consume_unrelated_staged_file(tmp_path):
    """`git commit --only` в shell-обёртке оставляет чужой index владельцу."""
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "main", str(repo)], check=True,
                   stdout=subprocess.DEVNULL)
    context = repo / "data/last_digest_context.json"
    context.parent.mkdir()
    context.write_text('{"delivered_at":"now"}', encoding="utf-8")
    unrelated = repo / "user-work.txt"
    unrelated.write_text("before", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "base")

    context.write_text("{}", encoding="utf-8")
    unrelated.write_text("user staged change", encoding="utf-8")
    _git(repo, "add", "data/last_digest_context.json", "user-work.txt")
    _git(
        repo, "commit", "--only", "-m", "rollback", "--",
        "data/last_digest_context.json",
    )

    committed = _git(repo, "show", "--pretty=format:", "--name-only", "HEAD")
    still_staged = _git(repo, "diff", "--cached", "--name-only")
    assert committed.splitlines() == ["data/last_digest_context.json"]
    assert still_staged.splitlines() == ["user-work.txt"]
