#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Crash recovery for parser data before digest-context WAL."""

from __future__ import annotations

import importlib.util
import json
import os


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOL = os.path.join(REPO, "ops", "mac-local-run", "parse_txn.py")
SPEC = importlib.util.spec_from_file_location("parse_txn", TOOL)
parse_txn = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(parse_txn)


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _setup(tmp_path):
    repo = tmp_path / "repo"
    runtime = repo / "ops" / "mac-local-run" / ".runtime"
    journal = runtime / "parse_txn.json"
    ack = runtime / "parse_txn.ack.json"
    context = repo / "data" / "last_digest_context.json"
    _write(repo / "data" / "cases.json", "cases-before")
    _write(repo / "data" / "cases_archive_2025.json", "cold-before")
    _write(context, "context-before")
    patterns = [
        "data/cases.json",
        "data/new.json",
        "data/cases_archive_*.json",
        "data/last_digest_context.json",
    ]
    txn_id = parse_txn.prepare(
        str(journal), str(ack), str(repo),
        "data/last_digest_context.json", patterns,
    )
    return repo, journal, ack, context, txn_id


def test_recovery_without_wal_restores_data_but_keeps_context(tmp_path):
    repo, journal, ack, context, _txn_id = _setup(tmp_path)
    _write(repo / "data" / "cases.json", "cases-after")
    _write(repo / "data" / "new.json", "created")
    _write(repo / "data" / "cases_archive_2025.json", "cold-after")
    _write(repo / "data" / "cases_archive_2026.json", "new-cold")
    # Context is the WAL and is deliberately outside the snapshot.
    _write(context, "context-wal")

    result, rc = parse_txn.recover(str(journal), str(ack))

    assert rc == 0 and result == "rolled_back:4"
    assert (repo / "data" / "cases.json").read_text() == "cases-before"
    assert not (repo / "data" / "new.json").exists()
    assert (repo / "data" / "cases_archive_2025.json").read_text() == "cold-before"
    assert not (repo / "data" / "cases_archive_2026.json").exists()
    assert context.read_text() == "context-wal"
    assert not journal.exists()


def test_matching_wal_ack_preserves_parser_state(tmp_path):
    repo, journal, ack, context, txn_id = _setup(tmp_path)
    _write(repo / "data" / "cases.json", "cases-after")
    _write(context, "context-wal")
    _write(ack, json.dumps({"txn_id": txn_id}))

    result, rc = parse_txn.recover(str(journal), str(ack))

    assert rc == 0 and result == "wal_committed"
    assert (repo / "data" / "cases.json").read_text() == "cases-after"
    assert context.read_text() == "context-wal"
    assert not journal.exists() and not ack.exists()


def test_success_without_ack_is_allowed_only_when_data_is_clean(tmp_path):
    _repo, journal, ack, _context, txn_id = _setup(tmp_path)
    result, rc = parse_txn.finish(str(journal), str(ack), txn_id)
    assert rc == 0 and result == "clean_without_wal"


def test_success_with_mutation_but_without_wal_rolls_back_and_fails(tmp_path):
    repo, journal, ack, _context, txn_id = _setup(tmp_path)
    _write(repo / "data" / "cases.json", "unacknowledged")

    result, rc = parse_txn.finish(str(journal), str(ack), txn_id)

    assert rc == 3 and result == "rolled_back_without_wal:1"
    assert (repo / "data" / "cases.json").read_text() == "cases-before"


def test_manifest_is_not_published_when_snapshot_copy_fails(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    journal = repo / ".runtime" / "parse.json"
    ack = repo / ".runtime" / "ack.json"
    _write(repo / "data" / "cases.json", "before")
    monkeypatch.setattr(
        parse_txn, "_copy_durable",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )

    try:
        parse_txn.prepare(
            str(journal), str(ack), str(repo),
            "data/last_digest_context.json", ["data/cases.json"],
        )
    except OSError:
        pass
    else:
        raise AssertionError("prepare must propagate snapshot failure")
    assert not journal.exists()
