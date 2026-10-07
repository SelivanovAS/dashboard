#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Crash recovery for parser data before digest-context WAL."""

from __future__ import annotations

import importlib.util
import json
import os
from copy import deepcopy
from types import SimpleNamespace

import pytest


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


def _candidate(number="2-1/2026", *, region="hmao", status="pending"):
    identity = [region, "bank", "test.sudrf.ru", 1540005, 1, number, "uid-" + number]
    key = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return key, {
        "key": key, "region": region, "kind": "bank", "court_domain": identity[2],
        "delo_id": identity[3], "srv_num": identity[4], "status": status,
        "row": {"case_number": number, "case_uid": identity[6], "link": "123|" + identity[6]},
        "attempt_count": 1, "reason": "discovered",
    }


def _queue(*pairs):
    return {"version": 1, "items": dict(pairs)}


def _setup_document(tmp_path, monkeypatch, before, *, region="hmao", path="data/cases.json"):
    monkeypatch.delenv("REGION", raising=False)
    monkeypatch.setenv("JSON_PATH", path)
    repo = tmp_path / "repo"
    _write(repo / "REGION", region)
    if before is not None:
        _write(repo / path, json.dumps(before))
    journal, ack = repo / ".runtime" / "parse.json", repo / ".runtime" / "ack.json"
    txn = parse_txn.prepare(str(journal), str(ack), str(repo),
                            "data/last_digest_context.json", [path])
    return repo, journal, ack, txn


def test_rollback_retains_discovery_only_and_reopens_uncommitted_admission(tmp_path, monkeypatch):
    old = _candidate()
    retained = _candidate("2-2/2026")
    before = {"version": 7, "updated_at": "before", "cases": [{"id": "old"}],
              "discovery_queue": _queue(old, retained),
              "pending_retry_context": {"events": ["old"]}, "other": "before"}
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, before)
    accepted = _candidate(status="accepted")
    pending = _candidate("2-3/2026")
    rejected = _candidate("2-4/2026", status="rejected")
    live = {"version": 1, "updated_at": "after", "cases": [{"id": "uncommitted"}],
            "discovery_queue": _queue(accepted, pending, rejected),
            "pending_retry_context": {"events": ["uncommitted"]}, "other": "after",
            "new_business_metadata": "uncommitted"}
    _write(repo / "data/cases.json", json.dumps(live))
    assert parse_txn.recover(str(journal), str(ack)) == ("rolled_back:1", 0)
    restored = json.loads((repo / "data/cases.json").read_text())
    expected = deepcopy(before)
    expected["discovery_queue"] = _queue(accepted, retained, pending, rejected)
    assert restored == expected

    from court_monitor import config, discovery_queue
    monkeypatch.setattr(config, "REGION", "hmao")
    court = SimpleNamespace(domain="test.sudrf.ru", delo_id=1540005, srv_num=1)
    discovery_queue.reconcile(restored, "bank", court, lambda _: False, persist=lambda _: None)
    tasks = restored["discovery_queue"]["items"]
    assert tasks[accepted[0]]["status"] == "pending"
    assert tasks[accepted[0]]["reason"] == "admission_not_committed"
    assert tasks[accepted[0]]["row"] == accepted[1]["row"]
    assert tasks[rejected[0]]["status"] == "rejected"


def test_rollback_filters_foreign_and_inconsistent_discovery_records(tmp_path, monkeypatch):
    before = {"version": 1, "cases": [], "metadata": {"keep": True}}
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, before, region="tyumen")
    valid = _candidate(region="tyumen")
    foreign = _candidate("2-2/2026", region="hmao")
    broken = []
    for idx, field, value in [(3, "region", "hmao"), (4, "court_domain", "other.sudrf.ru"),
                               (5, "srv_num", 2), (6, "status", "unknown"),
                               (7, "row", []), (8, "key", "another"), (9, "kind", [])]:
        key, task = _candidate(f"2-{idx}/2026", region="tyumen")
        task[field] = value
        broken.append((key, task))
    _write(repo / "data/cases.json", json.dumps({"cases": ["discard"],
           "discovery_queue": _queue(valid, foreign, *broken)}))
    assert parse_txn.recover(str(journal), str(ack))[1] == 0
    assert json.loads((repo / "data/cases.json").read_text()) == {
        **before, "discovery_queue": _queue(valid)}


@pytest.mark.parametrize("bad_queue", [[], {"version": 2, "items": {}},
                                       {"version": 1, "items": []}])
def test_malformed_discovery_does_not_prevent_snapshot_restore(tmp_path, monkeypatch, bad_queue):
    before = {"version": 1, "cases": [{"id": "before"}], "other": "keep"}
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, before)
    _write(repo / "data/cases.json", json.dumps({"cases": ["discard"], "discovery_queue": bad_queue}))
    assert parse_txn.recover(str(journal), str(ack))[1] == 0
    assert json.loads((repo / "data/cases.json").read_text()) == before


def test_interrupted_restore_reuses_journal_queue_instead_of_old_live_queue(tmp_path, monkeypatch):
    old = _candidate()
    before = {"version": 1, "cases": [], "discovery_queue": _queue(old)}
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, before)
    accepted, added = _candidate(status="accepted"), _candidate("2-2/2026")
    _write(repo / "data/cases.json", json.dumps({"cases": ["discard"],
           "discovery_queue": _queue(accepted, added)}))
    restore = parse_txn._restore_discovery
    monkeypatch.setattr(parse_txn, "_restore_discovery",
                        lambda *_: (_ for _ in ()).throw(OSError("power lost")))
    with pytest.raises(OSError, match="power lost"):
        parse_txn.recover(str(journal), str(ack))
    assert json.loads((repo / "data/cases.json").read_text()) == before
    assert json.loads(journal.read_text())[parse_txn.DISCOVERY_RECOVERY_FIELD] == _queue(accepted, added)
    monkeypatch.setattr(parse_txn, "_restore_discovery", restore)
    assert parse_txn.recover(str(journal), str(ack))[1] == 0
    assert json.loads((repo / "data/cases.json").read_text()) == {
        **before, "discovery_queue": _queue(accepted, added)}
    assert parse_txn.recover(str(journal), str(ack)) == ("absent", 1)


def test_discovery_journal_write_failure_keeps_live_data_and_snapshot(tmp_path, monkeypatch):
    before = {"version": 1, "cases": []}
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, before)
    live = {"cases": ["not yet rolled back"], "discovery_queue": _queue(_candidate())}
    _write(repo / "data/cases.json", json.dumps(live))
    original_journal = journal.read_bytes()
    monkeypatch.setattr(parse_txn, "_atomic_json",
                        lambda *_: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        parse_txn.recover(str(journal), str(ack))
    assert json.loads((repo / "data/cases.json").read_text()) == live
    assert journal.read_bytes() == original_journal


def test_unchanged_discovery_queue_keeps_clean_snapshot_bytes(tmp_path, monkeypatch):
    before = {"version": 1, "cases": [], "discovery_queue": _queue(_candidate())}
    repo, journal, ack, txn = _setup_document(tmp_path, monkeypatch, before)
    original = (repo / "data/cases.json").read_bytes()
    assert parse_txn.finish(str(journal), str(ack), txn) == ("clean_without_wal", 0)
    assert (repo / "data/cases.json").read_bytes() == original


def test_initially_absent_cases_file_keeps_only_candidate_queue(tmp_path, monkeypatch):
    repo, journal, ack, txn = _setup_document(tmp_path, monkeypatch, None, path="data/custom_cases.json")
    queue = _queue(_candidate(status="accepted"))
    _write(repo / "data/custom_cases.json", json.dumps({"version": 99, "cases": ["discard"],
           "discovery_queue": queue, "pending_retry_context": "discard"}))
    assert parse_txn.finish(str(journal), str(ack), txn) == ("rolled_back_without_wal:1", 3)
    assert json.loads((repo / "data/custom_cases.json").read_text()) == {
        "version": 1, "updated_at": "", "cases": [], "discovery_queue": queue}


def test_corrupt_preserved_queue_aborts_before_overwriting_live_state(tmp_path, monkeypatch):
    repo, journal, ack, _ = _setup_document(tmp_path, monkeypatch, {"version": 1, "cases": []})
    live = {"cases": ["after"], "discovery_queue": _queue(_candidate())}
    _write(repo / "data/cases.json", json.dumps(live))
    manifest = json.loads(journal.read_text())
    manifest[parse_txn.DISCOVERY_RECOVERY_FIELD] = _queue(_candidate(region="tyumen"))
    _write(journal, json.dumps(manifest))
    for _ in range(2):
        with pytest.raises(RuntimeError, match="повреждена сохранённая"):
            parse_txn.recover(str(journal), str(ack))
        assert json.loads((repo / "data/cases.json").read_text()) == live
        assert json.loads(journal.read_text()) == manifest
