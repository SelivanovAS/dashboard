"""Найденное дело переживает вытеснение из выдачи и сбой перед save базы."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import json

import pytest

from court_monitor import config, discovery_queue as dq
from court_monitor.regions import get_region


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "REGION", "hmao")
    path = tmp_path / "cases.json"
    path.write_text(json.dumps({"cases": [{"id": "disk-only"}], "other": 7}))
    monkeypatch.setattr(config, "JSON_PATH", str(path))
    return path, get_region().first_instance_courts[0], datetime(2026, 10, 7, 6)


def row(number="2-1/2026", uid="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"):
    return {"case_number": number, "link": "123|" + uid, "bank_role": "Истец"}


def test_checkpoint_preserves_disk_case_tracks_and_other_metadata(env):
    path, court, now = env
    data = {"cases": [{"id": "in-memory-bank-must-not-leak"}], "other": 99}
    dq.enqueue(data, "bank", court, [row()], now=now)
    disk = json.loads(path.read_text())
    assert disk["cases"] == [{"id": "disk-only"}]
    assert disk["other"] == 7
    assert len(disk[dq.FIELD]["items"]) == 1


def test_new_queue_is_saved_before_fetch_and_capped_candidates_survive(env):
    path, court, now = env
    data = {}
    keys = dq.enqueue(data, "bank", court, [row(), row("2-2/2026")], now=now)
    assert len(json.loads(path.read_text())[dq.FIELD]["items"]) == 2
    dq.defer(data, keys[1], "per_court_cap", now=now)
    # Завтра выдача уже не содержит первую строку. Она осталась в очереди.
    restored = json.loads(path.read_text())
    dq.enqueue(restored, "bank", court, [row("2-3/2026")], now=now + timedelta(days=1))
    assert {r["case_number"] for r in dq.due_rows(restored, "bank", court, now=now + timedelta(days=1))} == {
        "2-1/2026", "2-2/2026", "2-3/2026"}


def test_failed_attempt_cooldown_daily_limit_and_next_day(env):
    path, court, now = env
    data = {}
    key, = dq.enqueue(data, "bank", court, [row()], now=now)
    assert dq.begin_attempt(data, key, now=now)
    dq.defer(data, key, "read_timeout", now=now)
    data = json.loads(path.read_text())
    assert not dq.due_rows(data, "bank", court, now=now + timedelta(minutes=29))
    assert dq.begin_attempt(data, key, now=now + timedelta(minutes=30))
    dq.defer(data, key, "connection_reset", now=now + timedelta(minutes=30))
    assert not dq.due_rows(data, "bank", court, now=now + timedelta(hours=3))
    assert dq.retry_reason(dq.tasks(data)[key], now + timedelta(hours=3)) == "daily_retry_limit"
    assert dq.begin_attempt(data, key, now=now + timedelta(days=1))
    assert dq.tasks(data)[key]["attempt_count"] == 1


def test_terminal_rejection_not_retried_when_seen_again(env):
    _, court, now = env
    data = {}
    key, = dq.enqueue(data, "bank", court, [row()], now=now)
    dq.finish(data, key, accepted=False, reason="excluded_writ", now=now)
    later = now + timedelta(days=30)
    dq.enqueue(data, "bank", court, [row()], now=later)
    dq.reconcile(data, "bank", court, lambda _: False, now=later)
    assert not dq.due_rows(data, "bank", court, now=later)
    assert dq.tasks(data)[key]["reason"] == "excluded_writ"


def test_accepted_missing_after_crash_is_restored_but_known_case_not_queued(env):
    path, court, now = env
    data = {}
    assert dq.enqueue(data, "bank", court, [row()], known=lambda _: True, now=now) == []
    key, = dq.enqueue(data, "bank", court, [row()], now=now)
    dq.finish(data, key, accepted=True, reason="admitted", now=now)
    restored = json.loads(path.read_text())
    dq.reconcile(restored, "bank", court, lambda _: False, now=now + timedelta(hours=1))
    assert dq.tasks(restored)[key]["reason"] == "admission_not_committed"
    assert len(dq.due_rows(restored, "bank", court, now=now + timedelta(hours=1))) == 1
    dq.reconcile(restored, "bank", court, lambda _: True, now=now + timedelta(hours=2))
    assert not dq.due_rows(restored, "bank", court, now=now + timedelta(hours=2))


def test_identity_separates_region_source_court_site_and_uid(env, monkeypatch):
    _, court, _ = env
    original = dq.key("bank", court, row())
    assert dq.key("bank", replace(court, srv_num=2), row()) != original
    assert dq.key("bank", replace(court, domain="other.sudrf.ru"), row()) != original
    assert dq.key("cassation", court, row()) != original
    assert dq.key("bank", court, row(uid="another-uid")) != original
    monkeypatch.setattr(config, "REGION", "bashkortostan")
    assert dq.key("bank", court, row()) != original


def test_alternate_site_keeps_source_owner_and_uses_registered_target(env):
    _, _, now = env
    courts = get_region().first_instance_courts
    source = next(c for c in courts if c.domain == "vartovray--hmao.sudrf.ru" and c.srv_num == 1)
    target = next(c for c in courts if c.domain == source.domain and c.srv_num == 2)
    candidate = dict(row(), href_srv_num=2)
    data = {}
    task_key, = dq.enqueue(data, "bank", source, [candidate], now=now)
    assert dq.tasks(data)[task_key]["source_srv_num"] == 1
    assert dq.tasks(data)[task_key]["srv_num"] == 2
    pending, = dq.due_rows(data, "bank", source, now=now)
    assert dq.target_court("bank", source, pending) == target
    assert not dq.due_rows(data, "bank", target, now=now)  # один владелец работы
    invalid = dict(row("2-2/2026"), href_srv_num=999)
    invalid_key, = dq.enqueue(data, "bank", source, [invalid], now=now)
    assert len(dq.due_rows(data, "bank", source, now=now)) == 1
    assert dq.tasks(data)[invalid_key]["reason"] == "unknown_court_site"


def test_only_confirmed_accepted_can_expire(env):
    _, court, now = env
    data = {}
    accepted, missing, pending = dq.enqueue(data, "bank", court,
        [row("2-1/2026"), row("2-2/2026"), row("2-3/2026")], now=now)
    for task_key in (accepted, missing):
        dq.finish(data, task_key, accepted=True, reason="admitted", now=now)
    dq.reconcile(data, "bank", court, lambda r: r["case_number"] == "2-1/2026",
                 now=now + timedelta(days=31))
    assert accepted not in dq.tasks(data)
    assert dq.tasks(data)[missing]["reason"] == "admission_not_committed"
    assert dq.tasks(data)[pending]["status"] == "pending"


def test_report_explains_unread_work_without_party_data(env):
    _, court, now = env
    data = {}
    limited, review, due = dq.enqueue(data, "bank", court,
        [dict(row("2-1/2026"), plaintiff="PRIVATE PARTY"), row("2-2/2026"), row("2-3/2026")], now=now)
    dq.defer(data, limited, "run_intake_cap", now=now)
    dq.finish(data, review, accepted=False, reason="needs_review", now=now)
    result = dq.report(data, now=now)
    assert result["pending"] == result["due"] == 2
    assert result["capped"] == result["needs_review"] == result["rejected"] == 1
    assert result["date"] == "2026-10-07" and result["region"] == "hmao"
    assert "PRIVATE PARTY" not in json.dumps(result)


def test_cassation_roundtrip_restores_region_filter_and_rejects_foreign(env):
    path, court, now = env
    cass = get_region().cassation_court
    candidate = {"cassation_internal_number": "8Г-1/2026", "case_id": "12", "case_uid": "aa-bb",
                 "fi_court_config": court, "fi_case_number": "2-1/2026"}
    data = {}
    dq.enqueue(data, "cassation", cass, [candidate], now=now)
    restored = json.loads(path.read_text())
    decoded, = dq.due_rows(restored, "cassation", cass, now=now)
    assert decoded["fi_court_config"] == court
    foreign = deepcopy(candidate)
    foreign["case_uid"] = "foreign"
    foreign["fi_court_config"] = replace(court, domain="foreign.sudrf.ru")
    foreign_key, = dq.enqueue(restored, "cassation", cass, [foreign], now=now)
    assert len(dq.due_rows(restored, "cassation", cass, now=now)) == 1
    assert dq.tasks(restored)[foreign_key]["reason"] == "territory_unresolved"


@pytest.fixture
def bank_pipeline(env, monkeypatch):
    from court_monitor import runs, netutil
    path, court, now = env
    clock = [now]
    real_now = dq._now
    monkeypatch.setattr(dq, "_now", lambda value=None: real_now(value or clock[0]))
    monkeypatch.setattr(runs, "polite_delay", lambda: None)
    monkeypatch.setattr(runs, "card_breaker_allows", lambda _: True)
    monkeypatch.setattr(netutil, "run_deadline_reached", lambda: False)
    monkeypatch.setattr(config, "BANK_INTAKE_DRY_RUN", False)
    monkeypatch.setattr(runs, "parse_case_card", lambda *a: {"Статус": "В производстве", "_table_count": 4})
    data, index, seen = {}, set(), {}
    candidate = dict(row(), plaintiff="ПАО Сбербанк", defendant="Ответчик",
        category="Кредит", court=court.name, court_domain=court.domain,
        judge="", filing_date="07.10.2026", status="В производстве", result="",
        court_delo_id=court.delo_id, court_srv_num=court.srv_num)
    def intake(rows, budget=30):
        return runs.intake_bank_rows(court, rows, dedup_exact=index, dedup_wildcard=set(),
                                    seen=seen, budget=budget, queue_data=data)
    return runs, candidate, intake, data, clock, path


def test_bank_network_failure_retried_without_search_rows(bank_pipeline, monkeypatch):
    runs, candidate, intake, data, clock, path = bank_pipeline
    requests = []
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: requests.append(1) or "")
    assert intake([candidate])[1]["fetch_fail"] == 1
    assert len(json.loads(path.read_text())[dq.FIELD]["items"]) == 1
    assert intake([])[1]["cards"] == 0  # тот же слот / cooldown
    clock[0] += timedelta(minutes=31)
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: requests.append(1) or "<html/>")
    entries, counters = intake([])  # выдачу вообще не загружали
    assert len(entries) == counters["added"] == 1 and len(requests) == 2
    assert next(iter(dq.tasks(data).values()))["status"] == "accepted"


def test_bank_zero_budget_keeps_candidates_for_later_slot(bank_pipeline, monkeypatch):
    runs, candidate, intake, data, clock, _ = bank_pipeline
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: pytest.fail("cap must precede HTTP"))
    entries, counters = intake([candidate], budget=0)
    assert not entries and counters["capped"] == 1
    task = next(iter(dq.tasks(data).values()))
    assert task["reason"] == "run_intake_cap" and task["attempt_count"] == 0
    clock[0] += timedelta(minutes=31)
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: "<html/>")
    assert len(intake([])[0]) == 1


def test_bank_card_terminal_rejection_keeps_admission_rules(bank_pipeline, monkeypatch):
    runs, candidate, intake, data, clock, _ = bank_pipeline
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: "<html/>")
    monkeypatch.setattr(runs, "parse_case_card", lambda *a: {
        "Статус": "Решено", "_table_count": 4, "Результат": "Дело передано ПО ПОДСУДНОСТИ"})
    entries, counters = intake([candidate])
    assert not entries and counters["excluded_result"] == 1
    task = next(iter(dq.tasks(data).values()))
    assert task["status"] == "rejected" and task["reason"] == "excluded_result"
    clock[0] += timedelta(days=1)
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: pytest.fail("terminal refusal must not retry"))
    assert not intake([])[0]


def test_bank_known_case_does_not_enter_discovery_queue(bank_pipeline, env, monkeypatch):
    runs, candidate, intake, data, _, _ = bank_pipeline
    monkeypatch.setattr(runs, "row_tracking_status", lambda *a, **k: "tracked")
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **k: pytest.fail("known card"))
    assert not intake([candidate])[0]
    assert not dq.tasks(data)
