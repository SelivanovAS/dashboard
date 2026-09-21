"""Регрессии приема: неизвестность сохраняется, чужой суд не блокирует кандидата."""
from datetime import date
import json
from pathlib import Path

import pytest

import import_search_dump as dumps
import import_bank_registry as registry
import collect_bank_claims as collector
from court_monitor import config, identity_review as review, runs, targeted_add
from court_monitor.linking import collect_fi_dedup_index
from court_monitor.regions import get_region


@pytest.fixture
def env(tmp_path, monkeypatch):
    for name, filename in {
        "JSON_PATH": "cases.json", "JSON_ARCHIVE_PATH": "cases_archive.json",
        "JSON_BANK_PATH": "cases_bank.json", "JSON_BANK_EVENTS_PATH": "cases_bank_events.json",
        "JSON_BANK_ARCHIVE_PATH": "cases_bank_archive.json",
        "JSON_BANK_ARCHIVE_EVENTS_PATH": "cases_bank_archive_events.json",
        "BANK_INTAKE_SEEN_PATH": ".bank_intake_seen.json",
        "FI_IDENTITY_REVIEW_PATH": "fi_identity_review.json",
    }.items():
        monkeypatch.setattr(config, name, str(tmp_path / filename))
    monkeypatch.setattr(config, "REGION", "hmao")
    monkeypatch.setattr(config, "BANK_TRACK", True)
    monkeypatch.setattr(config, "BANK_INTAKE_DRY_RUN", False)
    for mod in (dumps, registry, collector, targeted_add, runs):
        monkeypatch.setattr(mod, "fetch_card_checked", lambda *a, **k: pytest.fail("unexpected card request"))
    return tmp_path


def court():
    return next(c for c in get_region("hmao").first_instance_courts
                if c.domain == "surggor--hmao.sudrf.ru")


def row(number="2-64/2026", selected=None):
    selected = selected or court()
    return {"case_number": number, "court": selected.name, "court_domain": selected.domain,
            "court_srv_num": selected.srv_num, "href_srv_num": selected.srv_num,
            "link": "123|fake-uuid", "bank_role": "Ответчик", "plaintiff": "Иванов",
            "defendant": "ПАО Сбербанк", "filing_date": date.today().isoformat(),
            "status": "В производстве", "court_delo_id": 1540005, "category": "кредит",
            "judge": "", "result": ""}


def save_cases(env, cases):
    (env / "cases.json").write_text(json.dumps({"version": 1, "cases": cases}, ensure_ascii=False))


def review_items(env):
    return json.loads((env / "fi_identity_review.json").read_text())["items"]


def test_quarantine_is_idempotent_and_closes_when_added(env):
    candidate = row()
    review.remember_identity_review(candidate, source="dump")
    candidate["judicial_uid"] = "72RS0001-01-2026-000001-01"
    review.remember_identity_review(candidate, source="auto_search")
    assert len(review_items(env)) == 1
    assert review_items(env)[0]["source"] == "auto_search"
    review.resolve_identity_review(candidate, outcome="added")
    assert review_items(env)[0]["status"] == "needs_review"
    review.flush_identity_reviews()
    assert review_items(env)[0]["status"] == "resolved"
    assert review_items(env)[0]["outcome"] == "added"


def test_quarantine_dry_run_does_not_write(env):
    review.remember_identity_review(row(), source="dump", dry_run=True)
    assert not (env / "fi_identity_review.json").exists()


@pytest.mark.parametrize("dry_run", [False, True])
def test_dump_unknown_court_never_says_already_or_adds(env, dry_run):
    original = {"id": "2-64/2026", "current_stage": "appeal", "first_instance": {}}
    save_cases(env, [original])
    result = dumps.import_rows([row()], {}, "тест", dry_run)
    assert result["counters"]["needs_review"] == 1
    assert result["counters"]["already"] == result["counters"]["added"] == 0
    assert json.loads((env / "cases.json").read_text())["cases"] == [original]
    if not dry_run:
        assert review_items(env)[0]["candidate"]["link"] == "123|fake-uuid"


def test_dump_full_name_of_other_court_does_not_block(env, monkeypatch):
    save_cases(env, [{"id": "2-64/2026", "first_instance": {
        "case_number": "2-64/2026", "court": "Нефтеюганский районный суд Ханты-Мансийского автономного округа-Югры"}}])
    monkeypatch.setattr(dumps, "_fetch_main_card", lambda *a: ({"Статус": "В производстве"}, ""))
    result = dumps.import_rows([row()], {}, "тест", False)
    assert result["counters"]["added"] == 1
    assert result["counters"]["already"] == result["counters"]["needs_review"] == 0
    cases = json.loads((env / "cases.json").read_text())["cases"]
    assert len(cases) == 2
    assert cases[0]["first_instance"]["court_domain"] == court().domain


def test_dump_conflicting_uid_never_overwrites_existing(env):
    candidate = row()
    candidate["judicial_uid"] = "new-uid"
    original = {"id": candidate["case_number"], "first_instance": {
        "court_domain": court().domain, "judicial_uid": "old-uid"}}
    save_cases(env, [original])
    result = dumps.import_rows([candidate], {}, "тест", False)
    assert result["counters"]["needs_review"] == 1
    assert result["counters"]["already"] == 0
    assert json.loads((env / "cases.json").read_text())["cases"] == [original]


def test_targeted_active_unknown_court_needs_review(env, monkeypatch):
    selected = court()
    save_cases(env, [{"id": "2-64/2026", "first_instance": {}}])
    monkeypatch.setattr(targeted_add, "courts_for_search", lambda *a, **k: [selected])
    monkeypatch.setattr(targeted_add, "fetch_page", lambda *a, **k: "<html/>")
    monkeypatch.setattr(targeted_add, "parse_first_instance_search", lambda *a, **k: [row()])
    monkeypatch.setattr(targeted_add, "polite_delay", lambda: None)
    state = targeted_add.load_tracked_state()
    result = targeted_add.process_item(state, "2-64/2026", "тест", "2026-09-21T12:00:00", selected)
    assert result["status"] == "needs_review"
    assert not state["dirty"]
    assert len(review_items(env)) == 1


def test_registry_unknown_court_is_review_not_already(env):
    save_cases(env, [{"id": "2-64/2026", "first_instance": {}}])
    result = registry.import_registry([(court().domain, "2-64/2026")], 0, "тест")
    assert result["needs_review"] == 1
    assert result["already"] == result["added"] == 0
    assert len(review_items(env)) == 1


def test_collector_unknown_court_is_review_not_already(env, monkeypatch):
    candidate = row()
    candidate["bank_role"] = "Истец"
    save_cases(env, [{"id": "2-64/2026", "first_instance": {}}])
    monkeypatch.setattr(collector, "fetch_search_rows", lambda *a: ([candidate], 1))
    result = collector.collect(court(), 1, 0, False, "тест")
    assert result["needs_review"] == 1
    assert result["already"] == result["added"] == 0
    assert len(review_items(env)) == 1


def test_auto_bank_unknown_court_saved_outside_rejection_cache(env):
    candidate = row()
    candidate["bank_role"] = "Истец"
    exact, uncertain = collect_fi_dedup_index([{"id": "2-64/2026", "first_instance": {}}])
    seen = {}
    entries, result = runs.intake_bank_rows(court(), [candidate], dedup_exact=exact,
                                           dedup_wildcard=uncertain, seen=seen, budget=5)
    assert entries == [] and seen == {}
    assert result["needs_review"] == 1
    assert result["already"] == 0
    assert len(review_items(env)) == 1


def test_auto_main_quarantines_unknown_and_keeps_other_court(env):
    exact, uncertain = collect_fi_dedup_index([
        {"id": "2-64/2026", "first_instance": {}},
        {"id": "2-65/2026", "first_instance": {"court": "Нефтеюганский районный суд Ханты-Мансийского автономного округа-Югры"}},
    ])
    result = runs.filter_new_fi_rows(court(), [row(), row("2-65/2026")], exact, uncertain)
    assert [r["case_number"] for r in result] == ["2-65/2026"]
    assert len(review_items(env)) == 1


def test_shared_site_index_preserves_site_for_same_batch(env, monkeypatch):
    monkeypatch.setattr(config, "REGION", "hmao")
    sites = [c for c in get_region("hmao").first_instance_courts
             if c.domain == "vartovray--hmao.sudrf.ru"]
    first, second = sorted(sites, key=lambda c: c.srv_num)
    exact, uncertain = collect_fi_dedup_index([])
    review.add_row_to_index(exact, row(selected=second))
    assert review.row_tracking_status(row(selected=second), exact, uncertain, source="test") == "tracked"
    assert review.row_tracking_status(row(selected=first), exact, uncertain, source="test") == "free"


def test_targeted_shared_site_archive_is_not_reactivated(env, monkeypatch):
    monkeypatch.setattr(config, "REGION", "hmao")
    state = targeted_add.load_tracked_state()
    state["main_archive"]["cases"] = [{"id": "2-64/2026", "first_instance": {
        "court_domain": "vartovray--hmao.sudrf.ru", "srv_num": 1}}]
    verdict, _, _ = targeted_add.dedup_verdict(state, "vartovray--hmao.sudrf.ru", "2-64/2026", srv_num=2)
    assert verdict == "free"
    verdict, _, _ = targeted_add.dedup_verdict(state, "vartovray--hmao.sudrf.ru", "2-64/2026")
    assert verdict == "needs_review"


def test_failed_case_save_leaves_candidate_open(env, monkeypatch):
    candidate = row()
    review.remember_identity_review(candidate, source="dump")
    monkeypatch.setattr(dumps, "_fetch_main_card", lambda *a: ({"Статус": "В производстве"}, ""))
    monkeypatch.setattr(dumps, "save_json", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        dumps.import_rows([candidate], {}, "тест", False)
    assert review_items(env)[0]["status"] == "needs_review"
    assert not (env / "cases.json").exists()


def test_successful_case_save_closes_candidate(env, monkeypatch):
    candidate = row()
    review.remember_identity_review(candidate, source="dump")
    monkeypatch.setattr(dumps, "_fetch_main_card", lambda *a: ({"Статус": "В производстве"}, ""))
    result = dumps.import_rows([candidate], {}, "тест", False)
    assert result["counters"]["added"] == 1
    assert review_items(env)[0]["status"] == "resolved"
    assert json.loads((env / "cases.json").read_text())["cases"][0]["id"] == "2-64/2026"


def test_targeted_archive_appeal_uid_conflict_requires_review(env):
    state = targeted_add.load_tracked_state()
    state["main_archive"]["cases"] = [{"id": "2-64/2026", "first_instance": {
        "court_domain": court().domain}, "appeal": {"judicial_uid": "old-uid"}}]
    verdict, _, _ = targeted_add.dedup_verdict(state, court().domain, "2-64/2026",
                                             judicial_uid="new-uid")
    assert verdict == "needs_review"
    assert not state["dirty"]


@pytest.mark.parametrize("record_count", [1, 2])
def test_material_uncertainty_prevents_creating_second_record(env, record_count):
    candidate = row()
    candidate["material_number"] = "М-64/2026"
    fi = {} if record_count == 1 else {"court_domain": court().domain}
    original = [{"id": "М-64/2026", "first_instance": fi} for _ in range(record_count)]
    save_cases(env, original)
    result = dumps.import_rows([candidate], {}, "тест", False)
    assert result["counters"]["needs_review"] == 1
    assert result["counters"]["added"] == result["counters"]["promoted"] == 0
    assert json.loads((env / "cases.json").read_text())["cases"] == original
    assert review_items(env)[0]["candidate"]["case_number"] == "2-64/2026"
    assert review_items(env)[0]["candidate"]["material_number"] == "М-64/2026"


def test_auto_filter_preserves_unknown_material_candidate(env):
    exact, uncertain = collect_fi_dedup_index([{"id": "М-64/2026", "first_instance": {}}])
    candidate = row()
    candidate["material_number"] = "М-64/2026"
    assert runs.filter_new_fi_rows(court(), [candidate], exact, uncertain) == []
    assert review_items(env)[0]["candidate"]["material_number"] == "М-64/2026"


def test_stored_site_is_not_replaced_by_court_default(env):
    selected = next(c for c in get_region("hmao").first_instance_courts
                    if c.domain == "vartovray--hmao.sudrf.ru" and c.srv_num == 1)
    stored = {"case_number": "2-64/2026", "court_domain": selected.domain, "srv_num": 2}
    exact, uncertain = collect_fi_dedup_index([{"id": "2-64/2026", "first_instance": stored}])
    identity = review.row_identity(stored, court=selected)
    assert identity["srv_num"] == 2
    assert review.row_tracking_status(stored, exact, uncertain, source="test",
                                      court=selected) == "tracked"
    assert stored["srv_num"] == 2
