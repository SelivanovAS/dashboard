"""Регрессии ложного «в этом суде» при импортах Тюмени 21.09.2026."""

from dataclasses import replace
from copy import deepcopy

import pytest

from court_monitor import fi_identity, linking
from court_monitor.fi_identity import (
    resolve_fi_identity, compare_fi_identity, fi_number_tracking_status,
)
from court_monitor.regions import get_region
from court_monitor.regions.base import CourtConfig


@pytest.fixture
def region(monkeypatch):
    courts = (
        CourtConfig("Абатский районный суд", "abatsky--tum.sudrf.ru", 1540005, "first_instance"),
        CourtConfig("Армизонский районный суд", "armizonsky--tum.sudrf.ru", 1540005, "first_instance"),
    )
    value = replace(get_region("hmao"), first_instance_courts=courts,
                    fi_region_markers=("тюменской области",), appeal_long_markers=())
    monkeypatch.setattr(fi_identity, "get_region", lambda: value)
    return value


def record(number="2-65/2026", **fi):
    return {"id": number, "first_instance": {"case_number": number, **fi}}


def test_tyumen_long_name_does_not_block_another_court(region):
    case = record(court="Армизонский районный суд Тюменской области")
    exact, unknown = linking.collect_fi_dedup_index([case])
    assert not unknown
    assert fi_number_tracking_status("2-65/2026", region.first_instance_courts[0].domain,
                                     exact, unknown) == "free"
    assert fi_number_tracking_status("2-65/2026 (2-650/2025;)",
                                     region.first_instance_courts[1].domain,
                                     exact, unknown) == "tracked"
    assert case["first_instance"].get("court_domain") is None


def test_long_name_requires_its_region(region):
    identity = resolve_fi_identity({"court": "Абатский районный суд другого региона"})
    assert identity.status == "needs_review" and not identity.domain


@pytest.mark.parametrize("code,name", [
    ("hmao", "Березовский районный суд Ханты-Мансийского автономного округа-Югры"),
    ("sverdlovsk_yanao", "Ивдельский городской суд Свердловской области"),
    ("bashkortostan", "Бижбулякский районный суд Республики Башкортостан"),
])
def test_other_territories_and_registered_aliases(code, name):
    identity = resolve_fi_identity({"court": name}, region=get_region(code))
    assert identity.status == "resolved" and identity.domain


def test_ambiguous_alias_and_conflicting_domain_require_review(region):
    a, b = region.first_instance_courts
    ambiguous = replace(region, first_instance_courts=(
        replace(a, name_aliases=("Старый районный суд",)),
        replace(b, name_aliases=("Старый районный суд",)),
    ))
    assert resolve_fi_identity({"court": "Старый районный суд"}, region=ambiguous).status == "needs_review"
    identity = resolve_fi_identity({"court_domain": a.domain, "court": b.name})
    assert identity.status == "needs_review"
    assert compare_fi_identity({"court_domain": a.domain, "court": b.name},
                               {"court_domain": a.domain}) == "needs_review"


def test_shared_domain_requires_site_and_never_chooses_first():
    region = get_region("sverdlovsk_yanao")
    domain = "kamyshlovsky--svd.sudrf.ru"
    unknown = {"court": "Камышловский районный суд Свердловской области"}
    assert resolve_fi_identity(unknown, region=region).status == "needs_review"
    assert compare_fi_identity(unknown, {"court_domain": domain, "srv_num": 1},
                               region=region) == "needs_review"
    assert compare_fi_identity({"court_domain": domain, "srv_num": 1},
                               {"court_domain": domain, "srv_num": 2}, region=region) == "different"
    site = resolve_fi_identity({"court": "Камышловский районный суд (п.п. Пышма)"}, region=region)
    assert site.status == "resolved" and site.srv_num == 2
    contradictory = {"court": "Камышловский районный суд (п.п. Пышма)",
                     "court_domain": domain, "srv_num": 1}
    assert resolve_fi_identity(contradictory, region=region).status == "needs_review"
    unregistered = {"court_domain": domain, "srv_num": 99}
    assert resolve_fi_identity(unregistered, region=region).status == "needs_review"
    assert compare_fi_identity(unregistered, {"court_domain": domain, "srv_num": 1},
                               region=region) == "needs_review"


def test_single_court_can_change_technical_server(region):
    domain = region.first_instance_courts[0].domain
    assert compare_fi_identity({"court_domain": domain, "srv_num": 1},
                               {"court_domain": domain, "srv_num": 2}) == "same"


@pytest.mark.parametrize("code,domain,name", [
    ("hmao", "surggor--hmao.sudrf.ru", "Сургутский городской суд Свердловской области"),
    ("hmao", "hantymansisky--hmao.sudrf.ru", "Ханты-Мансийский районный суд Свердловской области"),
    ("bashkortostan", "leninsky--bkr.sudrf.ru", "Ленинский районный суд г. Тюмени Тюменской области"),
    ("bashkortostan", "leninsky--bkr.sudrf.ru", "Ленинский районный суд Республики Сербия"),
])
def test_explicit_foreign_region_conflicts_with_local_domain(code, domain, name):
    region = get_region(code)
    fi = {"court_domain": domain, "court": name}
    assert resolve_fi_identity(fi, region=region).status == "needs_review"
    assert compare_fi_identity(fi, {"court_domain": domain}, region=region) == "needs_review"


def test_uid_conflict_is_not_an_already_match(region):
    domain = region.first_instance_courts[0].domain
    exact, unknown = linking.collect_fi_dedup_index([
        record(court_domain=domain, judicial_uid="uid-one"),
    ])
    assert fi_number_tracking_status("2-65/2026", domain, exact, unknown,
                                     judicial_uid="uid-two") == "needs_review"
    assert fi_number_tracking_status("2-65/2026", domain, exact, unknown,
                                     judicial_uid="uid-one") == "tracked"


def test_conflicting_uids_in_existing_stages_require_review(region):
    domain = region.first_instance_courts[0].domain
    case = record(court_domain=domain, judicial_uid="one")
    case["appeal"] = {"judicial_uid": "two"}
    exact, unknown = linking.collect_fi_dedup_index([case])
    assert fi_number_tracking_status("2-65/2026", domain, exact, unknown) == "needs_review"


def test_missing_court_is_needs_review_even_for_legacy_sets(region):
    domain = region.first_instance_courts[0].domain
    exact, unknown = linking.collect_fi_dedup_index([record()])
    assert fi_number_tracking_status("2-65/2026", domain, exact, unknown) == "needs_review"
    assert fi_number_tracking_status("2-65/2026", domain, set(), {"2-65/2026"}) == "needs_review"
    assert not linking.is_fi_number_tracked("2-65/2026", domain, exact, unknown)


def test_new_index_entries_keep_site_and_uid(monkeypatch):
    monkeypatch.setattr(fi_identity, "get_region", lambda: get_region("sverdlovsk_yanao"))
    domain = "kamyshlovsky--svd.sudrf.ru"
    exact, unknown = linking.collect_fi_dedup_index([])
    exact.add_identity(domain, "2-10/2026", {"court_domain": domain, "srv_num": 2,
                                           "judicial_uid": "one"})
    assert fi_number_tracking_status("2-10/2026", domain, exact, unknown, srv_num=2) == "tracked"
    assert fi_number_tracking_status("2-10/2026", domain, exact, unknown, srv_num=1) == "free"
    assert fi_number_tracking_status("2-10/2026", domain, exact, unknown) == "needs_review"
    exact.discard((domain, "2-10/2026"))
    exact.add((domain, "2-10/2026"))
    assert fi_number_tracking_status("2-10/2026", domain, exact, unknown, srv_num=2) == "needs_review"


def test_archive_does_not_drop_different_unknown_records(region):
    old, new = record(), record()
    assert linking.dedupe_new_archive_entries([old], [new]) == [new]
    own = record(court=region.first_instance_courts[0].name)
    assert linking.dedupe_new_archive_entries([own], [deepcopy(own)]) == []


def test_magistrate_never_blocks_district_court(region):
    exact, unknown = linking.collect_fi_dedup_index([record(magistrate=True)])
    assert not exact and not unknown


def test_backfill_never_searches_appeal_section_for_fi(monkeypatch):
    monkeypatch.setattr(fi_identity, "get_region", lambda: get_region("sverdlovsk_yanao"))
    monkeypatch.setattr(linking, "fetch_page", lambda *a, **k: pytest.fail("network"))
    case = record("2-1/2026", court="Суд Ямало-Ненецкого автономного округа",
                  court_domain="oblsud--ynao.sudrf.ru")
    case["current_stage"] = "first_instance"
    assert linking.backfill_fi_links([case]) == 0
