# -*- coding: utf-8 -*-
"""Охват Тюмени, ручной ввод местных дел и территориальный фильтр 7kas."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from court_monitor.courts import canon_sudrf_domain, courts_for_search, match_region_first_instance
from court_monitor.regions import get_region


REGION = get_region("tyumen")
REGISTRY_PATH = Path(__file__).resolve().parents[2] / "docs/regions/tyumen_courts.json"
REGISTRY = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
SOURCE_COURTS = (*REGION.first_instance_courts, *REGION.appeal_courts, REGION.cassation_court)
LOCAL_SECTIONS = (*REGION.first_instance_courts, *REGION.appeal_courts, *REGION.presidium_courts)
ALL_SECTIONS = (*LOCAL_SECTIONS, REGION.cassation_court)


def test_registry_preserves_all_source_records_and_original_urls():
    assert len(REGION.first_instance_courts) == 24
    assert len(REGION.appeal_courts) == 1
    assert len(SOURCE_COURTS) == 26
    assert len({court.domain for court in ALL_SECTIONS}) == 26
    assert len(ALL_SECTIONS) == 27
    assert len(REGION.presidium_courts) == 1
    assert REGISTRY["source"]["sha256"] == (
        "cac98f29721b2c9baf446aad186a8a8b16a923b8df6afa01f0f6742f116475de"
    )
    assert [record["source_row"] for record in REGISTRY["records"]] == list(range(1, 27))
    for record, court in zip(REGISTRY["records"], SOURCE_COURTS):
        assert record["source_url"] == record["source_hyperlink"]
        assert record["runtime_domain"] == court.domain
        assert record["runtime_name"] == court.name
        assert record["record_type"] == court.court_type
        source_host = urlsplit(record["source_url"]).hostname
        assert source_host == record["source_domain"]
        assert court.domain == record["canonical_domain"] == canon_sudrf_domain(source_host)
        assert urlsplit(court.base_url).hostname == court.domain
    assert all(court.domain.endswith("--tum.sudrf.ru") for court in LOCAL_SECTIONS)
    assert all(record["source_domain"].endswith(".tum.sudrf.ru") for record in REGISTRY["records"][:25])


def test_district_search_stays_gated_but_oblast_sections_search_automatically():
    assert courts_for_search(list(REGION.first_instance_courts)) == []
    assert all(court.enabled for court in LOCAL_SECTIONS)
    assert all(court.search_gated and court.search_disabled for court in REGION.first_instance_courts)
    for sources in (REGION.appeal_courts, REGION.presidium_courts):
        assert all(not court.search_gated and not court.search_disabled for court in sources)
        assert courts_for_search(list(sources)) == list(sources)
    assert REGION.cassation_court.enabled
    assert not REGION.cassation_court.search_gated
    assert not REGION.cassation_court.search_disabled
    assert REGION.manual_import_all_courts


@pytest.mark.parametrize("record", REGISTRY["records"][:24], ids=lambda row: str(row["source_row"]))
def test_cassation_matches_every_first_instance_from_workbook(record):
    court = match_region_first_instance(record["source_name"], REGION)
    assert court is not None
    assert court.domain == record["runtime_domain"]


@pytest.mark.parametrize("name", [
    "Ленинский районный суд г. Екатеринбурга Свердловской области",
    "Центральный районный суд г. Челябинска Челябинской области",
    "Калининский районный суд г. Уфы Республики Башкортостан",
    "Казанский районный суд Республики Татарстан",
    "Тюменцевский районный суд Алтайского края",
    "Сургутский городской суд Ханты-Мансийского автономного округа-Югры",
    "Салехардский городской суд Ямало-Ненецкого автономного округа",
    "Суд Ханты-Мансийского автономного округа-Югры",
    "Суд Ямало-Ненецкого автономного округа",
    "Мировой судья судебного участка № 1 Тюменской области",
    "Казанский районный суд",  # без территории одноимённый суд не угадываем
    "",
])
def test_cassation_rejects_foreign_courts_magistrates_and_unknown_origin(name):
    assert match_region_first_instance(name, REGION) is None


def test_cassation_matches_tyumen_oblast_court_without_confusing_district():
    assert match_region_first_instance("Тюменский областной суд", REGION) is REGION.appeal_courts[0]
    district = match_region_first_instance("Тюменский районный суд Тюменской области", REGION)
    assert district is not None
    assert district.domain == "tumensky--tum.sudrf.ru"
    assert district.court_type == "first_instance"


def test_seventh_cassation_preserves_existing_search_and_card_parameters():
    court = REGION.cassation_court
    search = urlsplit(court.search_url())
    query = parse_qs(search.query)
    assert search.hostname == "7kas.sudrf.ru"
    assert query["name_op"] == ["r"]
    assert query["delo_id"] == ["2800001"]
    assert query["delo_table"] == ["g33_case"]
    assert query["new"] == ["2800001"]
    assert query["srv_num"] == ["1"]
    assert parse_qs(search.query, encoding="windows-1251")["G33_PARTS__NAMESS"] == ["Сбербанк"]
    assert court.search_url() == get_region("hmao").cassation_court.search_url()
    card_query = parse_qs(urlsplit(court.card_url("123", "example-uid")).query)
    assert card_query["case_id"] == ["123"]
    assert card_query["case_uid"] == ["example-uid"]
    assert card_query["delo_id"] == card_query["new"] == ["2800001"]
    assert REGION.health_cassation_keys() == ("cassation:7kas:total", "cassation:7kas:tyumen")


def test_public_region_supports_operator_imports_with_tyumen_time():
    info = REGION.public_info()
    assert info["code"] == "tyumen"
    assert info["manual_import_all_courts"]
    assert info["timezone"] == "Asia/Yekaterinburg"
    sources = [*info["fi_courts"], *info["appeal_courts"], *info["presidium_courts"], info["cassation"]]
    assert len(sources) == 27
    assert all(source["timezone"] == "Asia/Yekaterinburg" for source in sources)
    presidium, = info["presidium_courts"]
    appeal, = info["appeal_courts"]
    assert presidium["name"] == "Президиум Тюменского областного суда"
    assert presidium["domain"] == appeal["domain"] == "oblsud--tum.sudrf.ru"
    assert presidium["delo_id"] == presidium["new"] == 2800001
    assert appeal["delo_id"] == appeal["new"] == 5
    assert presidium["cassation_kind"] == "presidium"
    assert not presidium["search_gated"] and not presidium["search_disabled"]
    assert not appeal["search_gated"] and not appeal["search_disabled"]
