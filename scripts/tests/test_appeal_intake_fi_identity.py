"""Приём апелляции сохраняет суд FI; однономерные сироты не смешиваются."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import sys

import pytest

from court_monitor import config
from court_monitor.appeal_intake import (
    appeal_row_to_json_case,
    enrich_appeal_row_from_card,
)
from court_monitor.lifecycle import dedupe_orphan_by_base_number
from court_monitor.regions import get_region
from court_monitor.regions.base import CourtConfig


@pytest.fixture
def tyumen(monkeypatch):
    # Территориальный реестр Тюмени живёт в её checkout; тест общей логики
    # воспроизводит подтверждённую коллизию 2-64/2026 без зависимости от него.
    appeal = CourtConfig("Тюменский областной суд", "oblsud--tum.sudrf.ru", 5, "appeal")
    region = replace(
        get_region("hmao"), code="tyumen", name="Тюменская область",
        appeal_courts=(appeal,),
        first_instance_courts=(
            CourtConfig("Абатский районный суд", "abatsky--tum.sudrf.ru", 1540005, "first_instance"),
            CourtConfig("Армизонский районный суд", "armizonsky--tum.sudrf.ru", 1540005, "first_instance"),
        ),
        fi_region_markers=("тюменской област", "тюменская област", "г. тюмени"),
        appeal_long_markers=(("тюменский областной суд", appeal.domain),),
    )
    monkeypatch.setitem(sys.modules, "court_monitor.regions.tyumen", SimpleNamespace(REGION=region))
    monkeypatch.setattr(config, "REGION", "tyumen")
    return region


def _appeal_row(court_name):
    return {
        "Номер дела": "33-321/2026",
        "Суд 1 инстанции": court_name,
        "Истец": "ПАО Сбербанк",
        "Ответчик": "Ответчик",
        "Судья 1 инстанции": "Судья",
        "Результат": "Решение оставлено без изменения",
    }


def test_long_tyumen_court_is_stored_at_appeal_intake(tyumen):
    row = _appeal_row("Армизонский районный суд Тюменской области")
    row["УИД"] = "72RS0002-01-2026-000064-11"
    original = deepcopy(row)
    court = tyumen.appeal_courts[0]
    case = appeal_row_to_json_case(row, {(court.domain, row["Номер дела"]): "2-64/2026"}, court=court)
    assert row == original
    assert case["current_stage"] == "appeal"
    assert case["first_instance"]["court_domain"] == "armizonsky--tum.sudrf.ru"
    assert case["first_instance"]["court"] == row["Суд 1 инстанции"]
    assert case["first_instance"]["case_number"] == "2-64/2026"
    assert case["first_instance"]["judicial_uid"] == row["УИД"]
    assert case["first_instance"]["srv_num"] == 1
    assert case["first_instance"]["judge"] == "Судья"
    assert case["appeal"]["court_domain"] == court.domain
    assert case["appeal"]["result"] == row["Результат"]


def test_long_armizon_court_orphan_does_not_merge_into_abat(tyumen):
    orphan = appeal_row_to_json_case(
        _appeal_row("Армизонский районный суд Тюменской области"),
        court=tyumen.appeal_courts[0],
    )
    # Историческая запись до исправления: длинное имя без домена.
    orphan["id"] = "2-64/2026"
    orphan["first_instance"].update(case_number="2-64/2026", court_domain="")
    orphan["first_instance"].pop("srv_num", None)
    host = {
        "id": "2-64/2026", "current_stage": "first_instance",
        "first_instance": {
            "court": "Абатский районный суд", "court_domain": "abatsky--tum.sudrf.ru",
            "events": [{"date": "21.09.2026", "text": "Иск удовлетворён"}],
        },
    }
    cases = [host, orphan]
    original = deepcopy(cases)
    assert dedupe_orphan_by_base_number(cases) == 0
    assert cases == original


def test_long_name_orphan_merges_into_confirmed_own_court(tyumen):
    orphan = {
        "id": "2-64/2026", "current_stage": "appeal",
        "first_instance": {"court": "Армизонский районный суд Тюменской области"},
        "appeal": {"case_number": "33-321/2026", "events": [{"text": "Решение отменено"}]},
    }
    host = {
        "id": "2-64/2026 (2-11/2025;)", "current_stage": "awaiting_appeal",
        "first_instance": {
            "court_domain": "armizonsky.tum.sudrf.ru",
            "events": [{"text": "Иск удовлетворён"}],
        },
        "history": [{"round": 1}],
    }
    saved_fi, saved_history = deepcopy(host["first_instance"]), deepcopy(host["history"])
    cases = [host, orphan]
    assert dedupe_orphan_by_base_number(cases) == 1
    assert cases == [host]
    assert host["first_instance"] == saved_fi and host["history"] == saved_history
    assert host["appeal"] == orphan["appeal"]


@pytest.mark.parametrize("court_name", ["", "Неизвестный районный суд", "Абатский районный суд Омской области"])
def test_unknown_or_foreign_court_name_is_not_assigned_local_domain(tyumen, court_name):
    row = _appeal_row(court_name)
    case = appeal_row_to_json_case(row, court=tyumen.appeal_courts[0])
    assert case["first_instance"]["court"] == court_name
    assert case["first_instance"]["court_domain"] == ""
    assert "srv_num" not in case["first_instance"]


def test_regional_court_as_fi_does_not_acquire_district_section(tyumen):
    case = appeal_row_to_json_case(_appeal_row("Тюменский областной суд"), court=tyumen.appeal_courts[0])
    fi = case["first_instance"]
    assert fi["court"] == "Тюменский областной суд"
    assert fi["court_domain"] == "oblsud--tum.sudrf.ru"
    assert "delo_id" not in fi


@pytest.mark.parametrize("court_name,srv_num", [
    ("Камышловский районный суд", None),
    ("Камышловский районный суд (п.п. Пышма)", 2),
    ("Железнодорожный районный суд г. Екатеринбурга", 2),
])
def test_appeal_intake_only_stores_confirmed_server(monkeypatch, court_name, srv_num):
    monkeypatch.setattr(config, "REGION", "sverdlovsk_yanao")
    case = appeal_row_to_json_case(_appeal_row(court_name), court=get_region().appeal_courts[0])
    fi = case["first_instance"]
    assert fi["court"] == court_name
    assert fi.get("srv_num") == srv_num
    if srv_num is None:
        assert fi["court_domain"] == ""
    else:
        assert fi["court_domain"].endswith("--svd.sudrf.ru")


def test_appeal_card_uid_survives_enrichment_and_intake(tyumen):
    row = _appeal_row("Абатский районный суд Тюменской области")
    card = {"УИД": "72RS0001-01-2026-000064-11", "Номер дела 1 инстанции": "2-64/2026"}
    assert enrich_appeal_row_from_card(row, card) == "2-64/2026"
    case = appeal_row_to_json_case(row, court=tyumen.appeal_courts[0])
    assert case["first_instance"]["judicial_uid"] == card["УИД"]
