# -*- coding: utf-8 -*-
"""Мульти-апелляция (этап 0.5 тиражирования): у региона может быть НЕСКОЛЬКО
апелляционных судов (Свердловская обл. + ЯНАО = облсуд + Суд ЯНАО).

Покрывает:
- appeal_court_by_domain — резолв суда по appeal.court_domain + фолбэки
- link_cases — составной ключ (домен, номер): одинаковые 33-номера в двух
  апел-судах не коллидируют
- migrate_appeal_court_fields — идемпотентный бэкфилл суда в блоках appeal
- _apel_csv_row_to_json_case — суд из сервисного ключа _appeal_domain
- _appeal_health_key — исторический ключ при одном суде, доменный при двух
- рендер дайджеста — имя апел-суда в строке дела при details["appeal_court"]

Тесты гоняются в дефолтном hmao-контексте; второй суд подкладывается
monkeypatch'ем В МОДУЛЬ-ДОМ (courts/runs) — см. правило config.X в CLAUDE.md.
"""

from __future__ import annotations

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import update_cases as uc  # noqa: E402
from court_monitor import courts as cm_courts  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.lifecycle import migrate_appeal_court_fields  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402

AP1 = cm_courts.APPEAL_COURTS[0]  # Суд ХМАО-Югры (активный регион тестов)
AP2 = CourtConfig("Суд ЯНАО (тест)", "oblsud--ynao-test.sudrf.ru", 5, "appeal")


def _fi_case(num: str) -> dict:
    return {
        "id": num,
        "current_stage": "awaiting_appeal",
        "plaintiff": "Иванов И.И.",
        "defendant": "ПАО Сбербанк",
        "first_instance": {
            "case_number": num,
            "court": "Сургутский городской суд",
            "court_domain": "surggor--hmao.sudrf.ru",
            "status": "Рассмотрено",
            "events": [{"date": "01.03.2026", "text": "Решение вынесено"}],
            "appeal_filed_date": "10.03.2026",
        },
        "appeal": None,
    }


def _orphan_appeal(ap_num: str, court: CourtConfig) -> dict:
    return {
        "id": ap_num,
        "current_stage": "appeal",
        "plaintiff": "",
        "defendant": "",
        "first_instance": None,
        "appeal": {
            "case_number": ap_num,
            "court": court.name,
            "court_domain": court.domain,
            "delo_id": court.delo_id,
            "status": "В производстве",
            "events": [],
            "act_published": False,
        },
    }


class TestAppealCourtByDomain:
    def test_known_domain(self, monkeypatch):
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP1, AP2))
        assert cm_courts.appeal_court_by_domain(AP2.domain) is AP2
        assert cm_courts.appeal_court_by_domain(AP1.domain) is AP1

    def test_empty_and_unknown_fall_back_to_first(self, monkeypatch):
        """Записи эпохи одной апелляции (без court_domain) и неизвестные
        домены → первый суд региона, а не падение."""
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP1, AP2))
        assert cm_courts.appeal_court_by_domain("") is AP1
        assert cm_courts.appeal_court_by_domain(None) is AP1
        assert cm_courts.appeal_court_by_domain("nope.sudrf.ru") is AP1


class TestLinkCasesTwoCourts:
    def test_same_appeal_number_in_two_courts_links_separately(self):
        """КЛЮЧЕВОЙ кейс мульти-апелляции: один и тот же номер 33-500/2026
        в двух апел-судах — каждый мержится в СВОЁ дело 1-й инстанции."""
        orphan1 = _orphan_appeal("33-500/2026", AP1)
        orphan2 = _orphan_appeal("33-500/2026", AP2)
        fi1 = _fi_case("2-111/2026")
        fi2 = _fi_case("2-222/2026")
        cases = [orphan1, orphan2, fi1, fi2]
        out = uc.link_cases(cases, {
            (AP1.domain, "33-500/2026"): "2-111/2026",
            (AP2.domain, "33-500/2026"): "2-222/2026",
        })
        assert len(out) == 2
        by_id = {c["id"]: c for c in out}
        assert by_id["2-111/2026"]["appeal"]["court_domain"] == AP1.domain
        assert by_id["2-222/2026"]["appeal"]["court_domain"] == AP2.domain
        assert by_id["2-111/2026"]["current_stage"] == "appeal"
        assert by_id["2-222/2026"]["current_stage"] == "appeal"

    def test_legacy_appeal_block_without_domain_still_links(self):
        """Блок appeal без court_domain (данные до миграции) находится
        fallback-поиском по пустому домену."""
        orphan = _orphan_appeal("33-777/2026", AP1)
        del orphan["appeal"]["court_domain"]
        fi = _fi_case("2-333/2026")
        out = uc.link_cases(
            [orphan, fi], {(AP1.domain, "33-777/2026"): "2-333/2026"}
        )
        assert len(out) == 1
        assert out[0]["id"] == "2-333/2026"
        assert out[0]["appeal"]["case_number"] == "33-777/2026"


class TestMigrateAppealCourtFields:
    def test_backfills_missing_fields(self):
        case = _fi_case("2-1/2026")
        case["appeal"] = {"case_number": "33-1/2026", "events": []}
        n = migrate_appeal_court_fields([case], AP1)
        assert n == 1
        ap = case["appeal"]
        assert ap["court_domain"] == AP1.domain
        assert ap["court"] == AP1.name
        assert ap["delo_id"] == AP1.delo_id

    def test_idempotent_and_preserves_existing(self):
        case = _fi_case("2-2/2026")
        case["appeal"] = {
            "case_number": "33-2/2026",
            "court": AP2.name,
            "court_domain": AP2.domain,
            "delo_id": 5,
        }
        assert migrate_appeal_court_fields([case], AP1) == 0
        assert case["appeal"]["court_domain"] == AP2.domain  # не затёрто

    def test_skips_cases_without_appeal(self):
        case = _fi_case("2-3/2026")  # appeal: None
        assert migrate_appeal_court_fields([case], AP1) == 0


class TestApelCsvRowToJsonCase:
    def _row(self, **over):
        row = {
            "Номер дела": "33-7/2026",
            "Истец": "Петров П.П.",
            "Ответчик": "ПАО Сбербанк",
            "Ссылка": "700800|eeee-ffff",
            "Статус": "В производстве",
        }
        row.update(over)
        return row

    def test_second_court_from_service_key(self, monkeypatch):
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP1, AP2))
        entry = cm_runs._apel_csv_row_to_json_case(
            self._row(_appeal_domain=AP2.domain),
            {(AP2.domain, "33-7/2026"): "2-9/2026"},
        )
        ap = entry["appeal"]
        assert ap["court"] == AP2.name
        assert ap["court_domain"] == AP2.domain
        assert ap["delo_id"] == AP2.delo_id
        assert entry["first_instance"]["case_number"] == "2-9/2026"

    def test_without_service_key_uses_first_court(self):
        entry = cm_runs._apel_csv_row_to_json_case(self._row())
        ap = entry["appeal"]
        assert ap["court"] == AP1.name
        assert ap["court_domain"] == AP1.domain

    def test_lookup_key_is_composite(self, monkeypatch):
        """Номер 1-й инст. другого суда с тем же 33-номером не подтягивается."""
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP1, AP2))
        entry = cm_runs._apel_csv_row_to_json_case(
            self._row(_appeal_domain=AP2.domain),
            {(AP1.domain, "33-7/2026"): "2-9/2026"},  # чужой суд
        )
        assert entry["first_instance"]["case_number"] == ""


class TestAppealHealthKey:
    def test_single_court_keeps_historic_key(self, monkeypatch):
        monkeypatch.setattr(cm_runs, "APPEAL_COURTS", (AP1,))
        assert cm_runs._appeal_health_key(AP1) == "appeal:oblsud"

    def test_multi_court_uses_domain(self, monkeypatch):
        monkeypatch.setattr(cm_runs, "APPEAL_COURTS", (AP1, AP2))
        assert cm_runs._appeal_health_key(AP1) == f"appeal:{AP1.domain}"
        assert cm_runs._appeal_health_key(AP2) == f"appeal:{AP2.domain}"


class TestTemplateShowsAppealCourt:
    def _render(self, details_extra: dict) -> str:
        details = {
            "plaintiff": "Иванов Иван Иванович",
            "defendant": "ПАО Сбербанк",
            "category": "",
            "case_url": "",
            "old_status": "В производстве",
            "new_status": "Приостановлено",
        }
        details.update(details_extra)
        return uc.generate_template_digest(
            new_cases=[],
            changes=[{"case": "33-42/2026", "type": ["status_change"],
                      "details": details}],
            fi_new_cases=[], fi_changes=[], cass_changes=[],
            cass_discovered=[],
            total_active_appeal=1, total_active_fi=1, total_active_cassation=1,
        )

    def test_appeal_court_rendered_when_present(self):
        html = self._render({"appeal_court": "Суд ЯНАО"})
        assert "Суд ЯНАО" in html

    def test_no_court_note_without_key(self):
        """ХМАО-путь: ключа appeal_court нет — рендер без имени суда."""
        html = self._render({})
        assert "Суд ЯНАО" not in html
