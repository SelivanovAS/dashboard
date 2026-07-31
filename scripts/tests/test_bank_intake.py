# -*- coding: utf-8 -*-
"""Общие правила приёма в трек «Иски банка» (court_monitor/bank_intake.py).

Модуль — единственный источник правды для трёх каналов ввода (реестр, разовый
сборщик выдачи, авто-подхват прогона). Тут проверяем сами правила и сборку
записи; e2e каналов — в их собственных файлах.
"""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import bank_intake  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402


def _court(domain: str = "surggor--hmao.sudrf.ru", srv: int = 1):
    return next(c for c in get_region("hmao").first_instance_courts
                if c.domain == domain and c.srv_num == srv)


# ── Ре-экспорты: каналы обязаны звать один и тот же код ──────────────────────

class TestReExports:
    def test_collector_reuses_shared_rules(self):
        import collect_bank_claims as cbc

        assert cbc.row_passes is bank_intake.row_passes
        assert cbc.card_rejects is bank_intake.card_rejects
        assert cbc._EXCLUDED_RESULT_RX is bank_intake._EXCLUDED_RESULT_RX

    def test_registry_reuses_shared_entry_builder(self):
        import import_bank_registry as ibr

        assert ibr.make_bank_entry is bank_intake.make_bank_entry


# ── card_rejects ─────────────────────────────────────────────────────────────

class TestCardRejects:
    @staticmethod
    def _decided(**extra):
        card = {
            "Статус": "Решено",
            "Дата заседания": "12.02.2026",
            "_events": [{"date": "12.02.2026",
                         "text": "Вынесено решение по делу. Иск удовлетворён"}],
        }
        card.update(extra)
        return card

    def test_clean_card_passes(self):
        assert bank_intake.card_rejects(self._decided()) == ""

    def test_excluded_result_from_card(self):
        """Выдача отстаёт от карточки — итог виден только в ней."""
        card = self._decided(**{"Результат": "Дело передано ПО ПОДСУДНОСТИ"})
        assert bank_intake.card_rejects(card) == "excluded_result"

    def test_refusal_is_not_excluded(self):
        """«Отказано» берём — по нему возможна апелляция банка."""
        card = self._decided(**{"Результат": "ОТКАЗАНО в удовлетворении иска"})
        assert bank_intake.card_rejects(card) == ""

    @pytest.mark.parametrize("flag", [
        "_fi_appeal_filed", "_fi_sent_to_appeal",
        "_fi_cassation_filed", "_fi_sent_to_cassation",
    ])
    def test_appeal_flags_reject_when_asked(self, flag):
        assert bank_intake.card_rejects(
            self._decided(**{flag: True}), skip_appeal=True) == "excluded_appeal"

    @pytest.mark.parametrize("flag", [
        "_fi_appeal_filed", "_fi_sent_to_appeal",
        "_fi_cassation_filed", "_fi_sent_to_cassation",
    ])
    def test_appeal_flags_pass_for_auto_intake(self, flag):
        """Решение юриста 31.07.2026: авто-подхват такие дела БЕРЁТ — они
        переезжают в основную картотеку и встают на мониторинг апелляции."""
        assert bank_intake.card_rejects(
            self._decided(**{flag: True}), skip_appeal=False) == ""

    @pytest.mark.parametrize("status", ["Выдан", "Отозван", "Возвращен"])
    def test_enforcement_writ_rejects_any_status(self, status):
        card = self._decided(_writs=[{"issue_date": "20.04.2026", "status": status}])
        assert bank_intake.card_rejects(card) == "excluded_writ"

    def test_interim_writ_passes(self):
        """Обеспечительный лист (выдан ДО решения) — дело ещё ждёт ИЛ."""
        card = self._decided(_writs=[{"issue_date": "01.11.2025", "status": "Выдан"}])
        assert bank_intake.card_rejects(card) == ""

    def test_writ_without_anchor_passes(self):
        """Ни решения, ни терминального статуса → interim, не пропуск."""
        card = {"_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]}
        assert bank_intake.card_rejects(card) == ""

    def test_decided_card_without_decision_event_uses_hearing_anchor(self):
        card = {"Статус": "Решено", "Дата заседания": "12.02.2026",
                "_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]}
        assert bank_intake.card_rejects(card) == "excluded_writ"

    def test_result_checked_before_appeal_and_writ(self):
        """Порядок причин стабилен: итог важнее прочего (отчёты каналов
        считают по одной причине на дело)."""
        card = self._decided(
            **{"Результат": "Производство по делу ПРЕКРАЩЕНО",
               "_fi_appeal_filed": True})
        assert bank_intake.card_rejects(card) == "excluded_result"


# ── delo_id / srv_num в записи ───────────────────────────────────────────────

class TestCourtIds:
    """Ссылку «в суд» фронт собирает из delo_id/srv_num (фолбэк 1540005/1);
    у записей ручных каналов их не было вовсе."""

    @staticmethod
    def _row(**extra):
        row = {
            "case_number": "2-100/2026", "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.", "category": "Кредит", "court": "Суд",
            "court_domain": "surggor--hmao.sudrf.ru", "judge": "Судья",
            "filing_date": "01.02.2026", "status": "В производстве",
            "result": "", "link": "1|a-1", "bank_role": "Истец",
        }
        row.update(extra)
        return row

    def test_ids_from_search_row(self):
        entry = bank_intake.make_bank_entry(
            self._row(court_delo_id=1540005, court_srv_num=1), {}, "тест", "now")
        assert entry["first_instance"]["delo_id"] == 1540005
        assert entry["first_instance"]["srv_num"] == 1

    def test_href_srv_wins_over_config(self):
        """Двухсерверные домены: href строки авторитетнее конфига."""
        entry = bank_intake.make_bank_entry(
            self._row(court_delo_id=1540005, court_srv_num=1, href_srv_num=2),
            {}, "тест", "now")
        assert entry["first_instance"]["srv_num"] == 2

    def test_ids_from_court_when_row_has_none(self):
        """Целевой поиск по номеру (parse_search_row) этих ключей не отдаёт."""
        court = _court()
        entry = bank_intake.make_bank_entry(
            self._row(), {}, "тест", "now", court=court)
        assert entry["first_instance"]["delo_id"] == court.delo_id
        assert entry["first_instance"]["srv_num"] == court.srv_num

    def test_no_ids_no_court_leaves_keys_absent(self):
        fi = bank_intake.make_bank_entry(self._row(), {}, "тест", "now")["first_instance"]
        assert "srv_num" not in fi

    def test_track_markers_intact(self):
        entry = bank_intake.make_bank_entry(self._row(), {}, "оператор", "now")
        assert entry["track"] == "plaintiff_light"
        assert entry["import"]["announced"] is True
        assert entry["initial_bank_role"] == "Истец"
