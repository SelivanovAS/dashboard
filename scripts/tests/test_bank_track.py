# -*- coding: utf-8 -*-
"""Лёгкий трек «Иски банка» (банк — истец): предикаты lifecycle, недельный
опрос should_skip_case, архивные окна и парсер вкладки «ИСПОЛНИТЕЛЬНЫЕ
ЛИСТЫ» (структура подтверждена пробой 25.07.2026 — ops/writ_probe/report.txt).
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, lifecycle  # noqa: E402
from court_monitor.parsing.cards import parse_case_card  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _track_case(**fi) -> dict:
    base_fi = {"case_number": "2-100/2026", "court_domain": "surggor--hmao.sudrf.ru"}
    base_fi.update(fi)
    return {
        "id": "2-100/2026",
        "current_stage": "first_instance",
        "bank_role": "Истец",
        "track": "plaintiff_light",
        "first_instance": base_fi,
    }


# ── Парсер вкладки «ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ» ────────────────────────────────────

class TestWritParsing:
    def test_writs_extracted(self):
        info = parse_case_card(_fixture("case_card_fi_writs.html"))
        assert info["_writs"] == [
            {"issue_date": "24.06.2026", "blank_number": "",
             "electronic_id": "86RS0017#2-37/2026#1", "status": "Возвращен",
             "recipient": "Отделение судебных приставов по Советскому району"},
            {"issue_date": "26.06.2026", "blank_number": "",
             "electronic_id": "86RS0017#2-37/2026#2", "status": "Выдан",
             "recipient": "Отделение судебных приставов по Советскому району"},
            {"issue_date": "21.11.2023", "blank_number": "ФС № 039166358",
             "electronic_id": "", "status": "Отозван", "recipient": "Взыскатель"},
        ]

    def test_card_without_writ_tab_gives_empty_list(self):
        """Вкладки нет, пока листов нет — это не ошибка, а пустой список."""
        info = parse_case_card(_fixture("case_card_first_instance.html"))
        assert info["_writs"] == []

    def test_movement_parsing_untouched(self):
        """Регресс-чек: добавление _writs не ломает разбор движения дела."""
        info = parse_case_card(_fixture("case_card_fi_writs.html"))
        assert info["Статус"] == "Решено"
        assert info["_events"]


# ── should_skip_case: недельный опрос решённых исков банка ───────────────────

class TestWritWeeklySkip:
    TODAY = date(2026, 7, 20)

    def _resolved(self, checked_days_ago: int | None) -> dict:
        fi: dict = {"status": "Решено"}
        if checked_days_ago is not None:
            fi["last_checked_at"] = (
                self.TODAY - timedelta(days=checked_days_ago)
            ).isoformat()
        return _track_case(**fi)

    def test_checked_recently_skipped(self):
        skip, reason = lifecycle.should_skip_case(self._resolved(3), self.TODAY)
        assert skip is True
        assert reason == "writ_weekly(3d/7d)"

    def test_week_passed_parsed(self):
        assert lifecycle.should_skip_case(self._resolved(8), self.TODAY) == (False, "")

    def test_never_checked_parsed(self):
        assert lifecycle.should_skip_case(self._resolved(None), self.TODAY) == (False, "")

    def test_non_track_resolved_untouched(self):
        """Обычное решённое дело (не трек) недельный ритм не получает."""
        case = self._resolved(3)
        case.pop("track")
        assert lifecycle.should_skip_case(case, self.TODAY) == (False, "")

    def test_active_track_case_standard_smart_skip(self):
        """До решения track-дело живёт обычным smart-skip по заседаниям."""
        future = (self.TODAY + timedelta(days=5)).strftime("%d.%m.%Y")
        case = _track_case(
            status="В производстве",
            last_checked_at=(self.TODAY - timedelta(days=1)).isoformat(),
            events=[{"date": future, "time": "10:00",
                     "text": f"Судебное заседание. 10:00. {future}"}],
        )
        skip, reason = lifecycle.should_skip_case(case, self.TODAY)
        assert skip is True
        assert reason.startswith("future_hearing")

    def test_smart_skip_switch_respected(self, monkeypatch):
        monkeypatch.setattr(config, "SMART_SKIP_CASES", False)
        assert lifecycle.should_skip_case(self._resolved(3), self.TODAY) == (False, "")

    def test_reason_translated(self):
        assert "раз в 7 дн" in lifecycle.skip_reason_ru("writ_weekly(3d/7d)")


# ── bank_case_left_track: переезд в основной трек ────────────────────────────

class TestLeftTrack:
    def test_plain_track_case_stays(self):
        assert lifecycle.bank_case_left_track(_track_case()) is False

    def test_appeal_filed_leaves(self):
        assert lifecycle.bank_case_left_track(
            _track_case(appeal_filed=True)) is True

    def test_appeal_filed_date_leaves(self):
        assert lifecycle.bank_case_left_track(
            _track_case(appeal_filed_date="01.07.2026")) is True

    def test_stage_advanced_leaves(self):
        case = _track_case()
        case["current_stage"] = "awaiting_appeal"
        assert lifecycle.bank_case_left_track(case) is True

    def test_non_track_never_leaves(self):
        case = _track_case(appeal_filed=True)
        case.pop("track")
        assert lifecycle.bank_case_left_track(case) is False


# ── bank_legal_force_est ─────────────────────────────────────────────────────

class TestLegalForceEst:
    def test_from_act_date_plus_30(self):
        est = lifecycle.bank_legal_force_est({"act_date": "02.02.2026"})
        assert est == date(2026, 3, 4)

    def test_fallback_to_hearing_date(self):
        est = lifecycle.bank_legal_force_est({"hearing_date": "02.02.2026"})
        assert est == date(2026, 3, 4)

    def test_weekend_shifted_forward(self):
        """+30 дн попало на выходной → сдвиг на ближайший рабочий."""
        est = lifecycle.bank_legal_force_est({"act_date": "05.03.2026"})
        assert est is not None
        from court_monitor.textutil import is_russian_working_day
        assert is_russian_working_day(est)
        assert est >= date(2026, 4, 4)

    def test_no_dates_none(self):
        assert lifecycle.bank_legal_force_est({}) is None


# ── is_case_archived: архивные окна трека ────────────────────────────────────

class TestBankTrackArchive:
    def _archived(self, case: dict) -> bool:
        return lifecycle.is_case_archived(case)

    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def test_writ_issued_long_ago_archived(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(20), "status": "Выдан"}],
        )
        assert self._archived(case) is True

    def test_writ_issued_recently_kept(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(5), "status": "Выдан"}],
        )
        assert self._archived(case) is False

    def test_resolved_without_writ_kept_beyond_60_days(self):
        """Ключевое отличие от основного трека: 60-дневное окно не действует,
        дело ждёт ИЛ (кейс, ради которого трек и заведён)."""
        case = _track_case(status="Решено", hearing_date=self._dmy(90))
        assert self._archived(case) is False

    def test_resolved_without_writ_ceiling(self):
        """Потолок: 180 дн от расчётного вступления в силу без ИЛ → архив."""
        case = _track_case(status="Решено", act_date=self._dmy(30 + 180 + 40))
        assert self._archived(case) is True

    def test_returned_after_window_archived(self):
        case = _track_case(status="Возвращено", event_date=self._dmy(40))
        assert self._archived(case) is True

    def test_returned_recently_kept(self):
        case = _track_case(status="Возвращено", event_date=self._dmy(10))
        assert self._archived(case) is False

    def test_appeal_flag_keeps_active(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(400), appeal_filed=True)
        assert self._archived(case) is False

    def test_active_case_kept(self):
        case = _track_case(status="В производстве")
        assert self._archived(case) is False

    def test_non_track_60_day_window_untouched(self):
        """Регресс-чек: обычное дело архивируется по старому окну."""
        case = _track_case(status="Решено", hearing_date=self._dmy(70))
        case.pop("track")
        assert self._archived(case) is True


# ── split_bank_track: раскладка перед сохранением (фаза 7c main_json) ────────

class TestSplitBankTrack:
    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def test_split_routes_all_kinds(self):
        from court_monitor.runs import split_bank_track
        ordinary = {"id": "2-1/2026", "current_stage": "first_instance",
                    "first_instance": {"case_number": "2-1/2026"}}
        active = _track_case(status="В производстве")
        archived = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(30), "status": "Выдан"}])
        left = _track_case(appeal_filed=True, appeal_filed_date="01.07.2026")
        rest, bank_active, bank_arch, moved = split_bank_track(
            [ordinary, active, archived, left])
        assert bank_active == [active]
        assert bank_arch == [archived]
        assert archived.get("archived_at")
        assert moved == 1
        # «Переехавшее» дело — в основном списке, маркер снят, след остался.
        assert left in rest and ordinary in rest
        assert "track" not in left
        assert left["track_origin"] == "plaintiff_light"

    def test_left_track_case_not_returned_to_bank_file(self):
        from court_monitor.runs import split_bank_track
        left = _track_case(appeal_filed_date="01.07.2026")
        rest, bank_active, bank_arch, moved = split_bank_track([left])
        assert (bank_active, bank_arch, moved) == ([], [], 1)
        assert rest == [left]
