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


# ── Исчисление процессуальных сроков (гл. 9 ГПК, textutil) ──────────────────

class TestCourtCalendar:
    def test_add_working_days_starts_next_day(self):
        """Ст. 107 ГПК: течение срока начинается на следующий день."""
        from court_monitor.textutil import add_working_days
        assert add_working_days(date(2026, 7, 1), 1) == date(2026, 7, 2)

    def test_add_working_days_skips_holidays(self):
        """Майские 2026: 09.05 (сб, праздник), 10.05 (вс), 11.05 (перенос)."""
        from court_monitor.textutil import add_working_days
        assert add_working_days(date(2026, 5, 8), 3) == date(2026, 5, 14)

    def test_month_term_plenum_example(self):
        """Пример п. 16 ПП ВС №16: мотивировка 31.07 → последний день 31.08."""
        from court_monitor.textutil import month_term_last_day
        assert month_term_last_day(date(2026, 7, 31)) == date(2026, 8, 31)

    def test_month_term_no_such_day_and_weekend(self):
        """31.01 → в феврале нет 31-го → 28.02 (сб) → перенос на 02.03 (пн)."""
        from court_monitor.textutil import month_term_last_day
        assert month_term_last_day(date(2026, 1, 31)) == date(2026, 3, 2)

    def test_month_term_new_year_holidays(self):
        """Конец срока в новогодние каникулы → первый рабочий день января."""
        from court_monitor.textutil import month_term_last_day
        assert month_term_last_day(date(2026, 12, 1)) == date(2027, 1, 11)

    def test_next_working_day(self):
        from court_monitor.textutil import next_working_day
        assert next_working_day(date(2026, 7, 25)) == date(2026, 7, 27)  # сб → пн
        assert next_working_day(date(2026, 7, 27)) == date(2026, 7, 27)  # пн — сам


# ── bank_legal_force_est ─────────────────────────────────────────────────────

class TestLegalForceEst:
    """Вступление в силу по ГПК: сроки в днях — рабочие (ст. 107), месяц —
    календарный (ст. 108), результат — ПЕРВЫЙ день в силе (последний день
    срока обжалования + 1 календарный, без сдвига на рабочий)."""

    def test_ordinary_from_act_date(self):
        """Обычное: act_date 02.02 → месяц → 02.03 (пн) → в силе 03.03."""
        est = lifecycle.bank_legal_force_est({"act_date": "02.02.2026"})
        assert est == date(2026, 3, 3)

    def test_ordinary_from_motivirovka_event(self):
        """Дата мотивировки из события карточки: act_date у всех решённых дел
        пилота пуст, событие «Изготовлено мотивированное решение» — у 25 из 39.
        Контрольный кейс 2-6140/2026."""
        fi = {"decision_date": "24.06.2026",
              "events": [
                  {"date": "24.06.2026", "text": "Вынесено решение по делу"},
                  {"date": "24.06.2026",
                   "text": "Изготовлено мотивированное решение в окончательной форме"}]}
        # 24.06 + месяц = 24.07 (пт) → в силе 25.07 — суббота, и это НЕ
        # сдвигается: в силу решение вступает и в выходной (переносится
        # только последний день срока, ч. 2 ст. 108).
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 7, 25)

    def test_ordinary_fallback_ten_workdays(self):
        """Мотивировки нигде нет → decision_date + 10 раб. дн (ст. 199)."""
        fi = {"decision_date": "24.06.2026",
              "events": [{"date": "24.06.2026", "text": "Вынесено решение по делу"}]}
        # 24.06 + 10 раб. дн = 08.07 → месяц → 08.08 (сб) → перенос 10.08 (пн)
        # → в силе 11.08
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 8, 11)

    def test_fallback_to_hearing_date(self):
        """Архивные записи без decision_date: якорь — hearing_date."""
        est = lifecycle.bank_legal_force_est({"hearing_date": "02.02.2026"})
        # 02.02 + 10 раб. дн = 16.02 → месяц → 16.03 (пн) → в силе 17.03
        assert est == date(2026, 3, 17)

    def test_default_judgment_copy_served(self):
        """Заочное, копия вручена: вручение + 7 раб. дн + месяц (ст. 237).
        Контрольный кейс 2-5671/2026."""
        fi = {"decision_date": "28.05.2026",
              "events": [
                  {"date": "28.05.2026", "text": "Вынесено заочное решение по делу"},
                  {"date": "26.06.2026",
                   "text": "Копия заочного решения ответчику (истцу) вручена"}]}
        # 26.06 + 7 раб. дн = 07.07 → месяц → 07.08 (пт) → в силе 08.08
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 8, 8)

    def test_default_judgment_vs_formula(self):
        """Заочное без сведений о вручении → формула ВС (Обзор №2 (2015),
        в. 14): решение + 3 раб. дн + 7 раб. дн + месяц."""
        fi = {"decision_date": "28.05.2026",
              "events": [{"date": "28.05.2026",
                          "text": "Вынесено заочное решение по делу"}]}
        # 28.05 + 3 раб = 02.06 → + 7 раб = 11.06 → месяц → 11.07 (сб) →
        # перенос 13.07 (пн) → в силе 14.07
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 7, 14)

    def test_default_judgment_returned_copy_vs_formula(self):
        """«Возвратилась невручённой» — сведений о вручении нет, формула ВС."""
        fi = {"decision_date": "28.05.2026",
              "events": [
                  {"date": "28.05.2026", "text": "Вынесено заочное решение по делу"},
                  {"date": "15.06.2026",
                   "text": "Копия заочного решения возвратилась невручённой"}]}
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 7, 14)

    def test_default_judgment_late_motivirovka_anchor(self):
        """Якорь заочного — мотивировка, если она ПОЗЖЕ даты решения
        (копию суд физически высылает после изготовления полного текста);
        добавка +10 раб. дн к заочному не применяется."""
        fi = {"decision_date": "28.05.2026",
              "events": [
                  {"date": "28.05.2026", "text": "Вынесено заочное решение по делу"},
                  {"date": "05.06.2026",
                   "text": "Изготовлено мотивированное решение в окончательной форме"}]}
        # 05.06 + 3 раб = 10.06 → + 7 раб (12.06 — праздник) = 22.06 →
        # месяц → 22.07 (ср) → в силе 23.07
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 7, 23)

    def test_no_dates_none(self):
        assert lifecycle.bank_legal_force_est({}) is None


# ── fi_decision_date_from_events: якорь типа листа до заморозки decision_date ─

class TestDecisionDateFromEvents:
    def test_takes_decision_event_date(self):
        assert lifecycle.fi_decision_date_from_events([
            {"date": "10.02.2026", "text": "Судебное заседание. 10:30"},
            {"date": "15.04.2026", "text": "Вынесено решение по делу. Иск УДОВЛЕТВОРЕН"},
        ]) == "15.04.2026"

    def test_default_judgment_counts(self):
        assert lifecycle.fi_decision_date_from_events([
            {"date": "01.03.2026", "text": "Вынесено заочное решение по делу"},
        ]) == "01.03.2026"

    def test_last_decision_wins(self):
        """Отмена заочного (ст. 241 ГПК) → новый круг: якорь — решение
        текущего круга, а не первого."""
        assert lifecycle.fi_decision_date_from_events([
            {"date": "01.03.2026", "text": "Вынесено заочное решение по делу"},
            {"date": "15.04.2026", "text": "Вынесено решение по делу. Иск УДОВЛЕТВОРЕН"},
        ]) == "15.04.2026"

    def test_no_decision_returns_empty(self):
        for events in (
            None, [],
            [{"date": "10.02.2026", "text": "Судебное заседание. Объявлен перерыв"}],
            [{"date": "", "text": "Вынесено решение по делу"}],
        ):
            assert lifecycle.fi_decision_date_from_events(events) == "", events


# ── bank_default_judgment_info: детект заочного производства ─────────────────

class TestDefaultJudgmentInfo:
    def test_detects_all_fields(self):
        fi = {"events": [
            {"date": "28.05.2026",
             "text": "Судебное заседание. 10:30. Вынесено заочное решение по делу. "
                     "Иск (заявление, жалоба) УДОВЛЕТВОРЕН"},
            {"date": "28.05.2026",
             "text": "Изготовлено мотивированное решение в окончательной форме"},
            {"date": "15.06.2026",
             "text": "Копия заочного решения возвратилась невручённой"},
            {"date": "26.06.2026",
             "text": "Копия заочного решения ответчику (истцу) вручена"},
        ]}
        assert lifecycle.bank_default_judgment_info(fi) == {
            "default_judgment": True,
            "motivirovka_date": "28.05.2026",
            "default_copy_served_date": "26.06.2026",
            "default_copy_returned": True,
        }

    def test_last_decision_wins(self):
        """Отмена заочного (ст. 241) → новое рассмотрение → обычное решение:
        флаг заочности снимается, хотя событие первого круга в истории."""
        fi = {"events": [
            {"date": "01.03.2026", "text": "Вынесено заочное решение по делу"},
            {"date": "20.03.2026", "text": "Рассмотрение дела начато с начала"},
            {"date": "15.04.2026", "text": "Вынесено решение по делу. Иск УДОВЛЕТВОРЕН"},
        ]}
        assert lifecycle.bank_default_judgment_info(fi)["default_judgment"] is False

    def test_ordinary_decision_empty_info(self):
        fi = {"events": [{"date": "24.06.2026", "text": "Вынесено решение по делу"}]}
        info = lifecycle.bank_default_judgment_info(fi)
        assert info["default_judgment"] is False
        assert info["default_copy_served_date"] == ""
        assert info["default_copy_returned"] is False

    def test_fallback_to_stamped_fields_without_events(self):
        """Лёгкая запись без склейки events (архив вне пайплайна) — детектор
        доверяет уже проштампованным полям."""
        fi = {"default_judgment": True, "default_copy_served_date": "26.06.2026"}
        info = lifecycle.bank_default_judgment_info(fi)
        assert info["default_judgment"] is True
        assert info["default_copy_served_date"] == "26.06.2026"

    def test_yo_normalization(self):
        """Суды пишут «невручённой» и «неврученной» вперемешку."""
        for word in ("возвратилась невручённой", "возвратилась неврученной"):
            fi = {"events": [
                {"date": "01.06.2026", "text": f"Копия заочного решения {word}"}]}
            assert lifecycle.bank_default_judgment_info(fi)["default_copy_returned"] is True


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

    def test_interim_only_writ_does_not_archive(self):
        """Кейс 2-6005 (вопрос юриста 26.07.2026): решено, но лист только
        обеспечительный (выдан ДО решения) — дело ждёт лист на исполнение."""
        case = _track_case(
            status="Решено", hearing_date=self._dmy(30),
            act_date=self._dmy(25),
            writs=[{"issue_date": self._dmy(60), "status": "Выдан"}],
        )
        assert self._archived(case) is False

    def test_mixed_writs_archive_by_enforcement(self):
        """Обеспечительный старый + исполнение свежее → окно по исполнению."""
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            writs=[
                {"issue_date": self._dmy(150), "status": "Выдан"},  # interim
                {"issue_date": self._dmy(5), "status": "Выдан"},    # enforcement
            ],
        )
        assert self._archived(case) is False  # исполнение выдано 5 дн назад
        case["first_instance"]["writs"][1]["issue_date"] = self._dmy(20)
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
        """Потолок: 180 дн от расчётного вступления в силу без ИЛ → архив.
        Слагаемые запаса: ~месяц на апелляцию + 180 потолок + 40 сверху."""
        case = _track_case(status="Решено", act_date=self._dmy(31 + 180 + 40))
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


# ── classify_writ_kind: исполнение решения vs обеспечительные меры ───────────

class TestClassifyWritKind:
    """Датовая эвристика (кластеры пилота 26.07.2026): лист до резолютивки —
    обеспечительный (арест), после — на исполнение решения."""

    def test_before_hearing_interim(self):
        # 2-6005/2026: подан 22.04, лист 23.04, решение 20.05.
        fi = {"hearing_date": "20.05.2026"}
        assert lifecycle.classify_writ_kind(
            {"issue_date": "23.04.2026"}, fi) == "interim"

    def test_after_hearing_enforcement(self):
        # 2-4292/2026: решение 30.04, лист 22.06 (+53 дн).
        fi = {"hearing_date": "30.04.2026"}
        assert lifecycle.classify_writ_kind(
            {"issue_date": "22.06.2026"}, fi) == "enforcement"

    def test_no_hearing_interim(self):
        """Решения нет — лист может быть только обеспечительным."""
        assert lifecycle.classify_writ_kind(
            {"issue_date": "01.07.2026"}, {}) == "interim"

    def test_no_issue_date_unknown(self):
        assert lifecycle.classify_writ_kind(
            {}, {"hearing_date": "01.07.2026"}) == "unknown"

    def test_frozen_decision_date_wins_over_drifting_hearing(self):
        """Пост-решенческое заседание НЕ переворачивает тип листа.

        hearing_date перечитывается каждым прогоном из последнего
        session-события карточки (parse_case_card → «Дата заседания», запись
        без гарда в update_active_cases). Назначь суд по решённому делу
        заседание о взыскании судебных расходов / индексации / разъяснении —
        дата уедет вперёд, и лист на исполнение оказался бы «до заседания».
        Якорь — замороженная decision_date.
        """
        лист = {"issue_date": "22.06.2026"}
        решено = {"decision_date": "30.04.2026", "hearing_date": "30.04.2026"}
        assert lifecycle.classify_writ_kind(лист, решено) == "enforcement"
        # Суд назначил заседание по судебным расходам на 15.09 — hearing_date
        # уехал, decision_date остался.
        дрейф = {"decision_date": "30.04.2026", "hearing_date": "15.09.2026"}
        assert lifecycle.classify_writ_kind(лист, дрейф) == "enforcement", (
            "Тип листа поехал за hearing_date — вместе с ним перевернутся "
            "бейдж, KPI «С ИЛ», заголовок секции и окно архива, причём "
            "дайджест об этом промолчит (гард case_decided)."
        )

    def test_hearing_date_still_fallback(self):
        """Архивные записи и воскрешённые дела без decision_date работают
        по-старому — фолбэк остаётся навсегда."""
        assert lifecycle.classify_writ_kind(
            {"issue_date": "22.06.2026"}, {"hearing_date": "30.04.2026"}
        ) == "enforcement"

    def test_legal_force_est_anchored_on_decision_date(self):
        """Часы ожидания ИЛ тоже не должны ехать за hearing_date.

        act_date у всех 43 решённых дел пилота пуст, поэтому фолбэк — не
        исключение, а единственный режим: одно поле держало и тип листа, и
        отсчёт ожидания.
        """
        решено = {"act_date": "", "decision_date": "30.04.2026",
                  "hearing_date": "30.04.2026"}
        было = lifecycle.bank_legal_force_est(решено)
        дрейф = dict(решено, hearing_date="15.09.2026")
        assert lifecycle.bank_legal_force_est(дрейф) == было

    def test_writ_archive_window_survives_drift(self):
        """Дело с выданным ИЛ уходит в архив по своему окну и после дрейфа.

        Иначе оно вываливается на потолок BANK_WRIT_WAIT_MAX_DAYS (180 дн) и
        продолжает опрашиваться еженедельно ещё до полугода.
        """
        дата = (datetime.now() - timedelta(days=30)).strftime("%d.%m.%Y")
        решение = (datetime.now() - timedelta(days=90)).strftime("%d.%m.%Y")
        будущее = (datetime.now() + timedelta(days=40)).strftime("%d.%m.%Y")
        case = _track_case(status="Решено", decision_date=решение,
                           hearing_date=будущее,
                           writs=[{"issue_date": дата, "status": "Выдан"}])
        assert lifecycle.is_case_archived(case) is True


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

    def test_legal_force_est_stamped(self):
        """Расчётная дата вступления в силу попадает В ЗАПИСЬ.

        Она и раньше считалась (ритм опроса, потолок архива), но жила только
        в памяти прогона — фронт её не воспроизведёт (производственного
        календаря в JS нет), а без неё «Ждут ИЛ» остаётся счётчиком без
        срока ожидания.
        """
        from court_monitor.runs import split_bank_track
        from court_monitor.lifecycle import bank_legal_force_est
        решено = _track_case(status="Решено", hearing_date=self._dmy(90))
        _rest, bank_active, _arch, _moved = split_bank_track([решено])
        est = bank_legal_force_est(решено["first_instance"])
        assert est is not None
        assert bank_active[0]["first_instance"]["legal_force_est"] == est.isoformat()

    def test_legal_force_est_dropped_when_no_anchor(self):
        """Решения ещё нет — ключа быть не должно (в т.ч. протухшего)."""
        from court_monitor.runs import split_bank_track
        дело = _track_case(status="В производстве")
        дело["first_instance"]["hearing_date"] = ""
        дело["first_instance"]["act_date"] = ""
        дело["first_instance"]["legal_force_est"] = "2026-01-01"
        _rest, bank_active, _arch, _moved = split_bank_track([дело])
        assert "legal_force_est" not in bank_active[0]["first_instance"]

    def test_legal_force_est_stamped_on_archived_too(self):
        """Архивные записи тоже получают поле — иначе при реактивации дела
        бейдж ожидания молча пропадёт."""
        from court_monitor.runs import split_bank_track
        arch = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(30), "status": "Выдан"}])
        _rest, _active, bank_arch, _moved = split_bank_track([arch])
        assert bank_arch and bank_arch[0]["first_instance"].get("legal_force_est")

    def test_default_judgment_fields_stamped(self):
        """Признаки заочного производства попадают в лёгкую запись: фронт
        bank-картотеки events не грузит, бейджу «Заочное» и строке о вручении
        нужны отдельные поля."""
        from court_monitor.runs import split_bank_track
        дело = _track_case(status="Решено", decision_date=self._dmy(30),
                           hearing_date=self._dmy(30))
        дело["first_instance"]["events"] = [
            {"date": self._dmy(30), "text": "Вынесено заочное решение по делу"},
            {"date": self._dmy(10),
             "text": "Копия заочного решения ответчику (истцу) вручена"},
        ]
        _rest, bank_active, _arch, _moved = split_bank_track([дело])
        fi = bank_active[0]["first_instance"]
        assert fi["default_judgment"] is True
        assert fi["default_copy_served_date"] == self._dmy(10)
        assert "default_copy_returned" not in fi  # пустые значения не пишем

    def test_stale_default_judgment_flag_removed(self):
        """Самоисцеление: заочное отменено, новое решение обычное — протухший
        флаг снимается на ближайшем прогоне."""
        from court_monitor.runs import split_bank_track
        дело = _track_case(status="Решено", decision_date=self._dmy(30),
                           hearing_date=self._dmy(30))
        дело["first_instance"]["default_judgment"] = True
        дело["first_instance"]["events"] = [
            {"date": self._dmy(30), "text": "Вынесено решение по делу"}]
        _rest, bank_active, _arch, _moved = split_bank_track([дело])
        assert "default_judgment" not in bank_active[0]["first_instance"]


# ── Дайджест: секция «Иски банка» ────────────────────────────────────────────

def _bank_change(types: list[str], details: dict | None = None) -> dict:
    d = {"link": "111|aaaa-1111", "court_domain": "surggor--hmao.sudrf.ru"}
    d.update(details or {})
    return {
        "case": "2-100/2026",
        "court": "Сургутский городской суд",
        "plaintiff": "ПАО Сбербанк",
        "defendant": "Иванов Иван Иванович",
        "bank_role": "Истец",
        "category": "",
        "type": types,
        "details": d,
        "track": "plaintiff_light",
    }


def _digest(fi_changes: list[dict]) -> str:
    from court_monitor.digest.template import generate_template_digest
    return generate_template_digest([], [], fi_changes=fi_changes)


class TestBankDigestSection:
    def test_writ_issued_rendered(self):
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [{
            "issue_date": "26.06.2026", "blank_number": "",
            "electronic_id": "86RS0004#2-100/2026#1", "status": "Выдан",
            "recipient": "Отделение судебных приставов по г. Сургуту",
        }]})])
        assert "ИСКИ БАНКА (1)" in html
        assert "выдан исполнительный лист" in html
        assert "2-100/2026" in html
        assert "26.06.2026" in html
        assert "86RS0004#2-100/2026#1" in html
        assert "по искам банка (🧾 1 ИЛ)" in html

    def test_interim_writ_rendered_with_mark(self):
        """Обеспечительный лист — с пометкой и своим счётчиком в сводке."""
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [{
            "issue_date": "23.04.2026", "electronic_id": "86RS0004#2-100/2026#1",
            "status": "Выдан", "kind": "interim",
        }]})])
        assert "выдан обеспечительный лист (арест)" in html
        assert "🛡 1 обеспечит." in html
        assert "1 ИЛ" not in html  # в счётчик «🧾 N ИЛ» interim не входит

    def test_writ_status_change_rendered(self):
        html = _digest([_bank_change(["fi_writ_status_changed"],
                                     {"writ_status_changes": [{
                                         "issue_date": "26.06.2026",
                                         "old_status": "Выдан",
                                         "status": "Отозван",
                                     }]})])
        assert "Выдан" in html and "Отозван" in html

    def test_writ_status_change_carries_number(self):
        """У дела бывает несколько листов одной даты в один ОСП — без номера
        непонятно, КАКОЙ отозван (Советский, 2-37/2026: #1 Возвращен, #2 Выдан)."""
        html = _digest([_bank_change(["fi_writ_status_changed"],
                                     {"writ_status_changes": [{
                                         "issue_date": "24.06.2026",
                                         "electronic_id": "86RS0017#2-37/2026#1",
                                         "old_status": "Выдан",
                                         "status": "Возвращен",
                                     }]})])
        assert "86RS0017#2-37/2026#1" in html, (
            "Смена статуса листа отрендерена без номера — юрист не поймёт, "
            "какой именно лист отозван."
        )

    def test_both_writ_numbers_rendered(self):
        """Электронный ИД и бумажный бланк — разные реквизиты одного листа.

        Было `electronic_id or blank_number`: заполни суд обе колонки —
        бумажный «ФС №…» молча пропал бы из Telegram.
        """
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [{
            "issue_date": "26.06.2026", "blank_number": "ФС № 039166358",
            "electronic_id": "86RS0004#2-100/2026#1", "status": "Выдан",
        }]})])
        assert "86RS0004#2-100/2026#1" in html
        assert "ФС № 039166358" in html

    def test_long_bailiff_shortened_not_cut(self):
        """Получатель сокращается осмысленно, а не режется по [:60]."""
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [{
            "issue_date": "26.06.2026", "electronic_id": "86RS0004#2-100/2026#1",
            "status": "Выдан",
            "recipient": ("Отделение судебных приставов по взысканию "
                          "задолженности с юридических лиц по г. Тюмени "
                          "и Тюменскому району"),
        }]})])
        assert "ОСП по взысканию задолж. с юрлиц по г. Тюмени и Тюменскому р-ну" in html
        assert "юридиче " not in html and "юридиче<" not in html

    def test_resolved_with_verdict(self):
        html = _digest([_bank_change(["fi_resolved"],
                                     {"verdict_label": "удовлетворено"})])
        assert "вынесено решение" in html and "удовлетворено" in html

    def test_status_change_suppressed_next_to_resolved(self):
        """«смена статуса» рядом с «вынесено решение» — эхо того же факта
        (зеркало дедупа секции 3.2; фидбэк юриста 30.07.2026)."""
        html = _digest([_bank_change(
            ["fi_resolved", "fi_status_change"],
            {"verdict_label": "удовлетворено",
             "old_status": "В производстве", "new_status": "Решено"},
        )])
        assert "вынесено решение" in html
        assert "смена статуса" not in html
        assert "статус:" not in html

    def test_status_change_suppressed_next_to_returned(self):
        """Возврат иска + смена статуса «→ Возвращено» — то же эхо."""
        html = _digest([_bank_change(
            ["fi_returned", "fi_status_change"],
            {"old_status": "В производстве", "new_status": "Возвращено"},
        )])
        assert "иск возвращён" in html
        assert "статус" not in html

    def test_lone_status_change_carries_details(self):
        """Одиночная смена статуса — с деталями «X → Y»: голая подпись
        «смена статуса» юристу ничего не говорила."""
        html = _digest([_bank_change(
            ["fi_status_change"],
            {"old_status": "В производстве",
             "new_status": "Приостановлено"},
        )])
        assert "ℹ️ статус: В производстве → Приостановлено" in html

    def test_lone_status_change_without_details_falls_back(self):
        """Старый контекст (--replay-last) без old/new_status — прежняя
        короткая форма."""
        html = _digest([_bank_change(["fi_status_change"])])
        assert "ℹ️ смена статуса" in html

    def test_fio_suffix_shortened_in_bank_line(self):
        """ФИО с «кызы»/«оглы» сокращаются и в bank-строке (до фикса
        30.07.2026 уходили в дайджест полными)."""
        ch = _bank_change(["fi_hearing_new"], {"hearing_date": "01.09.2026"})
        ch["defendant"] = ("Гаджиева Лейла Хандадаш кызы, "
                          "Меликов Аждар Гурбан оглы")
        html = _digest([ch])
        assert "Гаджиева Л.Х. кызы, Меликов А.Г. оглы" in html
        assert "Лейла" not in html

    def test_section_last_and_fi_counters_clean(self):
        """Секция банка — последней; счётчики 1-й инст. track-делами
        не раздуваются."""
        ordinary = _bank_change(["fi_hearing_new"], {"hearing_date": "01.09.2026"})
        ordinary.pop("track")
        bank = _bank_change(["fi_hearing_new"], {"hearing_date": "02.09.2026"})
        html = _digest([ordinary, bank])
        assert html.index("ПЕРВАЯ ИНСТАНЦИЯ") < html.index("ИСКИ БАНКА")
        assert html.index("ИСКИ БАНКА") < html.index("В производстве")
        assert "📅 1 заседание в 1-й инст." in html
        assert "1 событие по искам банка" in html

    def test_only_bank_changes_not_empty_digest(self):
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [
            {"issue_date": "01.07.2026", "status": "Выдан"}]})])
        assert "ИСКИ БАНКА" in html
        assert "Изменений нет" not in html


class TestBankRoutineFilter:
    def test_routine_dropped_substance_kept(self):
        from court_monitor.lifecycle import filter_bank_routine_events
        mixed = _bank_change(["fi_hearing_new", "fi_resolved"])
        routine_only = _bank_change(["fi_status_change"])
        ordinary = _bank_change(["fi_hearing_new"])
        ordinary.pop("track")
        out = filter_bank_routine_events([mixed, routine_only, ordinary])
        assert len(out) == 2
        assert out[0]["type"] == ["fi_resolved"]
        assert out[1] is ordinary

    def test_writ_never_routine(self):
        from court_monitor.lifecycle import (
            BANK_ROUTINE_EVENT_TYPES, FI_ECHO_CATCHUP_TYPES,
        )
        assert "fi_writ_issued" not in BANK_ROUTINE_EVENT_TYPES
        assert "fi_writ_issued" not in FI_ECHO_CATCHUP_TYPES
        assert "fi_writ_status_changed" not in FI_ECHO_CATCHUP_TYPES


# ── Проводка мастер-выключателя ──────────────────────────────────────────────

class TestBankTrackWiring:
    """BANK_TRACK должен быть достижим из Actions Variables территории.

    Выключатель читается кодом (config.BANK_TRACK), но пока его не прокидывает
    workflow, переменная в Settings → Variables не делает НИЧЕГО — прогон её
    не видит. Ровно так и было до 26.07.2026: флаг документировался как
    «мастер-выключатель трека», а ни один workflow его не передавал.
    """

    @staticmethod
    def _workflow() -> str:
        root = os.path.dirname(SCRIPTS_DIR)
        path = os.path.join(root, ".github", "workflows", "update_cases.yml")
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_bank_track_forwarded_from_variables(self):
        import re
        m = re.search(r"^\s*BANK_TRACK:\s*(.+)$", self._workflow(), re.M)
        assert m, (
            "update_cases.yml не прокидывает BANK_TRACK — переменная "
            "BANK_TRACK=0 в Settings → Variables территории молча не "
            "сработает, трек останется включённым."
        )
        assert "vars.BANK_TRACK" in m.group(1), (
            f"BANK_TRACK берётся не из Variables: {m.group(1).strip()!r}"
        )

    def test_default_keeps_current_behaviour(self):
        """Без переменной поведение прежнее — фолбэк '1'.

        Иначе одна эта строка выключила бы трек и на эталоне (пилот ХМАО), где
        переменная не задана.
        """
        import re
        m = re.search(r"^\s*BANK_TRACK:\s*(.+)$", self._workflow(), re.M)
        assert m and re.search(r"\|\|\s*'1'", m.group(1)), (
            "У проброса BANK_TRACK нет фолбэка '1' — территория без "
            "переменной получит пустую строку и трек выключится."
        )

    def test_code_default_is_on(self):
        """Фолбэк workflow и дефолт кода не должны разъезжаться."""
        import importlib
        os.environ.pop("BANK_TRACK", None)
        importlib.reload(config)
        assert config.BANK_TRACK is True
        os.environ["BANK_TRACK"] = "0"
        importlib.reload(config)
        assert config.BANK_TRACK is False
        os.environ.pop("BANK_TRACK", None)
        importlib.reload(config)
