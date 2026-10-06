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

from court_monitor import bank_intake, config, lifecycle  # noqa: E402
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
        est = lifecycle.bank_legal_force_est(
            {"status": "Решено", "act_date": "02.02.2026"})
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
        """РЕШЁННЫЕ записи без decision_date (архив/легаси): якорь —
        hearing_date, у решённого дела она держит дату решения."""
        est = lifecycle.bank_legal_force_est(
            {"status": "Решено", "hearing_date": "02.02.2026"})
        # 02.02 + 10 раб. дн = 16.02 → месяц → 16.03 (пн) → в силе 17.03
        assert est == date(2026, 3, 17)

    def test_undecided_case_returns_none(self):
        """Решения нет → None, даже при непустой hearing_date: у живого дела
        это БУДУЩЕЕ заседание (последнее session-событие карточки), и прежний
        фолбэк считал «вступление в силу» от ещё не состоявшегося заседания —
        03.09.2026 штамп стоял у 342 из 554 активных исков банка, drawer
        печатал «Вступило в силу … (расч.)» у дел без решения."""
        fi = {"status": "В производстве", "hearing_date": "30.09.2026",
              "last_event": "Подготовка дела (собеседование). 08:50. 505."}
        assert lifecycle.bank_legal_force_est(fi) is None
        # Пустой статус (строка выдачи без карточки) — тоже не решение.
        assert lifecycle.bank_legal_force_est({"hearing_date": "02.02.2026"}) is None
        # Замороженная decision_date — достаточное доказательство решения
        # и при отстающем статусе карточки.
        assert lifecycle.bank_legal_force_est(
            {"status": "В производстве", "decision_date": "02.02.2026"}
        ) == date(2026, 3, 17)

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
            "default_copy_returned_date": "15.06.2026",
        }

    def test_copy_returned_date_reset_by_new_round(self):
        """Граница круга (новое решение-событие) сбрасывает и дату возврата
        копии — иначе событие fi_default_copy_returned второго круга
        считалось бы уже объявленным по дате первого."""
        fi = {"events": [
            {"date": "01.03.2026", "text": "Вынесено заочное решение по делу"},
            {"date": "15.03.2026",
             "text": "Копия заочного решения возвратилась невручённой"},
            {"date": "15.04.2026",
             "text": "Вынесено заочное решение по делу. Иск УДОВЛЕТВОРЕН"},
        ]}
        info = lifecycle.bank_default_judgment_info(fi)
        assert info["default_copy_returned"] is False
        assert info["default_copy_returned_date"] == ""

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


class TestDefaultCopyReturnedSeeding:
    """Анти-паводок fi_default_copy_returned: migrate_stages засевает
    эмит-флаг делам, где возврат копии случился ДО появления события
    (2-4427/2026, 2-2803/2026) — первый прогон после деплоя не объявляет
    месячной давности факты новостями."""

    def _case(self) -> dict:
        return {
            "id": "2-4427/2026",
            "current_stage": "first_instance",
            "track": "plaintiff_light",
            "bank_role": "Истец",
            "first_instance": {
                "case_number": "2-4427/2026",
                "status": "Решено",
                "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
                "hearing_date": "03.06.2026",
                "events": [
                    {"date": "03.06.2026",
                     "text": "Судебное заседание. 11:00. Вынесено заочное "
                             "решение по делу. Иск УДОВЛЕТВОРЕН"},
                    {"date": "08.07.2026",
                     "text": "Копия заочного решения возвратилась "
                             "невручённой. 16:25. 23.07.2026"},
                ],
            },
        }

    def test_seeded_with_event_date(self):
        case = self._case()
        lifecycle.migrate_stages([case])
        assert (case["first_instance"]["default_copy_returned_emitted"]
                == "08.07.2026")

    def test_existing_flag_untouched(self):
        """Уже стоящий флаг (в т.ч. от эмита FI-цикла) посев не перетирает."""
        case = self._case()
        case["first_instance"]["default_copy_returned_emitted"] = "01.01.2026"
        lifecycle.migrate_stages([case])
        assert (case["first_instance"]["default_copy_returned_emitted"]
                == "01.01.2026")

    def test_no_copy_return_not_seeded(self):
        case = self._case()
        case["first_instance"]["events"] = case["first_instance"]["events"][:1]
        lifecycle.migrate_stages([case])
        assert ("default_copy_returned_emitted"
                not in case["first_instance"])


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

    # ── Процессуальное завершение со статусом «Решено» (разгон 14.08.2026) ──
    # Карточка отдаёт «Решено» с терминальным «Результатом» — до фикса такие
    # дела уходили в ветку «Решено без ИЛ» и 180 дней ждали лист, которого не
    # будет. Кейсы: 9-125/2026 (Пуровский, отказ в принятии), 9-31/2026
    # (Берёзовский, возврат), 2-1588/2026 и 2-8088/2026 (передача по
    # подсудности).

    REFUSAL_RESULT = ("ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ ПОДЛЕЖИТ "
                      "РАССМОТРЕНИЮ и разрешению в порядке гражданского "
                      "судопроизводства")

    def test_refusal_to_accept_archived_after_30_days(self):
        case = _track_case(status="Решено", result=self.REFUSAL_RESULT,
                           event_date=self._dmy(56), hearing_date="")
        assert self._archived(case) is True

    def test_refusal_to_accept_kept_within_30_days(self):
        """Окно на частную жалобу — дело живо."""
        case = _track_case(status="Решено", result=self.REFUSAL_RESULT,
                           event_date=self._dmy(10), hearing_date="")
        assert self._archived(case) is False

    def test_refusal_anchor_falls_back_to_event_date(self):
        """У такой карточки нет ни решения, ни заседания, ни мотивировки —
        якорем работает последний фолбэк event_date. Без него дело осталось
        бы активным навсегда (`bool(anchor)` = False)."""
        fi = _track_case(status="Решено", result=self.REFUSAL_RESULT,
                         event_date="", hearing_date="")["first_instance"]
        assert lifecycle._is_bank_track_archived(fi, datetime.now()) is False
        fi["event_date"] = self._dmy(56)
        assert lifecycle._is_bank_track_archived(fi, datetime.now()) is True

    def test_transfer_by_jurisdiction_not_waiting_for_writ(self):
        case = _track_case(status="Решено",
                           result="Передано по подсудности, подведомственности",
                           event_date=self._dmy(56), hearing_date="")
        assert lifecycle.bank_writ_expected(case["first_instance"]) is False
        assert self._archived(case) is True

    def test_returned_with_decided_status_archived(self):
        """Кейс 9-31/2026: возврат, но статус карточки «Решено» — ветка
        «Возвращено» его не ловит, ловит ветка «листа не будет»."""
        case = _track_case(
            status="Решено",
            result="Заявление ВОЗВРАЩЕНО заявителюНЕВЫПОЛНЕНИЕ УКАЗАНИЙ судьи",
            event_date=self._dmy(56), hearing_date="")
        assert self._archived(case) is True


# ── Гейт приёма: дело, уже отработавшее свой цикл ────────────────────────────

class TestEntryIsSpentGate:
    """`entry_is_spent` — последний рубеж всех трёх каналов ввода (разбор
    03.08.2026). Кейс 2-592/2025: решение 06.10.2025, в иске отказано, суд
    сдал дело в архив 12.11.2025 — сборщик завёл его 31.07.2026, первый же
    прогон качнул карточку, объявил «текст решения опубликован» и отправил
    дело в архив трека. 26 из 27 записей bank-архива прожили в треке не
    больше трёх дней."""

    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def test_denied_long_ago_rejected(self):
        """Дело 2-592/2025: в иске отказано, месячный срок на жалобу истёк."""
        entry = _track_case(
            status="Решено",
            result="ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)",
            motivirovka_date=self._dmy(300), hearing_date=self._dmy(304),
        )
        assert bank_intake.entry_is_spent(entry) is True

    def test_denied_within_appeal_window_taken(self):
        """Свежий отказ — берём: банк ещё может подать апелляционную жалобу,
        ради раннего сигнала о сроке такие дела в трек и вносят."""
        entry = _track_case(
            status="Решено",
            result="ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)",
            motivirovka_date=self._dmy(10), hearing_date=self._dmy(14),
        )
        assert bank_intake.entry_is_spent(entry) is False

    def test_case_awaiting_writ_taken(self):
        """Иск удовлетворён, лист ещё не выдан — ровно то, ради чего трек."""
        entry = _track_case(
            status="Решено", result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
            hearing_date=self._dmy(40), act_date=self._dmy(35),
        )
        assert bank_intake.entry_is_spent(entry) is False

    def test_pending_case_taken(self):
        assert bank_intake.entry_is_spent(
            _track_case(status="В производстве")) is False

    def test_old_refusal_to_accept_is_spent(self):
        """Побочный эффект фикса 14.08.2026: старый отказ в принятии больше
        не заводится — раньше он проходил приём и полгода качался каждую
        неделю в ожидании листа, которого не будет. Отказ вечный
        (`already_spent` в PERMANENT_REJECTIONS), карточка не перекачивается.
        """
        entry = _track_case(
            status="Решено", hearing_date="", event_date=self._dmy(56),
            result="ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ ПОДЛЕЖИТ "
                   "РАССМОТРЕНИЮ",
        )
        assert bank_intake.entry_is_spent(entry) is True

    def test_fresh_refusal_to_accept_taken(self):
        """Свежий отказ в принятии берём — окно на частную жалобу открыто."""
        entry = _track_case(
            status="Решено", hearing_date="", event_date=self._dmy(10),
            result="ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ ПОДЛЕЖИТ "
                   "РАССМОТРЕНИЮ",
        )
        assert bank_intake.entry_is_spent(entry) is False

    def test_appeal_flag_never_spent(self):
        """Признак жалобы гасит архивные окна первой же веткой — авто-подхват
        (skip_appeal=False) обязан такие дела заводить: тем же прогоном они
        переезжают в основной cases.json на мониторинг апелляции."""
        entry = _track_case(
            status="Решено",
            result="ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)",
            motivirovka_date=self._dmy(300), appeal_filed=True,
        )
        assert bank_intake.entry_is_spent(entry) is False

    def test_stale_writ_issued_rejected(self):
        """Лист на исполнение выдан давно → окно 14 дней истекло."""
        entry = _track_case(
            status="Решено", result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
            hearing_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(30), "status": "Выдан"}],
        )
        assert bank_intake.entry_is_spent(entry) is True

    def test_non_track_entry_untouched(self):
        """Предикат смотрит на трек-запись; у обычного дела своё 60-дневное
        окно — гейт зовут только каналы ввода трека, но перепутать нельзя."""
        entry = _track_case(status="В производстве", hearing_date=self._dmy(400))
        entry.pop("track")
        assert bank_intake.entry_is_spent(entry) is False


class TestMakeBankEntryFreezesDecisionDate:
    """make_bank_entry ставит resolved_emitted=True решённым делам, а эмит
    fi_resolved — единственное место, где замерзает decision_date: для
    импортированного дела он уже не выстрелит никогда. Без штампа якорем
    classify_writ_kind / bank_legal_force_est осталась бы дрейфующая
    hearing_date."""

    ROW = {
        "case_number": "2-1/2026", "court": "Сургутский городской суд",
        "court_domain": "surggor--hmao.sudrf.ru", "judge": "Иванов И.И.",
        "filing_date": "01.02.2026", "status": "Решено",
        "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
        "link": "1|a-1", "plaintiff": "ПАО Сбербанк", "defendant": "Петров П.П.",
        "category": "кредит", "bank_role": "Истец",
    }

    def _entry(self, card):
        return bank_intake.make_bank_entry(
            dict(self.ROW), card, "тест", "2026-08-03T09:00:00")

    def test_decision_date_taken_from_events(self):
        entry = self._entry({
            "Статус": "Решено", "Дата заседания": "20.06.2026",
            "_events": [{"date": "10.03.2026",
                         "text": "Вынесено решение по делу. Иск удовлетворён"},
                        {"date": "20.06.2026",
                         "text": "Судебное заседание. Заявление о расходах"}],
        })
        fi = entry["first_instance"]
        assert fi["resolved_emitted"] is True
        # Дата решения, а не пост-решенческого заседания.
        assert fi["decision_date"] == "10.03.2026"

    def test_pending_case_gets_no_decision_date(self):
        entry = self._entry({"Статус": "В производстве",
                             "Дата заседания": "20.09.2026", "_events": []})
        assert "decision_date" not in entry["first_instance"]

    def test_decided_card_without_decision_event(self):
        """Событие решения в истории не найдено — поле не выдумываем."""
        entry = self._entry({"Статус": "Решено",
                             "Дата заседания": "20.06.2026", "_events": []})
        assert entry["first_instance"].get("decision_date", "") == ""


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

    def test_refusal_stamps_writ_expected_false_and_drops_est(self):
        """Отказ в принятии: фронт получает готовый штамп «листа не будет».

        Без штампа дело попадает в KPI и чип «Ждут ИЛ» (`awaitsWrit` в app.js
        читает только его), а расчётная дата вступления в силу рисовала бы
        строку «Вступило в силу (расч.)» там, где исполнять нечего.
        Разовая миграция данных не нужна — штамп пересчитывается каждым
        прогоном, здесь это и проверяется: поле снимается с записи, где оно
        осталось от прошлых прогонов.
        """
        from court_monitor.runs import split_bank_track
        case = _track_case(
            status="Решено", hearing_date="", event_date=self._dmy(10),
            result="ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ ПОДЛЕЖИТ "
                   "РАССМОТРЕНИЮ",
            legal_force_est="2026-09-18",  # штамп прошлых прогонов
        )
        _, bank_active, _, _ = split_bank_track([case])
        assert bank_active == [case]
        fi = case["first_instance"]
        assert fi["writ_expected"] is False
        assert "legal_force_est" not in fi

    def test_gate_follows_data_not_load_counter(self):
        """Гейт раскладки смотрит на дела, а не на «сколько загрузилось из
        cases_bank.json»: на территории без файла трека (и при авто-подхвате
        прогоном) счётчик загрузки нулевой, и дела утекли бы в cases.json."""
        from court_monitor.runs import bank_track_pending
        ordinary = {"id": "2-1/2026", "first_instance": {}}
        assert bank_track_pending([ordinary]) is False
        assert bank_track_pending([ordinary, _track_case()]) is True

    def test_gate_off_when_track_disabled(self, monkeypatch):
        from court_monitor import config as cm_config
        from court_monitor.runs import bank_track_pending
        monkeypatch.setattr(cm_config, "BANK_TRACK", False)
        assert bank_track_pending([_track_case()]) is False

    def test_court_ids_backfilled(self):
        """Записи ручных каналов заведены без delo_id/srv_num — ссылку «в суд»
        фронт собирал по фолбэку 1540005/1."""
        from court_monitor.runs import split_bank_track
        case = _track_case(status="В производстве")
        split_bank_track([case])
        fi = case["first_instance"]
        assert fi["delo_id"] and fi["srv_num"] == 1

    def test_two_court_domain_not_guessed(self):
        """На vartovray--hmao.sudrf.ru два суда (районный srv 1 и Покачи srv 2):
        по одному домену сервер не угадать — неверный хуже фолбэка."""
        from court_monitor.runs import split_bank_track
        case = _track_case(status="В производстве")
        case["first_instance"]["court_domain"] = "vartovray--hmao.sudrf.ru"
        split_bank_track([case])
        assert "srv_num" not in case["first_instance"]

    def test_existing_ids_not_overwritten(self):
        from court_monitor.runs import split_bank_track
        case = _track_case(status="В производстве", delo_id=1, srv_num=2)
        split_bank_track([case])
        assert case["first_instance"]["srv_num"] == 2

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


# ── Возврат из горячего bank-архива (reactivate_bank_archived) ───────────────

class TestBankArchiveReactivation:
    """Регресс на инцидент 04–07.08.2026: возврат заочных из архива без гейта
    «уже в активных» клонировал дело каждым прогоном (+1 копия в день), а
    архивный файл не пересохранялся и продолжал отдавать те же записи."""

    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def _default_archived_entry(self, **extra) -> dict:
        """Заочное решение, ИЛ 30 дн назад: по старому окну (14 дн) уехало в
        архив, по текущему (BANK_DEFAULT_WRIT_ARCHIVE_DAYS=90) — активно."""
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            decision_date=self._dmy(120), default_judgment=True,
            writs=[{"issue_date": self._dmy(30), "status": "Выдан"}],
        )
        case["archived_at"] = "2026-07-27"
        case.update(extra)
        return case

    def test_returned_case_removed_from_archive(self):
        from court_monitor.linking import reactivate_bank_archived
        entry = self._default_archived_entry()
        cases: list[dict] = []
        archived = [entry]
        moved = reactivate_bank_archived(cases, archived)
        assert moved == 1
        assert archived == []           # мутация на месте — файл ужмётся
        assert cases == [entry]
        assert "archived_at" not in entry
        assert entry["track"] == "plaintiff_light"

    def test_already_active_not_duplicated(self):
        """Главный регресс: дело уже в активных → НЕ клонировать, запись
        остаётся в архиве (её судьбу решит юрист/ремонт, а не дубль)."""
        from court_monitor.linking import reactivate_bank_archived
        active = _track_case(status="Решено")
        archived_entry = self._default_archived_entry()
        cases = [active]
        archived = [archived_entry]
        moved = reactivate_bank_archived(cases, archived)
        assert moved == 0
        assert cases == [active]        # ни одной новой копии
        assert archived == [archived_entry]

    def test_same_number_other_court_still_returned(self):
        """Ключ — (домен, id): одноимённое дело ДРУГОГО суда возврату не
        мешает (номера дел не уникальны между судами)."""
        from court_monitor.linking import reactivate_bank_archived
        other_court = _track_case(status="В производстве")
        other_court["first_instance"]["court_domain"] = "nvartovsk--hmao.sudrf.ru"
        entry = self._default_archived_entry()
        cases = [other_court]
        archived = [entry]
        moved = reactivate_bank_archived(cases, archived)
        assert moved == 1
        assert entry in cases and archived == []

    def test_ordinary_judgment_stays_archived(self):
        """Регресс на 2-3898/2026: обычное (не заочное) решение с ИЛ 30 дн
        назад — окно 14 дн истекло, дело законно лежит в архиве."""
        from court_monitor.linking import reactivate_bank_archived
        entry = self._default_archived_entry()
        del entry["first_instance"]["default_judgment"]
        cases: list[dict] = []
        archived = [entry]
        moved = reactivate_bank_archived(cases, archived)
        assert moved == 0
        assert cases == [] and archived == [entry]

    def test_duplicates_inside_archive_collapse(self):
        """Два экземпляра одного дела в самом архиве → возвращается один,
        второй остаётся лежать (не два клона в активных)."""
        import copy
        from court_monitor.linking import reactivate_bank_archived
        a = self._default_archived_entry()
        b = copy.deepcopy(a)
        cases: list[dict] = []
        archived = [a, b]
        moved = reactivate_bank_archived(cases, archived)
        assert moved == 1
        assert len(cases) == 1 and len(archived) == 1

    def test_run_wiring_saves_archive_after_reactivation(self):
        """Проводка в main_json: счётчик bank_reactivated обязан входить в
        условие пересохранения bank-архива — иначе изъятие живёт только в
        памяти и клонирование возвращается (сам инцидент)."""
        import inspect
        import re
        from court_monitor import runs
        src = inspect.getsource(runs.main_json)
        assert re.search(
            r"if \(bank_newly_archived or bank_reactivated\b", src
        ), "bank_reactivated выпал из условия сохранения bank-архива"
        assert "reactivate_bank_archived(cases, bank_archived_cases)" in src


# ── Дайджест: секция «Иски банка» ────────────────────────────────────────────

def _bank_change(types: list[str], details: dict | None = None,
                 case: str = "2-100/2026") -> dict:
    d = {"link": "111|aaaa-1111", "court_domain": "surggor--hmao.sudrf.ru"}
    d.update(details or {})
    return {
        "case": case,
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

    def test_resolved_dismissal_prints_closure_with_reason(self):
        """Прекращение производства — не «вынесено решение», а определение
        с причиной (разбор юриста 12.08.2026, дела 2-3974/2026 и 2-6650/2026:
        отказ от иска и мировое печатались как решения без причины)."""
        html = _digest([_bank_change(["fi_resolved"], {
            "verdict_label": "прекращено",
            "raw_result": ("Производство по делу ПРЕКРАЩЕНОСТОРОНЫ ЗАКЛЮЧИЛИ "
                           "МИРОВОЕ СОГЛАШЕНИЕ и оно утверждено судом"),
            "decision_date": "06.08.2026",
        })])
        assert ("производство по делу прекращено</b> 06.08.2026 "
                "(в связи с утверждением мирового соглашения)") in html
        assert "вынесено решение" not in html

    def test_resolved_no_consideration_prints_closure(self):
        html = _digest([_bank_change(["fi_resolved"], {
            "verdict_label": "оставлено без рассмотрения",
            "raw_result": ("Иск (заявление, жалоба) ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ"
                           "ИСТЕЦ НЕ ЯВИЛСЯ В СУД ПО ВТОРИЧНОМУ ВЫЗОВУ"),
            "decision_date": "05.08.2026",
        })])
        assert ("иск оставлен без рассмотрения</b> 05.08.2026 "
                "(истец не явился в суд по вторичному вызову)") in html
        assert "вынесено решение" not in html

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

    # ── Правки по разбору дайджеста 07.08.2026 ──

    def test_grouped_by_importance(self):
        """Группировка «по важности» (решение юриста 17.08.2026): ИЛ →
        решения → иные (сюда же завершения) → заседания (ближайшие
        сверху) → новые иски последними."""
        h_far = _bank_change(["fi_hearing_new"], {"hearing_date": "20.09.2026"})
        h_far["case"] = "2-901/2026"
        h_near = _bank_change(["fi_hearing_next"],
                              {"hearing_date": "15.08.2026"})
        h_near["case"] = "2-902/2026"
        new_claim = _bank_change(["fi_bank_claim_registered"],
                                 {"filing_date": "05.08.2026"})
        new_claim["case"] = "М-903/2026"
        transfer = _bank_change(["fi_returned"],
                                {"termination_kind": "transfer"})
        transfer["case"] = "2-904/2026"
        writ = _bank_change(["fi_writ_issued"], {"writs": [{
            "issue_date": "06.08.2026", "electronic_id": "86RS#1",
            "status": "Выдан"}]})
        writ["case"] = "2-905/2026"
        resolved = _bank_change(["fi_resolved"],
                                {"verdict_label": "удовлетворено"})
        resolved["case"] = "2-906/2026"
        html = _digest([h_far, h_near, new_claim, transfer, writ, resolved])
        order = [html.index(n) for n in
                 ("2-905/2026", "2-906/2026", "2-904/2026", "2-902/2026",
                  "2-901/2026", "М-903/2026")]
        assert order == sorted(order), "порядок групп «по важности» нарушен"
        assert "ИСКИ БАНКА (6)" in html
        # Воздух (просьба юриста 10.08.2026): границы групп — «⸻»
        # (в наборе 5 групп → 4 границы; других секций в этом дайджесте
        # нет, ⸻ больше взяться неоткуда), между делами одной группы —
        # пустая строка (два заседания).
        assert html.count("⸻") == 4, "границы групп не помечены «⸻»"
        seg = html[html.index("2-902/2026"):html.index("2-901/2026")]
        assert "\n\n" in seg, (
            "между делами одной группы нет пустой строки (воздух 10.08.2026)"
        )
        assert "⸻" not in seg, "внутри группы не должно быть «⸻»"

    def test_final_event_motivirovka_informative(self):
        """За генериком «движение по делу» пряталась мотивировка — факт, от
        которого течёт срок на апелляцию (2-5178/2026, 07.08.2026)."""
        html = _digest([_bank_change(["fi_final_event"], {
            "event": ("Изготовлено мотивированное решение в окончательной "
                      "форме. 17:00. 06.08.2026"),
            "event_date": "31.07.2026"})])
        assert "📄 мотивировка изготовлена (06.08.2026)" in html
        assert "движение по делу" not in html

    def test_final_event_other_quoted(self):
        html = _digest([_bank_change(["fi_final_event"], {
            "event": "Производство по делу возобновлено. 10:00. 05.08.2026",
            "event_date": "05.08.2026"})])
        assert "Производство по делу возобновлено" in html
        assert "движение по делу" not in html

    def test_final_event_empty_falls_back(self):
        """Старый контекст без details.event — прежний генерик (replay)."""
        html = _digest([_bank_change(["fi_final_event"])])
        assert "движение по делу" in html

    def test_transfer_carries_date(self):
        """«Когда передано?» — вопрос юриста по 2-8088/2026: суд заполняет
        «Результат» с лагом в недели, дата события обязана быть в строке."""
        html = _digest([_bank_change(["fi_returned"], {
            "termination_kind": "transfer",
            "termination_date": "07.07.2026"})])
        assert "дело передано по подсудности (07.07.2026)" in html

    def test_transfer_without_date_no_parens(self):
        """Старый контекст без termination_date — без пустых скобок."""
        html = _digest([_bank_change(["fi_returned"],
                                     {"termination_kind": "transfer"})])
        assert "дело передано по подсудности" in html
        assert "по подсудности (" not in html

    def test_act_text_published_with_context(self):
        """Дата решения и заочность в строке публикации: суд задним числом
        выложил тексты июньских решений (2-4427/2026, 07.08.2026)."""
        html = _digest([_bank_change(["fi_act_text_published"], {
            "decision_date": "03.06.2026", "default_judgment": True})])
        assert "текст решения от 03.06.2026 опубликован (🌙 заочное)" in html

    def test_act_text_published_legacy_plain(self):
        html = _digest([_bank_change(["fi_act_text_published"])])
        assert "текст решения опубликован" in html
        assert "🌙" not in html

    def test_resolved_with_date_and_default_marker(self):
        html = _digest([_bank_change(["fi_resolved"], {
            "verdict_label": "удовлетворено", "decision_date": "03.06.2026",
            "default_judgment": True})])
        assert ("вынесено решение</b> 03.06.2026: удовлетворено (🌙 заочное)"
                in html)

    def test_default_copy_returned_rendered(self):
        """Новое событие: возврат копии заочного решения (формула ВС) —
        раньше факт жил только внутри расчёта legal_force_est."""
        html = _digest([_bank_change(["fi_default_copy_returned"], {
            "copy_returned_date": "08.07.2026"})])
        assert ("🌙 копия заочного решения возвратилась невручённой"
                in html)
        assert "08.07.2026" in html

    def test_summary_counts_bank_transfers(self):
        """Сводка «1 дело — по подсудности» при двух передачах в теле
        (07.08.2026: 2-822 в основном треке + 2-8088 в банковском)."""
        ordinary = _bank_change(["fi_returned"],
                                {"termination_kind": "transfer"})
        ordinary.pop("track")
        bank = _bank_change(["fi_returned"],
                            {"termination_kind": "transfer"})
        html = _digest([ordinary, bank])
        assert "➡️ 2 дела — по подсудности" in html

    def test_summary_bank_only_transfer_counted(self):
        html = _digest([_bank_change(["fi_returned"],
                                     {"termination_kind": "transfer"})])
        assert "➡️ 1 дело — по подсудности" in html

    def test_writ_batch_collapsed_to_range(self):
        """Пачка однотипных ИЛ схлопывается в «префикс + диапазон»
        (решение юриста 10.08.2026; кейс 2-201/2026 — 6 одинаковых фраз)."""
        writs = [{"issue_date": "05.08.2026",
                  "electronic_id": f"86RS0018#2-201/2026#{i}",
                  "blank_number": "", "status": "Выдан",
                  "recipient": "ОСП по Кондинскому району"}
                 for i in range(1, 7)]
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": writs})])
        assert ("выдано 6 исполнительных листов</b> 05.08.2026 "
                "(86RS0018#2-201/2026#1, №2–6)") in html
        assert html.count("86RS0018#2-201/2026#1") == 1
        assert "№2–6" in html, (
            "полный префикс ИД должен печататься один раз"
        )
        # Сводка считает ДЕЛА с выданными ИЛ (исторически), не листы —
        # схлопывание фраз её не меняет.
        assert "(🧾 1 ИЛ)" in html

    def test_writ_batch_split_by_recipient(self):
        """Разные получатели — отдельные фразы; разрывные номера — списком
        и диапазоном (кейс 2-4938/2026: Сургут №4,6 / Н.Уренгой №5,7 /
        Когалым №8–9)."""
        def w(i, rec):
            return {"issue_date": "04.08.2026",
                    "electronic_id": f"86RS0004#2-4938/2026#{i}",
                    "blank_number": "", "status": "Выдан", "recipient": rec}
        writs = [w(4, "ОСП по г. Сургуту"), w(5, "ОСП по г. Новому Уренгою"),
                 w(6, "ОСП по г. Сургуту"), w(7, "ОСП по г. Новому Уренгою"),
                 w(8, "ОСП по г. Когалыму"), w(9, "ОСП по г. Когалыму")]
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": writs})])
        assert "(86RS0004#2-4938/2026#4, №6) → ОСП по г. Сургуту" in html
        assert "(86RS0004#2-4938/2026#5, №7) → ОСП по г. Новому Уренгою" in html
        assert "(86RS0004#2-4938/2026#8, №9) → ОСП по г. Когалыму" in html

    def test_writ_batch_with_single_leftover(self):
        """Группа + одиночка (кейс 2-6140/2026: №1–3 в ОСП, №4 взыскателю):
        одиночный лист — прежним полным форматом."""
        def w(i, rec):
            return {"issue_date": "06.08.2026",
                    "electronic_id": f"86RS0004#2-6140/2026#{i}",
                    "blank_number": "", "status": "Выдан", "recipient": rec}
        writs = [w(1, "ОСП по г. Сургуту"), w(2, "ОСП по г. Сургуту"),
                 w(3, "ОСП по г. Сургуту"), w(4, "Взыскатель")]
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": writs})])
        assert "выдано 3 исполнительных листа</b> 06.08.2026" in html
        assert "(86RS0004#2-6140/2026#1, №2–3) → ОСП по г. Сургуту" in html
        assert ("выдан исполнительный лист</b> 06.08.2026 "
                "(86RS0004#2-6140/2026#4) → Взыскатель") in html

    def test_writ_batch_with_blank_number_not_collapsed(self):
        """Бумажный бланк «ФС №…» терять нельзя — пачка с ним не
        схлопывается (fail-open в прежний пер-листовый формат)."""
        writs = [{"issue_date": "05.08.2026",
                  "electronic_id": "86RS0018#2-201/2026#1",
                  "blank_number": "ФС № 039166358", "status": "Выдан",
                  "recipient": "ОСП по Кондинскому району"},
                 {"issue_date": "05.08.2026",
                  "electronic_id": "86RS0018#2-201/2026#2",
                  "blank_number": "", "status": "Выдан",
                  "recipient": "ОСП по Кондинскому району"}]
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": writs})])
        assert "выдано 2" not in html
        assert "ФС № 039166358" in html
        assert "86RS0018#2-201/2026#2" in html

    def test_interim_writ_batch_collapsed(self):
        writs = [{"issue_date": "23.04.2026", "kind": "interim",
                  "electronic_id": f"86RS0004#2-100/2026#{i}",
                  "blank_number": "", "status": "Выдан",
                  "recipient": ""} for i in (1, 2)]
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": writs})])
        assert ("🛡 <b>выдано 2 обеспечительных листа (арест)</b> 23.04.2026 "
                "(86RS0004#2-100/2026#1, №2)") in html

    def test_footer_mentions_bank_track(self):
        """Футер «всего 78» без упоминания сотен активных исков банка
        дезориентировал (разбор 07.08.2026); 0 = трек выключен, приписки
        нет (территория без трека и старые контексты replay)."""
        from court_monitor.digest.template import generate_template_digest
        html = generate_template_digest(
            [], [], fi_changes=[_bank_change(["fi_hearing_new"],
                                             {"hearing_date": "01.09.2026"})],
            total_active_bank=345,
        )
        assert "· 🏦 иски банка: 345 в производстве</b>" in html
        html0 = _digest([_bank_change(["fi_hearing_new"],
                                      {"hearing_date": "01.09.2026"})])
        assert "иски банка:" not in html0

    def test_footer_bank_tail_in_no_changes_digest(self):
        from court_monitor.digest.template import generate_template_digest
        html = generate_template_digest([], [], total_active_bank=345)
        assert "🏦 иски банка: 345 в производстве" in html

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
        assert "1 дело с событиями по искам банка" in html

    def test_only_bank_changes_not_empty_digest(self):
        html = _digest([_bank_change(["fi_writ_issued"], {"writs": [
            {"issue_date": "01.07.2026", "status": "Выдан"}]})])
        assert "ИСКИ БАНКА" in html
        assert "Изменений нет" not in html

    def test_auto_intake_announced(self):
        """Авто-подхват пополняет картотеку сам — пополнение не должно быть
        молчаливым (решение юриста 31.07.2026)."""
        html = _digest([_bank_change(["fi_bank_claim_registered"],
                                     {"filing_date": "28.07.2026"})])
        assert "ИСКИ БАНКА (1)" in html
        assert "иск банка взят на мониторинг" in html
        assert "подан 28.07.2026" in html
        assert "2-100/2026" in html

    def test_auto_intake_with_appeal_says_where_case_went(self):
        """Дело с жалобой тем же прогоном уезжает в основную картотеку —
        иначе юрист ищет его в лёгком треке и не находит."""
        html = _digest([_bank_change(["fi_bank_claim_registered"],
                                     {"left_track": True})])
        assert "подана жалоба, дело в общем треке" in html

    def test_intake_line_counted_in_summary(self):
        """Строка приёма считается в сводке как событие трека — иначе
        счётчик «(N)» заголовка разойдётся с содержимым (сторож линтера)."""
        html = _digest([_bank_change(["fi_bank_claim_registered"])])
        assert "1 дело с событиями по искам банка" in html


class TestBankIntakeDigestFold:
    """Свёртка «заведено N новых исков банка» (разгон Урала 14.08.2026).

    Первый боевой прогон авто-подхвата завёл 116 исков разом, и секция стала
    стеной одинаковых строк «взят на мониторинг» (HTML 60 КБ): решения, ИЛ и
    заседания в ней утонули. Порог — BANK_INTAKE_DIGEST_FOLD (25), условие
    «больше порога».
    """

    @staticmethod
    def _intake(n: int) -> list[dict]:
        return [_bank_change(["fi_bank_claim_registered"],
                             case=f"2-{1000 + i}/2026") for i in range(n)]

    def test_at_threshold_rendered_per_case(self):
        """Ровно порог — ещё подельно (условие «больше 25»)."""
        html = _digest(self._intake(25))
        assert "ИСКИ БАНКА (25)" in html
        assert "2-1000/2026" in html
        assert "заведено" not in html

    def test_above_threshold_folded(self):
        html = _digest(self._intake(26))
        assert "заведено 26 новых исков банка" in html
        assert "2-1000/2026" not in html
        assert "2-1025/2026" not in html
        # Все дела свёрнуты — заголовок без счётчика.
        assert "ИСКИ БАНКА:" in html and "ИСКИ БАНКА (" not in html

    def test_mixed_run_keeps_real_events(self):
        """Настоящие события печатаются подробно и только они в счётчике."""
        html = _digest(self._intake(30) + [
            _bank_change(["fi_writ_issued"], {"writs": [
                {"issue_date": "26.06.2026", "status": "Выдан"}]},
                case="2-500/2026"),
            _bank_change(["fi_resolved"], {"result": "Иск УДОВЛЕТВОРЕН"},
                         case="2-501/2026"),
        ])
        assert "ИСКИ БАНКА (2)" in html
        assert "2-500/2026" in html and "2-501/2026" in html
        assert html.count("взят на мониторинг") == 0
        assert "заведено 30 новых исков банка" in html
        # Подхват — в конце секции (решение юриста 17.08.2026): свёртка
        # ниже настоящих событий. Позиция считается по группам, и
        # перестановка групп утащила бы её в середину молча.
        assert html.index("заведено 30 новых исков банка") > max(
            html.index("2-500/2026"), html.index("2-501/2026"))

    def test_case_with_registration_and_event_not_folded(self):
        """Заведено И получило решение тем же прогоном — подробно."""
        html = _digest(self._intake(30) + [
            _bank_change(["fi_bank_claim_registered", "fi_resolved"],
                         {"result": "Иск УДОВЛЕТВОРЕН"}, case="2-777/2026"),
        ])
        assert "ИСКИ БАНКА (1)" in html
        assert "2-777/2026" in html

    def test_left_track_registration_kept_detailed(self):
        """Дело, тем же прогоном уехавшее в общий трек, — подробно: это
        единственный сигнал, что искать его надо уже не в лёгком треке."""
        html = _digest(self._intake(30) + [
            _bank_change(["fi_bank_claim_registered"], {"left_track": True},
                         case="2-888/2026"),
        ])
        assert "ИСКИ БАНКА (1)" in html
        assert "2-888/2026" in html
        assert "подана жалоба, дело в общем треке" in html

    def test_fold_off_by_zero(self, monkeypatch):
        monkeypatch.setattr(config, "BANK_INTAKE_DIGEST_FOLD", 0)
        html = _digest(self._intake(200))
        assert "ИСКИ БАНКА (200)" in html
        assert "заведено" not in html

    def test_summary_splits_counts(self):
        html = _digest(self._intake(30) + [
            _bank_change(["fi_resolved"], {"result": "Иск УДОВЛЕТВОРЕН"},
                         case="2-501/2026"),
        ])
        assert "1 дело с событиями по искам банка" in html
        assert "30 новых исков банка заведено" in html

    def test_summary_unchanged_below_threshold(self):
        """Регресс формата: без свёртки сводка прежняя."""
        html = _digest([_bank_change(["fi_bank_claim_registered"])])
        assert "1 дело с событиями по искам банка" in html
        assert "заведено" not in html

    def test_folded_line_has_no_case_number(self):
        """Иначе её посчитает _check_section_counters — счётчик разойдётся."""
        from court_monitor.digest.postprocess import _line_has_case_number
        from court_monitor.digest.template import _bank_intake_fold_line
        line = _bank_intake_fold_line(self._intake(30))
        assert _line_has_case_number(line) is False

    def test_folded_line_is_not_a_header(self):
        """Заголовком её принимать нельзя: машина состояний секций оборвала бы
        счёт соседней секции (инцидент 12.08.2026 с наследованием режима)."""
        from court_monitor.digest.postprocess import _DIGEST_HEADER_RE
        from court_monitor.digest.template import _bank_intake_fold_line
        line = _bank_intake_fold_line(self._intake(30))
        assert _DIGEST_HEADER_RE.match(line) is None

    def test_folded_line_counts_courts(self):
        """На разгоне юрист хочет видеть охват — хвост «в N судах»."""
        from court_monitor.digest.template import _bank_intake_fold_line
        changes = self._intake(30)
        for i, ch in enumerate(changes):
            ch["court"] = f"Суд №{i % 12}"
        assert "в 12 судах" in _bank_intake_fold_line(changes)


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

    def test_intake_announcement_never_routine(self):
        """Приём в трек переживает BANK_DIGEST_ROUTINE=0: это не рутина
        карточки, а единственный сигнал, что картотека выросла сама."""
        from court_monitor.lifecycle import (
            BANK_ROUTINE_EVENT_TYPES, filter_bank_routine_events,
        )
        assert "fi_bank_claim_registered" not in BANK_ROUTINE_EVENT_TYPES
        ch = _bank_change(["fi_bank_claim_registered"])
        assert filter_bank_routine_events([ch]) == [ch]


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


class TestBankIntakeCapsWiring:
    """Кэпы подхвата и порог алерта должны быть достижимы из Variables.

    Рычаг темпа ввода территории (разгон Урала 08.2026: юрист держит
    ~200 дел/день): без проброса в update_cases.yml переменные в Settings →
    Variables молча не работают, а константный порог алерта (50) при кэпе 200
    слал бы 🩺 «паводок» каждый штатный день разгона.
    """

    CAPS = {
        "BANK_INTAKE_MAX_PER_RUN": "30",
        "BANK_INTAKE_MAX_CARDS_PER_COURT": "10",
        "BANK_INTAKE_ALERT_ADDED": "50",
        "BANK_INTAKE_DIGEST_FOLD": "25",
    }

    @staticmethod
    def _workflow() -> str:
        return TestBankTrackWiring._workflow()

    def test_caps_forwarded_from_variables(self):
        import re
        wf = self._workflow()
        for name, default in self.CAPS.items():
            m = re.search(rf"^\s*{name}:\s*(.+)$", wf, re.M)
            assert m, (
                f"update_cases.yml не прокидывает {name} — переменная в "
                "Settings → Variables территории молча не сработает."
            )
            assert f"vars.{name}" in m.group(1), (
                f"{name} берётся не из Variables: {m.group(1).strip()!r}"
            )
            assert re.search(rf"\|\|\s*'{default}'", m.group(1)), (
                f"У проброса {name} нет фолбэка '{default}' — территория без "
                "переменной получит пустую строку."
            )

    def test_alert_threshold_reads_env(self):
        """config.BANK_INTAKE_ALERT_ADDED обязан читаться из env."""
        import importlib
        os.environ["BANK_INTAKE_ALERT_ADDED"] = "200"
        try:
            importlib.reload(config)
            assert config.BANK_INTAKE_ALERT_ADDED == 200
        finally:
            os.environ.pop("BANK_INTAKE_ALERT_ADDED", None)
            importlib.reload(config)
        assert config.BANK_INTAKE_ALERT_ADDED == 50

    def test_fold_threshold_reads_env(self):
        import importlib
        os.environ["BANK_INTAKE_DIGEST_FOLD"] = "0"
        try:
            importlib.reload(config)
            assert config.BANK_INTAKE_DIGEST_FOLD == 0
        finally:
            os.environ.pop("BANK_INTAKE_DIGEST_FOLD", None)
            importlib.reload(config)
        assert config.BANK_INTAKE_DIGEST_FOLD == 25

    def test_fold_forwarded_to_replay_workflows(self):
        """Порог свёртки обязан быть и в replay-путях: иначе тестовый или
        резервный дайджест разойдётся с боевым кроном."""
        import re
        root = os.path.dirname(SCRIPTS_DIR)
        for name in ("test_digest.yml", "replay_on_push.yml"):
            path = os.path.join(root, ".github", "workflows", name)
            with open(path, encoding="utf-8") as f:
                yml = f.read()
            m = re.search(r"^\s*BANK_INTAKE_DIGEST_FOLD:\s*(.+)$", yml, re.M)
            assert m, f"{name} не прокидывает BANK_INTAKE_DIGEST_FOLD"
            assert "vars.BANK_INTAKE_DIGEST_FOLD" in m.group(1)
            assert re.search(r"\|\|\s*'25'", m.group(1))

    def test_digest_slimming_knobs_forwarded_everywhere(self):
        """Свёртка «вступило в силу» и потолок сторон (21.08.2026).

        Три ручки сокращения дайджеста обязаны стоять во ВСЕХ трёх путях:
        крон, тестовый дайджест и replay Mac-резерва. Иначе выпуск, собранный
        replay'ем (боевой путь с 19.08.2026), разойдётся с кроновым.
        """
        import re
        root = os.path.dirname(SCRIPTS_DIR)
        knobs = {
            "BANK_FORCE_DIGEST_FOLD": "3",
            "DIGEST_PARTIES_MAX_LEN": "60",
            "DIGEST_PARTIES_KEEP": "2",
        }
        for name in ("update_cases.yml", "test_digest.yml",
                     "replay_on_push.yml"):
            path = os.path.join(root, ".github", "workflows", name)
            with open(path, encoding="utf-8") as f:
                yml = f.read()
            for knob, default in knobs.items():
                m = re.search(rf"^\s*{knob}:\s*(.+)$", yml, re.M)
                assert m, f"{name} не прокидывает {knob}"
                assert f"vars.{knob}" in m.group(1), (
                    f"{name}: {knob} берётся не из Variables")
                assert re.search(rf"\|\|\s*'{default}'", m.group(1)), (
                    f"{name}: у {knob} нет фолбэка '{default}'")

    def test_overdue_repeat_forwarded_to_run_workflow(self):
        """Шаг эскалации «ИЛ не выдан» — в update_cases.yml.

        Только туда: календарный проход живёт в main_json, а replay рендерит
        уже собранный контекст и событий не эмитит.
        """
        import re
        wf = self._workflow()
        m = re.search(r"^\s*BANK_WRIT_OVERDUE_REPEAT_DAYS:\s*(.+)$", wf, re.M)
        assert m, "update_cases.yml не прокидывает BANK_WRIT_OVERDUE_REPEAT_DAYS"
        assert "vars.BANK_WRIT_OVERDUE_REPEAT_DAYS" in m.group(1)
        assert re.search(r"\|\|\s*'30'", m.group(1))

    def test_overdue_steps_ladder(self):
        """Лестница порогов — один источник правды для прогона и тестов."""
        import importlib
        assert config.writ_overdue_steps() == (30, 60, 90, 120, 150)
        os.environ["BANK_WRIT_OVERDUE_REPEAT_DAYS"] = "0"
        try:
            importlib.reload(config)
            assert config.writ_overdue_steps() == (30,), (
                "REPEAT=0 обязан возвращать прежнее «один раз за жизнь дела»")
        finally:
            os.environ.pop("BANK_WRIT_OVERDUE_REPEAT_DAYS", None)
            importlib.reload(config)

    def test_slimming_thresholds_read_env(self):
        import importlib
        for knob, probe, default in (
            ("BANK_FORCE_DIGEST_FOLD", "0", 3),
            ("DIGEST_PARTIES_MAX_LEN", "0", 60),
            ("DIGEST_PARTIES_KEEP", "5", 2),
        ):
            os.environ[knob] = probe
            try:
                importlib.reload(config)
                assert getattr(config, knob) == int(probe), knob
            finally:
                os.environ.pop(knob, None)
                importlib.reload(config)
            assert getattr(config, knob) == default, knob


# ── Особый порядок отмены заочного решения (ст. 237-243 ГПК) ────────────────
# Ответчик подаёт заявление об отмене в ТОТ ЖЕ суд 1-й инстанции; это не
# апелляция, и апелляционный ход у него открывается только после определения
# об отказе (ст. 237 ч. 2). Формулировки событий — дословно с карточек
# 2-243/2026 (Югорский) и 2-616/2026 (Пыть-Яхский), проверенных 03.08.2026.

def _ev(date_: str, text: str, **cols) -> dict:
    return {"date": date_, "text": text, **cols}


def _default_events(filed: str = "", hearing: str = "",
                    result: str = "", decision: str = "06.05.2026") -> list:
    evs = [_ev(decision,
               "Судебное заседание. 11:30. Вынесено заочное решение по делу. "
               "Иск (заявление, жалоба) УДОВЛЕТВОРЕН. 15.04.2026",
               name="Судебное заседание",
               result_event="Вынесено заочное решение по делу")]
    if filed:
        evs.append(_ev(filed,
                       "Регистрация заявления об отмене заочного решения. "
                       "12:22. 08.07.2026",
                       name="Регистрация заявления об отмене заочного решения"))
    if hearing:
        text = "Рассмотрение заявления об отмене заочного решения. 09:20. "
        if result:
            text += result + ". "
        cols = {"name": "Рассмотрение заявления об отмене заочного решения"}
        if result:
            cols["result_event"] = result
        evs.append(_ev(hearing, text + "14.07.2026", **cols))
    return evs


class TestDefaultCancellationState:
    def test_no_application_no_state(self):
        st = lifecycle.default_cancellation_state(
            {"events": _default_events()}, date(2026, 8, 3))
        assert st["outcome"] == ""

    def test_filed_without_hearing_is_pending(self):
        st = lifecycle.default_cancellation_state(
            {"events": _default_events(filed="28.07.2026")}, date(2026, 8, 3))
        assert st["outcome"] == "pending"
        assert st["filed_date"] == "28.07.2026"

    def test_hearing_ahead_is_pending(self):
        """Кейс 2-616/2026: заседание 10.08 назначено, результата ещё нет."""
        st = lifecycle.default_cancellation_state(
            {"events": _default_events(filed="28.07.2026",
                                       hearing="10.08.2026")},
            date(2026, 8, 3))
        assert st["outcome"] == "pending"
        assert st["hearing_date"] == "10.08.2026"

    def test_cancelled(self):
        """Кейс 2-243/2026: «Заочное решение отменено» в колонке результата."""
        st = lifecycle.default_cancellation_state(
            {"events": _default_events(filed="08.07.2026",
                                       hearing="22.07.2026",
                                       result="Заочное решение отменено")},
            date(2026, 8, 3))
        assert st["outcome"] == "cancelled"
        assert st["outcome_date"] == "22.07.2026"

    def test_refused(self):
        st = lifecycle.default_cancellation_state(
            {"events": _default_events(
                filed="08.07.2026", hearing="22.07.2026",
                result="В удовлетворении заявления отказано")},
            date(2026, 8, 3))
        assert st["outcome"] == "refused"

    def test_unrelated_result_is_not_refusal(self):
        """⚠️ «Заседание отложено» встречается в корпусе 124 раза — объявить
        его отказом значило бы открыть апелляционный ход раньше времени."""
        for res in ("Заседание отложено", "Объявлен перерыв",
                    "Производство по делу приостановлено"):
            st = lifecycle.default_cancellation_state(
                {"events": _default_events(filed="08.07.2026",
                                           hearing="22.07.2026", result=res)},
                date(2026, 8, 3))
            assert st["outcome"] == "pending", res

    def test_legacy_event_without_columns(self):
        """43% событий 1-й инст. основной картотеки идут без колонок —
        исход должен читаться из склейки text."""
        evs = [_ev("06.05.2026",
                   "Судебное заседание. Вынесено заочное решение по делу."),
               _ev("08.07.2026",
                   "Регистрация заявления об отмене заочного решения."),
               _ev("22.07.2026",
                   "Рассмотрение заявления об отмене заочного решения. "
                   "Заочное решение отменено.")]
        st = lifecycle.default_cancellation_state({"events": evs},
                                                  date(2026, 8, 3))
        assert st["outcome"] == "cancelled"

    def test_pending_ceiling(self):
        """Суд не заполнил результат — через потолок дело возвращается к
        обычным окнам, иначе висело бы активным вечно."""
        st = lifecycle.default_cancellation_state(
            {"events": _default_events(filed="01.01.2026",
                                       hearing="15.01.2026")},
            date(2026, 8, 3))
        assert st["outcome"] == "unknown"

    def test_new_application_resets_previous_outcome(self):
        evs = _default_events(filed="08.07.2026", hearing="22.07.2026",
                              result="Заочное решение отменено")
        evs.append(_ev("30.07.2026",
                       "Регистрация заявления об отмене заочного решения.",
                       name="Регистрация заявления об отмене заочного решения"))
        st = lifecycle.default_cancellation_state({"events": evs},
                                                  date(2026, 8, 3))
        assert st["outcome"] == "pending"
        assert st["filed_date"] == "30.07.2026"


class TestDefaultJudgmentVacated:
    def test_vacated_when_cancellation_newer_than_frozen_decision(self):
        fi = {"events": _default_events(filed="08.07.2026",
                                        hearing="22.07.2026",
                                        result="Заочное решение отменено"),
              "decision_date": "06.05.2026"}
        assert lifecycle.default_judgment_vacated(fi) is True

    def test_new_decision_after_cancellation_heals_predicate(self):
        """⚠️ Ритм опроса решённого дела — неделя, поэтому отмена и новое
        решение по ст. 243 попадают в одно окно парса. Предикат гаснет по
        переставленной decision_date, а не по «нет более позднего решения»."""
        fi = {"events": _default_events(filed="08.07.2026",
                                        hearing="22.07.2026",
                                        result="Заочное решение отменено"),
              "decision_date": "10.08.2026"}
        assert lifecycle.default_judgment_vacated(fi) is False

    def test_vacated_survives_decision_date_move(self):
        """После отката дата лежит в decision_date_vacated — предикат обязан
        читать и её, иначе «решения нет» залипло бы навсегда."""
        fi = {"events": _default_events(filed="08.07.2026",
                                        hearing="22.07.2026",
                                        result="Заочное решение отменено"),
              "decision_date_vacated": "06.05.2026"}
        assert lifecycle.default_judgment_vacated(fi) is True

    def test_writ_kind_anchor_survives_vacating(self):
        """⚠️ Без якоря decision_date_vacated classify_writ_kind свалился бы
        на дрейфующую hearing_date и перевернул тип уже выданного листа."""
        fi = {"decision_date_vacated": "06.05.2026",
              "hearing_date": "20.09.2026"}
        assert lifecycle.classify_writ_kind(
            {"issue_date": "20.06.2026"}, fi) == "enforcement"


class TestDefaultCancellationGates:
    def _case(self, **fi) -> dict:
        return _track_case(**fi)

    def test_pending_keeps_case_in_track(self):
        """Кейс 2-616/2026: суд зарегистрировал апел. жалобу 23.07, но
        заявление об отмене ещё не рассмотрено — дело остаётся в треке."""
        case = self._case(appeal_filed=True, appeal_filed_date="23.07.2026",
                          events=_default_events(filed="28.07.2026",
                                                 hearing="10.08.2026"))
        assert lifecycle.bank_case_left_track(case) is False

    def test_pending_keeps_stage(self):
        case = self._case(appeal_filed=True, appeal_filed_date="23.07.2026",
                          events=_default_events(filed="28.07.2026",
                                                 hearing="10.08.2026"))
        assert lifecycle.advance_case_stage(case) is None
        assert case["current_stage"] == "first_instance"

    def test_refusal_opens_appeal_route(self):
        case = self._case(appeal_filed=True, appeal_filed_date="23.07.2026",
                          events=_default_events(
                              filed="08.07.2026", hearing="22.07.2026",
                              result="В удовлетворении заявления отказано"))
        assert lifecycle.bank_case_left_track(case) is True

    def test_appeal_after_vacating_opens_route(self):
        """Жалоба, поданная уже ПОСЛЕ возобновления, дело выпускает."""
        case = self._case(appeal_filed=True, appeal_filed_date="01.09.2026",
                          decision_date="06.05.2026",
                          events=_default_events(
                              filed="08.07.2026", hearing="22.07.2026",
                              result="Заочное решение отменено"))
        assert lifecycle.bank_case_left_track(case) is True

    def test_sent_to_appeal_always_leaves(self):
        """Дело физически ушло в облсуд — гейт не держит."""
        case = self._case(appeal_filed=True, sent_to_appeal=True,
                          events=_default_events(filed="28.07.2026",
                                                 hearing="10.08.2026"))
        assert lifecycle.bank_case_left_track(case) is True

    def test_bank_defendant_reaches_awaiting_appeal(self):
        """⚠️ advance_case_stage — общий код. Банк-ОТВЕТЧИК с заочным решением
        против него обязан дойти до awaiting_appeal: иначе relink_awaiting_appeal
        (единственный канал связки на капчёвых судах) его не увидит."""
        case = {
            "id": "2-18/2026", "current_stage": "first_instance",
            "bank_role": "Ответчик",
            "first_instance": {
                "appeal_filed": True, "appeal_filed_date": "23.07.2026",
                "events": _default_events(filed="28.07.2026",
                                          hearing="10.08.2026"),
            },
        }
        lifecycle.advance_case_stage(case)
        assert case["current_stage"] == "awaiting_appeal"

    def test_stage_gate_does_not_freeze_linked_appeal(self):
        """⚠️ Гейт правит только ветку признаков жалобы. Дело, законно
        переведённое link_cases в стадию appeal, обязано покинуть трек —
        иначе апел. блок писался бы в cases_bank.json, который основной фронт
        не грузит, и дело исчезло бы бесшумно."""
        case = self._case(events=_default_events(filed="28.07.2026",
                                                 hearing="10.08.2026"))
        case["current_stage"] = "appeal"
        assert lifecycle.bank_case_left_track(case) is True


class TestDefaultCancellationArchiveAndRhythm:
    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def test_default_judgment_gets_three_months(self):
        """Заочному — 90 дней от выдачи ИЛ вместо 14 (решение юриста
        03.08.2026): так 27.07 в архив ушли три дела Сургутского гор. суда."""
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            decision_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(40), "status": "Выдан"}],
            events=_default_events(decision=self._dmy(120)),
        )
        assert lifecycle.is_case_archived(case) is False

    def test_ordinary_judgment_keeps_two_weeks(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            decision_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(40), "status": "Выдан"}],
            events=[_ev(self._dmy(120),
                        "Судебное заседание. Вынесено решение по делу.")],
        )
        assert lifecycle.is_case_archived(case) is True

    def test_default_judgment_archives_after_three_months(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(200),
            decision_date=self._dmy(200),
            writs=[{"issue_date": self._dmy(95), "status": "Выдан"}],
            events=_default_events(decision=self._dmy(200)),
        )
        assert lifecycle.is_case_archived(case) is True

    def test_pending_application_blocks_archive(self):
        case = _track_case(
            status="Решено", hearing_date=self._dmy(120),
            decision_date=self._dmy(120),
            writs=[{"issue_date": self._dmy(95), "status": "Выдан"}],
            events=_default_events(decision=self._dmy(120),
                                   filed=self._dmy(5), hearing=self._dmy(-5)),
        )
        assert lifecycle.is_case_archived(case) is False

    def test_skip_until_cancellation_hearing(self):
        """Событие «Рассмотрение заявления об отмене…» не матчится
        _HEARING_MARKERS_RX, поэтому без своей ветки дело парсилось бы каждым
        прогоном."""
        case = _track_case(
            status="Решено", last_checked_at="2026-08-03",
            events=_default_events(filed="28.07.2026", hearing="10.08.2026"),
        )
        skip, reason = lifecycle.should_skip_case(case, date(2026, 8, 4))
        assert skip is True and reason.startswith("default_cancel_hearing")
        assert "заседание 2026-08-10" in lifecycle.skip_reason_ru(reason)
        skip, _ = lifecycle.should_skip_case(case, date(2026, 8, 10))
        assert skip is False

    def test_legal_force_est_silenced(self):
        pending = _track_case(
            decision_date="06.05.2026",
            events=_default_events(filed="28.07.2026", hearing="10.08.2026"),
        )["first_instance"]
        assert lifecycle.bank_legal_force_est(pending) is None
        vacated = _track_case(
            decision_date="06.05.2026",
            events=_default_events(filed="08.07.2026", hearing="22.07.2026",
                                   result="Заочное решение отменено"),
        )["first_instance"]
        assert lifecycle.bank_legal_force_est(vacated) is None

    def test_legal_force_est_from_refusal(self):
        """Отказ в отмене — месяц на апелляцию течёт с этого дня (ст. 237 ч. 2)."""
        fi = _track_case(
            decision_date="06.05.2026",
            events=_default_events(filed="08.07.2026", hearing="22.07.2026",
                                   result="В удовлетворении заявления отказано"),
        )["first_instance"]
        assert lifecycle.bank_legal_force_est(fi) == date(2026, 8, 25)


class TestDefaultCancelWeeklyRhythm:
    """Pending-отмена заочного БЕЗ даты заседания (или с прошедшей): первые
    BANK_DEFAULT_CANCEL_DAILY_GRACE_DAYS от якоря (дата заседания, иначе дата
    подачи) читаем ежедневно, дальше — недельный ритм. Прежний ранний выход
    `return False, ""` читал такое дело каждым прогоном все 90 дн потолка
    (2-3005/2026 Орджоникидзевского: заявление 21.07.2026, месяц ежедневных
    чтений)."""

    TODAY = date(2026, 8, 20)

    def _pending(self, filed: str, checked_days_ago: int,
                 hearing: str = "") -> dict:
        return _track_case(
            status="Решено",
            last_checked_at=(
                self.TODAY - timedelta(days=checked_days_ago)).isoformat(),
            events=_default_events(filed=filed, hearing=hearing),
        )

    def test_fresh_application_parsed_daily(self):
        """Свежее заявление: ст. 240 даёт суду 10 дней — дату ловим сразу."""
        case = self._pending(filed="17.08.2026", checked_days_ago=1)
        assert lifecycle.should_skip_case(case, self.TODAY) == (False, "")

    def test_stalled_application_weekly(self):
        case = self._pending(filed="31.07.2026", checked_days_ago=3)
        skip, reason = lifecycle.should_skip_case(case, self.TODAY)
        assert skip is True
        assert reason == "default_cancel_weekly(3d/7d)"

    def test_weekly_rhythm_parses_on_eighth_day(self):
        case = self._pending(filed="31.07.2026", checked_days_ago=8)
        assert lifecycle.should_skip_case(case, self.TODAY) == (False, "")

    def test_passed_hearing_fresh_parsed_daily(self):
        """Заседание прошло, результат не заполнен — свежий исход ловим
        ежедневно: якорь — дата ЗАСЕДАНИЯ, а не давняя подача."""
        case = self._pending(filed="20.07.2026", hearing="17.08.2026",
                             checked_days_ago=1)
        assert lifecycle.should_skip_case(case, self.TODAY) == (False, "")

    def test_passed_hearing_stalled_weekly(self):
        case = self._pending(filed="20.07.2026", hearing="31.07.2026",
                             checked_days_ago=2)
        skip, reason = lifecycle.should_skip_case(case, self.TODAY)
        assert skip is True
        assert reason == "default_cancel_weekly(2d/7d)"

    def test_reason_translated(self):
        assert "раз в 7 дн" in lifecycle.skip_reason_ru(
            "default_cancel_weekly(3d/7d)")

    def test_runs_wiring_counts_weekly(self):
        """Без проводки новая причина утекла бы в «без движения» — и в плане
        очереди, и в классификации скипов FI-цикла."""
        path = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")
        with open(path, encoding="utf-8") as f:
            src = f.read()
        assert src.count('"default_cancel_weekly"') >= 2, (
            "default_cancel_weekly должен стоять в ОБОИХ кортежах недельного "
            "ритма (план очереди + классификация скипов FI-цикла)."
        )


class TestRepairVacatedDefaultJudgments:
    def test_vacated_case_returns_to_work(self):
        case = _track_case(
            status="Решено", result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
            resolved_emitted=True, decision_date="06.05.2026",
            events=_default_events(filed="08.07.2026", hearing="22.07.2026",
                                   result="Заочное решение отменено"),
        )
        assert lifecycle.repair_vacated_default_judgments([case]) == 1
        fi = case["first_instance"]
        assert fi["status"] == "В производстве"
        assert fi["result"] == ""
        assert fi["resolved_emitted"] is False
        assert "decision_date" not in fi
        assert fi["decision_date_vacated"] == "06.05.2026"
        # Идемпотентность — иначе ремонт зовётся ДО FI-цикла каждым прогоном.
        assert lifecycle.repair_vacated_default_judgments([case]) == 0

    def test_case_returns_to_track(self):
        case = {
            "id": "2-616/2026", "current_stage": "awaiting_appeal",
            "bank_role": "Истец", "track_origin": "plaintiff_light",
            "appeal": None,
            "first_instance": {
                "appeal_filed": True, "appeal_filed_date": "23.07.2026",
                "events": _default_events(filed="28.07.2026",
                                          hearing="10.08.2026"),
            },
        }
        assert lifecycle.repair_vacated_default_judgments([case]) == 1
        assert case["track"] == "plaintiff_light"
        assert "track_origin" not in case
        assert case["current_stage"] == "first_instance"

    def test_real_appeal_untouched(self):
        """2-504/2026, 2-339/2026, 2-318/2026 — настоящие апелляции ответчика
        (заочными не являются), их переезд корректен."""
        case = {
            "id": "2-504/2026", "current_stage": "awaiting_appeal",
            "bank_role": "Истец", "track_origin": "plaintiff_light",
            "appeal": None,
            "first_instance": {
                "appeal_filed": True, "appeal_filed_date": "20.07.2026",
                "events": [_ev("02.07.2026",
                               "Судебное заседание. Вынесено решение по делу.")],
            },
        }
        assert lifecycle.repair_vacated_default_judgments([case]) == 0
        assert case["current_stage"] == "awaiting_appeal"
        assert "track" not in case

    def test_migrate_stages_runs_repair_first(self):
        """⚠️ Порядок: бэкфилл decision_date вернул бы снятую дату из
        hearing_date, а цикл advance_case_stage — стадию awaiting_appeal."""
        case = {
            "id": "2-243/2026", "current_stage": "first_instance",
            "bank_role": "Истец", "track": "plaintiff_light", "appeal": None,
            "first_instance": {
                "status": "Решено", "result": "Иск УДОВЛЕТВОРЕН",
                "resolved_emitted": True, "decision_date": "06.05.2026",
                "hearing_date": "06.05.2026",
                "events": _default_events(filed="08.07.2026",
                                          hearing="22.07.2026",
                                          result="Заочное решение отменено"),
            },
        }
        lifecycle.migrate_stages([case])
        fi = case["first_instance"]
        assert fi["status"] == "В производстве"
        assert "decision_date" not in fi
        assert case["current_stage"] == "first_instance"


class TestVacatedDefaultWiring:
    """Проводка отката отменённого заочного решения в FI-цикле main_json.

    Все три инварианта невидимы рендер-тестам: они проверяют уже готовую
    комбинацию типов, которую сломанный конвейер просто не произведёт.
    По образцу TestFiTerminationWiring — unit на исходник вместо тяжёлого e2e.
    """

    @staticmethod
    def _runs_src() -> str:
        path = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_guard_two_has_vacated_exception(self):
        """⚠️ Без исключения в Гарде 2 понижение статуса откатывается тем же
        прогоном: калитка fi_resolution_contradicted_by_future_hearing
        завершается `return not has_decision`, а у заочного дела событие
        решения есть всегда."""
        src = self._runs_src()
        guard = src[src.index("# Гард 2: регрессия статуса"):]
        guard = guard[:guard.index("new_status = old_status")]
        assert "not vacated_default" in guard, (
            "Из Гарда 2 пропало исключение vacated_default — статус «Решено» "
            "будет возвращаться, и отменённое заочное решение навсегда "
            "останется действующим."
        )

    def test_downgrade_is_one_transaction(self):
        """Сброс resolved_emitted отдельно от статуса даёт ложный «Иск
        удовлетворён» в дайджест КАЖДЫМ прогоном (ремонты зовутся до
        FI-цикла, а триггер-событие карточки никуда не девается)."""
        src = self._runs_src()
        block = src[src.index("if vacated_default:\n            if fi.get(\"result\")"):]
        block = block[:block.index("if new_hearing_date:")]
        assert 'fi["result"] = ""' in block
        assert 'fi["decision_date_vacated"] = fi.pop("decision_date")' in block, (
            "decision_date должна ПЕРЕЕЗЖАТЬ, а не удаляться: иначе "
            "classify_writ_kind свалится на дрейфующую hearing_date и "
            "перевернёт тип уже выданного листа."
        )
        for flag in ("resolved_emitted", "motivirovka_emitted"):
            assert flag in block, f"Транзакция не сбрасывает {flag}."

    def test_emit_block_before_hearing_block(self):
        """У решённого дела case_decided глушит hearing-блок — события особого
        порядка обязаны эмититься до него."""
        src = self._runs_src()
        i_cancel = src.index("_cancel_st = default_cancellation_state(fi, today)")
        i_hearing = src.index("# Новое/перенесённое заседание")
        assert i_cancel < i_hearing

    def test_emit_is_idempotent_by_value(self):
        """Флаги хранят ЗНАЧЕНИЯ, а не True: состояние пересчитывается из
        events каждым прогоном, а дифф _events_newly_match не годится (его
        ключ включает дату размещения — перепубликация карточки судом дала бы
        «новое» событие)."""
        src = self._runs_src()
        for flag in ("default_cancel_filed_emitted",
                     "default_cancel_hearing_emitted",
                     "default_cancel_outcome_emitted"):
            assert f'fi["{flag}"] = ' in src, f"Нет флага эмита {flag}."
            assert f'fi.get("{flag}") != ' in src, (
                f"Флаг {flag} сравнивается не по значению — повторный эмит "
                "на втором круге заочного производства не сработает."
            )

    def test_new_types_have_labels_everywhere(self):
        """Неизвестный тип даёт ГОЛУЮ строку дела в 3.2 и всё равно считается
        в счётчике «Изменения (N)» — подписи нужны в трёх местах."""
        base = os.path.join(SCRIPTS_DIR, "court_monitor", "digest")
        for fname in ("template.py", "core.py"):
            with open(os.path.join(base, fname), encoding="utf-8") as f:
                src = f.read()
            for t in ("fi_default_cancellation_filed",
                      "fi_default_cancellation_hearing",
                      "fi_default_judgment_vacated",
                      "fi_default_cancellation_refused",
                      "fi_default_copy_returned",
                      "fi_default_copy_served",
                      "fi_legal_force_reached",
                      "fi_writ_overdue",
                      "fi_post_decision_hearing"):
                assert t in src, f"{fname}: нет подписи для типа {t}."

    def test_types_are_not_routine_and_not_echo(self):
        """Трек существует ради таких событий — рутинным фильтром их гасить
        нельзя. Но в стародатном они быть ОБЯЗАНЫ: импортёр Урала заводит
        карточки с многолетней историей."""
        from court_monitor.lifecycle import (
            BANK_ROUTINE_EVENT_TYPES, FI_ECHO_CATCHUP_TYPES,
            _FI_CATCHUP_DATED_TYPES,
        )
        for t in ("fi_default_cancellation_filed",
                  "fi_default_judgment_vacated",
                  "fi_default_copy_returned",
                  "fi_default_copy_served"):
            assert t not in BANK_ROUTINE_EVENT_TYPES
            assert t not in FI_ECHO_CATCHUP_TYPES
            assert t in _FI_CATCHUP_DATED_TYPES

    def test_calendar_types_filter_wiring(self):
        """Календарные события и пост-решенческие заседания: не рутина и не
        эхо (ради них правка 13.08.2026 и делалась); «вступило в силу» — в
        стародатном (страховка от массового импорта давно решённых дел), а
        «ИЛ завис» — НЕТ намеренно: поздно обнаруженный зависший лист — тем
        более алерт. Пост-решенческое заседание — в анонсном списке (прошлая
        дата не анонс)."""
        from court_monitor.lifecycle import (
            BANK_ROUTINE_EVENT_TYPES, FI_ECHO_CATCHUP_TYPES,
            _FI_DATED_COMPLAINT_TYPES, _FI_HEARING_ANNOUNCE_TYPES,
        )
        for t in ("fi_legal_force_reached", "fi_writ_overdue",
                  "fi_post_decision_hearing"):
            assert t not in BANK_ROUTINE_EVENT_TYPES
            assert t not in FI_ECHO_CATCHUP_TYPES
        assert "fi_legal_force_reached" in _FI_DATED_COMPLAINT_TYPES
        assert (_FI_DATED_COMPLAINT_TYPES["fi_legal_force_reached"]
                == "legal_force_date")
        assert "fi_writ_overdue" not in _FI_DATED_COMPLAINT_TYPES
        assert "fi_post_decision_hearing" in _FI_HEARING_ANNOUNCE_TYPES


# ── Обогащение строк датами (разбор дайджеста 13.08.2026) ────────────────────

class TestBankDigestEnrichedDates:
    """Половина строк секции была голыми подписями без дат. Каждая ветка
    replay-safe: ключа в details нет (старый контекст) — прежняя подпись."""

    def test_objections_deadline_carries_date(self):
        """Дата срока — и есть новость; без неё строка не говорила ничего."""
        html = _digest([_bank_change(["fi_objections_deadline_set"],
                                     {"objections_due": "25.08.2026"})])
        assert "возражения на жалобу — до <b>25.08.2026</b>" in html

    def test_objections_deadline_legacy_fallback(self):
        html = _digest([_bank_change(["fi_objections_deadline_set"])])
        assert "установлен срок для возражений на жалобу" in html

    def test_cancellation_filed_carries_date(self):
        html = _digest([_bank_change(["fi_default_cancellation_filed"],
                                     {"cancel_filed_date": "12.08.2026"})])
        assert "заявление об отмене заочного решения (12.08.2026)" in html

    def test_cancellation_hearing_carries_date(self):
        """Дата заседания по заявлению — явка представителя банка."""
        html = _digest([_bank_change(["fi_default_cancellation_hearing"],
                                     {"cancel_hearing_date": "20.08.2026"})])
        assert "заседание по заявлению об отмене — <b>20.08.2026</b>" in html

    def test_cancellation_outcomes_carry_dates(self):
        html = _digest([_bank_change(["fi_default_judgment_vacated"],
                                     {"cancel_outcome_date": "12.08.2026"})])
        assert "дело рассматривается заново (12.08.2026)" in html
        html = _digest([_bank_change(["fi_default_cancellation_refused"],
                                     {"cancel_outcome_date": "12.08.2026"})])
        assert "в отмене заочного решения отказано (12.08.2026)" in html

    def test_cancellation_legacy_no_empty_parens(self):
        html = _digest([_bank_change(["fi_default_cancellation_filed"])])
        assert "подано заявление об отмене заочного решения" in html
        assert "решения (" not in html

    def test_appeal_filed_carries_date(self):
        html = _digest([_bank_change(["fi_appeal_filed"],
                                     {"appeal_filed_date": "11.08.2026"})])
        assert ("апел. жалоба ответчика от 11.08.2026 — дело уходит "
                "в общий трек") in html

    def test_appeal_filed_legacy_fallback(self):
        html = _digest([_bank_change(["fi_appeal_filed"])])
        assert "апел. жалоба ответчика — дело уходит в общий трек" in html

    def test_cassation_events_carry_dates(self):
        html = _digest([_bank_change(["fi_cassation_filed"],
                                     {"cassation_filed_date": "01.08.2026"})])
        assert "касс. жалоба (01.08.2026)" in html
        html = _digest([_bank_change(["fi_sent_to_cassation"],
                                     {"sent_to_cassation_date": "05.08.2026"})])
        assert "направлено в касс. суд (05.08.2026)" in html

    def test_motivirovka_carries_date(self):
        """От мотивировки течёт месяц на апелляцию (ст. 321 ГПК); дата была
        только у fi_final_event-ветки — асимметрия убрана."""
        html = _digest([_bank_change(["fi_motivirovka_emitted"],
                                     {"motivirovka_date": "08.08.2026"})])
        assert "мотивировка изготовлена (08.08.2026)" in html

    def test_act_published_carries_date(self):
        html = _digest([_bank_change(["fi_act_published"],
                                     {"act_date": "08.08.2026"})])
        assert "решение изготовлено (08.08.2026)" in html

    def test_hearing_carries_time(self):
        """Основной трек время печатает, банковский терял — а являться-то
        по времени."""
        html = _digest([_bank_change(["fi_hearing_new"],
                                     {"hearing_date": "17.08.2026",
                                      "hearing_time": "10:30"})])
        assert "заседание <b>17.08.2026</b> в 10:30" in html

    def test_hearing_midnight_placeholder_hidden(self):
        """00:00 — заглушка ГАС «времени нет» (правило _fmt_hearing_dt)."""
        html = _digest([_bank_change(["fi_hearing_new"],
                                     {"hearing_date": "17.08.2026",
                                      "hearing_time": "00:00"})])
        assert "в 00:00" not in html
        assert "заседание <b>17.08.2026</b>" in html

    def test_postponed_carries_time(self):
        html = _digest([_bank_change(["fi_hearing_postponed"],
                                     {"hearing_date": "21.08.2026",
                                      "hearing_time": "14:00"})])
        assert "отложено на <b>21.08.2026</b> в 14:00" in html

    def test_recess_carries_continuation(self):
        """details перерыва несут НОВУЮ дату продолжения (runs.py пишет
        hearing_* для всех исходов классификации) — раньше терялась."""
        html = _digest([_bank_change(["fi_hearing_recess"],
                                     {"hearing_date": "21.08.2026",
                                      "hearing_time": "09:00"})])
        assert "перерыв — продолжение <b>21.08.2026</b> в 09:00" in html

    def test_recess_legacy_fallback(self):
        html = _digest([_bank_change(["fi_hearing_recess"])])
        assert "перерыв в заседании" in html

    def test_bank_role_change_carries_roles(self):
        html = _digest([_bank_change(["fi_bank_role_changed"],
                                     {"old_role": "Истец",
                                      "new_role": "Третье лицо"})])
        assert "роль банка: Истец → Третье лицо" in html

    def test_bank_role_change_legacy_fallback(self):
        html = _digest([_bank_change(["fi_bank_role_changed"])])
        assert "роль банка изменилась" in html

    def test_summary_plural_cases(self):
        """Сводка считает ДЕЛА (записи секции) и склоняет честно — прежняя
        подпись «N событий» врала (у дела может быть несколько событий)."""
        second = _bank_change(["fi_resolved"])
        second["case"] = "2-101/2026"
        html = _digest([_bank_change(["fi_resolved"]), second])
        assert "2 дела с событиями по искам банка" in html


# ── Календарные события: рендер ──────────────────────────────────────────────

class TestBankCalendarEventRendering:
    def test_legal_force_reached_rendered(self):
        html = _digest([_bank_change(["fi_legal_force_reached"],
                                     {"legal_force_date": "06.08.2026"})])
        assert ("решение вступило в силу (расч. 06.08.2026) — ожидаем ИЛ"
                in html)

    def test_legal_force_reached_legacy_fallback(self):
        html = _digest([_bank_change(["fi_legal_force_reached"])])
        assert "решение вступило в силу (расч.) — ожидаем ИЛ" in html

    def test_writ_overdue_rendered(self):
        html = _digest([_bank_change(["fi_writ_overdue"],
                                     {"overdue_days": 33,
                                      "legal_force_date": "11.07.2026"})])
        assert ("ИЛ не выдан 33 дн. после вступления в силу</b> "
                "(в силе с 11.07.2026)") in html

    def test_post_decision_hearing_rendered(self):
        html = _digest([_bank_change(
            ["fi_post_decision_hearing"],
            {"hearing_date": "25.08.2026", "hearing_time": "11:00",
             "hearing_topic": "индексация присужденных сумм"})])
        assert ("заседание по решённому делу — <b>25.08.2026</b> в 11:00 "
                "(индексация присужденных сумм)") in html

    def test_post_decision_hearing_without_topic(self):
        html = _digest([_bank_change(["fi_post_decision_hearing"],
                                     {"hearing_date": "25.08.2026"})])
        assert "заседание по решённому делу — <b>25.08.2026</b>" in html

    def test_copy_served_rendered(self):
        html = _digest([_bank_change(["fi_default_copy_served"],
                                     {"copy_served_date": "12.08.2026"})])
        assert ("копия заочного решения вручена ответчику 12.08.2026 "
                "(7 раб. дн. на заявление об отмене)") in html

    def test_calendar_events_grouped_with_writs(self):
        """«Вступило в силу»/«лист завис» — группа ИЛ (этапы одной цепочки
        «решение → сила → выдача»); пост-решенческое заседание — в группе
        заседаний (сортировка по дате достаётся сама)."""
        from court_monitor.digest.template import _bank_change_group
        assert (_bank_change_group({"type": ["fi_legal_force_reached"]})
                == _bank_change_group({"type": ["fi_writ_issued"]}))
        assert (_bank_change_group({"type": ["fi_writ_overdue"]})
                == _bank_change_group({"type": ["fi_writ_issued"]}))
        assert (_bank_change_group({"type": ["fi_post_decision_hearing"]})
                == _bank_change_group({"type": ["fi_hearing_new"]}))

    def test_group_order_matches_lawyer_request(self):
        """Страж всего порядка (решение юриста 17.08.2026): листы →
        решения → иные (завершения тут же) → заседания → новые иски.
        Без него следующая перестановка групп прошла бы молча."""
        from court_monitor.digest.template import _bank_change_group
        groups = [_bank_change_group({"type": [t]}) for t in (
            "fi_writ_issued", "fi_resolved", "fi_returned",
            "fi_hearing_new", "fi_bank_claim_registered")]
        assert groups == sorted(groups) and len(set(groups)) == len(groups), (
            f"порядок групп секции нарушен: {groups}")
        # «Иные» — дефолт: незнакомый тип встаёт туда же, где завершения.
        assert (_bank_change_group({"type": ["fi_objections_deadline_set"]})
                == _bank_change_group({"type": ["fi_returned"]}))

    def test_motivirovka_does_not_pull_case_out_of_writs(self):
        """Мотивировочный fi_final_event поднимает дело в группу решений,
        но дело с ЛИСТОМ в том же прогоне остаётся в группе листов —
        ловушка гарда после перестановки (листы стали нулевой группой)."""
        from court_monitor.digest.template import _bank_change_group
        motiv = {"details": {"event": ("Изготовлено мотивированное решение "
                                       "в окончательной форме. 06.08.2026")}}
        assert (_bank_change_group({"type": ["fi_final_event"], **motiv})
                == _bank_change_group({"type": ["fi_resolved"]}))
        assert (_bank_change_group(
            {"type": ["fi_writ_issued", "fi_final_event"], **motiv})
            == _bank_change_group({"type": ["fi_writ_issued"]}))


# ── Структурные инварианты групп секции «Иски банка» ─────────────────────────

class TestBankGroupInvariants:
    """Ловушки перестановки 17.08.2026: «иные» — ДЕФОЛТ и сидит в СЕРЕДИНЕ
    порядка. Дефолт в середине ломает наивные приёмы (min() с дефолтом,
    `len()` как индекс, «новая группа в конец»), а падают они тихо —
    строка просто уезжает не в тот блок. Тесты ниже держат саму структуру,
    а не отдельный тип."""

    @staticmethod
    def _mod():
        from court_monitor.digest import template
        return template

    def test_other_group_owns_no_types(self):
        """У «иных» не должно быть своего набора типов: заведи ему набор —
        и дефолт молча сольётся с ним, а порядок останется прежним."""
        t = self._mod()
        assert t._BANK_GROUP_OTHER not in t._BANK_GROUP_ORDER

    def test_intake_group_stays_last(self):
        """Свёртка «заведено N новых исков» встаёт по числу дел с группой
        МЕНЬШЕ intake — новая группа после него увела бы её из хвоста
        секции, а юрист просил подхват именно в конце."""
        t = self._mod()
        assert t._BANK_GROUP_INTAKE == max(t._BANK_GROUP_ORDER)
        assert t._BANK_GROUP_INTAKE > t._BANK_GROUP_OTHER

    def test_group_sets_do_not_overlap(self):
        """Тип в двух группах — молчаливая зависимость от порядка обхода."""
        t = self._mod()
        seen: set[str] = set()
        for types in t._BANK_GROUP_ORDER.values():
            dup = seen & types
            assert not dup, f"тип в двух группах: {sorted(dup)}"
            seen |= types

    def test_hearing_and_writ_families_are_complete(self):
        """Новый `fi_hearing_*`/`fi_writ_*`, забытый в наборах, упал бы в
        «иные» и печатался бы среди сроков и жалоб. Проверяем по именам:
        типы без этих префиксов (fi_post_decision_hearing,
        fi_default_cancellation_hearing) — осознанные решения, не опечатки."""
        t = self._mod()
        known = set(t._BANK_TYPE_LABELS) | {
            x for types in t._BANK_GROUP_ORDER.values() for x in types}
        for typ in sorted(known):
            if typ.startswith("fi_hearing_"):
                assert t._bank_change_group({"type": [typ]}) == \
                    t._BANK_GROUP_HEARINGS, f"{typ} не в группе заседаний"
            elif typ.startswith("fi_writ_"):
                assert t._bank_change_group({"type": [typ]}) == \
                    t._BANK_GROUP_WRITS, f"{typ} не в группе листов"


# ── Календарный проход collect_bank_calendar_events ──────────────────────────

class TestBankCalendarEvents:
    """Юнит календарного прохода: «вступило в силу» и «ИЛ завис» наступают
    датой, а не карточкой — решённые дела живут в недельном ритме и в
    FI-цикле change не собирают."""

    TODAY = date(2026, 8, 13)

    def _case(self, **fi_extra) -> dict:
        fi = {
            "case_number": "2-100/2026",
            "court": "Сургутский городской суд",
            "court_domain": "surggor--hmao.sudrf.ru",
            "link": "111|aaaa-1111",
            "status": "Решено",
            "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
            "decision_date": "05.06.2026",
            # est = месяц от мотивировки + 1 день = 11.07.2026 (33 дн назад)
            "act_date": "10.06.2026",
        }
        fi.update(fi_extra)
        return _track_case(**fi)

    def _run(self, cases, monkeypatch, *, fi_changes=None,
             epoch=date(2026, 1, 1), today=None):
        from court_monitor.runs import collect_bank_calendar_events
        monkeypatch.setattr(config, "BANK_CALENDAR_EVENTS_SINCE", epoch)
        # Архивные окна считаются от РЕАЛЬНЫХ часов — фиксируем, чтобы тест
        # не флейкал, когда est уедет за 180-дневный потолок (своя логика
        # архивов покрыта TestBankTrackArchive).
        monkeypatch.setattr(lifecycle, "is_case_archived", lambda c: False)
        ch = fi_changes if fi_changes is not None else []
        n = collect_bank_calendar_events(cases, ch, today or self.TODAY)
        return n, ch

    def test_both_events_emitted(self, monkeypatch):
        case = self._case()
        n, ch = self._run([case], monkeypatch)
        assert n == 1 and len(ch) == 1
        assert ch[0]["type"] == ["fi_legal_force_reached", "fi_writ_overdue"]
        assert ch[0]["track"] == "plaintiff_light"
        assert ch[0]["case"] == "2-100/2026"
        d = ch[0]["details"]
        assert d["legal_force_date"] == "11.07.2026"
        assert d["overdue_days"] == 33
        assert d["court_domain"] == "surggor--hmao.sudrf.ru"
        assert d["link"] == "111|aaaa-1111"
        fi = case["first_instance"]
        assert fi["legal_force_emitted"] == "2026-07-11"
        # Маркер составной с 21.08.2026: «est|достигнутый порог».
        assert fi["writ_overdue_emitted"] == "2026-07-11|30"

    def test_force_only_before_threshold(self, monkeypatch):
        case = self._case(act_date="05.07.2026")  # est = 06.08, 7 дн назад
        n, ch = self._run([case], monkeypatch)
        assert ch[0]["type"] == ["fi_legal_force_reached"]
        assert ch[0]["details"]["legal_force_date"] == "06.08.2026"
        assert "writ_overdue_emitted" not in case["first_instance"]

    def test_future_est_silent(self, monkeypatch):
        case = self._case(act_date="20.07.2026")  # est = 21.08.2026
        assert self._run([case], monkeypatch) == (0, [])
        assert "legal_force_emitted" not in case["first_instance"]

    def test_backlog_before_epoch_marked_quietly(self, monkeypatch):
        """Решение юриста 13.08.2026: бэклог деплоя (64 «в силе» /
        14 «просрочено») не объявлять — маркеры ставятся тихо, дальше
        объявляются только новые наступления."""
        case = self._case()  # est 11.07.2026 — до эпохи 13.08.2026
        n, ch = self._run([case], monkeypatch, epoch=date(2026, 8, 13))
        assert (n, ch) == (0, [])
        fi = case["first_instance"]
        assert fi["legal_force_emitted"] == "2026-07-11"
        # Маркер составной с 21.08.2026: «est|достигнутый порог».
        assert fi["writ_overdue_emitted"] == "2026-07-11|30"

    def test_overdue_crossing_after_epoch_alerts(self, monkeypatch):
        """est до эпохи, но 30-дневный порог пересечён ПОСЛЕ неё — свежая
        просрочка, алерт обязан выйти (иначе дела, вступившие в силу
        накануне деплоя, зависали бы молча)."""
        case = self._case(act_date="05.07.2026")  # est 06.08, порог 05.09
        n, ch = self._run([case], monkeypatch, epoch=date(2026, 8, 13),
                          today=date(2026, 9, 10))
        assert len(ch) == 1
        assert ch[0]["type"] == ["fi_writ_overdue"]
        assert ch[0]["details"]["overdue_days"] == 35

    def test_not_resolved_gate(self, monkeypatch):
        """est умеет посчитаться от дрейфующей hearing_date и у нерешённого
        дела — ложное «в силе» недопустимо."""
        case = self._case(status="В производстве")
        assert self._run([case], monkeypatch) == (0, [])

    def test_left_track_gate(self, monkeypatch):
        case = self._case(appeal_filed=True)
        assert self._run([case], monkeypatch) == (0, [])

    def test_cassation_gate(self, monkeypatch):
        case = self._case(cassation_filed=True)
        assert self._run([case], monkeypatch) == (0, [])

    def test_denied_gate(self, monkeypatch):
        """В иске отказано — листа не будет (bank_writ_expected)."""
        case = self._case(result="В удовлетворении иска ОТКАЗАНО")
        assert self._run([case], monkeypatch) == (0, [])

    def test_refusal_to_accept_gate(self, monkeypatch):
        """Отказ в принятии — тот же гейт (разгон 14.08.2026, 9-125/2026).

        До фикса такое дело числилось ждущим ИЛ, и календарный проход слал бы
        по нему «✅ решение вступило в силу» и «⚠️ ИЛ не выдан N дн.» —
        события про лист, которого не будет.
        """
        case = self._case(result="ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ "
                                 "ПОДЛЕЖИТ РАССМОТРЕНИЮ")
        assert self._run([case], monkeypatch) == (0, [])

    def test_enforcement_writ_gate(self, monkeypatch):
        """Лист на исполнение уже выдан — цепочка ожидания закрыта."""
        case = self._case(writs=[{"issue_date": "01.08.2026",
                                  "status": "Выдан"}])
        assert self._run([case], monkeypatch) == (0, [])

    def test_interim_writ_does_not_gate(self, monkeypatch):
        """Обеспечительный лист (до решения) ожидание ИЛ не закрывает."""
        case = self._case(writs=[{"issue_date": "01.05.2026",
                                  "status": "Выдан"}])
        n, ch = self._run([case], monkeypatch)
        assert n == 1
        assert "fi_legal_force_reached" in ch[0]["type"]

    def test_archived_case_silent(self, monkeypatch):
        from court_monitor.runs import collect_bank_calendar_events
        monkeypatch.setattr(config, "BANK_CALENDAR_EVENTS_SINCE",
                            date(2026, 1, 1))
        monkeypatch.setattr(lifecycle, "is_case_archived", lambda c: True)
        ch: list = []
        assert collect_bank_calendar_events([self._case()], ch,
                                            self.TODAY) == 0
        assert ch == []

    def test_non_track_ignored(self, monkeypatch):
        case = self._case()
        case.pop("track")
        assert self._run([case], monkeypatch) == (0, [])

    def test_idempotent_second_run_silent(self, monkeypatch):
        case = self._case()
        self._run([case], monkeypatch)
        assert self._run([case], monkeypatch) == (0, [])

    # ── Эскалация напоминаний об ИЛ (решение юриста 21.08.2026) ──────────
    # До неё «⚠️ ИЛ не выдан N дн.» приходило РОВНО ОДИН раз — на 30-й день,
    # и дело, зависшее без листа, молчало до архива на 180-й. На 21.08.2026
    # так молчали 50 дел обеих территорий, рекорд — 171 день ожидания.

    def test_second_threshold_reminds_again(self, monkeypatch):
        """На 60-м дне напоминание повторяется, между порогами — тишина."""
        case = self._case()
        self._run([case], monkeypatch)          # 33 дн. — первый порог
        fi = case["first_instance"]
        assert fi["writ_overdue_emitted"] == "2026-07-11|30"
        # 50-й день: следующий порог не достигнут.
        assert self._run([case], monkeypatch,
                         today=date(2026, 8, 30)) == (0, [])
        # 60-й день: вторая строка, с ФАКТИЧЕСКИМ числом дней.
        n, ch = self._run([case], monkeypatch, today=date(2026, 9, 9))
        assert n == 1 and ch[0]["type"] == ["fi_writ_overdue"]
        assert ch[0]["details"]["overdue_days"] == 60
        assert fi["writ_overdue_emitted"] == "2026-07-11|60"
        # 61-й день — снова тишина до 90-го.
        assert self._run([case], monkeypatch,
                         today=date(2026, 9, 10)) == (0, [])

    def test_legacy_marker_counts_as_first_threshold(self, monkeypatch):
        """Старый маркер без «|» — это «первый порог объявлен».

        Миграции нет намеренно: значение «2026-07-11» и означало ровно то,
        что 30-дневное напоминание уже ушло. Дело не должно объявиться
        повторно на 40-м дне только из-за смены формата.
        """
        case = self._case()
        # В боевых данных маркеры стоят парой (оба пишутся одним проходом) —
        # иначе тест поймал бы «вступило в силу», а не просрочку.
        case["first_instance"]["legal_force_emitted"] = "2026-07-11"
        case["first_instance"]["writ_overdue_emitted"] = "2026-07-11"
        assert self._run([case], monkeypatch,
                         today=date(2026, 8, 20)) == (0, [])
        # А на 60-м — законно напомнит.
        n, ch = self._run([case], monkeypatch, today=date(2026, 9, 9))
        assert n == 1 and ch[0]["type"] == ["fi_writ_overdue"]

    def test_epoch_gates_only_first_threshold(self, monkeypatch):
        """Эпоха гасит бэклог на ПЕРВОМ пороге, но не хоронит дело навсегда.

        Она защищала от паводка при вводе фичи 13.08.2026. Останься она на
        повторных порогах — дело, вступившее в силу задолго до эпохи, не
        напомнило бы уже никогда: у 2-28/2026 Урала (171 день) следующий
        порог 180 совпадает с архивацией.
        """
        case = self._case()          # est 11.07.2026, эпоха 13.08.2026
        assert self._run([case], monkeypatch,
                         epoch=date(2026, 8, 13)) == (0, [])
        n, ch = self._run([case], monkeypatch, epoch=date(2026, 8, 13),
                          today=date(2026, 9, 9))
        assert n == 1 and ch[0]["type"] == ["fi_writ_overdue"]

    def test_repeat_days_zero_restores_single_shot(self, monkeypatch):
        """BANK_WRIT_OVERDUE_REPEAT_DAYS=0 — прежнее «один раз за жизнь»."""
        monkeypatch.setattr(config, "BANK_WRIT_OVERDUE_REPEAT_DAYS", 0)
        case = self._case()
        self._run([case], monkeypatch)
        for day in (date(2026, 9, 9), date(2026, 10, 9), date(2026, 11, 9)):
            assert self._run([case], monkeypatch, today=day) == (0, [])

    def test_no_reminder_past_archive_ceiling(self, monkeypatch):
        """Выше архивного потолка порогов нет — дело уже уходит из трека."""
        assert max(config.writ_overdue_steps()) < config.BANK_WRIT_WAIT_MAX_DAYS

    def test_est_shift_restarts_ladder(self, monkeypatch):
        """Сдвиг est обнуляет лестницу: сроки считаются от новой даты."""
        case = self._case()
        self._run([case], monkeypatch, today=date(2026, 9, 9))  # порог 60
        assert case["first_instance"]["writ_overdue_emitted"].endswith("|60")
        case["first_instance"]["act_date"] = "01.07.2026"       # est → 04.08
        n, ch = self._run([case], monkeypatch, today=date(2026, 9, 9))
        assert n == 1 and ch[0]["details"]["overdue_days"] == 36
        assert case["first_instance"]["writ_overdue_emitted"] == "2026-08-04|30"

    def test_est_shift_reemits_with_new_date(self, monkeypatch):
        """Сдвиг расчётной даты (поздняя мотивировка, вручение копии)
        переобъявляет событие с новой датой — идемпотентность ЗНАЧЕНИЕМ."""
        case = self._case()
        self._run([case], monkeypatch)
        # Месяц от 01.07 кончается 01.08 (суббота) → последний день срока
        # сдвигается на рабочий понедельник 03.08 (ст. 108 ГПК), сила — 04.08.
        case["first_instance"]["act_date"] = "01.07.2026"
        n, ch = self._run([case], monkeypatch)
        assert ch[0]["type"] == ["fi_legal_force_reached"]
        assert ch[0]["details"]["legal_force_date"] == "04.08.2026"
        assert case["first_instance"]["legal_force_emitted"] == "2026-08-04"

    def test_merges_into_existing_track_change(self, monkeypatch):
        """Дело парсилось этим прогоном (недельный ритм) — календарные типы
        дописываются в его запись: секция держит одну строку на дело."""
        case = self._case()
        existing = _bank_change(["fi_status_change"])
        n, ch = self._run([case], monkeypatch, fi_changes=[existing])
        assert n == 1 and len(ch) == 1
        assert ch[0] is existing
        assert ch[0]["type"] == ["fi_status_change", "fi_legal_force_reached",
                                 "fi_writ_overdue"]
        assert ch[0]["details"]["legal_force_date"] == "11.07.2026"

    def test_migrate_stages_does_not_seed_calendar_markers(self):
        """Ловушка порядка: migrate_stages идёт на загрузке РАНЬШЕ прохода —
        вечный посев «est наступила» глушил бы события в день наступления
        даты (данные не меняются, меняется календарь). Анти-паводок живёт
        в эпохе BANK_CALENDAR_EVENTS_SINCE, не в посеве."""
        case = self._case()
        lifecycle.migrate_stages([case])
        assert "legal_force_emitted" not in case["first_instance"]
        assert "writ_overdue_emitted" not in case["first_instance"]


# ── Вручение копии заочного решения ──────────────────────────────────────────

class TestDefaultCopyServedSeeding:
    """Анти-паводок fi_default_copy_served: migrate_stages засевает эмит-флаг
    делам, где вручение уже в истории событий, — зеркало посева возврата
    копии (TestDefaultCopyReturnedSeeding)."""

    def _case(self) -> dict:
        return {
            "id": "2-616/2026",
            "current_stage": "first_instance",
            "track": "plaintiff_light",
            "bank_role": "Истец",
            "first_instance": {
                "case_number": "2-616/2026",
                "status": "Решено",
                "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
                "hearing_date": "03.06.2026",
                "events": [
                    {"date": "03.06.2026",
                     "text": "Судебное заседание. 11:00. Вынесено заочное "
                             "решение по делу. Иск УДОВЛЕТВОРЕН"},
                    {"date": "10.06.2026",
                     "text": "Копия заочного решения ответчику (истцу) "
                             "вручена. 16:25. 23.06.2026"},
                ],
            },
        }

    def test_seeded_with_event_date(self):
        case = self._case()
        lifecycle.migrate_stages([case])
        assert (case["first_instance"]["default_copy_served_emitted"]
                == "10.06.2026")

    def test_existing_flag_untouched(self):
        case = self._case()
        case["first_instance"]["default_copy_served_emitted"] = "01.01.2026"
        lifecycle.migrate_stages([case])
        assert (case["first_instance"]["default_copy_served_emitted"]
                == "01.01.2026")

    def test_no_serving_not_seeded(self):
        case = self._case()
        case["first_instance"]["events"] = case["first_instance"]["events"][:1]
        lifecycle.migrate_stages([case])
        assert "default_copy_served_emitted" not in case["first_instance"]


# ── Проводка календарных событий и новых веток FI-цикла ──────────────────────

class TestBankCalendarWiring:
    """Инварианты порядка невидимы рендер-тестам — unit на исходник, по
    образцу TestVacatedDefaultWiring."""

    @staticmethod
    def _runs_src() -> str:
        path = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_pass_called_between_intake_insert_and_routine_filter(self):
        """Проход стоит ПОСЛЕ врезки новых исков (слияние строк по делу) и
        ДО фильтра рутины/save_digest_context — replay видит события."""
        src = self._runs_src()
        i_intake = src.index("for _bc in bank_new_cases:")
        i_call = src.index(
            "collect_bank_calendar_events(cases, fi_changes, today)")
        i_routine = src.index("lifecycle.filter_bank_routine_events(fi_changes)")
        assert i_intake < i_call < i_routine

    def test_pass_called_before_bank_new_cases_merged(self):
        """Свежезаведённое авто-подхватом дело проход видеть не должен:
        его старую историю отсеивают эпоха и стародатный фильтр, а не
        порядок, но порядок — первый рубеж."""
        src = self._runs_src()
        i_call = src.index(
            "collect_bank_calendar_events(cases, fi_changes, today)")
        i_merge = src.index("cases = bank_new_cases + cases")
        assert i_call < i_merge

    def test_post_decision_branch_after_hearing_block_and_gated(self):
        """Ветка пост-решенческого заседания: после hearing-блока, ОБА трека
        (track-гейт снят 13.08.2026 — в основной картотеке это расходы и
        индексация ПРОТИВ банка), только при подтверждающем session-событии,
        идемпотентна значением даты, не дублирует заседание по отмене
        заочного."""
        src = self._runs_src()
        i_hearing = src.index("# Новое/перенесённое заседание")
        i_branch = src.index("Заседание по РЕШЁННОМУ делу")
        i_promo = src.index("# Промоушен материала М→2")
        assert i_hearing < i_branch < i_promo
        branch = src[i_branch:i_promo]
        assert "case_decided and new_hearing_date" in branch
        assert "is_bank_plaintiff_track(case_j)" not in branch, (
            "Track-гейт вернулся: основной трек снова ослеп к заседаниям "
            "по решённым делам (судебные расходы/индексация против банка)."
        )
        assert "_SESSION_START_RX.search" in branch
        assert ('fi.get("post_decision_hearing_emitted") != new_hearing_date'
                in branch)
        assert "fi_default_cancellation_hearing" in branch

    def test_copy_served_emit_idempotent_by_value(self):
        src = self._runs_src()
        assert 'fi.get("default_copy_served_emitted") != _served' in src
        assert 'fi["default_copy_served_emitted"] = _served' in src


# ── Миграция: дата проверки делам, заведённым до появления штампа ───────────

class TestIntakeCheckedStampMigration:
    """Гейты нарочно избыточны: проштамповать запись, чью карточку никто не
    читал, значит ослепить прогон на 21 день (страховка force-parse)."""

    @staticmethod
    def _case(**over) -> dict:
        case = _track_case(
            events=[{"date": "20.09.2026", "text": "Судебное заседание. 10:00"}]
        )
        case["import"] = {"operator": "оператор", "at": "2026-08-14T05:53:09",
                          "source": "dump", "announced": True}
        case.update(over)
        return case

    def test_stamp_taken_from_import_date(self):
        """Дата ввода, а не сегодняшняя: сегодняшняя дала бы делу лишнюю
        неделю тишины."""
        case = self._case()
        assert lifecycle.migrate_intake_checked_stamp([case]) == 1
        assert case["first_instance"]["last_checked_at"] == "2026-08-14"
        assert case["first_instance"]["intake_card_parse"] is True

    def test_idempotent(self):
        case = self._case()
        lifecycle.migrate_intake_checked_stamp([case])
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0

    def test_existing_stamp_not_overwritten(self):
        """У воскрешённых из архива штамп сохранён и может быть старым —
        перетирать нельзя, там ждёт force-parse по 21 дню."""
        case = self._case()
        case["first_instance"]["last_checked_at"] = "2026-07-01"
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0
        assert case["first_instance"]["last_checked_at"] == "2026-07-01"

    def test_empty_events_refused(self):
        """Непустые events — единственное доказательство, что карточка
        читалась (основная картотека заводит дела со СТРОКИ выдачи)."""
        case = self._case()
        case["first_instance"]["events"] = []
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0
        assert "last_checked_at" not in case["first_instance"]

    def test_no_import_block_refused(self):
        case = self._case()
        case.pop("import")
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0

    def test_non_track_case_refused(self):
        case = self._case()
        case.pop("track")
        case["bank_role"] = "Ответчик"
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0

    def test_broken_import_date_refused(self):
        case = self._case()
        case["import"]["at"] = "вчера"
        assert lifecycle.migrate_intake_checked_stamp([case]) == 0

    def test_migrated_case_is_skipped_by_future_hearing(self):
        """Итог миграции — то же поведение, что у свежезаведённого дела."""
        case = self._case()
        lifecycle.migrate_intake_checked_stamp([case])
        skip, reason = lifecycle.should_skip_case(case, date(2026, 8, 17))
        assert skip is True
        assert reason.startswith("future_hearing")

    def test_called_from_migrate_stages(self):
        """Проводка: без вызова из migrate_stages миграция не отработает ни
        на одной территории."""
        case = self._case()
        lifecycle.migrate_stages([case])
        assert case["first_instance"]["last_checked_at"] == "2026-08-14"


# ── Drawer 03.09.2026: «в силе» только у решённого, «без рассмотрения» ───────

class TestLegalForceOnlyWhenDecided:
    """Разбор drawer'а юристом 03.09.2026: «Вступило в силу (расч.)» стояло у
    342 из 554 активных исков банка «В производстве» — est считался от
    БУДУЩЕГО заседания (hearing_date = последнее session-событие карточки,
    суд публикует назначение заранее). Гейт живёт в bank_legal_force_est,
    штампы split_bank_track (legal_force_est, writ_awaited_since) следуют
    за ним."""

    @staticmethod
    def _dmy(days_from_now: int) -> str:
        return (datetime.now() + timedelta(days=days_from_now)).strftime("%d.%m.%Y")

    def test_pending_case_gets_no_stamps(self):
        from court_monitor.runs import split_bank_track
        live = _track_case(
            status="В производстве", hearing_date=self._dmy(27),
            last_event="Подготовка дела (собеседование). 08:50. Зал 505.",
            legal_force_est="2026-11-17",  # штамп прошлых прогонов
            writ_awaited_since="2026-11-17",
        )
        _, bank_active, _, _ = split_bank_track([live])
        assert bank_active == [live]
        fi = live["first_instance"]
        assert "legal_force_est" not in fi
        assert "writ_awaited_since" not in fi
        # «Ждём лист» как факт остаётся (writ_expected не False): очередь
        # начнётся после решения.
        assert "writ_expected" not in fi

    def test_decided_case_keeps_stamps(self):
        from court_monitor.runs import split_bank_track
        done = _track_case(status="Решено", hearing_date=self._dmy(-40),
                           result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН")
        _, bank_active, _, _ = split_bank_track([done])
        fi = done["first_instance"]
        assert fi["legal_force_est"]
        assert fi["writ_awaited_since"] == fi["legal_force_est"]

    def test_archive_ceiling_for_decided_survives(self):
        """Потолок «Решено без ИЛ» по-прежнему считается от est (архивная
        ветка стоит под status == «Решено» — гейт её не задевает)."""
        case = _track_case(status="Решено", hearing_date=self._dmy(-400),
                           result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН")
        assert lifecycle.is_case_archived(case) is True
        live = _track_case(status="В производстве", hearing_date=self._dmy(-400))
        assert lifecycle.is_case_archived(live) is False


class TestLeftUnconsidered:
    """«Оставлено без рассмотрения» (ст. 222 ГПК) = листа не будет (решение
    юриста 03.09.2026). 9 дел трека ХМАО числились «ждущими ИЛ» и получали
    расчётное «вступило в силу»; статус карточки у них «В производстве»."""

    @staticmethod
    def _dmy(days_ago: int) -> str:
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    RESULT = "Иск (заявление, жалоба) ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ"

    def test_predicate_reads_result_only(self):
        assert lifecycle.fi_left_unconsidered({"result": self.RESULT}) is True
        assert lifecycle.fi_left_unconsidered(
            {"result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН"}) is False
        # История движения не читается: определение прошлого круга после
        # отмены по ст. 223 ч. 3 живёт в events навсегда.
        assert lifecycle.fi_left_unconsidered({
            "result": "",
            "events": [{"date": "01.06.2026",
                        "text": "Иск оставлен без рассмотрения"}],
            "last_event": "Иск оставлен без рассмотрения",
        }) is False
        assert lifecycle.fi_left_unconsidered({}) is False

    def test_no_writ_expected(self):
        from court_monitor.runs import split_bank_track
        case = _track_case(status="В производстве", hearing_date=self._dmy(20),
                           result=self.RESULT, legal_force_est="2026-10-09")
        fi = case["first_instance"]
        assert lifecycle.bank_writ_expected(fi) is False
        _, bank_active, _, _ = split_bank_track([case])
        assert fi["writ_expected"] is False
        assert "legal_force_est" not in fi

    def test_archived_after_window_despite_live_status(self):
        """Карточка держит «В производстве» (как у присоединения) — без
        оговорки в архивной ветке дело застряло бы в активных навсегда."""
        fresh = _track_case(status="В производстве", hearing_date=self._dmy(10),
                            result=self.RESULT)
        assert lifecycle.is_case_archived(fresh) is False
        old = _track_case(status="В производстве", hearing_date=self._dmy(40),
                          result=self.RESULT)
        assert lifecycle.is_case_archived(old) is True
        # Жалоба держит в активных при любом сроке.
        old["first_instance"]["appeal_filed"] = True
        assert lifecycle.is_case_archived(old) is False


class TestDecisionDateFrozenFromEvent:
    """Заморозка decision_date — от события «Вынесено (заочное) решение», а не
    от дрейфующей hearing_date: при недельном ритме трека эмит бывает ПОЗЖЕ
    назначения пост-решенческого заседания (расходы, индексация)."""

    def test_backfill_prefers_decision_event(self):
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Решено", "hearing_date": "15.08.2026",
                    "events": [
                        {"date": "01.07.2026",
                         "text": "Судебное заседание. 11:00. Вынесено решение по делу"},
                        {"date": "15.08.2026",
                         "text": "Судебное заседание. 10:00. Заявление о взыскании судебных расходов"},
                    ]}}
        lifecycle.migrate_stages([case])
        assert case["first_instance"]["decision_date"] == "01.07.2026"

    def test_backfill_falls_back_to_hearing_date(self):
        case = {"current_stage": "first_instance",
                "first_instance": {"status": "Решено", "hearing_date": "30.04.2026"}}
        lifecycle.migrate_stages([case])
        assert case["first_instance"]["decision_date"] == "30.04.2026"

    def test_emit_wired_to_decision_event(self):
        """Эмит fi_resolved (FI-цикл main_json, юнитом не достаётся) морозит
        дату тем же источником и кладёт в details ЗАМОРОЖЕННОЕ значение."""
        import inspect
        from court_monitor import runs
        src = inspect.getsource(runs)
        assert ('fi.setdefault(\n'
                '                    "decision_date",\n'
                '                    lifecycle.fi_decision_date_from_events(fi.get("events") or [])\n'
                '                    or fi.get("hearing_date", ""))') in src
        assert 'change["details"]["decision_date"] = fi.get("decision_date", "")' in src
        assert 'change["details"]["decision_date"] = fi.get("hearing_date", "")' not in src

    def test_left_track_drops_queue_stamps(self):
        """Дело с жалобой уезжает в основную картотеку БЕЗ штампов очереди
        ИЛ: там их никто не пересчитывает, а решение с жалобой в силу не
        вступило — drawer печатал «Вступило в силу (расч.)» у дела в апелляции."""
        from court_monitor.runs import split_bank_track
        left = _track_case(status="Решено", hearing_date="01.07.2026",
                           decision_date="01.07.2026",
                           result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
                           appeal_filed=True, appeal_filed_date="28.07.2026",
                           legal_force_est="2026-08-18",
                           writ_awaited_since="2026-08-18")
        rest, bank_active, _, moved = split_bank_track([left])
        assert rest == [left] and bank_active == [] and moved == 1
        fi = left["first_instance"]
        assert "legal_force_est" not in fi
        assert "writ_awaited_since" not in fi
