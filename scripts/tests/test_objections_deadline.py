"""
Срок для представления возражений на апел. жалобу (ст. 325 ГПК).

Строка «Установлен срок для предоставления возражений · Срок до ДД.ММ.ГГГГ»
приходит НЕ из «Движения дела», а из вкладки карточки «Обжалование решений,
определений» → вложенная таблица «Движение жалобы», и живёт в
first_instance.appeal_events. До 03.08.2026 она была сырым текстом в ленте
drawer'а: ни даты-дедлайна, ни строки в «Ключевых датах», ни события дайджеста.

Что охраняем:
1. Разбор обеих форм строки: с разобранными колонками (name + note) и
   legacy-склейкой (только date + text). На 03.08.2026 в корпусе 17 строк,
   из них 10 — legacy.
2. «Срок для устранения недостатков» (оставление жалобы без движения) в
   дедлайн возражений НЕ попадает: это срок противоположной полярности —
   недостатки устраняет тот, кто подал жалобу.
3. Штамп самоисцеляется: срок исчез из событий → поле снимается.
4. Анти-паводок. Все 17 сроков корпуса истекли; первый прогон после деплоя
   обязан промолчать. Но посев маркера в migrate_stages не должен глушить
   ЖИВОЙ срок — иначе дело, заведённое авто-подхватом, потеряет дедлайн
   навсегда (эмит-блок к тому моменту ещё не отрабатывал).
5. Проводка: эмит стоит ПОСЛЕ слияния appeal_events, иначе в день появления
   срока событие не выстрелит.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from court_monitor.lifecycle import (  # noqa: E402
    appeal_objections_deadline, stamp_objections_deadline, migrate_stages,
    _FI_DATED_COMPLAINT_TYPES,
)

RUNS_PY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "court_monitor", "runs.py",
)


def _ev(text, **extra):
    row = {"date": "28.07.2026", "text": text}
    row.update(extra)
    return row


# ===== 1. Разбор строки =====

class TestExtraction:
    def test_columns_form(self):
        """Карточка с разобранными колонками: name + note."""
        fi = {"appeal_events": [_ev(
            "Установлен срок для предоставления возражений · Срок до 18.08.2026",
            name="Установлен срок для предоставления возражений",
            note="Срок до 18.08.2026", posted_at="28.07.2026")]}
        assert appeal_objections_deadline(fi) == ("2026-07-28", "2026-08-18")

    def test_legacy_glued_form(self):
        """10 из 17 записей корпуса идут без колонок — только date + text."""
        fi = {"appeal_events": [_ev(
            "Установлен срок для предоставления возражений · Срок до 18.08.2026")]}
        assert appeal_objections_deadline(fi) == ("2026-07-28", "2026-08-18")

    def test_prolongation_wins(self):
        """Суд продлил срок: прежняя строка из карточки никуда не девается,
        побеждать обязана более поздняя дата, а не порядок в списке."""
        fi = {"appeal_events": [
            _ev("Установлен срок для предоставления возражений · Срок до 18.08.2026"),
            {"date": "10.08.2026",
             "text": "Установлен срок для предоставления возражений · Срок до 01.09.2026"},
        ]}
        assert appeal_objections_deadline(fi) == ("2026-08-10", "2026-09-01")

    def test_defects_term_is_not_objections(self):
        """Срок на устранение недостатков — противоположная полярность:
        устраняет тот, кто подал жалобу, а не тот, кто возражает."""
        fi = {"appeal_events": [_ev(
            "Оставление жалобы (представления) без движения · "
            "Срок для устранения недостатков до 25.12.2025")]}
        assert appeal_objections_deadline(fi) is None

    def test_other_rows_ignored(self):
        fi = {"appeal_events": [
            _ev("Регистрация жалобы (представления) в суде"),
            _ev("Направлено в вышестоящую инстанцию"),
        ]}
        assert appeal_objections_deadline(fi) is None

    def test_no_events(self):
        assert appeal_objections_deadline({}) is None
        assert appeal_objections_deadline({"appeal_events": []}) is None

    def test_row_without_date_skipped(self):
        """Строка есть, а даты в ней нет — не выдумываем."""
        fi = {"appeal_events": [_ev(
            "Установлен срок для предоставления возражений")]}
        assert appeal_objections_deadline(fi) is None


# ===== 2. Штамп =====

class TestStamp:
    def test_stamp_sets_both_fields(self):
        fi = {"appeal_events": [_ev(
            "Установлен срок для предоставления возражений · Срок до 18.08.2026")]}
        assert stamp_objections_deadline(fi) == "2026-08-18"
        assert fi["objections_due"] == "2026-08-18"
        assert fi["objections_set_at"] == "2026-07-28"

    def test_stamp_is_self_healing(self):
        """Суд убрал ошибочную строку — фантомный срок не должен пережить
        перепарс (тот же принцип, что writ_expected / legal_force_est)."""
        fi = {"objections_due": "2026-08-18", "objections_set_at": "2026-07-28",
              "appeal_events": []}
        assert stamp_objections_deadline(fi) == ""
        assert "objections_due" not in fi
        assert "objections_set_at" not in fi

    def test_stamp_is_idempotent(self):
        fi = {"appeal_events": [_ev(
            "Установлен срок для предоставления возражений · Срок до 18.08.2026")]}
        first = stamp_objections_deadline(fi)
        snapshot = dict(fi)
        assert stamp_objections_deadline(fi) == first
        assert fi == snapshot


# ===== 3. Анти-паводок в migrate_stages =====

class TestAntiFlood:
    @staticmethod
    def _case(due_ddmmyyyy):
        return {"id": "2-1/2026", "current_stage": "awaiting_appeal",
                "first_instance": {"appeal_events": [
                    {"date": "01.01.2026",
                     "text": "Установлен срок для предоставления возражений · "
                             f"Срок до {due_ddmmyyyy}"}]}}

    def test_expired_deadline_is_seeded_silently(self):
        """Все 17 сроков корпуса истекли: маркер засевается, дайджест молчит."""
        c = self._case("19.01.2026")
        migrate_stages([c])
        fi = c["first_instance"]
        assert fi["objections_due"] == "2026-01-19"
        assert fi["objections_emitted"] == "2026-01-19", (
            "Истёкший срок обязан быть засеян маркером — иначе первый прогон "
            "после деплоя объявит задним числом всю историю корпуса."
        )

    def test_live_deadline_is_not_seeded(self):
        """⚠️ Главный инвариант. Дело, заведённое авто-подхватом с живым сроком,
        эмит-блок ещё не проходило — безусловный посев закрыл бы дедлайн
        навсегда, то есть убил бы ровно то, ради чего правка делалась."""
        due = date.today() + timedelta(days=15)
        c = self._case(due.strftime("%d.%m.%Y"))
        migrate_stages([c])
        fi = c["first_instance"]
        assert fi["objections_due"] == due.isoformat()
        assert "objections_emitted" not in fi, (
            "Живой срок засеян маркером — дайджест никогда его не объявит."
        )

    def test_existing_marker_not_overwritten(self):
        c = self._case("19.01.2026")
        c["first_instance"]["objections_emitted"] = "2025-12-01"
        migrate_stages([c])
        assert c["first_instance"]["objections_emitted"] == "2025-12-01"


# ===== 4. Фильтры и проводка =====

class TestWiring:
    def test_stale_anchor_is_the_deadline_itself(self):
        """Якорь — САМ срок, а не дата установления: срок в будущем даёт
        отрицательный возраст и штатный дедлайн фильтр не тронет, а давно
        просроченный не проскочит."""
        assert _FI_DATED_COMPLAINT_TYPES["fi_objections_deadline_set"] == \
            "objections_due"

    def test_emit_follows_appeal_events_merge(self):
        """Порядок load-bearing: эмит читает УЖЕ обновлённый appeal_events.
        Встань он выше — в день появления срока событие не выстрелит."""
        src = open(RUNS_PY, encoding="utf-8").read()
        merge = src.index('("_fi_appeal_events", "appeal_events")')
        emit = src.index('change["type"].append("fi_objections_deadline_set")')
        assert merge < emit, (
            "Блок эмита срока возражений уехал ВЫШЕ слияния appeal_events — "
            "срок будет объявляться с опозданием на прогон."
        )

    def test_emit_is_gated_by_running_deadline(self):
        src = open(RUNS_PY, encoding="utf-8").read()
        блок = src[src.index("objections_due = stamp_objections_deadline(fi)"):
                   src.index('change["type"].append("fi_objections_deadline_set")')]
        assert ">= today" in блок, (
            "Пропал гейт «срок ещё идёт» — дайджест начнёт объявлять истёкшие "
            "сроки, в том числе всю историю только что заведённого дела."
        )

    def test_marker_compared_by_value(self):
        """Идемпотентность ЗНАЧЕНИЕМ, как default_cancel_*_emitted: продление
        срока обязано объявиться снова, рутинный перепарс — нет."""
        src = open(RUNS_PY, encoding="utf-8").read()
        assert 'objections_due != fi.get("objections_emitted")' in src

    def test_type_labelled_everywhere(self):
        """Неизвестный тип даёт голую строку дела, но всё равно считается
        в счётчике «Изменения (N)»."""
        base = os.path.join(os.path.dirname(RUNS_PY), "digest")
        for имя in ("template.py", "core.py"):
            src = open(os.path.join(base, имя), encoding="utf-8").read()
            assert "fi_objections_deadline_set" in src, (
                f"Тип не подписан в digest/{имя}."
            )
        tpl = open(os.path.join(base, "template.py"), encoding="utf-8").read()
        assert re.search(
            r'"fi_objections_deadline_set":\s*"[^"]*возражени', tpl), (
            "Тип отсутствует в _BANK_TYPE_LABELS — в секции «Иски банка» дело "
            "выведется голой строкой без причины."
        )


# ===== 5. Перенос движения жалобы авто-подхватом =====

class TestIntakeCarriesAppealMovement:
    def test_appeal_events_and_appellant_carried(self):
        """Регрессия 2-339/2026: дело зашло с флагом жалобы, но без движения
        жалобы и апеллянта — drawer обрывал хронологию на «дело сдано в отдел
        судебного делопроизводства», а срок возражений считать было не из чего."""
        from court_monitor.bank_intake import make_bank_entry
        card = {
            "Статус": "Решено",
            "Результат": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН ЧАСТИЧНО",
            "_fi_appeal_filed": True, "_fi_appeal_filed_date": "28.07.2026",
            "_fi_appellant_raw": "ОТВЕТЧИК",
            "_fi_appeal_events": [
                {"date": "28.07.2026",
                 "text": "Регистрация жалобы (представления) в суде"},
                {"date": "28.07.2026",
                 "text": "Установлен срок для предоставления возражений · "
                         "Срок до 18.08.2026"},
            ],
            "_events": [{"date": "01.07.2026",
                         "text": "Судебное заседание. Вынесено решение по делу."}],
        }
        row = {"case_number": "2-339/2026", "plaintiff": "ПАО Сбербанк",
               "defendant": "Моньш К.А., МТУ Росимущества", "category": "наследство",
               "court": "Советский районный суд",
               "court_domain": "sovetsk--hmao.sudrf.ru", "judge": "Чайкин В.В.",
               "filing_date": "11.02.2026", "status": "Решено",
               "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН ЧАСТИЧНО",
               "link": "266162762|df57fe78", "bank_role": "Истец"}
        fi = make_bank_entry(row, card, "тест", "now",
                             source="auto_search")["first_instance"]
        assert len(fi.get("appeal_events") or []) == 2, (
            "Движение жалобы не доехало до записи — хронология drawer'а снова "
            "оборвётся на день раньше подачи жалобы."
        )
        assert fi.get("appeal_appellant_is_bank") is False, (
            "Апеллянт не перенесён: без него не определить полярность срочности "
            "срока возражений."
        )
        assert stamp_objections_deadline(fi) == "2026-08-18"

    def test_empty_events_do_not_wipe(self):
        """Гард «только непустое» — перепарс огрызка карточки не должен
        затирать уже собранную историю жалобы."""
        from court_monitor.bank_intake import _stamp_appeal_flags
        fi = {"appeal_events": [{"date": "01.01.2026", "text": "было"}]}
        _stamp_appeal_flags(fi, {"_fi_appeal_events": []}, None)
        assert fi["appeal_events"] == [{"date": "01.01.2026", "text": "было"}]
