# -*- coding: utf-8 -*-
"""Матрица событий программного рендера дайджеста (generate_template_digest).

По одному тесту на КАЖДЫЙ тип события контекста + комбо-дедупы + «всё сразу».
Фикстуры собраны строго по продюсерам (какие поля details реально кладутся):
  - fi_changes  → scripts/court_monitor/runs.py:1430-1968 (update_active_cases,
    ветка 1-й инстанции; база change: case/court/plaintiff/defendant/bank_role/
    category + details.link/court_domain);
  - changes (апелляция) → runs.py:225-441 (main-обход апел. карточек; база
    details: plaintiff/defendant/role/category/appellant*/case_url);
  - cass_changes → linking.py:659-782 (link_cassation_cases);
  - new_cases (апелляция) → CSV-ключи «Номер дела»/«Истец»/«Ответчик»/…;
  - fi_new_cases / cass_discovered → JSON-структуры дел.

Ассерты — на смысловые маркеры строк («заседание отложено на», «ИТОГ:»),
а не байт-в-байт вёрстку: юрист периодически просит менять отступы.

Запуск: `python3 -m pytest tests/test_digest_template_events.py` из корня.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import update_cases as uc  # noqa: E402
# Конфиг и LLM патчатся на модулях-домах (код читает config.X / llm.X),
# см. docs/Распил_монолита_контекст.md.
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.digest import postprocess as cm_post  # noqa: E402

# Контракт фронта и attach_act_analyses: номер дела в <a ...><b>номер</b></a>.
ANCHOR_RE = re.compile(r"<a[^>]*><b>([^<]+)</b></a>")


def anchors(html: str) -> list[str]:
    """Все номера, обёрнутые по контракту <a><b>…</b></a>, в порядке появления."""
    return ANCHOR_RE.findall(html)


def render(**overrides) -> str:
    """generate_template_digest с пустыми дефолтами — единая точка вызова."""
    kwargs = {
        "new_cases": [],
        "changes": [],
        "fi_new_cases": [],
        "fi_changes": [],
        "cass_changes": [],
        "cass_discovered": [],
        "total_active_appeal": 1,
        "total_active_fi": 1,
        "total_active_cassation": 1,
    }
    kwargs.update(overrides)
    return uc.generate_template_digest(**kwargs)


# ── Фабрики: 1-я инстанция ───────────────────────────────────────────────────

# Эталонные details по типам — ровно те ключи, что кладёт update_active_cases.
FI_TYPE_DETAILS: dict[str, dict] = {
    # runs.py:1543-1550
    "fi_hearing_new": {
        "hearing_date": "15.08.2026", "hearing_time": "10:30",
        "hearing_type": "заседание",
    },
    # runs.py:1542-1550
    "fi_hearing_next": {
        "hearing_date": "15.08.2026", "hearing_time": "10:30",
        "hearing_type": "заседание",
    },
    # runs.py:1538-1544 (старая дата остаётся в details, но не рендерится)
    "fi_hearing_postponed": {
        "old_hearing_date": "15.07.2026", "old_hearing_time": "09:00",
        "hearing_date": "20.09.2026", "hearing_time": "12:00",
        "hearing_type": "заседание",
    },
    # runs.py:1536, 1543-1544
    "fi_hearing_recess": {
        "hearing_date": "21.08.2026", "hearing_time": "11:00",
        "hearing_type": "заседание",
    },
    # runs.py:1691-1693
    "fi_status_change": {
        "old_status": "В производстве", "new_status": "Приостановлено",
    },
    # runs.py:1499-1503
    "fi_returned": {
        "event_text": "Возвращение иска (заявления, жалобы). 09:50. "
                      "Исковое заявление возвращено: дело неподсудно данному суду",
        "return_reason": "дело неподсудно данному суду",
    },
    # runs.py:1622
    "fi_act_published": {"act_date": "10.06.2026"},
    # runs.py:1743-1752
    "fi_final_event": {
        "event": "Подготовка дела (собеседование)",
        "event_date": "05.06.2026",
        "scheduled_hearing_date": "25.08.2026",
        "scheduled_hearing_time": "09:00",
    },
    # runs.py:1795
    "fi_motivirovka_emitted": {"motivirovka_date": "11.06.2026"},
    # runs.py:1842-1846
    "fi_appeal_filed": {
        "appellant_role": "Ответчик", "appellant_name": "Иванов И.И.",
        "appeal_filed_date": "05.06.2026",
    },
    # runs.py:1917
    "fi_cassation_filed": {"cassation_filed_date": "05.06.2026"},
    # runs.py:1964
    "fi_sent_to_cassation": {"sent_to_cassation_date": "06.06.2026"},
    # runs.py:1812-1824
    "fi_hearing_restart": {
        "restart_event": "Судебное заседание. Рассмотрение дела начато с начала",
        "restart_date": "10.06.2026",
        "next_hearing_date": "20.08.2026", "next_hearing_time": "10:00",
    },
    # runs.py:1447-1455
    "fi_bank_role_changed": {
        "old_role": "Ответчик", "new_role": "Третье лицо",
        "reason_hint": "банк исключён из числа ответчиков",
    },
    # runs.py:1568-1569
    "fi_accepted_no_hearing": {
        "material_number": "М-321/2026", "filing_date": "01.06.2026",
    },
    # runs.py:1610-1615
    "fi_resolved": {
        "raw_result": "Иск удовлетворён частично",
        "verdict_label": "иск удовлетворён частично",
        "bank_outcome": "в пользу банка",
        "decision_date": "01.06.2026",
        "last_event": "Судебное заседание. Вынесено решение по делу",
        "category": "Кредитные правоотношения",
    },
    # runs.py:1642-1659
    "fi_act_text_published": {
        "act_text": "Суд установил, что заёмщик обязательства не исполнял. "
                    "Руководствуясь статьями 309, 810 ГК РФ, суд взыскал "
                    "задолженность в полном объёме.",
        "act_date": "10.06.2026",
        "decision_date": "01.06.2026",
        "verdict_label": "иск удовлетворён",
        "raw_result": "Иск удовлетворён",
        "bank_outcome": "в пользу банка",
        "category": "Кредитные правоотношения",
        "last_event": "Судебное заседание. Вынесено решение по делу",
    },
}


def make_fi_change(types: list[str], details: dict | None = None,
                   *, case: str = "2-100/2026", **top) -> dict:
    """Change 1-й инстанции — база как в runs.py:1430-1444."""
    merged: dict = {
        # link+court_domain обязательны: без них fi_card_url отдаёт пустой URL
        # и номер рендерится без <a> — контракт фронта нарушен.
        "link": "100200|aaaa-bbbb",
        "court_domain": "vartovgor--hmao.sudrf.ru",
    }
    for t in types:
        merged.update(FI_TYPE_DETAILS.get(t, {}))
    if details:
        merged.update(details)
    ch = {
        "case": case,
        "court": "Нижневартовский городской суд",
        "plaintiff": "ПАО Сбербанк",
        "defendant": "Иванов Иван Иванович",
        "bank_role": "Истец",
        "category": "Кредитные правоотношения",
        "type": list(types),
        "details": merged,
    }
    ch.update(top)
    return ch


def make_fi_new_case(case: str = "2-300/2026") -> dict:
    return {
        "id": case,
        "plaintiff": "ПАО Сбербанк",
        "defendant": "Петров Пётр Петрович",
        "category": "Кредитные правоотношения",
        "bank_role": "Истец",
        "first_instance": {
            "case_number": case,
            "court": "Нижневартовский городской суд",
            "filing_date": "01.07.2026",
            "judge": "Судьин С.С.",
            "link": "100300|cccc-dddd",
            "court_domain": "vartovgor--hmao.sudrf.ru",
        },
    }


# ── Фабрики: апелляция ───────────────────────────────────────────────────────

# База details — runs.py:413-422 (добавляется всем change с непустым type).
APPEAL_BASE_DETAILS: dict = {
    "plaintiff": "Смирнова Анна Викторовна",
    "defendant": "ООО «УК Комфорт»",
    "role": "Третье лицо",
    "category": "Споры → Жилищные споры → Иные жилищные споры",
    "appellant": "Иное лицо",
    "appellant_name": "Смирнова А.В.",
    "appellant_role": "Истец",
    "_appellant_raw": "Смирнова Анна Викторовна",
    "case_url": "https://example.sudrf.ru/case/33-100",
}

APPEAL_TYPE_DETAILS: dict[str, dict] = {
    # runs.py:228-231
    "status_change": {"old_status": "В производстве", "new_status": "Решено"},
    # runs.py:242-246
    "new_event": {
        "event": "Судебное заседание. 11:30. 03.08.2026",
        "event_date": "03.08.2026",
        "hearing_date": "03.08.2026",
        "hearing_time": "11:30",
    },
    # runs.py:259-266 (+ bank_outcome:436-440)
    "new_act": {
        "act_text": "Судебная коллегия установила, что выводы суда первой "
                    "инстанции соответствуют обстоятельствам дела. Доводы "
                    "жалобы направлены на переоценку доказательств.",
        "hearing_date": "28.07.2026",
        "act_date": "30.07.2026",
        "act_verdict_label": "решение оставлено без изменения, жалоба — без удовлетворения",
        "act_verdict_raw": "ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ",
        "bank_outcome": "нейтрально (банк — третье лицо)",
    },
    # runs.py:293-311 (+ bank_outcome:429-433)
    "new_result": {
        "result": "ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ",
        "hearing_date": "28.07.2026",
        "last_event": "Судебное заседание. Вынесено решение",
        "verdict_label": "решение оставлено без изменения, жалоба — без удовлетворения",
        "bank_outcome": "нейтрально (банк — третье лицо)",
    },
    # runs.py:353-357
    "hearing_postponed": {
        "old_hearing_date": "15.07.2026", "old_hearing_time": "10:00",
        "new_hearing_date": "05.08.2026", "new_hearing_time": "11:30",
    },
    # runs.py:359-361
    "hearing_new": {
        "new_hearing_date": "05.08.2026", "new_hearing_time": "11:30",
    },
    # runs.py:370-372
    "appeal_to_fi_rules": {
        "transition_event": "Переход к рассмотрению дела по правилам "
                            "производства в суде первой инстанции",
        "transition_date": "01.07.2026",
    },
}


def make_appeal_change(types: list[str], details: dict | None = None,
                       *, case: str = "33-100/2026") -> dict:
    merged = dict(APPEAL_BASE_DETAILS)
    for t in types:
        merged.update(APPEAL_TYPE_DETAILS.get(t, {}))
    if details:
        merged.update(details)
    return {"case": case, "type": list(types), "details": merged}


def make_appeal_new_case(case: str = "33-300/2026") -> dict:
    """Новое апел. дело — CSV-ключи, как в new_cases контекста."""
    return {
        "Номер дела": case,
        "Истец": "Петров Пётр Петрович",
        "Ответчик": "ПАО Сбербанк",
        "Роль банка": "Ответчик",
        "Категория": "Споры → Иски о взыскании сумм по кредитному договору",
        "Суд 1 инстанции": "Сургутский городской суд",
        "Дата поступления": "01.07.2026",
        "Ссылка": "700800|eeee-ffff",
    }


# ── Фабрики: кассация ────────────────────────────────────────────────────────

def make_cass_change(types: list[str], details: dict | None = None,
                     *, case: str = "2-500/2025",
                     cass_num: str = "8Г-100/2026") -> dict:
    """Change кассации — details как в linking.py:663-679 (+701-704 для new_act)."""
    merged = {
        "stage_prev": "cassation_pending",
        "stage_now": "cassation",
        "outcome": "",
        "review_result": "",
        "result_text": "",
        "result_for_appeal": "",
        "decision_date": "",
        "hearing_date": "",
        "hearing_time": "",
        "appellant": "Иванов Иван Иванович",
        "appellant_is_bank": False,
        "appellant_status": "ответчик",
        "act_kind": "",
        "act_published": False,
        "link": "300400|cc-dd",
    }
    if "new_act" in types:
        merged.update({
            "act_text": "Судебная коллегия кассационного суда не установила "
                        "нарушений норм материального и процессуального права. "
                        "Оценка доказательств отнесена к компетенции нижестоящих "
                        "судов.",
            "act_date": "15.06.2026",
            "act_published": True,
            "outcome": "cassation_upheld",
        })
    if details:
        merged.update(details)
    return {
        "case": case,
        "cassation_internal_number": cass_num,
        "type": list(types),
        "details": merged,
    }


def make_cass_discovered(case: str = "2-505/2025",
                         cass_num: str = "8Г-505/2026") -> dict:
    """Дело-discovery — структура new_case из linking.py:728-757."""
    return {
        "id": case,
        "current_stage": "cassation",
        "plaintiff": "Сидоров Сидор Сидорович",
        "defendant": "ПАО Сбербанк",
        "category": "Споры → Иски о взыскании сумм по кредитному договору",
        "bank_role": "Ответчик",
        "notes": "Найдено через парсер кассации (7kas)",
        "discovered_via_cassation": True,
        "first_instance": {
            "case_number": case,
            "court": "Сургутский городской суд",
            "court_domain": "surggor--hmao.sudrf.ru",
            "judge": "",
            "filing_date": "",
            "status": "Решено",
            "hearing_date": "10.02.2026",
            "link": "",
            "events": [],
        },
        "appeal": None,
        "cassation": {
            "case_number": cass_num,
            "link": "500600|gg-hh",
            "appellant": "Сидоров Сидор Сидорович",
            "appellant_is_bank": False,
            "appellant_status": "истец",
            "filing_date": "20.05.2026",
            "category": "Иски о взыскании сумм по кредитному договору",
        },
    }


# ── 1-я инстанция: по тесту на тип ───────────────────────────────────────────

class FiEventMatrixTest(unittest.TestCase):
    """Каждый fi_* тип: ожидаемая строка, секция, номер в <a><b>, один якорь."""

    def _one(self, types, details=None, **top):
        html = render(fi_changes=[make_fi_change(types, details, **top)])
        return html

    def assert_in_changes_section(self, html, fragment):
        self.assertIn("🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>", html)
        self.assertIn("📅 <b>Изменения (1):</b>", html)
        self.assertIn(fragment, html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_fi_hearing_new(self):
        html = self._one(["fi_hearing_new"])
        self.assert_in_changes_section(html, "📅 заседание 15.08.2026 10:30")

    def test_fi_hearing_new_unpublished(self):
        html = self._one(
            ["fi_hearing_new"],
            {"hearing_date_unpublished": True,
             "hearing_date": "", "hearing_time": ""},
        )
        self.assert_in_changes_section(
            html, "📅 назначено первое заседание (дата и время не опубликованы)"
        )

    def test_fi_hearing_next(self):
        html = self._one(["fi_hearing_next"])
        self.assert_in_changes_section(
            html, "📅 заседание назначено на 15.08.2026 10:30"
        )

    def test_fi_hearing_postponed_shows_only_new_date(self):
        html = self._one(["fi_hearing_postponed"])
        self.assert_in_changes_section(
            html, "🔁 заседание отложено на 20.09.2026 12:00"
        )
        # Старую дату юрист просил не показывать.
        self.assertNotIn("15.07.2026", html)

    def test_fi_hearing_recess(self):
        html = self._one(["fi_hearing_recess"])
        self.assert_in_changes_section(
            html, "🔁 в заседании объявлен перерыв до 21.08.2026 11:00"
        )

    def test_fi_status_change_alone(self):
        html = self._one(["fi_status_change"])
        self.assert_in_changes_section(
            html, "статус: В производстве → Приостановлено"
        )

    def test_fi_returned(self):
        html = self._one(["fi_returned"])
        self.assert_in_changes_section(
            html, "🔚 иск возвращён: дело неподсудно данному суду"
        )

    def test_fi_act_published(self):
        html = self._one(["fi_act_published"])
        self.assert_in_changes_section(
            html,
            "📄 мотивированное решение изготовлено 10.06.2026, "
            "полный текст не опубликован",
        )

    def test_fi_final_event_regular_with_scheduled_date(self):
        html = self._one(["fi_final_event"])
        self.assert_in_changes_section(html, "⚖️ Подготовка дела (собеседование)")
        self.assertIn("📅 заседание назначено на 25.08.2026 09:00", html)

    def test_fi_final_event_motivirovka_normalized(self):
        # Фраза «Изготовлено мотивированное решение…» нормализуется под
        # единую формулировку fi_act_published (template.py:533-546).
        html = self._one(
            ["fi_final_event"],
            {"event": "Изготовлено мотивированное решение в окончательной "
                      "форме 12.06.2026",
             "scheduled_hearing_date": "", "scheduled_hearing_time": ""},
        )
        self.assert_in_changes_section(
            html,
            "📄 мотивированное решение изготовлено 12.06.2026, "
            "полный текст не опубликован",
        )
        self.assertNotIn("⚖️ Изготовлено", html)

    def test_fi_motivirovka_emitted(self):
        html = self._one(["fi_motivirovka_emitted"])
        self.assert_in_changes_section(
            html,
            "📄 мотивированное решение изготовлено 11.06.2026, "
            "полный текст не опубликован",
        )

    def test_fi_appeal_filed(self):
        html = self._one(["fi_appeal_filed"])
        self.assert_in_changes_section(
            html,
            "📨 подана апелляц. жалоба (05.06.2026), "
            "апеллянт: Ответчик Иванов И.И.",
        )

    def test_fi_cassation_filed(self):
        html = self._one(["fi_cassation_filed"])
        self.assert_in_changes_section(
            html, "📨 подана кассационная жалоба (05.06.2026)"
        )

    def test_fi_sent_to_cassation(self):
        html = self._one(["fi_sent_to_cassation"])
        self.assert_in_changes_section(
            html, "📤 направлено в кассац. суд (06.06.2026)"
        )

    def test_fi_hearing_restart(self):
        html = self._one(["fi_hearing_restart"])
        self.assert_in_changes_section(
            html,
            "🔄 рассмотрение начато с начала (10.06.2026); "
            "след. заседание 20.08.2026 10:00",
        )

    def test_fi_bank_role_changed(self):
        html = self._one(["fi_bank_role_changed"])
        self.assert_in_changes_section(
            html,
            "🔄 роль банка: Ответчик → Третье лицо "
            "(банк исключён из числа ответчиков). Дальнейшие исходы — нейтральны.",
        )

    def test_fi_accepted_no_hearing(self):
        html = self._one(["fi_accepted_no_hearing"])
        self.assert_in_changes_section(
            html,
            "📥 принято к производству — заседание не назначено (было М-321/2026)",
        )

    def test_fi_resolved_in_3_5(self):
        html = self._one(["fi_resolved"])
        self.assertIn("⚖️ <b>Вынесенные решения (1):</b>", html)
        self.assertIn("Решение от 01.06.2026", html)
        self.assertIn("<b>ИТОГ:</b> иск удовлетворён частично", html)
        self.assertIn("<b>для банка:</b> в пользу банка", html)
        # Категория сокращается до «кредит».
        self.assertIn("категория: кредит", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)
        # В 3.2 «Изменения» дело не дублируется.
        self.assertNotIn("📅 <b>Изменения", html)

    def test_fi_act_text_published_in_3_6(self):
        html = self._one(["fi_act_text_published"])
        self.assertIn("📄 <b>Опубликованные тексты решений (1):</b>", html)
        self.assertIn("<b>Итог:</b> иск удовлетворён", html)
        self.assertIn("<b>Для банка:</b> в пользу банка", html)
        # Без summarizer — сырой excerpt мотивировки курсивом.
        self.assertIn("<i>Суд установил, что заёмщик", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_fi_new_case_card(self):
        html = render(fi_new_cases=[make_fi_new_case()])
        self.assertIn("📥 <b>Новые иски (1):</b>", html)
        self.assertIn("<b>01.07.2026</b> — 📥 иск зарегистрирован в суде", html)
        self.assertEqual(anchors(html).count("2-300/2026"), 1)


# ── 1-я инстанция: комбо-дедупы 3.2 / 3.5 / 3.6 ─────────────────────────────

class FiComboTest(unittest.TestCase):
    def test_resolved_plus_status_change_only_3_5(self):
        html = render(fi_changes=[
            make_fi_change(["fi_resolved", "fi_status_change"],
                           {"old_status": "В производстве",
                            "new_status": "Решено"})
        ])
        self.assertIn("Вынесенные решения (1)", html)
        # status_change информационно тождественен решению — в 3.2 не дублируем.
        self.assertNotIn("статус: В производстве → Решено", html)
        self.assertNotIn("📅 <b>Изменения", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_resolved_plus_act_text_only_3_6(self):
        html = render(fi_changes=[
            make_fi_change(["fi_resolved", "fi_act_text_published"])
        ])
        self.assertIn("Опубликованные тексты решений (1)", html)
        self.assertNotIn("Вынесенные решения", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_returned_plus_resolved_stays_in_changes(self):
        # Возврат материала — в 3.2 «Изменения», в 3.5 не дублируем
        # (template.py:451-457).
        html = render(fi_changes=[
            make_fi_change(["fi_returned", "fi_resolved"])
        ])
        self.assertIn("🔚 иск возвращён", html)
        self.assertNotIn("Вынесенные решения", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_act_published_suppressed_by_act_text(self):
        html = render(fi_changes=[
            make_fi_change(["fi_act_published", "fi_act_text_published"])
        ])
        self.assertIn("Опубликованные тексты решений (1)", html)
        # Флаг публикации подавлен текстом (types_for_line, template.py:465-470).
        self.assertNotIn("полный текст не опубликован", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 1)

    def test_resolved_plus_side_hearing_event_in_both_sections(self):
        # Побочное hearing-событие того же дела остаётся в 3.2, решение — в 3.5.
        # Номер появляется дважды (в разных секциях) — это осознанная норма.
        html = render(fi_changes=[
            make_fi_change(["fi_resolved", "fi_hearing_postponed"])
        ])
        self.assertIn("Вынесенные решения (1)", html)
        self.assertIn("🔁 заседание отложено на 20.09.2026 12:00", html)
        self.assertEqual(anchors(html).count("2-100/2026"), 2)


# ── Апелляция: по тесту на тип ──────────────────────────────────────────────

class AppealEventMatrixTest(unittest.TestCase):
    def test_new_case_card_three_lines(self):
        html = render(new_cases=[make_appeal_new_case()])
        self.assertIn("⚖️ <b>АПЕЛЛЯЦИЯ</b>", html)
        self.assertIn("📥 <b>Новые дела (1):</b>", html)
        self.assertIn("Суд 1 инст.:", html)
        self.assertIn("<b>01.07.2026</b> — 📥 поступило в апел. суд", html)
        self.assertEqual(anchors(html).count("33-300/2026"), 1)

    def test_hearing_new(self):
        html = render(changes=[make_appeal_change(["hearing_new"])])
        self.assertIn("📅 <b>Изменения (1):</b>", html)
        self.assertIn("📅 Заседание назначено на <b>05.08.2026 11:30</b>", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_hearing_postponed(self):
        html = render(changes=[make_appeal_change(["hearing_postponed"])])
        self.assertIn("🔁 Заседание отложено на <b>05.08.2026 11:30</b>", html)
        self.assertNotIn("15.07.2026", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_new_event_extracts_date_from_text(self):
        # new_hearing_date нет — дата и время достаются из текста события.
        html = render(changes=[make_appeal_change(["new_event"])])
        self.assertIn("📅 Заседание назначено на <b>03.08.2026 11:30</b>", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_new_result_in_acts_section(self):
        html = render(changes=[make_appeal_change(["new_result"])])
        self.assertIn("⚖️ <b>Вынесенные акты (1):</b>", html)
        self.assertIn("ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ", html)
        self.assertIn("Определение от 28.07.2026", html)
        # Банк не в сторонах, роль «Третье лицо» → хвост «(банк — третье лицо)».
        self.assertIn("(банк — третье лицо)", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_new_act_in_published_texts_section(self):
        html = render(changes=[make_appeal_change(["new_act"])])
        self.assertIn("📄 <b>Опубликованные тексты актов (1):</b>", html)
        # Без summarizer — excerpt (первые 1-2 предложения).
        self.assertIn("Мотивировка: Судебная коллегия установила", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_appeal_to_fi_rules(self):
        html = render(changes=[make_appeal_change(["appeal_to_fi_rules"])])
        self.assertIn("⚠ <b>Переход к правилам 1-й инст. (1):</b>", html)
        self.assertIn("(01.07.2026)", html)
        self.assertIn(
            "по правилам производства в суде первой инстанции", html
        )
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_status_change_alone_rendered(self):
        # Фикс B1: change с ЕДИНСТВЕННЫМ типом status_change раньше молча
        # выпадал из дайджеста — теперь строка «статус: X → Y» в 5.2.
        html = render(changes=[make_appeal_change(["status_change"])])
        self.assertIn("📅 <b>Изменения (1):</b>", html)
        self.assertIn("статус: В производстве → Решено", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_new_event_without_date_shows_raw_text(self):
        # Фикс B4: дата из события не распарсилась — показываем сырой текст
        # события, а не пустую карточку «номер + стороны».
        html = render(changes=[make_appeal_change(
            ["new_event"],
            {"event": "Ознакомление с материалами дела",
             "event_date": "", "hearing_date": "", "hearing_time": ""},
        )])
        self.assertIn("📌 Ознакомление с материалами дела", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)


# ── Апелляция: комбо-дедупы ─────────────────────────────────────────────────

class AppealComboTest(unittest.TestCase):
    def test_result_plus_act_only_in_published_texts(self):
        html = render(changes=[make_appeal_change(["new_result", "new_act"])])
        self.assertIn("Опубликованные тексты актов (1)", html)
        self.assertNotIn("Вынесенные акты", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_postponed_plus_event_only_postponed_line(self):
        html = render(changes=[
            make_appeal_change(["hearing_postponed", "new_event"])
        ])
        self.assertIn("🔁 Заседание отложено на", html)
        self.assertNotIn("Заседание назначено на", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_event_plus_result_single_anchor(self):
        # Дополнение к Δ2-тесту: дело ровно один раз, только в «Вынесенных актах».
        html = render(changes=[
            make_appeal_change(["new_event", "new_result"])
        ])
        self.assertIn("Вынесенные акты (1)", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_false_result_with_event_not_lost(self):
        # Фикс B2: «ложный» new_result (текст события в поле «Результат»)
        # исключался и из 5.4 (гард), и из 5.2 (по типу) — дело исчезало
        # из дайджеста целиком. Теперь оно в 5.2 «Изменения».
        html = render(changes=[make_appeal_change(
            ["new_event", "new_result"],
            {"result": "Заседание отложено на 05.08.2026 11:30",
             "event": "Судебное заседание. 11:30. 05.08.2026",
             "hearing_date": "05.08.2026", "hearing_time": "11:30"},
        )])
        self.assertNotIn("Вынесенные акты", html)
        self.assertIn("📅 <b>Изменения (1):</b>", html)
        self.assertIn("Заседание назначено на <b>05.08.2026 11:30</b>", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)

    def test_event_plus_act_only_in_published_texts(self):
        # Фикс B3: связка new_event+new_act раньше дублировалась в 5.2 и
        # 5.5 — теперь только «Опубликованные тексты актов».
        html = render(changes=[make_appeal_change(["new_event", "new_act"])])
        self.assertIn("Опубликованные тексты актов (1)", html)
        self.assertNotIn("📅 <b>Изменения", html)
        self.assertEqual(anchors(html).count("33-100/2026"), 1)


# ── Гибридный рендер актов: маркер «Почему:», Итог в 5.5, вёрстка ───────────

def _fake_summarizer(act_text, *, case_meta):
    return f"ПЕРЕСКАЗ_{(case_meta.get('stage') or '').upper()}"


class HybridActRenderingTest(unittest.TestCase):
    """Фиксы B5/B9: LLM-пересказ выводится с маркером «<b>Почему:</b>»
    (на нём держится attach_act_analyses и drawer карточки дела), excerpt
    остаётся без маркера; 5.5 получает строку Итог/Для банка и пустую
    строку между делами."""

    def test_pochemu_marker_in_3_6(self):
        html = render(
            fi_changes=[make_fi_change(["fi_act_text_published"])],
            act_summarizer=_fake_summarizer,
        )
        self.assertIn(
            "<b>Почему:</b> <i>ПЕРЕСКАЗ_FIRST_INSTANCE</i>", html
        )

    def test_pochemu_marker_in_5_5(self):
        html = render(
            changes=[make_appeal_change(["new_act"])],
            act_summarizer=_fake_summarizer,
        )
        self.assertIn("<b>Почему:</b> <i>ПЕРЕСКАЗ_APPEAL</i>", html)

    def test_pochemu_marker_in_cassation(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with patch.object(cm_config, "JSON_PATH",
                          os.path.join(tmp.name, "нет.json")):
            html = render(
                cass_changes=[make_cass_change(["outcome_change", "new_act"])],
                act_summarizer=_fake_summarizer,
            )
        self.assertIn("<b>Почему:</b> <i>ПЕРЕСКАЗ_CASSATION</i>", html)

    def test_excerpt_fallback_has_no_pochemu(self):
        # Сырой excerpt (LLM недоступен) — без маркера: «Почему» из
        # необработанного куска текста выглядело бы враньём.
        html = render(
            fi_changes=[make_fi_change(["fi_act_text_published"])],
            changes=[make_appeal_change(["new_act"])],
        )
        self.assertNotIn("Почему:", html)

    def test_5_5_itog_line(self):
        # Фикс B9: Итог из карточки + «в чью пользу» — симметрично 3.6.
        html = render(changes=[make_appeal_change(["new_act"])])
        self.assertIn(
            "<b>Итог:</b> решение оставлено без изменения, "
            "жалоба — без удовлетворения", html,
        )
        self.assertIn(
            "<b>Для банка:</b> нейтрально (банк — третье лицо)", html
        )

    def test_5_5_blank_line_between_cases(self):
        # Фикс B5: правило юриста — пустая строка между разными делами.
        html = render(changes=[
            make_appeal_change(["new_act"], case="33-101/2026"),
            make_appeal_change(["new_act"], case="33-102/2026"),
        ])
        first_end = html.index("33-102/2026")
        between = html[html.index("33-101/2026"):first_end]
        self.assertIn("\n\n", between,
                      "между делами 5.5 нет пустой строки")

    def test_blank_line_between_cases_in_multiline_sections(self):
        # То же правило для остальных многострочных секций: 3.1 «Новые
        # иски», 5.1 «Новые дела», 5.2 «Изменения» апелляции.
        pairs = [
            dict(fi_new_cases=[make_fi_new_case("2-301/2026"),
                               make_fi_new_case("2-302/2026")]),
            dict(new_cases=[make_appeal_new_case("33-301/2026"),
                            make_appeal_new_case("33-302/2026")]),
            dict(changes=[make_appeal_change(["hearing_new"],
                                             case="33-303/2026"),
                          make_appeal_change(["hearing_new"],
                                             case="33-304/2026")]),
        ]
        for kwargs in pairs:
            html = render(**kwargs)
            nums = sorted(
                n for lst in kwargs.values() for n in (
                    [c.get("id") or c.get("Номер дела") for c in lst]
                    if isinstance(lst[0], dict) and "type" not in lst[0]
                    else [ch["case"] for ch in lst]
                )
            )
            between = html[html.index(nums[0]):html.index(nums[1])]
            self.assertIn(
                "\n\n", between,
                f"между делами {nums} нет пустой строки",
            )


class ActAnalysisContractTest(unittest.TestCase):
    """attach_act_analyses должен вырезать «Почему»-абзац из гибридного
    дайджеста и класть его в cases.json (source=digest), включая кассацию,
    где шаблон оборачивает касс. номер вместо номера 1-й инст."""

    def test_appeal_act_analysis_from_hybrid_digest(self):
        html = render(
            changes=[make_appeal_change(["new_act"])],
            act_summarizer=_fake_summarizer,
        )
        cases = [{
            "id": "2-777/2025",
            "appeal": {"case_number": "33-100/2026"},
        }]
        updated = uc.attach_act_analyses(
            cases, html,
            all_changes=[make_appeal_change(["new_act"])],
            cass_changes=[],
        )
        self.assertEqual(updated, 1)
        aa = cases[0]["appeal"]["act_analysis"]
        self.assertEqual(aa["source"], "digest")
        self.assertIn("<b>Почему:</b>", aa["html"])
        self.assertIn("ПЕРЕСКАЗ_APPEAL", aa["html"])
        # В абзац не должна попасть вся секция целиком (см. B5: пустые
        # строки режут 5.5 на абзацы по-делово).
        self.assertNotIn("Опубликованные тексты", aa["html"])

    def test_cassation_act_analysis_via_cass_number(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cass_ch = make_cass_change(["outcome_change", "new_act"])
        with patch.object(cm_config, "JSON_PATH",
                          os.path.join(tmp.name, "нет.json")):
            html = render(
                cass_changes=[cass_ch], act_summarizer=_fake_summarizer,
            )
        cases = [{
            "id": "2-500/2025",
            "cassation": {"case_number": "8Г-100/2026"},
        }]
        updated = uc.attach_act_analyses(
            cases, html, all_changes=[], cass_changes=[cass_ch],
        )
        self.assertEqual(updated, 1)
        aa = cases[0]["cassation"]["act_analysis"]
        self.assertEqual(aa["source"], "digest")
        self.assertIn("ПЕРЕСКАЗ_CASSATION", aa["html"])


# ── Кассация ─────────────────────────────────────────────────────────────────

class CassationMatrixTest(unittest.TestCase):
    """Рендер кассации читает реальный data/cases.json для parent-lookup
    (template.py:975-978) — патчим JSON_PATH, иначе тесты недетерминированы."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        patcher = patch.object(
            cm_config, "JSON_PATH",
            os.path.join(self._tmp.name, "нет-такого-файла.json"),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self._tmp.cleanup)

    def test_new_cassation_hearing_line(self):
        html = render(cass_changes=[make_cass_change(
            ["new_cassation"],
            {"hearing_date": "20.08.2026", "hearing_time": "14:00"},
        )])
        self.assertIn("⚖️🔬 <b>КАССАЦИЯ</b>", html)
        self.assertIn("📑 <b>Касс. события (1):</b>", html)
        self.assertIn(
            "📅 Назначено судебное заседание на <b>20.08.2026 в 14:00</b>", html
        )
        self.assertEqual(anchors(html).count("8Г-100/2026"), 1)

    def test_outcome_change_itog_line(self):
        html = render(cass_changes=[make_cass_change(
            ["outcome_change"], {"outcome": "cassation_upheld"},
        )])
        # Метка из CASSATION_OUTCOME_RU + творительный падеж роли заявителя.
        self.assertIn("<b>Итог:</b> Оставлено без изменения", html)
        self.assertIn("подана Ответчиком", html)
        self.assertEqual(anchors(html).count("8Г-100/2026"), 1)

    def test_hearing_suppressed_when_outcome_present(self):
        html = render(cass_changes=[make_cass_change(
            ["outcome_change"],
            {"outcome": "cassation_upheld",
             "hearing_date": "20.05.2026", "hearing_time": "14:00"},
        )])
        # Заседание уже состоялось — «Назначено…» в прошлом обманывает.
        self.assertNotIn("Назначено судебное заседание", html)
        self.assertIn("<b>Итог:</b>", html)

    def test_review_result_change_label(self):
        html = render(cass_changes=[make_cass_change(
            ["review_result_change"],
            {"review_result": "ПРИНЯТО К ПРОИЗВОДСТВУ"},
        )])
        self.assertIn("<b>Итог:</b> 📥 Принято к производству", html)

    def test_accepted_label_suppressed_when_hearing_scheduled(self):
        html = render(cass_changes=[make_cass_change(
            ["review_result_change"],
            {"review_result": "ПРИНЯТО К ПРОИЗВОДСТВУ",
             "hearing_date": "20.08.2026", "hearing_time": "14:00"},
        )])
        # «Принято к производству» избыточно при видимой дате заседания.
        self.assertIn("Назначено судебное заседание", html)
        self.assertNotIn("Принято к производству", html)

    def test_terminated_split_into_concrete_label(self):
        # Общий enum cassation_terminated расщепляется на конкретику
        # (возврат/прекращение/отзыв) по тексту review_result/result_text.
        html = render(cass_changes=[make_cass_change(
            ["outcome_change"],
            {"outcome": "cassation_terminated",
             "review_result": "ВОЗВРАЩЕНО БЕЗ РАССМОТРЕНИЯ ПО СУЩЕСТВУ",
             "result_text": ""},
        )])
        self.assertIn("<b>Итог:</b> 🔚 Жалоба возвращена", html)

    def test_new_act_renders_summary_in_italic(self):
        def fake_summarizer(act_text, *, case_meta):
            self.assertEqual(case_meta.get("stage"), "cassation")
            return "КАСС_ПЕРЕСКАЗ"

        html = render(
            cass_changes=[make_cass_change(["outcome_change", "new_act"])],
            act_summarizer=fake_summarizer,
        )
        self.assertIn("<i>КАСС_ПЕРЕСКАЗ</i>", html)
        self.assertEqual(anchors(html).count("8Г-100/2026"), 1)

    def test_discovered_card_in_new_section(self):
        html = render(cass_discovered=[make_cass_discovered()])
        self.assertIn("📥 <b>Новые касс. дела (1):</b>", html)
        self.assertIn("поступила касс. жалоба", html)
        self.assertIn("<b>20.05.2026</b>", html)
        self.assertEqual(anchors(html).count("8Г-505/2026"), 1)

    def test_discovered_change_excluded_from_events(self):
        # Discovery-change не дублируется в «Касс. события» — дело уже
        # представлено карточкой в «Новых касс. делах».
        html = render(
            cass_discovered=[make_cass_discovered()],
            cass_changes=[make_cass_change(
                ["discovered_in_cassation"],
                case="2-505/2025", cass_num="8Г-505/2026",
            )],
        )
        self.assertIn("Новые касс. дела (1)", html)
        self.assertNotIn("Касс. события", html)

    def test_discovered_with_outcome_shows_itog_and_pochemu(self):
        # Фикс B6: discovery с уже известным исходом — раньше карточка
        # «нового дела» молчала об исходе, он терялся из дайджеста.
        c = make_cass_discovered()
        c["cassation"].update({
            "outcome": "cassation_upheld",
            "review_result": "",
            "result_text": "Жалоба оставлена без удовлетворения",
            "act_text": "Судебная коллегия не установила нарушений. " * 5,
        })
        html = render(
            cass_discovered=[c], act_summarizer=_fake_summarizer,
        )
        self.assertIn("<b>Итог:</b> Оставлено без изменения", html)
        self.assertIn("<b>Почему:</b> <i>ПЕРЕСКАЗ_CASSATION</i>", html)

    def test_discovered_accepted_label_suppressed(self):
        # «📥 Принято к производству» дублирует строку «📥 поступила касс.
        # жалоба» — в discovery-карточке метку не показываем.
        c = make_cass_discovered()
        c["cassation"]["review_result"] = "ПРИНЯТО К ПРОИЗВОДСТВУ"
        html = render(cass_discovered=[c])
        self.assertIn("поступила касс. жалоба", html)
        self.assertNotIn("Принято к производству", html)


# ── «Всё сразу»: полный контекст ────────────────────────────────────────────

APPEAL_TYPES_RENDERED = [
    "new_event", "new_result", "new_act",
    "status_change", "hearing_postponed", "hearing_new", "appeal_to_fi_rules",
]

FI_ALL_TYPES = [
    "fi_hearing_new", "fi_hearing_next", "fi_hearing_postponed",
    "fi_hearing_recess", "fi_status_change", "fi_returned",
    "fi_act_published", "fi_final_event", "fi_motivirovka_emitted",
    "fi_appeal_filed", "fi_cassation_filed", "fi_sent_to_cassation",
    "fi_hearing_restart", "fi_bank_role_changed", "fi_accepted_no_hearing",
    "fi_resolved", "fi_act_text_published",
]

CASS_EVENT_TYPES = [
    "new_cassation", "review_result_change", "outcome_change", "new_act",
]


class AllEventTypesTest(unittest.TestCase):
    """Контекст со всеми типами одновременно: полнота, счётчики, вёрстка."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls._patcher = patch.object(
            cm_config, "JSON_PATH",
            os.path.join(cls._tmp.name, "нет.json"),
        )
        cls._patcher.start()
        # Синтетический контекст «всё сразу» длиннее лимита Telegram — тут
        # проверяем ПОЛНОТУ рендера, а не обрезку (её тестирует TruncationTest).
        cls._limit_patcher = patch.object(
            cm_config, "TELEGRAM_MSG_LIMIT", 30000
        )
        cls._limit_patcher.start()

        cls.fi_changes = [
            make_fi_change([t], case=f"2-{101 + i}/2026")
            for i, t in enumerate(FI_ALL_TYPES)
        ]
        cls.appeal_changes = [
            make_appeal_change([t], case=f"33-{201 + i}/2026")
            for i, t in enumerate(APPEAL_TYPES_RENDERED)
        ]
        extra_cass = {
            "new_cassation": {"hearing_date": "20.08.2026",
                              "hearing_time": "14:00"},
            "review_result_change": {"review_result": "ПРИНЯТО К ПРОИЗВОДСТВУ"},
            "outcome_change": {"outcome": "cassation_upheld"},
            "new_act": {},
        }
        cls.cass_changes = [
            make_cass_change([t], extra_cass[t],
                             case=f"2-{501 + i}/2025",
                             cass_num=f"8Г-{501 + i}/2026")
            for i, t in enumerate(CASS_EVENT_TYPES)
        ]
        # Discovery: карточка в «Новых касс. делах» + служебный change.
        cls.cass_changes.append(make_cass_change(
            ["discovered_in_cassation"],
            case="2-599/2025", cass_num="8Г-599/2026",
        ))
        cls.cass_discovered = [
            make_cass_discovered(case="2-599/2025", cass_num="8Г-599/2026")
        ]
        cls.kwargs = dict(
            new_cases=[make_appeal_new_case()],
            changes=cls.appeal_changes,
            fi_new_cases=[make_fi_new_case()],
            fi_changes=cls.fi_changes,
            cass_changes=cls.cass_changes,
            cass_discovered=cls.cass_discovered,
            total_active_appeal=8,
            total_active_fi=20,
            total_active_cassation=2,
        )
        cls.html = render(**cls.kwargs)

    @classmethod
    def tearDownClass(cls):
        cls._limit_patcher.stop()
        cls._patcher.stop()
        cls._tmp.cleanup()

    def test_all_case_numbers_present(self):
        # Каждый номер (или его касс. альтернатива) обязан быть в HTML.
        expected: list[set] = [{"2-300/2026"}, {"33-300/2026"}]
        expected += [{ch["case"]} for ch in self.fi_changes]
        expected += [{ch["case"]} for ch in self.appeal_changes]
        # Касс. события рендерят кассационный номер (по просьбе юриста).
        expected += [
            {ch["case"], ch["cassation_internal_number"]}
            for ch in self.cass_changes
        ]
        missing = [
            alts for alts in expected
            if not any(n in self.html for n in alts)
        ]
        self.assertFalse(missing, f"Потеряны номера: {missing}")

    def test_all_rendered_numbers_wrapped_in_anchor(self):
        got = set(anchors(self.html))
        want = {"2-300/2026", "33-300/2026"}
        want |= {ch["case"] for ch in self.fi_changes}
        want |= {ch["case"] for ch in self.appeal_changes}
        want |= {ch["cassation_internal_number"] for ch in self.cass_changes}
        self.assertTrue(
            want <= got,
            f"Номера без <a><b>-обёртки: {sorted(want - got)}",
        )

    def test_section_counters_match(self):
        # 15 fi-типов → 3.2; fi_resolved → 3.5; fi_act_text → 3.6.
        self.assertIn("📅 <b>Изменения (15):</b>", self.html)
        self.assertIn("⚖️ <b>Вынесенные решения (1):</b>", self.html)
        self.assertIn("📄 <b>Опубликованные тексты решений (1):</b>", self.html)
        self.assertIn("📥 <b>Новые иски (1):</b>", self.html)
        # Апелляция: postponed + new_event + hearing_new + голый
        # status_change → «Изменения (4)».
        self.assertIn("📅 <b>Изменения (4):</b>", self.html)
        self.assertIn("⚖️ <b>Вынесенные акты (1):</b>", self.html)
        self.assertIn("📄 <b>Опубликованные тексты актов (1):</b>", self.html)
        self.assertIn("⚠ <b>Переход к правилам 1-й инст. (1):</b>", self.html)
        self.assertIn("📥 <b>Новые дела (1):</b>", self.html)
        # Кассация: 4 события (discovery-change исключён) + 1 discovery-карточка.
        self.assertIn("📑 <b>Касс. события (4):</b>", self.html)
        self.assertIn("📥 <b>Новые касс. дела (1):</b>", self.html)

    def test_footer_totals(self):
        self.assertIn(
            "📌 <b>В производстве: всего 30 "
            "(1 инст.: 20 | апел.: 8 | касс.: 2)</b>",
            self.html,
        )
        self.assertIn(cm_config.DASHBOARD_URL, self.html)

    def test_layout_no_blank_line_inside_case(self):
        # Правило юриста: строки одного дела ПОДРЯД. Пустая строка не может
        # предшествовать строке-продолжению (отступ 4+ пробела).
        self.assertNotRegex(self.html, r"\n\n {4,}\S")

    def test_layout_no_triple_blank_lines(self):
        self.assertNotIn("\n\n\n", self.html)

    def test_tags_balanced(self):
        self.assertEqual(cm_post._close_open_tags(self.html), self.html)

    def test_not_truncated(self):
        # С поднятым лимитом полный контекст обязан влезть целиком.
        self.assertNotIn("сообщение обрезано", self.html)

    def test_idempotent(self):
        self.assertEqual(render(**self.kwargs), self.html)

    def test_no_markdown_artifacts(self):
        self.assertNotIn("**", self.html)
        for line in self.html.split("\n"):
            self.assertFalse(line.lstrip().startswith("#"),
                             f"Markdown-заголовок: {line!r}")

    def test_summary_line_mentions_key_categories(self):
        # Сводка — вторая строка дайджеста. Включая счётчики фикса B7
        # (возвраты, касс. жалобы, направления в касс. суд, принято к
        # производству, голый статус апелляции).
        summary_line = self.html.split("\n")[1]
        for token in ("нов. 1 инст.", "нов. апелл.", "нов. касс.",
                      "реш. 1 инст.", "касс. акт.", "возвр. исков",
                      "касс. жалоб", "в касс. суд", "принято к пр-ву",
                      "статус апел."):
            self.assertIn(token, summary_line)


# ── Обрезка больших дайджестов ──────────────────────────────────────────────

class TruncationTest(unittest.TestCase):
    def test_huge_context_truncated_cleanly(self):
        many = [
            make_fi_new_case(case=f"2-{7000 + i}/2026") for i in range(80)
        ]
        for c in many:
            c["plaintiff"] = "МТУ Росимущества в Тюменской области, ХМАО-Югре, ЯНАО"
            c["defendant"] = "Общество с ограниченной ответственностью «Очень длинное название»"
        html = render(fi_new_cases=many, total_active_fi=80)
        self.assertLessEqual(len(html), uc.TELEGRAM_MSG_LIMIT * 2)
        self.assertIn("сообщение обрезано", html)
        # Теги после обрезки закрыты.
        self.assertEqual(cm_post._close_open_tags(html), html)


# ── Тихий день ──────────────────────────────────────────────────────────────

class QuietDigestTest(unittest.TestCase):
    """render_no_changes_digest читает реальный last_digest.json
    (template.py:294-319) — патчим путь, фиксируем оба варианта."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = os.path.join(self._tmp.name, "last_digest.json")
        patcher = patch.object(cm_config, "LAST_DIGEST_PATH", self.path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_quiet_without_previous(self):
        html = render()
        self.assertIn("изменений не было", html)
        self.assertIn("В производстве: всего 3", html)
        self.assertIn(cm_config.DASHBOARD_URL, html)
        self.assertNotIn("Предыдущий дайджест", html)

    def test_quiet_with_previous_digest(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({
                "version": 1,
                "generated_at": "2026-07-02T09:00:00",
                "summary": "тест",
                "html": "<b>Прошлый дайджест</b>",
                "is_empty": False,
            }, f, ensure_ascii=False)
        html = render()
        self.assertIn("Предыдущий дайджест", html)
        self.assertIn("от 02.07.2026", html)
        self.assertIn("<b>Прошлый дайджест</b>", html)


# ── Проводка пересказов через все три стадии ────────────────────────────────

class SummarizerWiringTest(unittest.TestCase):
    def test_three_stages_in_one_render(self):
        seen: list[str] = []

        def fake_summarizer(act_text, *, case_meta):
            stage = case_meta.get("stage", "")
            seen.append(stage)
            return f"ПЕРЕСКАЗ_{stage.upper()}"

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with patch.object(cm_config, "JSON_PATH",
                          os.path.join(tmp.name, "нет.json")):
            html = render(
                fi_changes=[make_fi_change(["fi_act_text_published"])],
                changes=[make_appeal_change(["new_act"])],
                cass_changes=[make_cass_change(["outcome_change", "new_act"])],
                act_summarizer=fake_summarizer,
            )
        self.assertEqual(
            sorted(seen), ["appeal", "cassation", "first_instance"]
        )
        self.assertIn("ПЕРЕСКАЗ_FIRST_INSTANCE", html)
        self.assertIn("ПЕРЕСКАЗ_APPEAL", html)
        self.assertIn("ПЕРЕСКАЗ_CASSATION", html)


if __name__ == "__main__":
    unittest.main()
