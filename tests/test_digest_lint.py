# -*- coding: utf-8 -*-
"""Тесты программного линтера дайджеста (court_monitor/digest/lint.py).

Линтер — сторож качества рендера: проверяет готовый HTML против контекста
данных (полнота номеров, обёртка <a><b>, счётчики (N), теги, футер, лимит).
Никогда не бросает исключений и ничего не блокирует.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import update_cases as uc  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402

from tests.test_digest_template_events import (  # noqa: E402
    make_appeal_change, make_fi_change, make_fi_new_case, render,
)


def _ctx(**overrides) -> dict:
    """Контекст-kwargs для lint_digest_html (без total_active_*)."""
    kwargs = {
        "new_cases": [],
        "changes": [],
        "fi_new_cases": [],
        "fi_changes": [],
        "cass_changes": [],
        "cass_discovered": [],
    }
    kwargs.update(overrides)
    return kwargs


class LintCleanDigestTest(unittest.TestCase):
    def test_clean_digest_no_problems(self):
        ctx = _ctx(
            fi_new_cases=[make_fi_new_case()],
            fi_changes=[make_fi_change(["fi_hearing_new"])],
            changes=[make_appeal_change(["hearing_new"])],
        )
        html = render(**ctx)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])

    def test_empty_context_no_checks(self):
        # Тихий день: структурные проверки неприменимы.
        self.assertEqual(uc.lint_digest_html("что угодно", **_ctx()), [])

    def test_is_empty_flag_skips_checks(self):
        ctx = _ctx(fi_new_cases=[make_fi_new_case()])
        self.assertEqual(
            uc.lint_digest_html("", is_empty=True, **ctx), []
        )


class LintProblemsTest(unittest.TestCase):
    def setUp(self):
        self.ctx = _ctx(fi_new_cases=[make_fi_new_case()])
        self.html = render(**self.ctx)

    def test_empty_html_with_context(self):
        problems = uc.lint_digest_html("", **self.ctx)
        self.assertTrue(any("пуст" in p for p in problems))

    def test_lost_case_number(self):
        broken = self.html.replace("2-300/2026", "X-000/0000")
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("потерян номер" in p and "2-300/2026" in p
                for p in problems),
            problems,
        )

    def test_number_without_anchor_wrap(self):
        # Номер остался текстом, но <a><b>-обёртка пропала.
        broken = self.html.replace(
            "<b>2-300/2026</b>", "2-300/2026"
        )
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("без <a><b>-обёртки" in p for p in problems), problems
        )

    def test_unbalanced_tags(self):
        broken = self.html + "\n<b>незакрытый"
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("несбалансированные" in p for p in problems), problems
        )

    def test_forbidden_tag(self):
        broken = self.html + "\n<p>абзац</p>"
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("запрещённый" in p for p in problems), problems
        )

    def test_missing_dashboard_link(self):
        broken = self.html.replace(cm_config.DASHBOARD_URL, "https://x")
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("дашборд" in p for p in problems), problems
        )

    def test_missing_footer(self):
        broken = self.html.replace("В производстве", "—")
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("футер" in p for p in problems), problems
        )

    def test_wrong_section_counter(self):
        broken = self.html.replace("Новые иски (1)", "Новые иски (3)")
        problems = uc.lint_digest_html(broken, **self.ctx)
        self.assertTrue(
            any("счётчик" in p and "заявлено 3" in p for p in problems),
            problems,
        )

    def test_full_llm_format_without_indent_counts_correctly(self):
        # Регресс A/B 03.07.2026: формат full-LLM пути не имеет отступов
        # у строк дел — счётчик по отступам давал ложный алерт «по факту
        # дел 0». Считаем по строкам с номерами дел, формат-независимо.
        ctx = _ctx(changes=[
            make_appeal_change(["hearing_new"], case="33-1/2026"),
            make_appeal_change(["hearing_new"], case="33-2/2026"),
        ])
        llm_style_html = (
            "📊 Дайджест судебных дел | 03.07.2026\n\n"
            "📅 <b>Изменения (2):</b>\n\n"
            '<a href="https://x/1"><b>33-1/2026</b></a> — Иванов vs Петров\n'
            "📅 Заседание назначено на 14.07.2026 14:30\n\n"
            '<a href="https://x/2"><b>33-2/2026</b></a> — Сидоров vs Козлов\n'
            "📅 Заседание назначено на 02.07.2026 15:00\n\n"
            "📌 <b>В производстве: всего 65</b>\n"
            f'<a href="{cm_config.DASHBOARD_URL}">📊 Дашборд</a>'
        )
        self.assertEqual(uc.lint_digest_html(llm_style_html, **ctx), [])

    def test_truncated_digest_flagged_without_number_noise(self):
        # Боевая генерация HTML больше не обрезает (truncate_html_message
        # снят), но линтер обязан корректно отработать страховочный путь:
        # если маркер обрезки всё же появится — сообщить об этом ОДНОЙ общей
        # проблемой и НЕ перечислять потерянные номера.
        many = [make_fi_new_case(case=f"2-{8000 + i}/2026") for i in range(80)]
        ctx = _ctx(fi_new_cases=many)
        # Симулируем обрезанный дайджест: только первое дело + маркер обрезки.
        html = (
            "📊 <b>Мониторинг</b>\n\n"
            '<a href="u"><b>2-8000/2026</b></a> — Истец vs Сбербанк\n\n'
            "…<i>сообщение обрезано</i>"
        )
        problems = uc.lint_digest_html(html, **ctx)
        self.assertTrue(
            any("обрезан" in p for p in problems), problems
        )
        self.assertFalse(
            any("потерян номер" in p for p in problems),
            "при обрезке пономерные жалобы — шум",
        )

    def test_never_raises_on_garbage(self):
        # Линтер не имеет права ронять прогон.
        for garbage in (None, 123, {"html": "x"}):
            try:
                uc.lint_digest_html(garbage, **self.ctx)  # type: ignore[arg-type]
            except Exception as exc:  # pragma: no cover
                self.fail(f"линтер бросил исключение: {exc!r}")


class LintBankSectionBoundaryTest(unittest.TestCase):
    """Граница секций у банк-блока (инцидент 12.08.2026).

    «🏦 ИСКИ БАНКА (N)» не входил в _DIGEST_HEADER_RE: линтер не видел, где
    кончается секция «📑 Касс. события», и приписывал ей все банк-строки
    («заявлено 1, по факту дел 22»). Заодно сам банк-счётчик не сверялся
    вовсе — заголовок без эмодзи из списка не распознавался секцией.
    """

    def test_bank_header_matches_header_re(self):
        from court_monitor.digest.postprocess import _DIGEST_HEADER_RE
        self.assertIsNotNone(
            _DIGEST_HEADER_RE.match("🏦 <b>ИСКИ БАНКА (21):</b>")
        )

    def test_bank_flagged_case_lines_are_not_headers(self):
        # Строки-дела кассации с флагом банка начинаются с 🏦, но после
        # тега идёт цифра/«8Г» — под [А-ЯA-Zа-яa-z] не подпадают.
        from court_monitor.digest.postprocess import _DIGEST_HEADER_RE
        for line in (
            '🏦 <a href="https://x"><b>8Г-11469/2026</b></a> — Сбербанк',
            "🏦 <b>8Г-11469/2026</b> — Сбербанк vs Иванов",
        ):
            self.assertIsNone(_DIGEST_HEADER_RE.match(line), line)

    @staticmethod
    def _html(bank_declared: int) -> str:
        return (
            "📑 <b>Касс. события (1):</b>\n\n"
            '<a href="https://x/k"><b>8Г-10462/2026</b></a> — Немцов vs Сбербанк\n'
            "Суд 1 инст.: Нижневартовский гор. суд | категория: Иное\n\n"
            f"🏦 <b>ИСКИ БАНКА ({bank_declared}):</b>\n\n"
            '<a href="https://x/1"><b>2-100/2026</b></a> — Сбербанк vs Иванов\n\n'
            '<a href="https://x/2"><b>2-200/2026</b></a> — Сбербанк vs Петров\n\n'
            "📌 <b>В производстве: всего 79</b>"
        )

    def test_cassation_counter_not_polluted_by_bank_section(self):
        from court_monitor.digest.lint import _check_section_counters
        self.assertEqual(_check_section_counters(self._html(2)), [])

    def test_bank_counter_now_checked(self):
        from court_monitor.digest.lint import _check_section_counters
        problems = _check_section_counters(self._html(5))
        self.assertTrue(
            any("ИСКИ БАНКА" in p and "заявлено 5" in p
                and "по факту дел 2" in p for p in problems),
            problems,
        )


class LintKillSwitchTest(unittest.TestCase):
    def test_runs_helper_respects_kill_switch(self):
        # DIGEST_LINT=0 → _lint_digest_and_alert не зовёт линтер вовсе.
        from court_monitor import runs as cm_runs
        called = []
        with patch.object(cm_config, "DIGEST_LINT", False), \
             patch.object(cm_runs, "lint_digest_html",
                          lambda *a, **kw: called.append(1) or []):
            cm_runs._lint_digest_and_alert("<b>x</b>")
        self.assertEqual(called, [])

    def test_runs_helper_sends_alert_on_problems(self):
        from court_monitor import runs as cm_runs
        sent = []
        with patch.object(cm_config, "DIGEST_LINT", True), \
             patch.object(cm_runs, "lint_digest_html",
                          lambda *a, **kw: ["проблема-1", "проблема-2"]), \
             patch.object(cm_runs, "send_telegram",
                          lambda text, **kw: sent.append(text)):
            cm_runs._lint_digest_and_alert("<b>x</b>")
        self.assertEqual(len(sent), 1)
        self.assertIn("🩺", sent[0])
        self.assertIn("Дайджест-линтер", sent[0])
        self.assertIn("• проблема-1", sent[0])
        self.assertIn("• проблема-2", sent[0])

    def test_runs_helper_silent_when_clean(self):
        from court_monitor import runs as cm_runs
        sent = []
        with patch.object(cm_config, "DIGEST_LINT", True), \
             patch.object(cm_runs, "lint_digest_html", lambda *a, **kw: []), \
             patch.object(cm_runs, "send_telegram",
                          lambda text, **kw: sent.append(text)):
            cm_runs._lint_digest_and_alert("<b>x</b>")
        self.assertEqual(sent, [])


class LintMainTrackAdditions13AugTest(unittest.TestCase):
    """Новые строки основного прогона (13.08.2026) проходят линтер. Самая
    опасная точка — номер дела 1-й инст. в строке 1 новой апелляции: счётчики
    считают дела по строкам с номерами, второй номер в ТОЙ ЖЕ строке дела
    удваивать счёт не должен."""

    def test_new_lines_pass_lint(self):
        appeal_new = {
            "Номер дела": "33-300/2026",
            "Истец": "Петров Пётр Петрович",
            "Ответчик": "ПАО Сбербанк",
            "Роль банка": "Ответчик",
            "Категория": "Споры → Иски о взыскании сумм по кредитному договору",
            "Суд 1 инстанции": "Сургутский городской суд",
            "Дата поступления": "01.07.2026",
            "Ссылка": "700800|eeee-ffff",
            "Номер дела 1 инстанции": "2-1234/2026",
        }
        from tests.test_digest_template_events import make_cass_change
        ctx = _ctx(
            new_cases=[appeal_new],
            fi_changes=[make_fi_change(
                ["fi_post_decision_hearing"],
                {"hearing_date": "25.08.2026", "hearing_time": "11:00"},
            )],
            cass_changes=[
                make_cass_change(
                    ["cass_hearing_scheduled"],
                    {"hearing_date": "26.08.2026", "hearing_time": "10:00"},
                ),
                make_cass_change(
                    ["cass_suspended"],
                    {"suspended_until": "30.08.2026"},
                    case="2-501/2025", cass_num="8Г-101/2026",
                ),
            ],
        )
        html = render(**ctx)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])


class LintBankActWhyTest(unittest.TestCase):
    """«Почему» в банк-секции (13.08.2026) не ломает счётчик «ИСКИ БАНКА (N)»:
    строка пересказа идёт без номера дела и линтером не считается."""

    def test_bank_act_with_pochemu_passes_lint(self):
        from tests.test_digest_template_events import (
            make_bank_act_change, _fake_summarizer,
        )
        ctx = _ctx(fi_changes=[
            make_bank_act_change(),
            make_fi_change(["fi_hearing_new"], case="2-101/2026",
                           track="plaintiff_light"),
        ])
        html = render(**ctx, act_summarizer=_fake_summarizer)
        self.assertIn("<b>Почему:</b>", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])


class LintBankCalendarEventsTest(unittest.TestCase):
    """Новые события трека исков банка (13.08.2026) проходят линтер: одна
    строка с номером на запись, счётчик «ИСКИ БАНКА (N)» сходится."""

    def test_new_bank_events_pass_lint(self):
        bank_changes = [
            make_fi_change(
                ["fi_legal_force_reached", "fi_writ_overdue"],
                {"legal_force_date": "11.07.2026", "overdue_days": 33},
                track="plaintiff_light",
            ),
            make_fi_change(
                ["fi_post_decision_hearing"],
                {"hearing_date": "25.08.2026", "hearing_time": "11:00",
                 "hearing_topic": "индексация присужденных сумм"},
                case="2-101/2026", track="plaintiff_light",
            ),
            make_fi_change(
                ["fi_default_copy_served"],
                {"copy_served_date": "12.08.2026"},
                case="2-102/2026", track="plaintiff_light",
            ),
        ]
        ctx = _ctx(fi_changes=bank_changes)
        html = render(**ctx)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])


if __name__ == "__main__":
    unittest.main()


class LintBankIntakeFoldTest(unittest.TestCase):
    """Свёртка «заведено N новых исков банка» (разгон Урала 14.08.2026).

    Первый боевой прогон авто-подхвата на территории завёл 116 исков за раз, и
    секция «🏦 ИСКИ БАНКА» стала стеной одинаковых строк «взят на мониторинг»
    (HTML 60 КБ). Рендер сворачивает их в одну строку — а линтер обязан знать
    об этом: `_expected_number_alternatives` перебирает ВЕСЬ fi_changes и
    требует каждый номер в HTML, то есть без правки дайджест-паводок просто
    переехал бы в 🩺-алерт на 116 строк «потерян номер дела».
    """

    @staticmethod
    def _intake(n: int) -> list[dict]:
        return [
            make_fi_change(["fi_bank_claim_registered"],
                           case=f"2-{1000 + i}/2026", track="plaintiff_light")
            for i in range(n)
        ]

    def test_folded_numbers_not_expected(self):
        """Свёрнутые дела номеров в HTML не дают — линтер молчит."""
        fi_changes = self._intake(30) + [
            make_fi_change(["fi_writ_issued"], case="2-500/2026",
                           track="plaintiff_light"),
            make_fi_change(["fi_resolved"], case="2-501/2026",
                           track="plaintiff_light"),
        ]
        ctx = _ctx(fi_changes=fi_changes)
        html = render(**ctx)
        self.assertNotIn("2-1000/2026", html)      # свёрнуто
        self.assertIn("2-500/2026", html)          # реальное событие осталось
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])

    def test_missing_number_still_caught(self):
        """Свёртка не глушит линтер вообще: настоящая потеря номера ловится."""
        fi_changes = self._intake(30) + [
            make_fi_change(["fi_writ_issued"], case="2-500/2026",
                           track="plaintiff_light"),
        ]
        ctx = _ctx(fi_changes=fi_changes)
        html = render(**ctx).replace("2-500/2026", "2-999/2026")
        problems = uc.lint_digest_html(html, **ctx)
        self.assertTrue(any("2-500/2026" in p for p in problems), problems)

    def test_section_counter_matches_detailed_rows(self):
        """Заголовок объявляет число ПОДРОБНЫХ дел, свёрнутая строка не в счёт."""
        from court_monitor.digest.lint import _check_section_counters
        fi_changes = self._intake(30) + [
            make_fi_change(["fi_writ_issued"], case="2-500/2026",
                           track="plaintiff_light"),
            make_fi_change(["fi_resolved"], case="2-501/2026",
                           track="plaintiff_light"),
        ]
        html = render(**_ctx(fi_changes=fi_changes))
        self.assertIn("ИСКИ БАНКА (2)", html)
        self.assertEqual(_check_section_counters(html), [])

    def test_below_threshold_untouched(self):
        """Ниже порога — всё как раньше: подельно и с полным счётчиком."""
        ctx = _ctx(fi_changes=self._intake(3))
        html = render(**ctx)
        self.assertIn("ИСКИ БАНКА (3)", html)
        self.assertIn("2-1000/2026", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])


class LintBankForceFoldTest(unittest.TestCase):
    """Свёртка «вступило в силу» и линтер (просьба юриста 21.08.2026).

    В отличие от intake-свёртки номера свёрнутых дел ОСТАЮТСЯ в HTML —
    строка перечисляет их ссылками. Значит `_expected_number_alternatives`
    и `llm._collect_case_numbers` менять не пришлось; задет только счётчик
    секции: он прибавлял 1 за строку, а одна строка теперь несёт N дел.
    """

    @staticmethod
    def _force(n: int) -> list[dict]:
        return [
            make_fi_change(["fi_legal_force_reached"],
                           {"legal_force_date": "21.08.2026"},
                           case=f"2-{700 + i}/2026", track="plaintiff_light")
            for i in range(n)
        ]

    def test_folded_digest_is_clean(self):
        ctx = _ctx(fi_changes=self._force(8))
        html = render(**ctx)
        self.assertIn("вступили в силу", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])

    def test_section_counter_counts_cases_not_rows(self):
        """Заголовок называет ДЕЛА: 8 свёрнутых лежат в одной строке."""
        from court_monitor.digest.lint import _check_section_counters
        ctx = _ctx(fi_changes=self._force(8))
        html = render(**ctx)
        self.assertIn("ИСКИ БАНКА (8)", html)
        self.assertEqual(_check_section_counters(html), [])

    def test_missing_folded_number_still_caught(self):
        """Свёртка не глушит проверку полноты: подмена номера ловится."""
        ctx = _ctx(fi_changes=self._force(8))
        html = render(**ctx).replace("2-700/2026", "2-999/2026")
        problems = uc.lint_digest_html(html, **ctx)
        self.assertTrue(any("2-700/2026" in p for p in problems), problems)

    def test_appeal_tail_number_not_double_counted(self):
        """Регресс счётчика: строка новой апелляции несёт номер 1-й инстанции
        ХВОСТОМ (голым, без <a><b>). Считай линтер все номера подряд — такая
        строка дала бы 2 дела вместо одного."""
        from court_monitor.digest.lint import _check_section_counters
        html = (
            '📥 <b>Новые дела в апелляции (1):</b>\n'
            '\n'
            '<a href="u"><b>33-100/2026</b></a> — Иванов vs Сбербанк '
            '| дело 1 инст. 2-555/2026\n'
        )
        self.assertEqual(_check_section_counters(html), [])


class LintBankOverdueFoldTest(unittest.TestCase):
    """Свёртка «ИЛ не выдан» и линтер (эскалация 21.08.2026).

    Как и у свёртки «вступило в силу», номера свёрнутых дел остаются в HTML —
    строка перечисляет их ссылками. Проверяем, что счётчик секции сходится и
    подмена номера по-прежнему ловится.
    """

    @staticmethod
    def _overdue(days_list: list[int]) -> list[dict]:
        return [
            make_fi_change(["fi_writ_overdue"],
                           {"overdue_days": d,
                            "legal_force_date": "01.06.2026"},
                           case=f"2-{800 + i}/2026", track="plaintiff_light")
            for i, d in enumerate(days_list)
        ]

    def test_folded_digest_is_clean(self):
        ctx = _ctx(fi_changes=self._overdue([171, 157, 99, 97, 85, 80]))
        html = render(**ctx)
        self.assertIn("ждут ИЛ дольше", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])

    def test_section_counter_matches(self):
        from court_monitor.digest.lint import _check_section_counters
        ctx = _ctx(fi_changes=self._overdue([171, 157, 99, 97, 85, 80]))
        html = render(**ctx)
        self.assertIn("ИСКИ БАНКА (6)", html)
        self.assertEqual(_check_section_counters(html), [])

    def test_missing_number_still_caught(self):
        ctx = _ctx(fi_changes=self._overdue([171, 157, 99, 97, 85, 80]))
        html = render(**ctx).replace("2-800/2026", "2-999/2026")
        problems = uc.lint_digest_html(html, **ctx)
        self.assertTrue(any("2-800/2026" in p for p in problems), problems)

    def test_both_folds_together_are_clean(self):
        """Две свёртки в одной секции не мешают друг другу."""
        fi = self._overdue([171, 157, 99, 97]) + [
            make_fi_change(["fi_legal_force_reached"],
                           {"legal_force_date": "21.08.2026"},
                           case=f"2-{900 + i}/2026", track="plaintiff_light")
            for i in range(5)
        ]
        ctx = _ctx(fi_changes=fi)
        html = render(**ctx)
        self.assertIn("ждут ИЛ дольше", html)
        self.assertIn("вступили в силу", html)
        self.assertIn("ИСКИ БАНКА (9)", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])


class LintArchiveFinalEventTest(unittest.TestCase):
    """Клерикальное «дело передано в архив» и линтер (Урал 27.08.2026).

    Рендер гасит fi_final_event про перенос в архив (`_strip_archive_final_events`,
    просьба юриста 07.07.2026), а линтер перебирал ВЕСЬ fi_changes — дело
    2-311/2026 (трек банка, единственное событие дня — архивный перенос,
    стародатный фильтр погасил fi_act_text_published) дало ложный 🩺
    «потерян номер дела» при корректном дайджесте.
    """

    @staticmethod
    def _archive_change(case: str = "2-311/2026") -> dict:
        return make_fi_change(
            ["fi_final_event"],
            {"event": "Дело передано в архив. 16:50. 25.08.2026",
             "last_event": "Дело передано в архив. 16:50. 25.08.2026",
             "event_date": "25.08.2026"},
            case=case, track="plaintiff_light",
        )

    def test_archive_only_change_not_expected(self):
        """Единственный тип — архивный перенос: строки в HTML нет, линтер молчит."""
        ctx = _ctx(fi_changes=[
            self._archive_change(),
            make_fi_change(["fi_writ_issued"], case="2-500/2026",
                           track="plaintiff_light"),
        ])
        html = render(**ctx)
        self.assertNotIn("2-311/2026", html)   # рендер гасит по замыслу
        self.assertIn("2-500/2026", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])

    def test_archive_with_other_type_still_expected(self):
        """Рядом с архивным событием есть настоящее — номер обязателен."""
        ch = self._archive_change()
        ch["type"] = ["fi_writ_issued", "fi_final_event"]
        ch["details"].update({"writ_number": "ФС № 123",
                              "writ_kind": "enforcement"})
        ctx = _ctx(fi_changes=[ch])
        html = render(**ctx)
        self.assertIn("2-311/2026", html)
        self.assertEqual(uc.lint_digest_html(html, **ctx), [])
        broken = html.replace("2-311/2026", "X-000/0000")
        problems = uc.lint_digest_html(broken, **ctx)
        self.assertTrue(any("2-311/2026" in p for p in problems), problems)

    def test_polish_validator_mirrors_the_gate(self):
        """Зеркало в _collect_case_numbers (валидатор полировщика)."""
        from court_monitor.digest import llm as cm_llm
        nums = cm_llm._collect_case_numbers(fi_changes=[
            self._archive_change(),
            make_fi_change(["fi_writ_issued"], case="2-500/2026",
                           track="plaintiff_light"),
        ])
        self.assertNotIn("2-311/2026", nums)
        self.assertIn("2-500/2026", nums)
