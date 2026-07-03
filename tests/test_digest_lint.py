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
        # Обрезка — одна общая проблема; потерянные номера не перечисляем.
        many = [make_fi_new_case(case=f"2-{8000 + i}/2026") for i in range(80)]
        ctx = _ctx(fi_new_cases=many)
        html = render(**ctx)
        self.assertIn("сообщение обрезано", html)
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


if __name__ == "__main__":
    unittest.main()
