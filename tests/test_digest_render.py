"""Baseline-тесты для generate_template_digest.

Зафиксированы инварианты, которые миграция к гибридной архитектуре
(программный рендер + LLM на пересказ act_text) не должна сломать:
контракт <a><b>номер</b></a> для attach_act_analyses, лимит длины
Telegram, присутствие всех номеров дел, идемпотентность, отсутствие
Markdown-артефактов.

Запуск: `python3 -m unittest tests.test_digest_render` из корня репо.
"""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import update_cases as uc  # noqa: E402

LAST_CTX_PATH = os.path.join(REPO_ROOT, "data", "last_digest_context.json")


def _load_ctx(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _ctx_to_kwargs(ctx):
    return {
        "new_cases": ctx.get("new_cases") or [],
        "changes": ctx.get("changes") or [],
        "cases": ctx.get("cases") or [],
        "fi_new_cases": ctx.get("fi_new_cases") or [],
        "stage_transitions": ctx.get("stage_transitions") or [],
        "fi_changes": ctx.get("fi_changes") or [],
        "total_active_appeal": ctx.get("total_active_appeal") or 0,
        "total_active_fi": ctx.get("total_active_fi") or 0,
        "total_active_cassation": ctx.get("total_active_cassation") or 0,
        "cass_changes": ctx.get("cass_changes") or [],
        "cass_discovered": ctx.get("cass_discovered") or [],
    }


class TemplateDigestBaselineTest(unittest.TestCase):
    """На сегодняшнем data/last_digest_context.json."""

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(LAST_CTX_PATH):
            raise unittest.SkipTest(f"Нет снимка {LAST_CTX_PATH}")
        cls.ctx = _load_ctx(LAST_CTX_PATH)
        cls.kwargs = _ctx_to_kwargs(cls.ctx)
        cls.html = uc.generate_template_digest(**cls.kwargs)

    def test_html_non_empty(self):
        self.assertTrue(self.html, "Шаблонный дайджест пуст")
        self.assertGreater(len(self.html), 200)

    def test_contract_anchor_bold(self):
        # Контракт для attach_act_analyses: каждый абзац дела начинается
        # с <a><b>номер</b></a>. Без него фронтовый drawer не получит
        # разбор акта.
        self.assertRegex(self.html, r"<a[^>]*><b>[^<]+</b></a>")

    def test_telegram_length_limit(self):
        # truncate_html_message режет до 2*4096 — не должны превысить.
        self.assertLessEqual(len(self.html), uc.TELEGRAM_MSG_LIMIT * 2)

    def test_idempotent(self):
        html2 = uc.generate_template_digest(**self.kwargs)
        self.assertEqual(self.html, html2,
                         "Шаблонный дайджест неидемпотентен")

    def test_all_case_numbers_present(self):
        nums = set()
        for c in self.ctx.get("new_cases") or []:
            n = (c.get("Номер дела") or "").strip()
            if n:
                nums.add(n)
        for c in self.ctx.get("fi_new_cases") or []:
            n = (c.get("id") or "").strip()
            if n:
                nums.add(n)
        for ch in self.ctx.get("changes") or []:
            n = (ch.get("case") or "").strip()
            if n:
                nums.add(n)
        for ch in self.ctx.get("fi_changes") or []:
            n = (ch.get("case") or "").strip()
            if n:
                nums.add(n)
        for ch in self.ctx.get("cass_changes") or []:
            n = (ch.get("case") or "").strip()
            if n:
                nums.add(n)
        if not nums:
            self.skipTest("В контексте нет номеров дел")
        present = sum(1 for n in nums if n in self.html)
        ratio = present / len(nums)
        # Допускаем небольшую усушку при truncate_html_message,
        # но >=70% номеров должны быть в HTML.
        self.assertGreaterEqual(
            ratio, 0.7,
            f"Слишком мало номеров в HTML: {present}/{len(nums)}",
        )

    def test_dashboard_link_present(self):
        self.assertIn(uc.DASHBOARD_URL, self.html)

    def test_no_markdown_headers(self):
        # Шаблон не должен оставлять Markdown — Telegram parse_mode=HTML
        # их не понимает.
        for line in self.html.split("\n"):
            self.assertFalse(
                line.lstrip().startswith("#"),
                f"Markdown-заголовок: {line!r}",
            )

    def test_no_double_asterisks(self):
        self.assertNotIn("**", self.html)


class EmptyContextTest(unittest.TestCase):
    def test_empty_renders_quiet(self):
        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=10,
            total_active_fi=20,
            total_active_cassation=2,
        )
        self.assertIn("изменений не было", html)
        self.assertIn(uc.DASHBOARD_URL, html)


class FiNewCaseSyntheticTest(unittest.TestCase):
    """Минимальный синтетический контекст: одно новое дело 1-й инст.
    Проверяем, что шаблон рендерит именно его секцию и контракт ссылки.
    """

    def setUp(self):
        self.fi_case = {
            "id": "2-9999/2026",
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.",
            "category": "Кредитный договор",
            "bank_role": "Истец",
            "first_instance": {
                "case_number": "2-9999/2026",
                "court": "Тестовый суд",
                "filing_date": "01.05.2026",
                "judge": "Тестов Т.Т.",
                "link": "12345|abcd-1234",
            },
        }

    def test_renders_fi_new_case(self):
        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[self.fi_case],
            fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=0, total_active_fi=1, total_active_cassation=0,
        )
        self.assertIn("2-9999/2026", html)
        self.assertIn("ПЕРВАЯ ИНСТАНЦИЯ", html)
        self.assertIn("Новые иски", html)
        # Дата подачи — отдельная строка с эмодзи 📥 ПОСЛЕ <b>дата</b>.
        self.assertIn("01.05.2026", html)


class BuildActSummaryPromptTest(unittest.TestCase):
    def test_includes_full_metadata(self):
        prompt = uc._build_act_summary_prompt(
            "Мотивировочная часть акта. " * 10,
            {
                "stage": "appeal",
                "bank_role": "Истец",
                "plaintiff": "ПАО Сбербанк",
                "defendant": "Иванов И.И.",
                "verdict_label": "оставлено без изменения",
                "category": "Кредитный договор",
            },
        )
        self.assertIn("апелляционное определение", prompt)
        self.assertIn("Истец", prompt)
        self.assertIn("ПАО Сбербанк", prompt)
        self.assertIn("Иванов И.И.", prompt)
        self.assertIn("оставлено без изменения", prompt)
        self.assertIn("Кредитный договор", prompt)
        self.assertIn("Мотивировочная часть", prompt)

    def test_uses_default_kind_when_unknown_stage(self):
        prompt = uc._build_act_summary_prompt("Текст. " * 30, {})
        self.assertIn("судебный акт", prompt)
        self.assertIn("Текст", prompt)

    def test_first_instance_kind(self):
        prompt = uc._build_act_summary_prompt(
            "Текст. " * 30, {"stage": "first_instance"}
        )
        self.assertIn("решение суда первой инстанции", prompt)

    def test_cassation_kind(self):
        prompt = uc._build_act_summary_prompt(
            "Текст. " * 30, {"stage": "cassation"}
        )
        self.assertIn("кассационное определение", prompt)


class CleanSummaryTest(unittest.TestCase):
    def test_strips_quotes(self):
        self.assertEqual(uc._clean_summary('"Резюме текста."'), "Резюме текста.")
        self.assertEqual(uc._clean_summary("«Текст»"), "Текст")

    def test_strips_prefixes(self):
        self.assertEqual(uc._clean_summary("Кратко: текст."), "текст.")
        self.assertEqual(uc._clean_summary("Резюме — суть."), "суть.")
        self.assertEqual(uc._clean_summary("Итого: вывод"), "вывод")

    def test_keeps_word_when_not_prefix(self):
        # «Резюме текста.» без двоеточия — это часть осмысленного
        # предложения, не префикс. Не должны срезать.
        self.assertEqual(
            uc._clean_summary("Резюме текста."), "Резюме текста."
        )

    def test_strips_code_fence(self):
        self.assertEqual(uc._clean_summary("```\nтекст\n```"), "текст")
        self.assertEqual(uc._clean_summary("```html\nтекст\n```"), "текст")

    def test_passthrough_clean_text(self):
        self.assertEqual(
            uc._clean_summary("Суд отказал в удовлетворении иска."),
            "Суд отказал в удовлетворении иска.",
        )


class SummarizeActMotivationTest(unittest.TestCase):
    def test_short_text_returns_none_without_llm_call(self):
        called = []

        def fake_claude(prompt, **kw):
            called.append(prompt)
            return "should not be called"

        with patch.object(uc, "_call_claude_simple", fake_claude):
            self.assertIsNone(
                uc.summarize_act_motivation(
                    "короткий", case_meta={"stage": "appeal"},
                    use_cache=False,
                )
            )
            self.assertIsNone(
                uc.summarize_act_motivation(
                    "", case_meta={"stage": "appeal"}, use_cache=False,
                )
            )
        self.assertEqual(called, [])

    def test_calls_llm_and_caches(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tf:
            cache_path = tf.name
        os.unlink(cache_path)  # пусть функция создаст с нуля
        calls: list[str] = []

        def fake_claude(prompt, **kw):
            calls.append(prompt)
            return "Тестовый пересказ."

        try:
            with patch.object(uc, "ACT_SUMMARIES_PATH", cache_path), \
                 patch.object(uc, "_call_claude_simple", fake_claude), \
                 patch.object(uc, "ANTHROPIC_API_KEY", "fake-key"), \
                 patch.object(uc, "LLM_PROVIDER", "claude"):
                act_text = "Мотивировочная часть акта. " * 10
                meta = {"stage": "appeal", "bank_role": "Истец"}
                s1 = uc.summarize_act_motivation(act_text, case_meta=meta)
                self.assertEqual(s1, "Тестовый пересказ.")
                self.assertEqual(len(calls), 1)
                # Второй вызов — берём из кэша.
                s2 = uc.summarize_act_motivation(act_text, case_meta=meta)
                self.assertEqual(s2, "Тестовый пересказ.")
                self.assertEqual(len(calls), 1, "LLM должен был быть кэширован")
                # Кэш-файл реально создан.
                self.assertTrue(os.path.exists(cache_path))
        finally:
            for p in (cache_path, cache_path + ".tmp"):
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass

    def test_returns_none_on_llm_failure(self):
        with patch.object(uc, "_call_claude_simple", lambda p, **kw: None), \
             patch.object(uc, "ANTHROPIC_API_KEY", "fake-key"), \
             patch.object(uc, "LLM_PROVIDER", "claude"):
            result = uc.summarize_act_motivation(
                "Мотивировочная часть. " * 10,
                case_meta={"stage": "appeal"},
                use_cache=False,
            )
            self.assertIsNone(result)

    def test_cleans_summary_before_returning(self):
        def fake_claude(prompt, **kw):
            return '"Кратко: суд отказал."'

        with patch.object(uc, "_call_claude_simple", fake_claude), \
             patch.object(uc, "ANTHROPIC_API_KEY", "fake-key"), \
             patch.object(uc, "LLM_PROVIDER", "claude"):
            result = uc.summarize_act_motivation(
                "Мотивировочная часть. " * 10,
                case_meta={"stage": "appeal"},
                use_cache=False,
            )
            self.assertEqual(result, "суд отказал.")


class TemplateDigestSummarizerIntegrationTest(unittest.TestCase):
    """Этап 3b: act_summarizer в generate_template_digest. Проверяем,
    что summarizer вызывается для секций 3.6 / 5.5 / касс. new_act
    и его результат попадает в HTML.
    """

    def _fi_resolved_change_with_act_text(self):
        return {
            "case": "2-1234/2026",
            "court": "Тестовый суд",
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.",
            "bank_role": "Истец",
            "type": ["fi_resolved", "fi_act_text_published"],
            "details": {
                "verdict_label": "иск удовлетворён",
                "raw_result": "Иск удовлетворён",
                "decision_date": "01.05.2026",
                "category": "Кредитный договор",
                "act_text": "Мотивировочная часть. " * 30,
                "act_date": "10.05.2026",
                "bank_outcome": "в пользу банка",
            },
        }

    def _appeal_act_change(self):
        return {
            "case": "33-5678/2026",
            "type": ["new_act"],
            "details": {
                "case_url": "https://example.com/appeal",
                "act_text": "Апелляционная мотивировка. " * 30,
                "act_date": "10.05.2026",
                "act_verdict_label": "оставлено без изменения",
                "plaintiff": "ПАО Сбербанк",
                "defendant": "Петров П.П.",
                "role": "Истец",
                "category": "Кредит",
            },
        }

    def _cass_change_with_act(self):
        return {
            "case": "2-9999/2025",
            "cassation_internal_number": "88-1234/2026",
            "type": ["new_cassation", "new_act"],
            "details": {
                "stage_prev": "cassation_pending",
                "stage_now": "cassation",
                "outcome": "cassation_upheld",
                "result_text": "Жалоба отклонена",
                "result_for_appeal": "БЕЗ ИЗМЕНЕНИЯ",
                "act_text": "Кассационная мотивировка. " * 30,
                "act_date": "12.05.2026",
                "appellant_is_bank": False,
                "plaintiff": "ПАО Сбербанк",
                "defendant": "Сидоров С.С.",
                "bank_role": "Истец",
                "category": "Кредит",
            },
        }

    def test_summarizer_used_for_fi_act_text(self):
        called = []

        def fake_summarizer(act_text, *, case_meta):
            called.append((act_text, case_meta))
            return "TEST_FI_SUMMARY"

        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[],
            fi_changes=[self._fi_resolved_change_with_act_text()],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=0, total_active_fi=1, total_active_cassation=0,
            act_summarizer=fake_summarizer,
        )
        self.assertEqual(len(called), 1, "summarizer должен быть вызван 1 раз")
        _, meta = called[0]
        self.assertEqual(meta["stage"], "first_instance")
        self.assertEqual(meta["bank_role"], "Истец")
        self.assertIn("TEST_FI_SUMMARY", html)
        # Сырой excerpt в этой строке быть не должен.
        self.assertNotIn("Мотивировочная часть. Мотивировочная", html)

    def test_summarizer_used_for_appeal_act(self):
        called = []

        def fake_summarizer(act_text, *, case_meta):
            called.append((act_text, case_meta))
            return "TEST_APPEAL_SUMMARY"

        html = uc.generate_template_digest(
            new_cases=[], changes=[self._appeal_act_change()],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
            act_summarizer=fake_summarizer,
        )
        self.assertEqual(len(called), 1)
        _, meta = called[0]
        self.assertEqual(meta["stage"], "appeal")
        self.assertIn("TEST_APPEAL_SUMMARY", html)

    def test_summarizer_used_for_cassation_act(self):
        called = []

        def fake_summarizer(act_text, *, case_meta):
            called.append((act_text, case_meta))
            return "TEST_CASS_SUMMARY"

        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[self._cass_change_with_act()],
            cass_discovered=[],
            total_active_appeal=0, total_active_fi=0, total_active_cassation=1,
            act_summarizer=fake_summarizer,
        )
        self.assertEqual(len(called), 1)
        _, meta = called[0]
        self.assertEqual(meta["stage"], "cassation")
        self.assertIn("TEST_CASS_SUMMARY", html)

    def test_summarizer_failure_falls_back_to_excerpt(self):
        # Если summarizer возвращает None (LLM упал), берём excerpt.
        def failing_summarizer(act_text, *, case_meta):
            return None

        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[],
            fi_changes=[self._fi_resolved_change_with_act_text()],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=0, total_active_fi=1, total_active_cassation=0,
            act_summarizer=failing_summarizer,
        )
        # Excerpt мотивировки должен быть в HTML.
        self.assertIn("Мотивировочная часть", html)

    def test_summarizer_exception_falls_back_to_excerpt(self):
        # Любая ошибка callable не должна валить рендер.
        def crashing_summarizer(act_text, *, case_meta):
            raise RuntimeError("boom")

        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[],
            fi_changes=[self._fi_resolved_change_with_act_text()],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=0, total_active_fi=1, total_active_cassation=0,
            act_summarizer=crashing_summarizer,
        )
        self.assertIn("Мотивировочная часть", html)

    def test_no_summarizer_keeps_legacy_behavior(self):
        # Без act_summarizer (по умолчанию None) — старая логика.
        # Не должен ничего изменить в выводе по сравнению с baseline.
        change = self._fi_resolved_change_with_act_text()
        html = uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[],
            fi_changes=[change],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=0, total_active_fi=1, total_active_cassation=0,
        )
        self.assertIn("Мотивировочная часть", html)
        self.assertIn("2-1234/2026", html)


class GenerateDigestEntryPointTest(unittest.TestCase):
    """Этап 4: generate_digest по умолчанию идёт в гибрид; старая
    ветка работает только при DIGEST_FULL_LLM=1.
    """

    def setUp(self):
        if not os.path.exists(LAST_CTX_PATH):
            self.skipTest(f"Нет снимка {LAST_CTX_PATH}")
        ctx = _load_ctx(LAST_CTX_PATH)
        self.kwargs = _ctx_to_kwargs(ctx)

    def test_default_uses_hybrid_path(self):
        # В гибридном режиме summarize_act_motivation должна быть
        # вызвана (если есть акты с текстом), а HTML — совпадать с
        # тем, что выдаёт generate_template_digest.
        called: list = []

        def fake_summarize(act_text, *, case_meta):
            called.append(case_meta.get("stage"))
            return "TEST_HYBRID_SUMMARY"

        with patch.object(uc, "DIGEST_FULL_LLM", False), \
             patch.object(uc, "summarize_act_motivation", fake_summarize):
            html = uc.generate_digest(**self.kwargs)
        self.assertTrue(html)
        # Контракт абзацев не должен быть нарушен.
        self.assertRegex(html, r"<a[^>]*><b>[^<]+</b></a>")
        self.assertIn(uc.DASHBOARD_URL, html)
        # Если в контексте были акты с текстом — пересказ должен
        # подставиться. Если их нет — нормально, fake_summarize не
        # был вызван. Проверяем мягко: либо нет актов, либо подмена
        # сработала.
        if called:
            self.assertIn("TEST_HYBRID_SUMMARY", html)

    def test_full_llm_flag_skips_hybrid_when_no_keys(self):
        # При DIGEST_FULL_LLM=1 и без API-ключей: старая логика
        # сразу падает обратно в generate_template_digest (без
        # act_summarizer — то есть legacy excerpt). Это обеспечивает
        # обратную совместимость, если ключ не настроен.
        with patch.object(uc, "DIGEST_FULL_LLM", True), \
             patch.object(uc, "ANTHROPIC_API_KEY", ""), \
             patch.object(uc, "LLM_PROVIDER", "claude"):
            html = uc.generate_digest(**self.kwargs)
        self.assertTrue(html)
        self.assertIn(uc.DASHBOARD_URL, html)


if __name__ == "__main__":
    unittest.main()
