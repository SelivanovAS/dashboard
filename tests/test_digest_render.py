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
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import update_cases as uc  # noqa: E402
# Конфиг-константы патчатся на модуле-доме: код читает их как config.X,
# патч фасада uc.X до чтений не доходит (см. docs/Распил_монолита_контекст.md).
from court_monitor import config as cm_config  # noqa: E402
# LLM-функции — тоже на модуле-доме: вызовы идут как llm.X(...).
from court_monitor.digest import llm as cm_llm  # noqa: E402

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

    def test_telegram_split_parts_fit_limit(self):
        # HTML дайджест больше не обрезаем: send_telegram раскладывает его на
        # сообщения через split_message — каждая часть обязана влезать в лимит,
        # и маркер обрезки не должен появляться.
        self.assertNotIn("сообщение обрезано", self.html)
        for part in uc.split_message(self.html, uc.TELEGRAM_MSG_LIMIT):
            self.assertLessEqual(len(part), uc.TELEGRAM_MSG_LIMIT)

    def test_idempotent(self):
        html2 = uc.generate_template_digest(**self.kwargs)
        self.assertEqual(self.html, html2,
                         "Шаблонный дайджест неидемпотентен")

    def test_all_case_numbers_present(self):
        # Каждый элемент — множество равнозначных представлений номера:
        # дело «присутствует», если в HTML есть хотя бы одно из них. Для
        # касс. событий рендер по просьбе юриста выводит кассационный номер
        # (8Г-…) вместо номера 1-й инст. — принимаем оба.
        expected: list[set] = []
        for c in self.ctx.get("new_cases") or []:
            n = (c.get("Номер дела") or "").strip()
            if n:
                expected.append({n})
        for c in self.ctx.get("fi_new_cases") or []:
            n = (c.get("id") or "").strip()
            if n:
                expected.append({n})
        for ch in self.ctx.get("changes") or []:
            n = (ch.get("case") or "").strip()
            if n:
                expected.append({n})
        # Ожидаемый список считаем ПО ТЕМ ЖЕ фильтрам, что применяет рендер,
        # иначе тест ловит не дефект, а штатное поведение:
        # • `_strip_archive_final_events` (с 29.07.2026) выбрасывает change,
        #   у которого единственное событие — административное «дело передано
        #   в архив» (кейс 2-345/2026, контекст ХМАО 14.08.2026);
        # • `split_bank_intake_fold` (с 14.08.2026) сворачивает массовые
        #   заведения исков банка — их номеров в HTML нет по замыслу
        #   (в контексте Урала 14.08 таких 116, baseline падал бы каждый
        #   день разгона).
        from court_monitor.digest.template import (
            _strip_archive_final_events, split_bank_intake_fold,
        )
        fi_changes = _strip_archive_final_events(
            self.ctx.get("fi_changes") or [])
        folded_ids = {id(ch) for ch in split_bank_intake_fold(
            [ch for ch in fi_changes if ch.get("track")])[1]}
        for ch in fi_changes:
            if id(ch) in folded_ids:
                continue
            n = (ch.get("case") or "").strip()
            if n:
                expected.append({n})
        for ch in self.ctx.get("cass_changes") or []:
            alts = {
                (ch.get("case") or "").strip(),
                (ch.get("cassation_internal_number") or "").strip(),
            } - {""}
            if alts:
                expected.append(alts)
        if not expected:
            self.skipTest("В контексте нет номеров дел")
        missing = [alts for alts in expected
                   if not any(n in self.html for n in alts)]
        # После фиксов покрытия (голый status_change, «ложный» new_result)
        # на реальных объёмах дайджест обязан нести ВСЕ номера. HTML больше
        # не обрезаем — потеря номера означала бы дефект рендера, а не лимит.
        self.assertFalse(
            missing,
            f"Потеряны номера ({len(missing)}/{len(expected)}): {missing}",
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
        # Ловим руны РОВНО из двух звёздочек — Markdown-жирность (**текст**),
        # которой в Telegram-HTML быть не должно. Более длинные руны (****) —
        # обезличивание персональных данных самим судом в текстах актов; они
        # легитимно попадают в дайджест с сырыми excerpt'ами мотивировок и
        # Markdown'ом не являются (падение CI Урала 17.07.2026: 34 «****»
        # в last_digest_context.json).
        m = re.search(r"(?<!\*)\*\*(?!\*)", self.html)
        if m:
            frag = self.html[max(0, m.start() - 40):m.start() + 42]
            self.fail(f"Markdown «**» в дайджесте: …{frag!r}…")


class EmptyContextTest(unittest.TestCase):
    """Ветка «за день изменений не было» (render_no_changes_digest).

    Тест обязан быть независим от data/last_digest.json: это файл ДАННЫХ, и в
    форке территории там лежит её дайджест со своей ссылкой на дашборд. Раньше
    тест читал его молча: в эталоне проходил случайно (ссылку давал приклеенный
    ХМАО-дайджест, а не сама «тихая» ветка), а в форке Урала падал — conftest
    прибивает тесты к hmao, и ХМАО-ссылка в уральском дайджесте не находилась.
    Поэтому обе ветки проверяем на своей LAST_DIGEST_PATH.
    """

    def _render(self):
        return uc.generate_template_digest(
            new_cases=[], changes=[],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=10,
            total_active_fi=20,
            total_active_cassation=2,
        )

    def test_empty_renders_quiet(self):
        # Прошлого дайджеста нет → «тихая» ветка даёт СВОЮ ссылку на дашборд.
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "last_digest.json")
            with patch.object(cm_config, "LAST_DIGEST_PATH", missing):
                html = self._render()
        self.assertIn("изменений не было", html)
        self.assertIn(cm_config.DASHBOARD_URL, html)

    def test_empty_attaches_previous_digest(self):
        # Прошлый дайджест есть → приклеивается блоком «Предыдущий дайджест».
        # Эту ветку раньше покрывал случайный вывод из data/last_digest.json.
        prev = {
            "generated_at": "2026-07-15T03:45:00",
            "html": "<b>ПРОШЛЫЙ ДАЙДЖЕСТ</b>",
            "is_empty": False,
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "last_digest.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(prev, f, ensure_ascii=False)
            with patch.object(cm_config, "LAST_DIGEST_PATH", path):
                html = self._render()
        self.assertIn("изменений не было", html)
        self.assertIn("Предыдущий дайджест", html)
        self.assertIn("15.07.2026", html)
        self.assertIn("<b>ПРОШЛЫЙ ДАЙДЖЕСТ</b>", html)


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

    def test_detailed_format_and_language(self):
        # Контракт «v3-detailed»: 2-3 предложения, лимит 450, русский язык.
        prompt = uc._build_act_summary_prompt("Текст. " * 30, {})
        self.assertIn("2-3 предложениями", prompt)
        self.assertIn("450", prompt)
        self.assertIn("на русском языке", prompt)

    def test_answer_anchor_is_last_after_act_text(self):
        # Якорь формата — последняя строка промпта, ПОСЛЕ текста акта
        # (слабые модели следуют ближайшей инструкции).
        act = "Уникальный текст акта для проверки якоря. " * 5
        prompt = uc._build_act_summary_prompt(act, {})
        self.assertTrue(prompt.endswith("Ответ (2-3 предложения):"))
        self.assertLess(prompt.rfind(act.strip()),
                        prompt.rfind("Ответ (2-3 предложения):"))


class CleanSummaryTest(unittest.TestCase):
    def test_strips_quotes(self):
        self.assertEqual(uc._clean_summary('"Резюме текста."'), "Резюме текста.")
        self.assertEqual(uc._clean_summary("«Текст»"), "Текст")

    def test_strips_prefixes(self):
        self.assertEqual(uc._clean_summary("Кратко: текст."), "текст.")
        self.assertEqual(uc._clean_summary("Резюме — суть."), "суть.")
        self.assertEqual(uc._clean_summary("Итого: вывод"), "вывод")

    def test_strips_answer_anchor_echo(self):
        # Эхо якоря промпта «Ответ (2-3 предложения):» и голое «Ответ:».
        self.assertEqual(
            uc._clean_summary("Ответ (2-3 предложения): Иск удовлетворён."),
            "Иск удовлетворён.",
        )
        self.assertEqual(
            uc._clean_summary("Ответ: Иск удовлетворён."),
            "Иск удовлетворён.",
        )

    def test_strips_vot_preamble_same_line(self):
        self.assertEqual(
            uc._clean_summary("Вот краткий пересказ: Иск удовлетворён."),
            "Иск удовлетворён.",
        )

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

    def test_strips_closed_think_block(self):
        # Reasoning-модели OpenRouter (DeepSeek R1): размышления в content.
        self.assertEqual(
            uc._clean_summary(
                "<think>Надо посмотреть, кто выиграл...</think>"
                "Иск удовлетворён, доводы ответчика отклонены."
            ),
            "Иск удовлетворён, доводы ответчика отклонены.",
        )

    def test_unclosed_think_block_is_garbage(self):
        # Обрыв по лимиту токенов посреди размышлений → мусор → откат.
        self.assertEqual(
            uc._clean_summary("<think>Так, сначала посмотрим на дело"),
            "",
        )

    def test_orphan_close_tag_takes_tail(self):
        # Провайдер срезал открывающий тег: «размышления…</think>ответ».
        self.assertEqual(
            uc._clean_summary(
                "Кто тут прав? Посмотрим.</think>Иск удовлетворён."
            ),
            "Иск удовлетворён.",
        )

    def test_unwraps_markdown(self):
        self.assertEqual(
            uc._clean_summary(
                "**Иск удовлетворён**, доводы *ответчика* отклонены, "
                "`расчёт` верен."
            ),
            "Иск удовлетворён, доводы ответчика отклонены, расчёт верен.",
        )

    def test_multiline_preamble_and_join(self):
        # Преамбула отдельной строкой отбрасывается, переносы склеиваются.
        self.assertEqual(
            uc._clean_summary(
                "Вот пересказ мотивировки:\n"
                "Иск удовлетворён.\nДоводы ответчика отклонены."
            ),
            "Иск удовлетворён. Доводы ответчика отклонены.",
        )

    def test_non_russian_answer_is_garbage(self):
        self.assertEqual(
            uc._clean_summary(
                "The court dismissed the claim because the plaintiff "
                "failed to prove ownership."
            ),
            "",
        )

    def test_overlong_trimmed_at_sentence_boundary(self):
        sent = "Довод ответчика о пропуске срока исковой давности отклонён. "
        cleaned = uc._clean_summary(sent * 20)  # ~1200 символов
        self.assertLessEqual(len(cleaned), cm_llm._SUMMARY_HARD_LIMIT)
        self.assertTrue(cleaned.endswith("отклонён."))

    def test_overlong_without_boundary_is_garbage(self):
        self.assertEqual(uc._clean_summary("а" * 700), "")


class SummarizeActMotivationTest(unittest.TestCase):
    def test_short_text_returns_none_without_llm_call(self):
        called = []

        def fake_claude(prompt, **kw):
            called.append(prompt)
            return "should not be called"

        with patch.object(cm_llm, "_call_claude_simple", fake_claude):
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
            with patch.object(cm_config, "ACT_SUMMARIES_PATH", cache_path), \
                 patch.object(cm_llm, "_call_claude_simple", fake_claude), \
                 patch.object(cm_config, "ANTHROPIC_API_KEY", "fake-key"), \
                 patch.object(cm_config, "LLM_PROVIDER", "claude"):
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
        with patch.object(cm_llm, "_call_claude_simple", lambda p, **kw: None), \
             patch.object(cm_config, "ANTHROPIC_API_KEY", "fake-key"), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.summarize_act_motivation(
                "Мотивировочная часть. " * 10,
                case_meta={"stage": "appeal"},
                use_cache=False,
            )
            self.assertIsNone(result)

    def test_cleans_summary_before_returning(self):
        def fake_claude(prompt, **kw):
            return '"Кратко: суд отказал."'

        with patch.object(cm_llm, "_call_claude_simple", fake_claude), \
             patch.object(cm_config, "ANTHROPIC_API_KEY", "fake-key"), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
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


class AppealResultDeduplicationTest(unittest.TestCase):
    """Δ2: дело с одновременно `new_event` и `new_result` не должно
    появляться в «Назначенные заседания» — только в «Вынесенные акты».
    Регресс относительно Claude-варианта (08.05.2026 после Δ1).
    """

    def _change_with_event_and_result(self):
        return {
            "case": "33-3138/2026",
            "type": ["status_change", "new_event", "new_result"],
            "details": {
                "old_status": "В производстве",
                "new_status": "Решено",
                "event": "Судебное заседание. 12:20. Зал 140. Вынесено решение. ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ. 16.04.2026",
                "event_date": "05.05.2026",
                "hearing_date": "05.05.2026",
                "hearing_time": "12:20",
                "result": "ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ",
                "verdict_label": "решение оставлено без изменения, жалоба — без удовлетворения",
                "plaintiff": "Магадиев М.Г.",
                "defendant": "ДСК-1",
                "role": "Третье лицо",
                "category": "Иные жилищные споры",
                "case_url": "https://example.com/case",
                "last_event": "Судебное заседание. 12:20. Зал 140. Вынесено решение. ОПРЕДЕЛЕНИЕ оставлено БЕЗ ИЗМЕНЕНИЯ. 16.04.2026",
                "bank_outcome": "нейтрально (банк — третье лицо)",
            },
        }

    def test_change_with_new_result_not_in_scheduled_section(self):
        html = uc.generate_template_digest(
            new_cases=[],
            changes=[self._change_with_event_and_result()],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
        )
        # Должно быть в «Вынесенные акты», не в «Назначенные заседания».
        self.assertIn("Вынесенные акты", html)
        self.assertNotIn("Назначенные заседания", html)
        self.assertIn("33-3138/2026", html)

    def test_no_prichina_line_in_acts_section(self):
        html = uc.generate_template_digest(
            new_cases=[],
            changes=[self._change_with_event_and_result()],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
        )
        # «Причина: ...» в Claude-варианте не появлялась — это просто
        # дубль last_event. Убрали из шаблона.
        self.assertNotIn("Причина:", html)

    def test_pure_new_event_still_in_scheduled(self):
        # Если у дела ТОЛЬКО new_event (без new_result) — оно по-прежнему
        # должно идти в секцию «📅 Изменения» строкой заседания (с
        # 30.07.2026 для прошедшей даты — 📌-цитатой факта), а не в
        # «🔁 Заседание отложено на». Бывшая
        # отдельная секция «Назначенные заседания» (5.3) объединена с
        # «Отложенные» (5.2) в одну «Изменения» — см. combined_apel_changes
        # в generate_template_digest и комментарий «бывшие "Отложенные" 5.2
        # и "Назначенные" 5.3 объединены» в GIGACHAT_SYSTEM_PROMPT.
        change = {
            "case": "33-9999/2026",
            "type": ["new_event"],
            "details": {
                "event": "Судебное заседание. 14:00. Зал 1. 20.05.2026",
                "event_date": "20.05.2026",
                "hearing_date": "20.05.2026",
                "hearing_time": "14:00",
                "plaintiff": "Истец И.И.",
                "defendant": "Ответчик О.О.",
                "role": "Истец",
                "case_url": "https://example.com/case2",
            },
        }
        html = uc.generate_template_digest(
            new_cases=[], changes=[change],
            fi_new_cases=[], fi_changes=[],
            cass_changes=[], cass_discovered=[],
            total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
        )
        self.assertIn("📅 <b>Изменения", html)
        self.assertIn("33-9999/2026", html)
        # Дата фикстуры (20.05.2026) в прошлом: гард 30.07.2026 не даёт
        # объявить прошедшее заседание «назначенным» — вместо этого факт
        # 📌-цитатой с датой события. Строка дела в «Изменениях» остаётся.
        self.assertIn("📌 Судебное заседание (20.05.2026)", html)
        self.assertNotIn("Заседание назначено на", html)
        self.assertNotIn("Заседание отложено", html)


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

        with patch.object(cm_config, "DIGEST_FULL_LLM", False), \
             patch.object(cm_llm, "summarize_act_motivation", fake_summarize):
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
        with patch.object(cm_config, "DIGEST_FULL_LLM", True), \
             patch.object(cm_config, "ANTHROPIC_API_KEY", ""), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            html = uc.generate_digest(**self.kwargs)
        self.assertTrue(html)
        self.assertIn(uc.DASHBOARD_URL, html)


class CollectCaseNumbersTest(unittest.TestCase):
    def test_collects_from_all_sources(self):
        nums = uc._collect_case_numbers(
            new_cases=[{"Номер дела": "33-100/2026"}],
            changes=[{"case": "33-200/2026"}],
            fi_new_cases=[{"id": "2-300/2026"}],
            fi_changes=[{"case": "2-400/2026"}],
            cass_changes=[{"case": "2-500/2025"}],
            cass_discovered=[{
                "id": "2-600/2025",
                "cassation": {"case_number": "8Г-9999/2026"},
            }],
        )
        self.assertEqual(
            nums,
            {"33-100/2026", "33-200/2026", "2-300/2026",
             "2-400/2026", "2-500/2025", "8Г-9999/2026"},
        )

    def test_cass_change_prefers_internal_number(self):
        # Шаблон рендерит касс. номер (8Г-…), а не номер 1-й инст. —
        # валидатор должен требовать видимый в HTML номер.
        nums = uc._collect_case_numbers(
            cass_changes=[{
                "case": "2-501/2025",
                "cassation_internal_number": "8Г-77/2026",
            }],
        )
        self.assertEqual(nums, {"8Г-77/2026"})

    def test_handles_empty_inputs(self):
        self.assertEqual(uc._collect_case_numbers(), set())

    def test_skips_blank_numbers(self):
        nums = uc._collect_case_numbers(
            changes=[{"case": ""}, {"case": "   "}, {"case": "OK-1"}],
        )
        self.assertEqual(nums, {"OK-1"})


class ValidatePolishedHtmlTest(unittest.TestCase):
    def setUp(self):
        self.draft = (
            f'<a href="u"><b>33-100/2026</b></a> текст\n\n'
            f'<a href="https://selivanovas.github.io/dashboard/sberbank_dashboard.html">📊</a>'
        )
        self.expected = {"33-100/2026"}
        self.max_len = uc.TELEGRAM_MSG_LIMIT * 2

    def _run(self, polished):
        return uc._validate_polished_html(
            polished,
            draft=self.draft,
            expected_case_numbers=self.expected,
            max_length=self.max_len,
        )

    def test_accepts_clean_html(self):
        polished = (
            f'<a href="u"><b>33-100/2026</b></a> текст с правкой\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>'
        )
        ok, reason = self._run(polished)
        self.assertTrue(ok, reason)

    def test_rejects_empty(self):
        ok, reason = self._run("")
        self.assertFalse(ok)
        self.assertIn("пустой", reason.lower())

    def test_rejects_too_long(self):
        polished = "x" * (self.max_len + 100)
        ok, reason = self._run(polished)
        self.assertFalse(ok)
        self.assertIn("длина", reason.lower())

    def test_rejects_forbidden_tag_p(self):
        polished = (
            f'<p>что-то</p><a href="u"><b>33-100/2026</b></a>\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 100
        )
        ok, reason = self._run(polished)
        self.assertFalse(ok)
        self.assertIn("запрещённый", reason.lower())

    def test_rejects_missing_dashboard_link(self):
        polished = f'<a href="u"><b>33-100/2026</b></a> текст' + "x" * 200
        ok, reason = self._run(polished)
        self.assertFalse(ok)
        self.assertIn("дашборд", reason.lower())

    def test_rejects_missing_case_number(self):
        polished = (
            f'<a href="u"><b>другой-номер</b></a> текст\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 100
        )
        ok, reason = self._run(polished)
        self.assertFalse(ok)
        self.assertIn("33-100/2026", reason)

    def test_rejects_case_number_without_anchor_wrap(self):
        # Номер есть как plain-text, но не обёрнут в <a><b>...</b></a>.
        polished = (
            f'33-100/2026 без обёртки\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 200
        )
        ok, reason = self._run(polished)
        self.assertFalse(ok)
        self.assertIn("обёртку", reason.lower())


class PolishDigestHtmlTest(unittest.TestCase):
    def setUp(self):
        self.draft = (
            f'  <a href="u1"><b>33-100/2026</b></a> Иванов vs Петров\n'
            f'     🔁 заседание отложено на 09.06.2026 15:00\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊 Дашборд</a>' + "x" * 200
        )
        self.expected = {"33-100/2026"}

    def test_returns_draft_on_empty_llm(self):
        with patch.object(cm_llm, "_call_claude_polish", lambda s, u: None), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.polish_digest_html(
                self.draft, expected_case_numbers=self.expected
            )
            self.assertEqual(result, self.draft)

    def test_uses_polished_html_when_valid(self):
        # Mock возвращает draft с микро-правкой (капитализация заседания).
        polished_input = self.draft.replace(
            "🔁 заседание отложено", "🔁 Заседание отложено"
        )

        def fake(system, user):
            return polished_input

        with patch.object(cm_llm, "_call_claude_polish", fake), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.polish_digest_html(
                self.draft, expected_case_numbers=self.expected
            )
            # polish_digest_html стрипует whitespace по краям, поэтому
            # сравниваем по сути, а не байт-в-байт.
            self.assertIn("Заседание отложено", result)
            self.assertNotIn("заседание отложено", result)
            self.assertIn("33-100/2026", result)

    def test_falls_back_when_validator_rejects(self):
        # Mock возвращает HTML с потерей номера дела.
        broken = (
            "Просто текст без номеров и без всего.\n\n"
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 100
        )

        def fake(system, user):
            return broken

        with patch.object(cm_llm, "_call_claude_polish", fake), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.polish_digest_html(
                self.draft, expected_case_numbers=self.expected
            )
            self.assertEqual(result, self.draft)

    def test_falls_back_when_polished_has_p_tag(self):
        # Mock добавил запрещённый <p>.
        broken = (
            f'<p><a href="u1"><b>33-100/2026</b></a> Иванов</p>\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 200
        )

        def fake(system, user):
            return broken

        with patch.object(cm_llm, "_call_claude_polish", fake), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.polish_digest_html(
                self.draft, expected_case_numbers=self.expected
            )
            self.assertEqual(result, self.draft)

    def test_strips_code_fence_from_llm_response(self):
        # LLM иногда оборачивает в ```html ... ``` несмотря на запрет.
        polished_with_fence = (
            "```html\n"
            f'<a href="u1"><b>33-100/2026</b></a> Иванов vs Петров\n'
            f'     🔁 Заседание отложено на <b>09.06.2026 15:00</b>\n\n'
            f'<a href="{uc.DASHBOARD_URL}">📊</a>' + "x" * 200
            + "\n```"
        )

        def fake(system, user):
            return polished_with_fence

        with patch.object(cm_llm, "_call_claude_polish", fake), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"):
            result = uc.polish_digest_html(
                self.draft, expected_case_numbers=self.expected
            )
            self.assertNotIn("```", result)
            self.assertIn("Заседание отложено", result)

    def test_empty_draft_returns_empty(self):
        result = uc.polish_digest_html("", expected_case_numbers=set())
        self.assertEqual(result, "")


class GenerateDigestPolishIntegrationTest(unittest.TestCase):
    """generate_digest с DIGEST_POLISH=1 — вызывает polish_digest_html
    после черновика и валидирует контракт.
    """

    def test_polish_disabled_by_default(self):
        # Без флага DIGEST_POLISH — полировщик не зовётся.
        called: list = []

        def fake_polish(draft, *, expected_case_numbers):
            called.append(draft)
            return draft

        with patch.object(cm_config, "DIGEST_POLISH", False), \
             patch.object(cm_llm, "polish_digest_html", fake_polish):
            uc.generate_digest(
                new_cases=[], changes=[],
                fi_new_cases=[], fi_changes=[],
                cass_changes=[], cass_discovered=[],
                total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
            )
        self.assertEqual(called, [], "polish не должен вызываться без DIGEST_POLISH=1")

    def test_polish_enabled_calls_polisher(self):
        called: list = []

        def fake_polish(draft, *, expected_case_numbers):
            called.append((draft, expected_case_numbers))
            return draft + "\n<!-- polished -->"

        change = {
            "case": "33-100/2026",
            "type": ["new_event"],
            "details": {
                "event": "Заседание. 14:00. 20.05.2026",
                "event_date": "20.05.2026",
                "hearing_date": "20.05.2026",
                "hearing_time": "14:00",
                "plaintiff": "ПАО Сбербанк",
                "defendant": "Иванов И.",
                "role": "Истец",
                "case_url": "https://example.com",
            },
        }
        with patch.object(cm_config, "DIGEST_POLISH", True), \
             patch.object(cm_llm, "polish_digest_html", fake_polish):
            html = uc.generate_digest(
                new_cases=[], changes=[change],
                fi_new_cases=[], fi_changes=[],
                cass_changes=[], cass_discovered=[],
                total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
            )
        self.assertEqual(len(called), 1)
        _, expected_nums = called[0]
        self.assertIn("33-100/2026", expected_nums)
        self.assertIn("<!-- polished -->", html)


class RecessAndRestartGuardTest(unittest.TestCase):
    """Регресс инцидента 30.06.2026 (дело 2-857/2026): решённое дело не должно
    «воскресать» как «рассмотрение начато с начала» из-за ретроактивной правки
    судом старого события; перерыв (ст. 157 ГПК) показывается отдельной строкой,
    а не как отложение."""

    def _fi_change(self, types, details):
        return {
            "case": "2-857/2026",
            "court": "Нижневартовский городской суд",
            "plaintiff": "Володин Степан Александрович",
            "defendant": "ПАО Сбербанк",
            "bank_role": "Ответчик",
            "category": "услуг кредитных организаций",
            "type": types,
            "details": {
                "link": "247736247|uid",
                "court_domain": "vartovgor--hmao.sudrf.ru",
                **details,
            },
        }

    def test_is_latest_session_event_false_for_backedit(self):
        # Суд дописал «начато с начала» в старое событие 30.09.2025, тогда как
        # последнее заседание — 25.06.2026. Это не свежий перезапуск.
        events = [
            {"date": "30.09.2025",
             "text": "Предварительное судебное заседание. 10:00. 307. "
                     "Рассмотрение дела начато с начала. 24.08.2025"},
            {"date": "25.06.2026",
             "text": "Судебное заседание. 09:45. 307. Вынесено решение по делу. "
                     "ОТКАЗАНО в удовлетворении иска. 25.06.2026"},
        ]
        ev = uc._events_newly_match([], events, uc._RESTART_RE)
        self.assertIsNotNone(ev)
        self.assertFalse(uc._is_latest_session_event(ev, events))

    def test_is_latest_session_event_true_for_fresh_restart(self):
        events = [
            {"date": "01.02.2026",
             "text": "Судебное заседание. 10:00. 307. 01.01.2026"},
            {"date": "01.06.2026",
             "text": "Судебное заседание. 11:00. 307. "
                     "Рассмотрение дела начато с начала. 02.02.2026"},
        ]
        ev = uc._events_newly_match([], events, uc._RESTART_RE)
        self.assertIsNotNone(ev)
        self.assertTrue(uc._is_latest_session_event(ev, events))

    def test_recess_regex(self):
        self.assertTrue(uc._RECESS_RE.search("Судебное заседание. Объявлен перерыв."))
        self.assertFalse(uc._RECESS_RE.search("Судебное заседание. Отложено."))

    def test_recess_renders_as_recess_line(self):
        html = uc.generate_template_digest(
            new_cases=[], changes=[], fi_changes=[self._fi_change(
                ["fi_hearing_recess"],
                {"hearing_date": "25.06.2026", "hearing_time": "09:45",
                 "hearing_type": "заседание"},
            )],
            total_active_fi=1,
        )
        self.assertIn(
            "в заседании объявлен перерыв до <b>25.06.2026 в 09:45</b>", html
        )
        self.assertNotIn("отложено на <b>25.06.2026", html)
        self.assertNotIn("рассмотрение начато с начала", html)

    def test_restart_future_date_shown(self):
        html = uc.generate_template_digest(
            new_cases=[], changes=[], fi_changes=[self._fi_change(
                ["fi_hearing_restart"],
                {"restart_date": "10.07.2026",
                 "next_hearing_date": "20.08.2026", "next_hearing_time": "10:00"},
            )],
            total_active_fi=1,
        )
        self.assertIn("рассмотрение начато с начала", html)
        self.assertIn("20.08.2026", html)

    def test_restart_without_next_hearing_has_no_following_session(self):
        # Future-gate отсёк прошедшую дату → поля next_hearing_date нет.
        html = uc.generate_template_digest(
            new_cases=[], changes=[], fi_changes=[self._fi_change(
                ["fi_hearing_restart"], {"restart_date": "30.09.2025"},
            )],
            total_active_fi=1,
        )
        self.assertIn("рассмотрение начато с начала", html)
        self.assertNotIn("след. заседание", html)


if __name__ == "__main__":
    unittest.main()
