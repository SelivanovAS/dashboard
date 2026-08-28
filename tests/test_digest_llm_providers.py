# -*- coding: utf-8 -*-
"""Тесты третьего LLM-провайдера (OpenRouter) и неймспейса кэша пересказов.

Покрывают: резолв «модели дня» (_resolve_openrouter_model) с мемоизацией и
fallback, низкоуровневый вызов _call_openrouter_chat (Bearer, без verify=False),
диспетчеризацию по config.LLM_PROVIDER в summarize_act_motivation /
polish_digest_html / generate_digest (полная LLM-ветка), неймспейс ключа
кэша .act_summaries.json (маркер стиля + провайдер:модель) и
validate_environment для openrouter.

Запуск: `python3 -m pytest tests/test_digest_llm_providers.py` из корня репо.
"""

import hashlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

# Конфиг-константы и LLM-функции патчатся на модуле-доме: код читает их
# как config.X / вызывает как llm.X(...) (см. docs/Распил_монолита_контекст.md).
from court_monitor import config as cm_config  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.digest import core as cm_core  # noqa: E402
from court_monitor.digest import llm as cm_llm  # noqa: E402
from court_monitor import delivery as cm_delivery  # noqa: E402


class _OpenRouterTestBase(unittest.TestCase):
    """Общий setUp: сброс мемо резолва модели, чтобы тесты не влияли
    друг на друга (мемо живёт на процесс), + ключи ВСЕХ провайдеров.

    Ключи обязательны: `summarize_act_motivation` с 21.08.2026 не ходит в
    сеть, когда у текущего провайдера ключа нет (Mac-резерв), и патч
    транспорта тесту уже не помогает — вызова просто не будет.
    """

    def setUp(self):
        cm_llm._openrouter_resolved_model = None
        cm_llm._llm_not_configured_reported = False
        for name in ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY",
                     "GIGACHAT_AUTH_KEY"):
            patcher = patch.object(cm_config, name, "test-key")
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        cm_llm._openrouter_resolved_model = None
        cm_llm._llm_not_configured_reported = False


def _fake_response(payload):
    r = MagicMock()
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


class ResolveOpenrouterModelTest(_OpenRouterTestBase):
    def test_env_model_wins_without_http(self):
        with patch.object(cm_config, "OPENROUTER_MODEL", "qwen/qwen3:free"), \
             patch.object(cm_llm.requests, "get") as mget:
            self.assertEqual(
                cm_llm._resolve_openrouter_model(), "qwen/qwen3:free"
            )
        mget.assert_not_called()

    def test_top_model_of_the_day(self):
        with patch.object(cm_config, "OPENROUTER_MODEL", ""), \
             patch.object(cm_llm.requests, "get") as mget:
            mget.return_value = _fake_response(
                {"models": [{"id": "top/model:free"}, {"id": "second"}]}
            )
            self.assertEqual(
                cm_llm._resolve_openrouter_model(), "top/model:free"
            )
            mget.assert_called_once_with(
                cm_config.OPENROUTER_TOP_MODELS_URL, timeout=15
            )
            # Мемоизация: второй вызов без HTTP.
            self.assertEqual(
                cm_llm._resolve_openrouter_model(), "top/model:free"
            )
            self.assertEqual(mget.call_count, 1)

    def test_fallback_on_network_error_and_memoized(self):
        with patch.object(cm_config, "OPENROUTER_MODEL", ""), \
             patch.object(cm_llm.requests, "get") as mget:
            mget.side_effect = cm_llm.requests.RequestException("boom")
            self.assertEqual(
                cm_llm._resolve_openrouter_model(),
                cm_config.OPENROUTER_FALLBACK_MODEL,
            )
            # Fallback тоже мемоизирован — прогон с N актами не должен
            # делать N неудачных запросов.
            self.assertEqual(
                cm_llm._resolve_openrouter_model(),
                cm_config.OPENROUTER_FALLBACK_MODEL,
            )
            self.assertEqual(mget.call_count, 1)

    def test_fallback_on_empty_models(self):
        for payload in ({"models": []}, {}, {"models": [{"id": ""}]}):
            cm_llm._openrouter_resolved_model = None
            with patch.object(cm_config, "OPENROUTER_MODEL", ""), \
                 patch.object(cm_llm.requests, "get") as mget:
                mget.return_value = _fake_response(payload)
                self.assertEqual(
                    cm_llm._resolve_openrouter_model(),
                    cm_config.OPENROUTER_FALLBACK_MODEL,
                    f"payload={payload}",
                )

    def test_rank_parsing(self):
        cases = {
            "": 1,
            "модель дня (топ-1)": 1,
            "Модель дня": 1,
            "авто": 1,
            "топ-2": 2,
            "топ-5": 5,
            "Топ 3": 3,
            "qwen/qwen3:free": None,     # буквальный id
            "GigaChat-2-Max": None,
        }
        for raw, expected in cases.items():
            with patch.object(cm_config, "OPENROUTER_MODEL", raw):
                self.assertEqual(
                    cm_llm._openrouter_requested_rank(), expected,
                    f"raw={raw!r}",
                )

    def test_rank_picks_nth_model(self):
        payload = {"models": [
            {"id": "first/model:free"},
            {"id": "second/model:free"},
            {"id": "third/model:free"},
        ]}
        with patch.object(cm_config, "OPENROUTER_MODEL", "топ-2"), \
             patch.object(cm_llm.requests, "get") as mget:
            mget.return_value = _fake_response(payload)
            self.assertEqual(
                cm_llm._resolve_openrouter_model(), "second/model:free"
            )

    def test_rank_beyond_list_takes_last(self):
        payload = {"models": [{"id": "only/model:free"}]}
        with patch.object(cm_config, "OPENROUTER_MODEL", "топ-5"), \
             patch.object(cm_llm.requests, "get") as mget:
            mget.return_value = _fake_response(payload)
            self.assertEqual(
                cm_llm._resolve_openrouter_model(), "only/model:free"
            )


class CallOpenrouterChatTest(_OpenRouterTestBase):
    def test_request_shape_and_parsing(self):
        with patch.object(cm_config, "OPENROUTER_API_KEY", "sk-or-test"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.return_value = _fake_response(
                {"choices": [{"message": {"content": "  привет  "}}]}
            )
            out = cm_llm._call_openrouter_chat(
                [{"role": "user", "content": "тест"}],
                max_tokens=400, temperature=0.2,
            )
            self.assertEqual(out, "привет")
            args, kwargs = mpost.call_args
            self.assertEqual(args[0], cm_config.OPENROUTER_API_URL)
            self.assertEqual(
                kwargs["headers"]["Authorization"], "Bearer sk-or-test"
            )
            self.assertEqual(kwargs["json"]["model"], "test/model")
            self.assertEqual(kwargs["json"]["max_tokens"], 400)
            # В отличие от GigaChat, TLS проверяется штатно.
            self.assertNotIn("verify", kwargs)

    def test_no_key_returns_none_without_http(self):
        with patch.object(cm_config, "OPENROUTER_API_KEY", ""), \
             patch.object(cm_llm.requests, "post") as mpost:
            self.assertIsNone(
                cm_llm._call_openrouter_chat(
                    [{"role": "user", "content": "x"}],
                    max_tokens=10, temperature=0.0,
                )
            )
        mpost.assert_not_called()

    def test_none_on_network_error_and_empty_choices(self):
        with patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.side_effect = cm_llm.requests.RequestException("boom")
            self.assertIsNone(
                cm_llm._call_openrouter_chat(
                    [{"role": "user", "content": "x"}],
                    max_tokens=10, temperature=0.0,
                )
            )
            mpost.side_effect = None
            mpost.return_value = _fake_response({"choices": []})
            self.assertIsNone(
                cm_llm._call_openrouter_chat(
                    [{"role": "user", "content": "x"}],
                    max_tokens=10, temperature=0.0,
                )
            )

    def test_empty_choices_and_content_log_warning(self):
        # Раньше пустой ответ возвращался молча — в логе прогона сбой был
        # неотличим от «модель ответила пусто» уровнем выше.
        with patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.return_value = _fake_response({"choices": []})
            with self.assertLogs("court-monitor", level="WARNING") as logs:
                self.assertIsNone(
                    cm_llm._call_openrouter_chat(
                        [{"role": "user", "content": "x"}],
                        max_tokens=10, temperature=0.0,
                    )
                )
            self.assertTrue(
                any("пустой список choices" in m for m in logs.output),
                logs.output,
            )
            mpost.return_value = _fake_response(
                {"choices": [{"message": {"content": "   "}}]}
            )
            with self.assertLogs("court-monitor", level="WARNING") as logs:
                self.assertIsNone(
                    cm_llm._call_openrouter_chat(
                        [{"role": "user", "content": "x"}],
                        max_tokens=10, temperature=0.0,
                    )
                )
            self.assertTrue(
                any("пустой content" in m for m in logs.output), logs.output
            )

    def test_model_override_goes_to_payload_without_resolve(self):
        # Явная модель (фолбэк-контур пересказов) кладётся в payload и не
        # трогает резолв «модели дня» — даже когда тот потребовал бы HTTP.
        with patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "OPENROUTER_MODEL", ""), \
             patch.object(cm_llm.requests, "get") as mget, \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.return_value = _fake_response(
                {"choices": [{"message": {"content": "ок"}}]}
            )
            out = cm_llm._call_openrouter_chat(
                [{"role": "user", "content": "x"}],
                max_tokens=10, temperature=0.0,
                model="openrouter/free",
            )
            self.assertEqual(out, "ок")
            _, kwargs = mpost.call_args
            self.assertEqual(kwargs["json"]["model"], "openrouter/free")
        mget.assert_not_called()


class SummaryTokenBudgetTest(_OpenRouterTestBase):
    """Лимиты max_tokens микро-вызовов пересказа: 700 у Claude/GigaChat
    (2-3 предложения ≈ 250-350 токенов кириллицы + запас), 4096 у
    OpenRouter (reasoning-модели тратят бюджет на размышления в content
    и с маленьким лимитом обрезаются посреди <think>)."""

    def test_openrouter_simple_budget(self):
        captured = {}

        def fake_chat(messages, *, max_tokens, temperature, model=None):
            captured.update(max_tokens=max_tokens, temperature=temperature)
            return "ок"

        with patch.object(cm_llm, "_call_openrouter_chat", fake_chat):
            self.assertEqual(cm_llm._call_openrouter_simple("тест"), "ок")
        self.assertEqual(captured["max_tokens"], 4096)

    def test_claude_simple_budget(self):
        with patch.object(cm_config, "ANTHROPIC_API_KEY", "k"), \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.return_value = _fake_response(
                {"content": [{"type": "text", "text": "ок"}]}
            )
            self.assertEqual(cm_llm._call_claude_simple("тест"), "ок")
            _, kwargs = mpost.call_args
            self.assertEqual(kwargs["json"]["max_tokens"], 700)

    def test_gigachat_simple_budget(self):
        with patch.object(cm_llm, "_gigachat_access_token", lambda: "tok"), \
             patch.object(cm_llm.requests, "post") as mpost:
            mpost.return_value = _fake_response(
                {"choices": [{"message": {"content": "ок"}}]}
            )
            self.assertEqual(cm_llm._call_gigachat_simple("тест"), "ок")
            _, kwargs = mpost.call_args
            self.assertEqual(kwargs["json"]["max_tokens"], 700)


class SummarizeDispatchTest(_OpenRouterTestBase):
    def test_openrouter_branch_called(self):
        called = {"openrouter": 0, "claude": 0, "gigachat": 0}

        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm, "_call_openrouter_simple",
                          lambda p, **kw: called.__setitem__(
                              "openrouter", called["openrouter"] + 1
                          ) or "Пересказ."), \
             patch.object(cm_llm, "_call_claude_simple",
                          lambda p, **kw: called.__setitem__(
                              "claude", called["claude"] + 1
                          ) or "нет"), \
             patch.object(cm_llm, "_call_gigachat_simple",
                          lambda p: called.__setitem__(
                              "gigachat", called["gigachat"] + 1
                          ) or "нет"):
            out = cm_llm.summarize_act_motivation(
                "Мотивировочная часть акта. " * 10,
                case_meta={"stage": "appeal"},
                use_cache=False,
            )
        self.assertEqual(out, "Пересказ.")
        self.assertEqual(called, {"openrouter": 1, "claude": 0, "gigachat": 0})


class SummarizeOpenrouterRetryTest(_OpenRouterTestBase):
    """Ретраи пересказа для openrouter: перегруженный free-пул отдаёт 429
    мгновенно, поэтому попытки на основной модели идут с нарастающей
    паузой (attempt * OPENROUTER_SUMMARY_RETRY_DELAY), а после них
    подключается фолбэк-модель OPENROUTER_FALLBACK_MODEL. У Claude/
    GigaChat ретрая нет."""

    ACT = "Мотивировочная часть акта. " * 10
    PRIMARY = "primary/model:free"

    def setUp(self):
        super().setUp()
        for k in ("llm_summary_calls", "llm_summary_cache_hits",
                  "llm_summary_failed", "llm_summary_fallback_saved",
                  "llm_summary_provider_fallback_saved"):
            cm_config.METRICS[k] = 0
        self.sleeps = []
        for p in (
            patch.object(cm_config, "LLM_PROVIDER", "openrouter"),
            patch.object(cm_config, "OPENROUTER_MODEL", self.PRIMARY),
            patch.object(cm_config, "OPENROUTER_SUMMARY_RETRIES", 3),
            patch.object(cm_config, "OPENROUTER_SUMMARY_FALLBACK_RETRIES", 2),
            patch.object(cm_config, "OPENROUTER_SUMMARY_RETRY_DELAY", 5),
            patch.object(cm_config, "LLM_SUMMARY_PROVIDER_FALLBACK", True),
            patch.object(cm_llm.time, "sleep", self.sleeps.append),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _summarize(self, fake, use_cache=False, claude=None):
        # Фолбэк-провайдер Claude патчится ВСЕГДА (база setUp даёт всем
        # провайдерам test-key, и без патча исчерпание openrouter-попыток
        # ушло бы настоящим HTTP в Anthropic); дефолт — «Claude тоже лёг».
        self.claude_calls = []

        def _claude_dead(prompt, **kw):
            self.claude_calls.append(prompt)
            return None

        with patch.object(cm_llm, "_call_openrouter_simple", fake), \
             patch.object(cm_llm, "_call_claude_simple",
                          claude or _claude_dead):
            return cm_llm.summarize_act_motivation(
                self.ACT, case_meta={"stage": "appeal"}, use_cache=use_cache,
            )

    def test_retry_with_pause_saves_summary(self):
        answers = ["<think>обрыв размышлений посреди", None,
                   "Иск удовлетворён."]
        calls = []

        def fake(prompt, *, model=None):
            calls.append(model)
            return answers[len(calls) - 1]

        self.assertEqual(self._summarize(fake), "Иск удовлетворён.")
        # Все три попытки — на основной модели, паузы нарастают: 5с, 10с.
        self.assertEqual(calls, [self.PRIMARY] * 3)
        self.assertEqual(self.sleeps, [5, 10])
        self.assertEqual(cm_config.METRICS["llm_summary_calls"], 3)
        self.assertEqual(cm_config.METRICS["llm_summary_fallback_saved"], 0)
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 0)

    def test_fallback_model_rescues(self):
        calls = []

        def fake(prompt, *, model=None):
            calls.append(model)
            if model == cm_config.OPENROUTER_FALLBACK_MODEL:
                return "Иск удовлетворён."
            return "<think>обрыв"

        with self.assertLogs("court-monitor", level="INFO") as logs:
            self.assertEqual(self._summarize(fake), "Иск удовлетворён.")
        self.assertEqual(
            calls,
            [self.PRIMARY] * 3 + [cm_config.OPENROUTER_FALLBACK_MODEL],
        )
        self.assertEqual(cm_config.METRICS["llm_summary_fallback_saved"], 1)
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 0)
        self.assertTrue(
            any("выручила фолбэк-модель" in m for m in logs.output),
            logs.output,
        )

    def test_fallback_success_cached_under_primary_key(self):
        saved = {}

        def fake(prompt, *, model=None):
            if model == cm_config.OPENROUTER_FALLBACK_MODEL:
                return "Иск удовлетворён."
            return None

        with patch.object(cm_llm, "_load_act_summaries", lambda: {}), \
             patch.object(cm_llm, "_save_act_summaries", saved.update):
            self.assertEqual(
                self._summarize(fake, use_cache=True), "Иск удовлетворён."
            )
        # Ключ — в неймспейсе ОСНОВНОЙ модели прогона (следующий прогон
        # его найдёт), а поле model честно называет фактического автора.
        key = cm_llm._act_cache_key(self.ACT.strip())
        self.assertIn(key, saved)
        self.assertEqual(saved[key]["model"], "openrouter:openrouter/free")

    def test_no_duplicate_attempts_when_primary_is_fallback(self):
        calls = []

        def fake(prompt, *, model=None):
            calls.append(model)
            return None

        with patch.object(cm_config, "OPENROUTER_MODEL",
                          cm_config.OPENROUTER_FALLBACK_MODEL):
            cm_llm._openrouter_resolved_model = None  # перечитать модель
            self.assertIsNone(self._summarize(fake))
        # Фолбэк-этап пропущен: основная модель и так openrouter/free.
        self.assertEqual(calls, [cm_config.OPENROUTER_FALLBACK_MODEL] * 3)
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 1)

    def test_all_attempts_dead_return_none_and_count_failure(self):
        calls = []

        def fake(prompt, *, model=None):
            calls.append(model)
            return "<think>обрыв"

        with self.assertLogs("court-monitor", level="WARNING") as logs:
            self.assertIsNone(self._summarize(fake))
        self.assertEqual(
            calls,
            [self.PRIMARY] * 3 + [cm_config.OPENROUTER_FALLBACK_MODEL] * 2,
        )
        # Паузы: 5с, 10с на основной + 5с внутри фолбэк-этапа.
        self.assertEqual(self.sleeps, [5, 10, 5])
        # После обеих openrouter-моделей пробовался фолбэк-провайдер Claude
        # (он в этом тесте тоже мёртв) — итого 5 + 1 вызовов.
        self.assertEqual(len(self.claude_calls), 1)
        self.assertEqual(cm_config.METRICS["llm_summary_calls"], 6)
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 1)
        self.assertEqual(cm_config.METRICS["llm_summary_fallback_saved"], 0)
        self.assertEqual(
            cm_config.METRICS["llm_summary_provider_fallback_saved"], 0
        )
        self.assertTrue(
            any("отбракован чисткой" in m for m in logs.output), logs.output
        )

    def test_provider_fallback_rescues(self):
        """Бесплатный пул лёг целиком → одна попытка Claude спасает пересказ
        (инцидент 28.08.2026: оба акта Урала ушли сырым отрывком при живом
        ANTHROPIC_API_KEY в env replay)."""
        or_calls = []

        def fake(prompt, *, model=None):
            or_calls.append(model)
            return None

        claude_calls = []

        def claude(prompt, **kw):
            claude_calls.append(prompt)
            return "Иск удовлетворён: наследники приняли наследство."

        saved = {}
        with patch.object(cm_config, "CLAUDE_MODEL", "claude-haiku-test"), \
             patch.object(cm_llm, "_load_act_summaries", lambda: {}), \
             patch.object(cm_llm, "_save_act_summaries", saved.update), \
             self.assertLogs("court-monitor", level="INFO") as logs:
            self.assertEqual(
                self._summarize(fake, use_cache=True, claude=claude),
                "Иск удовлетворён: наследники приняли наследство.",
            )
        self.assertEqual(
            or_calls,
            [self.PRIMARY] * 3 + [cm_config.OPENROUTER_FALLBACK_MODEL] * 2,
        )
        self.assertEqual(len(claude_calls), 1)
        self.assertEqual(
            cm_config.METRICS["llm_summary_provider_fallback_saved"], 1
        )
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 0)
        self.assertTrue(
            any("выручил фолбэк-провайдер claude" in m for m in logs.output),
            logs.output,
        )
        # Кэш-ключ — в openrouter-неймспейсе (следующий прогон его найдёт),
        # поле model честно называет фактического автора.
        key = cm_llm._act_cache_key(self.ACT.strip())
        self.assertIn(key, saved)
        self.assertEqual(saved[key]["model"], "claude:claude-haiku-test")

    def test_provider_fallback_needs_claude_key(self):
        """Без ANTHROPIC_API_KEY фолбэк-провайдер не зовётся — прежний отказ
        (Mac-резерв сюда не доходит вовсе: missing_llm_key_name отсекает
        раньше, но и с одним лишь openrouter-ключом Claude звать нечем)."""
        def fake(prompt, *, model=None):
            return None

        with patch.object(cm_config, "ANTHROPIC_API_KEY", ""):
            self.assertIsNone(self._summarize(fake))
        self.assertEqual(self.claude_calls, [])
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 1)
        self.assertEqual(
            cm_config.METRICS["llm_summary_provider_fallback_saved"], 0
        )

    def test_provider_fallback_switch_off(self):
        """LLM_SUMMARY_PROVIDER_FALLBACK=0 — чисто бесплатный пул, как до
        28.08.2026."""
        def fake(prompt, *, model=None):
            return None

        with patch.object(cm_config, "LLM_SUMMARY_PROVIDER_FALLBACK", False):
            self.assertIsNone(self._summarize(fake))
        self.assertEqual(self.claude_calls, [])
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 1)

    def test_provider_fallback_not_called_on_openrouter_success(self):
        def fake(prompt, *, model=None):
            return "Иск удовлетворён."

        self.assertEqual(self._summarize(fake), "Иск удовлетворён.")
        self.assertEqual(self.claude_calls, [])
        self.assertEqual(
            cm_config.METRICS["llm_summary_provider_fallback_saved"], 0
        )

    def test_claude_has_no_retry(self):
        calls = []

        def fake(prompt, **kw):
            calls.append(prompt)
            return None

        with patch.object(cm_config, "LLM_PROVIDER", "claude"), \
             patch.object(cm_llm, "_call_claude_simple", fake):
            self.assertIsNone(cm_llm.summarize_act_motivation(
                self.ACT, case_meta={"stage": "appeal"}, use_cache=False,
            ))
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sleeps, [])


class ActCacheKeyNamespaceTest(_OpenRouterTestBase):
    ACT = "Мотивировочная часть акта. " * 10

    def _key(self, provider, **cfg):
        patches = [patch.object(cm_config, "LLM_PROVIDER", provider)]
        for name, val in cfg.items():
            patches.append(patch.object(cm_config, name, val))
        for p in patches:
            p.start()
        try:
            return cm_llm._act_cache_key(self.ACT)
        finally:
            for p in patches:
                p.stop()

    def test_claude_key_matches_current_style_marker(self):
        # Маркер "v3-detailed" (июль 2026, пересказ в 2-3 предложения) —
        # смена маркера инвалидирует кэш НАМЕРЕННО и только при смене
        # стиля результата; правки надёжности промпта ключ не трогают.
        expected = hashlib.sha1(
            (self.ACT + "|v3-detailed").encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(self._key("claude"), expected)

    def test_providers_and_models_do_not_collide(self):
        claude = self._key("claude")
        giga = self._key("gigachat", GIGACHAT_MODEL="GigaChat-2-Max")
        or1 = self._key("openrouter", OPENROUTER_MODEL="a/b:free")
        or2 = self._key("openrouter", OPENROUTER_MODEL="c/d:free")
        keys = {claude, giga, or1, or2}
        self.assertEqual(len(keys), 4, f"коллизия ключей: {keys}")


class GigachatApiUrlTest(_OpenRouterTestBase):
    """Выбор базового адреса GigaChat по модели: 3-е поколение
    (GigaChat-3-Ultra) живёт на api.giga.chat, остальные — на
    стандартном gigachat.devices.sberbank.ru."""

    def test_ultra_uses_v3_url(self):
        with patch.object(cm_config, "GIGACHAT_MODEL", "GigaChat-3-Ultra"):
            self.assertEqual(
                cm_llm._gigachat_api_url(), cm_config.GIGACHAT_V3_API_URL
            )

    def test_gen2_and_default_use_standard_url(self):
        for model in ("GigaChat", "GigaChat-2", "GigaChat-2-Pro",
                      "GigaChat-2-Max"):
            with patch.object(cm_config, "GIGACHAT_MODEL", model):
                self.assertEqual(
                    cm_llm._gigachat_api_url(), cm_config.GIGACHAT_API_URL
                )


class CurrentModelNameTest(_OpenRouterTestBase):
    def test_openrouter_label(self):
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "qwen/qwen3:free"):
            self.assertEqual(
                cm_llm._current_digest_model_name(),
                "openrouter:qwen/qwen3:free",
            )


class PolishDispatchTest(_OpenRouterTestBase):
    def test_openrouter_branch_called(self):
        calls = []

        def fake_polish(system_prompt, user_prompt):
            calls.append(user_prompt)
            return None  # пустой ответ → polish вернёт черновик

        draft = '<a href="https://e.ru/1"><b>33-1/2026</b></a> — тест'
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_llm, "_call_openrouter_polish", fake_polish):
            out = cm_llm.polish_digest_html(
                draft, expected_case_numbers={"33-1/2026"}
            )
        self.assertEqual(out, draft)
        self.assertEqual(len(calls), 1)


class FullLlmDigestOpenrouterTest(_OpenRouterTestBase):
    """Полная LLM-ветка (DIGEST_FULL_LLM=1) с provider=openrouter."""

    MARKER = "УНИКАЛЬНЫЙ-МАРКЕР-OPENROUTER-ДАЙДЖЕСТА"

    @staticmethod
    def _minimal_changes():
        return [{
            "case": "33-1/2026",
            "type": ["status_change"],
            "details": {
                "case_url": "https://example.ru/case",
                "plaintiff": "ПАО Сбербанк",
                "defendant": "Иванов И.И.",
                "role": "Истец",
                "old_status": "В производстве",
                "new_status": "Решено",
            },
        }]

    def _generate(self):
        return cm_core.generate_digest(
            [], self._minimal_changes(),
            cases=[], total_active_appeal=1,
        )

    def test_success_goes_through_postprocessing(self):
        calls = []

        def fake_digest(prompt):
            calls.append(prompt)
            return f"<b>Дайджест</b>\n{self.MARKER}"

        with patch.object(cm_config, "DIGEST_FULL_LLM", True), \
             patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm, "_call_openrouter_digest", fake_digest):
            out = self._generate()
        self.assertEqual(len(calls), 1)
        self.assertIn(self.MARKER, out)
        # Постобработка дописала футер со ссылкой на дашборд.
        self.assertIn("Дашборд", out)

    def test_empty_llm_answer_falls_back_to_template(self):
        with patch.object(cm_config, "DIGEST_FULL_LLM", True), \
             patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "test/model"), \
             patch.object(cm_llm, "_call_openrouter_digest",
                          lambda prompt: None):
            out = self._generate()
        self.assertTrue(out)
        self.assertNotIn(self.MARKER, out)

    def test_missing_key_falls_back_to_template_without_llm(self):
        calls = []
        with patch.object(cm_config, "DIGEST_FULL_LLM", True), \
             patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", ""), \
             patch.object(cm_llm, "_call_openrouter_digest",
                          lambda prompt: calls.append(prompt) or "x"):
            out = self._generate()
        self.assertTrue(out)
        self.assertEqual(calls, [])


class ValidateEnvironmentOpenrouterTest(_OpenRouterTestBase):
    def test_missing_key_exits(self):
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", ""), \
             patch.object(cm_config, "TELEGRAM_BOT_TOKEN", "t"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID", "c"):
            with self.assertRaises(SystemExit):
                cm_runs.validate_environment()

    def test_with_key_passes(self):
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", "k"), \
             patch.object(cm_config, "TELEGRAM_BOT_TOKEN", "t"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID", "c"):
            cm_runs.validate_environment()  # не должно упасть


class SummarizeNoKeyTest(_OpenRouterTestBase):
    """Ключа провайдера нет вовсе (Mac-резерв, боевой путь с 19.08.2026).

    Это НЕ отказ провайдера: вызова не было. Прежний код считал такой прогон
    сбоем («сбоев 6 из 6») и поднимал 🩺-алерт о несуществующем 429, а
    дайджест-черновик уходил с сырым текстом акта.
    """

    ACT = "Мотивировочная часть акта. " * 10

    def setUp(self):
        super().setUp()
        for k in ("llm_summary_calls", "llm_summary_failed",
                  "llm_summary_cache_hits", "llm_summary_skipped_no_key"):
            cm_config.METRICS[k] = 0

    def test_missing_key_skips_call_and_is_not_a_failure(self):
        called = {"n": 0}
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", ""), \
             patch.object(cm_llm, "_call_openrouter_simple",
                          lambda p, **kw: called.__setitem__(
                              "n", called["n"] + 1) or "Пересказ."):
            out = cm_llm.summarize_act_motivation(
                self.ACT, case_meta={"stage": "appeal"}, use_cache=False)
        self.assertIsNone(out)
        self.assertEqual(called["n"], 0, "без ключа вызова быть не должно")
        self.assertEqual(cm_config.METRICS["llm_summary_failed"], 0)
        self.assertEqual(cm_config.METRICS["llm_summary_calls"], 0)
        self.assertEqual(cm_config.METRICS["llm_summary_skipped_no_key"], 1)

    def test_cache_still_answers_without_key(self):
        """Гард стоит ПОСЛЕ кэша: пересказ, оплаченный replay'ем и
        закоммиченный в .act_summaries.json, обязан отдаваться и на машине
        без ключей."""
        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "primary/model:free"), \
             patch.object(cm_config, "OPENROUTER_API_KEY", ""):
            # Ключ кэша неймспейсится «провайдер:модель» — считать его надо
            # в том же окружении, иначе тест мимо кэша.
            key = cm_llm._act_cache_key(self.ACT.strip())
            with patch.object(cm_llm, "_load_act_summaries",
                              lambda: {key: {"summary": "Готовый пересказ."}}):
                out = cm_llm.summarize_act_motivation(
                    self.ACT, case_meta={"stage": "appeal"}, use_cache=True)
        self.assertEqual(out, "Готовый пересказ.")
        self.assertEqual(cm_config.METRICS["llm_summary_skipped_no_key"], 0)
        self.assertEqual(cm_config.METRICS["llm_summary_cache_hits"], 1)

    def test_missing_key_name_matches_provider(self):
        pairs = (
            ("openrouter", "OPENROUTER_API_KEY"),
            ("gigachat", "GIGACHAT_AUTH_KEY"),
            ("claude", "ANTHROPIC_API_KEY"),
        )
        for provider, var in pairs:
            with patch.object(cm_config, "LLM_PROVIDER", provider), \
                 patch.object(cm_config, var, ""):
                self.assertEqual(cm_llm.missing_llm_key_name(), var)
                self.assertFalse(cm_llm.llm_is_configured())
            with patch.object(cm_config, "LLM_PROVIDER", provider):
                self.assertIsNone(cm_llm.missing_llm_key_name())
                self.assertTrue(cm_llm.llm_is_configured())

    def test_run_summary_names_the_skip(self):
        """Сводка обязана сказать, что пересказов не делали: при пропуске
        calls/failed нулевые, и штатная LLM-строка не печатается вовсе."""
        cm_config.METRICS["llm_summary_skipped_no_key"] = 4
        with patch.object(cm_delivery.log, "info") as minfo:
            cm_delivery.log_run_summary("main-json", {"total": 1.0})
        printed = "\n".join(str(c.args[0]) for c in minfo.call_args_list)
        self.assertIn("LLM не настроен", printed)
        self.assertIn("4", printed)


if __name__ == "__main__":
    unittest.main()
