# -*- coding: utf-8 -*-
"""Тесты третьего LLM-провайдера (OpenRouter) и неймспейса кэша пересказов.

Покрывают: резолв «модели дня» (_resolve_openrouter_model) с мемоизацией и
fallback, низкоуровневый вызов _call_openrouter_chat (Bearer, без verify=False),
диспетчеризацию по config.LLM_PROVIDER в summarize_act_motivation /
polish_digest_html / generate_digest (полная LLM-ветка), неймспейс ключа
кэша .act_summaries.json (claude-ключ побайтово прежний) и
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


class _OpenRouterTestBase(unittest.TestCase):
    """Общий setUp: сброс мемо резолва модели, чтобы тесты не влияли
    друг на друга (мемо живёт на процесс)."""

    def setUp(self):
        cm_llm._openrouter_resolved_model = None

    def tearDown(self):
        cm_llm._openrouter_resolved_model = None


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


class SummarizeDispatchTest(_OpenRouterTestBase):
    def test_openrouter_branch_called(self):
        called = {"openrouter": 0, "claude": 0, "gigachat": 0}

        with patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_llm, "_call_openrouter_simple",
                          lambda p: called.__setitem__(
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

    def test_claude_key_is_byte_identical_to_legacy(self):
        legacy = hashlib.sha1(
            (self.ACT + "|v2-ratio").encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(self._key("claude"), legacy)

    def test_providers_and_models_do_not_collide(self):
        claude = self._key("claude")
        giga = self._key("gigachat", GIGACHAT_MODEL="GigaChat-2-Max")
        or1 = self._key("openrouter", OPENROUTER_MODEL="a/b:free")
        or2 = self._key("openrouter", OPENROUTER_MODEL="c/d:free")
        keys = {claude, giga, or1, or2}
        self.assertEqual(len(keys), 4, f"коллизия ключей: {keys}")


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


if __name__ == "__main__":
    unittest.main()
