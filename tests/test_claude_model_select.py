# -*- coding: utf-8 -*-
"""Тесты выбора модели Claude (haiku/sonnet/opus) и уровня усилий.

Покрывают:
  - резолвер коротких имён в полный id API (config.resolve_claude_model);
  - резолвер уровня усилий (config.resolve_claude_effort);
  - модельно-зависимый пейлоад Anthropic API (llm._claude_payload): у моделей
    нового поколения temperature удалён из API (запрос с ним → 400
    «`temperature` is deprecated for this model»), вместо него adaptive-
    мышление + output_config.effort; боевой haiku-путь байт-в-байт прежний;
  - метку модели `_current_digest_model_name()` для ветки claude;
  - неймспейс кэша пересказов `_act_cache_key`: эталонная haiku-модель даёт
    те же ключи, что и раньше (боевой .act_summaries.json не переиндексируется),
    а sonnet/opus получают свой ключ; эффорт добавляет свой суффикс.

Запуск: `python3 -m pytest tests/test_claude_model_select.py` из корня репо.
"""

import os
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

from court_monitor import config as cm_config  # noqa: E402
from court_monitor.digest import llm as cm_llm  # noqa: E402


class ResolveClaudeModelTest(unittest.TestCase):
    def test_aliases_map_to_full_ids(self):
        self.assertEqual(
            cm_config.resolve_claude_model("haiku"), "claude-haiku-4-5-20251001"
        )
        self.assertEqual(cm_config.resolve_claude_model("sonnet"), "claude-sonnet-5")
        self.assertEqual(cm_config.resolve_claude_model("opus"), "claude-opus-4-8")

    def test_alias_case_insensitive(self):
        self.assertEqual(cm_config.resolve_claude_model("  Opus "), "claude-opus-4-8")

    def test_empty_falls_back_to_default(self):
        self.assertEqual(
            cm_config.resolve_claude_model(""), cm_config.DEFAULT_CLAUDE_MODEL
        )
        self.assertEqual(cm_config.DEFAULT_CLAUDE_MODEL, "claude-haiku-4-5-20251001")

    def test_exact_id_passes_through(self):
        # Точный id из поля «Точная модель» проходит без изменений.
        self.assertEqual(
            cm_config.resolve_claude_model("claude-sonnet-4-5-20250101"),
            "claude-sonnet-4-5-20250101",
        )


class ResolveClaudeEffortTest(unittest.TestCase):
    def test_valid_levels_pass(self):
        for level in ("low", "medium", "high", "xhigh", "max"):
            self.assertEqual(cm_config.resolve_claude_effort(level), level)
        self.assertEqual(cm_config.resolve_claude_effort("  MAX "), "max")

    def test_default_and_junk_resolve_to_empty(self):
        # «default» из селектора админки/workflow = не отправлять параметр.
        self.assertEqual(cm_config.resolve_claude_effort("default"), "")
        self.assertEqual(cm_config.resolve_claude_effort(""), "")
        self.assertEqual(cm_config.resolve_claude_effort("turbo"), "")


class ClaudePayloadTest(unittest.TestCase):
    MSGS = [{"role": "user", "content": "тест"}]

    def _payload(self, model, effort="", **kw):
        with patch.object(cm_config, "CLAUDE_MODEL", model), patch.object(
            cm_config, "CLAUDE_EFFORT", effort
        ):
            return cm_llm._claude_payload(
                max_tokens=kw.get("max_tokens", 700),
                temperature=kw.get("temperature", 0.2),
                messages=self.MSGS,
                system=kw.get("system"),
            )

    def test_haiku_payload_is_legacy(self):
        # Боевой путь: temperature на месте, без thinking/effort, лимит как был.
        p = self._payload(cm_config.DEFAULT_CLAUDE_MODEL)
        self.assertEqual(p["temperature"], 0.2)
        self.assertEqual(p["max_tokens"], 700)
        self.assertNotIn("thinking", p)
        self.assertNotIn("output_config", p)

    def test_haiku_ignores_effort(self):
        # haiku не поддерживает effort — даже заданный он не отправляется.
        p = self._payload(cm_config.DEFAULT_CLAUDE_MODEL, effort="high")
        self.assertNotIn("output_config", p)
        self.assertIn("temperature", p)

    def test_modern_drops_temperature_enables_thinking(self):
        # opus 4.8: temperature → 400, поэтому его нет; adaptive-мышление
        # включено; лимит расширен (размышления считаются в вывод).
        p = self._payload("claude-opus-4-8")
        self.assertNotIn("temperature", p)
        self.assertEqual(p["thinking"], {"type": "adaptive"})
        self.assertEqual(p["max_tokens"], 8000)
        self.assertNotIn("output_config", p)  # effort пуст = дефолт API

    def test_modern_includes_effort_when_set(self):
        p = self._payload("claude-sonnet-5", effort="xhigh")
        self.assertEqual(p["output_config"], {"effort": "xhigh"})
        self.assertNotIn("temperature", p)

    def test_modern_expands_large_budgets_proportionally(self):
        p = self._payload("claude-opus-4-8", max_tokens=4096)
        self.assertEqual(p["max_tokens"], 4096 * 4)

    def test_system_is_passed_through(self):
        p = self._payload("claude-opus-4-8", system="систем-промпт")
        self.assertEqual(p["system"], "систем-промпт")

    def test_timeout_grows_for_modern(self):
        with patch.object(cm_config, "CLAUDE_MODEL", "claude-opus-4-8"):
            self.assertEqual(cm_llm._claude_timeout(30), 180)
        with patch.object(
            cm_config, "CLAUDE_MODEL", cm_config.DEFAULT_CLAUDE_MODEL
        ):
            self.assertEqual(cm_llm._claude_timeout(30), 30)


class ClaudeSimpleRequestShapeTest(unittest.TestCase):
    def test_opus_request_has_no_temperature(self):
        from unittest.mock import MagicMock

        captured = {}

        def fake_post(url, **kwargs):
            captured.update(kwargs)
            r = MagicMock()
            r.json.return_value = {"content": [{"type": "text", "text": "ок"}]}
            r.raise_for_status.return_value = None
            return r

        with patch.object(cm_config, "ANTHROPIC_API_KEY", "k"), patch.object(
            cm_config, "CLAUDE_MODEL", "claude-opus-4-8"
        ), patch.object(cm_config, "CLAUDE_EFFORT", "high"), patch.object(
            cm_llm.requests, "post", fake_post
        ):
            self.assertEqual(cm_llm._call_claude_simple("тест"), "ок")
        body = captured["json"]
        self.assertNotIn("temperature", body)
        self.assertEqual(body["thinking"], {"type": "adaptive"})
        self.assertEqual(body["output_config"], {"effort": "high"})
        self.assertEqual(captured["timeout"], 180)


class CurrentModelLabelTest(unittest.TestCase):
    def test_claude_label_reflects_config(self):
        with patch.object(cm_config, "LLM_PROVIDER", "claude"), patch.object(
            cm_config, "CLAUDE_MODEL", "claude-opus-4-8"
        ):
            self.assertEqual(cm_llm._current_digest_model_name(), "claude-opus-4-8")


class ActCacheKeyNamespaceTest(unittest.TestCase):
    ACT = "Мотивировочная часть акта. " * 20  # заведомо длиннее 100 символов

    def _key(self, model):
        with patch.object(cm_config, "LLM_PROVIDER", "claude"), patch.object(
            cm_config, "CLAUDE_MODEL", model
        ):
            return cm_llm._act_cache_key(self.ACT)

    def test_haiku_key_is_unchanged(self):
        # Эталонная модель не добавляет суффикс: ключ = sha1(act|v3-detailed).
        import hashlib

        expected = hashlib.sha1(
            (self.ACT + "|v3-detailed").encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(self._key(cm_config.DEFAULT_CLAUDE_MODEL), expected)

    def test_non_default_models_get_distinct_keys(self):
        haiku = self._key(cm_config.DEFAULT_CLAUDE_MODEL)
        sonnet = self._key("claude-sonnet-5")
        opus = self._key("claude-opus-4-8")
        self.assertNotEqual(haiku, sonnet)
        self.assertNotEqual(haiku, opus)
        self.assertNotEqual(sonnet, opus)

    def _key_with_effort(self, model, effort):
        with patch.object(cm_config, "LLM_PROVIDER", "claude"), patch.object(
            cm_config, "CLAUDE_MODEL", model
        ), patch.object(cm_config, "CLAUDE_EFFORT", effort):
            return cm_llm._act_cache_key(self.ACT)

    def test_effort_namespaces_non_default_model(self):
        # Пересказы с разным эффортом не должны молча читать чужой кэш.
        default_eff = self._key_with_effort("claude-opus-4-8", "")
        high = self._key_with_effort("claude-opus-4-8", "high")
        maxi = self._key_with_effort("claude-opus-4-8", "max")
        self.assertNotEqual(default_eff, high)
        self.assertNotEqual(high, maxi)

    def test_haiku_key_ignores_effort(self):
        # Для эталона эффорт не отправляется — ключ остаётся боевым.
        plain = self._key_with_effort(cm_config.DEFAULT_CLAUDE_MODEL, "")
        with_eff = self._key_with_effort(cm_config.DEFAULT_CLAUDE_MODEL, "high")
        self.assertEqual(plain, with_eff)


if __name__ == "__main__":
    unittest.main()
