# -*- coding: utf-8 -*-
"""Тесты выбора модели Claude (haiku/sonnet/opus) для тестового дайджеста.

Покрывают:
  - резолвер коротких имён в полный id API (config.resolve_claude_model);
  - метку модели `_current_digest_model_name()` для ветки claude;
  - неймспейс кэша пересказов `_act_cache_key`: эталонная haiku-модель даёт
    те же ключи, что и раньше (боевой .act_summaries.json не переиндексируется),
    а sonnet/opus получают свой ключ.

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


if __name__ == "__main__":
    unittest.main()
