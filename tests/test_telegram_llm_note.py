# -*- coding: utf-8 -*-
"""Тесты сервисной приписки «какая LLM делала дайджест» в Telegram-версии
дайджеста (_telegram_digest_text / _llm_digest_note в runs.py).

Приписка добавляется ТОЛЬКО когда получатель — личный чат юриста
(TELEGRAM_CHAT_ID == TELEGRAM_CHAT_ID_PERSONAL); в корпоративную группу и
при незаданной TELEGRAM_CHAT_ID_PERSONAL служебная строка не уходит.

Запуск: `python3 -m pytest tests/test_telegram_llm_note.py` из корня репо.
"""

import os
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

from court_monitor import config as cm_config  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402

DIGEST = "📊 <b>Мониторинг дел</b>\nстрока дайджеста"


class TelegramLlmNoteTest(unittest.TestCase):
    def test_note_appended_for_personal_chat(self):
        with patch.object(cm_config, "TELEGRAM_CHAT_ID", "111"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID_PERSONAL", "111"), \
             patch.object(cm_config, "LLM_PROVIDER", "claude"), \
             patch.object(cm_config, "DIGEST_FULL_LLM", False):
            out = cm_runs._telegram_digest_text(DIGEST)
        self.assertIn("🤖 LLM: claude-haiku-4-5-20251001", out)
        self.assertIn("гибрид", out)
        self.assertTrue(out.startswith(DIGEST))

    def test_no_note_for_group_chat(self):
        with patch.object(cm_config, "TELEGRAM_CHAT_ID", "-100222"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID_PERSONAL", "111"):
            out = cm_runs._telegram_digest_text(DIGEST)
        self.assertNotIn("🤖 LLM:", out)

    def test_no_note_without_personal_env(self):
        # Локальный запуск / Mac-резерв: TELEGRAM_CHAT_ID_PERSONAL пуст.
        with patch.object(cm_config, "TELEGRAM_CHAT_ID", "111"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID_PERSONAL", ""):
            out = cm_runs._telegram_digest_text(DIGEST)
        self.assertNotIn("🤖 LLM:", out)

    def test_note_reflects_provider_and_full_llm_mode(self):
        with patch.object(cm_config, "TELEGRAM_CHAT_ID", "111"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID_PERSONAL", "111"), \
             patch.object(cm_config, "LLM_PROVIDER", "gigachat"), \
             patch.object(cm_config, "GIGACHAT_MODEL", "GigaChat-2-Max"), \
             patch.object(cm_config, "DIGEST_FULL_LLM", True):
            out = cm_runs._telegram_digest_text(DIGEST)
        self.assertIn("🤖 LLM: gigachat:GigaChat-2-Max", out)
        self.assertIn("полный LLM-дайджест", out)

    def test_note_for_openrouter_uses_model_label(self):
        with patch.object(cm_config, "TELEGRAM_CHAT_ID", "111"), \
             patch.object(cm_config, "TELEGRAM_CHAT_ID_PERSONAL", "111"), \
             patch.object(cm_config, "LLM_PROVIDER", "openrouter"), \
             patch.object(cm_config, "OPENROUTER_MODEL", "qwen/qwen3:free"), \
             patch.object(cm_config, "DIGEST_FULL_LLM", False):
            out = cm_runs._telegram_digest_text(DIGEST)
        self.assertIn("🤖 LLM: openrouter:qwen/qwen3:free", out)


if __name__ == "__main__":
    unittest.main()
