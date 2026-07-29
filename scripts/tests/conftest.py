# -*- coding: utf-8 -*-
"""Общие фикстуры тестов scripts/tests.

Пер-суд предохранитель карточек живёт в модульном config.CARD_BREAKER
(состояние одного прогона, в бою сбрасывается _metrics_reset на старте
main*). Тесты зовут netutil.fetch_card_checked с фейлами на общих
хостах-заглушках («x») — без очистки фейлы копились бы МЕЖДУ тестами и,
достигнув порога, молча отключали бы хост для последующих тестов файла
(fetch вернул бы "" без вызова замоканной сети). Чистим до и после каждого
теста.
"""

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config as cm_config  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_card_breaker():
    cm_config.CARD_BREAKER.clear()
    yield
    cm_config.CARD_BREAKER.clear()
