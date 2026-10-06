# -*- coding: utf-8 -*-
"""Общий conftest всего pytest-набора.

Тестовый набор написан в контексте региона hmao (реальные суды/фикстуры
ХМАО). Форк территории задаёт свой регион файлом REGION в корне репо —
без этой фиксации pytest в форке грузил бы чужой реестр и падал. pytest
импортирует conftest раньше тестовых модулей (и, следовательно, раньше
court_monitor.config), поэтому env-переменная успевает до чтения.

Тесты, которым нужен другой регион, передают его явно
(match_region_first_instance(name, region), get_region(code)) или патчат
config.REGION monkeypatch'ем.
"""

import os

os.environ["REGION"] = "hmao"

# Любой тест рендера теперь может сохранять очередь пересказов. Изоляция
# на уровне всего набора исключает запись в рабочие данные и вызовы API.
import pytest


@pytest.fixture(autouse=True)
def _isolate_summary_pipeline(tmp_path, monkeypatch, request):
    from court_monitor import config
    from court_monitor.digest import llm
    if request.node.path.name != 'test_data_files_staged.py':
        monkeypatch.setattr(config, 'ACT_SUMMARY_PENDING_PATH', str(tmp_path / 'pending.json'))
        monkeypatch.setattr(config, 'LLM_PROVIDER_STATE_PATH', str(tmp_path / 'provider.json'))
    # Существующие тесты интерфейса патчат выбранный ими провайдер сами.
    monkeypatch.setattr(config, 'LLM_PROVIDER', 'claude')
    monkeypatch.setattr(llm, '_openrouter_daily_exhausted', set())
    monkeypatch.setattr(llm, '_gigachat_token_cache', {})
    monkeypatch.setattr(config, 'SUMMARY_MODELS_USED', set())

    for key in ('OPENROUTER_API_KEY', 'GIGACHAT_AUTH_KEY', 'ANTHROPIC_API_KEY'):
        monkeypatch.setattr(config, key, '')
    import requests
    def unexpected_network(*args, **kwargs):
        raise AssertionError('Незамоканный HTTP в изолированном тесте')
    monkeypatch.setattr(requests.sessions.Session, 'request', unexpected_network)
