"""Прокси только для OpenRouter, отказ без утечки на прямой маршрут."""
from unittest.mock import Mock
import pytest
import requests
from court_monitor import config, openrouter_transport as route
from court_monitor.digest import llm


def test_disabled_and_invalid_route(monkeypatch):
    monkeypatch.delenv('OPENROUTER_PROXY_URL', raising=False)
    assert 'proxies' not in route.options()
    monkeypatch.setenv('OPENROUTER_PROXY_URL', 'http://secret@example.invalid:8080')
    with pytest.raises(requests.RequestException) as error:
        route.options()
    assert 'secret' not in str(error.value)


def test_summary_and_catalog_use_only_explicit_proxy(monkeypatch):
    monkeypatch.setenv('OPENROUTER_PROXY_URL', route.PROXY_URL)
    monkeypatch.setattr(config, 'OPENROUTER_API_KEY', 'fixture')
    monkeypatch.setattr(config, 'SUMMARY_CONTEXT_TOKENS', {})
    result = Mock()
    result.json.return_value = {'choices': [{'message': {'content': 'answer'}}]}
    post = Mock(return_value=result)
    monkeypatch.setattr(llm.requests, 'post', post)
    assert llm._call_openrouter_simple('test', model='test/model') == 'answer'
    assert post.call_args.kwargs['proxies'] == {'http': route.PROXY_URL, 'https': route.PROXY_URL}
    assert post.call_args.kwargs['allow_redirects'] is False
    models = Mock()
    models.json.return_value = {'data': [{'id': 'test/model', 'context_length': 16000}]}
    get = Mock(return_value=models)
    monkeypatch.setattr(llm.requests, 'get', get)
    monkeypatch.setattr(llm, '_context_limits', {})
    assert llm._context_limit('openrouter', 'test/model') == 16000
    assert get.call_args.kwargs['proxies']['https'] == route.PROXY_URL


def test_proxy_failure_does_not_retry_direct(monkeypatch):
    monkeypatch.setenv('OPENROUTER_PROXY_URL', route.PROXY_URL)
    monkeypatch.setattr(config, 'OPENROUTER_API_KEY', 'fixture')
    post = Mock(side_effect=requests.exceptions.ProxyError('tunnel unavailable'))
    monkeypatch.setattr(llm.requests, 'post', post)
    assert llm._call_openrouter_simple('test', model='test/model') is None
    assert post.call_count == 1
    assert post.call_args.kwargs['proxies']['https'] == route.PROXY_URL
