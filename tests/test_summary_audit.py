"""Технический отчёт не приписывает кэшу модель и объясняет реальный резерв."""
import json
import os
import sys
from collections import Counter
from unittest.mock import Mock

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from court_monitor import config
from court_monitor.digest import llm, summary_audit, summary_queue


ACT = ('Мотивировочная часть судебного акта. ' * 10 +
       ' Определила: решение оставить без изменения, жалобу без удовлетворения.')
SUMMARY = 'Долг подтверждён документами. Доказательств оплаты не представлено.'
META = {'stage': 'appeal', 'court_domain': 'example.sudrf.ru', 'case_number': '33-1/2026'}


def response(payload):
    result = Mock()
    result.json.return_value = payload
    return result


def http_error(status, body=''):
    result = requests.Response()
    result.status_code = status
    result._content = body.encode()
    return requests.HTTPError(response=result)


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    summary_audit.reset()
    for key, value in {
        'LLM_PROVIDER': 'openrouter', 'LLM_SUMMARY_PROVIDER_FALLBACK': True,
        'OPENROUTER_API_KEY': 'test-openrouter', 'GIGACHAT_AUTH_KEY': 'test-giga',
        'ANTHROPIC_API_KEY': 'test-claude',
        'OPENROUTER_SUMMARY_MODEL': 'apodex/apodex-1.1-mini:free',
        'GIGACHAT_MODEL': 'GigaChat-3-Pro', 'CLAUDE_MODEL': 'claude-haiku-4-5',
        'OPENROUTER_SUMMARY_RETRIES': 1,
        'LLM_PROVIDER_STATE_PATH': str(tmp_path / 'providers.json'),
        'ACT_SUMMARY_PENDING_PATH': str(tmp_path / 'pending.json'),
        'SUMMARY_CONTEXT_TOKENS': {'openrouter': 262144, 'gigachat': 262144, 'claude': 262144},
        'METRICS': Counter(), 'SUMMARY_MODELS_USED': set(),
    }.items():
        monkeypatch.setattr(config, key, value)
    monkeypatch.setattr(llm, '_openrouter_daily_exhausted', set())
    monkeypatch.setattr(llm, '_gigachat_token_cache', {})
    monkeypatch.setattr(llm.time, 'sleep', Mock())
    monkeypatch.setattr(requests.sessions.Session, 'request', Mock(
        side_effect=AssertionError('Сеть в тесте запрещена')))
    cache = {}
    monkeypatch.setattr(llm, '_load_act_summaries', lambda: dict(cache))
    monkeypatch.setattr(llm, '_save_act_summaries', lambda value: cache.update(value))
    yield cache
    summary_audit.reset()


def test_fallback_chain_actual_author_survives_cache_and_dedup(monkeypatch, isolated):
    post = Mock(side_effect=[
        http_error(429, 'Rate limit reached'), requests.Timeout('private details'),
        response({'model': 'claude-haiku-4-5-20251001',
                  'content': [{'type': 'text', 'text': SUMMARY}]}),
    ])
    monkeypatch.setattr(requests, 'post', post)
    monkeypatch.setattr(llm, '_gigachat_access_token', lambda: 'test-token')
    meta = dict(META)
    assert llm.summarize_act_motivation(ACT, case_meta=meta) == SUMMARY
    record = summary_audit.snapshot()[0]
    assert record['model'] == 'claude:claude-haiku-4-5-20251001'
    assert record['fallback'] is True
    assert record['cached'] is False
    assert [attempt['status'] for attempt in record['attempts']] == ['rate_limit', 'timeout', 'ready']
    assert next(iter(isolated.values()))['attempts'] == meta['_summary_result']['attempts']

    assert llm.summarize_act_motivation(ACT, case_meta=dict(META)) == SUMMARY
    assert summary_audit.snapshot() == [record]
    assert post.call_count == 3

    summary_audit.reset()
    assert llm.summarize_act_motivation(ACT, case_meta=dict(META)) == SUMMARY
    assert summary_audit.snapshot() == [dict(record, cached=True)]
    assert post.call_count == 3


def test_legacy_cache_model_is_unknown(monkeypatch):
    monkeypatch.setattr(llm, '_load_act_summaries', lambda: {llm._act_cache_key(ACT): {'summary': SUMMARY}})
    assert llm.summarize_act_motivation(ACT, case_meta=dict(META)) == SUMMARY
    record = summary_audit.snapshot()[0]
    assert (record['model'], record['cached'], record['fallback']) == ('unknown', True, None)


def test_tracked_early_cache_is_counted_once_with_original_history(monkeypatch):
    calls = Mock(side_effect=[http_error(503), response({
        'model': 'GigaChat-3-Pro-actual', 'choices': [{'message': {'content': SUMMARY}}]})])
    monkeypatch.setattr(requests, 'post', calls)
    monkeypatch.setattr(llm, '_gigachat_access_token', lambda: 'test-token')
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) == SUMMARY
    fresh = summary_audit.snapshot()
    assert len(fresh) == 1 and fresh[0]['cached'] is False
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) == SUMMARY
    assert summary_audit.snapshot() == fresh
    summary_audit.reset()
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) == SUMMARY
    assert summary_audit.snapshot() == [dict(fresh[0], cached=True)]
    assert [item['status'] for item in fresh[0]['attempts']] == ['http_503', 'ready']
    assert calls.call_count == 2


@pytest.mark.parametrize(('failure', 'reason'), [
    (http_error(429, 'Rate limit'), 'rate_limit'),
    (http_error(429, 'free-models-per-day'), 'daily_quota'),
    (http_error(502), 'http_502'),
    (requests.Timeout('secret timeout detail'), 'timeout'),
    (requests.ConnectionError('secret connection detail'), 'network'),
    (ValueError('secret invalid JSON detail'), 'invalid_response'),
    (requests.exceptions.JSONDecodeError('invalid JSON', 'secret', 0), 'invalid_response'),
])
@pytest.mark.parametrize('provider', ['openrouter', 'gigachat', 'claude'])
def test_recorded_failures_are_specific_and_safe(monkeypatch, provider, failure, reason):
    monkeypatch.setattr(config, 'LLM_PROVIDER', provider)
    monkeypatch.setattr(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', False)
    monkeypatch.setattr(llm, '_gigachat_access_token', lambda: 'test-token')
    monkeypatch.setattr(requests, 'post', Mock(side_effect=failure))
    assert llm.summarize_act_motivation(ACT, case_meta=dict(META), use_cache=False) is None
    record = summary_audit.snapshot()[0]
    assert record['status'] == reason
    assert record['attempts'][0]['status'] == reason
    serialized = json.dumps(record)
    assert 'secret' not in serialized and 'test-token' not in serialized and ACT not in serialized


def test_unknown_failure_does_not_reuse_previous_request_reason(monkeypatch):
    monkeypatch.setattr(llm, '_call_openrouter_simple', lambda *a, **kw: llm._summary_failure('rate_limit'))
    monkeypatch.setattr(llm, '_call_gigachat_simple', lambda *a, **kw: None)
    monkeypatch.setattr(llm, '_call_claude_simple', lambda *a, **kw: llm._ModelText(SUMMARY, 'actual'))
    assert llm.summarize_act_motivation(ACT, case_meta=dict(META), use_cache=False) == SUMMARY
    assert [a['status'] for a in summary_audit.snapshot()[0]['attempts']] == [
        'rate_limit', 'technical_error', 'ready']


def test_giga_oauth_error_is_recorded(monkeypatch):
    monkeypatch.setattr(config, 'LLM_PROVIDER', 'gigachat')
    monkeypatch.setattr(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', False)
    monkeypatch.setattr(requests, 'post', Mock(side_effect=http_error(401)))
    assert llm.summarize_act_motivation(ACT, case_meta=dict(META), use_cache=False) is None
    assert summary_audit.snapshot()[0]['attempts'][0]['status'] == 'http_401'


def test_identity_separates_courts_stages_and_sources():
    ids = {
        summary_audit.identity(ACT, META),
        summary_audit.identity(ACT, dict(META, court_domain='other.sudrf.ru')),
        summary_audit.identity(ACT, dict(META, stage='cassation')),
        summary_audit.identity(ACT + ' Дополнение.', META),
    }
    assert len(ids) == 4
    assert all(len(value) == 64 for value in ids)


def test_snapshot_has_no_source_or_exception_payload_and_is_independent():
    outcome = {'status': 'ready', 'summary': SUMMARY, 'text': ACT, 'api_key': 'secret',
               'attempts': [{'provider': 'claude', 'status': 'ready', 'body': 'secret'}]}
    key = summary_audit.identity(ACT, META)
    summary_audit.record(key, outcome)
    snapshot = summary_audit.snapshot()
    assert 'secret' not in json.dumps(snapshot) and ACT not in json.dumps(snapshot)
    snapshot[0]['attempts'][0]['status'] = 'modified'
    assert summary_audit.snapshot()[0]['attempts'][0]['status'] == 'ready'


@pytest.mark.parametrize('status', ['source_incomplete', 'needs_review', 'rate_limit'])
def test_deferred_queue_result_remains_visible(monkeypatch, status):
    key = summary_queue._key(ACT, META)
    summary_queue._save({key: {'status': status, 'text': ACT, 'meta': META,
                              'runs': 1, 'attempts': [], 'summary': '',
                              'retry_after': '9999-01-01T00:00:00+00:00'}})
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) is None
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) is None
    records = summary_audit.snapshot()
    assert len(records) == 1 and records[0]['status'] == status


def test_legacy_queue_fallback_is_proven_by_attempts_not_current_config(monkeypatch):
    monkeypatch.setattr(config, 'LLM_PROVIDER', 'gigachat')
    key = summary_queue._key(ACT, META)
    summary_queue._save({key: {'status': 'ready', 'text': ACT, 'meta': META,
        'summary': SUMMARY, 'model': 'gigachat:GigaChat-3-Pro', 'runs': 1,
        'attempts': [{'provider': 'openrouter', 'model': 'apodex/apodex-1.1-mini:free',
                      'status': 'rate_limit'},
                     {'provider': 'gigachat', 'model': 'GigaChat-3-Pro', 'status': 'ready'}]}})
    assert summary_queue.summarize_tracked(ACT, case_meta=dict(META)) == SUMMARY
    record = summary_audit.snapshot()[0]
    assert record['fallback'] is True and record['cached'] is True
    # Отсутствие истории не доказывает прежний резерв даже при другой
    # модели/провайдере в текущей конфигурации.
    summary_audit.reset()
    summary_audit.record('legacy', {'status': 'ready', 'model': 'claude:old'}, cached=True)
    assert summary_audit.snapshot()[0]['fallback'] is None
