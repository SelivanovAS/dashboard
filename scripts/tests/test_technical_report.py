"""Результаты публикации и доставки нельзя выводить из общего exit code."""
from copy import deepcopy
from datetime import datetime
import io
import json
import sys
from types import SimpleNamespace

import pytest

from court_monitor import config, delivery, runs, technical_report as report


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('TECHNICAL_REPORT_PATH', str(tmp_path / 'report.json'))
    monkeypatch.setenv('TELEGRAM_DIGEST_MODE', 'technical')
    monkeypatch.setenv('DEFER_TECHNICAL_REPORT', '1')
    monkeypatch.setenv('GITHUB_RUN_ID', '100')
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '1')
    monkeypatch.setattr(config, 'REGION', 'hmao')
    monkeypatch.setattr(config, 'PARSE_HEALTH_PATH', str(tmp_path / 'health.json'))
    monkeypatch.setattr(config, 'LAST_PERSONAL_PUSHES_PATH', str(tmp_path / 'pushes.json'))
    monkeypatch.setattr(config, 'ACT_SUMMARY_PENDING_PATH', str(tmp_path / 'summaries.json'))
    monkeypatch.setattr(report, 'now', lambda: '2026-10-07T09:05:00+05:00')
    config._metrics_reset()
    return tmp_path


def health(tmp_path, **changes):
    data = {'last_run': {'at': '2026-10-07T08:35:40+05:00', 'cards_read_today': 319,
             'cards_planned_today': 334, 'cards_read': 1, 'cards_planned': 16,
             'instances': {'cassation': {'read_today': 8, 'planned_today': 16}},
             'fail_kinds': {'portal_placeholder': 2}}, 'sources': {
             'fi:court': {'last_run_at': '2026-10-07T08:00:00', 'last_count': 8,
                           'fail_streak': 1, 'label': 'Тестовый суд'}}}
    data['last_run'].update(changes)
    (tmp_path / 'health.json').write_text(json.dumps(data))
    return data


def test_daily_coverage_and_failed_source_not_hidden_by_previous_success(isolated):
    health(isolated)
    result = report.parser_snapshot('2026-10-07')
    assert (result['read'], result['planned']) == (319, 334)
    assert result['status'] == 'partial'
    assert result['sources'][0]['status'] == 'failed'
    report.record_digest({'saved_at': '2026-10-07T08:45:00'}, summaries=[])
    rendered = report.render(report.finalize())
    assert '319/334' in rendered and 'Кассация: 8/16' in rendered
    assert 'заглушка портала — 2' in rendered


def test_yesterday_not_today_even_workflow_success(isolated):
    health(isolated, at='2026-10-06T08:35:40+05:00')
    result = report.finalize(workflow_status='success', digest_step='success')
    assert result['parser']['status'] == 'unconfirmed'
    assert 'сборка не подтверждена' in report.render(result)
    assert '319/334' not in report.render(result)


def test_report_day_uses_yekaterinburg_date(isolated):
    health(isolated, at='2026-10-06T23:50:00+00:00')
    report.record_digest({'saved_at': '2026-10-06T23:55:00Z'}, summaries=[])
    assert report.load()['date'] == '2026-10-07'
    assert report.load()['parser']['read'] == 319


def test_another_region_or_workflow_attempt_cannot_supply_success(isolated, monkeypatch):
    report.update(digest={'status': 'ready'})
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '2')
    assert report.load() == {}
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '1')
    monkeypatch.setattr(config, 'REGION', 'tyumen')
    assert report.load() == {}


def test_warning_pages_and_failed_push_survive_green_workflow(isolated):
    health(isolated)
    report.record_digest({'saved_at': '2026-10-07'}, summaries=[])
    report.record_push({'status': 'partial', 'accepted': 18, 'failed': 1, 'skipped': 5})
    result = report.finalize(workflow_status='success', data_publication='confirmed', pages='unconfirmed')
    text = report.render(result)
    assert 'свежий выпуск не подтверждён' in text
    assert 'принято сервисами — 18; ошибок — 1' in text
    assert 'получили' not in text


def test_model_fallback_cache_and_unknown_author_are_explicit():
    rows = [{'status': 'ready', 'model': 'claude:haiku', 'cached': False, 'fallback': True,
             'attempts': [{'model': 'Apodex', 'status': 'rate_limit'},
                          {'model': 'Apodex', 'status': 'rate_limit'},
                          {'model': 'GigaChat', 'status': 'timeout'},
                          {'model': 'haiku', 'status': 'ready'}]},
            {'status': 'ready', 'model': 'unknown', 'cached': True, 'fallback': None}]
    lines = report.summary_lines(rows)
    assert lines[0].count('Apodex') == 1
    assert 'GigaChat: таймаут' in lines[0] and 'резерв:' in lines[0]
    assert 'модель не зафиксирована: 1 из кэша' == lines[1]


def test_raw_acts_and_personal_details_never_enter_report(isolated):
    health(isolated)
    report.record_digest({'saved_at': '2026-10-07', 'fi_new_cases': [{'plaintiff': 'SECRET PERSON',
                         'act_text': 'SECRET ACT'}]}, summaries=[])
    saved = report.path().read_text()
    assert 'SECRET' not in saved
    assert report.load()['digest']['events']['new'] == 1


def test_render_escapes_dynamic_values(isolated):
    value = {'region': 'hmao', 'date': '2026-10-07', 'summaries': [
        {'status': 'ready', 'model': '<secret&>', 'cached': False}],
        'warnings': ['<script>alert(1)</script>']}
    rendered = report.render(value)
    assert '<script>' not in rendered and '&lt;secret&amp;&gt;' in rendered


def test_personal_digest_suppressed_group_kept(monkeypatch):
    monkeypatch.delenv('TELEGRAM_DIGEST_MODE', raising=False)
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID_PERSONAL', '123')
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID', '123')
    sent = []
    monkeypatch.setattr(runs, 'send_telegram', sent.append)
    monkeypatch.setattr(runs, '_telegram_digest_text', lambda s: s)
    runs._send_digest_telegram('digest')
    assert sent == []
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID', '-456')
    runs._send_digest_telegram('group digest')
    assert sent == ['group digest']


def test_telegram_api_confirmation_and_personal_recipient(isolated, monkeypatch):
    monkeypatch.setattr(config, 'TELEGRAM_BOT_TOKEN', 'never-log')
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID', '-group')
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID_PERSONAL', 'personal')
    requests = []
    def fake(request, **kwargs):
        requests.append(json.loads(request.data))
        return io.BytesIO(b'{"ok":false}')
    monkeypatch.setattr(report, 'urlopen', fake)
    assert report.telegram_send('text')['ok'] is False
    assert requests[0]['chat_id'] == 'personal'


def test_failed_notification_can_retry_success_deduplicates(isolated, monkeypatch):
    value = report.finalize()
    receipts = iter([{'ok': False}, {'ok': True, 'message_id': 9}])
    sent = []
    monkeypatch.setattr(report, 'telegram_send', lambda text: sent.append(text) or next(receipts))
    assert report.send(value) is False
    assert report.send(value) is True
    assert report.send(value) is True
    assert len(sent) == 2


@pytest.fixture
def push_env(isolated, monkeypatch):
    monkeypatch.setattr(config, 'PUSH_WORKER_URL', 'https://example.invalid')
    monkeypatch.setattr(config, 'PUSH_SECRET', 'secret')
    monkeypatch.setattr(config, 'VAPID_PRIVATE_KEY', 'key')
    class PushError(Exception):
        def __init__(self, status):
            self.response = SimpleNamespace(status_code=status)
    monkeypatch.setitem(sys.modules, 'py_vapid', SimpleNamespace(Vapid=SimpleNamespace(from_pem=lambda x: object())))
    return PushError


def test_push_counters_and_journal_record_actual_outcome(push_env, isolated, monkeypatch):
    subscriptions = [{'endpoint': 'ok'}, {'endpoint': 'expired'}, {'endpoint': 'skip'}]
    monkeypatch.setattr(delivery.requests, 'get', lambda *a, **k: SimpleNamespace(ok=True, json=lambda: subscriptions))
    def webpush(**kwargs):
        if kwargs['subscription_info']['endpoint'] == 'expired':
            raise push_env(410)
    monkeypatch.setitem(sys.modules, 'pywebpush', SimpleNamespace(webpush=webpush, WebPushException=push_env))
    dropped = []
    monkeypatch.setattr(delivery, '_drop_dead_subscription', dropped.append)
    result = delivery.send_web_push('title', 'body', per_subscriber=lambda sub:
        None if sub['endpoint'] == 'skip' else ('title', 'body', '/'))
    assert result == {'status': 'partial', 'subscriptions': 3, 'attempted': 2,
                      'accepted': 1, 'failed': 1, 'skipped': 1, 'expired': 1}
    assert dropped == ['expired']
    data = json.loads((isolated / 'pushes.json').read_text())
    assert [x['delivery_status'] for x in data['items']] == ['accepted', 'failed', 'skipped']
    assert report.load()['push']['accepted'] == 1


@pytest.mark.parametrize('response,reason', [(SimpleNamespace(ok=False, status_code=503), 'subscriptions_http_503'),
    (SimpleNamespace(ok=True, json=lambda: {'error': 'unexpected'}), 'invalid_subscriptions')])
def test_subscription_error_is_not_zero_success(push_env, monkeypatch, response, reason):
    monkeypatch.setattr(delivery.requests, 'get', lambda *a, **k: response)
    result = delivery.send_web_push('title', 'body')
    assert result['status'] == 'failed' and result['reason'] == reason
    assert report.load()['push']['status'] == 'failed'


def test_zero_subscriptions_is_known_empty(push_env, monkeypatch):
    monkeypatch.setattr(delivery.requests, 'get', lambda *a, **k: SimpleNamespace(ok=True, json=lambda: []))
    result = delivery.send_web_push('title', 'body')
    assert result['status'] == 'no_subscriptions' and result['subscriptions'] == 0


def test_absent_push_configuration_reported(isolated, monkeypatch):
    monkeypatch.setattr(config, 'PUSH_SECRET', '')
    result = delivery.send_web_push('title', 'body')
    assert result['status'] == 'not_configured'
    assert report.load()['push']['status'] == 'not_configured'


def test_calendar_skip_is_normal_and_does_not_claim_delivery(isolated):
    report.update(execution={'status': 'skipped', 'reason': 'non_working_day'})
    text = report.render(report.finalize(workflow_status='success', digest_step='success'))
    assert '⏸' in text and 'нерабочий день' in text
    assert 'проверить' not in text and 'свежий выпуск подтверждён' not in text


def test_intentionally_unpublished_test_is_not_a_publication_error(isolated):
    health(isolated, cards_read_today=334)
    report.record_digest({'saved_at': '2026-10-07'}, summaries=[])
    report.record_push({'status': 'not_configured'})
    value = report.finalize(workflow_status='success', data_publication='skipped', pages='skipped', push_step='skipped')
    text = report.render(value)
    assert value['push']['status'] == 'skipped'
    assert 'проверить публикацию' not in text and 'проверить ошибки push' not in text


def test_failed_push_step_keeps_counters_but_does_not_claim_completion(isolated):
    report.record_push({'status': 'complete', 'accepted': 3, 'failed': 0})
    result = report.finalize(push_step='failure')
    assert result['push'] == {'status': 'failure', 'accepted': 3, 'failed': 0}
    assert 'шаг отправки завершился ошибкой' in report.render(result)


def test_telegram_digest_receipt_records_failed_group(isolated, monkeypatch):
    monkeypatch.setenv('TELEGRAM_DIGEST_MODE', 'digest')
    monkeypatch.setattr(runs, '_telegram_digest_text', lambda s: s)
    monkeypatch.setattr(runs, 'send_telegram', lambda text: {'status': 'failed', 'failed': 1, 'accepted': 0})
    runs._send_digest_telegram('message')
    assert report.load()['telegram_digest']['target'] == 'group'
    text = report.render(report.finalize(workflow_status='success'))
    assert 'Telegram-группа: отправка не подтверждена' in text


def test_telegram_transport_requires_api_ok_and_counts_exception(isolated, monkeypatch):
    monkeypatch.setattr(config, 'TELEGRAM_BOT_TOKEN', 'test-token')
    monkeypatch.setattr(config, 'TELEGRAM_CHAT_ID', '-123')
    monkeypatch.setattr(delivery.requests, 'post', lambda *a, **k:
        SimpleNamespace(ok=True, status_code=200, text='rejected', json=lambda: {'ok': False}))
    assert delivery.send_telegram('message')['status'] == 'failed'
    def fail(*a, **k):
        raise TimeoutError('sensitive text')
    monkeypatch.setattr(delivery.requests, 'post', fail)
    assert delivery.send_telegram('message')['failed'] == 1
    assert config.METRICS['telegram_failed'] == 2


def test_deferred_crash_is_reported_once_and_does_not_leak_exception(isolated, monkeypatch):
    monkeypatch.setattr(delivery, 'send_telegram', lambda text: pytest.fail('duplicate crash alert'))
    delivery.send_crash_alert('replay-last', RuntimeError('SECRET'))
    value = report.load()
    assert value['warnings'] == ['Сбой режима replay-last: RuntimeError']
    assert 'SECRET' not in report.path().read_text()
