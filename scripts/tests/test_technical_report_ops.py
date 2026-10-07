"""Отчёты исполнителя: независимый контроль, дневной дедуп и честные итоги.

Telegram полностью заменён подтверждаемым транспортом; все данные временные.
"""
from argparse import Namespace
import fcntl
import json
from types import SimpleNamespace

import pytest

from court_monitor import technical_report_ops as ops


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.delenv('REGION', raising=False)
    repo = tmp_path / 'repo'
    (repo / 'data').mkdir(parents=True)
    (repo / 'REGION').write_text('tyumen')
    config = tmp_path / 'telegram'
    config.write_text('token=TEST_TOKEN\nchat_id=TEST_CHAT\n')
    clock = ['2026-10-07T10:00:00+05:00']
    sent = []
    monkeypatch.setattr(ops.report, 'now', lambda: clock[0])
    def send(text, **credentials):
        assert credentials == {'token': 'TEST_TOKEN', 'chat_id': 'TEST_CHAT'}
        sent.append(text)
        return {'ok': True}
    monkeypatch.setattr(ops.report, 'telegram_send', send)
    return SimpleNamespace(repo=repo, config=config, clock=clock, sent=sent)


def run(setup, event, message=''):
    return ops.main(Namespace(repo=str(setup.repo), event=event,
                              telegram_config=str(setup.config), message=message))


def health(setup, read=10, planned=100, at=None, failures=None, alerts=None, events=0):
    (setup.repo / 'data/parse_health.json').write_text(json.dumps({'last_run': {
        'at': at or setup.clock[0], 'cards_read_today': read, 'cards_planned_today': planned,
        'fail_kinds': failures or {}, 'alerts': alerts or []}}))
    (setup.repo / 'data/cases.json').write_text(json.dumps({
        'pending_retry_context': {'fi_changes': [{}] * events}}))


def state(setup):
    return ops.report.read_json(setup.repo / 'ops/mac-local-run/.runtime/technical_report_ops.tyumen.json')


def test_watchdog_notices_no_run_without_running_parser_and_sends_once(setup):
    setup.clock[0] = '2026-10-07T09:59:59+05:00'
    assert run(setup, 'watchdog') == 0
    assert not setup.sent
    setup.clock[0] = '2026-10-07T10:00:00+05:00'
    assert run(setup, 'watchdog') == 0
    assert len(setup.sent) == 1
    assert 'Завершение сегодняшнего прогона не подтверждено' in setup.sent[0]
    assert 'Тюменская область' in setup.sent[0]
    assert run(setup, 'watchdog') == 0
    assert len(setup.sent) == 1


@pytest.mark.parametrize('at', ['2026-10-07T06:00:00', '2026-10-07T01:00:00Z'])
def test_today_completed_run_suppresses_missing_alert(setup, at):
    health(setup, at=at)
    run(setup, 'parse-result')
    assert run(setup, 'watchdog') == 0
    assert not setup.sent


def test_today_health_without_after_publish_confirmation_is_not_a_completed_run(setup):
    health(setup)
    run(setup, 'watchdog')
    assert len(setup.sent) == 1
    assert 'Завершение сегодняшнего прогона не подтверждено' in setup.sent[0]
    assert 'не состоялся' not in setup.sent[0]


def test_yesterday_run_does_not_satisfy_watchdog(setup):
    health(setup, at='2026-10-06T23:00:00+05:00')
    assert run(setup, 'watchdog') == 0
    assert len(setup.sent) == 1


@pytest.mark.parametrize('actual_start,expected_count', [('original', 0), ('reused', 1)])
def test_watchdog_respects_live_lock_but_not_reused_pid(setup, monkeypatch, actual_start, expected_count):
    lock = setup.repo / 'ops/mac-local-run/.run.lock'
    lock.mkdir(parents=True)
    owner = {'pid': 123, 'process_start': 'original'}
    (lock / 'owner.json').write_text(json.dumps(owner))
    monkeypatch.setattr(ops.subprocess, 'run', lambda *a, **kw:
                        SimpleNamespace(returncode=0, stdout=actual_start))
    assert run(setup, 'watchdog') == 0
    assert len(setup.sent) == expected_count
    assert json.loads((lock / 'owner.json').read_text()) == owner


def test_failed_telegram_does_not_consume_daily_alert(setup, monkeypatch):
    attempts = []
    def send(text, **credentials):
        attempts.append(text)
        return {'ok': len(attempts) >= 2}
    monkeypatch.setattr(ops.report, 'telegram_send', send)
    assert run(setup, 'watchdog') == 0
    assert not state(setup)['sent']
    assert run(setup, 'watchdog') == 0
    assert len(state(setup)['sent']) == 1
    assert run(setup, 'watchdog') == 0
    assert len(attempts) == 2


def test_failure_is_deduplicated_sanitized_and_is_not_a_delivery_claim(setup):
    message = 'ошибка TEST_TOKEN chat TEST_CHAT https://host/?secret=RAW_TOKEN Authorization=SECRET'
    assert run(setup, 'failure', message) == 0
    assert run(setup, 'failure', message) == 0
    assert len(setup.sent) == 1
    for secret in ('TEST_TOKEN', 'TEST_CHAT', 'RAW_TOKEN', '=SECRET'):
        assert secret not in setup.sent[0]
        assert secret not in json.dumps(state(setup))
    assert 'рассылки этим сообщением не подтверждён' in setup.sent[0]
    setup.clock[0] = '2026-10-08T10:00:00+05:00'
    assert run(setup, 'failure', message) == 0
    assert len(setup.sent) == 2


def test_parse_result_saves_baseline_without_sending(setup):
    health(setup)
    before = {p.name: p.read_bytes() for p in (setup.repo / 'data').iterdir()}
    assert run(setup, 'parse-result') == 0
    assert not setup.sent
    assert state(setup)['baseline']['read'] == 10
    assert {p.name: p.read_bytes() for p in (setup.repo / 'data').iterdir()} == before


def test_retry_reports_significant_cumulative_gain_once(setup):
    health(setup)
    run(setup, 'parse-result')
    for read in (11, 12, 14):
        health(setup, read=read)
        assert run(setup, 'retry-result') == 0
    assert not setup.sent
    health(setup, read=15)
    run(setup, 'retry-result')
    assert len(setup.sent) == 1
    assert 'Дополнительно прочитано: 5' in setup.sent[0]
    assert 'Повторная рассылка дайджеста не запускалась' in setup.sent[0]
    run(setup, 'retry-result')
    health(setup, read=16)
    run(setup, 'retry-result')
    assert len(setup.sent) == 1
    health(setup, read=20)
    run(setup, 'retry-result')
    assert len(setup.sent) == 2


@pytest.mark.parametrize('change', [{'read': 100}, {'events': 1},
                                   {'failures': {'captcha': 1}}, {'alerts': ['Поиск недоступен']}])
def test_retry_completion_event_or_new_problem_is_meaningful(setup, change):
    health(setup, read=99)
    run(setup, 'parse-result')
    health(setup, **{'read': 99, **change})
    run(setup, 'retry-result')
    assert len(setup.sent) == 1
    run(setup, 'retry-result')
    assert len(setup.sent) == 1


def test_first_retry_does_not_attribute_whole_day_to_current_attempt(setup):
    health(setup, read=90, events=30)
    run(setup, 'retry-result')
    assert not setup.sent
    assert state(setup)['baseline']['read'] == 90


@pytest.mark.parametrize('failure_event', ['watchdog', 'failure'])
def test_recovery_after_successful_published_parse_is_reported_once(setup, failure_event):
    run(setup, failure_event, 'парсинг rc=7')
    health(setup)
    run(setup, 'parse-result')
    assert len(setup.sent) == 2
    assert 'Новый прогон завершён после сбоя; данные опубликованы' in setup.sent[-1]
    assert 'Web Push подтверждается отдельным отчётом' in setup.sent[-1]
    run(setup, 'parse-result')
    assert len(setup.sent) == 2


def test_failed_recovery_confirmation_can_be_retried(setup, monkeypatch):
    run(setup, 'failure', 'парсинг rc=7')
    health(setup)
    monkeypatch.setattr(ops.report, 'telegram_send', lambda *a, **k: {'ok': False})
    run(setup, 'parse-result')
    assert state(setup)['failure_active']
    monkeypatch.setattr(ops.report, 'telegram_send', lambda *a, **k: {'ok': True})
    run(setup, 'watchdog')
    assert not state(setup)['failure_active']
    assert 'pending_recovery' not in state(setup)


def test_watchdog_retries_unacknowledged_specific_failure_before_generic_missing_run(setup, monkeypatch):
    monkeypatch.setattr(ops.report, 'telegram_send', lambda *a, **k: {'ok': False})
    run(setup, 'failure', 'git push данных не удался')
    assert state(setup)['pending_failure']
    def send(text, **credentials):
        setup.sent.append(text)
        return {'ok': True}
    monkeypatch.setattr(ops.report, 'telegram_send', send)
    run(setup, 'watchdog')
    assert len(setup.sent) == 1
    assert 'git push данных не удался' in setup.sent[0]
    assert 'pending_failure' not in state(setup)
    run(setup, 'watchdog')
    assert len(setup.sent) == 1


def test_same_failure_after_confirmed_recovery_starts_new_episode(setup):
    run(setup, 'failure', 'парсинг rc=7')
    run(setup, 'failure', 'парсинг rc=7')
    assert len(setup.sent) == 1
    health(setup)
    run(setup, 'parse-result')
    run(setup, 'failure', 'парсинг rc=7')
    run(setup, 'failure', 'парсинг rc=7')
    assert len(setup.sent) == 3
    setup.clock[0] = '2026-10-07T11:00:00+05:00'
    health(setup)
    run(setup, 'parse-result')
    assert len(setup.sent) == 4


def test_successful_parse_clears_not_yet_sent_old_failure(setup, monkeypatch):
    monkeypatch.setattr(ops.report, 'telegram_send', lambda *a, **k: {'ok': False})
    run(setup, 'failure', 'парсинг rc=7')
    health(setup)
    run(setup, 'parse-result')
    assert 'pending_failure' not in state(setup)
    def send(text, **credentials):
        setup.sent.append(text)
        return {'ok': True}
    monkeypatch.setattr(ops.report, 'telegram_send', send)
    run(setup, 'watchdog')
    assert not setup.sent


def test_reporting_lock_does_not_take_or_modify_parser_lock(setup):
    runtime = setup.repo / 'ops/mac-local-run/.runtime'
    runtime.mkdir(parents=True)
    with (runtime / 'technical_report_ops.tyumen.lock').open('a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert run(setup, 'failure', 'failure') == 0
        assert not setup.sent
    assert not (runtime.parent / '.run.lock').exists()


def test_unknown_region_does_not_create_foreign_state_or_reveal_value(setup, capsys):
    (setup.repo / 'REGION').write_text('../secret-region')
    assert run(setup, 'watchdog') == 1
    assert 'secret-region' not in capsys.readouterr().err
    assert not setup.sent


def test_explicit_region_env_matches_parser_selection_precedence(setup, monkeypatch):
    monkeypatch.setenv('REGION', 'bashkortostan')
    run(setup, 'watchdog')
    assert 'Башкортостан' in setup.sent[0]
    runtime = setup.repo / 'ops/mac-local-run/.runtime'
    assert (runtime / 'technical_report_ops.bashkortostan.json').exists()
    assert not (runtime / 'technical_report_ops.tyumen.json').exists()
