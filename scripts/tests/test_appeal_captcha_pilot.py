"""Пилот без сети: территория, три попытки, API/суд и передача в обычный поиск."""
import ast
import base64
from contextlib import contextmanager
from dataclasses import replace
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from court_monitor import appeal_captcha as pilot, captcha_audit, config, runs
from court_monitor import technical_report as report
from court_monitor.regions import get_region

COURT = get_region('hmao').appeal_courts[0]
IMAGE = base64.b64encode(b'\x89PNG\r\n\x1a\nsynthetic-image').decode()
FORM = ('<form name="calform" method="get" action="modules.php">'
        '<input type="hidden" name="delo_id" value="5">'
        '<input type="hidden" name="delo_table" value="g2_case">'
        '<input type="hidden" name="captchaid" value="private-form-token">'
        '<input name="captcha"><img src="data:image/png;base64,' + IMAGE + '"></form>')
WRONG = 'Неверно указан проверочный код' + FORM
EMPTY = 'Данных по запросу не обнаружено'
RESULT = ('<table><tr><th>№ дела</th><th>Дата поступления</th><th>Стороны</th></tr>'
          '<tr><td><a href="/modules.php?name=sud_delo&amp;name_op=case&amp;delo_id=5'
          '&amp;case_id=7&amp;case_uid=abcdef">33-7/2026</a></td><td>10.10.2026</td>'
          '<td>ИСТЕЦ(ЗАЯВИТЕЛЬ): Иванов ОТВЕТЧИК: Сбербанк</td></tr></table>')


def response(text='', code=200, body=None):
    return SimpleNamespace(status_code=code, content=text.encode('windows-1251'), json=lambda: body)


def model(answer='00444', **changes):
    body = {'choices': [{'message': {'content': answer}}],
            'usage': {'prompt_tokens': 137, 'completion_tokens': 4}}
    body.update(changes)
    return response(body=body)


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv('HMAO_APPEAL_CAPTCHA_ENABLED', '1')
    monkeypatch.setenv('CLOUDRU_API_KEY', 'private-test-key')
    monkeypatch.setenv('TECHNICAL_REPORT_PATH', str(tmp_path / 'report.json'))
    monkeypatch.setenv('DEFER_TECHNICAL_REPORT', '1')
    monkeypatch.setattr(config, 'REGION', 'hmao')
    monkeypatch.setattr(config, 'PARSE_HEALTH_PATH', str(tmp_path / 'health.json'))
    monkeypatch.setattr(pilot.netutil, 'run_deadline_remaining', lambda: None)
    @contextmanager
    def slot(*args, **kwargs):
        yield True
    monkeypatch.setattr(pilot.host_throttle, 'request_slot', slot)
    calls, pending, sessions = [], [], []

    class Session:
        def __init__(self):
            self.headers = {}
            sessions.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def request(self, method, url, **kwargs):
            calls.append((method, url, kwargs, self))
            expected, result = pending.pop(0)
            assert expected == method
            assert kwargs['allow_redirects'] is False
            assert sum(kwargs['timeout']) <= pilot.PILOT_SECONDS
            if isinstance(result, Exception):
                raise result
            return result

        def get(self, url, **kwargs):
            return self.request('GET', url, **kwargs)

        def post(self, url, **kwargs):
            return self.request('POST', url, **kwargs)

    monkeypatch.setattr(pilot.requests, 'Session', Session)
    return SimpleNamespace(calls=calls, pending=pending, sessions=sessions, path=tmp_path)


def attempt(env, answer='00444', html=RESULT):
    env.pending.extend([('GET', response(FORM)), ('POST', model(answer)), ('GET', response(html))])


@pytest.mark.parametrize('region', ['hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen'])
def test_only_hmao_appeal_even_if_flag_leaks_to_other_region(env, monkeypatch, region):
    monkeypatch.setattr(config, 'REGION', region)
    for territory in ['hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen']:
        cfg = get_region(territory)
        for court in (*cfg.appeal_courts, *cfg.first_instance_courts, *cfg.presidium_courts, cfg.cassation_court):
            assert pilot.enabled_for(court) == (region == territory == 'hmao' and court == COURT)
            if not pilot.enabled_for(court):
                assert pilot.Pilot().search(court) is None
    assert env.calls == []


@pytest.mark.parametrize('changes', [{'delo_id': 2800001}, {'court_type': 'first_instance'},
    {'enabled': False}, {'srv_num': 2}, {'source': 'casebook'}, {'domain': 'example.invalid'}])
def test_pinned_scope(env, changes):
    assert not pilot.enabled_for(replace(COURT, **changes))


@pytest.mark.parametrize('flag', ['', '0', 'true'])
def test_off_by_default(env, monkeypatch, flag):
    monkeypatch.setenv('HMAO_APPEAL_CAPTCHA_ENABLED', flag)
    assert pilot.Pilot().search(COURT) is None
    assert not env.calls


def test_missing_key_does_not_fetch_court(env, monkeypatch):
    monkeypatch.delenv('CLOUDRU_API_KEY')
    p = pilot.Pilot()
    assert p.search(COURT) is None
    assert p.value['status'] == 'missing_key' and not env.calls


def test_two_rejections_then_success_fresh_cookies_fixed_model_and_no_fourth_call(env):
    attempt(env, html=WRONG)
    attempt(env, html=WRONG)
    attempt(env)
    p = pilot.Pilot()
    assert p.search(COURT) == RESULT
    assert p.search(COURT) is None
    assert p.value['status'] == 'accepted' and p.value['rows'] == 1
    assert p.value['model_calls'] == p.value['submissions'] == 3
    assert [x['status'] for x in p.value['attempts']] == ['wrong_code', 'wrong_code', 'accepted']
    assert p.value['estimated_rub'] == pytest.approx(3 * (137 * 34.16 + 4 * 136.64) / 1e6)
    assert p.value['usage_complete']
    for i in (0, 3, 6):
        capture, inference, submit = env.calls[i:i+3]
        assert capture[3] is submit[3] and inference[3] is not capture[3]
        assert capture[1] == pilot.FORM_URL and submit[1] == pilot.ORIGIN + '/modules.php'
        params = dict(submit[2]['params'])
        assert params['captcha'] == b'00444'  # Ведущие нули и повторы не меняются.
        assert params['G2_PARTS__NAMESS'] == 'Сбербанк'.encode('windows-1251')
        assert params['delo_id'] == b'5' and params['delo_table'] == b'g2_case'
        assert params['list'] == b'ON'
        assert 'Authorization' not in submit[2]['headers']
        payload = inference[2]['json']
        assert payload['model'] == captcha_audit.MODEL and payload['temperature'] == 0
        assert payload['messages'][0]['content'][1]['image_url']['url'].endswith(IMAGE)
    assert len({id(env.calls[i][3]) for i in (0, 3, 6)}) == 3
    saved = json.dumps(p.value)
    for secret in ('00444', IMAGE, 'private-test-key', 'private-form-token'):
        assert secret not in saved


def test_exactly_three_wrong_codes(env):
    for _ in range(3):
        attempt(env, html=WRONG)
    p = pilot.Pilot()
    assert p.search(COURT) is None
    assert p.value['status'] == 'attempts_exhausted'
    assert len(env.calls) == 9 and p.value['submissions'] == 3
    assert not p.value['accepted']


def test_invalid_output_never_submitted_and_still_counts_against_three(env):
    for answer in ('UNKNOWN', '123456', 'code 12345'):
        env.pending.extend([('GET', response(FORM)), ('POST', model(answer))])
    p = pilot.Pilot()
    assert p.search(COURT) is None
    assert p.value['model_calls'] == 3 and p.value['submissions'] == 0
    assert len(env.calls) == 6 and p.value['status'] == 'attempts_exhausted'


@pytest.mark.parametrize('reply,status', [(response(code=429), 'model_http'),
    (response(code=504), 'model_http'), (requests.Timeout('private-url'), 'model_timeout'),
    (requests.ConnectionError('private-key'), 'model_network'), (response(body={}), 'model_response')])
def test_model_errors_stop_without_spending_three_attempts(env, reply, status):
    env.pending.extend([('GET', response(FORM)), ('POST', reply)])
    p = pilot.Pilot()
    assert p.search(COURT) is None
    assert p.value['status'] == status and p.value['model_calls'] == 1
    assert p.value['submissions'] == 0 and not p.value['usage_complete']
    assert 'private-' not in json.dumps(p.value)


@pytest.mark.parametrize('reply,status', [(response(code=302), 'court_http'),
    (response(code=503), 'court_http'), (requests.Timeout('private-url'), 'court_timeout'),
    (requests.ConnectionError('private-cookie'), 'court_network')])
def test_court_transport_stops(env, reply, status):
    env.pending.append(('GET', reply))
    p = pilot.Pilot()
    assert p.search(COURT) is None
    assert p.value['status'] == status and not p.value['model_calls']


@pytest.mark.parametrize('bad', [FORM.replace('value="5"', 'value="2800001"'),
    FORM.replace('action="modules.php"', 'action="https://example.invalid/modules.php"'),
    FORM.replace('method="get"', 'method="post"'), FORM.replace(IMAGE, 'invalid'), FORM + FORM])
def test_bad_form_never_sent_to_model(env, bad):
    env.pending.append(('GET', response(bad)))
    p = pilot.Pilot()
    assert p.search(COURT) is None and p.value['status'] == 'court_form'
    assert p.value['model_calls'] == 0


@pytest.mark.parametrize('html,status', [(EMPTY, 'empty'), (FORM, 'court_unrecognized'),
    (EMPTY + FORM, 'court_unrecognized'), ('<a href="?case_id=7">menu</a>', 'court_unrecognized')])
def test_http_200_does_not_prove_search(env, html, status):
    attempt(env, html=html)
    p = pilot.Pilot()
    p.search(COURT)
    assert p.value['status'] == status
    assert p.value['accepted'] == (status == 'empty')


def test_missing_usage_keeps_success_but_cost_is_explicitly_incomplete(env):
    env.pending.extend([('GET', response(FORM)), ('POST', model(usage=None)), ('GET', response(RESULT))])
    p = pilot.Pilot()
    assert p.search(COURT) == RESULT
    assert not p.value['usage_complete']
    assert 'Стоимость неполная' in '\n'.join(captcha_audit.lines(p.value))


def test_deadline_before_start_and_after_model(env, monkeypatch):
    monkeypatch.setattr(pilot.netutil, 'run_deadline_remaining', lambda: 0)
    p = pilot.Pilot()
    assert p.search(COURT) is None and not env.calls
    assert p.value['status'] == 'deadline'
    monkeypatch.setattr(pilot.netutil, 'run_deadline_remaining', lambda: 0 if len(env.calls) >= 2 else 60)
    env.pending.extend([('GET', response(FORM)), ('POST', model())])
    p = pilot.Pilot()
    assert p.search(COURT) is None and p.value['submissions'] == 0
    assert p.value['model_calls'] == 1 and p.value['status'] == 'deadline'


def test_throttle_refusal_does_not_count_as_submission(env, monkeypatch):
    @contextmanager
    def slot(*args, **kwargs):
        yield not env.calls
    monkeypatch.setattr(pilot.host_throttle, 'request_slot', slot)
    env.pending.extend([('GET', response(FORM)), ('POST', model())])
    p = pilot.Pilot()
    assert p.search(COURT) is None and p.value['status'] == 'deadline'
    assert p.value['submissions'] == 0


def run_search_loop(p, *, retry=False, skip=False):
    # Выполняем настоящий изолированный блок main_json без картотеки/доставки.
    source = ast.parse(Path(inspect.getfile(runs)).read_text())
    main = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == 'main_json')
    loop = next(n for n in main.body if isinstance(n, ast.For) and ast.unparse(n.target) == '(_ap_i, _ap_court)')
    seen = []
    env = dict(vars(runs), APPEAL_COURTS=[COURT, COURT], captcha_pilot=p, retry_only=retry,
               appeal_search_gated_now=set(), health_obs={}, health_labels={}, health_captcha={},
               search_skip_keys={runs._appeal_health_key(COURT)} if skip else set(),
               csv_existing=[], csv_active_count=0, appeal_new_cases_csv=[], appeal_fi_numbers={},
               find_new_cases=lambda rows, old: seen.extend(rows) or [])
    exec(compile(ast.Module(body=[loop], type_ignores=[]), '<appeal loop>', 'exec'), env)
    return env, seen


def test_main_loop_passes_results_to_existing_intake_and_skips_auxiliary_search(env):
    attempt(env)
    p = pilot.Pilot()
    state, rows = run_search_loop(p)
    assert [x['Номер дела'] for x in rows] == ['33-7/2026']
    assert state['health_obs'][runs._appeal_health_key(COURT)] == 1
    assert COURT.domain in state['appeal_search_gated_now']
    assert len(env.calls) == 3  # Дубль суда в реестре не даёт ещё три попытки.
    assert report.load()['appeal_captcha']['status'] == 'accepted'


@pytest.mark.parametrize('retry,skip,status', [(True, False, 'skipped_retry_only'),
    (False, True, 'skipped_today')])
def test_daily_and_retry_gates_prevent_model_and_court_calls(env, retry, skip, status):
    p = pilot.Pilot()
    state, rows = run_search_loop(p, retry=retry, skip=skip)
    assert p.value['status'] == status and not env.calls and not rows
    assert not state['health_obs']


def test_disabled_main_search_remains_disabled(env, monkeypatch):
    monkeypatch.delenv('HMAO_APPEAL_CAPTCHA_ENABLED')
    state, rows = run_search_loop(None)
    assert not rows and not env.calls and not state['health_obs']


def test_main_search_failure_never_uses_stale_fetch_diag_or_closes_card_breaker(env, monkeypatch):
    env.pending.extend([('GET', response(FORM)), ('POST', response(code=429))])
    monkeypatch.setattr(config, 'FETCH_DIAG', {'kind': 'portal_placeholder'})
    def forbidden(*args, **kwargs):
        pytest.fail('Ошибка модели не является отказом карточек суда')
    monkeypatch.setattr(runs, 'card_breaker_note_failure', forbidden)
    state, rows = run_search_loop(pilot.Pilot())
    assert state['health_obs'][runs._appeal_health_key(COURT)] is None
    assert not rows and report.load()['appeal_captcha']['status'] == 'model_http'


def test_report_survives_replay_but_not_old_date_or_foreign_region(env, monkeypatch):
    attempt(env)
    p = pilot.Pilot()
    p.search(COURT)
    date = p.value['at'][:10]
    Path(config.PARSE_HEALTH_PATH).write_text(json.dumps({'last_run': {
        'at': p.value['at'], 'cards_read_today': 1, 'cards_planned_today': 1, 'appeal_captcha': p.value}}))
    snapshot = report.parser_snapshot(date)
    assert snapshot['appeal_captcha'] == p.value
    assert not any(x['domain'] == COURT.domain and x['delo_id'] == 5 for x in snapshot['manual_sources'])
    value = {'region': 'hmao', 'date': date, 'parser': snapshot}
    rendered = report.render(value)
    assert 'Попыток: 1/3' in rendered and 'сайт принял код' in rendered
    assert captcha_audit.MODEL in rendered and 'не выписка списаний' in rendered
    assert 'private-' not in rendered
    assert 'appeal_captcha' not in report.parser_snapshot('2000-01-01')
    assert 'Капча · апелляция' not in report.render(dict(value, date='2000-01-01'))
    monkeypatch.setattr(config, 'REGION', 'tyumen')
    assert 'appeal_captcha' not in report.parser_snapshot(date)
    assert 'Капча · апелляция' not in report.render(dict(value, region='tyumen'))


def test_error_report_distinguishes_api_and_requires_action(env):
    env.pending.extend([('GET', response(FORM)), ('POST', response(code=504))])
    p = pilot.Pilot()
    p.search(COURT)
    value = {'region': 'hmao', 'date': p.value['at'][:10], 'appeal_captcha': p.value,
             'parser': {'status': 'complete'}, 'digest': {'status': 'ready'},
             'publication': {'pages': 'confirmed', 'data': 'confirmed'}, 'push': {'status': 'skipped'}}
    rendered = report.render(value)
    assert rendered.startswith('⚠️')
    assert 'ошибка HTTP Cloud.ru' in rendered and 'HTTP 504' in rendered
    assert 'проверить доступ к Cloud.ru' in rendered and 'сайт отклонил код' not in rendered


def test_later_skip_retains_last_search_for_separate_delivery(env):
    attempt(env)
    original = pilot.Pilot()
    original.search(COURT)
    health = {'last_run': {}}
    original.save_snapshot(health)
    last = health['appeal_captcha_last_search']
    for status in ('skipped_today', 'skipped_retry_only'):
        later = pilot.Pilot(last)
        later.skip(status)
        later.save_snapshot(health)
        assert health['appeal_captcha_last_search'] == last
        assert health['last_run']['appeal_captcha']['model_calls'] == 0
        text = '\n'.join(captcha_audit.lines(later.value))
        assert 'Последний поиск сегодня' in text and 'попыток 1/3' in text
        assert 'сайт принял код' in text and captcha_audit.action(later.value) == ''
    assert len(env.calls) == 3
    yesterday = dict(last, at='2000-01-01T08:00:00+05:00')
    later = pilot.Pilot(yesterday)
    later.skip('skipped_retry_only')
    assert 'last_search' not in later.value


def test_last_failed_search_remains_actionable_in_retry_slot(env):
    env.pending.append(('GET', response(code=503)))
    original = pilot.Pilot()
    original.search(COURT)
    later = pilot.Pilot(original.value)
    later.skip('skipped_retry_only')
    assert captcha_audit.action(later.value)
