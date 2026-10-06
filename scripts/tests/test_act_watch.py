"""Поздние акты: история, архив, расписание, повторы и аварийное сохранение."""
from copy import deepcopy
from datetime import date, datetime, timedelta
import json

import pytest
from court_monitor import act_watch, config
from court_monitor.digest.template import generate_template_digest

TODAY = date(2026, 10, 5)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'JSON_PATH', str(tmp_path / 'cases.json'))
    monkeypatch.setattr(config, 'CASSATION_ACTS_PATH', str(tmp_path / 'acts'))
    monkeypatch.setattr(config, 'JSON_ARCHIVE_PATH', str(tmp_path / 'archive.json'))
    monkeypatch.setattr(config, 'BANK_TRACK', False)
    monkeypatch.setattr(config, 'REGION', 'hmao')


def case():
    return {'id': '2-716/2025', 'plaintiff': 'Тестовый истец', 'defendant': 'ПАО Сбербанк',
            'bank_role': 'Ответчик', 'current_stage': 'awaiting_relink',
            'first_instance': {'case_number': '2-716/2025', 'court': 'Сургутский городской суд'},
            'cassation': {'case_number': '8Г-12188/2026', 'court_domain': '7kas.sudrf.ru',
                         'link': '12120551|228a9fc7-0e96-4596-9ae4-99bc86b65684',
                         'judicial_uid': '86-test', 'decision_date': '08.09.2026',
                         'act_absent_checked_at': '2026-09-09', 'last_checked_at': '2026-09-09', 'outcome': 'cassation_remanded',
                         'act_published': False}}


def html(published=True, uid='86-test', number='8Г-12188/2026'):
    return ('<body>ДЕЛО № ' + number + '<table><tr><th>ДЕЛО</th></tr>'
            '<tr><td>Уникальный идентификатор дела</td><td>' + uid + '</td></tr>'
            '<tr><td>Дата рассмотрения</td><td>08.09.2026</td></tr></table>'
            '<table><tr><th>РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ</th></tr>'
            '<tr><td>Номер дела в первой инстанции</td><td>2-716/2025</td></tr></table>'
            + ('<div id="cont_doc1">Дело № 88-13066/2026 ' + 'Мотивировка. ' * 40 + '</div>' if published else '')
            + '</body>')


def run(data, cases, response=None, persist=lambda d: None, **kwargs):
    kwargs.setdefault('now', datetime.combine(TODAY, datetime.min.time())+timedelta(hours=6))
    return act_watch.refresh(data, cases, TODAY, lambda *a, **k: html() if response is None else response, persist, **kwargs)


@pytest.mark.parametrize('location', ['active', 'history', 'detached'])
def test_publication_survives_stage_and_location_changes(location):
    old = case()
    data = {}
    act_watch.sync(data, [old], TODAY)
    if location == 'history':
        parent = dict(old, current_stage='first_instance', cassation=None, history=[deepcopy(old)])
        cases = [parent]
    elif location == 'detached':
        cases = []
    else:
        cases = [old]
    report = run(data, cases)
    assert report['published'] == report['read'] == 1
    change = data['pending_cassation_changes'][0]
    assert change['type'] == ['new_act']
    assert change['publication_parent']['plaintiff'] == old['plaintiff']
    if location == 'history':
        assert parent['current_stage'] == 'first_instance' and parent['cassation'] is None
        assert parent['history'][0]['cassation']['act_published']
    assert run(data, cases)['planned'] == 0
    assert len(data['pending_cassation_changes']) == 1


@pytest.mark.parametrize('response,reason', [('', 'unread_card'), ('<html>captcha</html>', 'unread_card'),
    (html(uid='other'), 'identity_mismatch'), (html(number='8Г-999/2026'), 'identity_mismatch')])
def test_failures_remain_due(response, reason):
    c = case(); data = {}
    report = run(data, [c], response)
    assert report['unread'] == 1 and report['items'][0]['reason'] == reason
    assert c['cassation']['last_checked_at'] == '2026-09-09'
    assert run(data, [c])['published'] == 0
    if reason == 'identity_mismatch':
        assert next(iter(data[act_watch.FIELD].values()))['status'] == 'needs_review'
    else:
        assert run(data, [c], now=datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=6,minutes=31))['published'] == 1


def test_success_without_text_is_not_repeated_today():
    c = case(); data = {}
    assert run(data, [c], html(False))['read'] == 1
    assert run(data, [c], html(False))['planned'] == 0
    assert not data.get('pending_cassation_changes')


@pytest.mark.parametrize('decision,last,next_day', [
    ('08.09.2026', '2026-10-02', '2026-10-05'),
    ('01.08.2026', '2026-10-02', '2026-10-05'),
    ('01.06.2026', '2026-10-02', '2026-11-23'),
])
def test_cadence(decision, last, next_day):
    block = {'decision_date': decision, 'last_checked_at': last}
    assert act_watch.next_check(block, TODAY).isoformat() == next_day


def test_missing_link_is_visible_not_success():
    c = case(); c['cassation']['link'] = ''
    report = run({}, [c])
    assert report['due'] == report['unplanned'] == 1
    assert report['planned'] == report['read'] == 0


def test_checkpoint_survives_archive_write_failure(tmp_path):
    c = case(); data = {'cases': [c]}
    act_watch.save_json(data, config.JSON_PATH)
    run(data, [c], persist=act_watch.checkpoint)
    saved = act_watch.load_json(config.JSON_PATH)
    assert saved['pending_cassation_changes'][0]['type'] == ['new_act']
    assert not saved['cases'][0]['cassation']['act_published']  # основной прогон ещё не сохранил блок
    act_watch.sync(saved, saved['cases'], TODAY)
    assert saved['cases'][0]['cassation']['act_published']
    assert run(saved, saved['cases'])['planned'] == 0


def test_failed_checkpoint_does_not_consume_publication():
    c = case(); data = {}; saved = {}
    def save(d):
        if d.get('pending_cassation_changes'):
            raise OSError('disk full')
        saved.update(deepcopy(d))
    with pytest.raises(OSError):
        run(data, [c], persist=save)
    assert run(saved, [case()], now=datetime.combine(TODAY, datetime.min.time())+timedelta(hours=6,minutes=31))['published'] == 1


def test_normal_parser_publication_is_durable_even_if_old_dedup_was_written(tmp_path):
    c = case(); data = {}
    act_watch.sync(data, [c], TODAY)
    c['cassation'].update(act_notification_kind='new_publication', act_published=True, act_text='Мотивировка. ' * 50)
    (tmp_path / 'acts').write_text('8Г-12188/2026|08.09.2026\n')
    act_watch.sync(data, [c], TODAY)
    assert len(data['pending_cassation_changes']) == 1
    act_watch.sync(data, [c], TODAY)
    assert len(data['pending_cassation_changes']) == 1


def test_previously_announced_act_is_not_announced_again(tmp_path):
    (tmp_path / 'acts').write_text('8Г-12188/2026|08.09.2026\n')
    data = {}; run(data, [case()])
    assert not data.get('pending_cassation_changes')


def test_history_event_digest_uses_old_parties(tmp_path):
    data = {}; run(data, [case()])
    act_watch.save_json({'cases': []}, config.JSON_PATH)
    rendered = generate_template_digest([], [], cases=[], cass_changes=data['pending_cassation_changes'])
    assert '8Г-12188/2026' in rendered
    assert 'Тестовый истец' in rendered


def test_court_collision_never_overwrites_task():
    a = case(); b = deepcopy(a)
    b['cassation']['link'] = '99|other'
    data = {}; act_watch.sync(data, [a, b], TODAY)
    report = run(data, [a, b])
    assert report['unplanned'] == 1 and report['read'] == 0
    assert not data.get('pending_cassation_changes')


def test_completed_watch_refetches_incomplete_source_without_announcement():
    c = case(); data = {}
    run(data, [c])
    data['pending_cassation_changes'] = []
    c['cassation']['act_summary_needs_source'] = True
    full = html().replace('</div>', ' Определила: решение суда оставить без изменения, жалобу без удовлетворения.</div>')
    report = run(data, [c], full, force=True)
    assert report['read'] == 1 and report['published'] == 0
    assert not c['cassation']['act_summary_needs_source']
    assert c['cassation']['act_received_at'] and c['cassation']['act_source_url']
    assert not data['pending_cassation_changes']
    assert run(data, [c], full, force=True)['planned'] == 0
