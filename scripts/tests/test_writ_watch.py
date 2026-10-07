from copy import deepcopy
from datetime import date, timedelta

import pytest

from court_monitor import config, lifecycle, writ_watch as watch, act_watch_policy

TODAY = date(2026, 10, 7)


def case(**fi):
    return {'id': '2-10/2026', 'bank_role': 'Истец', 'track_origin': 'plaintiff_light',
            'current_stage': 'appeal', 'first_instance': {
                'case_number': '2-10/2026', 'court_domain': 'surggor--hmao.sudrf.ru',
                'link': '123|11111111-1111-1111-1111-111111111111',
                'status': 'Решено', 'result': 'Иск удовлетворён',
                'decision_date': '01.09.2026', 'hearing_date': '01.09.2026', **fi}}


@pytest.mark.parametrize('role', ['Третье лицо', 'Ответчик'])
def test_only_bank_plaintiff_needs_writ(role):
    c = case()
    c['bank_role'] = role
    assert watch.state(c, TODAY) == ('closed', 'bank_not_plaintiff')
    assert not watch.sync({}, [c], TODAY)


def test_refusal_closes_but_partial_award_remains():
    assert watch.state(case(result='В удовлетворении иска отказано'), TODAY) == ('closed', 'writ_not_required')
    assert watch.state(case(result='Иск удовлетворён частично, в остальной части отказано'), TODAY)[0] == 'waiting'


@pytest.mark.parametrize('outcome', ['cassation_remanded', 'cassation_reversed', 'cassation_modified'])
def test_changed_cassation_basis_not_old_fi_estimate(outcome):
    c = case(result='В иске отказано', legal_force_est='2026-10-01', writ_awaited_since='2026-10-01')
    c.update(current_stage='cassation', cassation={'outcome': outcome})
    tasks = watch.sync({}, [c], TODAY)
    assert next(iter(tasks.values()))['reason'] == 'basis_changed'
    assert 'legal_force_est' not in c['first_instance']
    assert 'writ_awaited_since' not in c['first_instance']


def test_upheld_appeal_keeps_waiting():
    c = case()
    c.update(current_stage='cassation_watch', appeal={'result': 'Решение оставлено без изменения'})
    assert watch.state(c, TODAY) == ('waiting', 'awaiting_writ')


def test_recalled_writ_does_not_finish_wait():
    c = case(writs=[{'issue_date': '10.09.2026', 'status': 'Отозван'}])
    assert watch.state(c, TODAY) == ('needs_review', 'writ_recalled_or_returned')


def test_fresh_normal_read_avoids_another_request():
    c = case(last_checked_at=TODAY.isoformat(), writ_checked_at=TODAY.isoformat())
    data = {}
    report = watch.refresh(data, [c], TODAY, lambda *a, **k: pytest.fail('duplicate request'),
                           budget=act_watch_policy.Budget(data, TODAY), persist=lambda _: None)
    assert report['planned'] == 0


@pytest.mark.parametrize('role,result,reason', [
    ('Третье лицо', 'Иск удовлетворён', 'bank_not_plaintiff'),
    ('Истец', 'В иске отказано', 'writ_not_required'),
])
def test_observed_role_result_survive_archive_sync(monkeypatch, role, result, reason):
    c = case()
    disk_copy = deepcopy(c)
    data = {}
    monkeypatch.setattr(watch, 'parse_case_card', lambda *_: {
        'Номер дела (карточка)': '2-10/2026', 'Результат': result,
        'participants': ['participant'], 'bank_role_from_participants': role,
        '_events': [{'date': '01.09.2026', 'text': 'решено'}], '_writs': []})
    monkeypatch.setattr(watch, 'card_is_empty_shell', lambda _: False)
    report = watch.refresh(data, [c], TODAY, lambda *a, **k: '<html>',
                           budget=act_watch_policy.Budget(data, TODAY), persist=lambda _: None)
    assert report['read'] == 1
    watch.sync(data, [disk_copy], TODAY)
    assert next(iter(data[watch.FIELD].values()))['reason'] == reason
    assert disk_copy['bank_role'] == role
    assert disk_copy['first_instance']['result'] == result


def test_first_historical_writ_load_silent_then_status_change():
    c = case()
    initial = {'_writs': [{'issue_date': '10.09.2026', 'status': 'Выдан', 'electronic_id': '1'}]}
    assert watch.observe(c, initial, TODAY, baseline=True) is None
    changed = deepcopy(initial)
    changed['_writs'][0]['status'] = 'Отозван'
    event = watch.observe(c, changed, TODAY + timedelta(days=7))
    assert event['type'] == ['fi_writ_status_changed']


def test_mismatched_card_does_not_write_writ(monkeypatch):
    c, data = case(), {}
    monkeypatch.setattr(watch, 'parse_case_card', lambda *_: {'Номер дела (карточка)': '2-999/2026'})
    monkeypatch.setattr(watch, 'card_is_empty_shell', lambda _: False)
    report = watch.refresh(data, [c], TODAY, lambda *a, **k: '<html>',
                           budget=act_watch_policy.Budget(data, TODAY), persist=lambda _: None)
    assert report['read'] == 0
    assert report['items'][0]['reason'] == 'identity_conflict'
    assert 'writ_checked_at' not in c['first_instance']


def test_unknown_court_site_is_reported_without_http():
    c, data = case(court_domain='other-region.sudrf.ru'), {}
    report = watch.refresh(data, [c], TODAY, lambda *a, **k: pytest.fail('foreign request'),
                           budget=act_watch_policy.Budget(data, TODAY), persist=lambda _: None)
    assert report['items'][0]['reason'] == 'unsupported_court_site'
    assert report['read'] == 0


def test_changed_date_stays_visible_in_archive_and_bank_split(tmp_path, monkeypatch):
    from court_monitor import runs
    from court_monitor.storage import load_json, save_json
    c = case(legal_force_est='2026-10-02', writ_awaited_since='2026-10-02')
    c.update(current_stage='first_instance', track='plaintiff_light')
    path = tmp_path / 'archive.json'
    monkeypatch.setattr(config, 'JSON_ARCHIVE_PATH', str(path))
    monkeypatch.setattr(config, 'BANK_TRACK', False)
    save_json({'cases': [deepcopy(c)]}, str(path))
    data = {}
    watch.sync(data, [c], TODAY)
    task = data[watch.FIELD][watch.identity(c['first_instance'])]
    task.update(status='needs_review', reason='decision_date_changed')
    watch.persist_archives(data, TODAY)
    restored = load_json(str(path))['cases'][0]
    assert restored['first_instance']['writ_watch_status'] == 'needs_review'
    assert 'legal_force_est' not in restored['first_instance']
    assert 'writ_awaited_since' not in restored['first_instance']
    monkeypatch.setattr(config, 'BANK_TRACK', True)
    runs.split_bank_track([restored])
    assert 'legal_force_est' not in restored['first_instance']
    assert 'writ_awaited_since' not in restored['first_instance']
    changes = []
    assert runs.collect_bank_calendar_events([restored], changes, TODAY) == 0
    assert changes == []


def test_new_full_card_role_wins_over_older_writ_observation():
    c = case(writ_checked_at='2026-10-06', writ_observed_bank_role='Истец')
    data = {}
    watch.sync(data, [c], TODAY)
    c['bank_role'] = 'Третье лицо'
    c['first_instance'].update(writ_checked_at='2026-10-01', last_checked_at=TODAY.isoformat())
    watch.sync(data, [c], TODAY)
    assert c['bank_role'] == 'Третье лицо'
    task = data[watch.FIELD][watch.identity(c['first_instance'])]
    assert task['status'] == 'closed'
    assert task['reason'] == 'bank_not_plaintiff'


@pytest.mark.parametrize('mode', ['archive', 'completed'])
def test_background_weekly_and_current_complaint_priority(monkeypatch, mode):
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    monkeypatch.setattr(config, 'SKIP_CHECKED_TODAY', True)
    c = case(last_checked_at='2026-10-05', decision_date='01.07.2026', hearing_date='01.07.2026')
    c.update(current_stage='first_instance', bank_role='Ответчик')
    c.pop('track_origin')
    if mode == 'archive':
        c['archived_at'] = '2026-09-01'
    else:
        c['first_instance'].update(status='В производстве', result='Заявление оставлено без рассмотрения')
    skip, reason = lifecycle.should_skip_case(c, TODAY)
    assert skip and reason.startswith(mode + '_weekly')
    c['first_instance'].update(appeal_filed=True, appeal_filed_date='06.10.2026')
    assert not lifecycle.should_skip_case(c, TODAY)[0]


def test_future_hearing_policy_unchanged(monkeypatch):
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    c = case(status='В производстве', last_checked_at='2026-08-01',
             events=[{'date': '20.10.2026', 'text': 'Судебное заседание'}])
    c['current_stage'] = 'first_instance'
    assert lifecycle.should_skip_case(c, TODAY)[0]
