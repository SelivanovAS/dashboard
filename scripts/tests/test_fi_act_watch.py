from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from court_monitor import config, fi_act_watch as watch, act_watch_policy as policy, netutil

DAY = date(2026, 10, 7)
DOMAIN = 'surggor--hmao.sudrf.ru'
TEXT = ('РЕШЕНИЕ 1 октября 2026 года. ' + 'Суд исследовал доказательства по делу. ' * 20
        + ' Решил: в удовлетворении исковых требований отказать полностью.')


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    for field, name in [('JSON_PATH', 'cases.json'), ('JSON_ARCHIVE_PATH', 'archive.json'),
                        ('JSON_BANK_ARCHIVE_PATH', 'bank_archive.json'),
                        ('JSON_BANK_ARCHIVE_EVENTS_PATH', 'bank_events.json'),
                        ('LAST_DIGEST_CONTEXT_PATH', 'context.json')]:
        monkeypatch.setattr(config, field, str(tmp_path / name))
    monkeypatch.setattr(config, 'BANK_TRACK', False)
    monkeypatch.setattr(config, 'cold_archive_glob', lambda: str(tmp_path / 'cold_*.json'))
    monkeypatch.setattr(config, 'bank_cold_archive_glob', lambda: str(tmp_path / 'bank_cold_*.json'))
    monkeypatch.setattr(config, 'FETCH_DIAG', {})
    monkeypatch.setattr(netutil, '_RUN_DEADLINE_AT', 0)
    monkeypatch.setattr(watch, 'FIRST_INSTANCE_COURTS', [
        SimpleNamespace(domain=d, base_url='https://' + d, delo_id=1540005, srv_num=s, new=0)
        for d, s in [(DOMAIN, 1), (DOMAIN, 2), ('hmray--hmao.sudrf.ru', 1)]])


def case(**extra):
    return dict({'id': '2-1/2026', 'current_stage': 'appeal',
                 'plaintiff': 'Истец прежнего круга', 'defendant': 'ПАО Сбербанк',
                 'bank_role': 'Ответчик', 'first_instance': {
                     'case_number': '2-1/2026', 'court_domain': DOMAIN,
                     'link': '123|card-uid', 'judicial_uid': 'court-uid',
                     'status': 'Решено', 'decision_date': '01.10.2026',
                     'last_checked_at': '2026-10-06', 'result': 'В иске отказано',
                     'act_absent_checked_at': '2026-10-05'}}, **extra)


def test_main_parser_observe_resets_stale_absence_but_accepts_new_observation():
    b = case()['first_instance']
    assert not watch.observe(b, TEXT, DAY, present=True, confirmed_date='01.10.2026')
    assert b['act_notification_kind'] == 'backfill'
    b = case()['first_instance']
    assert not watch.observe(b, '', DAY, present=False)
    assert watch.observe(b, TEXT, DAY + timedelta(days=1), present=True)


def test_normal_parser_new_publication_is_durable(monkeypatch):
    c = case(); data = {}; b = c['first_instance']
    watch.observe(b, '', DAY, present=False)
    watch.sync(data, [c], DAY)
    watch.observe(b, TEXT, DAY + timedelta(days=1), present=True)
    watch.sync(data, [c], DAY + timedelta(days=1))
    assert len(data[watch.PENDING]) == 1
    change = deepcopy(data[watch.PENDING][0])
    change['details'].pop('fi_act_watch_key')
    assert len(watch.merge_changes(data, [change])) == 1


def test_court_platform_and_round_identity_are_preserved(monkeypatch):
    c = case(); c['first_instance']['srv_num'] = 2
    assert parse_qs(urlparse(watch.card_url(c['first_instance'])).query)['srv_num'] == ['2']
    other = deepcopy(c); other['first_instance']['court_domain'] = 'hmray--hmao.sudrf.ru'
    other['first_instance']['srv_num'] = 1
    earlier = deepcopy(c); earlier['first_instance']['decision_date'] = '01.09.2026'
    data = {}; watch.sync(data, [c, other, earlier], DAY)
    assert len(data[watch.FIELD]) == 3


def test_reserve_stays_inside_shared_limit_and_has_distinct_reasons():
    clock = [0]; data = {}; budget = policy.Budget(data, DAY, seconds=policy.RESERVED_SECONDS, clock=lambda: clock[0])
    assert budget.remaining('waiting') == 90
    clock[0] = 90
    assert budget.reason('waiting') == 'watch_time_limit'
    assert budget.reason('waiting', run_remaining=0) == 'run_deadline'
    assert policy.Budget({}, DAY, seconds=1500).seconds == 1200
    budget = policy.Budget({}, DAY, clock=lambda: 0)
    budget.state['backfill_seconds'] = 180
    assert budget.reason('backfill') == 'backfill_daily_time_limit'
    assert budget.reason('waiting') == ''


def test_legacy_act_stamps_do_not_postpone_full_archive_check(monkeypatch):
    """Сохранённые до объединения очереди не становятся полным чтением."""
    from court_monitor.lifecycle import should_skip_case
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    monkeypatch.setattr(config, 'SKIP_CHECKED_TODAY', True)
    start = date(2026,10,1)
    c = case(current_stage='first_instance', archived=True, archived_at='2026-09-30')
    b = c['first_instance']
    b.update(decision_date='01.08.2026', last_checked_at=start.isoformat(),
             events=[{'date':'01.08.2026','text':'Вынесено решение по делу'}], writ_number='kept-writ')
    data = {}; watch.sync(data,[c],start)
    for offset in range(1,7):
        day = start + timedelta(days=offset)
        task = data[watch.FIELD][watch.identity(b)]
        watch.observe(task['block'],'',day,present=False)
        watch.sync(data,[c],day)
        assert b['last_checked_at'] == start.isoformat()
        assert b[watch.ACT_CHECKED_FIELD] == day.isoformat()
        assert should_skip_case(c,day)[1].startswith('archive_weekly')
    assert should_skip_case(c,start+timedelta(days=7)) == (False,'')
    assert b['writ_number'] == 'kept-writ' and c['archived']


def test_fresh_baseline_is_imported_even_with_same_day_stamp():
    c = case(); b = c['first_instance']; data = {}
    b['last_checked_at'] = DAY.isoformat()
    watch.sync(data,[c],DAY)
    watch.observe(b,'',DAY,present=False)
    watch.sync(data,[c],DAY)
    task = data[watch.FIELD][watch.identity(b)]
    assert task['block'][watch.BASELINE_FIELD] == watch.BASELINE_VERSION
    assert task['block']['act_absent_checked_at'] == DAY.isoformat()
    assert watch.observe(b,TEXT,DAY+timedelta(days=1),present=True)
