from copy import deepcopy
from datetime import date, datetime, timedelta
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
    monkeypatch.setattr(watch.telemetry, 'register_planned_case_ids', lambda *a: None)
    monkeypatch.setattr(watch.telemetry, 'mark_case_read', lambda *a: None)
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


def info(text=TEXT, **extra):
    return dict({'_table_count': 6, 'Номер дела (карточка)': '2-1/2026',
                 'УИД': 'court-uid', 'act_text': text,
                 '_events': [{'date': '01.10.2026', 'text': 'Вынесено решение по делу'}]}, **extra)


def run(monkeypatch, data, cases, card=None, day=DAY, **kwargs):
    monkeypatch.setattr(watch, 'parse_case_card', lambda *a: info() if card is None else card)
    kwargs.setdefault('persist', lambda d: None)
    kwargs.setdefault('now', datetime.combine(day, datetime.min.time()) + timedelta(hours=6))
    return watch.refresh(data, cases, day, lambda *a, **k: 'html', **kwargs)


@pytest.mark.parametrize('location', ['active', 'history', 'detached', 'bank'])
def test_initial_legacy_text_is_silent_everywhere(monkeypatch, location):
    c = case(track='plaintiff_light') if location == 'bank' else case()
    data = {}
    watch.sync(data, [c], DAY)
    if location == 'history':
        cases = [dict(c, first_instance={'case_number': '2-2/2026'}, history=[deepcopy(c)])]
    else:
        cases = [] if location == 'detached' else [c]
    report = run(monkeypatch, data, cases)
    assert report['backfilled'] == report['read'] == 1
    assert report['published'] == 0 and not data.get(watch.PENDING)
    task = next(iter(data[watch.FIELD].values()))
    assert task['legacy_absent_checked_at'] == '2026-10-05'
    assert task['block']['act_notification_kind'] == 'backfill'
    assert task['status'] == 'complete'
    assert c['current_stage'] == 'appeal'


def test_fresh_absence_then_text_emits_once_after_stage_change(monkeypatch):
    c = case(); data = {}
    assert run(monkeypatch, data, [c], info(''))['published'] == 0
    assert c['first_instance'][watch.BASELINE_FIELD] == watch.BASELINE_VERSION
    assert c['first_instance']['act_absent_checked_at'] == DAY.isoformat()
    c['current_stage'] = 'cassation'
    report = run(monkeypatch, data, [c], day=DAY + timedelta(days=1))
    assert report['published'] == report['read'] == 1
    change = data[watch.PENDING][0]
    assert change['type'] == ['fi_act_text_published']
    assert change['details']['act_detected_at'] == '2026-10-08'
    assert change['plaintiff'] == 'Истец прежнего круга'
    assert run(monkeypatch, data, [c], day=DAY + timedelta(days=1))['planned'] == 0
    assert len(data[watch.PENDING]) == 1 and c['current_stage'] == 'cassation'


def test_main_parser_observe_resets_stale_absence_but_accepts_new_observation():
    b = case()['first_instance']
    assert not watch.observe(b, TEXT, DAY, present=True, confirmed_date='01.10.2026')
    assert b['act_notification_kind'] == 'backfill'
    b = case()['first_instance']
    assert not watch.observe(b, '', DAY, present=False)
    assert watch.observe(b, TEXT, DAY + timedelta(days=1), present=True)


def test_regular_read_is_consumed_without_second_request(monkeypatch):
    c = case(); data = {}
    watch.sync(data, [c], DAY)
    watch.observe(c['first_instance'], '', DAY, present=False)
    c['first_instance']['last_checked_at'] = DAY.isoformat()
    r = watch.refresh(data, [c], DAY, lambda *a, **k: pytest.fail('duplicate card request'), persist=lambda d: None)
    assert r['read'] == r['planned'] == 0
    assert r['items'][0]['reason'] == 'scheduled'
    assert next(iter(data[watch.FIELD].values()))['block']['act_absent_checked_at'] == DAY.isoformat()


def test_act_only_read_never_closes_normal_fi_check(monkeypatch):
    c = case(current_stage='first_instance'); data = {}
    b = c['first_instance']
    checked_before = b['last_checked_at']
    assert run(monkeypatch, data, [c], info(''))['read'] == 1
    assert b['last_checked_at'] == checked_before
    assert b[watch.ACT_CHECKED_FIELD] == DAY.isoformat()
    task = next(iter(data[watch.FIELD].values()))
    assert task['block']['last_checked_at'] == checked_before
    assert run(monkeypatch, data, [c], info(''))['read'] == 0
    # Последующее полное чтение на следующий день закрывает актовую очередь
    # этого дня даже без вызова observe (обратная совместимость продюсеров).
    b['last_checked_at'] = (DAY + timedelta(days=1)).isoformat()
    r = watch.refresh(data, [c], DAY + timedelta(days=1),
                      lambda *a, **k: pytest.fail('normal FI read already covered act'),
                      persist=lambda d: None)
    assert r['planned'] == r['read'] == 0


def test_six_act_reads_leave_archive_weekly_check_due_on_seventh_day(monkeypatch):
    from court_monitor.lifecycle import should_skip_case
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    monkeypatch.setattr(config, 'SKIP_CHECKED_TODAY', True)
    start = date(2026, 10, 1)
    c = case(current_stage='first_instance', archived_at='2026-09-30', archived=True)
    b = c['first_instance']
    b.update(decision_date='01.08.2026', last_checked_at=start.isoformat(),
             events=[{'date': '01.08.2026', 'text': 'Вынесено решение по делу'}],
             writ_number='kept-writ', bank_role='Ответчик')
    data = {}
    for offset in range(1, 7):
        day = start + timedelta(days=offset)
        card = info('', _events=[{'date': '01.08.2026', 'text': 'Вынесено решение по делу'}])
        # Даже принудительная ежедневная дочитка старого акта, включая
        # выходные, не может переносить отдельный недельный цикл жалоб.
        assert run(monkeypatch, data, [c], card, day=day, force=True)['read'] == 1
        assert b['last_checked_at'] == start.isoformat()
        assert b[watch.ACT_CHECKED_FIELD] == day.isoformat()
        skipped, reason = should_skip_case(c, day)
        assert skipped and reason.startswith('archive_weekly')
    assert should_skip_case(c, start + timedelta(days=7)) == (False, '')
    assert b['events'] == [{'date': '01.08.2026', 'text': 'Вынесено решение по делу'}]
    assert b['writ_number'] == 'kept-writ' and b['bank_role'] == 'Ответчик'
    assert c['archived'] and c['archived_at'] == '2026-09-30'


def test_incomplete_source_reopens_complete_task_silently(monkeypatch):
    c = case(); data = {}
    partial = 'РЕШЕНИЕ 1 октября 2026 года. ' + 'Доводы истца. ' * 40
    assert run(monkeypatch, data, [c], info(partial))['backfilled'] == 1
    c['first_instance']['act_summary_needs_source'] = True
    report = run(monkeypatch, data, [c], day=DAY + timedelta(days=1))
    assert report['backfilled'] == 1 and report['published'] == 0
    assert c['first_instance']['act_text'] == TEXT
    assert c['first_instance']['act_summary_needs_source'] is False
    assert not data.get(watch.PENDING)


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


def test_fresh_baseline_is_imported_even_with_same_day_stamp(monkeypatch):
    c = case(); data = {}; b = c['first_instance']
    b['last_checked_at'] = DAY.isoformat()
    watch.sync(data, [c], DAY)
    watch.observe(b, '', DAY, present=False)
    watch.sync(data, [c], DAY)
    assert next(iter(data[watch.FIELD].values()))['block'][watch.BASELINE_FIELD] == watch.BASELINE_VERSION
    assert run(monkeypatch, data, [c], day=DAY + timedelta(days=1))['published'] == 1


@pytest.mark.parametrize('extra,reason', [
    ({'link': ''}, 'missing_card_link'),
    ({'decision_date': ''}, 'missing_decision_date'),
    ({'court_domain': ''}, 'missing_court_domain'),
    ({'case_number': ''}, 'missing_case_number'),
])
def test_missing_metadata_is_visible_without_requests(monkeypatch, extra, reason):
    c = case(); c['first_instance'].update(extra)
    r = watch.refresh({}, [c], DAY, lambda *a, **k: pytest.fail('invalid task fetched'), persist=lambda d: None)
    assert r['unplanned'] == 1 and r['read'] == 0
    assert r['items'][0]['reason'] == reason


def test_missing_date_can_be_completed_without_duplicate_task(monkeypatch):
    c = case(); c['first_instance']['decision_date'] = ''; data = {}
    watch.sync(data, [c], DAY)
    c['first_instance']['decision_date'] = '01.10.2026'
    assert run(monkeypatch, data, [c])['backfilled'] == 1
    assert len(data[watch.FIELD]) == 1


@pytest.mark.parametrize('card,reason', [
    ({'_table_count': 0}, 'unread_card'),
    (info(**{'УИД': 'other'}), 'identity_mismatch'),
    (info(**{'Номер дела (карточка)': '2-9/2026'}), 'identity_mismatch'),
    (info(TEXT.replace('1 октября', '2 октября')), 'act_identity_mismatch'),
    (info('Текст без даты ' * 40, _events=[]), 'missing_act_decision_date'),
])
def test_unverified_card_or_other_round_does_not_replace_source(monkeypatch, card, reason):
    c = case(); data = {}; r = run(monkeypatch, data, [c], card)
    assert r['read'] == 0 and r['items'][0]['reason'] == reason
    assert c['first_instance']['last_checked_at'] == '2026-10-06'
    assert not data.get(watch.PENDING) and not c['first_instance'].get('act_text')


def test_external_act_failure_does_not_confirm_absence(monkeypatch):
    c = case(); data = {}
    r = run(monkeypatch, data, [c], info('', _act_url='https://' + DOMAIN + '/act'),
            fetch_text=lambda *a, **k: '')
    assert r['items'][0]['reason'] == 'act_text_unread'
    b = next(iter(data[watch.FIELD].values()))['block']
    assert 'act_absent_checked_at' not in b and 'act_text' not in b
    r = run(monkeypatch, data, [c], info('', _act_url='https://other.example/act'),
            fetch_text=lambda *a, **k: pytest.fail('other court fetched'),
            now=datetime(2026, 10, 7, 6, 31))
    assert r['items'][0]['reason'] == 'identity_mismatch'


def test_document_date_is_not_replaced_by_later_card_movement(monkeypatch):
    c = case(); data = {}
    r = run(monkeypatch, data, [c], info(_events=[{'date': '02.10.2026', 'text': 'Вынесено решение по делу'}]))
    assert r['backfilled'] == 1
    assert c['first_instance']['act_decision_date'] == '01.10.2026'


def test_court_platform_and_round_identity_are_preserved(monkeypatch):
    c = case(); c['first_instance']['srv_num'] = 2
    assert parse_qs(urlparse(watch.card_url(c['first_instance'])).query)['srv_num'] == ['2']
    other = deepcopy(c); other['first_instance']['court_domain'] = 'hmray--hmao.sudrf.ru'
    other['first_instance']['srv_num'] = 1
    earlier = deepcopy(c); earlier['first_instance']['decision_date'] = '01.09.2026'
    data = {}; watch.sync(data, [c, other, earlier], DAY)
    assert len(data[watch.FIELD]) == 3


def test_shared_daily_quota_and_specific_budget_reasons(monkeypatch):
    data = {'act_watch_budget': {'date': DAY.isoformat(), 'backfill_count': 9, 'backfill_seconds': 0}}
    a = case(); b = deepcopy(a); b['first_instance']['court_domain'] = 'hmray--hmao.sudrf.ru'
    report = run(monkeypatch, data, [a, b])
    assert report['read'] == 1 and data['act_watch_budget']['backfill_count'] == 10
    assert any(i.get('reason') == 'backfill_daily_card_limit' for i in report['items'])
    assert not data.get(watch.PENDING)


def test_expired_historical_first_instance_is_not_reactivated(monkeypatch):
    c = case(archived=True); c['first_instance']['decision_date'] = '01.01.2026'
    r = watch.refresh({}, [c], DAY, lambda *a, **k: pytest.fail('expired archive fetched'), persist=lambda d: None)
    assert r['expired'] == 1 and r['items'][0]['reason'] == 'age_limit'
    assert c['archived'] and c['current_stage'] == 'appeal'


def test_checkpoint_recovery_and_context_ack_preserve_other_data(monkeypatch):
    c = case(); data = {'cases': [c], 'keep': {'other': True}}
    watch.save_json(data, config.JSON_PATH)
    run(monkeypatch, data, [c], info(''))
    run(monkeypatch, data, [c], day=DAY + timedelta(days=1))
    watch.checkpoint(data)
    saved = watch.load_json(config.JSON_PATH)
    watch.sync(saved, saved['cases'], DAY + timedelta(days=1))
    assert saved['cases'][0]['first_instance']['act_text'] == TEXT
    assert saved['keep'] == {'other': True}
    changes = watch.merge_changes(saved, [])
    with pytest.raises(RuntimeError): watch.acknowledge(saved, changes, 'issue')
    watch.save_json({'issue_key': 'issue', 'fi_changes': []}, config.LAST_DIGEST_CONTEXT_PATH)
    with pytest.raises(RuntimeError): watch.acknowledge(saved, [], 'issue')
    watch.save_json({'issue_key': 'issue', 'fi_changes': changes}, config.LAST_DIGEST_CONTEXT_PATH)
    watch.acknowledge(saved, changes, 'issue')
    assert watch.PENDING not in watch.load_json(config.JSON_PATH)


def test_pending_write_failure_is_not_acknowledged(monkeypatch):
    c = case(); data = {}; saved = {}
    run(monkeypatch, data, [c], info(''))
    def persist(d):
        if d.get(watch.PENDING): raise OSError('disk full')
        saved.update(deepcopy(d))
    with pytest.raises(OSError):
        run(monkeypatch, data, [c], day=DAY + timedelta(days=1), persist=persist)
    recovered = deepcopy(saved)
    r = run(monkeypatch, recovered, [], day=DAY + timedelta(days=1), now=datetime(2026, 10, 8, 6, 31))
    assert r['published'] == 1 and len(recovered[watch.PENDING]) == 1


def test_bank_archive_preserves_split_events(monkeypatch):
    from court_monitor.storage import load_bank_json, save_bank_json
    monkeypatch.setattr(config, 'BANK_TRACK', True)
    c = case(track='plaintiff_light', archived=True)
    c['first_instance']['events'] = [{'date': '01.10.2026', 'text': 'Вынесено решение по делу'}]
    save_bank_json({'cases': [c]}, config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
    data = {}; run(monkeypatch, data, [deepcopy(c)])
    watch.persist_archives(data, DAY)
    saved = load_bank_json(config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH)['cases'][0]
    assert saved['first_instance']['events'] == c['first_instance']['events']
    assert saved['first_instance']['act_text'] == TEXT and saved['archived']
    assert saved['first_instance']['last_checked_at'] == c['first_instance']['last_checked_at']
    assert saved['first_instance'][watch.ACT_CHECKED_FIELD] == DAY.isoformat()
    assert saved['bank_role'] == c['bank_role'] and saved['track'] == c['track']
    assert not data.get(watch.PENDING)


def test_reserve_stays_inside_shared_limit_and_has_distinct_reasons():
    clock = [0]; data = {}; budget = policy.Budget(data, DAY, seconds=policy.RESERVED_SECONDS, clock=lambda: clock[0])
    assert budget.remaining('waiting') == 90
    clock[0] = 90
    assert budget.reason('waiting') == 'watch_time_limit'
    assert budget.reason('waiting', run_remaining=0) == 'run_deadline'
    assert policy.Budget({}, DAY, seconds=900).seconds == 600
    budget = policy.Budget({}, DAY, clock=lambda: 0)
    budget.state['backfill_seconds'] = 180
    assert budget.reason('backfill') == 'backfill_daily_time_limit'
    assert budget.reason('waiting') == ''
