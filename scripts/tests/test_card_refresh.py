"""Единая проверка реально обновляет всю карточку и не теряет данные/события."""
from copy import deepcopy
from datetime import date, datetime, timedelta
import json

import pytest
from court_monitor import (card_refresh as refresh, runs, config, netutil,
                           fi_act_watch, writ_watch, act_watch_policy as policy)

DAY = date(2026, 10, 12)
DOMAIN = 'surggor--hmao.sudrf.ru'
TEXT = 'РЕШЕНИЕ 01.10.2026 ' + 'Суд исследовал доказательства. ' * 30 + ' РЕШИЛ: Исковые требования удовлетворить в полном объёме.'


def case(num='2-10/2026', **extra):
    fi = dict(case_number=num, court_domain=DOMAIN, court='Сургутский городской суд',
              link='123|11111111-1111-1111-1111-111111111111', judicial_uid='86-test',
              status='Решено', result='Иск удовлетворён', decision_date='01.10.2026',
              hearing_date='01.10.2026', last_checked_at='2026-10-01', writ_checked_at='2026-10-01',
              act_absent_checked_at='2026-10-01', fi_act_watch_baseline_version=1,
              resolved_emitted=True, events=[{'date':'01.10.2026','text':'Вынесено решение по делу'}])
    fi.update(extra)
    return dict(id=num, plaintiff='ПАО Сбербанк', defendant='Ответчик', bank_role='Истец',
                track_origin='plaintiff_light', current_stage='appeal', first_instance=fi)


def card(**extra):
    value = {'_table_count': 6, 'Номер дела (карточка)': '2-10/2026', 'УИД': '86-test',
             'Статус': 'Решено', 'Результат': 'Иск удовлетворён',
             'Судья': 'Новый судья', 'Акт опубликован': 'Да', 'act_text': TEXT,
             '_events': [{'date':'01.10.2026','text':'Вынесено решение по делу'},
                         {'date':'12.10.2026','text':'Поступила апелляционная жалоба'}],
             '_fi_appeal_filed': True, '_fi_appeal_filed_date':'12.10.2026',
             '_writs':[{'issue_date':'12.10.2026','blank_number':'123','status':'Выдан'}]}
    value.update(extra)
    return value


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    for attr in ('JSON_PATH','JSON_ARCHIVE_PATH','JSON_BANK_ARCHIVE_PATH',
                 'JSON_BANK_ARCHIVE_EVENTS_PATH','LAST_DIGEST_CONTEXT_PATH',
                 'DIGESTED_ACTS_PATH','CASSATION_ACTS_PATH'):
        monkeypatch.setattr(config, attr, str(tmp_path / attr))
    monkeypatch.setattr(config,'BANK_TRACK',False)
    monkeypatch.setattr(config,'REGION','hmao')
    monkeypatch.setattr(config,'FETCH_DIAG',{})
    monkeypatch.setattr(netutil,'run_deadline_remaining',lambda:None)
    monkeypatch.setattr(refresh,'run_deadline_remaining',lambda:None)
    monkeypatch.setattr(refresh,'_warn_if_card_degraded',lambda *a,**k:'')
    monkeypatch.setattr(runs,'fetch_act_text',lambda *a,**k: pytest.fail('unexpected document fetch'))


def run(monkeypatch, data, cases, info=None, **kwargs):
    monkeypatch.setattr(refresh,'parse_case_card',lambda *a: card() if info is None else deepcopy(info))
    requests=[]
    def fetch(url, **kwargs):
        requests.append(url)
        return 'html'
    kwargs.setdefault('budget',policy.Budget(data,DAY))
    kwargs.setdefault('persist',lambda d:None)
    result,reports=refresh.refresh(data,cases,DAY,fetch,runs.apply_existing_card,
                                  now=datetime(2026,10,12,6),**kwargs)
    return result,reports,requests


def test_writ_and_text_are_one_full_card(monkeypatch):
    c=case();data={}
    result,reports,requests=run(monkeypatch,data,[c])
    assert len(requests)==result['planned']==result['read']==1
    assert set(result['items'][0]['reasons'])=={'writ','text_waiting'}
    fi=c['first_instance']
    assert fi['act_text']==TEXT and fi['writs'][0]['blank_number']=='123'
    assert fi['judge']=='Новый судья' and fi['appeal_filed']
    assert len(fi['events'])==2 and fi['last_checked_at']==DAY.isoformat()
    assert c['current_stage']=='appeal'
    assert reports[writ_watch.FIELD]['read']==reports[fi_act_watch.FIELD]['read']==1
    assert refresh.pending_context(data)['fi_changes']
    assert run(monkeypatch,data,[c])[2]==[]


def test_main_full_read_closes_all_reasons(monkeypatch):
    c=case();data={}
    # The very same processor is used by the ordinary FI loop.
    runs.apply_first_instance_card(c,card(),fi_act_watch._court(c['first_instance']),DAY)
    result,_,requests=run(monkeypatch,data,[c])
    assert requests==[] and result['planned']==0


def test_writ_first_and_backfill_is_unique_daily_budget(monkeypatch):
    text_only=case('2-11/2026',link='124|11111111-1111-1111-1111-111111111111',
                   act_absent_checked_at='',writs=[{'issue_date':'01.09.2026','status':'Выдан'}])
    text_only['bank_role']='Ответчик';text_only['track_origin']=''
    writ=case();data={}
    cards,_=refresh.plan(data,[text_only,writ],DAY)
    assert cards[0]['reasons']==['text_waiting','writ']
    assert cards[0]['kind']=='waiting' and cards[1]['kind']=='backfill'
    assert [c['priority'] for c in cards]==[0,2]
    assert refresh.card_key('first_instance','https://a/?b=2&a=1')==refresh.card_key('first_instance','https://a/?a=1&b=2')
    assert refresh.card_key('first_instance','https://a/?srv_num=1')!=refresh.card_key('first_instance','https://a/?srv_num=2')


def test_history_updates_own_episode_only(monkeypatch):
    old=case();old['bank_role']='Ответчик';old['track_origin']=''
    parent=case('2-20/2026',act_text=TEXT)
    parent.update(current_stage='first_instance',bank_role='Ответчик',track_origin='',history=[old])
    new_before=deepcopy(parent['first_instance'])
    result,_,_=run(monkeypatch,{},[parent])
    assert result['read']==1
    assert old['first_instance']['act_text']==TEXT
    assert parent['first_instance']==new_before
    assert parent['current_stage']=='first_instance'


def test_historical_first_text_is_not_publication(monkeypatch):
    c=case(act_absent_checked_at='',writ_checked_at='')
    data={};run(monkeypatch,data,[c])
    changes=refresh.pending_context(data)['fi_changes']
    assert all('fi_act_text_published' not in c['type'] and 'fi_writ_issued' not in c['type'] for c in changes)
    assert c['first_instance']['act_notification_kind']=='backfill'


@pytest.mark.parametrize('bad,reason',[
    ({'_table_count':0},'empty_shell'),
    (card(**{'УИД':'another'}),'identity_mismatch'),
    (card(**{'Номер дела (карточка)':'2-999/2026'}),'identity_mismatch'),
])
def test_failed_card_not_full_read(monkeypatch,bad,reason):
    c=case();data={}
    r,_,_=run(monkeypatch,data,[c],bad)
    assert r['read']==0 and r['unread']==1 and r['items'][0]['reason']==reason
    assert c['first_instance']['last_checked_at']=='2026-10-01'
    assert not data.get(refresh.JOURNAL)


def test_full_snapshot_survives_restart_and_archive(monkeypatch):
    original=case(); c=deepcopy(original);data={}
    run(monkeypatch,data,[c])
    disk={'cases':[original]}
    Path=__import__('pathlib').Path
    Path(config.JSON_ARCHIVE_PATH).write_text(json.dumps(disk))
    refresh.persist_archives(data)
    saved=json.loads(Path(config.JSON_ARCHIVE_PATH).read_text())['cases'][0]
    assert saved['first_instance']['judge']=='Новый судья'
    assert saved['first_instance']['appeal_filed']
    assert saved['first_instance']['act_text']==TEXT
    assert saved['current_stage']=='appeal'
    saved['first_instance']['last_checked_at']='2026-10-13'
    saved['first_instance']['judge']='Ещё новее'
    assert not refresh.restore(data,[saved])
    assert saved['first_instance']['judge']=='Ещё новее'


def test_budget_exhaustion_keeps_unread_denominator(monkeypatch):
    c=case(); data={}
    result, reports, requests=run(monkeypatch,data,[c],budget=policy.Budget(data,DAY,seconds=0))
    assert not requests and result['unread']==result['planned']==1
    assert result['items'][0]['reason']=='watch_time_limit'
    assert reports[writ_watch.FIELD]['unread']==1


def test_summary_retry_does_not_reopen_good_source(monkeypatch):
    c=case(act_text=TEXT,act_summary_needs_source=False,
           writs=[{'issue_date':'01.09.2026','status':'Выдан'}])
    c['bank_role']='Ответчик';c['track_origin']=''
    data={'summary_queue':{'item':{'status':'retry_pending'}}}
    assert run(monkeypatch,data,[c])[2]==[]


def test_appeal_full_update_in_cassation_stage(monkeypatch):
    c=case(act_text=TEXT);c.update(bank_role='Ответчик',track_origin='',current_stage='cassation')
    c['appeal']=dict(case_number='33-10/2026',court_domain='oblsud--hmao.sudrf.ru',
        link='345|11111111-1111-1111-1111-111111111111',judicial_uid='86-test',
        status='Решено',result='Оставлено без изменения',hearing_date='01.10.2026',
        act_absent_checked_at='2026-10-01',last_checked_at='2026-10-01')
    info=card(**{'Номер дела (карточка)':'33-10/2026','Судья-докладчик':'Докладчик',
                 'Последнее событие':'Передано в экспедицию','Дата события':'12.10.2026',
                 'Результат':'Оставлено без изменения'})
    data={};r,_,_=run(monkeypatch,data,[c],info)
    assert r['read']==1 and c['current_stage']=='cassation'
    assert c['appeal']['judge_reporter']=='Докладчик'
    assert c['appeal']['last_event']=='Передано в экспедицию'
    assert c['appeal']['act_text']==TEXT
    assert refresh.pending_context(data)['changes']


def test_cassation_full_update_in_past_round(monkeypatch):
    old=case(act_text=TEXT);old.update(bank_role='Ответчик',track_origin='',current_stage='awaiting_relink')
    old['cassation']=dict(case_number='8Г-10/2026',court_domain='7kas.sudrf.ru',
        link='456|11111111-1111-1111-1111-111111111111',judicial_uid='86-test',
        decision_date='01.10.2026',last_checked_at='2026-10-01',act_absent_checked_at='2026-10-01')
    parent=case(act_text=TEXT);parent.update(bank_role='Ответчик',track_origin='',history=[old])
    parent['first_instance']['case_number']='2-20/2026'
    info=dict(page_case_number='8Г-10/2026',judicial_uid='86-test',decision_date='01.10.2026',
        act_text=TEXT,act_published=True,judge='Судья кассации',
        hearings=[{'date':'12.10.2026','text':'Сдано в архив'}],
        result_text='Оставить без изменения',fi_case_number='2-10/2026')
    monkeypatch.setattr(refresh,'parse_cassation_card',lambda *a: deepcopy(info))
    data={};r,_,_=run(monkeypatch,data,[parent])
    assert r['read']==1
    assert old['cassation']['judge']=='Судья кассации'
    assert old['cassation']['events']==info['hearings']
    assert old['cassation']['act_text']==TEXT
    assert 'cassation' not in parent and parent['current_stage']=='appeal'


def test_same_number_other_court_is_distinct(monkeypatch):
    first=case();second=deepcopy(first)
    second['first_instance']['court_domain']='surgray--hmao.sudrf.ru'
    cards,_=refresh.plan({},[first,second],DAY)
    assert len(cards)==2 and cards[0]['url']!=cards[1]['url']


def test_historical_round_does_not_take_new_decision(monkeypatch):
    old=case();old.update(bank_role='Ответчик',track_origin='')
    parent=case(act_text=TEXT);parent.update(bank_role='Ответчик',track_origin='',history=[old])
    parent['first_instance']['case_number']='2-20/2026'
    data={};r,_,_=run(monkeypatch,data,[parent],card(_events=[{'date':'10.10.2026','text':'Вынесено решение по делу'}]))
    assert r['read']==0 and r['items'][0]['reason']=='production_changed'
    assert old['first_instance']['last_checked_at']=='2026-10-01'


def test_journal_not_acknowledged_without_saved_events(monkeypatch):
    c=case();data={};run(monkeypatch,data,[c])
    with pytest.raises(RuntimeError):refresh.acknowledge(data,{})
    assert data[refresh.JOURNAL]
    refresh.acknowledge(data,refresh.pending_context(data))
    assert refresh.JOURNAL not in data


def test_force_does_not_process_same_card_twice_within_run(monkeypatch):
    c=case();data={}
    with refresh.card_session():
        runs.apply_first_instance_card(c,card(act_text='',**{'Акт опубликован':''}),
                                       fi_act_watch._court(c['first_instance']),DAY)
        r,_,requests=run(monkeypatch,data,[c],force=True)
        assert not requests and r['planned']==0
    # Следующий отдельный force-прогон вправе проверить карточку заново.
    assert run(monkeypatch,data,[c],force=True)[2]


@pytest.mark.parametrize('text,reason', [(TEXT.replace('01.10.2026','02.10.2026'),'act_identity_mismatch'),
                                      ('Текст без даты. '*50,'missing_act_decision_date')])
def test_wrong_act_does_not_discard_other_card_updates(monkeypatch, text, reason):
    c=case(); data={}
    r,_,_=run(monkeypatch,data,[c],card(act_text=text, _events=[]))
    assert r['read']==1 and c['first_instance']['judge']=='Новый судья'
    assert c['first_instance']['appeal_filed']
    assert not c['first_instance'].get('act_text')
    task=next(iter(data[fi_act_watch.FIELD].values()))
    assert task['status']=='needs_review' and task['reason']==reason


def test_changed_writ_basis_is_not_treated_as_old_award(monkeypatch):
    c=case(); data={}
    r,_,_=run(monkeypatch,data,[c],card(_events=[{'date':'11.10.2026','text':'Вынесено решение по делу'}]))
    assert r['read']==1 and c['first_instance']['judge']=='Новый судья'
    task=next(iter(data[writ_watch.FIELD].values()))
    assert task['status']=='needs_review' and task['reason']=='decision_date_changed'
    assert all('fi_writ_issued' not in ch['type'] for ch in refresh.pending_context(data)['fi_changes'])


def test_complete_card_keeps_complaint_tracking_and_applicant(monkeypatch):
    c=case(); c['current_stage']='cassation_watch'
    data={}
    run(monkeypatch,data,[c],card(_fi_cassation_filed=True,_fi_cassation_filed_date='12.10.2026',
                                _fi_cassator_raw='ПАО Сбербанк'))
    assert c['first_instance']['cassation_filed']
    assert c['cassation']['appellant_is_bank']
    assert c['complaint_tracking']


def test_historical_backfill_does_not_announce_old_resolution(monkeypatch):
    c=case(act_absent_checked_at='',resolved_emitted=False,decision_date='01.08.2026',hearing_date='01.08.2026')
    c['bank_role']='Ответчик';c['track_origin']=''
    data={};run(monkeypatch,data,[c],card(_fi_appeal_filed=False,act_text=TEXT.replace('01.10.2026','01.08.2026'),
        _events=[{'date':'01.08.2026','text':'Вынесено решение по делу'}]),force=True)
    assert all('fi_resolved' not in ch['type'] and 'fi_act_text_published' not in ch['type']
               for ch in refresh.pending_context(data)['fi_changes'])


def test_restart_between_card_checkpoint_and_watch_update_does_not_double_event(monkeypatch):
    c=case();before=deepcopy(c);data={}
    fi_act_watch.sync(data,[c],DAY)
    old_tasks=deepcopy(data[fi_act_watch.FIELD])
    run(monkeypatch,data,[c])
    data[fi_act_watch.FIELD]=old_tasks
    data.pop(fi_act_watch.PENDING,None)
    assert refresh.restore(data,[before])
    fi_act_watch.sync(data,[before],DAY)
    assert not data.get(fi_act_watch.PENDING)
    assert len(refresh.pending_context(data)['fi_changes'])==1
    # То же окно прерывания, если полный блок уже успел сохраниться.
    data[fi_act_watch.FIELD]=deepcopy(old_tasks)
    refresh.restore(data,[before])
    fi_act_watch.sync(data,[before],DAY)
    assert not data.get(fi_act_watch.PENDING)


def test_foreign_region_journal_is_never_consumed():
    data={refresh.JOURNAL:[{'region':'tyumen','stage':'first_instance','changes':[]}]}
    with pytest.raises(RuntimeError,match='другой территории'):
        refresh.acknowledge(data,{})
    assert data[refresh.JOURNAL]


def test_acknowledge_accepts_enrichment_but_not_other_court(monkeypatch):
    c=case();data={};run(monkeypatch,data,[c])
    saved=refresh.pending_context(data)
    for event in saved['fi_changes']:
        event['details']['merged_into']='2-200/2026'
    wrong=deepcopy(saved)
    wrong['fi_changes'][0]['case']='2-999/2026'
    with pytest.raises(RuntimeError):refresh.acknowledge(data,wrong)
    assert data[refresh.JOURNAL]
    refresh.acknowledge(data,saved)
    assert refresh.JOURNAL not in data


def test_foreign_document_is_not_fetched_but_card_is_updated(monkeypatch):
    c=case();data={}
    r,_,_=run(monkeypatch,data,[c],card(act_text='',_act_url='https://another-court.example/document'))
    assert r['read']==1 and c['first_instance']['judge']=='Новый судья'
    assert not c['first_instance'].get('act_text')
    task=next(iter(data[fi_act_watch.FIELD].values()))
    assert task['status']=='needs_review' and task['reason']=='document_host_mismatch'


def test_writ_alone_does_not_expand_disabled_courts(monkeypatch):
    from types import SimpleNamespace
    c=case(act_text=TEXT)
    monkeypatch.setattr(fi_act_watch,'_court',lambda _:SimpleNamespace(enabled=False,delo_id=1540005))
    cards,reports=refresh.plan({},[c],DAY)
    assert cards==[]
    assert reports[writ_watch.FIELD]['items'][0]['reason']=='unsupported_court_site'


@pytest.fixture(params=['first_instance', 'appeal', 'cassation'])
def publication(request, monkeypatch):
    """Полный обработчик каждой инстанции; подменяется только ответ суда."""
    from types import SimpleNamespace
    from court_monitor import act_watch, appeal_act_watch
    stage = request.param
    module = {'first_instance': fi_act_watch, 'appeal': appeal_act_watch,
              'cassation': act_watch}[stage]
    c = case()
    c.update(bank_role='Ответчик', track_origin='', plaintiff='Истец прежнего круга',
             defendant='ПАО Сбербанк', current_stage='cassation_watch')
    info = card(_events=[{'date':'01.10.2026','text':'Вынесено решение по делу'}],
                _fi_appeal_filed=False, _writs=[])
    if stage != 'first_instance':
        c['first_instance']['act_text'] = TEXT
        number, domain = (('33-10/2026', 'oblsud--hmao.sudrf.ru') if stage == 'appeal'
                          else ('8Г-10/2026', '7kas.sudrf.ru'))
        c[stage] = dict(case_number=number, court_domain=domain, link='345|card-uid',
            judicial_uid='86-test', status='Решено', result='Оставлено без изменения',
            decision_date='01.10.2026', hearing_date='01.10.2026',
            last_checked_at='2026-10-01', act_absent_checked_at='2026-10-01',
            act_published=False)
        if stage == 'appeal':
            info = {'_table_count':6, 'Номер дела (карточка)':number, 'УИД':'86-test',
                    'Статус':'Решено', 'Результат':'Оставлено без изменения',
                    'Акт опубликован':'Да', 'act_text':TEXT, 'Судья-докладчик':'Новый судья'}
        else:
            info = dict(page_case_number=number, judicial_uid='86-test',
                decision_date='01.10.2026', act_text=TEXT, act_published=True,
                judge='Новый судья', result_text='Оставить без изменения',
                fi_case_number='2-10/2026')
    def execute(data, cases, *, parsed=None, html='html', **kwargs):
        parser = 'parse_cassation_card' if stage == 'cassation' else 'parse_case_card'
        monkeypatch.setattr(refresh, parser, lambda *a: deepcopy(info if parsed is None else parsed))
        requests = []
        def fetch(url, **kw):
            requests.append(url)
            return html
        kwargs.setdefault('budget', policy.Budget(data, DAY))
        kwargs.setdefault('persist', lambda d: None)
        kwargs.setdefault('now', datetime(2026, 10, 12, 6))
        result, reports = refresh.refresh(data, cases, DAY, fetch, runs.apply_existing_card, **kwargs)
        return result, reports[module.FIELD], requests
    context = refresh.CONTEXT[stage]
    return SimpleNamespace(stage=stage, module=module, case=c, info=info, run=execute,
        events=lambda data: refresh.pending_context(data)[context],
        event_type='fi_act_text_published' if stage == 'first_instance' else 'new_act')


@pytest.mark.parametrize('location', ['active', 'history', 'archive'])
def test_late_publication_uses_full_processor_and_survives_location(publication, location):
    p = publication
    old = p.case
    data = {}
    if location == 'history':
        parent = dict(old, current_stage='first_instance', history=[old], plaintiff='Новый истец')
        parent[p.stage] = None
        cases = [parent]
    else:
        cases = [old]
    if location == 'archive':
        from court_monitor.storage import save_json
        old.update(archived=True, events=[{'type':'existing'}])
        save_json({'cases':[deepcopy(old)]}, config.JSON_ARCHIVE_PATH)
    result, report, requests = p.run(data, cases)
    assert len(requests) == result['read'] == report['published'] == 1
    assert old[p.stage]['act_text'] == TEXT
    assert old[p.stage]['last_checked_at'] == DAY.isoformat()
    assert old['current_stage'] == 'cassation_watch'
    events = p.events(data)
    assert sum(p.event_type in event['type'] for event in events) == 1
    assert p.run(data, cases)[0]['planned'] == 0
    assert p.events(data) == events
    if location == 'history':
        assert parent[p.stage] is None and parent['current_stage'] == 'first_instance'
    if location == 'archive':
        from court_monitor.storage import load_json
        refresh.persist_archives(data)
        saved = load_json(config.JSON_ARCHIVE_PATH)['cases'][0]
        assert saved[p.stage]['act_text'] == TEXT
        assert saved['archived'] and saved['events'] == [{'type':'existing'}]
        assert saved['current_stage'] == 'cassation_watch'


def test_detached_wait_is_preserved_until_production_is_loaded(publication):
    p = publication; data = {}
    p.module.sync(data, [p.case], DAY)
    before = deepcopy(data[p.module.FIELD])
    result, report, requests = p.run(data, [])
    assert not requests and result['planned'] == 0 and report['unplanned'] == 1
    assert report['items'][0]['reason'] == 'missing_production'
    assert data[p.module.FIELD] == before
    assert p.run(data, [p.case])[1]['published'] == 1


@pytest.mark.parametrize('failure', ['network', 'shell', 'number', 'uid'])
def test_failed_full_read_preserves_due_task_and_retry(publication, failure):
    p = publication; data = {}; parsed = deepcopy(p.info)
    if failure == 'shell':
        parsed = {}
    elif failure in ('number', 'uid'):
        key = ({'number':'page_case_number', 'uid':'judicial_uid'} if p.stage == 'cassation'
               else {'number':'Номер дела (карточка)', 'uid':'УИД'})[failure]
        parsed[key] = '2-999/2026' if failure == 'number' else 'other'
    result, report, _ = p.run(data, [p.case], parsed=parsed, html='' if failure == 'network' else 'html')
    reason = 'fetch_error' if failure == 'network' else 'empty_shell' if failure == 'shell' else 'identity_mismatch'
    assert result['unread'] == report['unread'] == 1
    assert result['items'][0]['reason'] == reason
    assert p.case[p.stage]['last_checked_at'] == '2026-10-01'
    assert not data.get(refresh.JOURNAL)
    assert not p.run(data, [p.case])[2]
    if failure in ('number', 'uid'):
        assert next(iter(data[p.module.FIELD].values()))['status'] == 'needs_review'
    else:
        assert p.run(data, [p.case], now=datetime(2026,10,12,6,31))[1]['published'] == 1


def test_publication_journal_recovers_and_requires_saved_context(publication):
    from court_monitor.storage import save_json, load_json
    p = publication; c = p.case; data = {'cases':[c], 'keep':{'other':True}}
    save_json(data, config.JSON_PATH)
    p.run(data, [c], persist=refresh.checkpoint)
    saved = load_json(config.JSON_PATH)
    assert not saved['cases'][0][p.stage].get('act_text')
    assert refresh.restore(saved, saved['cases'])
    assert saved['cases'][0][p.stage]['act_text'] == TEXT and saved['keep'] == {'other':True}
    assert p.run(saved, saved['cases'])[0]['planned'] == 0
    assert sum(p.event_type in e['type'] for e in p.events(saved)) == 1
    with pytest.raises(RuntimeError):
        refresh.acknowledge(saved, {'issue_key':'day'}, 'day')
    context = dict(refresh.pending_context(saved), issue_key='day')
    refresh.acknowledge(saved, context, 'day')
    assert refresh.JOURNAL not in load_json(config.JSON_PATH)


def test_failed_journal_write_keeps_publication_retryable(publication):
    p = publication; data = {}; saved = {}; original = deepcopy(p.case)
    def persist(value):
        if value.get(refresh.JOURNAL):
            raise OSError('disk full')
        saved.clear(); saved.update(deepcopy(value))
    with pytest.raises(OSError):
        p.run(data, [p.case], persist=persist)
    assert p.case[p.stage].get('act_text') is None
    assert p.case[p.stage]['last_checked_at'] == original[p.stage]['last_checked_at']
    assert p.run(saved, [original], now=datetime(2026,10,12,6,31))[1]['published'] == 1
    assert sum(p.event_type in e['type'] for e in p.events(saved)) == 1


def test_link_collision_is_visible_without_http(publication):
    p = publication; other = deepcopy(p.case)
    other[p.stage]['link'] = '999|another'
    result, report, requests = p.run({}, [p.case, other])
    assert not requests and result['read'] == 0 and report['unplanned'] == 1
    assert report['items'][0]['reason'] == 'identity_conflict'


def test_missing_link_is_visible_without_http(publication):
    p = publication; p.case[p.stage]['link'] = ''
    result, report, requests = p.run({}, [p.case])
    assert not requests and result['planned'] == 0 and report['unplanned'] == 1
    assert report['items'][0]['reason'] == 'missing_card_link'


def test_historical_text_and_incomplete_source_do_not_announce_again(publication):
    p = publication; data = {}
    p.case[p.stage].pop('act_absent_checked_at')
    partial = dict(p.info, act_text=TEXT.split(' РЕШИЛ:')[0])
    assert p.run(data, [p.case], parsed=partial)[1]['backfilled'] == 1
    assert all(p.event_type not in e['type'] for e in p.events(data))
    p.case[p.stage]['act_summary_needs_source'] = True
    assert p.run(data, [p.case], force=True)[1]['published'] == 0
    assert p.case[p.stage]['act_text'] == TEXT
    assert not p.case[p.stage].get('act_summary_needs_source')
    assert all(p.event_type not in e['type'] for e in p.events(data))
    assert p.run(data, [p.case], force=True)[0]['planned'] == 0


def test_legacy_completed_wait_restores_block_and_pending_event(publication):
    """Старый checkpoint читается после удаления прежних обходов."""
    from court_monitor import act_publication
    from court_monitor.storage import save_json, load_json
    p = publication; c = p.case; data = {'cases':[c], 'keep':True}
    save_json(data, config.JSON_PATH)
    p.module.sync(data, [c], DAY)
    observer = fi_act_watch.observe if p.stage == 'first_instance' else act_publication.observe
    assert observer(c[p.stage], TEXT, DAY, present=True, confirmed_date='01.10.2026')
    p.module.sync(data, [c], DAY)
    p.module.checkpoint(data)
    saved = load_json(config.JSON_PATH)
    p.module.sync(saved, saved['cases'], DAY)
    assert saved['cases'][0][p.stage]['act_text'] == TEXT and saved['keep']
    pending = {'cassation':'pending_cassation_changes', 'appeal':'pending_appeal_act_changes',
               'first_instance':fi_act_watch.PENDING}[p.stage]
    assert len(saved[pending]) == 1
    if p.stage != 'cassation':
        changes = p.module.merge_changes(saved, [])
        with pytest.raises(RuntimeError):
            p.module.acknowledge(saved, changes, 'day')
        save_json({'issue_key':'day', refresh.CONTEXT[p.stage]:changes}, config.LAST_DIGEST_CONTEXT_PATH)
        p.module.acknowledge(saved, changes, 'day')
        assert pending not in load_json(config.JSON_PATH)


def test_existing_announcement_dedup_is_preserved(publication):
    from pathlib import Path
    from court_monitor.storage import _cassation_act_key
    p = publication
    if p.stage == 'first_instance':
        # У FI уже доставленный текст отмечен в самом производстве.
        p.case[p.stage].update(act_text=TEXT, act_notification_kind='new_publication')
    elif p.stage == 'appeal':
        Path(config.DIGESTED_ACTS_PATH).write_text(p.case[p.stage]['case_number']+'\n')
    else:
        Path(config.CASSATION_ACTS_PATH).write_text(_cassation_act_key(p.case[p.stage])+'\n')
    data = {}; p.run(data, [p.case])
    assert all(p.event_type not in event['type'] for event in p.events(data))


@pytest.mark.parametrize('publication', ['first_instance', 'appeal'], indirect=True)
@pytest.mark.parametrize('foreign', [False, True])
def test_document_failure_does_not_confirm_absence(publication, monkeypatch, foreign):
    p = publication; c = p.case
    c[p.stage].pop('act_absent_checked_at')
    domain = 'foreign.example' if foreign else c[p.stage]['court_domain']
    fetched = []
    monkeypatch.setattr(runs, 'fetch_act_text', lambda url, **kw: fetched.append(url) or '')
    data = {}
    parsed = dict(p.info, act_text='', _act_url='https://'+domain+'/act')
    result, _, _ = p.run(data, [c], parsed=parsed)
    assert result['read'] == 1 and c[p.stage]['last_checked_at'] == DAY.isoformat()
    assert not c[p.stage].get('act_absent_checked_at')
    assert not c[p.stage].get('act_text')
    assert len(fetched) == (0 if foreign else 1)
    assert all(p.event_type not in e['type'] for e in p.events(data))


def test_bank_archive_preserves_split_events_on_full_refresh(monkeypatch):
    from court_monitor.storage import save_bank_json, load_bank_json
    monkeypatch.setattr(config, 'BANK_TRACK', True)
    c = case(); c.update(archived=True, track='plaintiff_light')
    save_bank_json({'cases':[deepcopy(c)]}, config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
    data = {}; run(monkeypatch, data, [c])
    refresh.persist_archives(data)
    saved = load_bank_json(config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH)['cases'][0]
    assert saved['first_instance']['events'] == c['first_instance']['events']
    assert saved['first_instance']['writs'] == c['first_instance']['writs']
    assert saved['first_instance']['act_text'] == TEXT and saved['archived']
    assert saved['first_instance']['last_checked_at'] == DAY.isoformat()
    assert saved['bank_role'] == c['bank_role'] and saved['track'] == c['track']


def test_full_read_without_text_waits_without_second_read(publication):
    p = publication; data = {}
    parsed = dict(p.info, act_text='', act_published=True, **{'Акт опубликован':'Да'})
    assert p.run(data, [p.case], parsed=parsed)[0]['read'] == 1
    assert next(iter(data[p.module.FIELD].values()))['status'] == 'waiting'
    assert p.run(data, [p.case], parsed=parsed)[0]['planned'] == 0
    p.case['current_stage'] = 'cassation'
    assert p.run(data, [p.case], force=True)[1]['published'] == 1
    assert p.case['current_stage'] == 'cassation'


def test_real_parser_flows_into_full_card(publication, monkeypatch):
    from court_monitor.parsing.cards import parse_case_card
    from court_monitor.parsing.cassation import parse_cassation_card
    p = publication; c = p.case; data = {}
    number = c[p.stage]['case_number']
    uid = '86RS0001-01-2026-000001-01'
    c[p.stage]['judicial_uid'] = c['first_instance']['judicial_uid'] = uid
    html = ('<body>ДЕЛО № '+number+'<table><tr><th>ДЕЛО</th></tr>'
            '<tr><td>Уникальный идентификатор дела</td><td>'+uid+'</td></tr>'
            '<tr><td>Дата рассмотрения</td><td>01.10.2026</td></tr></table>'
            + '<table><tr><td>СЛУЖЕБНЫЙ РАЗДЕЛ</td></tr></table>'*5
            + '<table><tr><th>РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ</th></tr>'
            '<tr><td>Номер дела в первой инстанции</td><td>2-10/2026</td></tr></table>'
            + '<div id="cont_doc1">'+TEXT+'</div></body>')
    monkeypatch.setattr(refresh, 'parse_case_card', parse_case_card)
    monkeypatch.setattr(refresh, 'parse_cassation_card', parse_cassation_card)
    result, _ = refresh.refresh(data, [c], DAY, lambda *a, **k:html, runs.apply_existing_card,
                               budget=policy.Budget(data,DAY), persist=lambda d:None)
    assert result['read'] == 1 and c[p.stage]['act_text'] == ' '.join(TEXT.split())
    assert any(p.event_type in event['type'] for event in p.events(data))


def test_legacy_publication_digest_keeps_original_parties(publication):
    from court_monitor import act_publication
    from court_monitor.digest.template import generate_template_digest
    p = publication; old = p.case; data = {}
    p.module.sync(data, [old], DAY)
    parent = dict(old, history=[old], plaintiff='Новый истец')
    parent[p.stage] = None
    observer = fi_act_watch.observe if p.stage == 'first_instance' else act_publication.observe
    observer(old[p.stage], TEXT, DAY, present=True, confirmed_date='01.10.2026')
    p.module.sync(data, [parent], DAY)
    pending = {'cassation':'pending_cassation_changes', 'appeal':'pending_appeal_act_changes',
               'first_instance':fi_act_watch.PENDING}[p.stage]
    kwargs = {'changes':[], 'cass_changes':[], 'fi_changes':[]}
    kwargs[refresh.CONTEXT[p.stage]] = data[pending]
    rendered = generate_template_digest([], cases=[], **kwargs)
    assert 'Истец прежнего круга' in rendered and 'Новый истец' not in rendered


@pytest.mark.parametrize('announced', [False, True])
def test_appeal_same_number_other_court_and_legacy_ambiguity(monkeypatch, announced):
    from types import SimpleNamespace
    from pathlib import Path
    from court_monitor import appeal_act_watch
    first = case(act_text=TEXT)
    first.update(bank_role='Ответчик', track_origin='')
    first['appeal'] = dict(case_number='33-10/2026', court_domain='oblsud--hmao.sudrf.ru',
        link='123|card', status='Решено', hearing_date='01.10.2026',
        act_absent_checked_at='2026-10-01', last_checked_at='2026-10-01')
    other = deepcopy(first); other['appeal']['court_domain'] = 'oblsud--svd.sudrf.ru'
    monkeypatch.setattr(appeal_act_watch, 'APPEAL_COURTS', [
        SimpleNamespace(domain=c['appeal']['court_domain'],base_url='https://'+c['appeal']['court_domain'])
        for c in (first,other)])
    if announced:
        Path(config.DIGESTED_ACTS_PATH).write_text('33-10/2026\n')
    cards, reports = refresh.plan({}, [first,other], DAY)
    report = reports[appeal_act_watch.FIELD]
    if announced:
        assert not cards and report['unplanned'] == 2
        assert {i['reason'] for i in report['items']} == {'legacy_announcement_ambiguous'}
    else:
        assert len(cards) == 2 and cards[0]['url'] != cards[1]['url']


@pytest.mark.parametrize('field,reason', [('case_number','missing_case_number'),
    ('court_domain','missing_court_domain'), ('decision_date','missing_decision_date')])
def test_missing_fi_metadata_is_visible_and_can_be_completed(field, reason):
    c = case(); c.update(bank_role='Ответчик',track_origin='')
    old = c['first_instance'][field]
    c['first_instance'][field] = ''
    if field == 'decision_date':
        c['first_instance'].pop('hearing_date')
        c['first_instance']['events'] = []
    data = {}; cards, reports = refresh.plan(data,[c],DAY)
    assert not cards and reports[fi_act_watch.FIELD]['unplanned'] == 1
    assert reports[fi_act_watch.FIELD]['items'][0]['reason'] == reason
    if field == 'decision_date':
        c['first_instance'][field] = old
        cards, _ = refresh.plan(data,[c],DAY)
        assert len(cards) == len(data[fi_act_watch.FIELD]) == 1


def test_shared_daily_backfill_quota_and_expired_archive(monkeypatch):
    first = case(act_absent_checked_at=''); first.update(bank_role='Ответчик',track_origin='')
    other = deepcopy(first); other['first_instance']['court_domain'] = 'surgray--hmao.sudrf.ru'
    data = {'act_watch_budget':{'date':DAY.isoformat(),'backfill_count':9,'backfill_seconds':0}}
    result, _, requests = run(monkeypatch,data,[first,other])
    assert result['read'] == len(requests) == 1 and result['unread'] == 1
    assert data['act_watch_budget']['backfill_count'] == 10
    assert any(i['reason']=='backfill_daily_card_limit' for i in result['items'])
    expired = case(decision_date='01.01.2026', hearing_date='01.01.2026')
    expired.update(bank_role='Ответчик',track_origin='',archived=True)
    cards, reports = refresh.plan({},[expired],DAY)
    assert not cards and reports[fi_act_watch.FIELD]['expired'] == 1
    assert reports[fi_act_watch.FIELD]['items'][0]['reason'] == 'age_limit'
    assert expired['archived'] and expired['current_stage'] == 'appeal'


@pytest.mark.parametrize('role,result,reason', [('Третье лицо','Иск удовлетворён','bank_not_plaintiff'),
                                             ('Истец','В иске отказано','writ_not_required')])
def test_full_observed_role_and_result_survive_archive(monkeypatch, role, result, reason):
    from court_monitor.storage import save_json, load_json
    c = case(); data = {}
    save_json({'cases':[deepcopy(c)]},config.JSON_ARCHIVE_PATH)
    run(monkeypatch,data,[c],card(participants=['participant'],bank_role_from_participants=role,
                                **{'Результат':result}))
    refresh.persist_archives(data)
    saved = load_json(config.JSON_ARCHIVE_PATH)['cases'][0]
    assert saved['bank_role'] == role and saved['first_instance']['result'] == result
    assert writ_watch.state(saved,DAY) == ('closed',reason)


def test_changed_writ_basis_survives_archive_and_bank_split(monkeypatch):
    from court_monitor.storage import save_json, load_json
    c = case(legal_force_est='2026-11-01',writ_awaited_since='2026-11-01')
    c.update(current_stage='first_instance',track='plaintiff_light')
    save_json({'cases':[deepcopy(c)]},config.JSON_ARCHIVE_PATH)
    data = {}
    run(monkeypatch,data,[c],card(_events=[{'date':'11.10.2026','text':'Вынесено решение по делу'}]))
    refresh.persist_archives(data)
    saved = load_json(config.JSON_ARCHIVE_PATH)['cases'][0]
    assert saved['first_instance']['writ_source_review_reason'] == 'decision_date_changed'
    # При загрузке архивов основной запуск синхронизирует основания до расчётов.
    writ_watch.sync(data,[saved],DAY)
    assert saved['first_instance']['writ_watch_status'] == 'needs_review'
    monkeypatch.setattr(config,'BANK_TRACK',True)
    runs.split_bank_track([saved])
    assert 'legal_force_est' not in saved['first_instance']
    assert 'writ_awaited_since' not in saved['first_instance']
    changes = []
    assert runs.collect_bank_calendar_events([saved],changes,DAY) == 0 and not changes


def test_document_date_wins_over_later_card_movement(monkeypatch):
    c = case(); c.update(bank_role='Ответчик',track_origin='')
    data = {}; run(monkeypatch,data,[c],card(_events=[{'date':'02.10.2026','text':'Вынесено решение по делу'}]))
    assert c['first_instance']['act_decision_date'] == '01.10.2026'
    assert c['first_instance']['act_text'] == TEXT


@pytest.mark.parametrize('reason', ['run_deadline', 'breaker'])
@pytest.mark.parametrize('backfill', [False, True])
def test_unrequested_preserves_attempt_cursor_events_and_daily_quota(monkeypatch, reason, backfill):
    c = case(act_absent_checked_at='' if backfill else '2026-10-01')
    c.update(bank_role='Ответчик', track_origin='')
    data = {'pending_retry_context': {'fi_changes': [{'type': 'existing_event'}]},
            'card_refresh_cursor': {'1': {'stage': 'appeal', 'keys': {'appeal': 'previous'}}}}
    preserved = deepcopy(data)
    snapshots = []
    def fetch(url, **kwargs):
        config.FETCH_DIAG.update(kind=reason)
        return ''
    budget = policy.Budget(data, DAY)
    result, _ = refresh.refresh(data, [c], DAY, fetch, runs.apply_existing_card,
        budget=budget, persist=lambda d: snapshots.append(deepcopy(d)), now=datetime(2026,10,12,6))
    assert result['read'] == 0 and result['unread'] == 1
    assert result['items'][0]['reason'] == ('watch_time_limit' if reason == 'run_deadline' else reason)
    assert data['act_watch_budget']['backfill_count'] == 0
    assert data['act_watch_budget']['backfill_seconds'] == 0
    assert data['card_refresh_cursor'] == preserved['card_refresh_cursor']
    assert data['pending_retry_context'] == preserved['pending_retry_context']
    tasks = data[fi_act_watch.FIELD].values()
    assert all(not task.get('attempt_count') for task in tasks)
    assert snapshots[-1] == data
    assert c['first_instance']['last_checked_at'] == '2026-10-01'


def test_short_budget_defers_without_entering_fetch(monkeypatch):
    data = {}; c = case()
    result, _, requests = run(monkeypatch, data, [c], budget=policy.Budget(data, DAY, seconds=70))
    assert not requests and result['items'][0]['reason'] == 'watch_time_limit'
    assert not data.get('card_refresh_cursor')


def test_last_allowed_backfill_card_has_request_budget(monkeypatch):
    data = {'act_watch_budget': {'date': DAY.isoformat(), 'backfill_count': 9, 'backfill_seconds': 0}}
    c = case(act_absent_checked_at=''); c.update(bank_role='Ответчик', track_origin='')
    r, _, requests = run(monkeypatch, data, [c])
    assert len(requests) == r['read'] == 1
    assert data['act_watch_budget']['backfill_count'] == 10


def test_round_robin_continues_after_restart_without_losing_cards():
    rotation = ['appeal', 'first_instance', 'cassation']
    cards = [dict(priority=1, stage=stage, key=f'{stage}-{n}')
             for stage in rotation for n in range(4)]
    cards += [dict(priority=0, stage='first_instance', key='writ')]
    data = {}
    first = refresh.fair_order(cards, {}, rotation)
    assert first[0]['key'] == 'writ'
    assert [c['stage'] for c in first[1:4]] == rotation
    for c in first[:3]:
        refresh.advance_cursor(data, c)
    # Даже если все карточки остались неуспешными, повторный запуск не
    # начинает снова с апелляции и начала списка первой инстанции.
    second = refresh.fair_order(cards, json.loads(json.dumps(data['card_refresh_cursor'])), rotation)
    assert second[0]['key'] == 'writ'
    assert [c['key'] for c in second[1:4]] == ['cassation-0', 'appeal-1', 'first_instance-1']
    assert sorted(c['key'] for c in second) == sorted(c['key'] for c in cards)


@pytest.mark.parametrize('time,expected', [('06:00:00',3300), ('08:30:00',600),
    ('08:39:00',60), ('08:40:00',0.001), ('08:44:59',0.001), ('08:45:00',3300)])
def test_morning_budget_reserves_publication_before_delivery(time, expected):
    now = datetime.fromisoformat('2026-10-20T'+time+'+05:00')
    assert policy.run_budget(3300, morning=True, now=now) == expected
    assert policy.run_budget(1500, morning=False, now=now) == 1500
    assert policy.run_budget(3300, morning=True, now=now.astimezone(__import__('datetime').timezone.utc)) == expected


def test_disabled_global_budget_does_not_disable_morning_cutoff():
    now = datetime.fromisoformat('2026-10-20T08:44:00+05:00')
    assert policy.run_budget(0, morning=True, now=now) == 0.001
