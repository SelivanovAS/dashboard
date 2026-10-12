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


def test_wrong_act_does_not_discard_other_card_updates(monkeypatch):
    c=case(); data={}
    r,_,_=run(monkeypatch,data,[c],card(act_text=TEXT.replace('01.10.2026','02.10.2026')))
    assert r['read']==1 and c['first_instance']['judge']=='Новый судья'
    assert c['first_instance']['appeal_filed']
    assert not c['first_instance'].get('act_text')
    task=next(iter(data[fi_act_watch.FIELD].values()))
    assert task['status']=='needs_review' and task['reason']=='act_identity_mismatch'


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
