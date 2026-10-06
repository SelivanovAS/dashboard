# coding: utf-8
from copy import deepcopy
from datetime import date, datetime, timedelta
from types import SimpleNamespace
import pytest
from court_monitor import appeal_act_watch as watch, config
from court_monitor.digest.template import generate_template_digest

TODAY = date(2026, 10, 5)
DOMAIN = 'oblsud--hmao.sudrf.ru'
TEXT = ('Суд исследовал доказательства и оставил решение без изменения. ' * 20).strip()

@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    for field, name in [('JSON_PATH','cases.json'), ('JSON_ARCHIVE_PATH','archive.json'),
                        ('DIGESTED_ACTS_PATH','acts'), ('LAST_DIGEST_CONTEXT_PATH','ctx.json')]:
        monkeypatch.setattr(config, field, str(tmp_path/name))
    monkeypatch.setattr(config, 'BANK_TRACK', False)
    monkeypatch.setattr(watch, 'APPEAL_COURTS', [SimpleNamespace(domain=DOMAIN,base_url='https://'+DOMAIN),
        SimpleNamespace(domain='oblsud--svd.sudrf.ru',base_url='https://oblsud--svd.sudrf.ru')])


def case():
    return {'id':'2-1/2026','current_stage':'cassation_watch','plaintiff':'Прежний истец',
            'defendant':'ПАО Сбербанк','bank_role':'Ответчик',
            'first_instance':{'case_number':'2-1/2026','judicial_uid':'86RS0001-01-2026-000001-01'},
            'appeal':{'case_number':'33-10/2026 (33-9/2025;)','court_domain':DOMAIN,
                      'link':'123|card-uid','status':'Решено','hearing_date':'08.09.2026',
                      'act_absent_checked_at':'2026-09-09','last_checked_at':'2026-09-09','result':'Оставлено без изменения',
                      'act_published':False}}


def info(text=TEXT, **extra):
    return dict({'_table_count':6,'Номер дела (карточка)':'33-10/2026','УИД':'86RS0001-01-2026-000001-01',
                 'act_text':text,'Дата публикации акта':'22.09.2026'}, **extra)


def run(monkeypatch,data,cases,card=None,**kwargs):
    monkeypatch.setattr(watch,'parse_case_card',lambda *a:info() if card is None else card)
    kwargs.setdefault('now', datetime.combine(TODAY, datetime.min.time())+timedelta(hours=6))
    return watch.refresh(data,cases,TODAY,lambda *a,**k:'html',persist=lambda d:None,**kwargs)


@pytest.mark.parametrize('location',['active','history','detached'])
def test_late_text_survives_stage_and_archive(monkeypatch,location):
    old=case();data={};watch.sync(data,[old],TODAY)
    if location=='history':
        parent=dict(old,current_stage='first_instance',appeal=None,history=[deepcopy(old)],plaintiff='Новый истец')
        cases=[parent]
    else:cases=[] if location=='detached' else [old]
    r=run(monkeypatch,data,cases)
    assert r['published']==r['read']==1
    ch=data['pending_appeal_act_changes'][0]
    assert ch['details']['plaintiff']=='Прежний истец'
    assert ch['details']['case_url'].startswith('https://'+DOMAIN)
    assert run(monkeypatch,data,cases)['planned']==0
    assert len(data['pending_appeal_act_changes'])==1
    if location=='history':
        assert parent['current_stage']=='first_instance' and parent['appeal'] is None
        assert parent['history'][0]['appeal']['act_text']==TEXT


def test_published_flag_without_text_keeps_waiting(monkeypatch):
    c=case();c['appeal']['act_published']=True
    data={};assert run(monkeypatch,data,[c],info(''))['published']==0
    assert next(iter(data[watch.FIELD].values()))['status']=='waiting'
    assert run(monkeypatch,data,[c],force=True)['published']==1


@pytest.mark.parametrize('card,reason',[
    ({'_table_count':0},'unread_card'),
    ({'_table_count':2},'unread_card'),
    (info(**{'УИД':'other'}),'identity_mismatch'),
    (info(**{'Номер дела (карточка)':'33-99/2026'}),'identity_mismatch'),
])
def test_failed_reads_are_retryable(monkeypatch,card,reason):
    c=case();data={};r=run(monkeypatch,data,[c],card)
    assert r['unread']==1 and r['items'][0]['reason']==reason
    assert next(iter(data[watch.FIELD].values()))['block']['last_checked_at']=='2026-09-09'
    assert run(monkeypatch,data,[c])['published']==0
    if reason == 'identity_mismatch':
        assert next(iter(data[watch.FIELD].values()))['status']=='needs_review'
    else:
        assert run(monkeypatch,data,[c],now=datetime.combine(TODAY, datetime.min.time())+timedelta(hours=6,minutes=31))['published']==1


def test_failed_act_download_does_not_mark_success(monkeypatch):
    c=case();data={}
    r=run(monkeypatch,data,[c],info('',_act_url='https://'+DOMAIN+'/act'),fetch_text=lambda *a,**k:'')
    assert r['read']==0 and r['items'][0]['reason']=='act_text_unread'
    assert run(monkeypatch,data,[c],now=datetime.combine(TODAY, datetime.min.time())+timedelta(hours=6,minutes=31))['published']==1


def test_same_numbers_in_different_courts_are_separate(monkeypatch):
    a=case();b=deepcopy(a);b['appeal']['court_domain']='oblsud--svd.sudrf.ru'
    data={};run(monkeypatch,data,[a,b]);assert len(data[watch.FIELD])==2
    assert len(watch.merge_changes(data,[]))==2


def test_link_conflict_requires_review(monkeypatch):
    a=case();b=deepcopy(a);b['appeal']['link']='999|other'
    r=run(monkeypatch,{},[a,b]);assert r['unplanned']==1 and r['read']==0


def test_previous_announcement_is_not_repeated(monkeypatch,tmp_path):
    (tmp_path/'acts').write_text(case()['appeal']['case_number']+'\n')
    data={};run(monkeypatch,data,[case()]);assert not data.get('pending_appeal_act_changes')


def test_normal_parser_publication_survives_crash(monkeypatch,tmp_path):
    c=case();data={};watch.sync(data,[c],TODAY)
    (tmp_path/'acts').write_text(c['appeal']['case_number']+'\n')
    c['appeal'].update(act_notification_kind='new_publication',act_published=True,act_text=TEXT)
    watch.sync(data,[c],TODAY);watch.checkpoint(data)
    saved=watch.load_json(config.JSON_PATH)
    assert len(saved['pending_appeal_act_changes'])==1
    assert len(watch.merge_changes(saved,[deepcopy(saved['pending_appeal_act_changes'][0])]))==1


def test_checkpoint_recovery_and_context_ack(monkeypatch):
    c=case();data={'cases':[c]};watch.save_json(data,config.JSON_PATH)
    run(monkeypatch,data,[c]);watch.checkpoint(data)
    saved=watch.load_json(config.JSON_PATH);watch.sync(saved,saved['cases'],TODAY)
    assert saved['cases'][0]['appeal']['act_text']==TEXT
    changes=watch.merge_changes(saved,[])
    with pytest.raises(RuntimeError):watch.acknowledge(saved,changes,'day')
    assert saved['pending_appeal_act_changes']
    watch.save_json({'issue_key':'day','changes':changes},config.LAST_DIGEST_CONTEXT_PATH)
    watch.acknowledge(saved,changes,'day')
    assert 'pending_appeal_act_changes' not in watch.load_json(config.JSON_PATH)


def test_no_task_for_unfinished_hearing():
    c=case();c['appeal']['status']='В производстве'
    assert not watch.sync({},[c],TODAY)


def test_history_snapshot_and_digest(monkeypatch):
    old=case();parent=dict(old,plaintiff='Новый истец',appeal=None,history=[old])
    data={};run(monkeypatch,data,[parent])
    ch=data['pending_appeal_act_changes']
    rendered=generate_template_digest([],ch,cases=[])
    assert 'Прежний истец' in rendered and 'Новый истец' not in rendered


def test_real_parser_identity_and_inline_text():
    html='<h1>ДЕЛО № 33-10/2026</h1><table><tr><th>ДЕЛО</th></tr><tr><td>Уникальный идентификатор дела</td><td>86RS0001-01-2026-000001-01</td></tr></table>'
    html+='<table><tr><td>СЛУЖЕБНЫЙ РАЗДЕЛ</td></tr></table>'*5
    html+='<div id="cont_doc1">'+TEXT+'</div>'
    c=case();data={}
    r=watch.refresh(data,[c],TODAY,lambda *a,**k:html,persist=lambda d:None)
    assert r['published']==1


def test_failed_write_leaves_event_retryable(monkeypatch):
    saved={};c=case();data={}
    monkeypatch.setattr(watch,'parse_case_card',lambda *a:info())
    def persist(d):
        if d.get('pending_appeal_act_changes'):raise OSError('disk full')
        saved.update(deepcopy(d))
    with pytest.raises(OSError):
        watch.refresh(data,[c],TODAY,lambda *a,**k:'html',persist=persist,now=datetime.combine(TODAY,datetime.min.time())+timedelta(hours=6))
    assert run(monkeypatch,saved,[case()],now=datetime.combine(TODAY,datetime.min.time())+timedelta(hours=6,minutes=31))['published']==1


def test_other_court_act_link_is_not_fetched(monkeypatch):
    def forbidden(*a,**k):raise AssertionError('wrong host fetched')
    r=run(monkeypatch,{},[case()],info('',_act_url='https://other.example/act'),fetch_text=forbidden)
    assert r['items'][0]['reason']=='identity_mismatch' and r['read']==0


def test_archive_update_keeps_stage_and_existing_events(monkeypatch):
    c=case();c['archived']=True;c['events']=[{'type':'existing'}]
    watch.save_json({'cases':[c]},config.JSON_ARCHIVE_PATH)
    data={};run(monkeypatch,data,[deepcopy(c)])
    watch.persist_archives(data,TODAY)
    stored=watch.load_json(config.JSON_ARCHIVE_PATH)['cases'][0]
    assert stored['archived'] and stored['current_stage']=='cassation_watch'
    assert stored['events']==c['events'] and stored['appeal']['act_text']==TEXT


def test_legacy_announcement_with_same_number_in_two_courts_needs_review(monkeypatch,tmp_path):
    a=case();b=deepcopy(a);b['appeal']['court_domain']='oblsud--svd.sudrf.ru'
    (tmp_path/'acts').write_text(a['appeal']['case_number']+'\n')
    data={};r=run(monkeypatch,data,[a,b])
    assert r['unplanned']==2 and r['read']==0
    assert {x['reason'] for x in r['items']}=={'legacy_announcement_ambiguous'}
