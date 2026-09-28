from copy import deepcopy
from datetime import date
from urllib.parse import parse_qs, urlsplit

from court_monitor import config
from court_monitor import cassation_lookup as lookup
from court_monitor.complaints import stamp_complaint_tracking
from court_monitor.courts import CASSATION_COURT, FIRST_INSTANCE_COURTS


def example():
    court=FIRST_INSTANCE_COURTS[0]
    c={'id':'2-1/2026','current_stage':'cassation_pending','first_instance':{
        'case_number':'2-1/2026','court':court.name,'court_domain':court.domain,'srv_num':court.srv_num,
        'judicial_uid':'86RS0001-01-2026-000001-01','cassation_filed_date':'01.06.2026',
        'cassation_events':[{'date':'01.07.2026','text':'Дата рассмотрения жалобы'}]},
       'appeal':{'case_number':'33-1/2026','filing_date':'01.05.2026'}}
    row={'case_id':'123','case_uid':'aa-bb','cassation_internal_number':'8Г-1/2026','fi_court_config':court}
    info={'page_case_number':'8Г-1/2026','fi_case_number':'2-1/2026','fi_court_config':court,
          'sber_present':True,'judicial_uid':c['first_instance']['judicial_uid'],
          'filing_date':'03.06.2026','decision_date':'01.07.2026','result_text':'Оставлено без изменения'}
    return c,row,info


def test_uid_then_number_with_verified_card(monkeypatch,tmp_path):
    c,row,info=example(); queries=[]
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    def fetch(url,**kw):
        queries.append(parse_qs(urlsplit(url).query))
        return 'Данных по запросу не обнаружено' if len(queries)==1 else 'search'
    monkeypatch.setattr(lookup,'fetch_page',fetch)
    monkeypatch.setattr(lookup,'parse_cassation_search_page',lambda h:[row] if h=='search' else [])
    monkeypatch.setattr(lookup,'fetch_card_checked',lambda *a,**kw:'card')
    monkeypatch.setattr(lookup,'parse_cassation_card',lambda *a:deepcopy(info))
    changes,stats=lookup.lookup_missing_cassations([c],date(2026,9,28))
    assert 'g33_case__JUDICIAL_UIDSS' in queries[0]
    assert queries[1]['G33_CASE__CASE_NUMBER_ISS']==['2-1/2026']
    assert c['cassation']['case_number']=='8Г-1/2026'
    assert c['current_stage']=='cassation'
    assert stats['parsed']==1
    assert c['complaint_tracking']['cassation']['state']=='completed'
    assert changes


def test_identity_uid_does_not_override_number_court_or_dates():
    c,row,info=example()
    assert lookup.card_matches_case(c,info,row)
    for patch in ({'fi_case_number':'2-99/2026'},{'page_case_number':'8Г-2/2026'},
                  {'fi_court_config':FIRST_INSTANCE_COURTS[-1]}, {'decision_date':'01.01.2026'},
                  {'judicial_uid':'another-uid'}, {'sber_present':False}):
        assert not lookup.card_matches_case(c,{**info,**patch},row),patch


def test_network_failure_is_not_empty_search(monkeypatch):
    c,_,_=example()
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    monkeypatch.setattr(lookup,'fetch_page',lambda *a,**kw:'')
    lookup.lookup_missing_cassations([c],date(2026,9,28))
    v=c['complaint_tracking']['cassation']['verification']
    assert v['attempts']==0 and v['last_error']
    assert v['last_attempt_at']=='2026-09-28'


def test_missing_outcome_of_known_card_has_bounded_search(monkeypatch,tmp_path):
    c,row,info=example(); info['result_text']=''; info['decision_date']=''
    c['cassation']={'case_number':'8Г-1/2026','filing_date':'03.06.2026'}
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    monkeypatch.setattr(lookup,'fetch_page',lambda *a,**kw:'search')
    monkeypatch.setattr(lookup,'parse_cassation_search_page',lambda h:[row])
    monkeypatch.setattr(lookup,'fetch_card_checked',lambda *a,**kw:'card')
    monkeypatch.setattr(lookup,'parse_cassation_card',lambda *a:deepcopy(info))
    for day in (1,8,15):lookup.lookup_missing_cassations([c],date(2026,9,day))
    assert c['complaint_tracking']['cassation']['state']=='needs_review'


def test_shared_cassation_closes_complaint_without_duplicate_or_changing_appeal(monkeypatch):
    c,row,info=example()
    owner=deepcopy(c);owner['id']='33-other/2026'
    owner['cassation']={'case_number':'8Г-1/2026','court_domain':CASSATION_COURT.domain,
                       'filing_date':'03.06.2026','decision_date':'01.07.2026',
                       'outcome':'cassation_upheld'}
    before=deepcopy(c['appeal'])
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    monkeypatch.setattr(lookup,'fetch_page',lambda *a,**kw:'search')
    monkeypatch.setattr(lookup,'parse_cassation_search_page',lambda h:[row])
    monkeypatch.setattr(lookup,'fetch_card_checked',lambda *a,**kw:'card')
    monkeypatch.setattr(lookup,'parse_cassation_card',lambda *a:deepcopy(info))
    changes,stats=lookup.lookup_missing_cassations([c,owner],date(2026,9,28))
    assert not changes and not c.get('cassation')
    assert c['appeal']==before and c['current_stage']=='cassation_pending'
    assert c['complaint_tracking']['cassation']['state']=='completed'
    assert c['complaint_tracking']['cassation']['resolved_cases'][0]['source_case_id']==owner['id']


def test_known_cassation_does_not_hide_a_later_complaint(monkeypatch,tmp_path):
    c,row,info=example()
    c['first_instance']['cassation_events']=[
        {'complaint_id':'1','date':'01.06.2026','text':'Регистрация жалобы'},
        {'complaint_id':'1','date':'01.07.2026','text':'Дата рассмотрения жалобы'},
        {'complaint_id':'2','date':'01.08.2026','text':'Регистрация жалобы'},
    ]
    c['cassation']={'case_number':'8Г-1/2026','court_domain':CASSATION_COURT.domain,
                   'filing_date':'03.06.2026','decision_date':'01.07.2026',
                   'outcome':'cassation_upheld'}
    row.update(cassation_internal_number='8Г-2/2026')
    info.update(page_case_number='8Г-2/2026',filing_date='03.08.2026',
                decision_date='',result_text='')
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    monkeypatch.setattr(lookup,'fetch_page',lambda *a,**kw:'search')
    monkeypatch.setattr(lookup,'parse_cassation_search_page',lambda h:[row])
    monkeypatch.setattr(lookup,'fetch_card_checked',lambda *a,**kw:'card')
    monkeypatch.setattr(lookup,'parse_cassation_card',lambda *a:deepcopy(info))
    _,stats=lookup.lookup_missing_cassations([c],date(2026,9,28))
    assert stats['searched']==1 and c['cassation']['case_number']=='8Г-2/2026'
    state=c['complaint_tracking']['cassation']
    assert [ep['state'] for ep in state['episodes']]==['completed','active']
    assert state['resolved_cases'][0]['case_number']=='8Г-1/2026'
    # По уже найденной незавершённой жалобе работает обычная дочитка карточки.
    _,stats=lookup.lookup_missing_cassations([c],date(2026,10,5))
    assert stats['searched']==0


def test_active_complaint_does_not_reset_unknown_outcome_attempts(monkeypatch,tmp_path):
    c,row,info=example()
    c['first_instance']['cassation_events']=[
        {'complaint_id':'1','date':'01.06.2026','text':'Регистрация жалобы'},
        {'complaint_id':'1','date':'01.07.2026','text':'Дата рассмотрения жалобы'},
        {'complaint_id':'2','date':'01.08.2026','text':'Регистрация жалобы'},
    ]
    row.update(cassation_internal_number='8Г-2/2026')
    info.update(page_case_number='8Г-2/2026',filing_date='03.08.2026',
                decision_date='',result_text='')
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    monkeypatch.setattr(lookup,'polite_delay',lambda:None)
    monkeypatch.setattr(lookup,'fetch_page',lambda *a,**kw:'search')
    monkeypatch.setattr(lookup,'parse_cassation_search_page',lambda h:[row])
    monkeypatch.setattr(lookup,'fetch_card_checked',lambda *a,**kw:'card')
    monkeypatch.setattr(lookup,'parse_cassation_card',lambda *a:deepcopy(info))
    for day in (1,8,15):lookup.lookup_missing_cassations([c],date(2026,9,day))
    state=c['complaint_tracking']['cassation']
    assert state['state']=='active'  # вторая жалоба всё ещё рассматривается
    assert state['verification']['attempts']==3 and state['verification']['reason']
    assert not state['verification'].get('resolved')
