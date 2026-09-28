"""Регрессии текущего обжалования, истории и ограниченного уточнения."""
from copy import deepcopy
from datetime import date, timedelta

import pytest

from court_monitor import config
from court_monitor.complaints import (complaint_state, stamp_complaint_tracking,
                                      verification_due, record_verification)
from court_monitor.lifecycle import (advance_case_stage, fi_termination_date,
                                     is_case_archived, should_parse_fi_card, should_skip_case)
from court_monitor.linking import link_cassation_cases
from court_monitor.courts import FIRST_INSTANCE_COURTS


def pending(kind='cassation', closed=True):
    return {'id':'2-1/2026', 'current_stage':'cassation_pending' if kind=='cassation' else 'awaiting_appeal',
            'first_instance': {'case_number':'2-1/2026', kind+'_filed':True,
              kind+'_filed_date':'01.06.2026', 'sent_to_'+kind:True,
              kind+'_events': [
                {'complaint_id':'1','date':'01.06.2026','text':'Регистрация жалобы (представления) в суде'},
                *([{'complaint_id':'1','date':'01.07.2026','text':'Дата рассмотрения жалобы'}] if closed else [])]},
            'appeal': {'case_number':'33-1/2026','filing_date':'01.05.2026','hearing_date':'15.05.2026'} if kind=='cassation' else None}


def test_procedural_date_is_not_publication_or_expedition():
    fi={'status':'Решено', 'result':'Заявление ВОЗВРАЩЕНО заявителю', 'event_date':'23.09.2026',
        'events':[{'date':'22.09.2026','text':'Возвращение иска (заявления, жалобы)', 'posted_at':'23.09.2026'},
                  {'date':'23.09.2026','text':'Дело передано в экспедицию'}]}
    assert fi_termination_date(fi)=='22.09.2026'
    assert fi_termination_date({**fi,'events':[]})==''
    assert fi_termination_date({**fi,'events':[]},'22.09.2026')=='22.09.2026'
    assert 'decision_date' not in fi
    assert fi_termination_date({**fi,'status':'В производстве'})==''


@pytest.mark.parametrize('kind',['appeal','cassation'])
def test_three_attempts_weekly_persist_restart_and_network(kind):
    c=pending(kind); today=date(2026,9,28)
    assert verification_due(c,kind,today)
    record_verification(c,kind,today,success=False,links=['https://7kas.sudrf.ru/'])
    c=deepcopy(c)  # сериализованное состояние следующего запуска
    assert not verification_due(c,kind,today+timedelta(days=6))
    assert verification_due(c,kind,today+timedelta(days=7))
    record_verification(c,kind,today+timedelta(days=7),success=False,links=[],error='Сеть')
    assert c['complaint_tracking'][kind]['verification']['attempts']==1
    record_verification(c,kind,today+timedelta(days=8),success=False,links=[])
    record_verification(c,kind,today+timedelta(days=15),success=False,links=[])
    assert c['complaint_tracking'][kind]['state']=='needs_review'
    assert not verification_due(c,kind,today+timedelta(days=22))
    assert not is_case_archived(c)
    c['first_instance'][kind+'_events'].append({'complaint_id':'2','date':'20.10.2026','text':'Регистрация жалобы'})
    assert verification_due(c,kind,today+timedelta(days=22))
    assert c['complaint_tracking'][kind]['state']=='active'
    assert 'verification' not in c['complaint_tracking'][kind]


def test_pending_not_capped_after_three_empty_searches():
    c=pending(closed=False)
    for i in range(4):
        today=date(2026,9,1)+timedelta(days=7*i)
        assert verification_due(c,'cassation',today)
        record_verification(c,'cassation',today,success=False,links=[])
    assert c['complaint_tracking']['cassation']['state']=='active'


def test_unresolved_old_complaint_does_not_archive_new_appeal():
    c=pending(closed=False)
    c['current_stage']='cassation_watch'
    c['first_instance']['cassation_events'][0]['date']='01.01.2025'
    c['appeal'].update(filing_date='01.02.2025',hearing_date='01.03.2025')
    assert complaint_state(c,'cassation')['state']=='historical'
    assert not is_case_archived(c)


def test_multiple_short_complaint_headers_keep_both_dates():
    from court_monitor.parsing.cards import parse_case_card
    html='<table><tr><td>Обжалование решений, определений (пост.)</td></tr></table>'
    for n,d in [(1,'01.06.2026'),(2,'20.06.2026')]:
        html+=f'''<table><tr><td>ЖАЛОБА № {n}</td></tr>
          <tr><td>Вид жалобы (представления)</td><td>апелляционная</td></tr>
          <tr><td>Дата поступления жалобы</td><td>{d}</td></tr></table>'''
    info=parse_case_card(html,'https://surggor--hmao.sudrf.ru')
    c={'first_instance':{'appeal_events':info['_fi_appeal_events']}}
    assert [e['filed_date'] for e in complaint_state(c,'appeal')['episodes']]==['01.06.2026','20.06.2026']


def test_old_cassation_without_result_keeps_current_appeal():
    from court_monitor.linking import retain_historical_cassation
    c=pending(closed=False)
    before=deepcopy(c)
    block={'case_number':'8Г-1/2026','filing_date':'01.04.2026'}
    assert retain_historical_cassation(c,block)
    assert c['appeal']==before['appeal'] and c['current_stage']==before['current_stage']
    assert c['history'][0]['cassation']==block


def test_separate_simultaneous_complaints_and_vacated_default():
    c=pending('appeal'); fi=c['first_instance']
    fi['appeal_events'].append({'complaint_id':'2','date':'20.06.2026','text':'Регистрация жалобы'})
    state=complaint_state(c,'appeal')
    assert state['state']=='active'
    assert [e['state'] for e in state['episodes']]==['resolving','active']
    fi['default_cancellation']={'outcome':'cancelled','outcome_date':'10.08.2026'}
    assert complaint_state(c,'appeal')['state']=='historical'
    assert advance_case_stage(c)=='awaiting_appeal'
    assert fi['appeal_filed']  # исторический флаг сохранён


@pytest.mark.parametrize('stage,kind',[('awaiting_appeal','appeal'),('cassation_pending','cassation')])
def test_sent_complaint_fi_is_still_checked_weekly(monkeypatch,stage,kind):
    monkeypatch.setattr(config,'SMART_SKIP_CASES',True)
    c=pending(kind,closed=False);c['current_stage']=stage
    c['first_instance']['last_checked_at']='2026-09-21'
    assert should_parse_fi_card(c)
    assert should_skip_case(c,date(2026,9,27))==(True,'complaint_weekly')
    assert should_skip_case(c,date(2026,9,28))==(False,'')
    monkeypatch.setattr(config,'SMART_SKIP_CASES',False)
    assert should_skip_case(c,date(2026,9,27))==(False,'')


@pytest.mark.parametrize('ap_num,cs_num,ended,ap_filed',[
    ('33-2629/2026','8Г-15647/2025','26.11.2025','24.03.2026'),
    ('33-2022/2026','8Г-17329/2025','22.01.2026','19.02.2026'),
    ('33-5177/2026','8Г-7248/2026','10.06.2026','04.07.2026'),
])
def test_old_cassation_does_not_replace_new_appeal(monkeypatch,tmp_path,ap_num,cs_num,ended,ap_filed):
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    court=FIRST_INSTANCE_COURTS[0]
    c={'id':ap_num,'current_stage':'appeal','first_instance':{'case_number':'2-1/2025','court_domain':court.domain,'court':court.name},
       'appeal':{'case_number':ap_num,'filing_date':ap_filed}}
    original=deepcopy(c['appeal'])
    info={'cassation_internal_number':cs_num,'fi_case_number':'2-1/2025','fi_court_config':court,
          'sber_present':True,'decision_date':ended,'result_text':'Оставлено без изменения',
          'link':'1|aa-bb','filing_date':'01.10.2025'}
    _,changes,found=link_cassation_cases([c],[info])
    assert changes==found==[]
    assert c['current_stage']=='appeal' and c['appeal']==original
    assert not c.get('cassation')
    assert c['history'][0]['cassation']['case_number']==cs_num
    link_cassation_cases([c],[info])
    assert len(c['history'])==1


def test_all_twelve_reported_cases_fixed_fixture():
    import json
    from pathlib import Path
    cases=json.loads((Path(__file__).parent/'fixtures/complaint_audit_20260928.json').read_text())
    assert len(cases)==12
    expected={'33-5177/2026':'resolving','33-2629/2026':'resolving','33-2022/2026':'resolving',
              '33-1577/2026':'resolving','33-4383/2026':'active','33-4289/2026':'active','33-3783/2026':'active',
              '2-1742/2026':'active','2-6206/2026':'active','2-399/2026':'active','2-3107/2026':'active','2-616/2026':'historical'}
    for c in cases:
        kind='cassation' if c['id'].startswith('33-') else 'appeal'
        assert complaint_state(c,kind)['state']==expected[c['id']],c['id']
    multi=next(c for c in cases if c['id']=='2-399/2026')
    assert len(complaint_state(multi,'appeal')['episodes'])==2


@pytest.mark.parametrize('number,ended',[('8Г-5947/2026','21.05.2026'),('8Г-11947/2026','08.09.2026')])
def test_current_completed_cassation_closes_pending(number,ended):
    c=pending(closed=False)
    c['first_instance']['cassation_filed_date']='01.04.2026'
    c['first_instance']['cassation_events'][0]['date']='01.04.2026'
    c['appeal']['filing_date']='01.03.2026'
    c['cassation']={'case_number':number,'filing_date':'14.04.2026','decision_date':ended,'outcome':'cassation_upheld'}
    assert complaint_state(c,'cassation')['state']=='completed'
