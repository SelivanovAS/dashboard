from pathlib import Path
from copy import deepcopy

import pytest

from court_monitor import config, targeted_add
from court_monitor.lifecycle import is_case_archived, advance_case_stage
from court_monitor.linking import _cassation_card_to_block, link_cassation_cases
from court_monitor.parsing.cassation import parse_cassation_card
from court_monitor.runs import announce_imported_cases, announce_imported_presidium_cases
from court_monitor.digest.template import generate_template_digest

FIX=Path(__file__).parent/'fixtures'
BASE='https://oblsud--hmao.sudrf.ru'


def card(name):return parse_cassation_card((FIX/name).read_text(),BASE)


@pytest.mark.parametrize('word',['ОТКАЗ В ПЕРЕДАЧЕ','ОТКАЗАНО В ПЕРЕДАЧЕ'])
def test_refusal_uses_named_review_date(word):
    raw=(FIX/'case_card_presidium_refusal.html').read_text().replace('ОТКАЗ В ПЕРЕДАЧЕ',word)
    info=parse_cassation_card(raw,BASE)
    assert info['review_date']==info['decision_date']=='11.08.2026'
    block=_cassation_card_to_block(info)
    assert block['outcome']=='cassation_dismissed_no_transfer'
    assert is_case_archived({'current_stage':'cassation','cassation':block})
    # Та же дата изучения без финального результата не завершает дело.
    info=parse_cassation_card(raw.replace(word,'ПЕРЕДАНО НА ИЗУЧЕНИЕ'),BASE)
    assert info['review_date']=='11.08.2026' and info['decision_date']==''


def test_early_card_admitted_without_inventing_identity(monkeypatch,tmp_path):
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    info=card('case_card_presidium_early.html'); info['link']='27000814|10f9d0d7-5e3f-4271-98c4-1b5457bbf00c'
    assert info['filing_date']=='15.09.2026'
    assert not info['judicial_uid'] and not info['fi_case_number']
    cases,_,found=link_cassation_cases([], [info])
    assert len(found)==1 and cases[0]['id']=='4Г-80/2026'
    assert cases[0]['first_instance']['court_domain']=='surggor--hmao.sudrf.ru'
    assert not cases[0]['first_instance']['magistrate']
    assert not cases[0]['first_instance']['case_number']
    assert cases[0]['first_instance']['decision_date']=='16.06.2026'
    assert cases[0]['first_instance']['hearing_date']==''
    assert cases[0]['cassation']['decision_date']==''
    assert link_cassation_cases(cases,[info])[2]==[]


@pytest.mark.parametrize('source', ['targeted_presidium', 'dump_presidium', 'dump_cassation'])
def test_import_announced_in_cassation_after_fi_channel(source, monkeypatch, tmp_path):
    monkeypatch.setattr(config, 'CASSATION_ACTS_PATH', str(tmp_path/'acts'))
    monkeypatch.setattr(config, 'JSON_PATH', str(tmp_path/'cases.json'))
    info = card('case_card_presidium_early.html')
    info['link'] = '27000814|10f9d0d7-5e3f-4271-98c4-1b5457bbf00c'
    cases, _, _ = link_cassation_cases([], [info])
    case = cases[0]
    case['import'] = {'source': source, 'announced': False}
    # Порядок полного прогона: сначала канал первой инстанции, затем кассации.
    fi_new = announce_imported_cases(cases)
    assert fi_new == []
    assert case['import']['announced'] is False
    cass_new = announce_imported_presidium_cases(cases)
    assert cass_new == [case]
    digest = generate_template_digest([], [], fi_new_cases=fi_new,
                                      cass_discovered=cass_new)
    assert 'КАССАЦИЯ' in digest and 'Новые касс. дела (1)' in digest
    assert '4Г-80/2026' in digest
    assert '15.09.2026</b> — 📥 поступила касс. жалоба' in digest
    assert 'ПЕРВАЯ ИНСТАНЦИЯ' not in digest and 'Новые иски' not in digest
    assert '16.06.2026' not in digest and 'заседание назначено' not in digest
    assert case['import']['announced'] is True
    assert announce_imported_cases(cases) == []
    assert announce_imported_presidium_cases(cases) == []


def test_presidium_district_remand_is_not_archived():
    c={'current_stage':'cassation','first_instance':{'court_domain':'surggor--hmao.sudrf.ru'},
       'cassation':{'case_number':'4Г-80/2026','court_domain':'oblsud--hmao.sudrf.ru',
                    'outcome':'cassation_remanded','decision_date':'01.01.2026'}}
    assert not is_case_archived(c)
    assert advance_case_stage(c)=='cassation'
    assert c['current_stage']=='awaiting_relink'


@pytest.mark.parametrize('domain',['oblsud.hmao.sudrf.ru','oblsud--hmao.sudrf.ru'])
def test_link_queue_add_and_repeat(domain,monkeypatch,tmp_path):
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'acts'))
    monkeypatch.setattr(targeted_add,'polite_delay',lambda:None)
    monkeypatch.setattr(targeted_add,'fetch_card_checked',lambda *a,**kw:(FIX/'case_card_presidium_early.html').read_text())
    state={key:{'cases':[]} for key in ('main','main_archive','bank','bank_archive')}
    state.update(cold={},bank_cold={},dirty=set())
    url=f'https://{domain}/modules.php?name_op=case&case_id=27000814&case_uid=aa-bb&delo_id=2800001&srv_num=1'
    first=targeted_add.process_item(state,url,'Тест','2026-09-28')
    assert first['status']==targeted_add.ST_ADDED_MAIN
    assert len(state['main']['cases'])==1
    assert targeted_add.process_item(state,url,'Тест','2026-09-28')['status']==targeted_add.ST_ALREADY
    assert targeted_add.process_item(state,url.replace('2800001','5'),'Тест','2026-09-28')['status']==targeted_add.ST_REFUSED
    assert targeted_add.process_item(state,url.replace(domain,'oblsud.svd.sudrf.ru'),'Тест','2026-09-28')['status']==targeted_add.ST_REFUSED
