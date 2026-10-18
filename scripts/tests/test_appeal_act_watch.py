# coding: utf-8
from copy import deepcopy
from datetime import date
from types import SimpleNamespace
import pytest
from court_monitor import appeal_act_watch as watch, config

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


def test_normal_parser_publication_survives_crash(monkeypatch,tmp_path):
    c=case();data={};watch.sync(data,[c],TODAY)
    (tmp_path/'acts').write_text(c['appeal']['case_number']+'\n')
    c['appeal'].update(act_notification_kind='new_publication',act_published=True,act_text=TEXT)
    watch.sync(data,[c],TODAY);watch.checkpoint(data)
    saved=watch.load_json(config.JSON_PATH)
    assert len(saved['pending_appeal_act_changes'])==1
    assert len(watch.merge_changes(saved,[deepcopy(saved['pending_appeal_act_changes'][0])]))==1


def test_no_task_for_unfinished_hearing():
    c=case();c['appeal']['status']='В производстве'
    assert not watch.sync({},[c],TODAY)
