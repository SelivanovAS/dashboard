#!/usr/bin/env python3
"""Офлайн-восстановление согласованного снимка ХМАО от 28.09.2026.

Читает заранее сохранённые HTML и urls.json из --cards-dir, проверяет
идентичность карточек. По умолчанию только отчёт; --apply пишет локальные
картотеки. Сеть, доставка, очередь и маркеры уже отправленных актов не меняются.
Повторный запуск с тем же снимком не меняет данные.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path

from court_monitor import config
from court_monitor.cassation_lookup import card_matches_case
from court_monitor.complaints import remember_cassation_resolution, stamp_complaint_tracking
from court_monitor.courts import canon_sudrf_domain
from court_monitor.lifecycle import fi_termination_date, is_case_archived, migrate_stages
from court_monitor.linking import _cassation_card_to_block, link_cassation_cases, retain_historical_cassation
from court_monitor.parsing.cards import parse_case_card
from court_monitor.parsing.cassation import parse_cassation_card
from court_monitor.targeted_add import load_tracked_state, parse_card_link, save_state

RECOVER = {
    '33-2629/2026': ('8Г-15647_2025', '26.11.2025'),
    '33-2022/2026': ('8Г-17329_2025', '22.01.2026'),
    '33-5177/2026': ('8Г-7248_2026', '10.06.2026'),
    '33-1577/2026': ('8Г-5947_2026', '21.05.2026'),
    '33-4383/2026': ('8Г-11947_2026', '08.09.2026'),
}
AUDIT = set(RECOVER) | {'33-4289/2026', '33-3783/2026', '2-1742/2026',
                         '2-6206/2026', '2-399/2026', '2-3107/2026'}


def repair(state, cards_dir):
    root=Path(cards_dir)
    urls=json.loads((root/'urls.json').read_text())
    sources={k:state[k].setdefault('cases',[]) for k in ('main','main_archive','bank','bank_archive')}
    before={k:deepcopy(state[k]) for k in sources}
    all_cases=[c for cases in sources.values() for c in cases]
    affected=[]
    for track, numbers in [('main', AUDIT | {'9-155/2026'}), ('bank', {'2-616/2026'})]:
        for c in sources[track]+sources[track+'_archive']:
            if c['id'] not in numbers:continue
            name=track+'-'+c['id'].replace('/','_')
            card=parse_case_card((root/(name+'.html')).read_text(),urls[name].split('/modules.php')[0])
            fi=c['first_instance']
            assert card.get('УИД')==fi.get('judicial_uid'), f"УИД не совпал: {name}"
            for kind in ('appeal','cassation'):
                events=card.get('_fi_'+kind+'_events')
                if events:fi[kind+'_events']=events
            if c['id']=='9-155/2026':
                value=fi_termination_date(fi,card.get('Дата рассмотрения (карточка)',''))
                assert value=='22.09.2026'
                fi['termination_date']=value
            affected.append(c)
    assert len(affected)==13, 'Проверяются ровно 12 жалоб и 9-155/2026'

    def cass_card(name):
        target=parse_card_link(urls[name]); assert target and target['delo_id']==2800001
        info=parse_cassation_card((root/(name+'.html')).read_text(),'https://'+target['domain'])
        assert info and info['sber_present'], name
        info.update(court_domain=target['domain'],link=f"{target['case_id']}|{target['case_uid']}",
                    cassation_internal_number=info['page_case_number'])
        return info

    for case_id,(filename,expected_date) in RECOVER.items():
        c=next(c for c in affected if c['id']==case_id)
        info=cass_card(filename)
        assert info['decision_date']==expected_date
        row={'cassation_internal_number':filename.replace('_','/')}
        assert card_matches_case(c,info,row), f"Не подтверждена связка: {case_id}"
        block=_cassation_card_to_block(info); block['last_checked_at']='2026-09-28'
        owners=[x for x in all_cases if x is not c and
                (x.get('cassation') or {}).get('case_number')==block['case_number'] and
                canon_sudrf_domain((x.get('cassation') or {}).get('court_domain') or '7kas.sudrf.ru')==block['court_domain']]
        if retain_historical_cassation(c,block):
            if owners:
                hist=next(h for h in c['history'] if (h.get('cassation') or {}).get('case_number')==block['case_number'])
                for key in ('first_instance','appeal'):
                    if owners[0].get(key):hist[key]=deepcopy(owners[0][key])
        elif owners:
            assert len(owners)==1 and card_matches_case(owners[0],info,row)
            remember_cassation_resolution(c,block,owners[0]['id'])
        else:
            _,_,discovered=link_cassation_cases([c],[info],record_delivery=False)
            assert not discovered
            c['cassation']['last_checked_at']='2026-09-28'
        stamp_complaint_tracking(c)

    pres16=next(c for c in all_cases if c['id']=='4Г-16/2026')
    info=cass_card('pres16')
    assert info['decision_date']=='11.08.2026'
    link_cassation_cases([pres16],[info],record_delivery=False)
    pres16['cassation']['last_checked_at']='2026-09-28'
    affected.append(pres16)
    if not any(c['id']=='4Г-80/2026' and (c.get('cassation') or {}).get('court_domain')=='oblsud--hmao.sudrf.ru' for c in all_cases):
        info=cass_card('pres80')
        assert info['filing_date']=='15.09.2026' and not info['fi_case_number'] and not info['judicial_uid']
        _,_,new=link_cassation_cases([], [info],record_delivery=False)
        assert len(new)==1
        new[0]['import']={'source':'targeted_presidium','operator':'Восстановление данных',
                          'imported_at':'2026-09-28T00:00:00+05:00','announced':False}
        new[0]['cassation']['last_checked_at']='2026-09-28'
        sources['main'].append(new[0]);affected.extend(new)
    pres80=next(c for c in sources['main'] if c['id']=='4Г-80/2026')
    pres80['notes']='Добавлено по карточке президиума (Президиум Суда ХМАО-Югры)'
    migrate_stages(affected)
    # Действующие окна архивации — только для исправленных записей.
    for track in ('main','bank'):
        for c in list(sources[track]):
            if any(c is x for x in affected) and is_case_archived(c):
                c.setdefault('archived_at','2026-09-28')
                sources[track].remove(c);sources[track+'_archive'].append(c)
    dirty_names={'main':'main','main_archive':'hot_archive','bank':'bank','bank_archive':'bank_archive'}
    for key in sources:
        if before[key]!=state[key]:state['dirty'].add(dirty_names[key])
    return [{'case':c['id'],'stage':c['current_stage'],
             'complaints':{k:v['state'] for k,v in (c.get('complaint_tracking') or {}).items()},
             'archived':any(c is x for x in sources['main_archive']+sources['bank_archive'])}
            for c in affected]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cards-dir',required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    if config.REGION!='hmao':parser.error('Восстановление предназначено только для ХМАО')
    state=load_tracked_state()
    report=repair(state,args.cards_dir)
    # Проверка идемпотентности ДО записи.
    once=deepcopy(state)
    repair(state,args.cards_dir)
    assert state==once, 'Повторное восстановление меняет данные'
    print(json.dumps({'changed':sorted(state['dirty']),'cases':report},ensure_ascii=False,indent=2))
    if args.apply:
        for path in save_state(state):print('Сохранено:',path)


if __name__=='__main__':main()
