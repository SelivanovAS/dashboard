"""Адресное исправление ХМАО; только на подтверждённых исходных HTML.

Без --apply выводит план, не изменяя файлы. Отправки сообщений нет.
Перед применением на исполнителе требуется его штатный .run.lock.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from court_monitor.parsing.cards import parse_case_card
from court_monitor.parsing.cassation import parse_cassation_card
from court_monitor.act_publication import document_date

CORRECT = ('Повторная судебно-психиатрическая экспертиза подтвердила, что при заключении '
           'кредитного договора заёмщица могла понимать значение своих действий и руководить ими. '
           'Оснований для признания договора недействительным по этому основанию не имелось. '
           'Апелляция отменила решение первой инстанции и отказала в иске к банку.')


def repair(root, appeal_html, old_cass_html, new_cass_html, apply=False):
    assert ((root/'REGION').read_text().strip() if (root/'REGION').exists() else 'hmao') == 'hmao', 'Исправление только для ХМАО'
    ap=parse_case_card(appeal_html)
    assert ap['УИД']=='86RS0004-01-2024-013180-35' and ap['Номер дела (карточка)']=='33-30/2026'
    text=ap['act_text']
    assert document_date(text)=='31.03.2026' and 'В удовлетворении исковых требований' in text and 'отказать' in text[-2000:]
    left=parse_cassation_card(old_cass_html,'https://7kas.sudrf.ru')
    right=parse_cassation_card(new_cass_html,'https://7kas.sudrf.ru')
    for value in (left,right):
        assert value['page_case_number']=='8Г-7248/2026' and value['judicial_uid']=='86RS0017-01-2025-000990-42'
        assert value['decision_date']=='10.06.2026'
    assert left==right, 'Адреса не доказаны как равнозначные'
    paths=['cases.json','cases_archive.json','last_digest_context.json','last_digest.json','.act_summaries.json']
    docs={name:json.loads((root/'data'/name).read_text()) for name in paths if (root/'data'/name).exists()}
    before=deepcopy(docs);bad=set();changed_blocks=0
    def productions(doc):
        for case in doc.get('cases',[]):
            yield case
            yield from case.get('history') or []
    for name in ('cases.json','cases_archive.json'):
        for case in productions(docs[name]):
            block=case.get('appeal') or {}
            if block.get('link','').startswith('23779386|'):
                aa=block.get('act_analysis') or {}
                match=re.search(r'<b>Почему:</b>\s*<i>(.*?)</i>',aa.get('html',''),re.S)
                if match and match[1] != CORRECT:bad.add(match[1])
                block.update(act_text=text,act_decision_date='31.03.2026',act_detected_at='2026-10-06',act_notification_kind='legacy_announced')
                if aa:
                    aa['html']=re.sub(r'<b>Почему:</b>\s*<i>.*?</i>','<b>Почему:</b> <i>'+CORRECT+'</i>',aa['html'],flags=re.S)
                    aa.update(act_date='31.03.2026',model='reviewed-source')
                    aa.setdefault('reviewed_at',datetime.now(timezone.utc).isoformat())
                    if 'Текст обнаружен системой:' not in aa['html']:
                        head,sep,tail=aa['html'].partition('\n')
                        aa['html']=head+'\nАкт вынесен: 31.03.2026\nТекст обнаружен системой: 06.10.2026\n'+tail
                changed_blocks+=1
            block=case.get('cassation') or {}
            if block.get('case_number')=='8Г-7248/2026' and block.get('judicial_uid')=='86RS0017-01-2025-000990-42':
                block['link']='15806445|3ba9803a-077c-42fe-83dd-03d6d5be74de'
                block['last_checked_at']='2026-10-06'
                if not right.get('act_text'):block['act_absent_checked_at']='2026-10-06'
    main=docs['cases.json']
    for task in (main.get('cassation_act_watch') or {}).values():
        b=task['block']
        if b.get('case_number')=='8Г-7248/2026' and b.get('judicial_uid')=='86RS0017-01-2025-000990-42':
            b.update(link='15806445|3ba9803a-077c-42fe-83dd-03d6d5be74de',last_checked_at='2026-10-06',act_absent_checked_at='2026-10-06')
            task.update(status='waiting',reason='verified_link_aliases');task.pop('next_check_at',None)
        if b.get('case_number')=='4Г-66/2026' and b.get('court_domain')=='oblsud--hmao.sudrf.ru':
            task.update(status='needs_review',reason='invalid_card_request')
    # Сохранённый контекст и карточка не должны возвращать ошибочный пересказ.
    for task in (main.get('appeal_act_watch') or {}).values():
        b=task['block']
        if b.get('link','').startswith('23779386|'):
            b.update(act_text=text,act_decision_date='31.03.2026',act_detected_at='2026-10-06',act_notification_kind='legacy_announced')
    context=docs.get('last_digest_context.json',{})
    for change in context.get('changes',[]):
        if change.get('case','').startswith('33-30/2026'):
            change['details'].update(act_text=text,act_decision_date='31.03.2026',act_detected_at='2026-10-06')
    cache=docs.get('.act_summaries.json',{})
    for key,value in list(cache.items()):
        if isinstance(value,dict) and value.get('summary') in bad:del cache[key]
    def replace(value):
        if isinstance(value,str):
            for old in bad:value=value.replace(old,CORRECT)
            return value
        if isinstance(value,dict):return {k:replace(v) for k,v in value.items()}
        if isinstance(value,list):return [replace(v) for v in value]
        return value
    docs['last_digest.json']=replace(docs.get('last_digest.json',{}))
    changed=[n for n in docs if docs[n]!=before.get(n)]
    if apply:
        for name in changed:
            path=root/'data'/name
            path.write_text(json.dumps(docs[name],ensure_ascii=False,indent=1 if name=='last_digest_context.json' else 2)+'\n')
    return {'files':changed,'appeal_blocks':changed_blocks,'bad_summaries':len(bad),'source_sha256':hashlib.sha256(appeal_html.encode()).hexdigest(),'applied':apply}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path.cwd());p.add_argument('--appeal-html',type=Path,required=True);p.add_argument('--old-cass-html',type=Path,required=True);p.add_argument('--new-cass-html',type=Path,required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    print(json.dumps(repair(a.root,a.appeal_html.read_text(),a.old_cass_html.read_text(),a.new_cass_html.read_text(),a.apply),ensure_ascii=False))
