#!/usr/bin/env python3
"""Однократное создание отдельного пилота ХМАО; запускать от root после установки кода."""
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import sys
import tomllib

ROOT=Path(__file__).resolve().parents[1]
BASE=Path('/srv/projects/sbersud/pilot/hmao')
sys.path.insert(0,str(ROOT/'scripts'))
from court_monitor.regions import get_region

def main():
    state=BASE/'state'
    publication=state/'publication'
    if state.exists():raise SystemExit('Состояние уже существует: повторная инициализация запрещена')
    if BASE.resolve()!=BASE:raise SystemExit('Неверный каталог')
    user=pwd.getpwnam('sbersud-pilot')
    data=publication/'snapshots/initial/data'
    data.mkdir(parents=True,mode=0o700)
    def write(path,doc):
        path.write_text(json.dumps(doc,ensure_ascii=False,indent=2)+'\n')
        path.chmod(0o600)
    info=get_region('hmao').public_info()
    write(publication/'.sbersud-isolated.json',{'region':'hmao','mode':'closed-pilot'})
    write(publication/'current.json',{'version':'initial'})
    write(publication/'court-network.json',{'region':'hmao','allowed_hosts':['surggor--hmao.sudrf.ru'],
          'max_requests':12,'ca_file':'/etc/ssl/certs/ca-certificates.crt'})
    for name,count,bank in [('cases.json',600,False),('cases_bank.json',200,True)]:
        records=[]
        for i in range(count):
            number=f'2-{900000+i+(1000 if bank else 0)}/2099'
            records.append({'id':number,'current_stage':'first_instance','bank_role':'Истец' if bank else 'Ответчик',
                'plaintiff':'Тестовый участник','defendant':'Тестовый участник','category':'Синтетический пилот',
                'first_instance':{'case_number':number,'court_domain':'surggor--hmao.sudrf.ru',
                  'court':'Сургутский городской суд','hearing_date':'25.12.2099','hearing_time':'10:30',
                  'timezone':'Asia/Yekaterinburg','events':[{'date':'25.12.2099','event':'Тестовое заседание'}],
                  'document':{'text':'Синтетический текст для проверки объёма. '*100}}})
        write(data/name,{'version':1,'region':info,'cases':records})
    for name in ['cases_archive.json','cases_bank_archive.json']:
        write(data/name,{'version':1,'region':info,'cases':[]})
    for name in ['cases_bank_events.json','cases_bank_archive_events.json']:
        write(data/name,{'version':1,'events':{}})
    profile=ROOT/'cloudflare-worker/wrangler.toml'
    worker=tomllib.loads(profile.read_text())
    variables=worker['vars'].copy()
    variables['IMPORT_GATEWAY_ORIGIN']='https://api2-hmao.delosud.ru'
    variables['CRON_UTC']=''
    variables['IMPORT_EXECUTOR']='vps'
    if not variables.get('VAPID_PUBLIC_KEY'):
        variables['VAPID_PUBLIC_KEY']=re.search(r'const VAPID_PUBLIC_KEY_DEFAULT = "([^"]+)"',(ROOT/'cloudflare-worker/worker.js').read_text()).group(1)
    courts=[*info.get('fi_courts',[]),*info.get('appeal_courts',[]),*info.get('presidium_courts',[]),info.get('cassation',{})]
    config={'mode':'closed-pilot','region':'hmao','port':18081,'publicOrigin':'https://api-hmao.delosud.ru',
        'database':str(state/'state.sqlite'),'dataIndex':str(state/'api-index.json'),'publicationRoot':str(publication),
        'allowedOrigins':[variables['ALLOWED_ORIGIN'],'https://delosud.ru'],
        'allowedCourts':sorted({c['domain'] for c in courts if c.get('domain')}),'vars':variables,
        'secrets':{key:secrets.token_urlsafe(32) for key in ['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET']}}
    (BASE/'config').mkdir(mode=0o700)
    write(BASE/'config/api.json',config)
    for parent in [state,BASE/'config']:
        for path in [parent,*parent.rglob('*')]:
            os.chown(path,user.pw_uid,user.pw_gid)
            if path.is_dir():path.chmod(0o700)
    print(json.dumps({'initialized':True,'region':'hmao','synthetic_cases':800,'production_data_copied':False}))

if __name__=='__main__':main()
