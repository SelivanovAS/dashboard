"""Суды другого пояса: исходные часы, календарь и изоляция нового фронта."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which('node')


def fn(src, name):
    m = re.search(r'(?:async )?function\s+' + re.escape(name) + r'\s*\([^\n]*\)[\s\S]*?\n\}', src)
    assert m, name
    return m.group(0)


def node(script, tz='UTC'):
    if not NODE:
        pytest.skip('node недоступен')
    result = subprocess.run([NODE, '-e', script], text=True, capture_output=True,
                            env={**os.environ, 'TZ': tz})
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize('namespace, copied', [('ural', True), ('bashkortostan', False), ('new-region', False)])
def test_only_legacy_ural_migrates_hmao_browser_data(namespace, copied):
    src = (ROOT / 'app.js').read_text().split('const STORAGE_KEY=', 1)[0]
    result = node('''
const store = new Map([['sber-court-sheet-url','https://hmao.example/cases.json'],['watchlist_v1','["2-1/2026"]']]);
const localStorage={getItem:k=>store.has(k)?store.get(k):null,setItem:(k,v)=>store.set(k,v)};
const window={REGION_FRONT:{STORAGE_NS:''' + json.dumps(namespace) + '''}};
''' + src + '''
console.log(JSON.stringify({source:store.get(window.REGION_FRONT.STORAGE_NS+':sber-court-sheet-url')||null,
  original:store.get('sber-court-sheet-url'),watch:store.get(window.REGION_FRONT.STORAGE_NS+':watchlist_v1')||null}));
''')
    assert result['original'] == 'https://hmao.example/cases.json'
    assert bool(result['source']) is copied
    assert bool(result['watch']) is copied


def frontend_bundle():
    src = (ROOT / 'app.js').read_text()
    return '\n'.join(fn(src, n) for n in ('hearingTimezone', 'hearingZoneLabel', 'hearingUtcMs', 'dayDiff'))


@pytest.mark.parametrize('browser_timezone', ['UTC', 'America/Los_Angeles', 'Asia/Tokyo'])
def test_hearing_time_does_not_depend_on_browser_timezone(browser_timezone):
    result = node(frontend_bundle() + '''
const window={REGION_INFO:{timezone:'Asia/Yekaterinburg',cassation:{domain:'6kas.sudrf.ru',timezone:'Europe/Samara'}}};
const c={stage:'cassation',_cs:{court_domain:'6kas.sudrf.ru',hearing_time:'09:30'}};
Date.now=()=>Date.parse('2026-09-10T19:30:00Z'); // Уже 11-е в Уфе, ещё 10-е в Самаре.
console.log(JSON.stringify({tz:hearingTimezone(c),label:hearingZoneLabel(c),
 instant:new Date(hearingUtcMs('2026-09-11',c._cs.hearing_time,hearingTimezone(c))).toISOString(),
 days:dayDiff('2026-09-11',hearingTimezone(c)),localDays:dayDiff('2026-09-11'),raw:c._cs.hearing_time}));
''', browser_timezone)
    assert result == {'tz': 'Europe/Samara', 'label': 'Самара, UTC+4',
                      'instant': '2026-09-11T05:30:00.000Z', 'days': 1, 'localDays': 0, 'raw': '09:30'}


def test_explicit_block_timezone_wins_and_other_stages_inherit_region():
    result = node(frontend_bundle() + '''
const window={REGION_INFO:{timezone:'Asia/Yekaterinburg',cassation:{domain:'6kas.sudrf.ru',timezone:'Europe/Samara'}}};
const c={stage:'cassation',_fi:{},_cs:{court_domain:'6kas.sudrf.ru',timezone:'Europe/Moscow'}};
console.log(JSON.stringify([hearingTimezone(c),hearingTimezone(c,'fi'),hearingZoneLabel(c,'fi')]));
''')
    assert result == ['Europe/Moscow', 'Asia/Yekaterinburg', '']


def ics_bundle():
    src = (ROOT / 'cloudflare-worker/worker.js').read_text()
    names = ['calDateLocal','calTimeLocal','calTodayYmd','calCourtMeta','calHearingTimezone',
             'calLocalUtcMs','calSelectHearing','calHearingPlace','calBuildCourtLink',
             'calDtstampUtc','buildVevent','buildIcs','icsEscape','icsFold','wnBareCaseNumber']
    return '\n'.join(fn(src, n) for n in names) + '''
function calTzid(){return 'Asia/Yekaterinburg';}
function siteBaseUrl(){return 'https://example.test/dashboard-bashkortostan';}
'''


def test_calendar_keeps_samara_and_ufa_local_times_as_distinct_instants():
    result = node(ics_bundle() + '''
const region={timezone:'Asia/Yekaterinburg',cassation:{domain:'6kas.sudrf.ru',timezone:'Europe/Samara',delo_id:2800001,new:2800001}};
const c={id:'2-11/2026',current_stage:'cassation',cassation:{case_number:'8Г-11/2026',court_domain:'6kas.sudrf.ru',hearing_date:'11.09.2026',hearing_time:'09:30',link:'11|abc-def'}};
const u={id:'2-12/2026',current_stage:'first_instance',first_instance:{case_number:'2-12/2026',hearing_date:'11.09.2026',hearing_time:'09:30'}};
const cs=calSelectHearing(c,region),fi=calSelectHearing(u,region);
const events=[...buildVevent(cs,c,'bash.test',calTzid()),...buildVevent(fi,u,'bash.test',calTzid())];
const ics=buildIcs(events,calTzid(),'Мои заседания');
console.log(JSON.stringify({starts:events.filter(x=>x.startsWith('DTSTART:')),link:calBuildCourtLink(cs),
 samaraToday:calTodayYmd(Date.parse('2026-09-10T19:30Z'),cs.timezone),ufaToday:calTodayYmd(Date.parse('2026-09-10T19:30Z'),fi.timezone),
 falseZone:ics.includes('VTIMEZONE'),raw:c.cassation.hearing_time}));
''')
    assert result['starts'] == ['DTSTART:20260911T053000Z', 'DTSTART:20260911T043000Z']
    assert result['samaraToday'] == '20260910' and result['ufaToday'] == '20260911'
    assert not result['falseZone'] and result['raw'] == '09:30'
    assert 'delo_id=2800001&new=2800001' in result['link']


def test_calendar_event_end_crosses_midnight_correctly():
    result = node(ics_bundle() + '''
const c={id:'2-1',current_stage:'cassation',cassation:{case_number:'8Г-1',timezone:'Europe/Samara',hearing_date:'30.09.2026',hearing_time:'23:30'}};
const ev=buildVevent(calSelectHearing(c),c,'h',calTzid());
console.log(JSON.stringify(ev.filter(x=>x.startsWith('DTSTART:')||x.startsWith('DTEND:'))));
''')
    assert result == ['DTSTART:20260930T193000Z', 'DTEND:20260930T203000Z']


def test_cassation_digest_labels_court_timezone_without_changing_hours(monkeypatch):
    from court_monitor.regions import get_region
    from court_monitor.digest import template
    r = get_region('hmao')
    r = replace(r, cassation_court=replace(r.cassation_court, domain='6kas.sudrf.ru', timezone='Europe/Samara'))
    monkeypatch.setattr(template, 'get_region', lambda: r)
    change = {'case':'2-1/2026','cassation_internal_number':'8Г-1/2026','type':['cass_hearing_scheduled'],
              'details':{'hearing_date':'11.09.2026','hearing_time':'09:30','court_domain':'6kas.sudrf.ru'}}
    html = template.generate_template_digest([], [], cass_changes=[change])
    assert '11.09.2026 в 09:30</b> (Самара, UTC+4)' in html
    assert '10:30' not in html
    change['details']['hearing_time'] = ''
    html = template.generate_template_digest([], [], cass_changes=[change])
    assert 'Самара, UTC+4' not in html


@pytest.mark.parametrize('role', ['owner', 'operator'])
def test_rendered_admin_lists_ksou_separately_from_presidium(tmp_path, role):
    if not NODE:
        pytest.skip('node недоступен')
    module = tmp_path / 'admin.mjs'
    module.write_text((ROOT / 'cloudflare-worker/admin_page.js').read_text())
    result = subprocess.run([NODE, '--input-type=module', '-e',
        'import {renderAdminHtml} from ' + json.dumps(module.as_uri()) + ';\n'
        'console.log(JSON.stringify(renderAdminHtml("dummy",' + json.dumps(role) + ',{})));'],
        capture_output=True, text=True, check=True)
    html = json.loads(result.stdout)
    script = next(s for s in re.findall(r'<script>([\s\S]*?)</script>', html) if 'function loadImportCourts' in s)
    names = ['loadImportCourts','impCourtKey','impCourtLabel','impDomainOf','impCourtLink',
             'canonSudrfHost','acCheckLink','impIsPresidium','impIsCassation','impIsAppeal','impDisplayResult','impUnread','impResultParts','impVerdict']
    bundle = '\n'.join(fn(script, n) for n in names)
    result = node('''
const elements = new Map();
const document={getElementById:k=>{if(!elements.has(k))elements.set(k,{style:{},innerHTML:'',value:''});return elements.get(k);},
 querySelector:k=>({style:{},classList:{add(){}}})};
let acRegion=null,impCourts=[],impAppealDomains={},impPresidiumByDomain={},impCourtNameByDomain={};
const CASES_URL='https://test.invalid/data/cases.json';
const region={fi_courts:[],appeal_courts:[],cassation:{name:'Шестой КСОЮ',domain:'6kas.sudrf.ru',srv_num:1,delo_id:2800001,new:2800001,search_gated:true},
 presidium_courts:[{name:'Суд региона',domain:'oblsud.sudrf.ru',srv_num:1,delo_id:2800001,new:2800001,search_gated:true}]};
const fetch=async()=>({ok:true,json:async()=>({region})});
const impHideAlert=()=>{},wwSetRegionCourts=()=>{},acFillCourts=()=>{},acUpdateState=()=>{},syncImportCourtLink=()=>{},loadImportLog=async()=>[],impShowAlert=x=>{throw Error(x);};
const escHtml=s=>String(s),nPlural=(n,a,b,c)=>n+' '+c,impRetryPromise=()=> 'сервер повторит';
''' + bundle + '''
(async()=>{
await loadImportCourts();
const ks=impCourts.find(c=>c.domain==='6kas.sudrf.ru');
const cs={court_domain:'6kas.sudrf.ru',section:'cassation',fetch_fail:2,needs_review:1,skipped_region:3};
console.log(JSON.stringify({count:impCourts.length,label:impCourtLabel(ks),pinned:ks.pinned,link:impCourtLink(impCourtKey(ks)),
 pres:impIsPresidium(cs),oldPres:impIsPresidium({court_domain:'oblsud.sudrf.ru',section:'cassation'}),
 hint:acCheckLink('https://6kas.sudrf.ru/modules.php?name=sud_delo&name_op=case&case_id=1'),
 parts:impResultParts(cs),verdict:impVerdict(cs)}));
})().catch(e=>{console.error(e);process.exit(1)});
''')
    assert result['count'] == 2 and result['pinned']
    assert result['label'] == 'Шестой КСОЮ — кассация'
    assert not result['pres'] and result['oldPres']
    assert 'delo_id=2800001' in result['link'] and 'new=2800001' in result['link']
    assert 'поиск суда закрыт' in result['hint'] and 'автоматически' not in result['hint']
    assert 'дел кассации не заведено' in ' '.join(result['parts']['problems'])
    assert 'дел другого региона' in ' '.join(result['parts']['skipped'])
    assert result['verdict']['kind'] == 'bad' and 'проверка' in result['verdict']['text']


@pytest.mark.parametrize('role', ['owner', 'operator'])
def test_bashkortostan_manual_history_offers_all_courts_but_only_ksou_is_due(tmp_path, role):
    from court_monitor.regions import get_region
    if not NODE:
        pytest.skip('node недоступен')
    module = tmp_path / 'admin.mjs'
    module.write_text((ROOT / 'cloudflare-worker/admin_page.js').read_text())
    rendered = subprocess.run([NODE, '--input-type=module', '-e',
        'import {renderAdminHtml} from ' + json.dumps(module.as_uri()) + ';\n'
        'console.log(JSON.stringify(renderAdminHtml("dummy",' + json.dumps(role) + ',{})));'],
        capture_output=True, text=True, check=True)
    html = json.loads(rendered.stdout)
    script = next(s for s in re.findall(r'<script>([\s\S]*?)</script>', html) if 'function loadImportCourts' in s)
    bundle = '\n'.join(fn(script, n) for n in ['loadImportCourts','impCourtKey','impCourtLabel',
        'impDomainOf','impCourtLink','syncImportCourtLink','renderImportFreshness',
        'impRecordDeloId','impSectionKey','canonSudrfHost'])
    region = get_region('bashkortostan').public_info()
    result = node('''
const elements=new Map(),tiles={};let freshRows=[];
const document={getElementById:k=>{if(!elements.has(k))elements.set(k,{style:{},innerHTML:'',value:''});return elements.get(k);},
  querySelector:k=>({style:{},classList:{add(){}}})};
let acRegion=null,impCourts=[],impAppealDomains={},impPresidiumByDomain={},impCourtNameByDomain={};
const CASES_URL='https://test.invalid/cases.json',region=''' + json.dumps(region) + ''';
const fetch=async()=>({ok:true,json:async()=>({region})});
const impHideAlert=()=>{},wwSetRegionCourts=()=>{},acFillCourts=()=>{},acUpdateState=()=>{},loadImportLog=async()=>[],impShowAlert=x=>{throw Error(x);};
const escHtml=String,parseIso=Date.parse,collectCardTrouble=()=>{},myCourts=()=>({}),myCourtsCount=()=>0,renderMyBar=()=>{};
const impCardTrouble={},IMP_FRESH_WARN_DAYS=7,IMP_FRESH_STALE_DAYS=14,impMyEdit=false,impFreshAutoPicked=true;
const setTile=(k,...v)=>{tiles[k]=v;},freshList=rows=>{freshRows=rows;return '';};
''' + bundle + '''
(async()=>{
await loadImportCourts();renderImportFreshness([],{});
const sel=document.getElementById('imp-court');
sel.value=impCourtKey(impCourts[0]);syncImportCourtLink();
const closedStep=document.getElementById('imp-search-step').textContent;
const ap=impCourts.find(c=>c.section==='appeal');sel.value=impCourtKey(ap);syncImportCourtLink();
console.log(JSON.stringify({count:impCourts.length,options:(sel.innerHTML.match(/<option/g)||[]).length,
 first:impCourts[0].domain,firstPinned:impCourts[0].pinned,fi:impCourts.filter(c=>!c.section).length,
 label:impCourtLabel(ap),openLink:document.getElementById('imp-court-link').href,
 closedStep,openStep:document.getElementById('imp-search-step').textContent,
 regular:freshRows.map(x=>x.court.domain),tile:tiles.import,availableCount:document.getElementById('imp-court-count').textContent,
 hint:document.getElementById('imp-mode-hint').textContent}));
})().catch(e=>{console.error(e);process.exit(1)});
''')
    assert result['count'] == result['options'] == 48 and result['fi'] == 45
    assert result['first'] == '6kas.sudrf.ru' and result['firstPinned']
    assert result['regular'] == ['6kas.sudrf.ru'] and result['availableCount'] == '48'
    assert result['tile'][1] == '1 просрочено' and 'из 1 судов' in result['tile'][2]
    assert result['label'].endswith(' — апелляция') and 'delo_id=5' in result['openLink']
    assert 'проверочный код' in result['closedStep'] and 'проверочный код' not in result['openStep']
    assert 'Историю любого суда' in result['hint']


def test_worker_preserves_cassation_review_counters_and_kind():
    src = (ROOT / 'cloudflare-worker/worker.js').read_text()
    section_ids = re.search(r'const IMPORT_SECTION_DELO_IDS\s*=\s*\{[^}]+\};', src)
    assert section_ids, 'Не найдены идентификаторы разделов настоящего Worker'
    counters = re.search(r'const IMPORT_RESULT_COUNTERS\s*=\s*\[[\s\S]*?\];', src)
    assert counters
    result = node(section_ids.group(0) + '\n' + counters.group(0) + '\n' + '\n'.join(fn(src, name) for name in (
        'importQueuePending', 'importLogWriteOptions', 'listImportLogKeys', 'handleImportResult',
        'importSectionIdentity', 'detectDumpCardDeloIds', 'canonSudrfHost', 'importAttemptApply'
    )) + '''
const IMPORT_LOG_TTL=100;
const importChannelAuthOk=()=>true;
const uuid='11111111-1111-1111-1111-111111111111';
const key='import:log:stamp|'+uuid;
const data=new Map([[key,JSON.stringify({court_domain:'6kas.sudrf.ru',kind:'dump'})]]);
const env={PUSH_SUBSCRIPTIONS:{list:async()=>({keys:[{name:key}]}),get:async k=>data.get(k),put:async(k,v)=>data.set(k,v)}};
(async()=>{
const response=await handleImportResult(new Request('https://test.invalid/import-result',{method:'POST',body:JSON.stringify({dump_key:'import:dump:'+uuid,status:'done',section:'cassation',cassation_kind:'court',needs_review:2,skipped_region:3,fetch_fail:1})}),env);
const stored=JSON.parse(data.get(key));
console.log(JSON.stringify({status:response.status,kind:stored.cassation_kind,review:stored.needs_review,other:stored.skipped_region,section:stored.section,fresh:[...data.keys()].some(k=>k.startsWith('import:last:'))}));
})().catch(e=>{console.error(e);process.exit(1)});
''')
    assert result == {'status':200,'kind':'court','review':2,'other':3,'section':'cassation','fresh':False}


def test_legacy_presidium_and_appeal_on_same_domain_use_their_own_section():
    from court_monitor.regions import get_region
    region = get_region('hmao').public_info()
    # Разные метаданные времени дополнительно доказывают выбор по инстанции.
    region['presidium_courts'][0]['timezone'] = 'Europe/Moscow'
    result = node(ics_bundle() + frontend_bundle() + '''
const region=''' + json.dumps(region) + ''';
const window={REGION_INFO:region};
const block={court_domain:'oblsud--hmao.sudrf.ru',case_number:'44Г-1/2019',
  hearing_date:'11.09.2026',hearing_time:'09:30',link:'11|abc-def'};
const cass={id:'2-1/2019',current_stage:'cassation',cassation:block};
const appeal={id:'2-2/2019',current_stage:'appeal',appeal:{...block,case_number:'33-2/2019'}};
const cs=calSelectHearing(cass,region),ap=calSelectHearing(appeal,region);
console.log(JSON.stringify({cassLink:calBuildCourtLink(cs),appealLink:calBuildCourtLink(ap),
  workerZones:[cs.timezone,ap.timezone],frontendZones:[
    hearingTimezone({stage:'cassation',_cs:block}),hearingTimezone({stage:'appeal',_ap:block})]}));
''')
    assert 'delo_id=2800001&new=2800001' in result['cassLink']
    assert 'delo_id=5&new=5' in result['appealLink']
    assert result['workerZones'] == ['Europe/Moscow', 'Asia/Yekaterinburg']
    assert result['frontendZones'] == result['workerZones']


def test_court_unknown_time_is_all_day_not_midnight():
    result = node(ics_bundle() + '''
const c={id:'2-1',current_stage:'cassation',cassation:{case_number:'8Г-1',timezone:'Europe/Samara',hearing_date:'30.09.2026',hearing_time:'00:00'}};
const ev=buildVevent(calSelectHearing(c),c,'h',calTzid());
console.log(JSON.stringify(ev.filter(x=>x.startsWith('DTSTART')||x.startsWith('DTEND'))));
''')
    assert result == ['DTSTART;VALUE=DATE:20260930','DTEND;VALUE=DATE:20261001']
