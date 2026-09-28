"""Исполняемые проверки бейджей и дат, а не снимок боевой картотеки."""
import json
import subprocess
from pathlib import Path

from scripts.tests.test_frontend_drawer_dates import _fn_src, _run_node

ROOT=Path(__file__).resolve().parents[2]


def test_dated_return_and_refusal_shared_by_all_views():
    result=_run_node("""
process.stdout.write(JSON.stringify({
 returned:stageResolvedDate('fi',{result:'Заявление ВОЗВРАЩЕНО',termination_date:'22.09.2026',event_date:'23.09.2026'}),
 noGuess:stageResolvedDate('fi',{result:'Заявление ВОЗВРАЩЕНО',event_date:'23.09.2026'}),
 refusal:stageResolvedDate('cs',{decision_date:'11.08.2026',review_date:'11.08.2026'}),
 refusalResult:normalizeResult('Отказ в передаче')
}));""")
    assert result=={'returned':'2026-09-22','noGuess':'','refusal':'2026-08-11','refusalResult':'no_transfer'}


def test_badges_use_current_complaint_and_manual_review():
    code='\n'.join(_fn_src(name) for name in ('pendingAppealBadge','currentComplaintBlocksArchive'))
    code+='''
const base={stage:'cassation_pending',fiCassationFiled:true};
const out={};
for(const state of ['active','historical','completed','resolving','needs_review']){
 const c={...base,complaintTracking:{cassation:{state}}};
 out[state]=[pendingAppealBadge(c),currentComplaintBlocksArchive(c)];
}
process.stdout.write(JSON.stringify(out));
'''
    r=subprocess.run(['node','-e',code],check=True,capture_output=True,text=True)
    values=json.loads(r.stdout)
    assert 'Обжалуется' in values['active'][0]
    assert values['historical']==values['completed']==['',False]
    assert 'Результат уточняется' in values['resolving'][0]
    assert 'Нужна проверка' in values['needs_review'][0] and values['needs_review'][1]


def test_owner_and_operator_accept_presidium_link_only_in_its_section():
    script=r'''
const fs=require('fs'),vm=require('vm');
const src=fs.readFileSync('cloudflare-worker/admin_page.js','utf8').replace('export function renderAdminHtml','function renderAdminHtml');
const module={};vm.runInNewContext(src+'\nthis.renderAdminHtml=renderAdminHtml;',module);
for(const role of ['owner','operator']){
 const html=module.renderAdminHtml('local-test',role,{siteBase:'http://127.0.0.1'});
 const scripts=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n');
 const names=['canonSudrfHost','acCheckLink'];
 const functions=names.map(n=>{const m=scripts.match(new RegExp('function '+n+'\\([^]*?\\n\\}')); if(!m)throw Error(n);return m[0];}).join('\n');
 const context={URL,acRegion:{presidium_courts:[{domain:'oblsud--hmao.sudrf.ru',delo_id:2800001}],appeal_courts:[{domain:'oblsud--hmao.sudrf.ru'}],fi_courts:[]}};
 vm.runInNewContext(functions+'\nthis.check=acCheckLink;',context);
 for(const domain of ['oblsud.hmao.sudrf.ru','oblsud--hmao.sudrf.ru']){
  const url='https://'+domain+'/modules.php?name_op=case&case_id=27000814&case_uid=aa-bb&delo_id=2800001';
  if(context.check(url)!=='')throw Error(role+': rejected presidium');
  if(context.check(url.replace('2800001','5'))==='')throw Error('accepted appeal');
  if(context.check(url.replace('2800001','28000012'))==='')throw Error('accepted wrong section');
  if(context.check(url.replace('&case_uid=aa-bb',''))==='')throw Error('accepted incomplete card');
 }
}
'''
    subprocess.run(['node','-e',script],cwd=ROOT,check=True,capture_output=True,text=True)
