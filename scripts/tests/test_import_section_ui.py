"""Разделы одного суда не подтверждают импорт и ошибки карточек друг друга."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Нужен Node для настоящего JS админки")

HARNESS = r'''
import assert from "node:assert/strict";
import vm from "node:vm";
import { renderAdminHtml } from "./admin_page.mjs";

const domain = process.argv[2];
for (const role of ["owner", "operator"]) {
  const html = renderAdminHtml("test-only", role, {});
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
  scripts.forEach(script => new vm.Script(script));
  const source = scripts.join("\n");
  const names = ["impRecordDeloId", "impSectionKey", "impCourtSection", "impCourtKey",
    "impCourtLabel", "impDomainOf", "canonSudrfHost", "collectCardTrouble",
    "renderImportFreshness", "impCacheFreshRecords", "impDetectDeloIds", "impDetectDomains", "impSend",
    "impVerdict", "acResultText"];
  const functions = names.map(name => {
    const found = new RegExp("(?:async )?function " + name + "\\([^]*?\\n\\}").exec(source);
    assert.ok(found, name);
    // impCourtKey/impDomainOf are one-line declarations.
    return found[0].split(/\n(?=\/\/)/)[0];
  }).join("\n");
  const appeal = {name:"Областной суд",domain,delo_id:5,section:"appeal",pinned:true,search_gated:true};
  const presidium = {...appeal,name:"Президиум областного суда",delo_id:2800001,section:"cassation"};
  const district = {name:"Районный суд",domain:"district--tum.sudrf.ru",delo_id:1540005,search_gated:true};
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id,{innerHTML:"",value:"",style:{},className:""});
    return nodes.get(id);
  };
  let rows, fetched=0, sent, status="";
  const context = vm.createContext({
    console, Date, URL, Object, String, Array, Math, Infinity,
    document:{getElementById:node},
    impCourts:[appeal,presidium,district],
    acRegion:{appeal_courts:[appeal],presidium_courts:[presidium],fi_courts:[district]},
    impCardTrouble:{}, impLastSectionFreshMap:{}, impFreshAutoPicked:true, impCourtTouched:false, impMyEdit:false,
    IMP_FRESH_WARN_DAYS:7, IMP_FRESH_STALE_DAYS:14,
    parseIso:Date.parse,myCourts:()=>({}),myCourtsCount:()=>0,setTile:()=>{},
    renderMyBar:value=>{rows=value;},freshList:()=>"",nPlural:()=>"",plural:()=>"",escHtml:String,
    impSending:false,impSelectedFile:null,SECRET:"test-only",impUpdateSendState:()=>{},
    localStorage:{setItem(){}},impSetStatus:value=>{status=value;},impCourtNameByDomain:{},
    fetch:async (_url,options)=>{fetched++;sent=JSON.parse(options.body);return {ok:true,json:async()=>({ok:true,key:"test-job"})};},
    impRememberAccepted:()=>{},impRunDetect:()=>{},
  });
  vm.runInContext(functions,context);
  const now=new Date().toISOString();
  const record=(section,extra={})=>({court_domain:domain,section,status:"done",ts:now,added:1,...extra});
  const render=(items=[],last={},sections={})=>context.renderImportFreshness(items,last,sections);
  const row=delo=>rows.find(r=>String(r.court.delo_id)===String(delo));
  assert.equal(context.impVerdict({needs_review:1}).kind,"bad");
  assert.match(context.acResultText({status:"done",needs_review:1}),/уточнения суда/);
  assert.doesNotMatch(context.impVerdict({skipped_role:1}).text,/всё уже в базе/);

  for (const [section,id,other] of [["appeal",5,2800001],["cassation",2800001,5]]) {
    render([record(section)]);
    assert.equal(row(id).level,0,role+": свой раздел свежий");
    assert.equal(row(other).level,2,role+": соседний раздел не импортирован");
    render([],{[domain]:{...record(section),status:undefined}});
    assert.equal(row(id).level,0,"старый ключ с section сохраняет верную историю");
    assert.equal(row(other).level,2,"старый ключ с section не распространяется на соседний раздел");
    render([],{}, {[domain+":"+id]:record(section,{delo_id:id})});
    assert.equal(row(id).level,0,"новая долговечная отметка");
    assert.equal(row(other).level,2,"новая отметка независима");
    render([record(section,{fetch_fail:1})]);
    assert.equal(row(id).level,2,"непрочитанные карточки не подтверждают свежесть");
    assert.equal(row(id).trouble.unread,1,"ошибка своего раздела");
    assert.equal(row(other).trouble,null,"ошибка не затрагивает соседний раздел");
    render([record(section,{needs_review:1})]);
    assert.equal(row(id).level,2,"неопределённая связь не подтверждает полный импорт");
    context.impCacheFreshRecords([record(section,{needs_review:1})]);
    assert.equal(Object.keys(context.impLastSectionFreshMap).length,0,"незавершённая проверка не попадает в кэш свежести");
  }
  render([],{[domain]:{court_domain:domain,ts:now,added:5}});
  assert.equal(row(5).level,2,"неоднозначная старая отметка не означает апелляцию");
  assert.equal(row(5).uncertain,true,"старый импорт не выдаётся за отсутствие истории");
  assert.equal(row(2800001).level,2,"неоднозначная старая отметка не означает президиум");
  render([{court_domain:domain,status:"done",ts:now}]);
  assert.equal(row(5).level,2,"журнал без раздела тоже не угадывает инстанцию");
  render([],{[district.domain]:{ts:now,added:2}});
  assert.equal(row(1540005).level,0,"старый однозначный районный импорт сохранён");
  render([record("appeal",{kind:"case"})]);
  assert.equal(row(5).level,2,"точечное добавление не подтверждает полный дамп");
  render([record("appeal"),record("cassation")]);
  assert.equal(row(5).level,0); assert.equal(row(2800001).level,0);

  context.impCacheFreshRecords([record("appeal")]);
  render([],{},context.impLastSectionFreshMap);
  assert.equal(row(5).level,0,"новый результат сохраняется в кэше после выхода из журнала");
  assert.equal(row(2800001).level,2);

  // Настоящая функция отправки: выбранная апелляция и кассационный HTML.
  const dump=id=>'<a href="https://'+domain+'/modules.php?name=sud_delo&name_op=case&case_id=1&delo_id='+id+'">дело</a>'+' '.repeat(1100);
  assert.equal(context.impDetectDeloIds('<a href=modules.php?delo_id=2800001&amp;case_id=2>2</a>')[0],"2800001");
  node("imp-court").value=context.impCourtKey(appeal);
  node("imp-name").value="Тест";
  node("imp-paste").innerHTML=dump(2800001);
  await context.impSend();
  assert.equal(fetched,0,"неверный раздел не попадает в очередь");
  assert.match(status,/другого раздела/);
  node("imp-paste").innerHTML=dump(5)+dump(2800001);
  await context.impSend();
  assert.equal(fetched,0,"смешанные разделы тоже отклоняются");
  node("imp-paste").innerHTML=dump(5);
  await context.impSend();
  assert.equal(fetched,1,"правильная апелляция принята");
  assert.equal(sent.delo_id,5); assert.equal(sent.section,"appeal");
  node("imp-court").value=context.impCourtKey(presidium);
  node("imp-paste").innerHTML=dump(2800001);
  await context.impSend();
  assert.equal(fetched,2,"правильный президиум принят");
  assert.equal(sent.delo_id,2800001); assert.equal(sent.section,"cassation");
}
'''


@pytest.mark.parametrize("domain", [
    "oblsud--hmao.sudrf.ru", "oblsud--svd.sudrf.ru", "oblsud--ynao.sudrf.ru",
    "vs--bkr.sudrf.ru", "oblsud--tum.sudrf.ru",
])
def test_sections_stay_independent_in_rendered_admin(tmp_path, domain):
    shutil.copyfile(ROOT / "cloudflare-worker/admin_page.js", tmp_path / "admin_page.mjs")
    harness = tmp_path / "check.mjs"
    harness.write_text(HARNESS)
    result = subprocess.run([NODE, str(harness), domain], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
