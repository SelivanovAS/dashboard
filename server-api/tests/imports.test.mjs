import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile,cp,realpath} from 'node:fs/promises';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {request as httpRequest} from 'node:http';
import {fileURLToPath} from 'node:url';
import {fixture,requester} from './helpers.mjs';
import {serve} from '../server.mjs';
import {isolatedImportAdapter} from '../isolated-import.mjs';
import {runOne,resumeJob} from '../runner.mjs';
import {atomicJson} from '../data-index.mjs';
import {DataIndex} from '../data-index.mjs';
import worker from '../../cloudflare-worker/worker.js';

const python=process.env.TEST_PYTHON||'python3';
const codeRoot=fileURLToPath(new URL('../../',import.meta.url));
const domain='surggor--hmao.sudrf.ru';
const courtURL=(id,appeal=false)=>`https://${appeal?'oblsud--hmao.sudrf.ru':domain}/modules.php?name=sud_delo&srv_num=1&name_op=case&case_id=${id}&case_uid=bbbb-${id}&delo_id=${appeal?5:1540005}&new=${appeal?5:0}`;
const today=new Date().toLocaleDateString('ru-RU');
function card(number,bank=false) {
  return `<html><body><div>ДЕЛО № ${number}</div><table>
  <tr><td><b>Дата поступления</b></td><td>${today}</td></tr>
  <tr><td><b>Категория дела</b></td><td>Иски о взыскании сумм по кредитному договору</td></tr>
  <tr><td><b>Судья</b></td><td>Тестовый судья</td></tr></table><table>
  <tr><td>Лица, участвующие в деле</td></tr><tr><td>Вид лица</td><td>ФИО</td></tr>
  <tr><td>ИСТЕЦ</td><td>${bank?'ПАО Сбербанк':'Тестовый истец'}</td></tr>
  <tr><td>ОТВЕТЧИК</td><td>${bank?'Тестовый ответчик':'ПАО Сбербанк'}</td></tr></table>
  <table><tr><th>Наименование события</th><th>Дата</th><th>Время</th><th>Место проведения</th><th>Результат</th></tr>
  <tr><td>Судебное заседание</td><td>25.12.2099</td><td>10:30</td><td>Зал 1</td><td></td></tr></table></body></html>`;
}
function dump(id,number) {
  return `<html><body><table><tr><td>№ дела</td><td>Дата поступления</td><td>Категория / Стороны</td><td>Судья</td><td>Дата решения</td><td>Решение</td></tr>
  <tr><td><a href="${courtURL(id)}">${number}</a></td><td>${today}</td>
  <td>КАТЕГОРИЯ: Иски о взыскании сумм по кредитному договору ИСТЕЦ (ЗАЯВИТЕЛЬ): Тестовый истец ОТВЕТЧИК: ПАО Сбербанк</td><td>Тестовый судья</td><td></td><td></td></tr></table></body></html>`;
}
function child(code,args=[]) {
  return new Promise((resolve,reject)=>{
    const p=spawn(process.execPath,['--input-type=module','-e',code,...args],{cwd:codeRoot,
      env:{PATH:process.env.PATH,TEST_PYTHON:python},stdio:['ignore','pipe','pipe']});
    let stdout='',stderr='';p.stdout.on('data',b=>stdout+=b);p.stderr.on('data',b=>stderr+=b);
    p.on('error',reject);p.on('close',(code,signal)=>resolve({code,signal,stdout,stderr}));
  });
}
test('HMAO real import commands, atomic HTTP publication, retries, crash recovery and restored copy',async t=>{
  const f=await fixture('hmao',{count:0});f.root=await realpath(f.root);const root=join(f.root,'publication');
  const initial=join(root,'snapshots','initial','data');await mkdir(initial,{recursive:true});
  await atomicJson(join(root,'.sbersud-isolated.json'),{region:'hmao',synthetic:true});
  await atomicJson(join(root,'current.json'),{version:'initial'});
  for(const name of ['cases.json','cases_archive.json','cases_bank.json','cases_bank_archive.json','cases_archive_2024.json']){
    await atomicJson(join(initial,name),{region:f.main.region,version:1,cases:[]});
  }
  for(const name of ['cases_bank_events.json','cases_bank_archive_events.json'])await atomicJson(join(initial,name),{version:1,events:{}});
  await atomicJson(join(initial,'cases_bank.json'),{region:f.main.region,version:1,cases:[{
    id:'2-999/2026',current_stage:'first_instance',bank_role:'Истец',track:'plaintiff_light',
    first_instance:{case_number:'2-999/2026',court_domain:domain,status:'Решено',
      result:'Иск (заявление, жалоба) УДОВЛЕТВОРЕН',decision_date:'05.06.2026',act_date:'10.06.2026'}}]});
  await atomicJson(join(initial,'cases_bank_events.json'),{version:1,events:{[`${domain}|2-999/2026`]:[{event:'Сохранить событие',date:'05.06.2026'}]}});
  await atomicJson(join(initial,'unrelated.json'),{keep:'unchanged'});
  await atomicJson(join(initial,'.bank_intake_seen.json'),{version:1,seen:{}});
  const recording={region:'hmao',synthetic:true,responses:{}};
  const saveResponses=()=>atomicJson(join(root,'responses.json'),recording);
  await saveResponses();
  f.config.publicationRoot=root;f.config.allowedCourts.push('oblsud--hmao.sudrf.ru');
  const configPath=join(f.root,'config.json');await atomicJson(configPath,f.config);
  const instance=await serve(f.config),runtime=instance.runtime,call=requester(runtime,f.config);
  t.after(()=>instance.close());
  const adapter=isolatedImportAdapter(root,'hmao',python);
  const query=(id)=>runtime.store.db.prepare('SELECT * FROM jobs WHERE id=?').get(id);
  const log=async id=>(await (await call(`/admin/import-log?tracked=${id}`,{role:'OWNER_SECRET'})).json()).tracked[0];
  const httpGet=path=>new Promise((resolve,reject)=>{
    const req=httpRequest({host:'127.0.0.1',port:instance.server.address().port,path,headers:{host:'api-hmao.delosud.ru'}},res=>{
      const chunks=[];res.on('data',b=>chunks.push(b));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks)}));
    });req.on('error',reject);req.end();
  });
  const submit=async(path,body)=>{
    if(body.html)body={...body,html:body.html.padEnd(1600,' ')};
    const response=await call(path,{body,role:'OPERATOR_SECRET'});const value=await response.json();
    assert.equal(response.status,200,JSON.stringify(value));return value.key;
  };
  const publish=async()=>{const result=await runOne(runtime,adapter);assert.ok(result);return result;};

  await t.test('initial publication can be read before the first import',async()=>{
    assert.equal((await httpGet('/data/cases.json')).status,503);
    assert.equal((await adapter.initialize()).initialized,true);
    assert.deepEqual(JSON.parse((await httpGet('/data/cases.json')).body).cases,[]);
    await assert.rejects(adapter.initialize());
  });
  await t.test('dump uses saved court/section; files and index become visible together',async()=>{
    recording.responses[courtURL('911')]={body:card('2-911/2026')};await saveResponses();
    const id=await submit('/admin/import-dump',{court_domain:domain,section:'first_instance',delo_id:'1540005',html:dump('911','2-911/2026'),operator:'Тест'});
    runtime.store.db.prepare("UPDATE jobs SET inputs='{}' WHERE id=?").run(id); // queued by the earlier prototype
    assert.deepEqual(JSON.parse((await httpGet('/data/cases.json')).body).cases,[]);
    assert.equal((await publish()).state,'done');
    const published=await httpGet('/data/cases.json');assert.equal(published.status,200);
    assert.equal(JSON.parse(published.body).cases[0].id,'2-911/2026');
    assert.equal((await log(id)).added,1);
    assert.equal((await log(id)).section,'first_instance');
    assert.equal((await httpGet('/data/../responses.json')).status,404);
    const index=new DataIndex(f.config.dataIndex,'hmao',f.config.vars.CASES_DATA_URL,worker.buildAliasMap,root);
    const loaded=await index.load();assert.equal(loaded.main.cases[0].id,'2-911/2026');
    assert.equal(JSON.parse(await readFile(join(root,'snapshots',`${id}-1`,'data','unrelated.json'))).keep,'unchanged');
  });
  await t.test('batch preserves both tracks and retries only after publishing partial work',async()=>{
    recording.responses[courtURL('921')]={body:card('2-921/2026',true)};await saveResponses();
    const body={items:[courtURL('921'),courtURL('922')],operator:'Тест'};
    const id=await submit('/admin/add-case',body);
    assert.equal((await publish()).state,'retry_wait');
    assert.equal(query(id).state,'retry_wait');
    assert.equal((await log(id)).fetch_error,1);
    assert.ok((await (await call('/admin/import-log?include_queue=1',{role:'OWNER_SECRET'})).json()).queue.some(j=>j.uuid===id));
    assert.equal(JSON.parse((await httpGet('/data/cases_bank.json')).body).cases[0].id,'2-921/2026');
    assert.equal(await runOne(runtime,adapter),null);
    assert.equal(await submit('/admin/add-case',body),id);
    recording.responses[courtURL('922')]={body:card('2-922/2026')};await saveResponses();
    runtime.store.db.prepare('UPDATE jobs SET lease_until=0 WHERE id=?').run(id);
    assert.equal((await publish()).state,'done');
    const report=await log(id);assert.equal(report.fetch_error,0);assert.equal(report.totals.added_bank,1);assert.equal(report.totals.added_main,1);
    const events=JSON.parse((await httpGet('/data/cases_bank_events.json')).body);
    assert.ok(events.events[`${domain}|2-921/2026`].length);
  });
  await t.test('bad dump is terminal and cannot change the published version',async()=>{
    const before=await readFile(join(root,'current.json'),'utf8');
    const id=await submit('/admin/import-dump',{court_domain:domain,html:'<html><body>'+('Нет таблицы выдачи. '.repeat(100))+'</body></html>'});
    assert.equal((await publish()).state,'failed');assert.equal(query(id).state,'failed');
    assert.match((await log(id)).error,/повтор не поможет/);
    assert.ok(!(await (await call('/admin/import-log?include_queue=1',{role:'OWNER_SECRET'})).json()).queue.some(j=>j.uuid===id));
    assert.equal(await readFile(join(root,'current.json'),'utf8'),before);
    assert.equal(await runOne(runtime,adapter),null);
  });
  await t.test('appeal dump publishes its CSV and preserves selected section',async()=>{
    const url=courtURL('931',true);
    recording.responses[url]={body:`<html><body><table id="tablcont"><tr><td><b>Уникальный идентификатор дела</b></td><td>86RS0001-01-2026-009931-01</td></tr><tr><td><b>Номер дела в первой инстанции</b></td><td>2-9931/2026</td></tr><tr><td><b>Судья-докладчик</b></td><td>Тестовый судья</td></tr></table></body></html>`};await saveResponses();
    const html=`<html><body><table><tr><th>Номер дела</th><th>Дата поступления</th><th>Стороны и суд</th><th>Судья</th><th>Дата рассмотрения</th></tr><tr><td><a href="${url}">33-931/2026</a></td><td>${today}</td><td>КАТЕГОРИЯ: Споры, связанные с наследованием ИСТЕЦ(ЗАЯВИТЕЛЬ): Тестовый истец ОТВЕТЧИК: ПАО Сбербанк Суд (мировой судья) первой инстанции: Сургутский городской суд</td><td>Тестовый судья</td><td>25.12.2099</td></tr></table></body></html>`;
    const id=await submit('/admin/import-dump',{court_domain:'oblsud--hmao.sudrf.ru',section:'appeal',delo_id:'5',html});
    assert.equal((await publish()).state,'done');
    const csv=await httpGet('/data/sberbank_cases.csv');assert.equal(csv.status,200);assert.match(csv.body.toString(),/33-931\/2026/);
    assert.equal(csv.body.subarray(0,3).toString('hex'),'efbbbf');
    assert.equal((await log(id)).section,'appeal');
  });
  await t.test('number batch performs real search and card parsing, keeping cold archives',async()=>{
    const number='2-935/2026';
    const search=`https://${domain}/modules.php?name=sud_delo&srv_num=1&name_op=r&delo_id=1540005&case_type=0&new=0&G1_CASE__CASE_NUMBERSS=${encodeURIComponent(number)}&delo_table=g1_case&Submit=%CD%E0%E9%F2%E8`;
    recording.responses[search]={body:dump('935',number)};
    recording.responses[courtURL('935')]={body:card(number)};await saveResponses();
    const id=await submit('/admin/add-case',{court_domain:domain,items:[number]});
    assert.equal((await publish()).state,'done');assert.equal((await log(id)).added_main,1);
    const transport=JSON.parse(await readFile(join(root,'snapshots',`${id}-1`,'transport.json')));
    assert.equal(transport.requests.length,2);assert.equal(transport.external_requests,0);
    assert.deepEqual(JSON.parse((await httpGet('/data/cases_archive_2024.json')).body).cases,[]);
  });
  await t.test('writ action uses the same publication and preserves separate archive events',async()=>{
    const id=await submit('/admin/writ-waiver',{action:'set',items:[{case_id:'2-999/2026',court_domain:domain,reason:'debt_paid'}]});
    const writ=isolatedImportAdapter(root,'hmao',python,['mark_writ.yml']);
    assert.equal((await runOne(runtime,writ)).state,'done');assert.equal((await log(id)).waived,1);
    const archive=JSON.parse((await httpGet('/data/cases_bank_archive.json')).body);
    assert.equal(archive.cases.find(c=>c.id==='2-999/2026').first_instance.writ_waived.reason,'debt_paid');
    const events=JSON.parse((await httpGet('/data/cases_bank_archive_events.json')).body);
    assert.equal(events.events[`${domain}|2-999/2026`][0].event,'Сохранить событие');
  });
  for(const checkpoint of ['apply','publish'])await t.test(`SIGKILL after ${checkpoint} resumes without reapplying or double counts`,async()=>{
    const n=checkpoint==='apply'?'941':'942';recording.responses[courtURL(n)]={body:card(`2-${n}/2026`)};await saveResponses();
    const id=await submit('/admin/add-case',{items:[courtURL(n)]});
    const result=await child(`import {readFile} from 'node:fs/promises';import {createRuntime} from './server-api/runtime.mjs';import {isolatedImportAdapter} from './server-api/isolated-import.mjs';
      const c=JSON.parse(await readFile(process.argv[1]));const r=createRuntime(c);const a=isolatedImportAdapter(c.publicationRoot,c.region,process.env.TEST_PYTHON);
      const j=await r.jobs.claim({workflows:a.workflows});const receipt=await a.apply(j);
      if(process.argv[2]==='publish'){await r.jobs.applied(j.id,j.generation,receipt);await a.publish(receipt);}
      process.kill(process.pid,'SIGKILL');`,[configPath,checkpoint]);
    assert.equal(result.signal,'SIGKILL',result.stderr);
    assert.notEqual(query(id).state,'done');
    if(checkpoint==='apply')await runtime.store.transaction(()=>runtime.store.kv.delete(query(id).body_key));
    assert.equal(await runtime.jobs.recoverNext({workflows:adapter.workflows}),null);
    runtime.store.db.prepare('UPDATE jobs SET lease_until=0 WHERE id=?').run(id);
    const recovering=await runtime.jobs.recoverNext({workflows:adapter.workflows});
    assert.equal(await runtime.jobs.recoverNext({workflows:adapter.workflows}),null);
    assert.equal(await runOne(runtime,adapter),null);
    const receiptPath=join(root,'snapshots',`${id}-1`,'receipt.json');const receipt=await readFile(receiptPath,'utf8');
    await assert.rejects(runtime.jobs.applied(id,recovering.generation-1,JSON.parse(receipt)),/Устаревший/);
    assert.equal((await resumeJob(runtime,adapter,recovering)).state,'done');
    assert.equal(await readFile(receiptPath,'utf8'),receipt);
    assert.equal(JSON.parse((await httpGet('/data/cases.json')).body).cases.filter(c=>c.id===`2-${n}/2026`).length,1);
    assert.equal((await log(id)).totals.added_main,1);
  });
  await t.test('unpublished damaged candidate is rebuilt; changed base or corrupted publication fails closed',async()=>{
    recording.responses[courtURL('951')]={body:card('2-951/2026')};await saveResponses();
    const id=await submit('/admin/add-case',{items:[courtURL('951')]});
    const job=await runtime.jobs.claim({workflows:adapter.workflows});
    const pending=join(root,'snapshots',`${id}-1`);await mkdir(join(pending,'data'),{recursive:true});
    await writeFile(join(pending,'data','cases.json'),'{truncated');
    await runtime.jobs.interrupted(id,job.generation);
    const recovering=await runtime.jobs.recoverNext({workflows:adapter.workflows});
    assert.equal((await resumeJob(runtime,adapter,recovering)).state,'done');
    const file=join(pending,'data','cases.json'),original=await readFile(file);
    await writeFile(file,'{}');assert.equal((await httpGet('/data/cases.json')).status,503);
    await writeFile(file,original);
    assert.equal((await httpGet('/data/cases.json')).status,200);
    recording.responses[courtURL('952')]={body:card('2-952/2026')};await saveResponses();
    const next=await submit('/admin/add-case',{items:[courtURL('952')]});
    const baseFile=join(pending,'data','unrelated.json'),base=await readFile(baseFile);
    await assert.rejects(runOne(runtime,{...adapter,publish:async receipt=>{
      await writeFile(baseFile,JSON.stringify({changed:'concurrent writer'}));return adapter.publish(receipt);
    }}));
    assert.equal(query(next).state,'applied');
    assert.equal(JSON.parse(await readFile(join(root,'current.json'))).version,`${id}-1`);
    await writeFile(baseFile,base);
    const resumed=await runtime.jobs.recoverNext({workflows:adapter.workflows});
    assert.equal((await resumeJob(runtime,adapter,resumed)).state,'done');
  });
  await t.test('restore database plus publication tree, then finish saved candidate in a fresh process',async()=>{
    recording.responses[courtURL('961')]={body:card('2-961/2026')};await saveResponses();
    const id=await submit('/admin/add-case',{items:[courtURL('961')]});
    await assert.rejects(runOne(runtime,{...adapter,publish:async()=>{throw Error('publication outage');}}),/publication outage/);
    const backup=join(f.root,'restored');await mkdir(backup);
    await cp(root,join(backup,'publication'),{recursive:true});
    const backupDb=join(backup,'state.sqlite');await runtime.store.backup(backupDb);
    const config={...f.config,database:backupDb,publicationRoot:join(backup,'publication')};
    const path=join(backup,'config.json');await atomicJson(path,config);
    const result=await child(`import {readFile} from 'node:fs/promises';import {createRuntime} from './server-api/runtime.mjs';import {isolatedImportAdapter} from './server-api/isolated-import.mjs';import {resumeJob} from './server-api/runner.mjs';
      const c=JSON.parse(await readFile(process.argv[1]));const r=createRuntime(c),a=isolatedImportAdapter(c.publicationRoot,c.region,process.env.TEST_PYTHON);
      const j=await r.jobs.recoverNext({workflows:a.workflows});const result=await resumeJob(r,a,j);
      const response=await r.fetch(new Request(c.publicOrigin+'/data/cases.json'));const data=await response.json();
      process.stdout.write(JSON.stringify({result,status:response.status,count:data.cases.filter(x=>x.id==='2-961/2026').length})+'\\n');r.close();`,[path]);
    assert.equal(result.code,0,result.stderr);const restored=JSON.parse(result.stdout.trim().split('\n').at(-1));
    assert.equal(restored.result.state,'done');assert.equal(restored.status,200);assert.equal(restored.count,1);
    assert.equal(query(id).state,'applied'); // original was not rolled back or changed by restore
    const recovering=await runtime.jobs.recoverNext({workflows:adapter.workflows});await resumeJob(runtime,adapter,recovering);
  });
});
