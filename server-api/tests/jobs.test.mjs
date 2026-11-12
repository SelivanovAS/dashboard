import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Store } from '../store.mjs';
import { Jobs } from '../jobs.mjs';
import { runOne,resumeJob,tick } from '../runner.mjs';
import { isolatedWritAdapter } from '../isolated-writ.mjs';
import { createRuntime } from '../runtime.mjs';
import { fixture,requester } from './helpers.mjs';

test('leases, restart, stale runner rejection and publication failure cannot produce done',async()=>{
  const root=await mkdtemp(join(tmpdir(),'sbersud-jobs-'));let now=Date.now();
  let store=new Store(join(root,'db.sqlite'),'hmao',{clock:()=>now});let jobs=new Jobs(store);
  const id=crypto.randomUUID();
  const log=`import:log:${new Date(now).toISOString()}|${id}`;
  await store.transaction(async()=>{
    await store.kv.put(`import:case:${id}`,'{}',{expirationTtl:3600});
    await store.kv.put(log,JSON.stringify({uuid:id,kind:'case',status:'queued',ts:new Date(now).toISOString()}));
    await jobs.enqueueImport('case',id);
  });
  const claimed=await Promise.all([1,2].map(()=>jobs.claim({workflows:['add_cases.yml'],leaseMs:1000})));
  assert.equal(claimed.filter(Boolean).length,1);const job=claimed.find(Boolean);
  store.close();now+=2000;store=new Store(join(root,'db.sqlite'),'hmao',{clock:()=>now});jobs=new Jobs(store);
  assert.equal(await jobs.claim({workflows:['add_cases.yml']}),null);
  assert.equal(store.db.prepare('SELECT state FROM jobs').get().state,'uncertain');
  await assert.rejects(jobs.finish(id,job.generation,()=>new Response('ok')));
  const receipt={region:'hmao',job_id:id,sha256:'a'.repeat(64)};
  await assert.rejects(jobs.applied(id,job.generation+1,receipt));
  await jobs.applied(id,job.generation,receipt);
  await assert.rejects(jobs.published(id,job.generation,{...receipt,sha256:'b'.repeat(64),verified:true}));
  assert.equal(store.db.prepare('SELECT state FROM jobs').get().state,'applied');
  await jobs.published(id,job.generation,{...receipt,verified:true});
  await assert.rejects(jobs.finish(id,job.generation,()=>new Response('fail',{status:500})));
  assert.equal(store.db.prepare('SELECT state FROM jobs').get().state,'published');
  await jobs.finish(id,job.generation,()=>new Response('ok'));
  assert.equal((await jobs.finish(id,job.generation,()=>{throw Error('double');})).duplicate,true);
  store.close();
});

test('real local writ logic preserves bank events, publishes isolated snapshot and resumes after failed publication',async t=>{
  const f=await fixture();const runtime=createRuntime(f.config);const call=requester(runtime,f.config);
  t.after(()=>runtime.close());
  const root=join(f.root,'writ');await mkdir(join(root,'snapshots','initial','data'),{recursive:true});
  await writeFile(join(root,'.sbersud-isolated.json'),JSON.stringify({region:'hmao',synthetic:true}));
  await writeFile(join(root,'current.json'),JSON.stringify({version:'initial'}));
  const data=join(root,'snapshots','initial','data');
  const bankCase={id:'2-100/2026',current_stage:'first_instance',bank_role:'Истец',track:'plaintiff_light',
    first_instance:{case_number:'2-100/2026',court_domain:'surggor--hmao.sudrf.ru',status:'Решено',
      result:'Иск (заявление, жалоба) УДОВЛЕТВОРЕН',decision_date:'05.06.2026',act_date:'10.06.2026'}};
  for(const name of ['cases.json','cases_archive.json','cases_bank.json','cases_bank_archive.json']) {
    await writeFile(join(data,name),JSON.stringify({region:{code:'hmao'},cases:name==='cases_bank.json'?[bankCase]:[]}));
  }
  const events={'surggor--hmao.sudrf.ru|2-100/2026':[{date:'05.06.2026',event:'Сохранить событие'}]};
  await writeFile(join(data,'cases_bank_events.json'),JSON.stringify({version:1,events}));
  await writeFile(join(data,'cases_bank_archive_events.json'),JSON.stringify({version:1,events:{}}));
  const body={action:'set',operator:'Тестовый оператор',items:[{case_id:'2-100/2026',court_domain:'surggor--hmao.sudrf.ru',reason:'debt_paid'}]};
  const response=await call('/admin/writ-waiver',{body,role:'OPERATOR_SECRET'});assert.equal(response.status,200);
  const id=(await response.json()).key;
  const adapter=isolatedWritAdapter(root,'hmao',process.env.TEST_PYTHON||'python3');
  const failedPublish={...adapter,publish:async()=>{throw Error('Simulated publication outage');}};
  await runtime.store.transaction(()=>runtime.store.kv.put('server:maintenance','1'));
  assert.deepEqual(await tick(runtime,adapter),{fenced:true});
  assert.equal(runtime.store.db.prepare('SELECT state FROM jobs WHERE id=?').get(id).state,'queued');
  await runtime.store.transaction(()=>runtime.store.kv.delete('server:maintenance'));
  await assert.rejects(tick(runtime,failedPublish),/publication outage/);
  const row=runtime.store.db.prepare('SELECT * FROM jobs WHERE id=?').get(id);assert.equal(row.state,'applied');
  assert.equal(JSON.parse(await readFile(join(root,'current.json'))).version,'initial');
  await tick(runtime,adapter);
  assert.equal(runtime.store.db.prepare('SELECT state FROM jobs WHERE id=?').get(id).state,'done');
  assert.equal(JSON.parse(await readFile(join(root,'current.json'))).version,id);
  const archive=JSON.parse(await readFile(join(root,'snapshots',id,'data','cases_bank_archive.json')));
  assert.equal(archive.cases[0].first_instance.writ_waived.reason,'debt_paid');
  const archivedEvents=JSON.parse(await readFile(join(root,'snapshots',id,'data','cases_bank_archive_events.json')));
  assert.deepEqual(archivedEvents.events['surggor--hmao.sudrf.ru|2-100/2026'],events['surggor--hmao.sudrf.ru|2-100/2026']);
  const log=await (await call(`/admin/import-log?tracked=${id}`,{role:'OWNER_SECRET'})).json();
  assert.equal(log.tracked[0].status,'done');
  assert.equal((await (await call('/admin/writ-waiver',{body,role:'OPERATOR_SECRET'})).json()).key,id);
  assert.equal(await tick(runtime,adapter),null);
});
