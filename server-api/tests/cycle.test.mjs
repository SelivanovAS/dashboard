import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,readFile,realpath} from 'node:fs/promises';
import {join} from 'node:path';
import {fixture} from './helpers.mjs';
import {createRuntime} from '../runtime.mjs';
import {atomicJson} from '../data-index.mjs';
import {isolatedImportAdapter} from '../isolated-import.mjs';
import {tick} from '../runner.mjs';

test('normal parser completes on a separate candidate without delivery, then publishes atomically',async t=>{
  const f=await fixture('hmao',{count:0});f.root=await realpath(f.root);
  const root=join(f.root,'publication'),data=join(root,'snapshots/initial/data');
  await mkdir(data,{recursive:true});
  await atomicJson(join(root,'.sbersud-isolated.json'),{region:'hmao',synthetic:true});
  await atomicJson(join(root,'current.json'),{version:'initial'});
  await atomicJson(join(root,'responses.json'),{region:'hmao',synthetic:true,responses:{}});
  for(const name of ['cases.json','cases_bank.json','cases_archive.json','cases_bank_archive.json'])
    await atomicJson(join(data,name),{region:f.main.region,version:1,cases:[]});
  f.config.publicationRoot=root;
  const runtime=createRuntime(f.config);t.after(()=>runtime.close());
  const adapter=isolatedImportAdapter(root,'hmao',process.env.TEST_PYTHON||'python3',['update_cases.yml']);
  await adapter.initialize();
  const job=await runtime.store.transaction(()=>runtime.jobs.enqueueParse({ignore_calendar:true}));
  assert.equal((await runtime.store.transaction(()=>runtime.jobs.enqueueParse())).key,job.key);
  const result=await tick(runtime,adapter);assert.equal(result.state,'done');
  const receipt=JSON.parse(await readFile(join(root,'snapshots',job.key+'-1','receipt.json')));
  assert.equal(receipt.summary.parse_completed,true);
  assert.equal(receipt.summary.delivery_enabled,false);
  assert.equal(receipt.summary.models_enabled,false);
  const transport=JSON.parse(await readFile(join(root,'snapshots',job.key+'-1','transport.json')));
  assert.equal(transport.external_requests,0);
  const response=await runtime.fetch(new Request(f.config.publicOrigin+'/data/cases.json'));
  assert.equal(response.status,200);
  assert.equal((await response.json()).region.code,'hmao');
  assert.equal((await runtime.store.transaction(()=>runtime.jobs.runs())).main_run.conclusion,'success');
  assert.equal(await tick(runtime,adapter),null);
  const submit=()=>runtime.fetch(new Request(f.config.publicOrigin+'/admin/dispatch?secret='+f.config.secrets.OWNER_SECRET,
    {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({workflow:'update_cases.yml',inputs:{ignore_calendar:'true'}})})).then(r=>r.json());
  const first=await submit(),repeated=await submit();
  assert.equal(first.ok,true);assert.deepEqual(first,repeated);
  assert.equal((await tick(runtime,adapter)).state,'done');
  await submit();
  assert.equal(runtime.store.db.prepare("SELECT count(*) AS n FROM jobs WHERE state='queued'").get().n,1);
});
