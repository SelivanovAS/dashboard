import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, unlink } from 'node:fs/promises';
import worker from '../../cloudflare-worker/worker.js';
import { createRuntime } from '../runtime.mjs';
import { Store,hash } from '../store.mjs';
import { fixture,requester } from './helpers.mjs';

test('HMAO: profiles, pair codes, CAS, calendar UID/timezone, roles, subscriptions and queues',async t=>{
  const f=await fixture();
  f.config.vars.IMPORT_GATEWAY_ORIGIN='https://api2-hmao.delosud.ru';
  const dump={court_domain:'surggor--hmao.sudrf.ru',html:'<html>'+('synthetic '.repeat(180))+'</html>'};
  const gatewayBody=Buffer.from(JSON.stringify(dump));let gatewayCalls=0;
  const runtime=createRuntime(f.config,{gatewayFetch:async(url,options)=>{
    gatewayCalls++;assert.ok(url.startsWith('https://api2-hmao.delosud.ru/_gateway-upload/'));
    assert.equal(options.redirect,'manual');
    return new Response(gatewayBody,{headers:{'content-type':'application/json','content-length':String(gatewayBody.length)}});
  }});const call=requester(runtime,f.config);
  const oldFetch=globalThis.fetch;globalThis.fetch=()=>{throw new Error('Unexpected external fetch');};
  t.after(()=>{globalThis.fetch=oldFetch;runtime.close();});
  assert.throws(()=>createRuntime(f.config),/Один региональный/);
  const issued=await (await call('/profile/link-code',{body:{watchlist:['2-100/2026']}})).json();
  assert.ok(issued.profile_id);assert.match(issued.code,/^\d{6}$/);
  const links=await Promise.all([1,2].map(()=>call('/profile/link',{body:{code:issued.code,watchlist:[]}})));
  assert.deepEqual(links.map(r=>r.status).sort(),[200,404]);
  const profile=await (await call('/profile/get',{body:{profile_id:issued.profile_id}})).json();
  const writes=await Promise.all(['2-100/2026','8Г-100/2026'].map(n=>call('/profile/watchlist',{
    body:{profile_id:issued.profile_id,base_ts:profile.updated_at,watchlist:[n]}})));
  assert.deepEqual(writes.map(r=>r.status).sort(),[200,409]);
  const tokens=await Promise.all([1,2].map(()=>call('/profile/calendar-token',{
    body:{profile_id:issued.profile_id,regenerate:true}}).then(r=>r.json())));
  const feeds=await Promise.all(tokens.map(x=>call(x.path)));
  assert.deepEqual(feeds.map(r=>r.status),[404,200]);
  const ics=await feeds[1].text();assert.match(ics,/UID:2-100\/2026--cassation@api-hmao.delosud.ru/);
  assert.match(ics,/DTSTART:20991225T063000Z/);assert.match(ics,/Зал 1/);
  // Same shared handler, full source documents versus compact index: byte-for-byte ICS.
  const reference=new Store(':memory:','hmao');await reference.restore(await runtime.store.snapshot());
  const referenceEnv={...runtime.env,PUSH_SUBSCRIPTIONS:reference.kv,API_DATA:{
    read:async url=>url.endsWith('cases_bank.json')?f.bank:f.main,aliases:async()=>worker.buildAliasMap(f.main.cases)}};
  const full=await reference.transaction(()=>worker.fetch(new Request(f.config.publicOrigin+tokens[1].path),referenceEnv));
  assert.equal(await full.text(),ics);reference.close();
  // The runtime always resets its own fixed env at the next request.
  assert.equal((await call('/admin/data')).status,401);
  assert.equal((await call('/admin/data',{role:'OPERATOR_SECRET'})).status,403);
  assert.equal((await call('/admin',{role:'OPERATOR_SECRET'})).status,200);
  assert.equal((await call('/admin',{role:'OWNER_SECRET'})).status,200);
  await runtime.store.transaction(async()=>{
    for(let i=0;i<1005;i++) await runtime.store.kv.put(`sub:${String(i).padStart(4,'0')}`,
      JSON.stringify({endpoint:`https://example.invalid/push/${i}`,keys:{p256dh:'test',auth:'test'},profile_id:issued.profile_id}));
  });
  const subs=await (await call('/subscriptions',{headers:{authorization:`Bearer ${f.config.secrets.PUSH_SECRET}`}})).json();
  assert.equal(subs.length,1005);
  const data=await (await call('/admin/data',{role:'OWNER_SECRET'})).json();assert.equal(data.subs.length,1005);
  const body={items:['2-123/2026'],court_domain:'surggor--hmao.sudrf.ru',operator:'Тест'};
  const first=await (await call('/admin/add-case',{body,role:'OPERATOR_SECRET'})).json();
  const repeated=await (await call('/admin/add-case',{body,role:'OPERATOR_SECRET'})).json();assert.equal(first.key,repeated.key);
  const withId={body,role:'OWNER_SECRET',headers:{'idempotency-key':'synthetic-request-1'}};
  assert.equal((await call('/admin/add-case',withId)).status,200);
  assert.equal((await call('/admin/add-case',{...withId,body:{...body,operator:'Changed'}})).status,409);
  assert.equal((await call('/admin/add-case',{body:{...body,court_domain:'foreign.sudrf.ru'},role:'OWNER_SECRET'})).status,400);
  assert.equal((await call('/admin/dispatch',{body:{workflow:'test_digest.yml'},role:'OWNER_SECRET'})).status,503);
  assert.equal((await call('/admin/dispatch',{body:{workflow:'test_digest.yml'},role:'OPERATOR_SECRET'})).status,403);
  assert.equal((await call('/admin/test-push',{body:{},role:'OWNER_SECRET'})).status,503);
  assert.equal((await call('/import-result',{body:{job_key:`import:case:${first.key}`,status:'done'},
    headers:{authorization:`Bearer ${f.config.secrets.PUSH_SECRET}`}})).status,409);
  const queue=await (await call('/admin/import-log?include_queue=1',{role:'OPERATOR_SECRET'})).json();assert.equal(queue.queue.length,2);
  const envelope={__gateway_upload:{id:crypto.randomUUID(),token:'a'.repeat(64),bytes:gatewayBody.length,sha256:hash(gatewayBody)}};
  const viaGateway=await call('/admin/import-dump',{body:envelope,role:'OPERATOR_SECRET'});
  assert.equal(viaGateway.status,200);assert.equal(viaGateway.headers.get('x-import-gateway-sha256'),hash(gatewayBody));
  const bad=await call('/admin/import-dump',{body:{__gateway_upload:{...envelope.__gateway_upload,sha256:'b'.repeat(64)}},role:'OPERATOR_SECRET'});
  assert.equal(bad.status,502);assert.equal(gatewayCalls,2);
  const cors=await call('/profile/get',{method:'OPTIONS',headers:{origin:'https://delosud.ru'}});
  assert.equal(cors.headers.get('access-control-allow-origin'),'https://delosud.ru');
  assert.equal((await call('/profile/get',{method:'OPTIONS',headers:{origin:'https://evil.invalid'}})).headers.get('access-control-allow-origin'),'');
  assert.equal((await call('/admin',{headers:{'x-forwarded-host':'api-ural.delosud.ru'}})).status,400);
  assert.equal((await runtime.fetch(new Request('https://api-ural.delosud.ru/admin'))).status,421);
  const inactive=crypto.randomUUID();
  await runtime.store.transaction(async()=>{
    await runtime.store.kv.put(`profile:${inactive}`,JSON.stringify({schema_version:1,watchlist:[],updated_at:1,created_at:'2026-01-01T00:00:00Z'}));
    await runtime.store.kv.put(`profile-activity:${inactive}`,JSON.stringify({tracking_started_at:Date.now()-45*86400000,last_used_at:null,active_until:Date.now()-40*86400000}));
  });
  const clean=await runtime.cleanup();assert.equal(clean.removed,1);assert.equal(clean.protected_by_push,1);
  assert.equal((await call('/profile/get',{body:{profile_id:inactive}})).status,200);
  await runtime.store.transaction(async()=>assert.equal(await runtime.store.kv.get(`profile-removed:${inactive}`),null));
  await unlink(f.config.dataIndex);
  assert.equal((await call(tokens[1].path)).status,503);
});
