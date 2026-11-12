import {test} from 'node:test';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
import {bridgeFetch} from '../cloudflare-bridge.mjs';
import {productionTransports} from '../production-transports.mjs';
import {BRIDGE_HEADER,BROKER_PATH} from '../bridge-contract.mjs';
import {fixture,requester} from './helpers.mjs';
import {createRuntime,validateConfig} from '../runtime.mjs';

test('temporary bridge preserves existing client secrets and confines GitHub dispatch',async()=>{
 const names=['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET'];
 const roles=Object.fromEntries(names.map(k=>[k,'server-'+k+'-private-test']));
 const env={...Object.fromEntries(names.map(k=>[k,'old-'+k])),BRIDGE_SECRET:'synthetic-bridge-secret-32-characters',
  BRIDGE_ROLE_SECRETS:JSON.stringify(roles),BRIDGE_MAINTENANCE:'0',SERVER_ORIGIN:'https://monitor.delosud.ru:8443/sbersud-api/hmao/',GH_REPO:'synthetic/hmao',GITHUB_PAT:'private-github-test'};
 const request=new Request('https://api-hmao.delosud.ru/admin?secret='+env.OWNER_SECRET,{headers:{'x-forwarded-host':'evil.invalid'}});
 let calls=0;
 await bridgeFetch(request,env,async(url,options)=>{
  calls++;const target=new URL(url);assert.equal(target.origin,'https://monitor.delosud.ru:8443');
  assert.equal(target.pathname,'/sbersud-api/hmao/admin');assert.equal(target.searchParams.get('secret'),roles.OWNER_SECRET);
  assert.equal(options.headers.get(BRIDGE_HEADER),env.BRIDGE_SECRET);assert.equal(options.headers.get('x-forwarded-host'),null);
  return new Response('ok');
 });
 assert.equal((await bridgeFetch(request,{...env,BRIDGE_MAINTENANCE:'1'},()=>{throw Error('unexpected');})).status,503);
 const broker=job=>new Request('https://api-hmao.delosud.ru'+BROKER_PATH,{method:'POST',headers:{[BRIDGE_HEADER]:env.BRIDGE_SECRET},body:JSON.stringify(job)});
 const job={url:'https://api.github.com/repos/synthetic/hmao/actions/workflows/update_cases.yml/dispatches',method:'POST',body:JSON.stringify({ref:'main',inputs:{smart_skip:'true'}})};
 assert.equal((await bridgeFetch(broker({...job,url:job.url.replace('synthetic/hmao','foreign/repository')}),env)).status,403);
 assert.equal((await bridgeFetch(new Request('https://api-hmao.delosud.ru'+BROKER_PATH),env)).status,404);
 const transport=productionTransports({publicOrigin:'https://api-hmao.delosud.ru',vars:{GH_REPO:env.GH_REPO},secrets:{BRIDGE_SECRET:env.BRIDGE_SECRET},executionEnabled:true},
  (url,options)=>bridgeFetch(new Request(url,options),env,async(url,options)=>{
    calls++;assert.equal(url,job.url);assert.equal(options.headers.Authorization,'Bearer '+env.GITHUB_PAT);
    return new Response(null,{status:204});
  }));
 assert.equal((await transport.github(job.url,{method:'POST',body:job.body})).status,204);assert.equal(calls,2);
 assert.equal((await bridgeFetch(new Request('https://court-monitor-trigger.7selivanov-a.workers.dev/profile/get'),env,async()=>new Response('fallback'))).status,200);
 assert.equal((await bridgeFetch(broker({...job,url:'not-a-url'}),env)).status,403);
 await assert.rejects(transport.github('https://evil.invalid/'),/not allowed/);
 const staged=productionTransports({publicOrigin:'https://api-hmao.delosud.ru',vars:{GH_REPO:env.GH_REPO},secrets:{BRIDGE_SECRET:env.BRIDGE_SECRET},executionEnabled:false},()=>{throw Error('unexpected');});
 await assert.rejects(staged.github(job.url,{method:'POST',body:job.body}),/not enabled/);
});

test('existing executor retains its queue/result contract and publication, with no second local writer',async t=>{
 const f=await fixture();f.config.executor='legacy-vps';f.config.executionEnabled=true;
 const runtime=createRuntime(f.config),call=requester(runtime,f.config);
 const old=globalThis.fetch,dispatches=[];
 globalThis.fetch=async(url,options)=>{dispatches.push({url,body:JSON.parse(options.body)});return new Response(null,{status:204});};
 t.after(()=>{globalThis.fetch=old;runtime.close();});
 const accepted=await (await call('/admin/add-case',{role:'OPERATOR_SECRET',body:{court_domain:'surggor--hmao.sudrf.ru',items:['2-432/2026']}})).json();
 assert.equal(accepted.ok,true);assert.equal(runtime.store.db.prepare('SELECT count(*) AS n FROM jobs').get().n,0);
 const queue=await (await call('/admin/import-log?include_queue=1',{role:'OPERATOR_SECRET'})).json();
 assert.ok(queue.queue.some(j=>j.uuid===accepted.key));
 const result=await call('/import-result',{headers:{authorization:'Bearer '+f.config.secrets.PUSH_SECRET},
  body:{job_key:'import:case:'+accepted.key,status:'done',source:'vps',attempt_id:'test-published-commit',added:1}});
 assert.equal(result.status,200);
 assert.ok(!(await (await call('/admin/import-log?include_queue=1',{role:'OPERATOR_SECRET'})).json()).queue.some(j=>j.uuid===accepted.key));
 assert.equal((await call('/admin/dispatch',{role:'OWNER_SECRET',body:{workflow:'update_cases.yml',inputs:{smart_skip:'true'}}})).status,200);
 assert.equal(dispatches.length,0); // HTTP acceptance committed without external dispatch.
 assert.equal((await runtime.dispatches.tick(globalThis.fetch,{enabled:true})).state,'accepted');
 assert.equal(dispatches.length,1);assert.ok(dispatches[0].url.endsWith('/update_cases.yml/dispatches'));
 await call('/admin/dispatch',{role:'OWNER_SECRET',body:{workflow:'update_cases.yml',inputs:{smart_skip:'true'}}});
 assert.equal((await runtime.dispatches.tick(async()=>{throw Error('lost reply');},{enabled:true})).state,'uncertain');
 assert.equal(await runtime.dispatches.tick(()=>{throw Error('must not repeat');},{enabled:true}),null);
 const repeated=await call('/admin/dispatch',{role:'OWNER_SECRET',body:{workflow:'update_cases.yml',inputs:{smart_skip:'true'}}});
 assert.equal(repeated.status,500);assert.match((await repeated.json()).error,/не подтверждён/);
 const production={...f.config,mode:'production',region:'hmao',port:18181,
  database:'/srv/projects/sbersud/api/hmao/state/state.sqlite',dataIndex:'/srv/projects/sbersud/api/hmao/state/api-index.json',secrets:{...f.config.secrets,BRIDGE_SECRET:'synthetic-bridge-secret-32-characters'}};
 assert.doesNotThrow(()=>validateConfig(production));
 assert.throws(()=>validateConfig({...production,database:'/opt/court-monitor/state.sqlite'}),/границы/);
});

test('prepared Cloudflare configuration parses and preserves existing regional bindings',()=>{
 const script=fileURLToPath(new URL('../prepare-transition.py',import.meta.url));
 const check=spawnSync(process.env.TEST_PYTHON||'python3',['-c',`
import runpy,tomllib,sys
from pathlib import Path
script=Path(sys.argv[1]);root=script.parents[1]
source=root/'cloudflare-worker/wrangler.toml'
if not source.exists():source=root/'deployment/regions/hmao/files/cloudflare-worker/wrangler.toml'
original=tomllib.loads(source.read_text())
new=tomllib.loads(runpy.run_path(str(script))['bridge_toml'](source.read_text()))
assert new['kv_namespaces']==original['kv_namespaces']
assert new['routes']==original['routes'] and new['workers_dev'] is True
assert new['vars']['BRIDGE_MAINTENANCE']=='1'
assert all(new['vars'][k]==v for k,v in original['vars'].items())
`,script],{encoding:'utf8'});
 assert.equal(check.status,0,check.stderr);
});
