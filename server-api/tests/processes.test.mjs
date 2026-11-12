import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { writeFile,symlink } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { request as httpRequest } from 'node:http';
import { Store } from '../store.mjs';
import { fixture } from './helpers.mjs';

const serverScript=fileURLToPath(new URL('../server.mjs',import.meta.url));
async function start(f,script=serverScript) {
  const path=join(f.root,'config.json');await writeFile(path,JSON.stringify(f.config),{mode:0o600});
  const child=spawn(process.execPath,[script,path],{stdio:['ignore','pipe','pipe']});
  let logs='';child.stderr.on('data',b=>{logs+=b;});
  const address=await new Promise((resolve,reject)=>{
    let output='';const timeout=setTimeout(()=>{child.kill();reject(Error('start timeout'));},10000);
    child.stdout.on('data',b=>{output+=b;if(output.includes('\n')){
      clearTimeout(timeout);try{resolve(JSON.parse(output.split('\n')[0]));}catch(e){reject(e);}
    }});
    child.once('exit',()=>{clearTimeout(timeout);reject(Error('Server failed: '+logs));});
  });
  const call=(path,body,headers={})=>new Promise((resolve,reject)=>{
    const req=httpRequest({hostname:'127.0.0.1',port:address.port,path,
      method:body===undefined?'GET':'POST',headers:{host:new URL(f.config.publicOrigin).host,'content-type':'application/json',...headers}},res=>{
        const parts=[];res.on('data',b=>parts.push(b));res.on('end',()=>resolve(new Response(Buffer.concat(parts),{status:res.statusCode,headers:res.headers})));
      });
    req.once('error',reject);req.end(body===undefined?undefined:JSON.stringify(body));
  });
  return {child,call,stop:async(signal='SIGTERM')=>{const stopped=once(child,'exit');child.kill(signal);await stopped;}};
}

test('server starts through an immutable release symlink',async t=>{
  const f=await fixture();const link=join(f.root,'server.mjs');await symlink(serverScript,link);
  const process=await start(f,link);t.after(()=>process.stop());
  assert.equal((await process.call('/data/nonexistent')).status,404);
});

test('another process opens an existing store and waits asynchronously for the writer',async()=>{
  const f=await fixture();const first=new Store(f.config.database,'hmao');
  const script=`import {Store} from ${JSON.stringify(new URL('../store.mjs',import.meta.url).href)};
    const s=new Store(process.argv[1],'hmao');process.stdout.write('opened\\n');
    await s.transaction(()=>s.kv.put('runner:concurrent','accepted'));s.close();`;
  await first.transaction(async()=>{
    await first.kv.put('api:transaction','saved');
    const child=spawn(process.execPath,['--input-type=module','-e',script,f.config.database],{stdio:['ignore','pipe','pipe']});
    let error='';child.stderr.on('data',b=>error+=b);
    f.completion=new Promise((resolve,reject)=>child.on('exit',code=>code===0?resolve():reject(Error(error))));
    await Promise.race([once(child.stdout,'data'),f.completion.then(()=>{throw Error('writer was not held');})]);
    await new Promise(resolve=>setTimeout(resolve,1300));
  });
  await f.completion;
  await first.transaction(async()=>assert.equal(await first.kv.get('runner:concurrent'),'accepted'));
  first.close();
});

test('four real processes isolate profiles/secrets and survive SIGKILL without losing accepted UUID',async t=>{
  const fixtures=await Promise.all(['hmao','sverdlovsk_yanao','bashkortostan','tyumen'].map(r=>fixture(r)));
  const processes=await Promise.all(fixtures.map(f=>start(f)));
  t.after(async()=>{for(const p of processes) if(p.child.exitCode===null && p.child.signalCode===null) await p.stop();});
  const tokens=await Promise.all(processes.map(p=>p.call('/profile/calendar-token',{watchlist:[]}).then(r=>r.json())));
  for(let i=0;i<4;i++){
    assert.equal((await processes[i].call('/profile/get',{profile_id:tokens[i].profile_id})).status,200);
    for(let j=0;j<4;j++)if(j!==i){
      assert.equal((await processes[i].call('/profile/get',{profile_id:tokens[j].profile_id})).status,404);
      assert.equal((await processes[i].call(`/admin?secret=${fixtures[j].config.secrets.OWNER_SECRET}`)).status,401);
      assert.equal((await processes[i].call(tokens[j].path)).status,404);
    }
  }
  const path=`/admin/add-case?secret=${fixtures[0].config.secrets.OWNER_SECRET}`;
  const body={items:['2-789/2026'],court_domain:'surggor--hmao.sudrf.ru'};
  const accepted=await (await processes[0].call(path,body)).json();
  await processes[0].stop('SIGKILL');processes[0]=await start(fixtures[0]);
  const retried=await (await processes[0].call(path,body)).json();assert.equal(retried.key,accepted.key);
  assert.equal((await processes[0].call(tokens[0].path)).status,200);
  const log=await (await processes[0].call(`/admin/import-log?include_queue=1&secret=${fixtures[0].config.secrets.OWNER_SECRET}`)).json();
  assert.equal(log.queue.length,1);assert.equal(log.queue[0].uuid,accepted.key);
  assert.equal((await processes[0].call('/admin',undefined,{host:'api-ural.delosud.ru'})).status,421);
});

test('SIGKILL inside multi-key transaction leaves no partial one-time code or calendar state',async()=>{
  const f=await fixture();
  const storeUrl=new URL('../store.mjs',import.meta.url).href;
  const script=`import {Store} from ${JSON.stringify(storeUrl)};
    const s=new Store(${JSON.stringify(f.config.database)},'hmao');
    await s.transaction(async()=>{
      await s.kv.put('paircode:123456','half');
      await s.kv.put('calfeed:partial','half');
      process.stdout.write('uncommitted\\n');
      await new Promise(()=>setInterval(()=>{},1000));
    });`;
  const child=spawn(process.execPath,['--input-type=module','-e',script],{stdio:['ignore','pipe','ignore']});
  await once(child.stdout,'data');const stopped=once(child,'exit');child.kill('SIGKILL');await stopped;
  const store=new Store(f.config.database,'hmao');
  await store.transaction(async()=>{
    assert.equal(await store.kv.get('paircode:123456'),null);
    assert.equal(await store.kv.get('calfeed:partial'),null);
  });store.close();
});
