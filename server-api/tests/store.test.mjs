import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Store } from '../store.mjs';

test('KV: binary values, metadata, absolute expiry, pagination, region binding and restore',async () => {
  const root=await mkdtemp(join(tmpdir(),'sbersud-store-')); let now=1900000000000;
  const a=new Store(join(root,'hmao.sqlite'),'hmao',{clock:()=>now});
  const b=new Store(join(root,'ural.sqlite'),'sverdlovsk_yanao',{clock:()=>now});
  await a.transaction(async()=>{
    await a.kv.put('binary',new Uint8Array([0,255,1]),{metadata:{a:1},expirationTtl:60});
    for(let i=0;i<1005;i++) await a.kv.put(`profile:${String(i).padStart(4,'0')}`,'{}');
    let p=await a.kv.list({prefix:'profile:',limit:999});
    assert.equal(p.keys.length,999);assert.equal(p.list_complete,false);
    const next=await a.kv.list({prefix:'profile:',cursor:p.cursor});assert.equal(next.keys.length,6);
    await b.transaction(async()=>{
      assert.equal(await b.kv.get('binary'),null);
    });
    assert.deepEqual([...new Uint8Array(await a.kv.get('binary','arrayBuffer'))],[0,255,1]);
    assert.deepEqual((await a.kv.getWithMetadata('binary','arrayBuffer')).metadata,{a:1});
  });
  await assert.rejects(b.transaction(()=>b.kv.list({cursor:Buffer.from(JSON.stringify({region:'hmao',prefix:'',after:''})).toString('base64url')})));
  const snapshot=await a.snapshot(); now+=61000;
  const restored=new Store(join(root,'restored.sqlite'),'hmao',{clock:()=>now});
  assert.equal((await restored.restore(snapshot)).expired,1);
  assert.deepEqual(await restored.snapshot(),snapshot);
  await restored.transaction(async()=>assert.equal(await restored.kv.get('binary'),null));
  await assert.rejects(b.restore(snapshot));
  assert.throws(()=>new Store(join(root,'hmao.sqlite'),'tyumen'));
  await assert.rejects(restored.restore(snapshot));
  assert.equal((await stat(join(root,'hmao.sqlite'))).mode & 0o777,0o600);
  a.close();b.close();restored.close();
});

test('whole operation rollback survives caught storage errors and concurrent requests',async()=>{
  const store=new Store(':memory:','hmao');
  const results=await Promise.all(Array.from({length:30},()=>store.transaction(async()=>{
    const value=Number(await store.kv.get('counter')||0);await Promise.resolve();await store.kv.put('counter',String(value+1));
  })));
  await store.transaction(async()=>assert.equal(await store.kv.get('counter'),'30'));
  await assert.rejects(store.transaction(async()=>{
    await store.kv.put('first','written');
    try {await store.kv.put('bad','x',{expirationTtl:1});}catch{}
  }));
  await store.transaction(async()=>assert.equal(await store.kv.get('first'),null));
  await store.transaction(async()=>{await store.kv.put('first','written');return new Response('error',{status:500});});
  await store.transaction(async()=>assert.equal(await store.kv.get('first'),null));
  store.close();
});

test('SQLite online backup restores committed state and passes integrity check',async()=>{
  const root=await mkdtemp(join(tmpdir(),'sbersud-backup-'));
  const store=new Store(join(root,'source.sqlite'),'hmao');
  await store.transaction(()=>store.kv.put('saved','yes'));
  await store.backup(join(root,'backup.sqlite'));
  const copy=new Store(join(root,'backup.sqlite'),'hmao');
  assert.deepEqual(await copy.snapshot(),await store.snapshot());
  copy.close();store.close();
});
