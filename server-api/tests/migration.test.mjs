import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store } from '../store.mjs';
import { collectNamespace,stableNamespaceSnapshot } from '../namespace-snapshot.mjs';

test('full namespace export includes unknown keys and empty intermediate pages; missing values fail closed',async()=>{
  const source=new Store(':memory:','hmao');const target=new Store(':memory:','hmao');
  await source.transaction(async()=>{
    for(let i=0;i<1003;i++) await source.kv.put(`unknown:${i}`,'payload',{metadata:{index:i},expirationTtl:600});
    const {snapshot}=await stableNamespaceSnapshot(source.kv,'hmao');
    assert.equal(snapshot.entries.length,1003);
    await target.restore(snapshot);
    assert.equal((await target.snapshot()).sha256,snapshot.sha256);
  });
  let calls=0;
  const emptyPage={list:async()=>++calls===1?{keys:[],list_complete:false,cursor:'continue'}:{keys:[{name:'x'}],list_complete:true},
    getWithMetadata:async()=>({value:new TextEncoder().encode('kept').buffer,metadata:null})};
  assert.equal((await collectNamespace(emptyPage,'hmao')).snapshot.entries.length,1);
  const bad={list:async()=>({keys:[{name:'x'}],list_complete:true}),getWithMetadata:async()=>({value:null})};
  await assert.rejects(collectNamespace(bad,'hmao'),/Непросроченное/);
  let count=0;
  await assert.rejects(stableNamespaceSnapshot({...bad,getWithMetadata:async()=>({value:new TextEncoder().encode(String(++count)).buffer,metadata:null})},'hmao'),/различаются/);
  source.close();target.close();
});
