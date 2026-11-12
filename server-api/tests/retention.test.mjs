import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,utimes,readdir,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pruneSnapshots} from '../retention.mjs';
import {manifestHash} from '../publication.mjs';

test('retention preserves current, two predecessors, initial, unknown and unfinished snapshots',async()=>{
 const root=await realpath(await mkdtemp(join(tmpdir(),'sbersud-retention-'))),rows=[];
 await mkdir(join(root,'snapshots'));
 await writeFile(join(root,'.sbersud-isolated.json'),JSON.stringify({region:'hmao',synthetic:true}));
 const payload={files:{},index_sha256:'test'},sha256=manifestHash(payload);
 const versions=[];
 for(let i=0;i<7;i++){
  const id=`00000000-0000-0000-0000-${String(i).padStart(12,'0')}`,name=id+'-1',path=join(root,'snapshots',name);
  versions.push(name);rows.push({id,state:i===0?'leased':'done'});
  await mkdir(path);await writeFile(join(path,'receipt.json'),JSON.stringify({...payload,sha256,region:'hmao',job_id:id,version:name,outcome:'done'}));
  await utimes(path,new Date(1000+i),new Date(1000+i));
 }
 for(const name of ['initial','unknown'])await mkdir(join(root,'snapshots',name));
 await writeFile(join(root,'current.json'),JSON.stringify({version:versions[6],sha256}));
 let fenced=true;
 const runtime={publicationRoot:root,store:{region:'hmao',transaction:fn=>fn(),kv:{get:async()=>fenced?'1':null},db:{prepare:()=>({all:()=>rows})}}};
 assert.deepEqual(await pruneSnapshots(runtime),{removed:0,fenced:true});fenced=false;
 assert.equal((await pruneSnapshots(runtime)).removed,3);
 assert.deepEqual((await readdir(join(root,'snapshots'))).sort(),[versions[0],versions[4],versions[5],versions[6],'initial','unknown'].sort());
 assert.equal((await pruneSnapshots(runtime)).removed,0);
});
