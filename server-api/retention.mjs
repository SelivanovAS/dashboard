import {readFile,realpath,readdir,stat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {publishedSnapshot} from './publication.mjs';

// Completed, old private candidates only. Initial, current, unknown and unfinished
// snapshots survive. The one-hour grace exceeds any normal in-flight file read.
export async function pruneSnapshots(runtime,{keep=3,graceMs=3600000,now=Date.now()}={}){
  if(!Number.isInteger(keep)||keep<3||graceMs<3600000)throw Error('Unsafe retention policy');
  const root=runtime.publicationRoot;
  if(!root)return {removed:0};
  const rows=await runtime.store.transaction(async()=>{
    if(await runtime.store.kv.get('server:maintenance')==='1')return null;
    return runtime.store.db.prepare("SELECT id,state FROM jobs").all();
  });
  if(!rows)return {removed:0,fenced:true};
  const terminal=new Set(rows.filter(r=>['done','failed','expired','cancelled'].includes(r.state)).map(r=>r.id));
  const first=await publishedSnapshot(root,runtime.store.region);
  const directory=join(root,'snapshots'),candidates=[];
  if(await realpath(directory)!==directory)throw Error('Unexpected snapshot link');
  for(const name of await readdir(directory)){
    const match=/^([a-f0-9-]{36})-[1-9][0-9]*$/.exec(name);
    if(!match||!terminal.has(match[1])||name===first.version)continue;
    const path=join(directory,name);if(await realpath(path)!==path)throw Error('Unexpected candidate link');
    const info=await stat(path);if(!info.isDirectory())continue;
    const receipt=JSON.parse(await readFile(join(path,'receipt.json'),'utf8'));
    if(receipt.region!==runtime.store.region||receipt.job_id!==match[1]||receipt.version!==name)throw Error('Invalid candidate receipt');
    candidates.push({name,path,mtime:info.mtimeMs});
  }
  candidates.sort((a,b)=>b.mtime-a.mtime);
  let removed=0;
  // Keep current plus at least two completed predecessors, as well as initial.
  for(const candidate of candidates.slice(keep-1)){
    if(now-candidate.mtime<graceMs)continue;
    if((await publishedSnapshot(root,runtime.store.region)).version!==first.version)break;
    await rm(candidate.path,{recursive:true});removed++;
  }
  return {removed};
}
