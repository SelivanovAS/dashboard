// Bounded load against the already running closed pilot. No court requests or sends.
import {readFile,writeFile} from 'node:fs/promises';
import {request} from 'node:http';
import {setTimeout as sleep} from 'node:timers/promises';
import {loadConfig} from './server.mjs';
import {validateConfig} from './runtime.mjs';
import {publishedSnapshot} from './publication.mjs';
import {join} from 'node:path';

const c=validateConfig(await loadConfig(process.argv[2]));
if(c.mode!=='closed-pilot')throw Error('Нужен закрытый пилот');
const call=(path,body)=>new Promise((resolve,reject)=>{
  const start=performance.now();
  const req=request({host:'127.0.0.1',port:18081,path,method:body?'POST':'GET',
    headers:{host:'api-hmao.delosud.ru','content-type':'application/json','accept-encoding':'gzip'}},res=>{
    let data='',bytes=0;res.on('data',b=>{bytes+=b.length;if(!path.startsWith('/data/'))data+=b;});
    res.on('end',()=>resolve({status:res.statusCode,ms:performance.now()-start,data,bytes}));
  });req.setTimeout(10000,()=>req.destroy(Error('timeout')));req.on('error',reject);req.end(body?JSON.stringify(body):undefined);
});
const profiles=[];
const publication=await publishedSnapshot(c.publicationRoot,c.region);
const index=JSON.parse(await readFile(join(publication.directory,'api-index.json'),'utf8'));
const watchlist=index.main.cases.slice(0,10).map(c=>c.id);
for(let i=0;i<24;i++){
  const r=await call('/profile/calendar-token',{watchlist});
  if(r.status!==200)throw Error('Не создан тестовый профиль');profiles.push(JSON.parse(r.data));
}
const report={author:'Александр Селиванов',region:'hmao',cases:[index.main.cases.length,index.bank.cases.length],synthetic_profiles:profiles.length,
  started_at:new Date().toISOString(),external_requests:0,stages:[]};
for(const stage of [{rate:8,seconds:30},{rate:24,seconds:60}]){
  const results=[],running=new Set(),statuses={};let peak=0,errors=0,bytes=0;
  const start=performance.now();
  for(let i=0;i<stage.rate*stage.seconds;i++){
    const delay=start+i*1000/stage.rate-performance.now();if(delay>0)await sleep(delay);
    if(running.size>=8)await Promise.race(running);
    const profile=profiles[i%profiles.length];
    const task=(async()=>{
      try{
        const r=i%10===0?await call('/data/cases.json'):i%2?await call(profile.path):await call('/profile/get',{profile_id:profile.profile_id});
        results.push(r.ms);bytes+=r.bytes;statuses[r.status]=(statuses[r.status]||0)+1;if(r.status!==200)errors++;
      }catch(e){errors++;const code=e.code||'transport_error';statuses[code]=(statuses[code]||0)+1;}
    })();running.add(task);peak=Math.max(peak,running.size);task.finally(()=>running.delete(task));
  }
  await Promise.all(running);results.sort((a,b)=>a-b);
  report.stages.push({...stage,requests:stage.rate*stage.seconds,errors,bytes,statuses,peak_concurrency:peak,
    elapsed_ms:performance.now()-start,p50_ms:results[Math.floor(results.length*.5)],p95_ms:results[Math.floor(results.length*.95)],max_ms:results.at(-1)});
}
report.completed_at=new Date().toISOString();
await writeFile(process.argv[3],JSON.stringify(report,null,2)+'\n',{mode:0o600});
process.stdout.write(JSON.stringify(report)+'\n');
if(report.stages.some(s=>s.errors))process.exitCode=1;
