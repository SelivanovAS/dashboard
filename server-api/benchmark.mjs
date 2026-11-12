import { spawn,execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { request } from 'node:http';
import { fixture } from './tests/helpers.mjs';

const report={author:'Александр Селиванов',schema:1,synthetic:true,host:process.platform,node:process.version,
  started_at:new Date().toISOString(),regions:[],external_requests:0};
const processes=[];
try{
  for(const [region,count] of [['hmao',400],['sverdlovsk_yanao',8000],['bashkortostan',500],['tyumen',400]]){
    const bankCount=region==='sverdlovsk_yanao'?4000:100;
    const f=await fixture(region,{count,bankCount});const path=join(f.root,'config.json');
    await writeFile(path,JSON.stringify(f.config),{mode:0o600});
    const child=spawn(process.execPath,[fileURLToPath(new URL('./server.mjs',import.meta.url)),path],{stdio:['ignore','pipe','ignore']});
    processes.push(child);
    const address=await new Promise((resolve,reject)=>{
      let output='';child.stdout.on('data',b=>{output+=b;if(output.includes('\n'))resolve(JSON.parse(output.split('\n')[0]));});
      child.once('exit',()=>reject(Error('benchmark server exited')));
    });
    const call=(path,body)=>new Promise((resolve,reject)=>{
      const req=request({hostname:'127.0.0.1',port:address.port,path,method:body?'POST':'GET',
        headers:{host:new URL(f.config.publicOrigin).host,'content-type':'application/json'}},res=>{
        const parts=[];res.on('data',b=>parts.push(b));res.on('end',()=>resolve({status:res.statusCode,text:Buffer.concat(parts).toString()}));
      });req.once('error',reject);req.end(body?JSON.stringify(body):undefined);
    });
    const watchlist=[...Array.from({length:10},(_,i)=>`2-${100+i}/2026`),
      ...Array.from({length:10},(_,i)=>`surggor--hmao.sudrf.ru|2-${90000+i}/2026`)];
    const token=JSON.parse((await call('/profile/calendar-token',{watchlist})).text);
    report.regions.push({region,cases:count,...f.size,pid:child.pid,call,token,timings:[],errors:0});
  }
  const rss=()=>report.regions.map(r=>Number(execFileSync('ps',['-o','rss=','-p',String(r.pid)],{encoding:'utf8'}).trim())*1024);
  report.rss_before=rss();
  const started=performance.now();
  await Promise.all(report.regions.map(async r=>{
    for(let batch=0;batch<10;batch++)await Promise.all(Array.from({length:8},async(_,i)=>{
      const start=performance.now();
      const response=i%2?await r.call(r.token.path):await r.call('/profile/get',{profile_id:r.token.profile_id});
      r.timings.push(performance.now()-start);if(response.status!==200)r.errors++;
    }));
  }));
  report.elapsed_ms=performance.now()-started;report.rss_after=rss();
  for(const r of report.regions){
    r.timings.sort((a,b)=>a-b);r.requests=r.timings.length;r.p50_ms=r.timings[Math.floor(r.requests*.5)];
    r.p95_ms=r.timings[Math.floor(r.requests*.95)];r.max_ms=r.timings.at(-1);
    delete r.call;delete r.token;delete r.timings;delete r.pid;
  }
  report.completed_at=new Date().toISOString();
  if(report.regions.some(r=>r.errors))throw Error('benchmark errors');
  if(!process.argv[2]?.startsWith('/'))throw Error('Нужен абсолютный путь отчёта');
  await writeFile(process.argv[2],JSON.stringify(report,null,2)+'\n',{mode:0o600});
  process.stdout.write(JSON.stringify(report,null,2)+'\n');
}finally{
  for(const child of processes)if(child.exitCode===null&&child.signalCode===null){
    const stopped=once(child,'exit');child.kill('SIGTERM');await stopped;
  }
}
