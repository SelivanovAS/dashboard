import { spawn } from 'node:child_process';
import { readFile, realpath, lstat, mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { atomicJson } from './data-index.mjs';
import {validMarker} from './pilot-guard.mjs';

// Domain parsers and import commands are the existing production implementation.
// The marker selects recorded HTTP or the guarded closed-pilot court transport.
export function isolatedImportAdapter(root,region,python='python3',workflows=['import_cases.yml','add_cases.yml']) {
  const script=fileURLToPath(new URL('./isolated_import.py',import.meta.url));
  const attemptOf=job=>JSON.parse(job.inputs).attempt;
  async function check() {
    const resolved=await realpath(root);
    if(root!==resolved)throw Error('Нужна отдельная копия без ссылок');
    const marker=JSON.parse(await readFile(join(root,'.sbersud-isolated.json'),'utf8'));
    if(!validMarker(marker,root,region))throw Error('Нет изолированной копии или закрытого пилота');
    await mkdir(join(root,'jobs'),{recursive:true,mode:0o700});
    if((await lstat(join(root,'jobs'))).isSymbolicLink())throw Error('Ссылка вместо каталога');
  }
  async function invoke(command,id,attempt) {
    await check();
    if(!/^[0-9a-f-]{36}$/.test(id)||!Number.isSafeInteger(attempt)||attempt<1)throw Error('Неверная попытка');
    return new Promise((resolve,reject)=>{
      const child=spawn(python,[script,command,root,region,id,String(attempt),process.execPath],{
        env:{PATH:process.env.PATH,LANG:'en_US.UTF-8',PYTHONDONTWRITEBYTECODE:'1'},stdio:['ignore','pipe','pipe']});
      let output='',diagnostic='';
      child.stdout.on('data',b=>{output+=b;if(output.length>4*1024*1024)child.kill();});
      child.stderr.on('data',b=>{diagnostic=(diagnostic+b).slice(-65536);});
      child.once('error',reject);
      child.once('close',async code=>{
        if(code!==0){
          // Private diagnostic artifact, never journal stderr or an HTTP response.
          await writeFile(join(root,'jobs',`${id}-${attempt}-${crypto.randomUUID()}.error.txt`),diagnostic,
            {mode:0o600,flag:'wx'}).catch(()=>{});
          reject(Error('Изолированный импорт остановлен; проверьте закрытый журнал задания'));
        }
        else{try{resolve(JSON.parse(output.trim().split('\n').at(-1)));}catch(e){reject(e);}}
      });
    });
  }
  async function apply(job) {
    await check();
    const attempt=attemptOf(job), path=join(root,'jobs',`${job.id}-${attempt}.json`);
    const payload={id:job.id,attempt,workflow:job.workflow,inputs:JSON.parse(job.inputs),body:job.body};
    let previous;
    try{previous=JSON.parse(await readFile(path,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    if(previous){
      if(previous.id!==job.id||previous.attempt!==attempt||previous.workflow!==job.workflow
        ||job.body!=null&&previous.body!==job.body)throw Error('Сохранённое задание отличается');
    }else{
      if(typeof job.body!=='string')throw Error('Тело задания истекло до сохранения');
      await atomicJson(path,payload);
    }
    return invoke('apply',job.id,attempt);
  }
  return {publicationRoot:root,workflows,apply,recover:apply,
    initialize:()=>invoke('initialize','00000000-0000-0000-0000-000000000000',1),
    inspect:job=>invoke('inspect',job.id,attemptOf(job)),
    publish:receipt=>invoke('publish',receipt.job_id,receipt.attempt)};
}
