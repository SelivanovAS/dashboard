import { spawn } from 'node:child_process';
import { readFile,realpath,lstat } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { atomicJson } from './data-index.mjs';

export function isolatedWritAdapter(root, region, python = 'python3') {
  const script = fileURLToPath(new URL('./isolated_writ.py',import.meta.url));
  const invoke = (command,id) => new Promise((resolve,reject) => {
    const child = spawn(python,[script,command,root,region,id],{
      env:{ PATH:process.env.PATH, LANG:'en_US.UTF-8', PYTHONDONTWRITEBYTECODE:'1' },
      stdio:['ignore','pipe','pipe'],
    });
    let output='';
    child.stdout.on('data',b => { output+=b; if(output.length>1024*1024) child.kill(); });
    child.stderr.resume(); // Python logs may contain case details; never forward them.
    child.once('error',reject);
    child.once('close',code => {
      if(code!==0) reject(new Error('Изолированный обработчик ИЛ не завершился'));
      else { try { resolve(JSON.parse(output.trim().split('\n').at(-1))); } catch(e) { reject(e); } }
    });
  });
  return {
    workflows:['mark_writ.yml'],
    async apply(job) {
      if(!/^[0-9a-f-]{36}$/.test(job.id)) throw new Error('Неверный UUID');
      const resolved=await realpath(root);
      if(resolved.startsWith('/opt/')||resolved.startsWith('/srv/'))throw new Error('Нужна изолированная копия');
      const marker=JSON.parse(await readFile(join(root,'.sbersud-isolated.json'),'utf8'));
      if(marker.region!==region||marker.synthetic!==true)throw new Error('Нет синтетической копии');
      try { if((await lstat(join(root,'jobs'))).isSymbolicLink())throw new Error('Ссылка вместо каталога заданий'); }
      catch(e) { if(e.code!=='ENOENT')throw e; }
      await atomicJson(join(root,'jobs',`${job.id}.json`),JSON.parse(job.body));
      return invoke('apply',job.id);
    },
    inspect: job => invoke('inspect',job.id),
    // Only this adapter can safely repeat an incomplete apply: it modifies a
    // private candidate, while current.json remains unchanged until publish.
    recover: job => invoke('apply',job.id),
    publish: receipt => invoke('publish',receipt.job_id),
  };
}
