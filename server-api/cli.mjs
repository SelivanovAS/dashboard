import { pathToFileURL } from 'node:url';
import { loadConfig } from './server.mjs';
import { createRuntime } from './runtime.mjs';
import { buildIndex } from './data-index.mjs';
import { runOne,resumeJob } from './runner.mjs';
import { isolatedWritAdapter } from './isolated-writ.mjs';

// No scheduler is installed or started by this CLI.
const [command,configPath,arg] = process.argv.slice(2);
const config=await loadConfig(configPath);
if(command==='index'){
  process.stdout.write(JSON.stringify(await buildIndex({dataDir:arg,output:config.dataIndex,region:config.region}))+'\n');
}else{
  console.log=()=>{};console.warn=()=>{};console.error=()=>{};
  const runtime=createRuntime(config);
  try{
    let result;
    if(command==='cleanup')result=await runtime.cleanup();
    else if(command==='reconcile')result={imported_jobs:await runtime.jobs.reconcile()};
    else if(command==='fence')result=await runtime.store.transaction(async()=>{
      await runtime.store.kv.put('server:maintenance','1');return {fenced:true};
    });
    else if(command==='writ-once')result=await runOne(runtime,isolatedWritAdapter(arg,config.region,process.env.TEST_PYTHON||'python3'));
    else if(command==='writ-resume'){
      const rows=runtime.store.db.prepare("SELECT * FROM jobs WHERE workflow='mark_writ.yml' AND state IN ('leased','uncertain','applied','published') ORDER BY created_at").all();
      result=[];
      for(const row of rows)result.push(await resumeJob(runtime,isolatedWritAdapter(arg,config.region,process.env.TEST_PYTHON||'python3'),{
        ...row,state:row.state==='leased'?'uncertain':row.state}));
    }else throw Error('Команды: index, cleanup, reconcile, fence, writ-once, writ-resume');
    process.stdout.write(JSON.stringify(result)+'\n');
  }finally{runtime.close();}
}
