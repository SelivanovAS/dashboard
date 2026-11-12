import { pathToFileURL } from 'node:url';
import { loadConfig } from './server.mjs';
import { createRuntime } from './runtime.mjs';
import { buildIndex } from './data-index.mjs';
import { runOne,resumeJob,tick } from './runner.mjs';
import { isolatedWritAdapter } from './isolated-writ.mjs';
import {isolatedImportAdapter} from './isolated-import.mjs';
import {pruneSnapshots} from './retention.mjs';

// No scheduler is installed or started by this CLI.
const [command,configPath,arg] = process.argv.slice(2);
const config=await loadConfig(configPath);
if(command==='index'){
  process.stdout.write(JSON.stringify(await buildIndex({dataDir:arg,output:config.dataIndex,region:config.region}))+'\n');
}else{
  console.log=()=>{};console.warn=()=>{};console.error=()=>{};
  globalThis.fetch=async()=>{throw Error('External fetch disabled in pilot CLI');};
  const runtime=createRuntime(config);
  try{
    let result;
    if(command==='queue-tick'){
      result=await tick(runtime,isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3',
        ['import_cases.yml','add_cases.yml','mark_writ.yml','update_cases.yml']));
      if(!result?.fenced)await pruneSnapshots(runtime);
    }
    else if(command==='parse-enqueue')result=await runtime.store.transaction(()=>runtime.jobs.enqueueParse());
    else if(command==='cleanup')result=await runtime.cleanup();
    else if(command==='reconcile')result={imported_jobs:await runtime.jobs.reconcile()};
    else if(command==='fence')result=await runtime.store.transaction(async()=>{
      await runtime.store.kv.put('server:maintenance','1');return {fenced:true};
    });
    else if(command==='publication-init'){
      if(arg!==config.publicationRoot)throw Error('API читает другую публикацию');
      result=await isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3').initialize();
    }
    else if(command==='writ-once')result=await runOne(runtime,config.publicationRoot
      ?isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3',['mark_writ.yml'])
      :isolatedWritAdapter(arg,config.region,process.env.TEST_PYTHON||'python3'));
    else if(command==='import-once')result=await runOne(runtime,isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3'));
    else if(['writ-resume','import-resume'].includes(command)){
      const adapter=command==='writ-resume'?(config.publicationRoot
        ?isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3',['mark_writ.yml'])
        :isolatedWritAdapter(arg,config.region,process.env.TEST_PYTHON||'python3'))
        :isolatedImportAdapter(arg,config.region,process.env.TEST_PYTHON||'python3');
      const job=await runtime.jobs.recoverNext({workflows:adapter.workflows});
      result=job?await resumeJob(runtime,adapter,job):null;
    }else throw Error('Команды: index, cleanup, reconcile, fence, publication-init, writ-once, writ-resume, import-once, import-resume');
    process.stdout.write(JSON.stringify(result)+'\n');
  }finally{runtime.close();}
}
