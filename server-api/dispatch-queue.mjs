import {hash} from './store.mjs';
import {githubRequestAllowed} from './bridge-contract.mjs';

// Commit acceptance before any external action. An uncertain dispatch is never
// repeated automatically: GitHub does not provide an idempotency key for it.
export class DispatchQueue {
  constructor(store,repository){this.store=store;this.repository=repository;}
  async enqueue(workflow,inputs){
    this.store.assertTransaction();
    const url=`https://api.github.com/repos/${this.repository}/actions/workflows/${workflow}/dispatches`;
    const body=JSON.stringify({ref:'main',inputs});
    if(!githubRequestAllowed(url,'POST',body,this.repository))return {ok:false,error:'invalid_workflow'};
    const fingerprint=hash(url+'|'+body),active='server:dispatch-active:'+fingerprint;
    const previous=await this.store.kv.get(active);
    if(previous){const row=await this.store.kv.get('server:dispatch:'+previous,'json');
      if(row&&['queued','leased','uncertain'].includes(row.state))return {ok:true,queued:true,id:previous};}
    const id=crypto.randomUUID();
    await this.store.kv.put('server:dispatch:'+id,JSON.stringify({id,url,body,workflow,inputs,state:'queued',created_at:Date.now()}),{expirationTtl:72*3600});
    await this.store.kv.put(active,id,{expirationTtl:72*3600});
    return {ok:true,queued:true,id};
  }
  async tick(fetcher,{enabled=false}={}){
    if(!enabled)return {disabled:true};
    const job=await this.store.transaction(async()=>{
      if(await this.store.kv.get('server:maintenance')==='1')return null;
      const rows=[];let cursor='';
      do{const page=await this.store.kv.list({prefix:'server:dispatch:',cursor});
        for(const key of page.keys){const row=await this.store.kv.get(key.name,'json');if(row)rows.push(row);}cursor=page.cursor;
      }while(cursor);
      for(const row of rows.filter(r=>r.state==='leased'&&r.lease_until<Date.now())){
        row.state='uncertain';await this.store.kv.put('server:dispatch:'+row.id,JSON.stringify(row),{expirationTtl:72*3600});
      }
      const row=rows.filter(r=>r.state==='queued').sort((a,b)=>a.created_at-b.created_at)[0];if(!row)return null;
      row.state='leased';row.lease=crypto.randomUUID();row.lease_until=Date.now()+60000;
      await this.store.kv.put('server:dispatch:'+row.id,JSON.stringify(row),{expirationTtl:72*3600});return row;
    });
    if(!job)return null;
    let status,state='uncertain';
    try{const response=await fetcher(job.url,{method:'POST',body:job.body});status=response.status;
      state=status===204?'accepted':status>=400&&status<500?'failed':'uncertain';
    }catch{}
    await this.store.transaction(async()=>{
      const key='server:dispatch:'+job.id,row=await this.store.kv.get(key,'json');
      if(row?.state!=='leased'||row.lease!==job.lease)throw Error('Dispatch lease changed');
      await this.store.kv.put(key,JSON.stringify({...row,state,http_status:status,updated_at:Date.now()}),{expirationTtl:72*3600});
      const bodyKey=job.inputs.job_key||job.inputs.dump_key;
      if(bodyKey){const id=bodyKey.split(':').at(-1);
        for(const log of this.store.db.prepare("SELECT key FROM kv WHERE key LIKE 'import:log:%' AND key LIKE ?").all('%|'+id)){
          const record=await this.store.kv.get(log.key,'json');
          if(!record||record.status==='done')continue;
          await this.store.kv.put(log.key,JSON.stringify({...record,status:state==='accepted'?'dispatched':'failed',
            error:state==='accepted'?'':'Запуск не подтверждён; автоматический повтор остановлен.',updated_at:new Date().toISOString()}),{expirationTtl:90*86400});
        }
      }
    });
    return {id:job.id,state,http_status:status};
  }
}
