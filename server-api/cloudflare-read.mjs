// Explicitly read-only Cloudflare adapter. Credentials never enter logs or URLs.
import {readFile,lstat,open} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {stableNamespaceSnapshot} from './namespace-snapshot.mjs';

export function cloudflareRead({account,namespace,token,fetcher=fetch}) {
  if(!/^[a-f0-9]+$/.test(account)||!/^[a-f0-9]+$/.test(namespace))throw Error('Invalid account/namespace');
  const base=`https://api.cloudflare.com/client/v4/accounts/${account}/storage/kv/namespaces/${namespace}`;
  async function get(path,json=true){
    for(let attempt=0;attempt<4;attempt++){
      const response=await fetcher(base+path,{method:'GET',redirect:'error',
        headers:{Authorization:`Bearer ${token}`},signal:AbortSignal.timeout(30000)});
      if((response.status===429||response.status>=500)&&attempt<3){
        await new Promise(r=>setTimeout(r,1000*(attempt+1)));continue;
      }
      if(!response.ok)throw Error(`Cloudflare read failed: HTTP ${response.status}`);
      if(!json)return response.arrayBuffer();
      const doc=await response.json();if(doc.success!==true)throw Error('Cloudflare read failed');
      return doc;
    }
  }
  return {
    async list({limit=1000,cursor=''}){
      const p=new URLSearchParams({limit:String(limit),...(cursor?{cursor}:{})});
      const doc=await get('/keys?'+p);
      if(!Array.isArray(doc.result))throw Error('Invalid keys response');
      const next=doc.result_info?.cursor||'';
      return {keys:doc.result,cursor:next,list_complete:!next};
    },
    async getWithMetadata(key){
      const encoded=encodeURIComponent(key);
      const [value,metadata]=await Promise.all([get('/values/'+encoded,false),get('/metadata/'+encoded)]);
      return {value,metadata:metadata.result??null};
    }
  };
}

if(import.meta.url===pathToFileURL(process.argv[1]).href){
  const [account,namespace,tokenPath,output]=process.argv.slice(2);
  const stat=await lstat(tokenPath);
  if(!stat.isFile()||stat.isSymbolicLink()||(stat.mode&0o077))throw Error('Token must be a private regular file');
  process.umask(0o077);
  const kv=cloudflareRead({account,namespace,token:(await readFile(tokenPath,'utf8')).trim()});
  let count=0;
  const original=kv.getWithMetadata;
  kv.getWithMetadata=async key=>{
    const value=await original(key);
    if(++count%50===0)process.stderr.write(JSON.stringify({read_values:count})+'\n');
    return value;
  };
  const {snapshot,observed_expired}=await stableNamespaceSnapshot(kv,'hmao');
  const file=await open(output,'wx',0o600);
  try{await file.writeFile(JSON.stringify(snapshot));await file.sync();}finally{await file.close();}
  process.stdout.write(JSON.stringify({region:'hmao',keys:snapshot.entries.length,sha256:snapshot.sha256,
    observed_expired,matching_passes:2,production_fenced:false,purpose:'closed-pilot-rehearsal'})+'\n');
}
