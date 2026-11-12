// Prepared replacement entry point; not deployed by the ordinary program package.
// Existing Cloudflare secrets remain there. KV is frozen while this bridge runs.
import {BRIDGE_HEADER,BROKER_PATH,githubRequestAllowed} from './bridge-contract.mjs';

export async function bridgeFetch(request,env,fetcher=fetch){
  const source=new URL(request.url);
  if(!['https://api-hmao.delosud.ru','https://court-monitor-trigger.7selivanov-a.workers.dev'].includes(source.origin))return new Response('Wrong host',{status:421});
  if(typeof env.BRIDGE_SECRET!=='string'||env.BRIDGE_SECRET.length<32)return new Response('Not configured',{status:503});
  if(source.pathname===BROKER_PATH){
    if(request.method!=='POST'||request.headers.get(BRIDGE_HEADER)!==env.BRIDGE_SECRET)return new Response('Not Found',{status:404});
    const raw=await request.text();if(raw.length>262144)return new Response('Too large',{status:413});
    let job;try{job=JSON.parse(raw);}catch{return new Response('Bad request',{status:400});}
    if(!githubRequestAllowed(job.url,job.method,job.body,env.GH_REPO))return new Response('Forbidden',{status:403});
    if(env.BRIDGE_MAINTENANCE!=='0'&&job.method!=='GET')return new Response('Service Unavailable',{status:503});
    if(!env.GITHUB_PAT)return new Response('Not configured',{status:503});
    const result=await fetcher(job.url,{method:job.method,redirect:'manual',signal:AbortSignal.timeout(18000),headers:{
      Authorization:`Bearer ${env.GITHUB_PAT}`,Accept:'application/vnd.github+json','User-Agent':'court-monitor-server-bridge/1.0'},
      ...(job.method==='POST'?{body:job.body}:{})});
    if(result.status>=300&&result.status<400)return new Response('Upstream redirect blocked',{status:502});
    return new Response(result.body,{status:result.status,headers:{'content-type':result.headers.get('content-type')||'application/json','cache-control':'no-store'}});
  }
  if(env.BRIDGE_MAINTENANCE!=='0')return new Response('Service Unavailable',{status:503,headers:{'retry-after':'60','cache-control':'no-store'}});
  if(source.pathname.startsWith('/__sbersud/'))return new Response('Not Found',{status:404});
  const destination=new URL(env.SERVER_ORIGIN);
  if(destination.origin!=='https://monitor.delosud.ru:8443'||destination.pathname!=='/sbersud-api/hmao/'||destination.username||destination.password||destination.search||destination.hash)throw Error('Invalid origin');
  destination.pathname+=source.pathname.slice(1);destination.search=source.search;
  const roles=JSON.parse(env.BRIDGE_ROLE_SECRETS||'{}'),headers=new Headers(request.headers);
  let queryMapped=false;
  for(const key of ['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET']){
    if(typeof roles[key]!=='string'||roles[key].length<16)return new Response('Not configured',{status:503});
    if(!queryMapped&&env[key]&&source.searchParams.get('secret')===env[key]){destination.searchParams.set('secret',roles[key]);queryMapped=true;}
    if(env[key]&&headers.get('authorization')===`Bearer ${env[key]}`)headers.set('authorization',`Bearer ${roles[key]}`);
  }
  for(const name of ['host','forwarded','x-forwarded-host','x-forwarded-proto','x-original-url',BRIDGE_HEADER])headers.delete(name);
  headers.set(BRIDGE_HEADER,env.BRIDGE_SECRET);
  const response=await fetcher(destination.href,{method:request.method,headers,redirect:'manual',
    ...(!['GET','HEAD'].includes(request.method)?{body:request.body}:{})});
  if(response.status>=300&&response.status<400)return new Response('Upstream redirect blocked',{status:502});
  return new Response(response.body,{status:response.status,headers:response.headers});
}
export default {
  fetch(request,env){return bridgeFetch(request,env);},
  // Old KV must remain frozen, including the old profile cleanup cron.
  scheduled(){},
};
