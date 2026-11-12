import {BRIDGE_HEADER,BROKER_PATH,githubRequestAllowed} from './bridge-contract.mjs';

export function productionTransports(config,nativeFetch){
  const github=async(url,options={})=>{
    const method=options.method||'GET';
    if(!githubRequestAllowed(url,method,options.body,config.vars.GH_REPO))throw Error('External operation is not allowed');
    if(method==='POST'&&config.executionEnabled!==true)throw Error('Execution is not enabled');
    return nativeFetch(config.publicOrigin+BROKER_PATH,{method:'POST',redirect:'error',signal:AbortSignal.timeout(20000),
      headers:{[BRIDGE_HEADER]:config.secrets.BRIDGE_SECRET,'content-type':'application/json','User-Agent':'court-monitor-server/1.0'},
      body:JSON.stringify({url:String(url),method,body:options.body})});
  };
  const gateway=async(url,options={})=>{
    const u=new URL(url);
    if(u.origin!=='https://api2-hmao.delosud.ru'||!/^\/_gateway-upload\/[0-9a-f-]{36}$/.test(u.pathname)
      ||u.username||u.password||u.hash||u.search||options.method!=='GET')throw Error('Invalid import gateway');
    return nativeFetch(url,{...options,redirect:'error',signal:AbortSignal.any([options.signal,AbortSignal.timeout(30000)].filter(Boolean))});
  };
  return {github,gateway};
}
