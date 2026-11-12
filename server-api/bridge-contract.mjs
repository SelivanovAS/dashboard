export const BRIDGE_HEADER='x-sbersud-bridge';
export const BROKER_PATH='/__sbersud/github';
const workflows=new Set(['update_cases.yml','test_digest.yml','import_cases.yml','add_cases.yml','mark_writ.yml','replay_on_push.yml']);
export function githubRequestAllowed(url,method,body,repository){
  let u;try{u=new URL(url);}catch{return false;}
  const prefix=`/repos/${repository}/actions/`;
  if(u.origin!=='https://api.github.com'||u.username||u.password||u.hash||!u.pathname.startsWith(prefix))return false;
  const path=u.pathname.slice(prefix.length);
  if(method==='GET')return path==='runs'||/^workflows\/[a-z_]+\.yml\/runs$/.test(path)&&workflows.has(path.split('/')[1]);
  if(method!=='POST'||u.search||!/^workflows\/[a-z_]+\.yml\/dispatches$/.test(path)||!workflows.has(path.split('/')[1]))return false;
  try{const d=JSON.parse(body);return d.ref==='main'&&d.inputs&&typeof d.inputs==='object'&&!Array.isArray(d.inputs);}catch{return false;}
}
