import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildIndex } from '../data-index.mjs';

export const names = { hmao:'hmao',sverdlovsk_yanao:'ural',bashkortostan:'bashkortostan',tyumen:'tyumen' };
export function caseRecord(id='2-100/2026') {
  return { id, current_stage:'cassation',bank_role:'Ответчик',plaintiff:'Тестовый истец',defendant:'Банк',category:'Тест',
    first_instance:{case_number:id,material_number:'М-100/2026',court_domain:'surggor--hmao.sudrf.ru'},
    cassation:{case_number:'8Г-100/2026',court_domain:'6kas.sudrf.ru',hearing_date:'25.12.2099',hearing_time:'10:30',
      last_event:'Назначено заседание',court:'Тестовый суд',events:[{date:'25.12.2099',place:'Зал 1'}],
      timezone:'Europe/Samara',judge:'Судья',link:'https://6kas.sudrf.ru/modules.php?case_id=123',
      document:{text:'Синтетический текст '.repeat(200)} } };
}
export async function fixture(region='hmao', { count=1, bankCount=0 }={}) {
  const root=await mkdtemp(join(tmpdir(),'sbersud-api-'));
  const data=join(root,'input'); await mkdir(data);
  const meta={code:region,timezone:'Asia/Yekaterinburg',cassation:{domain:'6kas.sudrf.ru',timezone:'Europe/Samara'}};
  const main={region:meta,cases:Array.from({length:count},(_,i)=>caseRecord(`2-${100+i}/2026`))};
  const bank={cases:Array.from({length:bankCount},(_,i)=>{
    const c=caseRecord(`2-${90000+i}/2026`);
    c.current_stage='first_instance';c.bank_role='Истец';
    c.first_instance={...c.cassation,...c.first_instance,hearing_date:'25.12.2099',hearing_time:'11:00',timezone:'Asia/Yekaterinburg'};
    delete c.cassation;
    return c;
  })};
  await writeFile(join(data,'cases.json'),JSON.stringify(main));
  await writeFile(join(data,'cases_bank.json'),JSON.stringify(bank));
  const index=join(root,'api-index.json');
  const size=await buildIndex({dataDir:data,output:index,region});
  const config={mode:'isolated',region,database:join(root,'state.sqlite'),dataIndex:index,port:0,
    publicOrigin:`https://api-${names[region]}.delosud.ru`,
    allowedOrigins:['https://selivanovas.github.io','https://delosud.ru'],
    allowedCourts:['surggor--hmao.sudrf.ru','6kas.sudrf.ru'],
    vars:{CASES_DATA_URL:`https://selivanovas.github.io/dashboard${region==='hmao'?'':'-'+names[region]}/data/cases.json`,
      GH_REPO:`synthetic/${region}`,VAPID_PUBLIC_KEY:'synthetic-vapid-public-key',CRON_UTC:'',IMPORT_EXECUTOR:'vps',
      PROFILE_CLEANUP_ENABLED:'1',VISITS_ENABLED:'1'},
    secrets:Object.fromEntries(['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET']
      .map(k=>[k,`synthetic-${region}-${k}-only`])) };
  return {root,data,main,bank,config,size};
}
export function requester(runtime,config) {
  return (path,{body,role,headers={},method}={}) => {
    const url=new URL(config.publicOrigin+path);
    if(role) url.searchParams.set('secret',config.secrets[role]);
    return runtime.fetch(new Request(url,{method:method||(body===undefined?'GET':'POST'),headers:{'content-type':'application/json',...headers},
      ...(body===undefined?{}:{body:JSON.stringify(body)})}));
  };
}
