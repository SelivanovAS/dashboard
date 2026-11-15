const {test} = require('node:test');
const assert = require('node:assert/strict');
const {webcrypto, createHash} = require('node:crypto');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const app = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
const source = app.slice(app.indexOf('function oldHmaoIPhone()'),app.indexOf('async function setupPushNotifications(reg)'));
const receiver = fs.readFileSync(path.join(root,'ops/vps-run/public-site/iphone-hmao.js'),'utf8');
const tag = (salt,str) => createHash('sha256').update(salt+':'+str).digest('hex');
const profile = 'synthetic-profile', endpoint = 'https://example.invalid/synthetic', salt = 'test-salt';
const pilot = {salt,device:tag(salt,endpoint),profile:tag(salt,profile)};
function state(initial={}) {
  const map=new Map(Object.entries(initial));
  return {getItem:k=>map.get(k)??null,setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)};
}
function original(local, remote=local) {
  const s = {requests:[],links:[],went:'',watchlist:new Set(local),remote:[...remote]};
  const location = {origin:'https://selivanovas.github.io',pathname:'/dashboard/sberbank_dashboard.html',search:'?digest=open&mine=1&owner=ignore',replace:u=>{s.went=u;}};
  const ctx={location,navigator:{standalone:true,userAgent:'iPhone'},URL,URLSearchParams,TextEncoder,crypto:webcrypto,
    fetch:async()=>({ok:true,json:async()=>pilot}),document:{body:{append:x=>s.links.push(x)},createElement:()=>({style:{},remove(){}})},
    getProfileId:()=>profile,isProfileDirty:()=>false,profileSessionOps:new Map(),localStorage:state(),watchlist:s.watchlist,
    canonCaseNumber:x=>x,showToast(){},applyProfileServerState(list){s.watchlist.clear();for(const x of list)s.watchlist.add(x);},
    workerFetch:async(route,req)=>{s.requests.push(route);const body=JSON.parse(req.body);
      if(route==='/profile/watchlist'){s.remote=body.watchlist;return {ok:true,json:async()=>({ok:true,canonical:s.remote,updated_at:3})};}
      assert.equal(route,'/profile/link-code');assert.deepEqual(body,{profile_id:profile});
      return {ok:true,json:async()=>({ok:true,code:'376101',profile_id:profile,updated_at:2,watchlist:s.remote})};}};
  vm.createContext(ctx);vm.runInContext(source,ctx);return {s,ctx};
}
async function receive(url, remote, initial={}) {
  const target=new URL(url);const s={requests:[],went:'',ui:{},local:state(initial)};
  const ctx={location:{hash:target.hash,pathname:target.pathname,search:target.search,origin:target.origin,replace:u=>{s.went=u;}},
    history:{replaceState:(_d,_t,url)=>{s.cleaned=url;}},localStorage:s.local,document:{getElementById:()=>s.ui},
    URL, URLSearchParams,TextEncoder,crypto:webcrypto,fetch:async(url,req)=>{
      if(url.endsWith('config.json'))return {ok:true,json:async()=>pilot};
      s.requests.push([url,JSON.parse(req.body)]);
      return {ok:true,json:async()=>({ok:true,profile_id:profile,updated_at:2,watchlist:remote})};}};
  await vm.runInNewContext(receiver,ctx);
  return s;
}
test('старое приложение передаёт свои ★ и открывает нужный раздел, подписка не заменяется',async()=>{
  const {s,ctx}=original(['A','B']);
  assert(ctx.oldHmaoIPhone());await ctx.openHmaoPilot({endpoint},['B','A']);
  const dest=new URL(s.went);assert.equal(dest.origin,'https://delosud.ru');
  assert(!dest.search.includes('376101')&&!dest.href.includes(profile)&&!dest.href.includes('owner='));
  const n=await receive(s.went,['A','B']);
  assert.equal(n.went,'https://delosud.ru/hmao/?digest=open&mine=1');
  assert.deepEqual(n.requests,[['/_api/hmao/profile/link',{code:'376101',watchlist:[]}]]);
  assert.equal(n.local.getItem('delosud-hmao:profile_id'),profile);
  assert.deepEqual(JSON.parse(n.local.getItem('delosud-hmao:watchlist_v1')),['A','B']);
  assert(!n.cleaned.includes('376101'));
  const second=await receive(s.went,['A','B'],{'delosud-hmao:profile_id':profile});
  assert.equal(second.requests[0][0],'/_api/hmao/profile/get');
});
test('чужая подписка, чужой профиль и расхождение списков не включают переход',async()=>{
  const {s,ctx}=original(['A']);await ctx.openHmaoPilot({endpoint:endpoint+'different'},['A']);
  assert.equal(s.went,'');assert.deepEqual(s.requests,[]);
  ctx.navigator.standalone=false;assert(!ctx.oldHmaoIPhone());
  await ctx.openHmaoPilot({endpoint},['A']);
  assert.equal((await receive(s.went,['B'])).went,'');
  const conflict=await receive(s.went,['A'],{'delosud-hmao:profile_id':'different'});
  assert.equal(conflict.went,'');assert.deepEqual(conflict.requests,[]);
});
test('локальные ★ владельца сохраняются, объединение выполняется только по нажатию',async()=>{
  const {s,ctx}=original([],[]);
  await ctx.openHmaoPilot({endpoint},['local']);
  assert(!s.went);assert(!s.requests.includes('/profile/watchlist'));
  assert.match(s.links[0].textContent,/Сохранить ★/);
  await s.links[0].onclick();assert(s.went);
  const n=await receive(s.went,s.remote);
  assert.deepEqual(JSON.parse(n.local.getItem('delosud-hmao:watchlist_v1')),['local']);
});
