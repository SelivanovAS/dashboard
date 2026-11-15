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
const pilot = {salt,noticeDevice:tag(salt,endpoint),profile:tag(salt,profile)};
function state(initial={}) {
  const map=new Map(Object.entries(initial));
  return {getItem:k=>map.get(k)??null,setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)};
}
function original(local, {remote=local, storage=state(), currentId=profile}={}) {
  const details = {open:false};
  const elements = Object.fromEntries(
    ['hmao-move','hmao-move-title','hmao-move-code','hmao-move-result','hmao-move-hide','btn-settings']
      .map(key => [key, {hidden:true, textContent:'', focus(){ this.focused=true; }}]));
  elements['hmao-move'].querySelector = () => details;
  const s = {requests:[], watchlist:new Set(local), remote:[...remote], settingsClosed:false};
  const location = {origin:'https://selivanovas.github.io', pathname:'/dashboard/sberbank_dashboard.html'};
  const ctx = {
    location, navigator:{standalone:true, userAgent:'iPhone'}, TextEncoder, crypto:webcrypto,
    fetch:async () => ({ok:true, json:async () => pilot}),
    document:{getElementById:key => elements[key]},
    lsKey:key => key, localStorage:storage, getProfileId:() => currentId, isProfileDirty:() => false,
    profileSessionOps:new Map(), watchlist:s.watchlist, canonCaseNumber:x => x,
    fmtPairCode:x => x.slice(0,3)+'-'+x.slice(3), closeSettingsSheet(){ s.settingsClosed=true; },
    applyProfileServerState(list){ s.watchlist.clear(); for (const x of list) s.watchlist.add(x); },
    workerFetch:async (route,req) => {
      s.requests.push(route);
      const body = JSON.parse(req.body);
      if (route === '/profile/watchlist') {
        assert.equal(body.base_ts, 2); s.remote=body.watchlist;
        return {ok:true, json:async () => ({ok:true,canonical:s.remote,updated_at:3})};
      }
      assert.equal(route,'/profile/link-code'); assert.deepEqual(body,{profile_id:profile});
      return {ok:true, json:async () => ({ok:true,code:'376101',profile_id:profile,updated_at:2,watchlist:s.remote})};
    }
  };
  vm.createContext(ctx); vm.runInContext(source,ctx); return {s,ctx,elements};
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
test('подсказка не уводит со старого адреса и запрашивает код только по нажатию', async () => {
  const {s,ctx,elements} = original(['A','B']);
  await ctx.setupHmaoMoveNotice({endpoint}, ['A','B']);
  assert.equal(elements['hmao-move'].hidden,false);
  assert.deepEqual(s.requests,[]);
  await elements['hmao-move-code'].onclick();
  assert.deepEqual(s.requests,['/profile/link-code']);
  assert.match(elements['hmao-move-result'].textContent,/376-101/);
  const html = fs.readFileSync(path.join(root,'sberbank_dashboard.html'),'utf8');
  assert.match(html, /id="hmao-move"[^>]+hidden>/);
  assert.match(html, /href="https:\/\/delosud\.ru\/hmao\/" target="_blank" rel="noopener noreferrer"/);
});
test('скрытие переживает перезапуск, инструкцию можно вернуть из настроек', async () => {
  const storage = state();
  const first = original([], {storage});
  await first.ctx.setupHmaoMoveNotice({endpoint}, []);
  first.elements['hmao-move-hide'].onclick();
  assert.equal(first.elements['hmao-move'].hidden,true);
  const next = original([], {storage});
  await next.ctx.setupHmaoMoveNotice({endpoint}, []);
  assert.equal(next.elements['hmao-move'].hidden,true);
  next.ctx.reopenHmaoMoveNotice();
  assert.equal(next.s.settingsClosed,true);
  assert.equal(next.elements['hmao-move'].hidden,false);
  assert.equal(next.elements['hmao-move-title'].focused,true);
  assert.deepEqual(next.s.requests,[]);
});
test('чужая подписка, другой профиль, обычная вкладка и новый адрес не участвуют', async () => {
  for (const kind of ['endpoint','profile','standalone','origin','old-config']) {
    const {s,ctx,elements} = original([], {currentId:kind === 'profile' ? 'other' : profile});
    if (kind === 'standalone') ctx.navigator.standalone=false;
    if (kind === 'origin') ctx.location.origin='https://delosud.ru';
    if (kind === 'old-config') ctx.fetch=async () => ({ok:true,json:async () => ({salt,device:tag(salt,endpoint),profile:pilot.profile})});
    await ctx.setupHmaoMoveNotice({endpoint:kind === 'endpoint' ? 'other' : endpoint}, []);
    ctx.reopenHmaoMoveNotice();
    assert.equal(elements['hmao-move'].hidden,true,kind);
    assert.deepEqual(s.requests,[],kind);
  }
});
test('локальные ★ можно вернуть в общий профиль только явным подтверждением', async () => {
  const {s,ctx,elements} = original([]);
  await ctx.setupHmaoMoveNotice({endpoint}, ['local']);
  const button = elements['hmao-move-code'];
  await button.onclick();
  assert.deepEqual(s.remote,[]);
  assert.match(button.textContent,/Сохранить ★/);
  await button.onclick();
  assert.deepEqual(s.remote,['local']);
  assert.match(elements['hmao-move-result'].textContent,/376-101/);
});
test('ссылка, ранее выданная пилотом, сохраняет проверку списка и профиля', async () => {
  const url='https://delosud.ru/iphone-hmao/?digest=open&mine=1#code=376101&stars='+tag('376101',JSON.stringify(['A','B']));
  const n=await receive(url,['A','B']);
  assert.equal(n.went,'https://delosud.ru/hmao/?digest=open&mine=1');
  assert.deepEqual(n.requests,[['/_api/hmao/profile/link',{code:'376101',watchlist:[]}]]);
  assert.equal(n.local.getItem('delosud-hmao:profile_id'),profile);
  assert.deepEqual(JSON.parse(n.local.getItem('delosud-hmao:watchlist_v1')),['A','B']);
  assert(!n.cleaned.includes('376101'));
  assert.equal((await receive(url,['B'])).went,'');
  const conflict=await receive(url,['A','B'],{'delosud-hmao:profile_id':'different'});
  assert.equal(conflict.went,'');assert.deepEqual(conflict.requests,[]);
  const second=await receive(url,['A','B'],{'delosud-hmao:profile_id':profile});
  assert.equal(second.requests[0][0],'/_api/hmao/profile/get');
});
