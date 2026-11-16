const {test} = require('node:test');
const assert = require('node:assert/strict');
const {webcrypto, createHash} = require('node:crypto');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const app = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
const source = app.slice(app.indexOf('function oldHmaoSite()'),app.indexOf('async function setupPushNotifications(reg)'));
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
  elements['hmao-move'].querySelectorAll = () => [details];
  const s = {requests:[], watchlist:new Set(local), remote:[...remote], settingsClosed:false};
  const location = {origin:'https://selivanovas.github.io', pathname:'/dashboard/sberbank_dashboard.html'};
  const ctx = {
    location, navigator:{standalone:true, userAgent:'iPhone'}, TextEncoder, crypto:webcrypto,
    fetch:async () => ({ok:true, json:async () => pilot}),
    document:{getElementById:key => elements[key]},
    lsKey:key => key, localStorage:storage, getProfileId:() => currentId, setProfileLink:id=>{currentId=id;},
    ensureWorkerHost:async()=>{}, _saveProfileOps(){}, _adoptServerWatchlist:arr=>{s.watchlist=new Set(arr);}, isProfileDirty:() => false,
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
      assert.equal(route,'/profile/link-code'); if (currentId) assert.deepEqual(body,{profile_id:currentId});
      else { assert.deepEqual(body.watchlist, [...s.watchlist]); s.remote = body.watchlist; }
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
  ctx.setupHmaoMoveNotice();
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
  first.ctx.setupHmaoMoveNotice();
  first.elements['hmao-move-hide'].onclick();
  assert.equal(first.elements['hmao-move'].hidden,true);
  const next = original([], {storage});
  next.ctx.setupHmaoMoveNotice();
  assert.equal(next.elements['hmao-move'].hidden,true);
  next.ctx.reopenHmaoMoveNotice();
  assert.equal(next.s.settingsClosed,true);
  assert.equal(next.elements['hmao-move'].hidden,false);
  assert.equal(next.elements['hmao-move-title'].focused,true);
  assert.deepEqual(next.s.requests,[]);
});
test('обе старые площадки ХМАО показывают подсказку на трёх платформах без push и профиля', async () => {
  for (const agent of ['iPhone','Android','Linux']) {
    for (const [origin, pathname] of [['https://selivanovas.github.io','/dashboard/'], ['https://api2-hmao.delosud.ru','/site/']]) {
      const {s,ctx,elements}=original(['A'],{currentId:''});
      Object.assign(ctx.location,{origin,pathname});
      ctx.navigator={userAgent:agent};
      ctx.profileSessionOps.set('A','add');
      ctx.setupHmaoMoveNotice();
      assert.equal(elements['hmao-move'].hidden,false);
      assert.deepEqual(s.requests,[]);
      await elements['hmao-move-code'].onclick();
      assert.equal(ctx.getProfileId(),profile);
      assert.equal(ctx.profileSessionOps.size,0);
      assert.match(elements['hmao-move-result'].textContent,/376-101/);
    }
  }
});
test('новый адрес и другие территории не показывают подсказку', () => {
  for (const [origin, pathname] of [['https://delosud.ru','/hmao/'], ['https://api2-ural.delosud.ru','/site/'], ['https://selivanovas.github.io','/dashboard-ural/']]) {
    const {s,ctx,elements}=original([]);
    Object.assign(ctx.location,{origin,pathname});
    ctx.setupHmaoMoveNotice();
    ctx.reopenHmaoMoveNotice();
    assert.equal(elements['hmao-move'].hidden,true);
    assert.deepEqual(s.requests,[]);
  }
});
test('локальные ★ можно вернуть в общий профиль только явным подтверждением', async () => {
  const {s,ctx,elements} = original([]);
  ctx.rememberHmaoMoveStars(profile, ['local'], []);
  ctx.setupHmaoMoveNotice();
  const button = elements['hmao-move-code'];
  await button.onclick();
  assert.deepEqual(s.remote,[]);
  assert.match(button.textContent,/Сохранить эти ★/);
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

const firstLoad = app.slice(app.indexOf('async function readHmaoStartCases('), app.indexOf('async function fetchJsonCases('));
test('резерв берёт только файл картотеки, когда VPS не отдаёт тело, и сохраняет данные для повторного запуска', async () => {
  const original='data/cases.json', expected='https://selivanovas.github.io/dashboard/data/cases.json';
  const calls=[], stored=new Map(), source={region:{code:'hmao'},cases:[{id:'A'}],updated_at:'2026-11-15T08:45:00Z'};
  let network=true, mirrorTerritory='hmao';
  let earlyBackup;
  const ctx={
    MAIN_JSON_URL:original, location:{href:'https://delosud.ru/hmao/sberbank_dashboard.html?private=test'},
    navigator:{}, URL, AbortController, Response, Headers, dataCacheName:()=>'hmao-test',
    // Пропускаем 6 секунд ожидания резерва, после того как основной ответ уже дал заголовки.
    setTimeout:(fn,ms)=>ms===6000?(earlyBackup=fn):undefined, clearTimeout:()=>{},
    readJsonFromCache:async key=>stored.get(key)||null,
    caches:{open:async()=>({put:async(key,res)=>stored.set(key,await res.json())})},
    fetch:async(address,options)=>{
      calls.push(address);
      assert.equal(options.credentials,'omit');assert.equal(options.referrerPolicy,'no-referrer');
      if(!network) throw Error('offline');
      if(address===expected)return Response.json({...source, region:{code:mirrorTerritory}});
      if(mirrorTerritory !== 'hmao')throw Error('unavailable');
      return {ok:true,json:()=>new Promise((_ok,fail)=>{
        options.signal.addEventListener('abort',()=>fail(Error('cancelled')),{once:true});
        earlyBackup();
      })};
    }
  };
  vm.createContext(ctx);vm.runInContext(firstLoad,ctx);
  const online=await ctx.readHmaoStartCases(original);
  assert.deepEqual(calls,['https://delosud.ru/hmao/data/start/cases.json',expected]);
  assert.equal(online.fromCache,false);assert.equal(online.data.cases[0].id,'A');
  await new Promise(resolve=>setImmediate(resolve));
  network=false;
  const offline=await ctx.readHmaoStartCases(original);
  assert.equal(offline.fromCache,true);assert.equal(offline.data.cases[0].id,'A');
  network=true;stored.clear();mirrorTerritory='tyumen';
  await assert.rejects(()=>ctx.readHmaoStartCases(original),/Не удалось загрузить дела/);
});

test('шесть нажатий на цифровой клавиатуре, вставка и исправление кода', () => {
  const code = app.slice(app.indexOf('function fmtPairCode('), app.indexOf('// Блок «Календарь заседаний»'));
  const ctx = vm.createContext({});
  vm.runInContext(code,ctx);
  const input = {value:'', selectionStart:0, setSelectionRange(start){this.selectionStart=start;}};
  function edit(raw, caret, type='insertText') {
    input.value=raw; input.selectionStart=caret;
    ctx.formatPairCodeInput({target:input,inputType:type});
  }
  for (const digit of '012345') edit(input.value+digit, input.value.length+1);
  assert.equal(input.value,'012-345');
  edit(' 012-345 ',9,'insertFromPaste'); assert.equal(input.value,'012-345');
  edit('012345',3,'deleteContentBackward'); assert.equal(input.selectionStart,3);
  edit('012345',3,'deleteContentForward'); assert.equal(input.selectionStart,4);
  edit('012-45',4,'deleteContentForward'); assert.equal(input.value,'012-45');
  edit('',0); assert.equal(input.value,'');
});
