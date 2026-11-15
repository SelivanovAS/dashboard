// Страница вне области service worker: одноразовый код не попадёт в его кэш,
// а установка / обновление приложения не прервёт обмен посередине.
async function openTerritory() {
  const fields = new URLSearchParams(location.hash.slice(1));
  history.replaceState(null, '', location.pathname + location.search);
  const code = fields.get('code');
  if (!/^\d{6}$/.test(code || '') || !/^[a-f0-9]{64}$/.test(fields.get('stars') || '')) {
    throw Error('Откройте прежнее приложение ХМАО на iPhone ещё раз.');
  }
  const digest = async (salt, value) => {
    const raw = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(salt + ':' + value));
    return Array.from(new Uint8Array(raw), n => n.toString(16).padStart(2, '0')).join('');
  };
  const key = name => 'delosud-hmao:' + name;
  const get = name => localStorage.getItem(key(name));
  const checkState = () => JSON.stringify(['profile_id','profile_dirty','profile_ops','watchlist_v1','profile_base_ts'].map(get));
  const initial = checkState();
  const cfg = await fetch('/iphone-hmao/config.json', {cache:'no-store',redirect:'error'});
  if (!cfg.ok) throw Error('Проверка перехода завершена. Прежнее приложение доступно.');
  const pilot = await cfg.json();
  const id = get('profile_id');
  const stars = JSON.parse(get('watchlist_v1') || '[]');
  if ((id && await digest(pilot.salt, id) !== pilot.profile) || (!id && stars.length)
      || get('profile_dirty') === '1' || JSON.parse(get('profile_ops') || '[]').length) {
    throw Error('На новом адресе уже есть другой профиль или несохранённые ★ дела. Оба списка сохранены.');
  }
  // При повторном запуске читаем уже связанный профиль. При первом обмениваем
  // одноразовый код. Новый push-адрес не создаётся и в запросе не передаётся.
  const route = id ? 'get' : 'link';
  const r = await fetch('/_api/hmao/profile/' + route, {
    method:'POST', headers:{'Content-Type':'application/json'}, cache:'no-store', redirect:'error',
    body:JSON.stringify(id ? {profile_id:id} : {code, watchlist:[]}),
  });
  if (!r.ok) throw Error('Откройте прежнее приложение ещё раз, чтобы повторить переход.');
  const p = await r.json();
  const profile = id || p.profile_id;
  if (!p.ok || !profile || p.profile_id !== profile || await digest(pilot.salt, profile) !== pilot.profile || !Array.isArray(p.watchlist)
      || !p.watchlist.every(s => typeof s === 'string') || !Number.isFinite(p.updated_at)
      || await digest(code, JSON.stringify([...new Set(p.watchlist)].sort())) !== fields.get('stars')
      || initial !== checkState()) throw Error('★ дела не совпали. Вернитесь в прежнее приложение.');
  // При повторном открытии сохраняем локальные данные для обычной сверки
  // версии профиля в приложении. Первую привязку фиксируем последней записью.
  if (!id) {
    const values = {watchlist_v1:JSON.stringify(p.watchlist), profile_base_ts:String(p.updated_at), profile_id:profile};
    const previous = Object.fromEntries(Object.keys(values).map(k => [k,get(k)]));
    const written = [];
    try {
      for (const [name, value] of Object.entries(values)) {
        localStorage.setItem(key(name), value);
        written.push(name);
      }
    } catch (e) {
      for (const name of written.reverse()) {
        if (previous[name] === null) localStorage.removeItem(key(name));
        else localStorage.setItem(key(name), previous[name]);
      }
      throw e;
    }
  }
  const target = new URL('/hmao/', location.origin);
  const args = new URLSearchParams(location.search);
  if (args.get('digest') === 'open') target.searchParams.set('digest','open');
  if (args.get('mine') === '1') target.searchParams.set('mine','1');
  location.replace(target.href);
}
openTerritory().catch(e => {
  document.getElementById('status').textContent = e.message || 'Не удалось подключиться. Откройте прежнее приложение ещё раз.';
});
