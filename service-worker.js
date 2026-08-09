/* Service Worker для дашборда юриста Сбербанка (PWA).
   Стратегии:
     · App shell (HTML/CSS/JS/иконки/манифест) — cache-first
     · data/*.json|.ics                       — stale-while-revalidate
     · Google Fonts                            — cache-first с долгим TTL
   При обновлении файлов — увеличить CACHE_VERSION, старые кэши очистятся в activate.
   ⚠️ CACHE_VERSION = единый номер с ?v= у styles.css/app.js в HTML
   (сверяется тестом scripts/tests/test_versions.py).
*/

const CACHE_VERSION = 'v130';
// Территория в имени кэша: фронты ХМАО (/dashboard/) и Урала (/dashboard-ural/)
// живут на одном origin github.io, а Cache Storage общий на весь origin —
// без суффикса activate-очистка одной территории сносила бы кэши другой при
// каждом расхождении версий (эталон и форк обновляются не синхронно).
// Каталог, из которого зарегистрирован SW ('/dashboard/service-worker.js' →
// 'dashboard'); в корне (локальная отладка) — 'root'.
const SCOPE_NS = self.location.pathname.split('/').slice(0, -1).filter(Boolean).join('-') || 'root';
const CACHE_NAME = `sber-jurist-${SCOPE_NS}-${CACHE_VERSION}`;
const FONTS_CACHE = `sber-jurist-fonts-${SCOPE_NS}-${CACHE_VERSION}`;

// App shell — то, без чего страница не запустится. Все пути относительные:
// SW регистрируется на /dashboard/service-worker.js, scope = /dashboard/.
// styles.css/app.js без `?v=` — pre-cache на голый URL для офлайна; реальные
// запросы из HTML идут с актуальной `?v=N` и попадают в кэш по cache-first
// после первого fetch (мисс по голому URL → сеть → кэш с querystring).
const APP_SHELL = [
  './',
  './sberbank_dashboard.html',
  './styles.css',
  './app.js',
  './region_front.js',
  './manifest.json',
  './icon-180.png',
  './icon-192.png',
  './icon-512.png',
];

// Минимальная офлайн-страница на случай, если HTML не оказалось ни в сети, ни в кэше.
const OFFLINE_HTML = `<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Нет связи · Сбер Юрист</title>
<style>
  body{margin:0;font-family:-apple-system,system-ui,sans-serif;background:#f1faf3;color:#14181f;
       display:flex;align-items:center;justify-content:center;min-height:100vh;padding:24px;text-align:center}
  .card{max-width:360px}
  h1{margin:0 0 12px;font-size:22px;color:#157f3a}
  p{margin:0 0 20px;line-height:1.5;color:#4a5160}
  button{background:#21a038;color:#fff;border:0;padding:12px 20px;border-radius:8px;
         font-size:15px;font-weight:600;cursor:pointer}
</style></head><body><div class="card">
<h1>Нет связи</h1>
<p>Дашборд работает офлайн, но эта страница ещё не закэширована.<br>Попробуйте перезагрузить, когда появится сеть.</p>
<button onclick="location.reload()">Перезагрузить</button>
</div></body></html>`;

// ---------- install: pre-cache app shell ----------
// ⚠️ НЕ cache.addAll: он атомарный — один 404 отклоняет install целиком, SW
// становится redundant, serviceWorker.ready не резолвится → нет пуша,
// офлайна и колокольчика ВООБЩЕ. Так «./» без index.html молча убивал SW
// у всех новых устройств в Chromium (инцидент Урала 17.07.2026; iOS-парк
// выжил только благодаря снисходительности WebKit). Кэшируем поэлементно:
// битый файл — warning в консоль и пропуск, недостающее докэширует
// cacheFirst при первом обращении.
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => Promise.all(APP_SHELL.map(
        (u) => cache.add(u).catch((e) => console.warn('SW pre-cache пропуск:', u, e && e.message))
      )))
      .then(() => self.skipWaiting())
  );
});

// ---------- activate: чистим старые кэши ----------
self.addEventListener('activate', (event) => {
  const allowed = new Set([CACHE_NAME, FONTS_CACHE]);
  // Удаляем только кэши СВОЕЙ территории + легаси-имена без территории
  // (формат до v107, одноразово). Сносить «всё не своё» нельзя — на общем
  // origin это живые кэши соседней территории. Проверка остатка через
  // /^v\d+$/ обязательна: префикс 'sber-jurist-dashboard-' — надстрока
  // имени 'sber-jurist-dashboard-ural-v107', голый startsWith снёс бы соседа.
  const ownVersion = (k) => {
    const prefixes = [`sber-jurist-${SCOPE_NS}-`, `sber-jurist-fonts-${SCOPE_NS}-`];
    return prefixes.some((p) => k.startsWith(p) && /^v\d+$/.test(k.slice(p.length)));
  };
  const legacy = (k) => /^sber-jurist-(fonts-)?v\d+$/.test(k);
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys
          .filter((k) => !allowed.has(k) && (ownVersion(k) || legacy(k)))
          .map((k) => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// ---------- helpers ----------
function isDataRequest(url) {
  // /dashboard/data/cases.json и т.п. — любые .json/.ics в подкаталоге data/
  return /\/data\/.+\.(json|ics)(\?|$)/i.test(url.pathname);
}

function isLastDigestRequest(url) {
  // last_digest.json обновляется тестовыми workflow'ами (Push Last Digest,
  // Digest Only) — пользователь хочет видеть свежий вид сразу, а не на
  // следующей перезагрузке. Поэтому network-first, а не SWR.
  return /\/data\/last_digest\.json(\?|$)/i.test(url.pathname);
}

function isFontRequest(url) {
  return url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com';
}

// network-first: тянем из сети, при сбое — отдаём кэш. Для критичных файлов,
// где свежесть важнее скорости (last_digest.json — пользователь только что
// сгенерил его и хочет увидеть СЕЙЧАС, не «через перезагрузку»).
async function networkFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  try {
    const res = await fetch(request);
    if (res && res.ok) cache.put(request, res.clone());
    return res;
  } catch (err) {
    const cached = await cache.match(request);
    if (cached) return cached;
    const isNav = request.mode === 'navigate'
      || (request.headers.get('accept') || '').includes('text/html');
    if (isNav) {
      // Пре-кэш хранит голый './sberbank_dashboard.html', а открытие по клику
      // на push несёт query (?digest=open / ?mine=1) — точный match промахнётся.
      const bare = await cache.match(request, { ignoreSearch: true });
      if (bare) return bare;
      return new Response(OFFLINE_HTML, {
        status: 200,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    return new Response('{}', {
      status: 503,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
  }
}

// Версия ответа для сравнения «кэш vs сеть». ETag GitHub Pages отдаёт всегда;
// Last-Modified — фолбэк для локального http.server и прочих хостингов.
function responseTag(res) {
  if (!res) return null;
  return res.headers.get('ETag') || res.headers.get('Last-Modified') || null;
}

// stale-while-revalidate: отдаём из кэша моментально, в фоне обновляем кэш.
// ⚠️ Страница получает ВЧЕРАШНИЙ снимок: свежая копия ложится в кэш и видна
// только со следующего открытия. Для дашборда это значило «прогон был, а дела
// прежние» (кейс 2-592/2025, 03.08.2026: дело ушло в архив трека, а картотека
// показывала его активным — при том что last_digest.json идёт network-first и
// дайджест рядом был уже сегодняшний). Поэтому, обновив кэш, сообщаем об этом
// клиентам — app.js перечитает датасет из кэша (мгновенно, без сети) и
// перерисуется. Network-first здесь не годится: cases.json 2 МБ,
// cases_bank.json 1.4 МБ — первый экран встал бы на мобильной сети.
// ⚠️ `event` обязателен: отдав ответ из кэша, браузер вправе усыпить SW — и
// фоновая ревалидация умрёт вместе с ним, не обновив кэш и не сообщив
// странице. event.waitUntil держит SW живым до конца дозагрузки. Без этого
// именно на телефоне (где SW усыпляют агрессивнее) юрист снова увидел бы
// вчерашние данные.
async function staleWhileRevalidate(request, cacheName, event) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  const network = fetch(request)
    .then(async (res) => {
      if (res && res.ok) {
        const changed = cached && responseTag(cached) !== responseTag(res);
        await cache.put(request, res.clone());
        // Сообщаем только о РЕАЛЬНОМ обновлении уже показанных данных:
        // первая загрузка (кэша не было) и так отдала свежее, а версия без
        // ETag/Last-Modified неотличима — молчим, прежнее поведение.
        if (changed) await notifyDataUpdated(request.url);
      }
      return res;
    })
    .catch(() => null);
  if (cached && event) event.waitUntil(network);
  return cached || (await network) || new Response('[]', {
    status: 503,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
  });
}

async function notifyDataUpdated(url) {
  const clients = await self.clients.matchAll({ type: 'window' });
  for (const client of clients) {
    client.postMessage({ type: 'data-updated', url });
  }
}

// cache-first: сначала кэш, если нет — сеть, при ответе — кладём в кэш.
async function cacheFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) return cached;
  try {
    const res = await fetch(request);
    if (res && res.ok && res.type !== 'opaque') {
      // opaque (no-cors) тоже можно класть, но размер не определён — пропускаем во избежание раздувания
      cache.put(request, res.clone());
    } else if (res && res.type === 'opaque') {
      cache.put(request, res.clone());
    }
    return res;
  } catch (err) {
    // Финальный фолбэк для HTML-навигаций — офлайн-страница.
    if (request.mode === 'navigate' || (request.headers.get('accept') || '').includes('text/html')) {
      return new Response(OFFLINE_HTML, {
        status: 200,
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    throw err;
  }
}

// ---------- push: входящее уведомление от сервера ----------
self.addEventListener('push', (event) => {
  const data = event.data
    ? event.data.json()
    : { title: 'Сбер Юрист', body: 'Есть обновления по делам' };

  // URL, который SW откроет по клику. Бэкенд (Python send_web_push) присылает
  // абсолютный путь '/sberbank_dashboard.html?digest=open' — приводим к
  // относительному в рамках scope SW, чтобы работало на GitHub Pages
  // (хостинг под /dashboard/).
  const rawUrl = (data.data && data.data.url) || './sberbank_dashboard.html?digest=open';
  const clickUrl = rawUrl.startsWith('/') ? '.' + rawUrl : rawUrl;

  event.waitUntil(
    self.registration.showNotification(data.title || 'Сбер Юрист', {
      body: data.body || 'Есть обновления по делам',
      icon: './icon-192.png',
      badge: './icon-192.png',
      data: { url: clickUrl },
      vibrate: [200, 100, 200],
    })
  );
});

// ---------- notificationclick: открыть приложение по клику ----------
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url)
    || './sberbank_dashboard.html?digest=open';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      const existing = list.find((w) => w.url.includes('sberbank_dashboard'));
      if (existing) {
        // Окно уже открыто — фокусируем и просим страницу развернуть дайджест
        // (URL-параметр уже не сработает — страница не перезагружается).
        existing.postMessage({ type: 'open-digest' });
        return existing.focus();
      }
      return clients.openWindow(url);
    })
  );
});

// ---------- fetch: маршрутизация ----------
self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Игнорируем chrome-extension://, data:, blob: и пр.
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;

  // last_digest.json должен обновляться сразу после прогона workflow —
  // ставим network-first, отдельно от остальных data/*.json.
  if (isLastDigestRequest(url)) {
    event.respondWith(networkFirst(request, CACHE_NAME));
    return;
  }

  if (isDataRequest(url)) {
    event.respondWith(staleWhileRevalidate(request, CACHE_NAME, event));
    return;
  }

  if (isFontRequest(url)) {
    event.respondWith(cacheFirst(request, FONTS_CACHE));
    return;
  }

  // HTML (navigate) — networkFirst, чтобы PWA не залипал на старом sberbank_dashboard.html
  // со ссылкой на устаревший styles.css?v=N. Офлайн-fallback — кэш + offline-страница.
  if (request.mode === 'navigate' || (request.headers.get('accept') || '').includes('text/html')) {
    event.respondWith(networkFirst(request, CACHE_NAME));
    return;
  }

  // Только same-origin для остального — чужие домены пусть идут напрямую.
  if (url.origin === self.location.origin) {
    event.respondWith(cacheFirst(request, CACHE_NAME));
  }
});
