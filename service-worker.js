/* Service Worker для дашборда юриста Сбербанка (PWA).
   Стратегии:
     · App shell (HTML/CSS/JS/шрифты/иконки/манифест) — cache-first
     · data/*.json|.ics                              — stale-while-revalidate
     · data/last_digest.json                         — network-first
   При обновлении файлов — увеличить CACHE_VERSION, старый КОДОВЫЙ кэш
   очистится в activate. Кэш ДАННЫХ (DATA_CACHE) не версионируется и деплой
   переживает — см. комментарий у объявления.
   ⚠️ CACHE_VERSION = единый номер с ?v= у styles.css/app.js в HTML
   (сверяется тестом scripts/tests/test_versions.py).
*/

const CACHE_VERSION = 'v187';
// Территория в имени кэша: фронты ХМАО (/dashboard/) и Урала (/dashboard-ural/)
// живут на одном origin github.io, а Cache Storage общий на весь origin —
// без суффикса activate-очистка одной территории сносила бы кэши другой при
// каждом расхождении версий (эталон и форк обновляются не синхронно).
// Каталог, из которого зарегистрирован SW ('/dashboard/service-worker.js' →
// 'dashboard'); в корне (локальная отладка) — 'root'.
const SCOPE_NS = self.location.pathname.split('/').slice(0, -1).filter(Boolean).join('-') || 'root';
const CACHE_NAME = `sber-jurist-${SCOPE_NS}-${CACHE_VERSION}`;
// ⚠️ Кэш данных НЕ версионируется — и это не забывчивость. До v165 data/*.json
// лежали в версионированном кэше, а правка фронта = обязательный бамп
// CACHE_VERSION (см. CLAUDE.md «Bust фронта/PWA») — то есть activate сносил
// данные вместе с кодом. За 30 дней перед правкой — 39 бампов: офлайн-датасет
// юриста обнулялся чаще раза в день, и PWA без сети открывался белым экраном
// с 4 демо-делами (жалоба 15.08.2026). Код обязан обновляться атомарно,
// данные — переживать деплой; жизненные циклы разведены.
// ⚠️ Имя обязано НЕ оканчиваться на -v<цифры>: только это спасает его от
// предиката ownVersion в activate (страж test_frontend_offline.py).
const DATA_CACHE = `sber-jurist-${SCOPE_NS}-data`;
// Каталог регистрации SW ('/dashboard/') — граница «своих» URL при миграции
// данных из старых кэшей: легаси-кэши формата до v107 были общими на весь
// origin и могут нести записи соседней территории.
const SCOPE_PATH = self.location.pathname.replace(/[^/]*$/, '');

// App shell — то, без чего страница не запустится. Все пути относительные:
// SW регистрируется на /dashboard/service-worker.js, scope = /dashboard/.
// styles.css/app.js без `?v=` — pre-cache на голый URL для офлайна; реальные
// запросы из HTML идут с актуальной `?v=N` и попадают в кэш по cache-first
// после первого fetch, а до него офлайн выручает ignoreSearch-фолбэк
// в cacheFirst. Шрифты — свои woff2 (с 15.08.2026): Google Fonts подключались
// render-blocking `<link>` и при непрогретом кэше офлайн держали белый экран
// до сетевого таймаута — defer-скрипты не исполняются, пока висит стилевой лист.
const APP_SHELL = [
  './',
  './sberbank_dashboard.html',
  './styles.css',
  './app.js',
  './region_front.js',
  './qrcode-gen.js',
  './manifest.json',
  './icon-180.png',
  './icon-192.png',
  './icon-512.png',
  './fonts/ibm-plex-sans-cyrillic.woff2',
  './fonts/ibm-plex-sans-cyrillic-ext.woff2',
  './fonts/ibm-plex-sans-latin.woff2',
  './fonts/ibm-plex-sans-latin-ext.woff2',
];

// Минимальная офлайн-страница на случай, если HTML не оказалось ни в сети, ни в кэше.
const OFFLINE_HTML = `<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Нет связи · СберСуд</title>
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
// ⚠️ cache:'no-cache' обязателен: GitHub Pages отдаёт max-age=600, и в
// 10-минутном окне после деплоя cache.add(url) мог взять ПРОШЛУЮ версию
// файла из HTTP-кэша браузера — офлайн отдавал бы старый JS под новый HTML.
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => Promise.all(APP_SHELL.map(
        (u) => cache.add(new Request(u, { cache: 'no-cache' }))
          .catch((e) => console.warn('SW pre-cache пропуск:', u, e && e.message))
      )))
      .then(() => self.skipWaiting())
  );
});

// ---------- activate: миграция данных + чистка старых кэшей ----------
self.addEventListener('activate', (event) => {
  const allowed = new Set([CACHE_NAME, DATA_CACHE]);
  // Удаляем только кэши СВОЕЙ территории + легаси-имена без территории
  // (формат до v107, одноразово). Сносить «всё не своё» нельзя — на общем
  // origin это живые кэши соседней территории. Проверка остатка через
  // /^v\d+$/ обязательна: префикс 'sber-jurist-dashboard-' — надстрока
  // имени 'sber-jurist-dashboard-ural-v107', голый startsWith снёс бы соседа.
  // Она же щадит неверсионированный DATA_CACHE (остаток 'data') и старый
  // шрифтовой кэш соседа; свои sber-jurist-fonts-<ns>-vN уходят штатно —
  // шрифты с 15.08.2026 локальные и живут в CACHE_NAME.
  const ownVersion = (k) => {
    const prefixes = [`sber-jurist-${SCOPE_NS}-`, `sber-jurist-fonts-${SCOPE_NS}-`];
    return prefixes.some((p) => k.startsWith(p) && /^v\d+$/.test(k.slice(p.length)));
  };
  const legacy = (k) => /^sber-jurist-(fonts-)?v\d+$/.test(k);
  event.waitUntil((async () => {
    const keys = await caches.keys();
    const doomed = keys.filter((k) => !allowed.has(k) && (ownVersion(k) || legacy(k)));
    // Перенос данных ДО удаления: у уже установленных PWA data/*.json ещё
    // лежат в старом версионированном кэше, и переход на новую схему без
    // миграции повторил бы ровно ту потерю, ради которой схема вводится.
    // Устойчивость к обрыву: если браузер убьёт SW посреди activate, старые
    // кэши останутся неудалёнными — их доберёт activate следующего бампа.
    try {
      await migrateDataCache(doomed);
    } catch (e) {
      console.warn('SW: миграция data-кэша не удалась:', e && e.message);
    }
    await Promise.all(doomed.map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

// Одноразовая миграция: копируем записи data/*.json|.ics из приговорённых
// кэшей в DATA_CACHE. Три гарда обязательны:
//   · только свой origin и свой каталог (SCOPE_PATH) — легаси-кэши до v107
//     писались одним именем на весь origin и могут нести записи СОСЕДНЕЙ
//     территории (/dashboard-ural/...), тащить их к себе — мусор в кэше;
//   · уже лежащее в DATA_CACHE не перетираем — там свежее (SWR обновляет
//     его при каждом онлайн-визите);
//   · падение одной записи (квота, битый ответ) не роняет миграцию целиком.
// Цена — одноразовые ~0.3-1.5 с копирования диск-в-диск при первой активации
// новой схемы; со следующего бампа приговорённые кэши несут только код, и
// цикл пробегает по ним мгновенно.
async function migrateDataCache(oldKeys) {
  if (!oldKeys.length) return;
  const data = await caches.open(DATA_CACHE);
  for (const key of oldKeys) {
    const old = await caches.open(key);
    for (const req of await old.keys()) {
      try {
        const u = new URL(req.url);
        if (u.origin !== self.location.origin) continue;
        if (!u.pathname.startsWith(SCOPE_PATH)) continue;
        if (!isDataRequest(u)) continue;
        if (await data.match(req)) continue;
        const res = await old.match(req);
        if (res) await data.put(req, res);
      } catch (e) {
        console.warn('SW: перенос записи не удался:', req.url, e && e.message);
      }
    }
  }
}

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

// ---------- дедлайны ----------
// До 15.08.2026 ни в одной стратегии не было таймаута. При «сеть есть,
// интернета нет» (метро, оператор в минусе, корпоративный Wi-Fi с порталом)
// fetch не падает, а висит до таймаута ОС — десятки секунд белого экрана.
// navigator.onLine тут бесполезен: он говорит «интерфейс поднят», а не
// «интернет есть».
const NAV_TIMEOUT_MS = 3500;     // навигация: кэшированный HTML ждать нечего
const DIGEST_TIMEOUT_MS = 4000;  // last_digest.json ~26 КБ
const STATIC_TIMEOUT_MS = 8000;  // app.js/styles.css при промахе кэша
const DATA_TIMEOUT_MS = 25000;   // страховка SW; страница режет свои fetch'и раньше

// ⚠️ Гонка промисов, а НЕ AbortController — по двум причинам.
// (1) fetch(request, {signal}) пересобирает Request, и у навигационного
//     запроса mode:'navigate' при непустом init схлопывается в 'same-origin' —
//     ломается обработка редиректов навигации.
// (2) Проигравший гонку ответ всё равно ценен: он дотечёт и ляжет в кэш
//     «на следующий раз» (SW держим живым через event.waitUntil).
// Сетевую ошибку и просрочку не различаем — реакция одна: кэш.
const DEADLINE = Symbol('deadline');
function withDeadline(promise, ms) {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve(DEADLINE), ms);
    const done = (v) => { clearTimeout(t); resolve(v); };
    promise.then(done, () => done(DEADLINE));
  });
}

// Запись в кэш НИКОГДА не должна портить уже полученный ответ. До 15.08.2026
// `await cache.put(...)` стоял внутри .then, а .catch(()=>null) висел на всей
// цепочке: QuotaExceededError превращал валидный 200 в null, SWR отдавал
// синтетический 503, app.js падал в catch и подставлял демо-дела. Пишем
// «в сторону»: результат записи интересен только решению «сообщать ли окнам
// об обновлении» (единственный вход cache.put — этот хелпер, страж
// test_frontend_offline.py).
async function cachePutSafe(cache, request, response) {
  try {
    await cache.put(request, response);
    return true;
  } catch (e) {
    console.warn('SW: запись в кэш не удалась (квота?):', request.url, e && e.message);
    return false;
  }
}

// network-first: тянем из сети (не дольше timeoutMs), при сбое — кэш.
// Для критичных файлов, где свежесть важнее скорости (last_digest.json —
// пользователь только что сгенерил его и хочет увидеть СЕЙЧАС) и навигаций
// (иначе PWA залипает на старом HTML со ссылкой на устаревший styles.css?v=N).
async function networkFirst(request, cacheName, event, timeoutMs) {
  const cache = await caches.open(cacheName);
  const network = fetch(request).then(async (res) => {
    if (res && res.ok) await cachePutSafe(cache, request, res.clone());
    return res;
  });
  network.catch(() => {});
  const res = await withDeadline(network, timeoutMs);
  if (res !== DEADLINE && res) return res;
  // Сеть не успела / упала: ответ дотечёт в кэш в фоне, а юзеру — кэш сейчас.
  if (event) event.waitUntil(network.catch(() => {}));
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
        const stored = await cachePutSafe(cache, request, res.clone());
        // Сообщаем только о РЕАЛЬНОМ обновлении уже показанных данных:
        // первая загрузка (кэша не было) и так отдала свежее, версия без
        // ETag/Last-Modified неотличима, а при отказе записи (stored=false)
        // в кэше остался прежний файл — перечитывать его странице незачем,
        // вышел бы тост «Данные обновлены» поверх старых данных.
        if (changed && stored) await notifyDataUpdated(request.url);
      }
      return res;
    })
    .catch(() => null);
  if (cached && event) event.waitUntil(network);
  if (cached) return cached;
  // Кэша нет — ждём сеть, но не вечно: страница отваливается по своему
  // FETCH_TIMEOUT_MS, а SW без дедлайна продолжал бы висеть на запросе.
  const fresh = await withDeadline(network, DATA_TIMEOUT_MS);
  if (fresh !== DEADLINE && fresh) return fresh;
  if (event) event.waitUntil(network);
  return new Response('[]', {
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

// cache-first: сначала кэш, если нет — сеть (не дольше timeoutMs), при
// ответе — кладём в кэш.
async function cacheFirst(request, cacheName, event, timeoutMs) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) return cached;
  const network = fetch(request).then(async (res) => {
    // opaque (no-cors) кладём тоже: размер у него не определён и бьёт по
    // квоте с запасом, но иначе такие ресурсы офлайн не работали бы вовсе.
    if (res && (res.ok || res.type === 'opaque')) {
      await cachePutSafe(cache, request, res.clone());
    }
    return res;
  });
  network.catch(() => {});
  const res = await withDeadline(network, timeoutMs);
  if (res !== DEADLINE && res) return res;
  if (event) event.waitUntil(network.catch(() => {}));
  // ⚠️ Мисс по точному URL — пробуем без querystring. APP_SHELL несёт голые
  // './styles.css' и './app.js', а HTML просит их с актуальной '?v=N' —
  // до этого фолбэка офлайн-старт СРАЗУ после деплоя падал на «HTML из кэша
  // есть, кода нет»: версионированная запись появляется только после первой
  // онлайн-загрузки страницы. Тот же приём стоит для навигаций в networkFirst.
  const bare = await cache.match(request, { ignoreSearch: true });
  if (bare) return bare;
  // Финальный фолбэк для HTML-навигаций — офлайн-страница.
  if (request.mode === 'navigate' || (request.headers.get('accept') || '').includes('text/html')) {
    return new Response(OFFLINE_HTML, {
      status: 200,
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
  throw new Error('SW: ни кэша, ни сети для ' + request.url);
}

// ---------- push: входящее уведомление от сервера ----------
self.addEventListener('push', (event) => {
  const data = event.data
    ? event.data.json()
    : { title: 'СберСуд', body: 'Есть обновления по делам' };

  // URL, который SW откроет по клику. Бэкенд (Python send_web_push) присылает
  // абсолютный путь '/sberbank_dashboard.html?digest=open' — приводим к
  // относительному в рамках scope SW, чтобы работало на GitHub Pages
  // (хостинг под /dashboard/).
  const rawUrl = (data.data && data.data.url) || './sberbank_dashboard.html?digest=open';
  const clickUrl = rawUrl.startsWith('/') ? '.' + rawUrl : rawUrl;

  event.waitUntil(
    self.registration.showNotification(data.title || 'СберСуд', {
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
    event.respondWith(networkFirst(request, DATA_CACHE, event, DIGEST_TIMEOUT_MS));
    return;
  }

  if (isDataRequest(url)) {
    event.respondWith(staleWhileRevalidate(request, DATA_CACHE, event));
    return;
  }

  // HTML (navigate) — networkFirst, чтобы PWA не залипал на старом sberbank_dashboard.html
  // со ссылкой на устаревший styles.css?v=N. Офлайн-fallback — кэш + offline-страница.
  if (request.mode === 'navigate' || (request.headers.get('accept') || '').includes('text/html')) {
    event.respondWith(networkFirst(request, CACHE_NAME, event, NAV_TIMEOUT_MS));
    return;
  }

  // Только same-origin для остального (код, иконки, свои шрифты) — чужие
  // домены пусть идут напрямую.
  if (url.origin === self.location.origin) {
    event.respondWith(cacheFirst(request, CACHE_NAME, event, STATIC_TIMEOUT_MS));
  }
});
