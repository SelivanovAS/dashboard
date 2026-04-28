// Нерабочие праздничные дни РФ на 2026 год (производственный календарь).
// Постановление Правительства РФ от 24.09.2025 N 1466.
// Обновлять ежегодно после публикации нового постановления.
const HOLIDAYS_2026 = new Set([
  "01-01", "01-02", "01-03", "01-04", "01-05", "01-06", "01-07", "01-08",
  "01-09", // перенос с 03.01 (сб)
  "02-23",
  "03-08", "03-09", // 08.03 (вс) + перенос на 09.03 (пн)
  "05-01",
  "05-09", "05-11", // 09.05 (сб) + перенос на 11.05 (пн)
  "06-12",
  "11-04",
  "12-31", // перенос с 04.01 (вс)
]);

// GitHub Pages URL для CORS
const ALLOWED_ORIGIN = "https://selivanovas.github.io";

function isHoliday(date) {
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const key = `${mm}-${dd}`;
  const year = date.getFullYear();
  const holidays = { 2026: HOLIDAYS_2026 };
  const set = holidays[year];
  return set ? set.has(key) : false;
}

function corsHeaders(origin) {
  const allowed = origin === ALLOWED_ORIGIN || origin === "http://localhost:8081";
  return {
    "Access-Control-Allow-Origin": allowed ? origin : "",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };
}

// ── HTTP-обработчик (push-подписки) ──────────────────────────────────────────

// Ключ KV из endpoint подписки. Хвост endpoint браузерного push-сервиса
// уникален и стабилен в рамках одной подписки.
function endpointToKey(endpoint) {
  const parts = endpoint.split("/");
  return `sub:${parts[parts.length - 1].slice(0, 80)}`;
}

async function handleSubscribe(request, env) {
  const origin = request.headers.get("Origin") || "";
  try {
    const sub = await request.json();
    if (!sub.endpoint) {
      return new Response("Bad Request", { status: 400 });
    }
    const key = endpointToKey(sub.endpoint);
    // Сохраняем флаги, проставленные пользователем ранее — иначе любое
    // освежение подписки (которое PWA делает при каждой загрузке) стирает
    // их: is_owner сломает фильтр тестовых push, watchlist обнулит
    // персональную фильтрацию дайджеста.
    let prev = null;
    const existing = await env.PUSH_SUBSCRIPTIONS.get(key);
    if (existing) {
      try {
        prev = JSON.parse(existing);
        if (prev.is_owner === true) sub.is_owner = true;
        if (Array.isArray(prev.watchlist)) sub.watchlist = prev.watchlist;
        if (prev.created_at) sub.created_at = prev.created_at;
        if (prev.last_watchlist_update_at) {
          sub.last_watchlist_update_at = prev.last_watchlist_update_at;
        }
      } catch (_) { /* игнор: невалидный JSON в KV — перезапишем */ }
    }
    // Метаданные для админки: устройство, когда создана, когда последний
    // раз заходил юрист в PWA. created_at ставим только при первом субскрайбе,
    // last_seen_at обновляем на каждом /subscribe (PWA дёргает его при открытии).
    sub.user_agent = request.headers.get("User-Agent") || "";
    if (!sub.created_at) sub.created_at = new Date().toISOString();
    sub.last_seen_at = new Date().toISOString();
    // TTL 60 дней — браузер обновит подписку сам при следующем открытии
    await env.PUSH_SUBSCRIPTIONS.put(key, JSON.stringify(sub), {
      expirationTtl: 60 * 24 * 3600,
    });
    console.log(`Подписка сохранена: ${key}${sub.is_owner ? " (owner)" : ""}`);
    // Возвращаем сохранённый watchlist — клиент использует его при первой
    // загрузке после переустановки PWA, чтобы восстановить локальный список
    // отслеживаемых дел без принуждения юриста кликать звёздочки заново.
    return new Response(JSON.stringify({
      ok: true,
      watchlist: Array.isArray(sub.watchlist) ? sub.watchlist : [],
    }), {
      headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
    });
  } catch (e) {
    console.error("subscribe error:", e);
    return new Response("Error", { status: 500 });
  }
}

async function handleSetWatchlist(request, env) {
  const origin = request.headers.get("Origin") || "";
  try {
    const body = await request.json();
    const endpoint = body.endpoint;
    const watchlist = body.watchlist;
    if (!endpoint || typeof endpoint !== "string" || !Array.isArray(watchlist)) {
      return new Response("Bad Request", {
        status: 400,
        headers: corsHeaders(origin),
      });
    }
    // Чистим: только строки, обрезаем длину, дедупим. Без auth — защита
    // через привязку к существующему endpoint: чужой endpoint узнать
    // нельзя, а перезаписать запись чужого юриста — только зная его.
    const cleaned = Array.from(new Set(
      watchlist
        .filter((x) => typeof x === "string" && x.length > 0 && x.length < 100)
        .slice(0, 500)
    ));
    const key = endpointToKey(endpoint);
    const existing = await env.PUSH_SUBSCRIPTIONS.get(key);
    if (!existing) {
      return new Response(
        JSON.stringify({ ok: false, error: "subscription_not_found" }),
        {
          status: 404,
          headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
        }
      );
    }
    const sub = JSON.parse(existing);
    sub.watchlist = cleaned;
    sub.last_watchlist_update_at = new Date().toISOString();
    await env.PUSH_SUBSCRIPTIONS.put(key, JSON.stringify(sub), {
      expirationTtl: 60 * 24 * 3600,
    });
    console.log(`Watchlist обновлён (${cleaned.length} дел): ${key}`);
    return new Response(JSON.stringify({ ok: true, count: cleaned.length }), {
      headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
    });
  } catch (e) {
    console.error("watchlist error:", e);
    return new Response("Error", { status: 500, headers: corsHeaders(origin) });
  }
}

async function handleUnsubscribe(request, env) {
  // Удалить подписку из KV. Используется автоочисткой из Python: при
  // получении 410/404 от push-сервиса (FCM/Mozilla/APNs) подписка мёртвая и
  // её надо вычистить, иначе она будет ронять каждый прогон. Авторизация
  // через PUSH_SECRET — тот же шаблон, что и /subscriptions.
  const auth = request.headers.get("Authorization") || "";
  if (!env.PUSH_SECRET || auth !== `Bearer ${env.PUSH_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  try {
    const body = await request.json();
    const endpoint = body && body.endpoint;
    if (!endpoint || typeof endpoint !== "string") {
      return new Response("Bad Request", { status: 400 });
    }
    const key = endpointToKey(endpoint);
    const existed = await env.PUSH_SUBSCRIPTIONS.get(key);
    await env.PUSH_SUBSCRIPTIONS.delete(key);
    console.log(`Подписка удалена: ${key} (${existed ? "была" : "не было"})`);
    return new Response(JSON.stringify({ ok: true, existed: !!existed }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("unsubscribe error:", e);
    return new Response("Error", { status: 500 });
  }
}

async function handleListSubscriptions(request, env) {
  const auth = request.headers.get("Authorization") || "";
  if (!env.PUSH_SECRET || auth !== `Bearer ${env.PUSH_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  try {
    const url = new URL(request.url);
    const ownerOnly = url.searchParams.get("role") === "owner";
    const list = await env.PUSH_SUBSCRIPTIONS.list({ prefix: "sub:" });
    const subs = await Promise.all(
      list.keys.map(async (k) => {
        const val = await env.PUSH_SUBSCRIPTIONS.get(k.name);
        return val ? JSON.parse(val) : null;
      })
    );
    // Фильтр owner: только подписки, помеченные через POST /mark-owner.
    // Поле is_owner добавляется на запись в KV, в push-payload не уходит.
    const filtered = subs.filter((s) => {
      if (!s) return false;
      return ownerOnly ? s.is_owner === true : true;
    });
    return new Response(JSON.stringify(filtered), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("list error:", e);
    return new Response("Error", { status: 500 });
  }
}

async function handleMarkOwner(request, env) {
  const origin = request.headers.get("Origin") || "";
  const auth = request.headers.get("Authorization") || "";
  if (!env.OWNER_SECRET || auth !== `Bearer ${env.OWNER_SECRET}`) {
    return new Response("Unauthorized", {
      status: 401,
      headers: corsHeaders(origin),
    });
  }
  try {
    const body = await request.json();
    const endpoint = body.endpoint;
    if (!endpoint || typeof endpoint !== "string") {
      return new Response("Bad Request", {
        status: 400,
        headers: corsHeaders(origin),
      });
    }
    const key = endpointToKey(endpoint);
    const existing = await env.PUSH_SUBSCRIPTIONS.get(key);
    if (!existing) {
      // Подписка не зарегистрирована — попросим клиент сначала /subscribe.
      return new Response(
        JSON.stringify({ ok: false, error: "subscription_not_found" }),
        {
          status: 404,
          headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
        }
      );
    }
    const sub = JSON.parse(existing);
    sub.is_owner = true;
    await env.PUSH_SUBSCRIPTIONS.put(key, JSON.stringify(sub), {
      expirationTtl: 60 * 24 * 3600,
    });
    console.log(`Подписка помечена как owner: ${key}`);
    return new Response(JSON.stringify({ ok: true }), {
      headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
    });
  } catch (e) {
    console.error("mark-owner error:", e);
    return new Response("Error", { status: 500, headers: corsHeaders(origin) });
  }
}

// ── Админка подписчиков ───────────────────────────────────────────────────────

// Возвращает JSON со всеми подписками (как /subscriptions, но авторизация
// через ?secret=<OWNER_SECRET> в URL — чтобы HTML-страница могла дёрнуть
// данные без хранения PUSH_SECRET в JS-коде в браузере).
async function handleAdminData(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  try {
    const list = await env.PUSH_SUBSCRIPTIONS.list({ prefix: "sub:" });
    const subs = await Promise.all(
      list.keys.map(async (k) => {
        const val = await env.PUSH_SUBSCRIPTIONS.get(k.name);
        return val ? JSON.parse(val) : null;
      })
    );
    // Не отдаём приватные части push-подписки (auth/p256dh) — админке они
    // не нужны, а светить через GET-параметр в URL secret лишний раз
    // не стоит.
    const safe = subs
      .filter((s) => s)
      .map((s) => ({
        endpoint: s.endpoint || "",
        is_owner: s.is_owner === true,
        watchlist: Array.isArray(s.watchlist) ? s.watchlist : [],
        user_agent: s.user_agent || "",
        created_at: s.created_at || "",
        last_seen_at: s.last_seen_at || "",
        last_watchlist_update_at: s.last_watchlist_update_at || "",
      }));
    return new Response(JSON.stringify(safe), {
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });
  } catch (e) {
    console.error("admin/data error:", e);
    return new Response("Error", { status: 500 });
  }
}

// HTML-страница админки. Открывается напрямую в браузере по URL
// `/admin?secret=<OWNER_SECRET>`. Содержит inline-стили и JS, который
// тянет /admin/data (с тем же secret) и cases.json с GitHub Pages.
async function handleAdmin(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  // Embed secret в HTML, чтобы JS мог дёрнуть /admin/data. Secret уже в URL,
  // дополнительная утечка минимальна, но всё равно экранируем кавычки.
  const safeSecret = secret.replace(/[<>"&']/g, "");
  const html = renderAdminHtml(safeSecret);
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function renderAdminHtml(secret) {
  return `<!doctype html><html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Подписчики · мониторинг дел</title>
<style>
:root {
  color-scheme: light dark;
  --fg: #14181f; --fg-2: #4a5160; --fg-3: #707788;
  --bg: #f7f9fb; --bg-1: #fff; --bg-2: #eef1f5;
  --border: #e0e4eb; --accent: #21a038; --amber: #f59e0b;
}
@media (prefers-color-scheme: dark) {
  :root { --fg:#e8ecf2; --fg-2:#aab1bf; --fg-3:#7a8090; --bg:#0e1116; --bg-1:#161b22; --bg-2:#1f252e; --border:#2a313c; }
}
* { box-sizing: border-box; }
body { margin:0; padding:16px; font-family:-apple-system,system-ui,Segoe UI,Roboto,sans-serif;
       background:var(--bg); color:var(--fg); font-size:14px; line-height:1.5; }
header { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;
         margin-bottom:16px; padding-bottom:12px; border-bottom:1px solid var(--border); }
h1 { margin:0; font-size:18px; font-weight:600; }
.refresh { background:var(--accent); color:#fff; border:0; padding:8px 14px; border-radius:8px;
           font-size:13px; font-weight:600; cursor:pointer; font-family:inherit; }
.refresh:hover { opacity:0.92; }
.summary { color:var(--fg-3); font-size:13px; }
.subs { display:flex; flex-direction:column; gap:10px; }
.sub-card { background:var(--bg-1); border:1px solid var(--border); border-radius:10px; padding:12px 14px; }
.sub-row { display:flex; flex-wrap:wrap; gap:10px 18px; align-items:baseline; }
.sub-device { font-weight:600; }
.badge-owner { display:inline-block; background:rgba(245,158,11,0.14); color:var(--amber);
               padding:2px 8px; border-radius:999px; font-size:11px; font-weight:700; letter-spacing:0.4px; }
.kv { color:var(--fg-3); font-size:12px; }
.kv b { color:var(--fg-2); font-weight:500; }
.endpoint { font-family:ui-monospace,Menlo,monospace; color:var(--fg-3); font-size:11px;
            overflow:hidden; text-overflow:ellipsis; max-width:220px; white-space:nowrap; }
details { margin-top:10px; }
details > summary { cursor:pointer; color:var(--fg-2); font-size:13px; padding:6px 0; outline:none;
                    user-select:none; }
details > summary:hover { color:var(--fg); }
.cases { margin-top:6px; padding-left:8px; border-left:2px solid var(--border); display:flex;
         flex-direction:column; gap:4px; }
.case-row { display:flex; gap:8px; flex-wrap:wrap; align-items:baseline; padding:4px 0;
            border-bottom:1px dashed var(--border); }
.case-row:last-child { border-bottom:0; }
.case-num { font-family:ui-monospace,Menlo,monospace; font-weight:600; color:var(--accent); min-width:140px; }
.case-parties { color:var(--fg-2); }
.case-meta { color:var(--fg-3); font-size:12px; }
.empty { color:var(--fg-3); font-style:italic; padding:6px 0; }
.error { color:#dc2626; padding:12px; background:rgba(220,38,38,0.08); border-radius:8px; }
.loading { color:var(--fg-3); padding:24px; text-align:center; }
@media (max-width: 600px) {
  .endpoint { max-width:100%; white-space:normal; word-break:break-all; }
  .case-num { min-width:auto; }
}
</style>
</head><body>
<header>
  <h1>📡 Подписчики · мониторинг дел Сбера</h1>
  <div style="display:flex;gap:8px;align-items:center;">
    <span class="summary" id="summary">…</span>
    <button class="refresh" onclick="render(true)">Обновить</button>
  </div>
</header>
<div id="root" class="loading">Загрузка…</div>
<script>
const SECRET = ${JSON.stringify(secret)};
const CASES_URL = "https://selivanovas.github.io/dashboard/data/cases.json";

function bareCaseNumber(n) {
  return String(n || "").trim().split(/[\\s(]/)[0];
}
function detectDevice(ua) {
  if (!ua) return "—";
  const s = ua;
  let os = "?", browser = "?";
  if (/iPhone|iPad|iPod/.test(s)) os = /iPad/.test(s) ? "iPad" : "iPhone";
  else if (/Android/.test(s)) os = "Android";
  else if (/Macintosh/.test(s)) os = "macOS";
  else if (/Windows/.test(s)) os = "Windows";
  else if (/Linux/.test(s)) os = "Linux";
  if (/Edg\\//.test(s)) browser = "Edge";
  else if (/OPR\\/|Opera/.test(s)) browser = "Opera";
  else if (/YaBrowser/.test(s)) browser = "Yandex";
  else if (/Firefox/.test(s)) browser = "Firefox";
  else if (/Chrome/.test(s)) browser = "Chrome";
  else if (/Safari/.test(s)) browser = "Safari";
  return os + " · " + browser;
}
function relTime(iso) {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (isNaN(t)) return "—";
  const diff = Math.round((Date.now() - t) / 1000);
  if (diff < 60) return "только что";
  if (diff < 3600) return Math.floor(diff/60) + " мин назад";
  if (diff < 86400) return Math.floor(diff/3600) + " ч назад";
  if (diff < 86400*2) return "вчера в " + new Date(iso).toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit"});
  if (diff < 86400*30) return Math.floor(diff/86400) + " дн назад";
  return new Date(iso).toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric"});
}
function fullDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"});
}
function escHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

async function fetchAll() {
  const subsRes = await fetch("/admin/data?secret=" + encodeURIComponent(SECRET));
  if (!subsRes.ok) throw new Error("HTTP " + subsRes.status + " /admin/data");
  const subs = await subsRes.json();
  let casesMap = new Map();
  try {
    const casesRes = await fetch(CASES_URL, { cache: "no-cache" });
    if (casesRes.ok) {
      const casesJson = await casesRes.json();
      const list = Array.isArray(casesJson?.cases) ? casesJson.cases : [];
      for (const c of list) {
        const id = bareCaseNumber(c.id);
        if (!id) continue;
        casesMap.set(id, {
          plaintiff: c.plaintiff || "",
          defendant: c.defendant || "",
          court: c.first_instance?.court || c.appeal?.court || "",
          stage: c.current_stage || "",
        });
      }
    }
  } catch (e) {
    console.warn("cases.json не загружен:", e);
  }
  return { subs, casesMap };
}

function renderCard(sub, casesMap) {
  const dev = escHtml(detectDevice(sub.user_agent));
  const owner = sub.is_owner ? '<span class="badge-owner">★ owner</span>' : "";
  const ep = escHtml((sub.endpoint || "").slice(-48));
  const wl = Array.isArray(sub.watchlist) ? sub.watchlist : [];
  const cases = wl.length
    ? wl.map((num) => {
        const bare = bareCaseNumber(num);
        const c = casesMap.get(bare);
        if (c) {
          const parties = (c.plaintiff && c.defendant)
            ? escHtml(c.plaintiff) + ' <span style="color:var(--fg-3)">vs</span> ' + escHtml(c.defendant)
            : escHtml(c.plaintiff || c.defendant || "");
          return '<div class="case-row"><span class="case-num">'+escHtml(num)+'</span>'
                 + '<span class="case-parties">'+parties+'</span>'
                 + (c.court ? '<span class="case-meta">· '+escHtml(c.court)+'</span>' : '')
                 + '</div>';
        }
        return '<div class="case-row"><span class="case-num">'+escHtml(num)+'</span>'
               + '<span class="case-meta">· нет в cases.json</span></div>';
      }).join("")
    : '<div class="empty">Юрист не отслеживает ни одно дело</div>';
  return '<div class="sub-card">'
    + '<div class="sub-row">'
    +   '<span class="sub-device">'+dev+'</span>'
    +   owner
    +   '<span class="kv"><b>Создана:</b> '+escHtml(relTime(sub.created_at))+'</span>'
    +   '<span class="kv"><b>Последний вход:</b> '+escHtml(relTime(sub.last_seen_at))+' <span style="color:var(--fg-3)">('+escHtml(fullDate(sub.last_seen_at))+')</span></span>'
    +   '<span class="kv"><b>Watchlist обновлён:</b> '+escHtml(relTime(sub.last_watchlist_update_at))+'</span>'
    +   '<span class="kv"><b>Дел:</b> '+wl.length+'</span>'
    + '</div>'
    + '<div class="kv endpoint" title="'+ep+'">…'+ep+'</div>'
    + '<details'+(wl.length<=10 ? ' open' : '')+'>'
    +   '<summary>Список отслеживаемых дел ('+wl.length+')</summary>'
    +   '<div class="cases">'+cases+'</div>'
    + '</details>'
    + '</div>';
}

async function render(force) {
  const root = document.getElementById("root");
  if (force) root.className = "loading", root.textContent = "Загрузка…";
  try {
    const { subs, casesMap } = await fetchAll();
    const owners = subs.filter((s) => s.is_owner).length;
    const totalWl = subs.reduce((a, s) => a + (s.watchlist?.length || 0), 0);
    document.getElementById("summary").textContent =
      subs.length + " подписок · " + owners + " owner · " + totalWl + " дел в watchlist'ах";
    // Сортируем: owner вверх, затем по последнему входу (свежие первыми).
    subs.sort((a, b) => {
      if (a.is_owner !== b.is_owner) return a.is_owner ? -1 : 1;
      const ta = new Date(a.last_seen_at || 0).getTime();
      const tb = new Date(b.last_seen_at || 0).getTime();
      return tb - ta;
    });
    root.className = "subs";
    root.innerHTML = subs.map((s) => renderCard(s, casesMap)).join("");
    if (subs.length === 0) {
      root.innerHTML = '<div class="empty">Подписок нет.</div>';
    }
  } catch (e) {
    root.className = "error";
    root.textContent = "Ошибка: " + e.message;
  }
}

render();
</script>
</body></html>`;
}

// ── Экспорт ───────────────────────────────────────────────────────────────────

export default {
  // ── Cron-триггер: запуск GitHub Actions ─────────────────────────────────
  async scheduled(event, env) {
    // Текущая дата по МСК (UTC+3)
    const now = new Date(Date.now() + 3 * 3600 * 1000);

    if (isHoliday(now)) {
      console.log(`Пропуск: ${now.toISOString().slice(0, 10)} — праздничный день`);
      return;
    }

    const response = await fetch(
      "https://api.github.com/repos/SelivanovAS/dashboard/actions/workflows/update_cases.yml/dispatches",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.GITHUB_PAT}`,
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "CloudflareWorker",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );

    if (response.ok) {
      console.log(`dispatch ok: ${response.status}`);
    } else {
      const body = await response.text();
      const bodyPreview = body.length > 500 ? body.slice(0, 500) + "..." : body;
      console.error(
        `dispatch failed: ${response.status} ${response.statusText} | body: ${bodyPreview}`
      );
    }
  },

  // ── HTTP-обработчик: управление push-подписками ──────────────────────────
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";

    // Preflight CORS
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (url.pathname === "/subscribe" && request.method === "POST") {
      return handleSubscribe(request, env);
    }

    if (url.pathname === "/unsubscribe" && request.method === "POST") {
      return handleUnsubscribe(request, env);
    }

    if (url.pathname === "/subscriptions" && request.method === "GET") {
      return handleListSubscriptions(request, env);
    }

    if (url.pathname === "/mark-owner" && request.method === "POST") {
      return handleMarkOwner(request, env);
    }

    if (url.pathname === "/watchlist" && request.method === "POST") {
      return handleSetWatchlist(request, env);
    }

    if (url.pathname === "/admin" && request.method === "GET") {
      return handleAdmin(request, env);
    }

    if (url.pathname === "/admin/data" && request.method === "GET") {
      return handleAdminData(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },
};
