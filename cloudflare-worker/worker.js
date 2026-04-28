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
        if (typeof prev.label === "string") sub.label = prev.label;
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
        label: typeof s.label === "string" ? s.label : "",
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

// Утилиты для всех /admin/<action> endpoints: проверка secret + загрузка
// существующей подписки по endpoint.
async function adminAuthAndLoad(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return { error: new Response("Unauthorized", { status: 401 }) };
  }
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return { error: new Response("Bad JSON", { status: 400 }) };
  }
  const endpoint = body && body.endpoint;
  if (!endpoint || typeof endpoint !== "string") {
    return { error: new Response("Bad Request: endpoint required", { status: 400 }) };
  }
  const key = endpointToKey(endpoint);
  const existing = await env.PUSH_SUBSCRIPTIONS.get(key);
  if (!existing) {
    return {
      error: new Response(
        JSON.stringify({ ok: false, error: "subscription_not_found" }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      ),
    };
  }
  let sub;
  try {
    sub = JSON.parse(existing);
  } catch (_) {
    return { error: new Response("KV corrupt", { status: 500 }) };
  }
  return { sub, key, body };
}

// 1) Назначить/обновить label подписки (отображаемое имя «Иван», и т.п.).
async function handleAdminLabel(request, env) {
  const r = await adminAuthAndLoad(request, env);
  if (r.error) return r.error;
  const label = typeof r.body.label === "string" ? r.body.label.slice(0, 60).trim() : "";
  r.sub.label = label;
  await env.PUSH_SUBSCRIPTIONS.put(r.key, JSON.stringify(r.sub), {
    expirationTtl: 60 * 24 * 3600,
  });
  return new Response(JSON.stringify({ ok: true, label }), {
    headers: { "Content-Type": "application/json" },
  });
}

// 3) Удалить подписку из KV (вместо очистки по 410 Gone).
async function handleAdminUnsubscribe(request, env) {
  const r = await adminAuthAndLoad(request, env);
  if (r.error) return r.error;
  await env.PUSH_SUBSCRIPTIONS.delete(r.key);
  return new Response(JSON.stringify({ ok: true }), {
    headers: { "Content-Type": "application/json" },
  });
}

// 4) Перезаписать watchlist чужой подписки (когда коллега не разобралась
// со звёздочками — админ ставит дела руками).
async function handleAdminWatchlist(request, env) {
  const r = await adminAuthAndLoad(request, env);
  if (r.error) return r.error;
  const wl = Array.isArray(r.body.watchlist) ? r.body.watchlist : null;
  if (!wl) {
    return new Response("Bad Request: watchlist must be array", { status: 400 });
  }
  const cleaned = Array.from(new Set(
    wl.filter((x) => typeof x === "string" && x.length > 0 && x.length < 100).slice(0, 500)
  ));
  r.sub.watchlist = cleaned;
  r.sub.last_watchlist_update_at = new Date().toISOString();
  await env.PUSH_SUBSCRIPTIONS.put(r.key, JSON.stringify(r.sub), {
    expirationTtl: 60 * 24 * 3600,
  });
  return new Response(JSON.stringify({ ok: true, count: cleaned.length }), {
    headers: { "Content-Type": "application/json" },
  });
}

// ── VAPID JWT для тестового push (RFC 8292) ──────────────────────────────────

// VAPID public key захардкожен — он публичный (известен Service Worker'у через
// applicationServerKey) и не секретный. Приватный должен быть в secret
// `VAPID_PRIVATE_KEY` (PEM от py_vapid). Без него тест push возвращает 503.
const VAPID_PUBLIC_KEY = "BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU";
const VAPID_SUB = "mailto:7selivanov.a@gmail.com";

function pemToArrayBuffer(pem) {
  const b64 = pem
    .replace(/-----BEGIN [^-]+-----/g, "")
    .replace(/-----END [^-]+-----/g, "")
    .replace(/\s/g, "");
  const bin = atob(b64);
  const buf = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
  return buf.buffer;
}
function b64urlString(s) {
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function b64urlBytes(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function buildVapidAuth(env, audience) {
  const pem = env.VAPID_PRIVATE_KEY;
  if (!pem) {
    throw new Error("VAPID_PRIVATE_KEY не настроен в Worker — выполни `wrangler secret put VAPID_PRIVATE_KEY`");
  }
  const key = await crypto.subtle.importKey(
    "pkcs8",
    pemToArrayBuffer(pem),
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["sign"]
  );
  const header = b64urlString(JSON.stringify({ typ: "JWT", alg: "ES256" }));
  const claims = b64urlString(JSON.stringify({
    aud: audience,
    exp: Math.floor(Date.now() / 1000) + 12 * 3600,
    sub: VAPID_SUB,
  }));
  const data = new TextEncoder().encode(header + "." + claims);
  const sig = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    key,
    data
  );
  const jwt = header + "." + claims + "." + b64urlBytes(new Uint8Array(sig));
  return { jwt, header: `vapid t=${jwt}, k=${VAPID_PUBLIC_KEY}` };
}

// 5) Тестовый push конкретной подписке. Без encryption: SW сам покажет
// дефолтное уведомление «Сбер Юрист — есть обновления по делам». Этого
// достаточно чтобы убедиться, что push реально доходит до устройства.
async function handleAdminTestPush(request, env) {
  const r = await adminAuthAndLoad(request, env);
  if (r.error) return r.error;
  const endpoint = r.body.endpoint;
  let auth;
  try {
    const ep = new URL(endpoint);
    auth = await buildVapidAuth(env, ep.origin);
  } catch (e) {
    return new Response(
      JSON.stringify({ ok: false, error: e.message }),
      { status: 503, headers: { "Content-Type": "application/json" } }
    );
  }
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: {
        "TTL": "60",
        "Authorization": auth.header,
        "Content-Length": "0",
      },
    });
    if (res.status === 404 || res.status === 410) {
      // Подписка мертва — заодно почистим из KV.
      await env.PUSH_SUBSCRIPTIONS.delete(r.key);
      return new Response(
        JSON.stringify({ ok: false, error: "endpoint_dead", status: res.status, deleted: true }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      return new Response(
        JSON.stringify({ ok: false, status: res.status, body: text.slice(0, 200) }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    return new Response(JSON.stringify({ ok: true, status: res.status }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    return new Response(
      JSON.stringify({ ok: false, error: String(e).slice(0, 200) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
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
.actions { display:flex; flex-wrap:wrap; gap:6px; margin-top:10px; }
.btn { background:var(--bg-2); color:var(--fg-2); border:1px solid var(--border); padding:5px 10px;
       border-radius:6px; font-size:12px; cursor:pointer; font-family:inherit; line-height:1.2; }
.btn:hover { background:var(--bg-1); color:var(--fg); }
.btn-danger:hover { color:#dc2626; border-color:#dc2626; }
.label-name { color:var(--fg); font-weight:600; }
.label-empty { color:var(--fg-3); font-style:italic; font-weight:400; }
.action-flash { font-size:11px; color:var(--fg-3); margin-left:6px; }
.action-flash.ok { color:var(--accent); }
.action-flash.err { color:#dc2626; }
.last-push { margin-top:8px; padding:10px 12px; background:var(--bg-2); border-radius:8px;
             border-left:3px solid var(--accent); font-size:13px; }
.last-push.broadcast { border-left-color:#3b82f6; }
.last-push.general { border-left-color:#f59e0b; }
.last-push.skip { border-left-color:#94a3b8; opacity:0.7; }
.last-push-head { display:flex; gap:8px; align-items:baseline; flex-wrap:wrap; margin-bottom:4px; }
.last-push-variant { font-weight:700; font-size:11px; text-transform:uppercase; letter-spacing:0.5px;
                     padding:1px 8px; border-radius:999px; background:rgba(33,168,92,0.16); color:var(--accent); }
.last-push.broadcast .last-push-variant { background:rgba(59,130,246,0.16); color:#3b82f6; }
.last-push.general .last-push-variant { background:rgba(245,158,11,0.16); color:#b45309; }
.last-push.skip .last-push-variant { background:rgba(148,163,184,0.18); color:var(--fg-3); }
.last-push-title { font-weight:600; color:var(--fg); }
.last-push-body { color:var(--fg-2); margin-top:2px; }
.last-push-meta { color:var(--fg-3); font-size:12px; margin-top:4px; }
.last-push-meta a { color:var(--accent); text-decoration:none; word-break:break-all; }
.last-push-meta a:hover { text-decoration:underline; }
.last-push-empty { color:var(--fg-3); font-style:italic; padding:6px 0 0; font-size:12px; }
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
const PUSHES_URL = "https://selivanovas.github.io/dashboard/data/last_personal_pushes.json";

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
  // Журнал последней push-рассылки. Собираем карту endpoint → запись;
  // если файла нет (старый деплой / только что чистый репо) — пустая карта.
  let pushesMap = new Map();
  let pushesGeneratedAt = "";
  try {
    const r = await fetch(PUSHES_URL, { cache: "no-cache" });
    if (r.ok) {
      const j = await r.json();
      pushesGeneratedAt = j?.generated_at || "";
      for (const item of (j?.items || [])) {
        if (item?.endpoint) pushesMap.set(item.endpoint, item);
      }
    }
  } catch (e) {
    console.warn("last_personal_pushes.json не загружен:", e);
  }
  return { subs, casesMap, pushesMap, pushesGeneratedAt };
}

function renderLastPush(item, generatedAt) {
  if (!item) {
    return generatedAt
      ? '<div class="last-push-empty">Нет записи в журнале последней рассылки (' + escHtml(relTime(generatedAt)) + ')</div>'
      : '<div class="last-push-empty">Журнал push-рассылок пока пуст</div>';
  }
  const labels = {
    personal: "personal",
    general: "general",
    skip: "skip",
    broadcast: "broadcast",
  };
  const v = labels[item.variant] || item.variant || "—";
  const skipped = item.variant === "skip";
  const headTitle = skipped
    ? '<span class="last-push-title">Push не отправлен — нет событий по watchlist</span>'
    : '<span class="last-push-title">' + escHtml(item.title || "—") + '</span>';
  const body = !skipped && item.body
    ? '<div class="last-push-body">' + escHtml(item.body) + '</div>'
    : "";
  const click = !skipped && item.click_url
    ? '<div class="last-push-meta">click_url: <a href="https://selivanovas.github.io/dashboard'
        + escHtml(item.click_url) + '" target="_blank" rel="noopener">'
        + escHtml(item.click_url) + '</a></div>'
    : "";
  const ts = generatedAt
    ? '<div class="last-push-meta">Рассылка: ' + escHtml(relTime(generatedAt)) + '</div>'
    : "";
  return '<div class="last-push ' + escHtml(item.variant || "") + '">'
    + '<div class="last-push-head">'
    +   '<span class="last-push-variant">' + escHtml(v) + '</span>'
    +   headTitle
    + '</div>'
    + body + click + ts
    + '</div>';
}

function renderCard(sub, casesMap, lastPush, pushesGeneratedAt) {
  const dev = escHtml(detectDevice(sub.user_agent));
  const owner = sub.is_owner ? '<span class="badge-owner">★ owner</span>' : "";
  const ep = escHtml((sub.endpoint || "").slice(-48));
  const epAttr = escHtml(sub.endpoint || "");
  const wl = Array.isArray(sub.watchlist) ? sub.watchlist : [];
  const labelHtml = sub.label
    ? '<span class="label-name">'+escHtml(sub.label)+'</span>'
    : '<span class="label-empty">без имени</span>';
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
  return '<div class="sub-card" data-endpoint="'+epAttr+'">'
    + '<div class="sub-row">'
    +   labelHtml
    +   '<span class="sub-device">'+dev+'</span>'
    +   owner
    +   '<span class="kv"><b>Создана:</b> '+escHtml(relTime(sub.created_at))+'</span>'
    +   '<span class="kv"><b>Последний вход:</b> '+escHtml(relTime(sub.last_seen_at))+' <span style="color:var(--fg-3)">('+escHtml(fullDate(sub.last_seen_at))+')</span></span>'
    +   '<span class="kv"><b>Watchlist обновлён:</b> '+escHtml(relTime(sub.last_watchlist_update_at))+'</span>'
    +   '<span class="kv"><b>Дел:</b> '+wl.length+'</span>'
    + '</div>'
    + '<div class="kv endpoint" title="'+ep+'">…'+ep+'</div>'
    + '<div class="actions">'
    +   '<button class="btn" data-action="rename">✏ Имя</button>'
    +   '<button class="btn" data-action="watchlist">📋 Ред. watchlist</button>'
    +   '<button class="btn btn-danger" data-action="delete">🗑 Удалить</button>'
    +   '<span class="action-flash"></span>'
    + '</div>'
    + '<details>'
    +   '<summary>🪞 Последний push для этой подписки</summary>'
    +   renderLastPush(lastPush, pushesGeneratedAt)
    + '</details>'
    + '<details'+(wl.length<=10 ? ' open' : '')+'>'
    +   '<summary>Список отслеживаемых дел ('+wl.length+')</summary>'
    +   '<div class="cases">'+cases+'</div>'
    + '</details>'
    + '</div>';
}

async function postAdmin(path, body) {
  const r = await fetch(path + "?secret=" + encodeURIComponent(SECRET), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  let data = null;
  try { data = await r.json(); } catch (_) {}
  return { ok: r.ok, status: r.status, data };
}

function flash(card, text, kind) {
  const el = card.querySelector(".action-flash");
  if (!el) return;
  el.className = "action-flash " + (kind || "");
  el.textContent = text;
  setTimeout(() => { el.textContent = ""; el.className = "action-flash"; }, 3500);
}

async function handleAction(card, action, currentSub) {
  const endpoint = card.getAttribute("data-endpoint");
  if (!endpoint) return;
  if (action === "rename") {
    const cur = currentSub.label || "";
    const next = prompt("Имя для подписки (Иван, рабочий iPhone и т.п.). Пусто — снять имя.", cur);
    if (next === null) return;
    flash(card, "сохраняю…", "");
    const res = await postAdmin("/admin/label", { endpoint, label: next });
    if (res.ok) { flash(card, "✓ сохранено", "ok"); render(true); }
    else { flash(card, "× ошибка", "err"); }
  } else if (action === "delete") {
    const lbl = currentSub.label ? '"' + currentSub.label + '"' : detectDevice(currentSub.user_agent);
    if (!confirm("Удалить подписку " + lbl + " из KV? Юрист потеряет push до следующего входа в PWA.")) return;
    flash(card, "удаляю…", "");
    const res = await postAdmin("/admin/unsubscribe", { endpoint });
    if (res.ok) { render(true); }
    else { flash(card, "× ошибка", "err"); }
  } else if (action === "watchlist") {
    const cur = (currentSub.watchlist || []).join(", ");
    const next = prompt("Watchlist через запятую. Пусто — очистить.", cur);
    if (next === null) return;
    const list = next.split(",").map((x) => x.trim()).filter(Boolean);
    flash(card, "сохраняю…", "");
    const res = await postAdmin("/admin/watchlist", { endpoint, watchlist: list });
    if (res.ok) { flash(card, "✓ " + (res.data?.count ?? 0) + " дел", "ok"); render(true); }
    else { flash(card, "× ошибка", "err"); }
  }
}

async function render(force) {
  const root = document.getElementById("root");
  if (force) root.className = "loading", root.textContent = "Загрузка…";
  try {
    const { subs, casesMap, pushesMap, pushesGeneratedAt } = await fetchAll();
    const owners = subs.filter((s) => s.is_owner).length;
    const totalWl = subs.reduce((a, s) => a + (s.watchlist?.length || 0), 0);
    const pushTime = pushesGeneratedAt ? " · последний push: " + relTime(pushesGeneratedAt) : "";
    document.getElementById("summary").textContent =
      subs.length + " подписок · " + owners + " owner · " + totalWl + " дел в watchlist'ах" + pushTime;
    // Сортируем: owner вверх, затем по последнему входу (свежие первыми).
    subs.sort((a, b) => {
      if (a.is_owner !== b.is_owner) return a.is_owner ? -1 : 1;
      const ta = new Date(a.last_seen_at || 0).getTime();
      const tb = new Date(b.last_seen_at || 0).getTime();
      return tb - ta;
    });
    root.className = "subs";
    root.innerHTML = subs.map((s) => renderCard(s, casesMap, pushesMap.get(s.endpoint), pushesGeneratedAt)).join("");
    if (subs.length === 0) {
      root.innerHTML = '<div class="empty">Подписок нет.</div>';
    }
    // Делегированный клик по кнопкам действий: ищем data-action на кнопке,
    // ближайший .sub-card — карточка, по data-endpoint находим текущую sub.
    const subsByEp = new Map(subs.map((s) => [s.endpoint, s]));
    root.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-action]");
      if (!btn) return;
      const card = btn.closest(".sub-card");
      if (!card) return;
      const sub = subsByEp.get(card.getAttribute("data-endpoint"));
      if (!sub) return;
      handleAction(card, btn.getAttribute("data-action"), sub);
    });
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

    if (url.pathname === "/admin/label" && request.method === "POST") {
      return handleAdminLabel(request, env);
    }

    if (url.pathname === "/admin/unsubscribe" && request.method === "POST") {
      return handleAdminUnsubscribe(request, env);
    }

    if (url.pathname === "/admin/watchlist" && request.method === "POST") {
      return handleAdminWatchlist(request, env);
    }

    if (url.pathname === "/admin/test-push" && request.method === "POST") {
      return handleAdminTestPush(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },
};
