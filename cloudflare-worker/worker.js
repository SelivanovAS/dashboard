import { renderAdminHtml } from "./admin_page.js";

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
  // Второй щит: суббота/воскресенье — нерабочие дни. Защищает от сюрпризов
  // cron-парсера (см. wrangler.toml) и от ручной правки расписания.
  // getDay(): 0 = Sunday, 6 = Saturday — стандарт JS.
  const dow = date.getDay();
  if (dow === 0 || dow === 6) return true;
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

// ── Канонизация watchlist (Этап 4c) ──────────────────────────────────────────
// При POST /watchlist и /admin/watchlist прогоняем входящие номера через
// alias-карту от текущего cases.json. ★ на апел./касс./hybrid → канон. FI-ID.
// Идея зеркальная Этапу 4a (Python) и Этапу 1 (inline-JS админки).

const CASES_DATA_URL = "https://selivanovas.github.io/dashboard/data/cases.json";

function wnBareCaseNumber(n) {
  return String(n || "").trim().split(/[\s(]/)[0];
}
function wnExtractParenNumbers(s) {
  const m = String(s || "").match(/\(([^)]+)\)/);
  if (!m) return [];
  return m[1].split(/[;,]/).map((x) => wnBareCaseNumber(x)).filter(Boolean);
}
function wnBuildAliasToCanonical(cases) {
  const map = new Map();
  for (const c of cases || []) {
    const canonical = wnBareCaseNumber(c.id);
    if (!canonical) continue;
    const fi = c.first_instance || {};
    const ap = c.appeal || {};
    const ca = c.cassation || {};
    const candidates = [
      c.id,
      fi.case_number, fi.material_number,  // material_number — М-предок (Этап 3)
      ap.case_number,
      ca.case_number, ca.cassation_number,
      ...wnExtractParenNumbers(c.id),
    ];
    for (const raw of candidates) {
      const bare = wnBareCaseNumber(raw);
      if (bare && !map.has(bare)) map.set(bare, canonical);
    }
  }
  return map;
}
// Возвращает Map<bare → canonical> от свежего cases.json через CF edge cache.
// TTL 300s — cases.json регенерируется кроном раз в день, держать дольше
// нет смысла, держать короче — лишние fetch'и. Если cases.json недоступен
// (ошибка сети или 5xx), возвращает null — в этом случае канонизация
// пропускается и в KV ложится то, что отправил клиент.
async function getAliasMapCached() {
  try {
    const r = await fetch(CASES_DATA_URL, {
      cf: { cacheTtl: 300, cacheEverything: true },
    });
    if (!r.ok) return null;
    const j = await r.json();
    const list = Array.isArray(j?.cases) ? j.cases : [];
    return wnBuildAliasToCanonical(list);
  } catch (e) {
    console.warn("Канонизация watchlist: cases.json недоступен:", e);
    return null;
  }
}
// Канонизирует массив номеров через alias-карту. Дедупит, сохраняет порядок.
// Если aliasMap = null — возвращает исходный массив без изменений.
function canonicalizeWatchlistArr(arr, aliasMap) {
  if (!aliasMap) return arr;
  const out = [];
  const seen = new Set();
  for (const x of arr || []) {
    const bare = wnBareCaseNumber(x);
    if (!bare) continue;
    const canonical = aliasMap.get(bare) || bare;
    if (!seen.has(canonical)) {
      seen.add(canonical);
      out.push(canonical);
    }
  }
  return out;
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
    // Канонизация: апел./касс./hybrid → канон. FI-ID. Если cases.json
    // недоступен (edge cache промахнулся + ошибка сети) — сохраняем cleaned
    // как есть, фильтр Python всё равно расширит через алиасы (Этап 4a).
    const aliasMap = await getAliasMapCached();
    const canonical = canonicalizeWatchlistArr(cleaned, aliasMap);
    const sub = JSON.parse(existing);
    sub.watchlist = canonical;
    sub.last_watchlist_update_at = new Date().toISOString();
    await env.PUSH_SUBSCRIPTIONS.put(key, JSON.stringify(sub), {
      expirationTtl: 60 * 24 * 3600,
    });
    console.log(
      `Watchlist обновлён (${canonical.length} дел, ` +
      `${cleaned.length - canonical.length} алиасов схлопнуто): ${key}`
    );
    return new Response(
      JSON.stringify({ ok: true, count: canonical.length, canonical }),
      { headers: { "Content-Type": "application/json", ...corsHeaders(origin) } }
    );
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

// ── Живой лог прогона ────────────────────────────────────────────────────────
// Канал общий для двух отправителей (одновременно они не работают — Mac спит;
// если бы работали, current/prev пинг-понговали бы ротацией по run_id):
// - GitHub Actions (scripts/gh_progress_pusher.py, source="github") — весь
//   лог основного прогона update_cases.yml, батчами;
// - Mac-резерв (ops/mac-local-run/parse_and_push.sh → progress_pusher.py,
//   без source) — только вехи парсинга.
// Auth — низкопривилегированный PROGRESS_SECRET (умеет ТОЛЬКО дописывать
// строки прогресса, доступа к подпискам/делам не даёт) ИЛИ PUSH_SECRET (он
// уже есть в GitHub secrets и привилегированнее — ничего не ослабляет).
// Ключи progress:* не пересекаются с подписками — все выборки подписок идут
// по префиксу "sub:".
async function handleRunProgress(request, env) {
  const auth = request.headers.get("Authorization") || "";
  const okProgress = env.PROGRESS_SECRET && auth === `Bearer ${env.PROGRESS_SECRET}`;
  const okPush = env.PUSH_SECRET && auth === `Bearer ${env.PUSH_SECRET}`;
  if (!okProgress && !okPush) {
    return new Response("Unauthorized", { status: 401 });
  }
  try {
    const body = await request.json();
    const runId = String(body.run_id || "");
    const newLines = Array.isArray(body.lines)
      ? body.lines.map(String).slice(0, 100)
      : [];
    if (!runId) return new Response("Bad Request", { status: 400 });
    // Источник прогона: старый Mac-пушер поля не шлёт → "mac" (обратная
    // совместимость), gh_progress_pusher.py шлёт "github" + link на run.
    const source = body.source === "github" ? "github" : "mac";
    const link = (typeof body.link === "string" && /^https:\/\//.test(body.link))
      ? body.link.slice(0, 300)
      : "";

    const now = new Date().toISOString();
    const raw = await env.PUSH_SUBSCRIPTIONS.get("progress:current");
    let cur = null;
    try { cur = raw ? JSON.parse(raw) : null; } catch (_) { cur = null; }

    if (cur && cur.run_id !== runId) {
      // Начался новый прогон — прежний уезжает в progress:prev.
      await env.PUSH_SUBSCRIPTIONS.put("progress:prev", JSON.stringify(cur), {
        expirationTtl: 14 * 24 * 3600,
      });
      cur = null;
    }
    if (!cur) cur = { run_id: runId, started_at: now, lines: [], source };
    if (link && !cur.link) cur.link = link;
    // Cap 1000 (было 300): облачный прогон шлёт весь лог (~350 строк INFO);
    // DEBUG-прогон срежет ранние строки — админка мягко деградирует.
    cur.lines = cur.lines.concat(newLines).slice(-1000);
    cur.updated_at = now;
    if (body.done === true) cur.done = true;
    await env.PUSH_SUBSCRIPTIONS.put("progress:current", JSON.stringify(cur), {
      expirationTtl: 14 * 24 * 3600,
    });
    return new Response(JSON.stringify({ ok: true, total: cur.lines.length }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("run-progress error:", e);
    return new Response("Error", { status: 500 });
  }
}

// JSON для блока «🛰 Парсинг» в админке: текущий и предыдущий прогон.
async function handleAdminRunProgress(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  try {
    const [curRaw, prevRaw] = await Promise.all([
      env.PUSH_SUBSCRIPTIONS.get("progress:current"),
      env.PUSH_SUBSCRIPTIONS.get("progress:prev"),
    ]);
    const parse = (s) => {
      try { return s ? JSON.parse(s) : null; } catch (_) { return null; }
    };
    return new Response(
      JSON.stringify({ current: parse(curRaw), prev: parse(prevRaw) }),
      { headers: { "Content-Type": "application/json; charset=utf-8" } }
    );
  } catch (e) {
    console.error("admin/run-progress error:", e);
    return new Response("Error", { status: 500 });
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
  // Канонизация — та же логика что в /watchlist (handleSetWatchlist).
  // Python (Этап 4b) сюда шлёт уже канон. версию; повторная канонизация
  // идемпотентна. Админ через UI может прислать апел./касс. номер —
  // схлопнем в канон.
  const aliasMap = await getAliasMapCached();
  const canonical = canonicalizeWatchlistArr(cleaned, aliasMap);
  r.sub.watchlist = canonical;
  r.sub.last_watchlist_update_at = new Date().toISOString();
  await env.PUSH_SUBSCRIPTIONS.put(r.key, JSON.stringify(r.sub), {
    expirationTtl: 60 * 24 * 3600,
  });
  return new Response(
    JSON.stringify({ ok: true, count: canonical.length, canonical }),
    { headers: { "Content-Type": "application/json" } }
  );
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

// ── Прогоны GitHub Actions для админки ──────────────────────────────────────

const GH_REPO_API = "https://api.github.com/repos/SelivanovAS/dashboard";

// Ближайший запуск cron'а Worker'а (45 3 * * mon-fri UTC) с учётом праздников
// РФ — зеркалит scheduled(): день оценивается по МСК (UTC+3).
function nextCronAt() {
  const now = new Date();
  for (let i = 0; i < 30; i++) {
    const day = new Date(now.getTime() + i * 86400000);
    const fire = new Date(Date.UTC(
      day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), 3, 45, 0
    ));
    if (fire.getTime() <= now.getTime()) continue;
    const msk = new Date(fire.getTime() + 3 * 3600 * 1000);
    if (isHoliday(msk)) continue;
    return fire.toISOString();
  }
  return null;
}

// JSON для блока «🚀 Прогоны»: последние runs GitHub Actions. PAT остаётся
// на сервере — страница ходит сюда со своим OWNER_SECRET.
async function handleAdminGhRuns(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  const ghHeaders = {
    Authorization: `Bearer ${env.GITHUB_PAT}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "CloudflareWorker",
  };
  const mapRun = (run) => ({
    name: run.name || "",
    path: run.path || "",
    status: run.status || "",
    conclusion: run.conclusion || "",
    run_started_at: run.run_started_at || "",
    updated_at: run.updated_at || "",
    html_url: run.html_url || "",
    run_number: run.run_number || 0,
    event: run.event || "",
  });
  try {
    // Общий список + отдельно последний запуск основного workflow: пары
    // «Тесты+Pages» от частых пушей вытесняют его из первых 20 runs, а
    // плитке «Последний прогон» нужен именно он.
    const [r, rMain] = await Promise.all([
      fetch(GH_REPO_API + "/actions/runs?per_page=20", { headers: ghHeaders }),
      fetch(GH_REPO_API + "/actions/workflows/update_cases.yml/runs?per_page=1", { headers: ghHeaders })
        .catch(() => null),
    ]);
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      return new Response(
        JSON.stringify({
          error: `GitHub ${r.status}`,
          detail: text.slice(0, 200),
          next_cron_at: nextCronAt(),
        }),
        { status: 502, headers: { "Content-Type": "application/json; charset=utf-8" } }
      );
    }
    const j = await r.json();
    const runs = (j.workflow_runs || []).map(mapRun);
    let mainRun = null;
    if (rMain && rMain.ok) {
      const jm = await rMain.json().catch(() => null);
      if (jm && Array.isArray(jm.workflow_runs) && jm.workflow_runs.length) {
        mainRun = mapRun(jm.workflow_runs[0]);
      }
    }
    return new Response(
      JSON.stringify({ runs, main_run: mainRun, next_cron_at: nextCronAt() }),
      { headers: { "Content-Type": "application/json; charset=utf-8" } }
    );
  } catch (e) {
    console.error("admin/gh-runs error:", e);
    return new Response(
      JSON.stringify({ error: String(e).slice(0, 200), next_cron_at: null }),
      { status: 500, headers: { "Content-Type": "application/json; charset=utf-8" } }
    );
  }
}

// Белый список запуска workflow из админки: только эти файлы и только эти
// inputs. Значения — строки («true»/«false» для булевых — так требует
// GitHub REST API, тип из workflow_dispatch он приводит сам).
const DISPATCH_WORKFLOWS = {
  "update_cases.yml": new Set(["to_group", "smart_skip"]),
  "test_digest.yml": new Set([
    "to_group", "push_all", "full_llm", "llm_provider",
    "claude_model", "claude_effort", "gigachat_model", "openrouter_model",
    "llm_model", "commit_results",
  ]),
};

// Запуск workflow по кнопке из админки (workflow_dispatch, ветка main).
async function handleAdminDispatch(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!env.OWNER_SECRET || secret !== env.OWNER_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return new Response("Bad JSON", { status: 400 });
  }
  const jsonHeaders = { "Content-Type": "application/json; charset=utf-8" };
  const workflow = String((body && body.workflow) || "");
  const allowed = DISPATCH_WORKFLOWS[workflow];
  if (!allowed) {
    return new Response(
      JSON.stringify({ ok: false, error: `workflow не в белом списке: ${workflow}` }),
      { status: 400, headers: jsonHeaders }
    );
  }
  const inputs = {};
  const src = body.inputs && typeof body.inputs === "object" ? body.inputs : {};
  for (const [k, v] of Object.entries(src)) {
    if (!allowed.has(k)) {
      return new Response(
        JSON.stringify({ ok: false, error: `input не разрешён: ${k}` }),
        { status: 400, headers: jsonHeaders }
      );
    }
    if (typeof v !== "string" || v.length > 100) {
      return new Response(
        JSON.stringify({ ok: false, error: `input ${k}: ожидается строка ≤100 символов` }),
        { status: 400, headers: jsonHeaders }
      );
    }
    inputs[k] = v;
  }
  try {
    const r = await fetch(
      `${GH_REPO_API}/actions/workflows/${workflow}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.GITHUB_PAT}`,
          Accept: "application/vnd.github+json",
          "User-Agent": "CloudflareWorker",
        },
        body: JSON.stringify({ ref: "main", inputs }),
      }
    );
    if (r.status === 204) {
      console.log(`admin dispatch ok: ${workflow} ${JSON.stringify(inputs)}`);
      return new Response(JSON.stringify({ ok: true }), { headers: jsonHeaders });
    }
    const text = await r.text().catch(() => "");
    return new Response(
      JSON.stringify({ ok: false, error: `GitHub ${r.status}`, detail: text.slice(0, 200) }),
      { status: 502, headers: jsonHeaders }
    );
  } catch (e) {
    console.error("admin/dispatch error:", e);
    return new Response(
      JSON.stringify({ ok: false, error: String(e).slice(0, 200) }),
      { status: 500, headers: jsonHeaders }
    );
  }
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
        body: JSON.stringify({
          ref: "main",
          // Cron всегда передаёт smart_skip=true: парсер пропускает нерабочие
          // дни РФ (двойная защита поверх isHoliday() выше) и дела с
          // известной будущей датой (заседание/«без движения») — экономит
          // запросы к ГАС «Правосудие». Ручной workflow_dispatch из UI
          // запускается без этого input и парсит всё как раньше.
          inputs: { smart_skip: "true" },
        }),
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

    if (url.pathname === "/run-progress" && request.method === "POST") {
      return handleRunProgress(request, env);
    }

    if (url.pathname === "/admin/run-progress" && request.method === "GET") {
      return handleAdminRunProgress(request, env);
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

    if (url.pathname === "/admin/gh-runs" && request.method === "GET") {
      return handleAdminGhRuns(request, env);
    }

    if (url.pathname === "/admin/dispatch" && request.method === "POST") {
      return handleAdminDispatch(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },
};
