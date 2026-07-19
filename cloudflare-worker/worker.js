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

// ── Пер-инстансные настройки (форк территории меняет ТОЛЬКО wrangler.toml) ──
// Значения берутся из [vars] wrangler.toml (env), фолбэки — боевые значения
// ХМАО-инстанса: воркер работает и до деплоя с vars. env недоступен на уровне
// модуля (только в хендлерах), поэтому fetch/scheduled кладут его в RUNTIME_ENV.
let RUNTIME_ENV = {};
function cfgVar(name, fallback) {
  const v = RUNTIME_ENV && RUNTIME_ENV[name];
  return (v === undefined || v === null || v === "") ? fallback : v;
}

// GitHub Pages URL для CORS
const ALLOWED_ORIGIN_DEFAULT = "https://selivanovas.github.io";
function allowedOrigin() { return cfgVar("ALLOWED_ORIGIN", ALLOWED_ORIGIN_DEFAULT); }

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
  const allowed = origin === allowedOrigin() || origin === "http://localhost:8081";
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

const CASES_DATA_URL_DEFAULT = "https://selivanovas.github.io/dashboard/data/cases.json";
function casesDataUrl() { return cfgVar("CASES_DATA_URL", CASES_DATA_URL_DEFAULT); }
// Производные URL территории — ВСЕ данные страницы админки выводятся из
// CASES_DATA_URL (wrangler.toml форка), а не хардкодятся: иначе админка
// Урала показывала бы дела и здоровье парсеров ХМАО.
function siteBaseUrl() {
  // "https://…/dashboard/data/cases.json" → "https://…/dashboard"
  return casesDataUrl().replace(/\/data\/cases\.json$/, "");
}
function adminPageConfig() {
  const base = siteBaseUrl();
  return {
    casesUrl: base + "/data/cases.json",
    archiveUrl: base + "/data/cases_archive.json",
    pushesUrl: base + "/data/last_personal_pushes.json",
    digestUrl: base + "/data/last_digest.json",
    healthUrl: base + "/data/parse_health.json",
    dashboardUrl: base + "/sberbank_dashboard.html",
    siteBase: base,
    ghRepo: cfgVar("GH_REPO", GH_REPO_DEFAULT),
  };
}

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
    const r = await fetch(casesDataUrl(), {
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
    // Экономия KV writes (free-tier 1000/день на аккаунт, инцидент 17.07.2026):
    // если подписка не изменилась и last_seen_at свежее 12 часов — put
    // пропускаем. Гранулярность 12 ч безвредна: бейдж «⏳ истекает» смотрит
    // на 45 дней, KV-TTL 60 дней освежится первым же открытием после окна.
    if (prev && prev.endpoint === sub.endpoint
        && JSON.stringify(prev.keys || null) === JSON.stringify(sub.keys || null)
        && prev.user_agent === sub.user_agent
        && prev.last_seen_at
        && Date.now() - Date.parse(prev.last_seen_at) < 12 * 3600 * 1000) {
      return new Response(JSON.stringify({
        ok: true,
        watchlist: Array.isArray(prev.watchlist) ? prev.watchlist : [],
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders(origin) },
      });
    }
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
// Доступен и оператору: живой лог импорт-прогона — его обратная связь.
async function handleAdminRunProgress(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
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

// Роли админки (с 16.07.2026, тиражирование): owner — юрист-владелец
// (OWNER_SECRET, всё как раньше), operator — сопровождающий капчёвого суда
// (общий OPERATOR_SECRET на ~14 человек; имя оператора — поле формы импорта,
// доверительное, не аутентификация). Оператор видит статус/здоровье/живой
// лог/импорт; подписчики и запуски прогонов закрыты И на сервере (не только
// в UI). OPERATOR_SECRET не задан (ХМАО-инстанс) → роль неактивна.
function resolveAdminRole(request, env) {
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  if (!secret) return null;
  if (env.OWNER_SECRET && secret === env.OWNER_SECRET) return "owner";
  if (env.OPERATOR_SECRET && String(env.OPERATOR_SECRET).length > 0
      && secret === env.OPERATOR_SECRET) return "operator";
  return null;
}
// Общий гейт: чужой/пустой секрет → 401, валидный секрет без нужной роли
// (оператор на owner-эндпоинте) → 403. Возвращает {role} либо {error}.
function requireAdminRole(request, env, roles) {
  const role = resolveAdminRole(request, env);
  if (role && roles.includes(role)) return { role };
  return {
    error: new Response(role ? "Forbidden" : "Unauthorized",
      { status: role ? 403 : 401 }),
  };
}

// Возвращает JSON со всеми подписками (как /subscriptions, но авторизация
// через ?secret=<OWNER_SECRET> в URL — чтобы HTML-страница могла дёрнуть
// данные без хранения PUSH_SECRET в JS-коде в браузере).
async function handleAdminData(request, env) {
  const gate = requireAdminRole(request, env, ["owner"]);
  if (gate.error) return gate.error;
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
// `/admin?secret=<OWNER_SECRET|OPERATOR_SECRET>`. Содержит inline-стили и JS,
// который тянет /admin/data (с тем же secret) и cases.json с GitHub Pages.
// Роль вшивается в страницу (ROLE) — операторский рендер прячет owner-блоки,
// но реальный запрет — на эндпоинтах (requireAdminRole).
async function handleAdmin(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
  const url = new URL(request.url);
  const secret = url.searchParams.get("secret") || "";
  // Embed secret в HTML, чтобы JS мог дёрнуть /admin/data. Secret уже в URL,
  // дополнительная утечка минимальна, но всё равно экранируем кавычки.
  const safeSecret = secret.replace(/[<>"&']/g, "");
  const html = renderAdminHtml(safeSecret, gate.role, adminPageConfig());
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

// Утилиты для /admin/<action> endpoints ПОДПИСОК: проверка secret + загрузка
// существующей подписки по endpoint. Только owner: label/watchlist/unsubscribe/
// test-push оперируют чужими push-подписками — оператору они закрыты.
async function adminAuthAndLoad(request, env) {
  const gate = requireAdminRole(request, env, ["owner"]);
  if (gate.error) return { error: gate.error };
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

// VAPID public key — публичный (известен Service Worker'у через
// applicationServerKey), не секретный. У каждой территории СВОЯ пара:
// public задаётся в [vars] wrangler.toml форка (VAPID_PUBLIC_KEY) и обязан
// совпадать с region_front.js фронта; фолбэк — ключ ХМАО-инстанса.
// Приватный — в secret `VAPID_PRIVATE_KEY` (PEM). Без него тест push → 503.
const VAPID_PUBLIC_KEY_DEFAULT = "BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU";
function vapidPublicKey() { return cfgVar("VAPID_PUBLIC_KEY", VAPID_PUBLIC_KEY_DEFAULT); }
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
  return { jwt, header: `vapid t=${jwt}, k=${vapidPublicKey()}` };
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

// Репозиторий инстанса (форк задаёт GH_REPO в [vars] wrangler.toml).
const GH_REPO_DEFAULT = "SelivanovAS/dashboard";
function ghRepoApi() { return "https://api.github.com/repos/" + cfgVar("GH_REPO", GH_REPO_DEFAULT); }

// Время крона инстанса (UTC, "Ч:ММ") — из [vars] wrangler.toml территории
// (CRON_UTC), держать в синхроне с [triggers].crons! Плитка «Автозапуск»
// в админке раньше врала: время было захардкожено (03:45 — расписание
// ХМАО до 15.07.2026). Фолбэк — текущий крон ХМАО-инстанса (03:30).
function cronUtcParts() {
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(cfgVar("CRON_UTC", "3:30")).trim());
  return m ? [Number(m[1]), Number(m[2])] : [3, 30];
}

// Ближайший запуск cron'а Worker'а с учётом праздников РФ — зеркалит
// scheduled(): день оценивается по МСК (UTC+3).
function nextCronAt() {
  const now = new Date();
  const [cronH, cronM] = cronUtcParts();
  for (let i = 0; i < 30; i++) {
    const day = new Date(now.getTime() + i * 86400000);
    const fire = new Date(Date.UTC(
      day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), cronH, cronM, 0
    ));
    if (fire.getTime() <= now.getTime()) continue;
    const msk = new Date(fire.getTime() + 3 * 3600 * 1000);
    if (isHoliday(msk)) continue;
    return fire.toISOString();
  }
  return null;
}

// JSON для блока «🚀 Прогоны»: последние runs GitHub Actions. PAT остаётся
// на сервере — страница ходит сюда со своим секретом. Оператору тоже
// доступен: плитка «Последний прогон» и статус импорт-прогона.
async function handleAdminGhRuns(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
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
      fetch(ghRepoApi() + "/actions/runs?per_page=20", { headers: ghHeaders }),
      fetch(ghRepoApi() + "/actions/workflows/update_cases.yml/runs?per_page=1", { headers: ghHeaders })
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

// Белый список запуска workflow из админки: только эти файлы, только эти
// inputs и только этим ролям. Значения inputs — строки («true»/«false» для
// булевых — так требует GitHub REST API, тип из workflow_dispatch он
// приводит сам). Проверка roles здесь — РЕАЛЬНЫЙ запрет (оператор не может
// запустить полный прогон или Claude-дайджест), скрытие кнопок в UI — лишь UX.
const DISPATCH_WORKFLOWS = {
  "update_cases.yml": {
    inputs: new Set(["to_group", "smart_skip"]),
    roles: ["owner"],
  },
  "test_digest.yml": {
    inputs: new Set([
      "to_group", "push_all", "full_llm", "llm_provider",
      "claude_model", "claude_effort", "gigachat_model", "openrouter_model",
      "llm_model", "commit_results",
    ]),
    roles: ["owner"],
  },
  "import_cases.yml": {
    inputs: new Set(["dump_key", "court_domain", "operator"]),
    roles: ["owner", "operator"],
  },
};

// Один POST workflow_dispatch на GitHub API (ветка main). Общий для кнопок
// админки (handleAdminDispatch) и внутреннего диспатча импорта
// (handleAdminImportDump). Возвращает {ok} | {ok:false, error, detail}.
async function dispatchWorkflowOnGitHub(env, workflow, inputs) {
  try {
    const r = await fetch(
      `${ghRepoApi()}/actions/workflows/${workflow}/dispatches`,
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
      console.log(`dispatch ok: ${workflow} ${JSON.stringify(inputs)}`);
      return { ok: true };
    }
    const text = await r.text().catch(() => "");
    return { ok: false, error: `GitHub ${r.status}`, detail: text.slice(0, 200) };
  } catch (e) {
    console.error(`dispatch ${workflow} error:`, e);
    return { ok: false, error: String(e).slice(0, 200) };
  }
}

// Запуск workflow по кнопке из админки (workflow_dispatch, ветка main).
async function handleAdminDispatch(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
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
  if (!allowed.roles.includes(gate.role)) {
    return new Response(
      JSON.stringify({ ok: false, error: `роль ${gate.role} не может запускать ${workflow}` }),
      { status: 403, headers: jsonHeaders }
    );
  }
  const inputs = {};
  const src = body.inputs && typeof body.inputs === "object" ? body.inputs : {};
  for (const [k, v] of Object.entries(src)) {
    if (!allowed.inputs.has(k)) {
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
  const res = await dispatchWorkflowOnGitHub(env, workflow, inputs);
  if (res.ok) {
    return new Response(JSON.stringify({ ok: true }), { headers: jsonHeaders });
  }
  return new Response(JSON.stringify(res), {
    status: res.error && res.error.startsWith("GitHub") ? 502 : 500,
    headers: jsonHeaders,
  });
}

// ── Импорт дел капчёвых судов ────────────────────────────────────────────────
// Поток: оператор решает код на сайте суда → вставляет дамп выдачи в админку →
// POST /admin/import-dump кладёт дамп в KV (import:dump:<uuid>, TTL 24 ч),
// заводит запись журнала (import:log:<ts>|<uuid>, TTL 90 дн — пер-ключевой
// журнал, без гонок read-modify-write) и диспатчит import_cases.yml →
// Action забирает дамп GET /import-dump (Bearer PUSH_SECRET), гонит
// import_search_dump.py, коммитит cases.json и постит итог POST /import-result
// → страница поллит GET /admin/import-log и показывает оператору «+N».

const IMPORT_DUMP_TTL = 24 * 3600;        // дамп нужен только ближайшему прогону
const IMPORT_LOG_TTL = 90 * 24 * 3600;    // история импортов в админке
const IMPORT_HTML_MIN = 1024;             // меньше — заведомо не страница выдачи
const IMPORT_HTML_MAX = 2 * 1024 * 1024;  // 2 МБ: страница выдачи sudrf ≤ ~300 КБ

// Sudrf-хосты дампа: абсолютные ссылки карточек (rich-paste абсолютизирует
// href «https://<суд>/modules.php?…name=sud_delo…») + маркер Chrome
// «saved from url=…» из файла «только HTML». Пустой массив = хостов в дампе
// нет (относительные href) — сверять нечего, финальная проверка в импортёре.
function detectDumpSudrfHosts(html) {
  const hosts = new Set();
  const cardRe = /https?:\/\/([a-z0-9][a-z0-9.-]*\.sudrf\.ru)\/modules\.php\?[^"'\s<>]*name=sud_delo/gi;
  let m;
  while ((m = cardRe.exec(html)) !== null) hosts.add(m[1].toLowerCase());
  m = /saved from url=\(\d+\)https?:\/\/([a-z0-9][a-z0-9.-]*\.sudrf\.ru)(?=[/\s])/i.exec(html);
  if (m) hosts.add(m[1].toLowerCase());
  return Array.from(hosts).sort();
}

// Приём дампа от оператора/владельца: валидация → KV → журнал → dispatch.
async function handleAdminImportDump(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
  const jsonHeaders = { "Content-Type": "application/json; charset=utf-8" };
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return new Response("Bad JSON", { status: 400 });
  }
  const courtDomain = String((body && body.court_domain) || "").trim().toLowerCase();
  const operator = String((body && body.operator) || "").trim().slice(0, 60);
  const html = typeof (body && body.html) === "string" ? body.html : "";
  if (!/^[a-z0-9][a-z0-9.-]*\.sudrf\.ru$/.test(courtDomain)) {
    return new Response(
      JSON.stringify({ ok: false, error: "court_domain не похож на домен sudrf.ru" }),
      { status: 400, headers: jsonHeaders }
    );
  }
  if (html.length < IMPORT_HTML_MIN) {
    return new Response(
      JSON.stringify({ ok: false, error: "страница слишком короткая — скопируйте страницу результатов целиком (выделением) или приложите файл «только HTML»" }),
      { status: 400, headers: jsonHeaders }
    );
  }
  if (html.length > IMPORT_HTML_MAX) {
    return new Response(
      JSON.stringify({ ok: false, error: "файл больше 2 МБ — это не страница результатов; сохраните её как «только HTML», без картинок" }),
      { status: 400, headers: jsonHeaders }
    );
  }
  // Дамп чужого суда: хост из ссылок карточек обязан совпадать с выбранным
  // судом (страховка от обхода клиентской проверки; импортёр перепроверит).
  const dumpHosts = detectDumpSudrfHosts(html);
  if (dumpHosts.length && (dumpHosts.length > 1 || dumpHosts[0] !== courtDomain)) {
    return new Response(
      JSON.stringify({
        ok: false,
        error: "в странице ссылки суда " + dumpHosts.join(", ")
          + ", а выбран " + courtDomain + " — проверьте выбор суда",
      }),
      { status: 400, headers: jsonHeaders }
    );
  }
  const uuid = crypto.randomUUID();
  const dumpKey = `import:dump:${uuid}`;
  const ts = new Date().toISOString();
  const logKey = `import:log:${ts}|${uuid}`;
  const record = {
    uuid, court_domain: courtDomain, operator, ts,
    status: "dispatched", updated_at: ts,
  };
  await env.PUSH_SUBSCRIPTIONS.put(dumpKey, html, { expirationTtl: IMPORT_DUMP_TTL });
  await env.PUSH_SUBSCRIPTIONS.put(logKey, JSON.stringify(record), {
    expirationTtl: IMPORT_LOG_TTL,
  });
  const res = await dispatchWorkflowOnGitHub(env, "import_cases.yml", {
    dump_key: dumpKey, court_domain: courtDomain, operator,
  });
  if (!res.ok) {
    // Диспатч не прошёл — фиксируем в журнале, оператор увидит «failed»
    // сразу, а не по таймауту поллинга.
    record.status = "failed";
    record.error = `${res.error || "dispatch failed"}${res.detail ? ": " + res.detail : ""}`;
    record.updated_at = new Date().toISOString();
    await env.PUSH_SUBSCRIPTIONS.put(logKey, JSON.stringify(record), {
      expirationTtl: IMPORT_LOG_TTL,
    });
    return new Response(JSON.stringify({ ok: false, key: uuid, error: record.error }), {
      status: 502, headers: jsonHeaders,
    });
  }
  console.log(`import dump принят: ${dumpKey} (${courtDomain}, ${operator || "без имени"}, ${html.length} байт)`);
  return new Response(JSON.stringify({ ok: true, key: uuid }), { headers: jsonHeaders });
}

// Выдача сырого дампа GitHub Action'у (Bearer PUSH_SECRET — он уже есть в
// GH secrets; шаблон /subscriptions).
async function handleImportDumpGet(request, env) {
  const auth = request.headers.get("Authorization") || "";
  if (!env.PUSH_SECRET || auth !== `Bearer ${env.PUSH_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  const url = new URL(request.url);
  const key = url.searchParams.get("key") || "";
  if (!/^import:dump:[0-9a-f-]{36}$/.test(key)) {
    return new Response("Bad Request", { status: 400 });
  }
  const html = await env.PUSH_SUBSCRIPTIONS.get(key);
  if (html === null) {
    return new Response("Not Found (дамп истёк — TTL 24 ч — или не существовал)", { status: 404 });
  }
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

// Итог импорта от Action'а: started/done/failed + числа + строки отчёта.
// Обновляет запись журнала по uuid из dump_key.
async function handleImportResult(request, env) {
  const auth = request.headers.get("Authorization") || "";
  if (!env.PUSH_SECRET || auth !== `Bearer ${env.PUSH_SECRET}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return new Response("Bad JSON", { status: 400 });
  }
  const m = /^import:dump:([0-9a-f-]{36})$/.exec(String(body.dump_key || ""));
  const status = String(body.status || "");
  if (!m || !["started", "done", "failed"].includes(status)) {
    return new Response("Bad Request", { status: 400 });
  }
  const uuid = m[1];
  // Пер-ключевой журнал: ищем запись по суффиксу |uuid. Записей ≤ сотни
  // (TTL 90 дн), list по префиксу дешёвый.
  const list = await env.PUSH_SUBSCRIPTIONS.list({ prefix: "import:log:" });
  const entry = list.keys.find((k) => k.name.endsWith(`|${uuid}`));
  if (!entry) {
    return new Response(JSON.stringify({ ok: false, error: "запись журнала не найдена" }), {
      status: 404, headers: { "Content-Type": "application/json; charset=utf-8" },
    });
  }
  let record = {};
  try { record = JSON.parse(await env.PUSH_SUBSCRIPTIONS.get(entry.name)) || {}; } catch (_) {}
  record.status = status;
  record.updated_at = new Date().toISOString();
  for (const num of ["added", "promoted", "already", "skipped_role", "no_link", "subsidiary", "rows"]) {
    if (typeof body[num] === "number") record[num] = body[num];
  }
  if (Array.isArray(body.lines)) {
    record.lines = body.lines.map(String).slice(0, 100);
  }
  if (typeof body.run_url === "string" && /^https:\/\//.test(body.run_url)) {
    record.run_url = body.run_url.slice(0, 300);
  }
  if (typeof body.error === "string" && body.error) {
    record.error = body.error.slice(0, 500);
  }
  await env.PUSH_SUBSCRIPTIONS.put(entry.name, JSON.stringify(record), {
    expirationTtl: IMPORT_LOG_TTL,
  });
  // Свежесть по суду (светофор в админке): последний УСПЕШНЫЙ импорт домена.
  // Отдельный вечный ключ (без TTL): журнал живёт 90 дней и отдаётся
  // последними 50 записями — при ~52 судах с еженедельным регламентом
  // окна журнала на «когда суд импортировался в последний раз» не хватает.
  if (status === "done" && record.court_domain) {
    await env.PUSH_SUBSCRIPTIONS.put(
      `import:last:${record.court_domain}`,
      JSON.stringify({
        court_domain: record.court_domain,
        ts: record.updated_at,
        operator: record.operator || "",
        added: record.added || 0,
        promoted: record.promoted || 0,
      })
    );
  }
  return new Response(JSON.stringify({ ok: true }), {
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

// Журнал импортов для админки (обе роли): последние 50, свежие первыми,
// + карта «последний успешный импорт по домену» (светофор свежести).
async function handleAdminImportLog(request, env) {
  const gate = requireAdminRole(request, env, ["owner", "operator"]);
  if (gate.error) return gate.error;
  try {
    // ?logonly=1 — горячий поллинг ожидания импорта: клиенту нужен только
    // журнал (найти свою запись по uuid). Пропускаем блок import:last:* —
    // это второй KV-list + get по всем доменам, а lists-лимит free-tier
    // всего 1000/день (инцидент 17.07.2026: отладка импорта сожгла 50%).
    const logOnly = new URL(request.url).searchParams.get("logonly") === "1";
    const list = await env.PUSH_SUBSCRIPTIONS.list({ prefix: "import:log:" });
    // Ключ начинается с ISO-времени → лексикографический порядок = хронология.
    const keys = list.keys.map((k) => k.name).sort().reverse().slice(0, 50);
    const items = (await Promise.all(keys.map(async (name) => {
      try { return JSON.parse(await env.PUSH_SUBSCRIPTIONS.get(name)); }
      catch (_) { return null; }
    }))).filter(Boolean);
    const last = {};
    if (!logOnly) {
      const lastList = await env.PUSH_SUBSCRIPTIONS.list({ prefix: "import:last:" });
      (await Promise.all(lastList.keys.map(async (k) => {
        try { return JSON.parse(await env.PUSH_SUBSCRIPTIONS.get(k.name)); }
        catch (_) { return null; }
      }))).filter(Boolean).forEach((e) => {
        if (e.court_domain) last[e.court_domain] = e;
      });
    }
    return new Response(JSON.stringify({ items, last }), {
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });
  } catch (e) {
    console.error("admin/import-log error:", e);
    return new Response("Error", { status: 500 });
  }
}

// ── Экспорт ───────────────────────────────────────────────────────────────────

export default {
  // ── Cron-триггер: запуск GitHub Actions ─────────────────────────────────
  async scheduled(event, env) {
    RUNTIME_ENV = env; // [vars] wrangler.toml → cfgVar()
    // Текущая дата по МСК (UTC+3)
    const now = new Date(Date.now() + 3 * 3600 * 1000);

    if (isHoliday(now)) {
      console.log(`Пропуск: ${now.toISOString().slice(0, 10)} — праздничный день`);
      return;
    }

    const response = await fetch(
      ghRepoApi() + "/actions/workflows/update_cases.yml/dispatches",
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
    RUNTIME_ENV = env; // [vars] wrangler.toml → cfgVar()
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

    if (url.pathname === "/admin/import-dump" && request.method === "POST") {
      return handleAdminImportDump(request, env);
    }

    if (url.pathname === "/import-dump" && request.method === "GET") {
      return handleImportDumpGet(request, env);
    }

    if (url.pathname === "/import-result" && request.method === "POST") {
      return handleImportResult(request, env);
    }

    if (url.pathname === "/admin/import-log" && request.method === "GET") {
      return handleAdminImportLog(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },
};
