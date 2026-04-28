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
      } catch (_) { /* игнор: невалидный JSON в KV — перезапишем */ }
    }
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

    return new Response("Not Found", { status: 404 });
  },
};
