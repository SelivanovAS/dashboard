// Сроки согласованы 15.09.2026. Активность отделена от profile:*:
// фоновая отметка входа не должна перезаписать watchlist и его LWW-штамп.
export const PROFILE_CLEANUP_CRON = "17 21 * * *"; // ежедневно 02:17 UTC+5
const DAY = 86400000;
const EMPTY_IDLE = 7 * DAY;
const PROFILE_IDLE = 30 * DAY;
const RECOVERY = 7 * DAY;
// После закрытия восстановления ждём ещё сутки до физического удаления.
// За это время KV распространяет маркер; новые запросы уже получают 404,
// а запросы, начатые в разрешённое окно, успевают сохранить активность.
const PURGE_DELAY = DAY;
const profileKey = (id) => `profile:${id}`;
const activityKey = (id) => `profile-activity:${id}`;
const removedKey = (id) => `profile-removed:${id}`;
const validId = (id) => typeof id === "string" && /^[0-9a-f-]{36}$/.test(id);
const pendingUses = new WeakMap(); // /subscribe и /profile/get на старте PWA идут параллельно

async function readJson(kv, key) {
  const raw = await kv.get(key);
  if (raw === null || raw === undefined) return null;
  const value = JSON.parse(raw);
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Invalid profile lifecycle record");
  }
  return value;
}

async function readActivity(kv, id) {
  const a = await readJson(kv, activityKey(id));
  if (a && (!Number.isFinite(a.tracking_started_at) || a.tracking_started_at <= 0
      || !Number.isFinite(a.active_until) || a.active_until < a.tracking_started_at
      || (a.last_used_at !== null && !Number.isFinite(a.last_used_at)))) {
    throw new Error("Invalid profile activity");
  }
  return a;
}

async function readRemoved(kv, id) {
  const r = await readJson(kv, removedKey(id));
  if (r && (!Number.isFinite(r.removed_at) || !Number.isFinite(r.restore_until)
      || r.restore_until !== r.removed_at + RECOVERY || !r.fingerprint)) {
    throw new Error("Invalid profile recovery deadline");
  }
  return r;
}

function usedAfterRemoval(a, r) {
  return a && r && Number.isFinite(a.last_used_at) && a.last_used_at >= r.removed_at;
}

export async function recordProfileUse(env, id, force = false) {
  const kv = env.PUSH_SUBSCRIPTIONS;
  let pending = pendingUses.get(kv);
  if (!pending) { pending = new Map(); pendingUses.set(kv, pending); }
  if (pending.has(id)) return pending.get(id);
  const operation = writeProfileUse(env, id, force);
  pending.set(id, operation);
  try { await operation; } finally { pending.delete(id); }
}

async function writeProfileUse(env, id, force) {
  const kv = env.PUSH_SUBSCRIPTIONS;
  const now = Date.now();
  const previous = await readActivity(kv, id);
  if (!force && previous && previous.last_used_at !== null && previous.active_until > now) return;
  await kv.put(activityKey(id), JSON.stringify({
    tracking_started_at: previous ? previous.tracking_started_at : now,
    last_used_at: now,
    // Одна запись за сутки UTC. Для очистки берём КОНЕЦ суток: экономия
    // writes может отложить очистку, но не сократить согласованный срок.
    active_until: Math.max(previous?.active_until || 0, (Math.floor(now / DAY) + 1) * DAY),
  }));
}

export async function readProfile(env, id, { use = false, calendarToken = "" } = {}) {
  if (!validId(id)) return null;
  const kv = env.PUSH_SUBSCRIPTIONS;
  const [profile, removed] = await Promise.all([
    readJson(kv, profileKey(id)), readRemoved(kv, id),
  ]);
  if (!profile || (calendarToken && profile.feed_token !== calendarToken)) return null;
  let recover = false;
  if (removed) {
    const activity = await readActivity(kv, id);
    if (!usedAfterRemoval(activity, removed)) {
      if (!use || Date.now() >= removed.restore_until) return null;
      recover = true;
    }
  }
  if (use) {
    await recordProfileUse(env, id, recover);
    if (removed) await kv.delete(removedKey(id));
  }
  return profile;
}

export async function profileLifecycleFields(env, id) {
  const kv = env.PUSH_SUBSCRIPTIONS;
  const [activity, removed] = await Promise.all([readActivity(kv, id), readRemoved(kv, id)]);
  const retired = removed && !usedAfterRemoval(activity, removed);
  const iso = (n) => Number.isFinite(n) ? new Date(n).toISOString() : "";
  return {
    lifecycle_status: retired ? (Date.now() < removed.restore_until ? "recoverable" : "expired") : "active",
    activity_started_at: iso(activity?.tracking_started_at),
    last_used_at: iso(activity?.last_used_at),
    removed_at: retired ? iso(removed.removed_at) : "",
    restore_until: retired ? iso(removed.restore_until) : "",
  };
}

async function allKeys(kv, prefix) {
  const keys = [];
  let cursor;
  for (let page = 0; page < 100; page++) {
    const result = await kv.list({ prefix, ...(cursor ? { cursor } : {}) });
    if (!Array.isArray(result.keys)) throw new Error("Incomplete profile KV list");
    keys.push(...result.keys);
    if (result.list_complete === true) return keys;
    if (!result.cursor || result.cursor === cursor) throw new Error("Incomplete profile KV pagination");
    cursor = result.cursor;
  }
  throw new Error("Profile KV list page limit exceeded");
}

async function fingerprint(profile) {
  const bytes = new TextEncoder().encode(JSON.stringify(profile));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

export async function cleanupProfiles(env) {
  if (String(env.PROFILE_CLEANUP_ENABLED) !== "1") return { disabled: true };
  const kv = env.PUSH_SUBSCRIPTIONS;
  const now = Date.now();
  // Полные списки ДО первого удаления: ошибка/обрыв страницы не должны
  // превратить связанный профиль в якобы оставшийся без push.
  const [profiles, subscriptions, removedKeys] = await Promise.all([
    allKeys(kv, "profile:"), allKeys(kv, "sub:"), allKeys(kv, "profile-removed:"),
  ]);
  const linked = new Set();
  for (const key of subscriptions) {
    const sub = await readJson(kv, key.name);
    if (sub && validId(sub.profile_id)) linked.add(sub.profile_id);
  }
  const counts = { checked: 0, tracking_started: 0, protected_by_push: 0, removed: 0, restored: 0, purged: 0 };
  for (const key of profiles) {
    const id = key.name.slice("profile:".length);
    if (!validId(id)) continue;
    const profile = await readJson(kv, key.name);
    if (!profile) continue;
    // Не удаляем запись с неизвестной/повреждённой схемой.
    if (profile.schema_version !== 1 || !Array.isArray(profile.watchlist)) continue;
    counts.checked++;
    const [activity, removed] = await Promise.all([readActivity(kv, id), readRemoved(kv, id)]);
    if (!activity) {
      // Старые created_at/updated_at не доказывают бездействие. Отсчёт
      // начинается с первого наблюдения этой территории после включения.
      await kv.put(activityKey(id), JSON.stringify({ tracking_started_at: now, last_used_at: null, active_until: now }));
      if (removed) await kv.delete(removedKey(id));
      counts.tracking_started++;
      continue;
    }
    if (linked.has(id) || usedAfterRemoval(activity, removed)) {
      if (removed) { await kv.delete(removedKey(id)); counts.restored++; }
      if (linked.has(id)) counts.protected_by_push++;
      continue;
    }
    if (removed) {
      if (now < removed.restore_until + PURGE_DELAY) continue;
      // Повторное чтение перед уничтожением; возврат пользователя и любые
      // изменения профиля отменяют удаление снимка. Маркер закрыт для
      // восстановления уже сутки, поэтому активный HTTP-запрос не может
      // соревноваться с физическим удалением через eventual-consistent KV.
      const [freshProfile, freshActivity, freshRemoved] = await Promise.all([
        readJson(kv, key.name), readActivity(kv, id), readRemoved(kv, id),
      ]);
      if (!freshProfile || !freshActivity || !freshRemoved) continue;
      if (freshRemoved.removed_at !== removed.removed_at) continue;
      if (usedAfterRemoval(freshActivity, freshRemoved)
          || await fingerprint(freshProfile) !== freshRemoved.fingerprint) {
        await kv.delete(removedKey(id)); counts.restored++; continue;
      }
      if (freshProfile.feed_token) {
        const tokenKey = `calfeed:${freshProfile.feed_token}`;
        const index = await readJson(kv, tokenKey);
        if (index?.profile_id === id) await kv.delete(tokenKey);
      }
      await kv.delete(key.name);
      await kv.delete(activityKey(id));
      await kv.delete(removedKey(id));
      counts.purged++;
      continue;
    }
    const idle = !profile.watchlist.length && !profile.feed_token ? EMPTY_IDLE : PROFILE_IDLE;
    if (now < activity.active_until + idle) continue;
    // Только маркер: профиль и токен сохраняются целиком на 7 дней.
    await kv.put(removedKey(id), JSON.stringify({
      removed_at: now, restore_until: now + RECOVERY,
      fingerprint: await fingerprint(profile),
      reason: idle === EMPTY_IDLE ? "empty" : "inactive",
    }));
    counts.removed++;
  }
  // Доводим прерванную очистку, если удаление profile:* уже прошло,
  // а удаление служебных ключей завершилось ошибкой.
  for (const key of removedKeys) {
    const id = key.name.slice("profile-removed:".length);
    if (!validId(id)) continue;
    const removed = await readRemoved(kv, id);
    if (removed && now >= removed.restore_until + PURGE_DELAY
        && !(await kv.get(profileKey(id)))) {
      await kv.delete(activityKey(id));
      await kv.delete(key.name);
    }
  }
  await kv.put("profile-maintenance:last", JSON.stringify({ ...counts, completed_at: new Date(now).toISOString() }));
  console.log("Очистка профилей:", JSON.stringify(counts)); // без UUID/токенов
  return counts;
}
