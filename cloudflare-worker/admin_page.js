// Страница админки подписчиков (/admin) — отдельный модуль, чтобы не раздувать
// worker.js. `wrangler deploy` бандлит импорт сам (esbuild).
//
// ⚠️ Экранирование: всё содержимое — ОДИН внешний template literal. Внутренний
// JS страницы пишется БЕЗ template literals и без `${` (только конкатенация),
// а backslash в его регексах/строках удваивается (`\\d`, `\\n`). Единственная
// интерполяция внешнего литерала — SECRET.

export function renderAdminHtml(secret) {
  return `<!doctype html><html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="referrer" content="no-referrer">
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
.refresh:disabled { opacity:0.6; cursor:default; }
.summary { color:var(--fg-3); font-size:13px; }
.subs { display:flex; flex-direction:column; gap:10px; }
.sub-card { background:var(--bg-1); border:1px solid var(--border); border-radius:10px; padding:12px 14px; }
.sub-row { display:flex; flex-wrap:wrap; gap:10px 18px; align-items:baseline; }
.sub-device { font-weight:600; }
.badge-owner { display:inline-block; background:rgba(245,158,11,0.14); color:var(--amber);
               padding:2px 8px; border-radius:999px; font-size:11px; font-weight:700; letter-spacing:0.4px; }
.badge-expiry { display:inline-block; background:rgba(245,158,11,0.14); color:var(--amber);
                padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; }
.kv { color:var(--fg-3); font-size:12px; }
.kv b { color:var(--fg-2); font-weight:500; }
.endpoint { font-family:ui-monospace,Menlo,monospace; color:var(--fg-3); font-size:11px;
            overflow:hidden; text-overflow:ellipsis; max-width:220px; white-space:nowrap; }
.actions { display:flex; flex-wrap:wrap; gap:6px; margin-top:10px; }
.btn { background:var(--bg-2); color:var(--fg-2); border:1px solid var(--border); padding:5px 10px;
       border-radius:6px; font-size:12px; cursor:pointer; font-family:inherit; line-height:1.2; }
.btn:hover { background:var(--bg-1); color:var(--fg); }
.btn:disabled { opacity:0.55; cursor:default; }
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
.progress-card { background:var(--bg-1); border:1px solid var(--border); border-radius:10px;
                 padding:12px 14px; margin-bottom:14px; }
.progress-head { display:flex; gap:10px; align-items:baseline; flex-wrap:wrap; }
.progress-title { font-weight:600; }
.progress-state { font-weight:700; }
.progress-state.running { color:var(--amber); }
.progress-state.done { color:var(--accent); }
.progress-meta { color:var(--fg-3); font-size:12px; }
.progress-log { margin:8px 0 0; padding:10px 12px; background:var(--bg-2); border-radius:8px;
                font-family:ui-monospace,Menlo,monospace; font-size:11.5px; line-height:1.55;
                max-height:340px; overflow:auto; white-space:pre-wrap; word-break:break-word; }
.runs-list { margin-top:8px; display:flex; flex-direction:column; }
.run-row { display:flex; gap:8px; align-items:baseline; padding:4px 0; flex-wrap:wrap;
           border-bottom:1px dashed var(--border); }
.run-row:last-child { border-bottom:0; }
.run-name { font-weight:600; color:var(--fg); text-decoration:none; }
.run-name:hover { color:var(--accent); }
.run-meta { color:var(--fg-3); font-size:12px; }
.digest-card { background:var(--bg-1); border:1px solid var(--border); border-radius:10px;
               padding:10px 14px; margin-bottom:14px; font-size:13px; color:var(--fg-2);
               display:flex; gap:4px 10px; flex-wrap:wrap; align-items:baseline; }
.digest-card a { color:var(--accent); text-decoration:none; }
.digest-card a:hover { text-decoration:underline; }
.dot { color:var(--fg-3); }
.health-row { display:flex; gap:10px; align-items:baseline; padding:3px 0; font-size:12.5px; flex-wrap:wrap; }
.health-name { min-width:230px; }
.health-spark { font-family:ui-monospace,Menlo,monospace; color:var(--fg-3); letter-spacing:1px; }
.health-meta { color:var(--fg-3); font-size:12px; }
details { margin-top:10px; }
details > summary { cursor:pointer; color:var(--fg-2); font-size:13px; padding:6px 0; outline:none;
                    user-select:none; }
details > summary:hover { color:var(--fg); }
.llm-top { background:var(--bg-1); border:1px solid var(--border); border-radius:10px;
           padding:2px 14px 10px; margin:0 0 14px; }
.llm-top > summary { font-weight:600; color:var(--fg); font-size:13.5px; }
.llm-row { padding:3px 0; font-family:ui-monospace,Menlo,monospace; font-size:12.5px; }
.llm-row b { display:inline-block; min-width:52px; }
.llm-meta { color:var(--fg-3); font-size:12px; margin-top:8px; }
.llm-meta a { color:var(--accent); }
.tf { margin-top:10px; padding-top:10px; border-top:1px dashed var(--border);
      display:flex; flex-direction:column; gap:8px; }
.tf-row { display:flex; gap:8px 16px; flex-wrap:wrap; align-items:center; font-size:13px; }
.tf-row label { display:flex; gap:6px; align-items:center; color:var(--fg-2); }
.tf select, .tf input[type=text] { font-family:inherit; font-size:13px; padding:4px 8px;
      border-radius:6px; border:1px solid var(--border); background:var(--bg-2); color:var(--fg); }
.tf input[type=text] { min-width:220px; }
.cases { margin-top:6px; padding-left:8px; border-left:2px solid var(--border); display:flex;
         flex-direction:column; gap:4px; }
.case-row { display:flex; gap:8px; flex-wrap:wrap; align-items:baseline; padding:4px 0;
            border-bottom:1px dashed var(--border); }
.case-row:last-child { border-bottom:0; }
.case-num { font-family:ui-monospace,Menlo,monospace; font-weight:600; color:var(--accent); min-width:140px; }
.case-parties { color:var(--fg-2); }
.case-meta { color:var(--fg-3); font-size:12px; }
.case-alias { font-family:ui-monospace,Menlo,monospace; color:var(--fg-3); font-size:12px;
              background:rgba(127,127,127,0.10); padding:1px 6px; border-radius:4px; }
dialog.wl { border:1px solid var(--border); border-radius:12px; background:var(--bg-1); color:var(--fg);
            padding:16px; width:min(560px, calc(100vw - 32px)); max-height:88vh; }
dialog.wl::backdrop { background:rgba(0,0,0,0.45); }
.wl-head { font-weight:600; margin-bottom:10px; }
.wl-search { width:100%; padding:7px 10px; border-radius:8px; border:1px solid var(--border);
             background:var(--bg-2); color:var(--fg); font-family:inherit; font-size:13px; margin-bottom:8px; }
.wl-list { max-height:42vh; overflow:auto; display:flex; flex-direction:column;
           border:1px solid var(--border); border-radius:8px; padding:4px 10px; }
.wl-row { display:flex; gap:8px; align-items:baseline; padding:5px 0; font-size:13px;
          border-bottom:1px dashed var(--border); cursor:pointer; }
.wl-row:last-child { border-bottom:0; }
.wl-row input { margin:0; flex-shrink:0; position:relative; top:2px; }
.wl-num { font-family:ui-monospace,Menlo,monospace; font-weight:600; color:var(--accent); }
.wl-parties { color:var(--fg-2); font-size:12.5px; }
.wl-manual { display:flex; gap:6px; margin-top:8px; }
.wl-manual input { flex:1; padding:6px 10px; border-radius:6px; border:1px solid var(--border);
                   background:var(--bg-2); color:var(--fg); font-family:inherit; font-size:13px; }
.wl-foot { display:flex; gap:10px; align-items:center; justify-content:flex-end; margin-top:12px; }
.wl-count { color:var(--fg-3); font-size:12px; margin-right:auto; }
.empty { color:var(--fg-3); font-style:italic; padding:6px 0; }
.error { color:#dc2626; padding:12px; background:rgba(220,38,38,0.08); border-radius:8px; }
.loading { color:var(--fg-3); padding:24px; text-align:center; }
@media (max-width: 600px) {
  .endpoint { max-width:100%; white-space:normal; word-break:break-all; }
  .case-num { min-width:auto; }
  .health-name { min-width:0; }
}
</style>
</head><body>
<header>
  <h1>📡 Подписчики · мониторинг дел Сбера</h1>
  <div style="display:flex;gap:8px;align-items:center;">
    <span class="summary" id="summary">…</span>
    <button class="refresh" onclick="refreshAll()">Обновить</button>
  </div>
</header>
<div class="progress-card" id="runs-card">
  <div class="progress-head">
    <span class="progress-title">🚀 Прогоны GitHub Actions</span>
    <span class="progress-meta" id="runs-next"></span>
    <button class="btn" id="btn-run-main" style="margin-left:auto;">▶ Полный прогон</button>
    <span class="action-flash" id="runs-flash"></span>
  </div>
  <div class="runs-list" id="runs-list">Загрузка…</div>
</div>
<div class="progress-card" id="progress-card" style="display:none;">
  <div class="progress-head">
    <span class="progress-title">🛰 Парсинг на Mac</span>
    <span class="progress-state" id="progress-state"></span>
    <span class="progress-meta" id="progress-meta"></span>
  </div>
  <pre class="progress-log" id="progress-log"></pre>
  <details id="progress-prev" style="display:none;">
    <summary>Предыдущий прогон</summary>
    <pre class="progress-log" id="progress-prev-log"></pre>
  </details>
</div>
<details class="llm-top" id="progress-stale" style="display:none;">
  <summary id="progress-stale-sum">🛰 Резерв: парсинг на Mac</summary>
  <pre class="progress-log" id="progress-stale-log"></pre>
</details>
<div class="digest-card" id="digest-card" style="display:none;"></div>
<details class="llm-top" id="health-top">
  <summary id="health-sum">🩺 Здоровье парсеров</summary>
  <div id="health-body" class="loading">Загрузка…</div>
</details>
<details class="llm-top" id="llm-top">
  <summary>🧠 Топ бесплатных LLM OpenRouter + запуск теста дайджеста</summary>
  <div id="llm-top-body" class="loading">Загрузка…</div>
  <div class="tf" id="tf">
    <div class="tf-row">
      <label>Провайдер
        <select id="tf-provider">
          <option value="claude" selected>claude</option>
          <option value="gigachat">gigachat</option>
          <option value="openrouter">openrouter</option>
        </select>
      </label>
      <label id="tf-giga-wrap" style="display:none;">Модель
        <select id="tf-giga">
          <option value="GigaChat-2-Pro" selected>GigaChat-2-Pro</option>
          <option value="GigaChat-2">GigaChat-2</option>
          <option value="GigaChat-2-Max">GigaChat-2-Max</option>
        </select>
      </label>
      <label id="tf-or-wrap" style="display:none;">Модель
        <select id="tf-or">
          <option value="модель дня (топ-1)" selected>модель дня (топ-1)</option>
          <option value="топ-2">топ-2</option>
          <option value="топ-3">топ-3</option>
          <option value="топ-4">топ-4</option>
          <option value="топ-5">топ-5</option>
        </select>
      </label>
      <label>Точная модель <input type="text" id="tf-model" placeholder="пусто = по выбору выше"></label>
    </div>
    <div class="tf-row">
      <label><input type="checkbox" id="tf-to-group"> в корп. группу ⚠️</label>
      <label><input type="checkbox" id="tf-push-all"> push всем ⚠️</label>
      <label><input type="checkbox" id="tf-full-llm"> полный LLM (старый режим)</label>
      <label><input type="checkbox" id="tf-commit"> опубликовать результаты</label>
    </div>
    <div class="tf-row">
      <button class="btn" id="tf-run">▶ Запустить тест дайджеста</button>
      <span class="action-flash" id="tf-flash"></span>
    </div>
    <div class="llm-meta">Без галок безопасно: Telegram только в личный чат, без публикации на дашборд и без push. «Push всем» работает только вместе с «опубликовать».</div>
  </div>
</details>
<div id="root" class="loading">Загрузка…</div>
<dialog class="wl" id="wl-modal">
  <div class="wl-head">📋 Watchlist: <span id="wl-who"></span></div>
  <input class="wl-search" id="wl-search" type="text" placeholder="Поиск: номер дела, сторона или суд…">
  <div class="wl-list" id="wl-list"></div>
  <div id="wl-extras"></div>
  <div class="wl-manual">
    <input type="text" id="wl-manual-input" placeholder="Добавить номер вручную (напр. 2-123/2026)">
    <button class="btn" type="button" id="wl-manual-add">＋ Добавить</button>
  </div>
  <div class="wl-foot">
    <span class="wl-count" id="wl-count"></span>
    <button class="btn" type="button" id="wl-cancel">Отмена</button>
    <button class="refresh" type="button" id="wl-save">💾 Сохранить</button>
  </div>
</dialog>
<script>
const SECRET = ${JSON.stringify(secret)};
const CASES_URL = "https://selivanovas.github.io/dashboard/data/cases.json";
const PUSHES_URL = "https://selivanovas.github.io/dashboard/data/last_personal_pushes.json";
const DIGEST_URL = "https://selivanovas.github.io/dashboard/data/last_digest.json";
const HEALTH_URL = "https://selivanovas.github.io/dashboard/data/parse_health.json";
const DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html";

// ── Общие хелперы ────────────────────────────────────────────────────────────
// Python на GitHub-раннере пишет naive-таймстампы в UTC без «Z»
// (last_digest.json, last_personal_pushes.json, parse_health.json). Голый
// Date.parse счёл бы их локальным временем и врал бы на величину пояса
// (для ХМАО — на 5 часов). Строки со смещением/Z проходят без изменений.
function parseIso(s) {
  if (!s) return NaN;
  let x = String(s);
  if (/T\\d\\d:\\d\\d/.test(x) && !/(Z|[+-]\\d\\d:?\\d\\d)$/.test(x)) x += "Z";
  return Date.parse(x);
}
function relTime(iso) {
  const t = parseIso(iso);
  if (isNaN(t)) return "—";
  const diff = Math.round((Date.now() - t) / 1000);
  if (diff < 60) return "только что";
  if (diff < 3600) return Math.floor(diff/60) + " мин назад";
  if (diff < 86400) return Math.floor(diff/3600) + " ч назад";
  if (diff < 86400*2) return "вчера в " + new Date(t).toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit"});
  if (diff < 86400*30) return Math.floor(diff/86400) + " дн назад";
  return new Date(t).toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric"});
}
function fullDate(iso) {
  const t = parseIso(iso);
  if (isNaN(t)) return iso || "—";
  return new Date(t).toLocaleString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"});
}
function escHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function bareCaseNumber(n) {
  return String(n || "").trim().split(/[\\s(]/)[0];
}
// Достаёт номера из скобок hybrid-ID. Пример:
// "2-208/2026 (2-1148/2025;)" → ["2-1148/2025"].
function extractParenNumbers(s) {
  const m = String(s || "").match(/\\(([^)]+)\\)/);
  if (!m) return [];
  return m[1].split(/[;,]/).map((x) => bareCaseNumber(x)).filter(Boolean);
}
// Кладёт алиас в карту, не перезатирая уже существующий ключ —
// первое добавление становится канонической записью для алиаса.
function addAlias(map, key, payload) {
  const bare = bareCaseNumber(key);
  if (bare && !map.has(bare)) map.set(bare, payload);
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

// Состояние, разделяемое между блоками (обновляется в render()).
let casesMapGlobal = new Map();
let activeCasesGlobal = [];
let subsByEp = new Map();

// ── Блок «🚀 Прогоны GitHub Actions» ─────────────────────────────────────────
const WF_NAMES = {
  "update_cases.yml": "Основной прогон",
  "test_digest.yml": "Тест дайджеста",
  "tests.yml": "Тесты (pytest)",
  "probe_courts.yml": "Проба доступности судов",
  "replay_on_push.yml": "Дайджест-на-push",
  "pages-build-deployment": "Публикация Pages",
};
function wfShortName(run) {
  const base = String(run.path || "").split("/").pop();
  return WF_NAMES[base] || run.name || base || "?";
}
function runIcon(run) {
  if (run.status !== "completed") return "⏳";
  if (run.conclusion === "success") return "✅";
  if (run.conclusion === "failure" || run.conclusion === "startup_failure"
      || run.conclusion === "timed_out") return "❌";
  return "⚪";
}
function fmtDur(startIso, endIso) {
  const a = parseIso(startIso);
  const b = endIso ? parseIso(endIso) : Date.now();
  if (isNaN(a) || isNaN(b) || b < a) return "";
  const s = Math.round((b - a) / 1000);
  if (s < 60) return s + " с";
  if (s < 3600) return Math.round(s / 60) + " мин";
  return Math.floor(s / 3600) + " ч " + Math.round((s % 3600) / 60) + " мин";
}
let ghTimer = null;
async function loadGhRuns() {
  clearTimeout(ghTimer);
  const listEl = document.getElementById("runs-list");
  try {
    const r = await fetch("/admin/gh-runs?secret=" + encodeURIComponent(SECRET));
    const d = await r.json().catch(function () { return {}; });
    if (d.next_cron_at) {
      const t = parseIso(d.next_cron_at);
      if (!isNaN(t)) {
        document.getElementById("runs-next").textContent = "⏰ автозапуск: "
          + new Date(t).toLocaleString("ru-RU",
              { weekday: "short", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
      }
    }
    if (!r.ok) {
      const txt = String(d.error || "") + " " + String(d.detail || "");
      const hint = txt.indexOf("403") >= 0 ? " — похоже, у GITHUB_PAT нет прав actions:read" : "";
      listEl.innerHTML = '<div class="empty">GitHub API недоступен: '
        + escHtml(d.error || ("HTTP " + r.status)) + escHtml(hint) + '</div>';
      return;
    }
    const runs = (d.runs || []).slice(0, 8);
    if (!runs.length) {
      listEl.innerHTML = '<div class="empty">Прогонов пока нет</div>';
      return;
    }
    let hasActive = false;
    listEl.innerHTML = runs.map(function (run) {
      const active = run.status !== "completed";
      if (active) hasActive = true;
      const dur = fmtDur(run.run_started_at, active ? null : run.updated_at);
      return '<div class="run-row">' + runIcon(run)
        + ' <a class="run-name" href="' + escHtml(run.html_url) + '" target="_blank" rel="noopener noreferrer">'
        + escHtml(wfShortName(run)) + '</a>'
        + '<span class="run-meta">#' + escHtml(String(run.run_number || "?"))
        + ' · ' + escHtml(relTime(run.run_started_at))
        + (dur ? " · " + escHtml(dur) + (active ? " (идёт)" : "") : "")
        + '</span></div>';
    }).join("");
    // Пока есть живой прогон — обновляемся сами, чтобы видеть ✅/❌ без F5.
    if (hasActive) ghTimer = setTimeout(loadGhRuns, 15000);
  } catch (e) {
    listEl.innerHTML = '<div class="empty">Ошибка: ' + escHtml(String(e)) + '</div>';
  }
}
async function dispatchWorkflow(workflow, inputs, flashEl) {
  flashEl.className = "action-flash";
  flashEl.textContent = "запускаю…";
  try {
    const r = await fetch("/admin/dispatch?secret=" + encodeURIComponent(SECRET), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ workflow: workflow, inputs: inputs }),
    });
    const d = await r.json().catch(function () { return {}; });
    if (r.ok && d.ok) {
      flashEl.className = "action-flash ok";
      flashEl.textContent = "✓ запущен — статус появится в списке";
      // GitHub регистрирует run не мгновенно — обновим список дважды.
      setTimeout(loadGhRuns, 3000);
      setTimeout(loadGhRuns, 12000);
    } else {
      flashEl.className = "action-flash err";
      flashEl.textContent = "× " + (d.error || d.detail || ("HTTP " + r.status));
    }
  } catch (e) {
    flashEl.className = "action-flash err";
    flashEl.textContent = "× " + e;
  }
  setTimeout(function () { flashEl.textContent = ""; flashEl.className = "action-flash"; }, 9000);
}
document.getElementById("btn-run-main").addEventListener("click", function () {
  if (!confirm("Запустить полный прогон сейчас?\\n\\nПарсинг всех судов + дайджест + Telegram + push подписчикам — как ручной запуск из GitHub UI (без smart-skip).")) return;
  dispatchWorkflow("update_cases.yml", { smart_skip: "false" }, document.getElementById("runs-flash"));
});

// ── Блок «🛰 Парсинг на Mac» (резерв): вехи прогона, автообновление ──────────
function progressAgo(iso) {
  const t = parseIso(iso);
  if (isNaN(t)) return "";
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 90) return Math.round(s) + " сек назад";
  if (s < 5400) return Math.round(s / 60) + " мин назад";
  return new Date(t).toLocaleString("ru-RU");
}
let progressTimer = null;
async function loadProgress() {
  try {
    const r = await fetch("/admin/run-progress?secret=" + encodeURIComponent(SECRET));
    if (!r.ok) return;
    const d = await r.json();
    const card = document.getElementById("progress-card");
    const stale = document.getElementById("progress-stale");
    const cur = d.current;
    if (!cur) { card.style.display = "none"; stale.style.display = "none"; return; }
    const running = cur.done !== true;
    // Mac — спящий резерв: завершённый прогон старше суток не заслуживает
    // большой карточки, сворачиваем в details-строку.
    const isStale = !running && (Date.now() - parseIso(cur.updated_at)) > 24 * 3600 * 1000;
    if (isStale) {
      card.style.display = "none";
      stale.style.display = "";
      document.getElementById("progress-stale-sum").textContent =
        "🛰 Резерв: парсинг на Mac — последний прогон " + fullDate(cur.updated_at);
      document.getElementById("progress-stale-log").textContent = (cur.lines || []).join("\\n");
      return;
    }
    stale.style.display = "none";
    card.style.display = "";
    const st = document.getElementById("progress-state");
    st.textContent = running ? "⏳ идёт" : "✅ завершён";
    st.className = "progress-state " + (running ? "running" : "done");
    document.getElementById("progress-meta").textContent =
      "обновлено " + progressAgo(cur.updated_at) + " · старт " + progressAgo(cur.started_at);
    const logEl = document.getElementById("progress-log");
    const atBottom = logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 40;
    logEl.textContent = (cur.lines || []).join("\\n");
    if (atBottom) logEl.scrollTop = logEl.scrollHeight;
    if (d.prev && Array.isArray(d.prev.lines) && d.prev.lines.length) {
      document.getElementById("progress-prev").style.display = "";
      document.getElementById("progress-prev-log").textContent = d.prev.lines.join("\\n");
    }
    clearTimeout(progressTimer);
    if (running) progressTimer = setTimeout(loadProgress, 5000);
  } catch (e) { /* сеть мигнула — не мешаем остальной админке */ }
}

// ── Блок «🩺 Здоровье парсеров» (parse_health.json с GitHub Pages) ───────────
const COURT_NAMES = {
  "appeal:oblsud": "Суд ХМАО-Югры (апелляция)",
  "cassation:7kas:total": "7 КСОЮ — весь поиск",
  "cassation:7kas:hmao": "7 КСОЮ — ХМАО-фильтр",
  "fi:surggor--hmao.sudrf.ru": "Сургутский городской суд",
  "fi:surgray--hmao.sudrf.ru": "Сургутский районный суд",
  "fi:vartovgor--hmao.sudrf.ru": "Нижневартовский городской суд",
  "fi:vartovray--hmao.sudrf.ru": "Нижневартовский районный суд",
  "fi:hmray--hmao.sudrf.ru": "Ханты-Мансийский районный суд",
  "fi:uray--hmao.sudrf.ru": "Урайский городской суд",
  "fi:nyagan--hmao.sudrf.ru": "Няганский городской суд",
  "fi:uganskray--hmao.sudrf.ru": "Нефтеюганский районный суд",
  "fi:kogalym--hmao.sudrf.ru": "Когалымский городской суд",
  "fi:kondinsk--hmao.sudrf.ru": "Кондинский районный суд",
  "fi:langepas--hmao.sudrf.ru": "Лангепасский городской суд",
  "fi:megion--hmao.sudrf.ru": "Мегионский городской суд",
  "fi:sovetsk--hmao.sudrf.ru": "Советский районный суд",
  "fi:ugorsk--hmao.sudrf.ru": "Югорский районный суд",
  "fi:bel--hmao.sudrf.ru": "Белоярский городской суд",
  "fi:pth--hmao.sudrf.ru": "Пыть-Яхский городской суд",
  "fi:berezovo--hmao.sudrf.ru": "Берёзовский районный суд",
  "fi:rdj--hmao.sudrf.ru": "Радужнинский городской суд",
  "fi:oktb--hmao.sudrf.ru": "Октябрьский районный суд",
};
function healthMedian(arr) {
  const a = (arr || []).slice().sort(function (x, y) { return x - y; });
  if (!a.length) return 0;
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}
function healthSpark(counts) {
  const last = (counts || []).slice(-12);
  if (!last.length) return "";
  const max = Math.max.apply(null, last);
  const blocks = "▁▂▃▄▅▆▇█";
  return last.map(function (c) {
    if (max <= 0) return "▁";
    return blocks[Math.min(blocks.length - 1, Math.round((c / max) * (blocks.length - 1)))];
  }).join("");
}
// Светофор зеркалит семантику health.py: тревожен ноль там, где обычно
// что-то находится (медиана ≥1), и серия HTTP-фейлов.
function healthColor(s) {
  const med = healthMedian(s.counts);
  if ((s.fail_streak || 0) >= 3 || s.alerted_zero) return "🔴";
  if ((s.fail_streak || 0) >= 1 || ((s.zero_streak || 0) >= 1 && med >= 1)) return "🟡";
  return "🟢";
}
async function loadHealth() {
  const body = document.getElementById("health-body");
  const sum = document.getElementById("health-sum");
  try {
    const r = await fetch(HEALTH_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    const sources = d.sources || {};
    const keys = Object.keys(sources);
    if (!keys.length) { body.className = ""; body.innerHTML = '<div class="empty">Журнал пуст</div>'; return; }
    let nRed = 0, nYellow = 0;
    const rows = keys.map(function (k) {
      const s = sources[k] || {};
      const color = healthColor(s);
      if (color === "🔴") nRed++;
      else if (color === "🟡") nYellow++;
      const streaks = [];
      if ((s.zero_streak || 0) > 0) streaks.push("нулей подряд: " + s.zero_streak);
      if ((s.fail_streak || 0) > 0) streaks.push("HTTP-фейлов подряд: " + s.fail_streak);
      return '<div class="health-row">' + color
        + ' <span class="health-name">' + escHtml(COURT_NAMES[k] || k) + '</span>'
        + '<span class="health-spark">' + healthSpark(s.counts) + '</span>'
        + '<span class="health-meta">посл.: ' + escHtml(String(s.last_count ?? "—"))
        + (streaks.length ? " · " + escHtml(streaks.join(" · ")) : "") + '</span>'
        + '</div>';
    });
    body.className = "";
    body.innerHTML = rows.join("")
      + '<div class="llm-meta">Число результатов поиска по прогонам (история справа новее) · обновлено '
      + escHtml(relTime(d.updated_at)) + '</div>';
    const green = keys.length - nRed - nYellow;
    sum.textContent = "🩺 Здоровье парсеров — " + keys.length + " источников · "
      + (nRed || nYellow
          ? (nRed ? nRed + " 🔴 · " : "") + (nYellow ? nYellow + " 🟡 · " : "") + green + " 🟢"
          : "всё 🟢");
  } catch (e) {
    body.className = "";
    body.innerHTML = '<div class="empty">Не удалось загрузить parse_health.json: ' + escHtml(String(e)) + '</div>';
  }
}

// ── Блок «🧠 Топ бесплатных LLM»: рейтинг shir-man + мини-форма теста ────────
// Рейтинг грузим лениво — при первом раскрытии details (API отдаёт CORS *).
let llmTopLoaded = false;
async function loadLlmTop() {
  if (llmTopLoaded) return;
  llmTopLoaded = true;
  const el = document.getElementById("llm-top-body");
  try {
    const r = await fetch("https://shir-man.com/api/free-llm/top-models");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    const models = (d.models || []).slice(0, 5);
    if (!models.length) { el.textContent = "Рейтинг пуст."; el.className = ""; return; }
    el.className = "";
    el.innerHTML = models.map(function (m, i) {
      return '<div class="llm-row"><b>топ-' + (i + 1) + '</b> · ' + escHtml(m.id || "?")
        + (m.contextLength ? ' <span style="color:var(--fg-3)">(' + Math.round(m.contextLength / 1024) + 'k контекст)</span>' : '')
        + '</div>';
    }).join("")
      + '<div class="llm-meta">Рейтинг обновлён: '
      + (d.updatedAt ? new Date(d.updatedAt).toLocaleString("ru-RU") : "?")
      + ' · «топ-N» в форме ниже — это место в этом рейтинге · '
      + '<a href="https://github.com/SelivanovAS/dashboard/actions/workflows/test_digest.yml" target="_blank" rel="noopener noreferrer">форма в GitHub UI</a></div>';
    // Подписи «топ-N» в селекте обогащаем конкретными моделями (value не трогаем).
    const orSel = document.getElementById("tf-or");
    models.forEach(function (m, i) {
      if (orSel.options[i] && m.id) {
        orSel.options[i].textContent = orSel.options[i].value + " · " + m.id;
      }
    });
  } catch (e) {
    el.textContent = "Не удалось загрузить рейтинг: " + e;
    llmTopLoaded = false; // при следующем раскрытии попробуем ещё раз
  }
}
document.getElementById("llm-top").addEventListener("toggle", function () {
  if (this.open) loadLlmTop();
});
document.getElementById("tf-provider").addEventListener("change", function () {
  document.getElementById("tf-giga-wrap").style.display = this.value === "gigachat" ? "" : "none";
  document.getElementById("tf-or-wrap").style.display = this.value === "openrouter" ? "" : "none";
});
document.getElementById("tf-run").addEventListener("click", function () {
  const provider = document.getElementById("tf-provider").value;
  const toGroup = document.getElementById("tf-to-group").checked;
  const pushAll = document.getElementById("tf-push-all").checked;
  const inputs = {
    llm_provider: provider,
    to_group: toGroup ? "true" : "false",
    push_all: pushAll ? "true" : "false",
    full_llm: document.getElementById("tf-full-llm").checked ? "true" : "false",
    commit_results: document.getElementById("tf-commit").checked ? "true" : "false",
  };
  if (provider === "gigachat") inputs.gigachat_model = document.getElementById("tf-giga").value;
  if (provider === "openrouter") inputs.openrouter_model = document.getElementById("tf-or").value;
  const manual = document.getElementById("tf-model").value.trim();
  if (manual) inputs.llm_model = manual;
  if (toGroup || pushAll) {
    const parts = [];
    if (toGroup) parts.push("дайджест уйдёт в КОРПОРАТИВНУЮ ГРУППУ");
    if (pushAll) parts.push("push уйдёт ВСЕМ подписчикам");
    if (!confirm("Внимание: " + parts.join(" и ") + ". Продолжить?")) return;
  }
  dispatchWorkflow("test_digest.yml", inputs, document.getElementById("tf-flash"));
});

// ── Данные подписок + карточки ───────────────────────────────────────────────
async function fetchAll() {
  const results = await Promise.all([
    fetch("/admin/data?secret=" + encodeURIComponent(SECRET)),
    fetch(CASES_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(PUSHES_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(DIGEST_URL, { cache: "no-cache" }).catch(function () { return null; }),
  ]);
  const subsRes = results[0];
  if (!subsRes.ok) throw new Error("HTTP " + subsRes.status + " /admin/data");
  const subs = await subsRes.json();
  const casesMap = new Map();
  const activeCases = [];
  try {
    const casesRes = results[1];
    if (casesRes && casesRes.ok) {
      const casesJson = await casesRes.json();
      const list = Array.isArray(casesJson?.cases) ? casesJson.cases : [];
      for (const c of list) {
        // Канонический bare-id — приоритетный ключ карты. Если его нет
        // (теоретически невозможно), пропускаем запись целиком.
        const canonical = bareCaseNumber(c.id);
        if (!canonical) continue;
        const payload = {
          plaintiff: c.plaintiff || "",
          defendant: c.defendant || "",
          court: c.first_instance?.court || c.appeal?.court || "",
          stage: c.current_stage || "",
          canonical_id: canonical,
        };
        activeCases.push({
          id: canonical,
          plaintiff: payload.plaintiff,
          defendant: payload.defendant,
          court: payload.court,
        });
        // Канонический ID — первым (он же дефолт для алиаса).
        addAlias(casesMap, c.id, payload);
        // Алиасы: FI / апелл. / касс. (касс. бывает в двух полях —
        // case_number и cassation_number, заполняем оба варианта).
        // material_number — М-предок дела (Этап 3): когда юрист звёздит
        // материал, а парсер потом промоутит его в 2-XXX, эта связь
        // сохраняется и звезда не теряется.
        addAlias(casesMap, c.first_instance?.case_number, payload);
        addAlias(casesMap, c.first_instance?.material_number, payload);
        addAlias(casesMap, c.appeal?.case_number, payload);
        addAlias(casesMap, c.cassation?.case_number, payload);
        addAlias(casesMap, c.cassation?.cassation_number, payload);
        // Предыдущие номера из hybrid-ID '2-208/2026 (2-1148/2025;)'.
        for (const prev of extractParenNumbers(c.id)) {
          addAlias(casesMap, prev, payload);
        }
      }
    }
  } catch (e) {
    console.warn("cases.json не загружен:", e);
  }
  // Журнал последней push-рассылки. Собираем карту endpoint → запись;
  // если файла нет (старый деплой / только что чистый репо) — пустая карта.
  const pushesMap = new Map();
  let pushesGeneratedAt = "";
  try {
    const r = results[2];
    if (r && r.ok) {
      const j = await r.json();
      pushesGeneratedAt = j?.generated_at || "";
      for (const item of (j?.items || [])) {
        if (item?.endpoint) pushesMap.set(item.endpoint, item);
      }
    }
  } catch (e) {
    console.warn("last_personal_pushes.json не загружен:", e);
  }
  let digest = null;
  try {
    const r = results[3];
    if (r && r.ok) digest = await r.json();
  } catch (e) {
    console.warn("last_digest.json не загружен:", e);
  }
  return { subs, casesMap, activeCases, pushesMap, pushesGeneratedAt, digest };
}

// Тонкая карточка «📨 Последний дайджест» + агрегат последней push-рассылки.
function renderDigestCard(digest, pushesMap, pushesGeneratedAt) {
  const el = document.getElementById("digest-card");
  if (!digest || !digest.generated_at) { el.style.display = "none"; return; }
  const stats = {};
  pushesMap.forEach(function (item) {
    const v = item.variant || "?";
    stats[v] = (stats[v] || 0) + 1;
  });
  const pushParts = ["personal", "general", "broadcast", "skip"]
    .filter(function (k) { return stats[k]; })
    .map(function (k) { return stats[k] + " " + k; });
  let html = '<span>📨 <b>Последний дайджест:</b> ' + escHtml(relTime(digest.generated_at))
    + ' <span class="dot">(' + escHtml(fullDate(digest.generated_at)) + ')</span></span>';
  const sum = digest.is_empty ? "изменений не было" : (digest.summary || "");
  if (sum) html += '<span class="dot">·</span><span>' + escHtml(sum) + '</span>';
  if (pushParts.length) {
    html += '<span class="dot">·</span><span>push: ' + escHtml(pushParts.join(" / "))
      + (pushesGeneratedAt ? ' <span class="dot">(' + escHtml(relTime(pushesGeneratedAt)) + ')</span>' : '')
      + '</span>';
  }
  html += '<span class="dot">·</span><a href="' + DASHBOARD_URL + '?digest=open" target="_blank" rel="noopener noreferrer">открыть на дашборде</a>';
  el.innerHTML = html;
  el.style.display = "";
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
        + escHtml(item.click_url) + '" target="_blank" rel="noopener noreferrer">'
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

// KV-TTL подписки — 60 дней от последней записи; last_seen_at обновляется на
// каждый вход в PWA. Долгое отсутствие входа = подписка тихо истечёт и юрист
// перестанет получать push. Предупреждаем заранее.
function expiryBadge(sub) {
  const t = parseIso(sub.last_seen_at);
  if (isNaN(t)) return "";
  const days = (Date.now() - t) / 86400000;
  if (days < 45) return "";
  const left = Math.round(60 - days);
  const txt = left > 0 ? "истекает ≈ через " + left + " дн — нужен вход в PWA" : "могла истечь — нужен вход в PWA";
  return '<span class="badge-expiry">⏳ ' + txt + '</span>';
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
          // Алиас-плашка: ★ стоит на номере, который отличается от
          // канонического ID дела (звезда выставлена по апел./касс./
          // hybrid-предку, а дело хранится под номером 1-й инст.).
          const aliasNote = (c.canonical_id && c.canonical_id !== bare)
            ? '<span class="case-alias">→ '+escHtml(c.canonical_id)+'</span>'
            : '';
          return '<div class="case-row"><span class="case-num">'+escHtml(num)+'</span>'
                 + aliasNote
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
    +   expiryBadge(sub)
    +   '<span class="kv"><b>Создана:</b> '+escHtml(relTime(sub.created_at))+'</span>'
    +   '<span class="kv"><b>Последний вход:</b> '+escHtml(relTime(sub.last_seen_at))+' <span style="color:var(--fg-3)">('+escHtml(fullDate(sub.last_seen_at))+')</span></span>'
    +   '<span class="kv"><b>Watchlist обновлён:</b> '+escHtml(relTime(sub.last_watchlist_update_at))+'</span>'
    +   '<span class="kv"><b>Дел:</b> '+wl.length+'</span>'
    + '</div>'
    + '<div class="kv endpoint" title="'+ep+'">…'+ep+'</div>'
    + '<div class="actions">'
    +   '<button class="btn" data-action="rename">✏ Имя</button>'
    +   '<button class="btn" data-action="watchlist">📋 Ред. watchlist</button>'
    +   '<button class="btn" data-action="testpush">📨 Тест push</button>'
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
  setTimeout(() => { el.textContent = ""; el.className = "action-flash"; }, 5000);
}

// ── Модалка редактирования watchlist ─────────────────────────────────────────
// Заменяет prompt() со списком через запятую: чекбоксы по активным делам из
// cases.json + ручное добавление номеров, которых в активных делах нет.
let wlState = null; // { endpoint, selected:Set, extras:[], card }
function openWlModal(card, sub) {
  const selected = new Set();
  const extras = [];
  for (const num of (Array.isArray(sub.watchlist) ? sub.watchlist : [])) {
    const c = casesMapGlobal.get(bareCaseNumber(num));
    if (c) selected.add(c.canonical_id);
    else if (extras.indexOf(num) < 0) extras.push(num);
  }
  wlState = { endpoint: sub.endpoint, selected: selected, extras: extras, card: card };
  document.getElementById("wl-who").textContent = sub.label || detectDevice(sub.user_agent);
  document.getElementById("wl-search").value = "";
  document.getElementById("wl-manual-input").value = "";
  buildWlList("");
  renderWlExtras();
  updateWlCount();
  document.getElementById("wl-modal").showModal();
}
function buildWlList(query) {
  if (!wlState) return;
  const q = String(query || "").trim().toLowerCase();
  const rows = activeCasesGlobal.filter(function (c) {
    if (!q) return true;
    return (c.id + " " + c.plaintiff + " " + c.defendant + " " + c.court).toLowerCase().indexOf(q) >= 0;
  }).map(function (c) {
    const parties = (c.plaintiff && c.defendant)
      ? c.plaintiff + " vs " + c.defendant
      : (c.plaintiff || c.defendant || "");
    return '<label class="wl-row"><input type="checkbox" data-case-id="' + escHtml(c.id) + '"'
      + (wlState.selected.has(c.id) ? " checked" : "") + '>'
      + '<span class="wl-num">' + escHtml(c.id) + '</span>'
      + '<span class="wl-parties">' + escHtml(parties)
      + (c.court ? ' <span class="dot">·</span> ' + escHtml(c.court) : '') + '</span>'
      + '</label>';
  });
  document.getElementById("wl-list").innerHTML =
    rows.join("") || '<div class="empty">Ничего не найдено</div>';
}
function renderWlExtras() {
  const el = document.getElementById("wl-extras");
  if (!wlState || !wlState.extras.length) { el.innerHTML = ""; return; }
  el.innerHTML = '<div class="llm-meta" style="margin-top:8px;">Номера не из активных дел (уйдут как есть):</div>'
    + wlState.extras.map(function (n) {
      return '<div class="wl-row" style="cursor:default;"><span class="wl-num">' + escHtml(n) + '</span>'
        + '<button class="btn" type="button" data-extra-del="' + escHtml(n) + '">✕</button></div>';
    }).join("");
}
function updateWlCount() {
  if (!wlState) return;
  document.getElementById("wl-count").textContent =
    "выбрано: " + (wlState.selected.size + wlState.extras.length);
}
function addManualCase() {
  if (!wlState) return;
  const inp = document.getElementById("wl-manual-input");
  const v = inp.value.trim();
  if (!v) return;
  const c = casesMapGlobal.get(bareCaseNumber(v));
  if (c) {
    // Номер известен (в т.ч. как алиас) — просто ставим галку на деле.
    wlState.selected.add(c.canonical_id);
    buildWlList(document.getElementById("wl-search").value);
  } else if (wlState.extras.indexOf(v) < 0) {
    wlState.extras.push(v);
    renderWlExtras();
  }
  inp.value = "";
  updateWlCount();
}
async function saveWlModal() {
  if (!wlState) return;
  const list = Array.from(wlState.selected).concat(wlState.extras);
  const btn = document.getElementById("wl-save");
  btn.disabled = true;
  btn.textContent = "Сохраняю…";
  const res = await postAdmin("/admin/watchlist", { endpoint: wlState.endpoint, watchlist: list });
  btn.disabled = false;
  btn.textContent = "💾 Сохранить";
  if (res.ok) {
    const card = wlState.card;
    document.getElementById("wl-modal").close();
    wlState = null;
    if (card) flash(card, "✓ " + ((res.data && res.data.count) ?? 0) + " дел", "ok");
    render(true);
  } else if (wlState.card) {
    flash(wlState.card, "× ошибка сохранения", "err");
  }
}
document.getElementById("wl-search").addEventListener("input", function () { buildWlList(this.value); });
document.getElementById("wl-list").addEventListener("change", function (e) {
  const cb = e.target.closest("input[data-case-id]");
  if (!cb || !wlState) return;
  const id = cb.getAttribute("data-case-id");
  if (cb.checked) wlState.selected.add(id);
  else wlState.selected.delete(id);
  updateWlCount();
});
document.getElementById("wl-extras").addEventListener("click", function (e) {
  const btn = e.target.closest("[data-extra-del]");
  if (!btn || !wlState) return;
  const num = btn.getAttribute("data-extra-del");
  wlState.extras = wlState.extras.filter(function (x) { return x !== num; });
  renderWlExtras();
  updateWlCount();
});
document.getElementById("wl-manual-add").addEventListener("click", addManualCase);
document.getElementById("wl-manual-input").addEventListener("keydown", function (e) {
  if (e.key === "Enter") { e.preventDefault(); addManualCase(); }
});
document.getElementById("wl-cancel").addEventListener("click", function () {
  document.getElementById("wl-modal").close();
  wlState = null;
});
document.getElementById("wl-save").addEventListener("click", saveWlModal);

// ── Действия на карточках подписок ───────────────────────────────────────────
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
    openWlModal(card, currentSub);
  } else if (action === "testpush") {
    const lbl = currentSub.label || detectDevice(currentSub.user_agent);
    if (!confirm("Отправить тестовый push на «" + lbl + "»? Придёт уведомление «есть обновления по делам».")) return;
    flash(card, "отправляю…", "");
    const res = await postAdmin("/admin/test-push", { endpoint });
    const d = res.data || {};
    if (res.ok && d.ok) {
      flash(card, "✓ доставлен push-сервису (" + (d.status || "?") + ")", "ok");
    } else if (d.error === "endpoint_dead") {
      flash(card, "× endpoint мёртв (" + (d.status || "?") + ") — подписка удалена", "err");
      setTimeout(function () { render(true); }, 1500);
    } else {
      flash(card, "× " + String(d.error || ("HTTP " + res.status)).slice(0, 120), "err");
    }
  }
}

async function render(force) {
  const root = document.getElementById("root");
  if (force) { root.className = "loading"; root.textContent = "Загрузка…"; }
  try {
    const all = await fetchAll();
    const subs = all.subs;
    casesMapGlobal = all.casesMap;
    activeCasesGlobal = all.activeCases;
    renderDigestCard(all.digest, all.pushesMap, all.pushesGeneratedAt);
    const owners = subs.filter((s) => s.is_owner).length;
    const totalWl = subs.reduce((a, s) => a + (s.watchlist?.length || 0), 0);
    const pushTime = all.pushesGeneratedAt ? " · последний push: " + relTime(all.pushesGeneratedAt) : "";
    document.getElementById("summary").textContent =
      subs.length + " подписок · " + owners + " owner · " + totalWl + " дел в watchlist'ах" + pushTime;
    // Сортируем: owner вверх, затем по последнему входу (свежие первыми).
    subs.sort((a, b) => {
      if (a.is_owner !== b.is_owner) return a.is_owner ? -1 : 1;
      const ta = parseIso(a.last_seen_at) || 0;
      const tb = parseIso(b.last_seen_at) || 0;
      return tb - ta;
    });
    root.className = "subs";
    root.innerHTML = subs.map((s) => renderCard(s, all.casesMap, all.pushesMap.get(s.endpoint), all.pushesGeneratedAt)).join("");
    if (subs.length === 0) {
      root.innerHTML = '<div class="empty">Подписок нет.</div>';
    }
    subsByEp = new Map(subs.map((s) => [s.endpoint, s]));
  } catch (e) {
    root.className = "error";
    root.textContent = "Ошибка: " + e.message;
  }
}

// Делегированный клик по кнопкам действий — вешается ОДИН раз (раньше висел
// внутри render() и после каждого обновления обработчики множились: prompt
// выскакивал по 2-3 раза).
document.getElementById("root").addEventListener("click", (e) => {
  const btn = e.target.closest("[data-action]");
  if (!btn) return;
  const card = btn.closest(".sub-card");
  if (!card) return;
  const sub = subsByEp.get(card.getAttribute("data-endpoint"));
  if (!sub) return;
  handleAction(card, btn.getAttribute("data-action"), sub);
});

function refreshAll() {
  render(true);
  loadGhRuns();
  loadHealth();
  loadProgress();
}

loadProgress();
loadGhRuns();
loadHealth();
render();
</script>
</body></html>`;
}
