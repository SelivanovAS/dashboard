// Страница админки подписчиков (/admin) — отдельный модуль, чтобы не раздувать
// worker.js. `wrangler deploy` бандлит импорт сам (esbuild).
//
// Дизайн v2 (13.07.2026, согласованный мокап): визуальный язык дашборда
// (токены styles.css, IBM Plex, бейджи-пилюли, glass-шапка, 3-режимная тема),
// IA — «пульт» из 4 плиток + секции #system / #llm / #subs с чипами-якорями.
//
// ⚠️ Экранирование: всё содержимое — ОДИН внешний template literal. Внутренний
// JS страницы пишется БЕЗ template literals и без `${` (только конкатенация),
// а backslash в его регексах/строках удваивается (`\\d`, `\\n`). Интерполяции
// внешнего литерала — только значения из аргументов (SECRET/ROLE/CFG) и
// модульные хелперы-константы; всё интерполируемое проходит JSON.stringify.
//
// Роли (16.07.2026): owner — всё как раньше; operator (сопровождающие
// капчёвых судов) — статус, здоровье, живой лог и «Импорт дел». Owner-блоки
// скрываются атрибутом data-owner-only + html[data-role] (реальный запрет —
// на эндпоинтах Worker'а: /admin/data и др. отдают оператору 403).

export function renderAdminHtml(secret, role, cfg) {
  role = role === "operator" ? "operator" : "owner";
  // Производные URL территории: приходят из worker.js (adminPageConfig()).
  // Фолбэки — боевые значения ХМАО-инстанса (деплой без [vars] работает).
  const base = (cfg && cfg.siteBase) || "https://selivanovas.github.io/dashboard";
  const CFG = {
    casesUrl: (cfg && cfg.casesUrl) || base + "/data/cases.json",
    archiveUrl: (cfg && cfg.archiveUrl) || base + "/data/cases_archive.json",
    pushesUrl: (cfg && cfg.pushesUrl) || base + "/data/last_personal_pushes.json",
    digestUrl: (cfg && cfg.digestUrl) || base + "/data/last_digest.json",
    healthUrl: (cfg && cfg.healthUrl) || base + "/data/parse_health.json",
    dashboardUrl: (cfg && cfg.dashboardUrl) || base + "/sberbank_dashboard.html",
    siteBase: base,
    ghRepo: (cfg && cfg.ghRepo) || "SelivanovAS/dashboard",
  };
  return `<!doctype html><html lang="ru" data-role="${role}"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="referrer" content="no-referrer">
<title>Админка · мониторинг дел Сбера</title>
<meta name="theme-color" content="#21a038">
<script>
  // Тема: 3 состояния — 'auto' | 'light' | 'dark' (паттерн дашборда, анти-FOUC).
  (function(){
    var ROOT = document.documentElement;
    var MQ = window.matchMedia('(prefers-color-scheme: dark)');
    var listener = null;
    function readPref(){
      try { return localStorage.getItem('admin_theme') || 'auto'; } catch(e){ return 'auto'; }
    }
    function applyResolved(pref){
      var resolved = (pref === 'auto') ? (MQ.matches ? 'dark' : 'light') : pref;
      ROOT.setAttribute('data-theme', resolved);
      ROOT.setAttribute('data-theme-pref', pref);
      var meta = document.querySelector('meta[name="theme-color"]');
      if (meta) meta.setAttribute('content', resolved === 'dark' ? '#0f1217' : '#21a038');
    }
    function bindAuto(pref){
      if (listener) {
        if (MQ.removeEventListener) MQ.removeEventListener('change', listener);
        else MQ.removeListener(listener);
        listener = null;
      }
      if (pref === 'auto') {
        listener = function(){ applyResolved('auto'); };
        if (MQ.addEventListener) MQ.addEventListener('change', listener);
        else MQ.addListener(listener);
      }
    }
    function setTheme(pref){
      try { localStorage.setItem('admin_theme', pref); } catch(e){}
      applyResolved(pref);
      bindAuto(pref);
    }
    window.toggleTheme = function(){
      var order = ['auto', 'light', 'dark'];
      var cur = readPref();
      var next = order[(order.indexOf(cur) + 1) % order.length];
      setTheme(next);
    };
    var initial = readPref();
    applyResolved(initial);
    bindAuto(initial);
  })();
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap&subset=cyrillic,cyrillic-ext,latin,latin-ext" rel="stylesheet">
<style>
/* ═══ Токены — перенос из styles.css дашборда ═══ */
:root {
  --sber-green-700: #157f3a;
  --sber-green-600: #1a9f29;
  --sber-green-500: #21a038;
  --sber-green-100: #e1f5e5;
  --sber-green-50:  #f1faf3;

  --gray-900: #14181f; --gray-700: #2d333d; --gray-600: #4a5160;
  --gray-500: #6b7280; --gray-400: #9aa0ac; --gray-300: #c8ccd4;
  --gray-200: #e4e7eb; --gray-150: #eef0f3; --gray-100: #f4f6f8;
  --white: #ffffff;

  --blue-700: #1e4dbb; --blue-500: #3b82f6; --blue-100: #dbeafe;
  --amber-700: #92400e; --amber-600: #d97706; --amber-500: #f59e0b; --amber-100: #fef3c7;
  --red-700: #991b1b; --red-600: #dc2626; --red-500: #ef4444; --red-100: #fee2e2;
  --violet-700: #6d28d9; --violet-100: #ede9fe;
  --teal-800: #115e59; --teal-100: #ccfbf1;
  --indigo-700: #3730a3; --indigo-100: #e0e7ff;
  --green-favorable-fg: #065f46; --green-favorable-bg: #d1fae5;

  --fg-1: var(--gray-900); --fg-2: var(--gray-700); --fg-3: var(--gray-600); --fg-4: var(--gray-500);
  --bg-1: var(--white); --bg-2: #eceef2; --bg-3: var(--gray-100); --bg-4: var(--gray-150);
  --border: var(--gray-200); --border-strong: var(--gray-300); --divider: var(--gray-150);

  --accent: var(--sber-green-500);
  --accent-hover: var(--sber-green-600);
  --accent-active: var(--sber-green-700);
  --accent-bg-soft: var(--sber-green-50);
  --accent-bg-strong: var(--sber-green-100);
  --focus-ring: 0 0 0 3px rgba(33, 160, 56, 0.30);

  --stage-fi-fg: var(--teal-800);       --stage-fi-bg: var(--teal-100);
  --stage-appeal-fg: var(--indigo-700); --stage-appeal-bg: var(--indigo-100);
  --stage-cass-fg: var(--violet-700);   --stage-cass-bg: var(--violet-100);

  --success-bg: var(--green-favorable-bg); --success-fg: var(--green-favorable-fg);
  --warning-bg: var(--amber-100); --warning-fg: var(--amber-700);
  --danger-bg: var(--red-100); --danger-fg: var(--red-700);
  --info-bg: var(--blue-100); --info-fg: var(--blue-700);

  --font-sans: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  --font-code: 'IBM Plex Mono', 'SF Mono', Menlo, Consolas, monospace;

  --fs-2xs: 11px; --fs-xs: 12px; --fs-sm: 13px; --fs-md: 15px;
  --fs-lg: 16px; --fs-xl: 18px; --fs-2xl: 22px; --fs-3xl: 28px;
  --fw-regular: 400; --fw-medium: 500; --fw-semibold: 600; --fw-bold: 700;

  --sp-1: 4px; --sp-2: 8px; --sp-3: 12px; --sp-4: 16px; --sp-5: 20px; --sp-6: 24px;
  --radius-xs: 4px; --radius-sm: 6px; --radius: 6px; --radius-md: 8px; --radius-lg: 12px; --radius-pill: 999px;

  --shadow-1: 0 1px 2px rgba(13,17,22,0.04);
  --shadow-md: 0 2px 6px rgba(13,17,22,0.06), 0 1px 2px rgba(13,17,22,0.04);
  --shadow-lg: 0 8px 24px rgba(13,17,22,0.08), 0 2px 6px rgba(13,17,22,0.04);

  --glass-bg: rgba(236, 238, 242, 0.72);
  --glass-border: rgba(15, 23, 42, 0.08);
  --glass-blur: saturate(180%) blur(24px);

  --ease-out: cubic-bezier(0.2, 0, 0, 1);
  --dur-fast: 120ms; --dur-base: 180ms;

  --content-max: 1140px;
  color-scheme: light;
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --fg-1: #ecedef; --fg-2: #c5c8cf; --fg-3: #a8aeba; --fg-4: #80868f;
  --bg-1: #181c22; --bg-2: #0f1217; --bg-3: #232830; --bg-4: #2c333d;
  --border: #2a2f38; --border-strong: #3a414c; --divider: #232830;
  --accent: #2fbf4a; --accent-hover: #3fd05c; --accent-active: #2aa540;
  --accent-bg-soft: rgba(47,191,74,0.10); --accent-bg-strong: rgba(47,191,74,0.20);
  --focus-ring: 0 0 0 3px rgba(47,191,74,0.40);
  --stage-fi-fg: #5eead4;     --stage-fi-bg: rgba(20,184,166,0.16);
  --stage-appeal-fg: #c7d2fe; --stage-appeal-bg: rgba(99,102,241,0.18);
  --stage-cass-fg: #c4b5fd;   --stage-cass-bg: rgba(139,92,246,0.20);
  --success-bg: rgba(47,191,74,0.18); --success-fg: #86efac;
  --warning-bg: rgba(245,158,11,0.18); --warning-fg: #f4c97c;
  --danger-bg: rgba(239,68,68,0.18); --danger-fg: #fca5a5;
  --info-bg: rgba(59,130,246,0.18); --info-fg: #8ab4ff;
  --amber-100: rgba(245,158,11,0.18); --amber-700: #f4c97c;
  --red-100: rgba(239,68,68,0.18); --red-700: #fca5a5;
  --blue-100: rgba(59,130,246,0.18); --blue-700: #8ab4ff;
  --green-favorable-bg: rgba(47,191,74,0.18); --green-favorable-fg: #86efac;
  --glass-bg: rgba(15, 18, 23, 0.66);
  --glass-border: rgba(255, 255, 255, 0.08);
  --shadow-1: 0 1px 2px rgba(0,0,0,0.40);
  --shadow-md: 0 2px 8px rgba(0,0,0,0.40), 0 1px 2px rgba(0,0,0,0.30);
  --shadow-lg: 0 12px 32px rgba(0,0,0,0.50), 0 2px 8px rgba(0,0,0,0.30);
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]):not([data-theme="dark"]) { color-scheme: dark; }
}
html { transition: background-color 200ms var(--ease-out); }
body, .app-header, .card, .stat-card, .chip-btn, .badge, .btn-refresh, .theme-toggle, .sub-card {
  transition: background-color 200ms var(--ease-out), border-color 200ms var(--ease-out), color 200ms var(--ease-out);
}

/* ═══ База ═══ */
* { margin:0; padding:0; box-sizing:border-box; }
html, body { overflow-x:hidden; max-width:100%; }
body {
  font-family: var(--font-sans);
  font-weight: var(--fw-medium);
  font-size: var(--fs-md);
  line-height: 1.5;
  color: var(--fg-1);
  background: var(--bg-2);
  font-variant-numeric: tabular-nums;
}
a { color: var(--accent); }

/* ═══ Шапка ═══ */
.app-header { position:sticky; top:0; z-index:100; background:var(--glass-bg);
  -webkit-backdrop-filter:var(--glass-blur); backdrop-filter:var(--glass-blur);
  border-bottom:1px solid var(--glass-border); padding:0 20px; }
@supports not (backdrop-filter: blur(1px)) { .app-header { background:var(--bg-2); } }
.header-inner { max-width:var(--content-max); margin:0 auto; display:flex; align-items:center;
  gap:16px; padding:10px 0; min-height:56px; flex-wrap:wrap; }
.header-brand { display:flex; align-items:center; gap:10px; margin-right:4px; }
.header-logo { width:32px; height:32px; flex-shrink:0; display:inline-flex; align-items:center;
  justify-content:center; border-radius:8px; background:var(--accent); color:#fff; }
.header-logo svg { width:18px; height:18px; }
.header-title { font-size:var(--fs-xl); font-weight:var(--fw-bold); letter-spacing:-0.01em; line-height:1.2; white-space:nowrap; }
.header-sub { font-size:var(--fs-2xs); color:var(--fg-3); line-height:1.2; }
.header-nav { display:flex; gap:6px; align-items:center; flex:1; min-width:0; overflow-x:auto;
  scrollbar-width:none; -webkit-overflow-scrolling:touch; }
.header-nav::-webkit-scrollbar { display:none; }
.header-actions { display:flex; align-items:center; gap:8px; margin-left:auto; }
.header-meta { font-size:var(--fs-xs); color:var(--fg-3); text-align:right; line-height:1.35; white-space:nowrap; }
.header-meta b { color:var(--fg-1); font-weight:var(--fw-semibold); }

.chip-btn { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; border:1px solid var(--border);
  border-radius:var(--radius-pill); background:var(--bg-1); color:var(--fg-2); font-size:var(--fs-sm);
  font-weight:var(--fw-semibold); cursor:pointer; transition:all 120ms var(--ease-out);
  font-family:var(--font-sans); white-space:nowrap; line-height:1.3; text-decoration:none; }
.chip-btn:hover { border-color:var(--border-strong); background:var(--bg-3); }
.chip-btn.active { background:var(--accent); border-color:var(--accent); color:#fff; }
.chip-count { display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px;
  padding:0 5px; border-radius:8px; background:var(--bg-3); color:var(--fg-3); font-size:10px; font-weight:var(--fw-bold); }
.chip-btn.active .chip-count { background:rgba(255,255,255,0.25); color:#fff; }

.theme-toggle { display:inline-flex; align-items:center; justify-content:center; width:38px; height:38px;
  padding:0; border:1px solid var(--border); background:var(--bg-1); color:var(--fg-2);
  border-radius:var(--radius); cursor:pointer; transition:all 120ms var(--ease-out); flex-shrink:0; }
.theme-toggle:hover { background:var(--bg-3); border-color:var(--border-strong); color:var(--fg-1); }
.theme-toggle:active { transform:scale(0.94); }
.theme-toggle svg { width:17px; height:17px; }
.theme-toggle .icon-moon, .theme-toggle .icon-sun, .theme-toggle .icon-auto { display:none; }
:root[data-theme-pref="auto"]  .theme-toggle .icon-auto { display:inline; }
:root[data-theme-pref="light"] .theme-toggle .icon-sun  { display:inline; }
:root[data-theme-pref="dark"]  .theme-toggle .icon-moon { display:inline; }

.btn-refresh { display:inline-flex; align-items:center; gap:7px; padding:8px 14px; background:var(--bg-1);
  color:var(--fg-1); border:1px solid var(--border); border-radius:var(--radius); font-size:var(--fs-sm);
  font-weight:var(--fw-semibold); font-family:var(--font-sans); cursor:pointer; transition:all 120ms var(--ease-out); flex-shrink:0; }
.btn-refresh:hover { background:var(--bg-3); border-color:var(--border-strong); }
.btn-refresh:active { transform:scale(0.97); }
.btn-refresh svg { width:15px; height:15px; }

/* ═══ Контент ═══ */
.app-main { max-width:var(--content-max); margin:0 auto; padding:20px 20px 48px; }

/* Пульт */
.pult { display:grid; grid-template-columns:repeat(4, 1fr); gap:12px; margin-bottom:26px; }
.stat-card { background:var(--bg-1); border-radius:var(--radius-md); padding:12px 14px;
  box-shadow:var(--shadow-1); border-left:3px solid var(--border-strong);
  transition:box-shadow 150ms var(--ease-out); cursor:pointer; text-align:left;
  border-top:0; border-right:0; border-bottom:0; font-family:var(--font-sans);
  display:block; width:100%; min-width:0; }
.stat-card:hover { box-shadow:var(--shadow-md); }
.stat-card[data-accent="green"] { border-left-color:var(--accent); }
.stat-card[data-accent="red"]   { border-left-color:var(--red-500); }
.stat-card[data-accent="amber"] { border-left-color:var(--amber-500); }
.stat-card[data-accent="blue"]  { border-left-color:var(--blue-500); }
.stat-card[data-accent="gray"]  { border-left-color:var(--border-strong); }
.stat-label { font-size:var(--fs-2xs); color:var(--fg-3); font-weight:var(--fw-semibold);
  text-transform:uppercase; letter-spacing:0.05em; }
.stat-value { font-size:var(--fs-2xl); font-weight:var(--fw-bold); letter-spacing:-0.02em;
  line-height:1.15; color:var(--fg-1); margin-top:4px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.stat-sub { font-size:var(--fs-xs); color:var(--fg-3); margin-top:3px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }

/* Секции */
.section { margin-bottom:30px; scroll-margin-top:76px; }
.section-head { display:flex; align-items:center; gap:10px; margin-bottom:12px; flex-wrap:wrap; }
.section-icon { width:30px; height:30px; border-radius:var(--radius-md); background:var(--accent-bg-strong);
  color:var(--accent-active); display:inline-flex; align-items:center; justify-content:center; flex-shrink:0; }
:root[data-theme="dark"] .section-icon { color:var(--accent); }
.section-icon svg { width:16px; height:16px; }
.section-title { font-size:var(--fs-lg); font-weight:var(--fw-bold); letter-spacing:-0.01em; }
.section-counter { display:inline-block; padding:2px 10px; border-radius:var(--radius-pill);
  background:var(--bg-4); color:var(--fg-2); font-size:var(--fs-sm); font-weight:var(--fw-bold); }
.section-head .spacer { flex:1; }

.card { background:var(--bg-1); border-radius:var(--radius-lg); box-shadow:var(--shadow-1); padding:14px 16px; }
.card-head { display:flex; align-items:center; gap:10px; margin-bottom:8px; flex-wrap:wrap; }
.card-title { font-size:var(--fs-sm); font-weight:var(--fw-semibold); color:var(--fg-3); }
.card-head .spacer { flex:1; }

.system-grid { display:grid; grid-template-columns:7fr 5fr; gap:14px; align-items:start; }

/* Кнопки */
.btn-primary { display:inline-flex; align-items:center; gap:7px; padding:8px 16px; background:var(--accent);
  color:#fff; border:none; border-radius:var(--radius); font-size:var(--fs-sm); font-weight:var(--fw-semibold);
  cursor:pointer; font-family:var(--font-sans); transition:background 120ms var(--ease-out);
  white-space:nowrap; }
.btn-primary:hover { background:var(--accent-hover); }
.btn-primary:active { background:var(--accent-active); transform:scale(0.98); }
.btn-primary:disabled { opacity:0.6; cursor:default; }
.btn-primary svg { width:13px; height:13px; }
.btn-outline { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; background:var(--bg-1);
  border:1px solid var(--border); border-radius:var(--radius); font-size:var(--fs-sm); cursor:pointer;
  color:var(--fg-1); font-weight:var(--fw-semibold); font-family:var(--font-sans); transition:all 120ms var(--ease-out);
  white-space:nowrap; }
.btn-outline:hover { border-color:var(--border-strong); background:var(--bg-3); }
.btn-outline:disabled { opacity:0.6; cursor:default; }
.btn-outline.btn-danger { color:var(--danger-fg); }
.btn-outline.btn-danger:hover { border-color:var(--red-500); background:var(--danger-bg); }
.btn-icon { display:inline-flex; align-items:center; justify-content:center; width:30px; height:30px;
  padding:0; border:1px solid var(--border); background:var(--bg-1); color:var(--fg-3);
  border-radius:var(--radius); cursor:pointer; transition:all 120ms var(--ease-out); }
.btn-icon:hover { background:var(--bg-3); color:var(--fg-1); border-color:var(--border-strong); }
.btn-icon svg { width:14px; height:14px; }

/* Бейджи */
.badge { display:inline-flex; align-items:center; gap:4px; padding:3px 9px; border-radius:var(--radius-pill);
  font-size:var(--fs-2xs); font-weight:var(--fw-semibold); white-space:nowrap; line-height:1.3; }
.badge-owner { background:var(--warning-bg); color:var(--warning-fg); font-weight:var(--fw-bold);
  letter-spacing:0.03em; text-transform:uppercase; }
.badge-expiry { background:var(--warning-bg); color:var(--warning-fg); }
.badge-device { background:var(--bg-3); color:var(--fg-2); font-weight:var(--fw-medium); }
.badge-ok   { background:var(--success-bg); color:var(--success-fg); }
.badge-fail { background:var(--danger-bg); color:var(--danger-fg); }
.badge-run  { background:var(--warning-bg); color:var(--warning-fg); }
.badge-skip { background:var(--bg-4); color:var(--fg-3); }
.badge-fi        { background:var(--stage-fi-bg); color:var(--stage-fi-fg); font-weight:var(--fw-bold); letter-spacing:0.02em; }
.badge-appeal    { background:var(--stage-appeal-bg); color:var(--stage-appeal-fg); font-weight:var(--fw-bold); letter-spacing:0.02em; }
.badge-cassation { background:var(--stage-cass-bg); color:var(--stage-cass-fg); font-weight:var(--fw-bold); letter-spacing:0.02em; }
.badge-watch     { background:var(--info-bg); color:var(--info-fg); }
.badge-archive   { background:var(--bg-4); color:var(--fg-3); }

/* Статусные точки */
.dot { width:9px; height:9px; border-radius:50%; flex-shrink:0; display:inline-block; }
.dot-green { background:var(--accent); }
.dot-red   { background:var(--red-500); }
.dot-amber { background:var(--amber-500); }
.dot-gray  { background:var(--gray-400); }
.dot-pulse { animation:pulse 1.6s ease-in-out infinite; }
@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.35; } }
@media (prefers-reduced-motion: reduce) { .dot-pulse { animation:none; } }

/* Прогоны */
.run-row { display:flex; align-items:baseline; gap:10px; padding:7px 0; border-bottom:1px solid var(--divider); }
.run-row:last-child { border-bottom:0; }
.run-row .dot { align-self:center; }
.run-name { font-weight:var(--fw-semibold); color:var(--fg-1); text-decoration:none; font-size:var(--fs-sm); }
.run-name:hover { color:var(--accent); }
.run-meta { color:var(--fg-3); font-size:var(--fs-xs); }
.run-ext { margin-left:auto; color:var(--fg-4); flex-shrink:0; align-self:center; display:inline-flex; }
.run-ext svg { width:13px; height:13px; }
.run-ext:hover { color:var(--accent); }
.action-flash { font-size:var(--fs-2xs); color:var(--fg-3); }
.action-flash.ok { color:var(--accent); }
.action-flash.err { color:var(--red-600); }
:root[data-theme="dark"] .action-flash.err { color:var(--danger-fg); }

/* Здоровье */
.health-row { display:flex; align-items:baseline; gap:9px; padding:5px 0; font-size:var(--fs-sm);
  border-bottom:1px solid var(--divider); }
.health-row:last-child { border-bottom:0; }
.health-row .dot { align-self:center; width:8px; height:8px; }
.health-name { color:var(--fg-2); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.health-spark { font-family:var(--font-code); color:var(--fg-4); font-size:var(--fs-xs);
  letter-spacing:1px; margin-left:auto; flex-shrink:0; }
.health-count { color:var(--fg-1); font-weight:var(--fw-semibold); font-size:var(--fs-xs);
  min-width:22px; text-align:right; flex-shrink:0; }
.health-note { color:var(--warning-fg); font-size:var(--fs-2xs); flex-shrink:0; }
.health-more { color:var(--fg-3); font-size:var(--fs-xs); padding-top:8px; }

/* Details-свёртки */
details.fold { margin-top:8px; }
details.fold > summary { cursor:pointer; color:var(--fg-3); font-size:var(--fs-sm);
  font-weight:var(--fw-medium); padding:5px 0; user-select:none; list-style:none;
  display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
details.fold > summary::-webkit-details-marker { display:none; }
details.fold > summary::before { content:''; width:0; height:0; border-left:5px solid var(--fg-4);
  border-top:4px solid transparent; border-bottom:4px solid transparent; transition:transform 120ms var(--ease-out); flex-shrink:0; }
details.fold[open] > summary::before { transform:rotate(90deg); }
details.fold > summary:hover { color:var(--fg-1); }
.fold-body { padding:6px 0 2px 14px; }
.log-pre { padding:10px 12px; background:var(--bg-2); border-radius:var(--radius-md);
  font-family:var(--font-code); font-size:var(--fs-2xs); line-height:1.55; max-height:300px;
  overflow:auto; white-space:pre-wrap; word-break:break-word; color:var(--fg-2); }

/* Живой Mac-прогон */
.mac-live { margin-top:10px; padding-top:10px; border-top:1px solid var(--divider); }
.mac-live-head { display:flex; gap:10px; align-items:baseline; flex-wrap:wrap; margin-bottom:6px; }
.mac-live-title { font-weight:var(--fw-semibold); font-size:var(--fs-sm); }
.mac-live-state { font-weight:var(--fw-bold); font-size:var(--fs-sm); }
.mac-live-state.running { color:var(--warning-fg); }
.mac-live-state.done { color:var(--accent); }

/* Лог прогона: свёртка по фазам «— [N/9] …» (renderLogGroups) */
.log-groups { max-height:340px; overflow:auto; background:var(--bg-2);
  border-radius:var(--radius-md); padding:6px 10px; }
.log-groups .log-pre { max-height:none; overflow:visible; background:transparent;
  padding:2px 0 6px 14px; }
.log-groups details.fold { margin-top:2px; }
.log-groups .fold-body { padding:0; }
.log-phase-n { color:var(--fg-4); font-weight:var(--fw-bold);
  font-family:var(--font-code); font-size:var(--fs-2xs); }
.log-warn-badge { color:var(--warning-fg); font-size:var(--fs-2xs); }
.log-err-badge { color:var(--danger-fg); font-size:var(--fs-2xs); }
.log-line-warn { color:var(--warning-fg); }
.log-line-err { color:var(--danger-fg); }
.log-summary { border-top:1px dashed var(--divider); margin-top:6px; }

/* LLM */
.llm-row { display:flex; align-items:baseline; gap:10px; padding:5px 0; font-size:var(--fs-sm);
  border-bottom:1px solid var(--divider); flex-wrap:wrap; }
.llm-row:last-child { border-bottom:0; }
.llm-rank { min-width:44px; color:var(--fg-3); font-weight:var(--fw-bold); font-size:var(--fs-xs); }
.llm-id { font-family:var(--font-code); font-size:var(--fs-xs); color:var(--fg-1); word-break:break-all; }
.llm-ctx { color:var(--fg-4); font-size:var(--fs-2xs); }
.llm-updated { color:var(--fg-3); font-size:var(--fs-xs); margin-top:8px; }

.tform { margin-top:12px; padding-top:12px; border-top:1px solid var(--divider);
  display:flex; flex-direction:column; gap:10px; }
.tform-row { display:flex; gap:8px 18px; flex-wrap:wrap; align-items:center; font-size:var(--fs-sm); }
.tform-row label { display:flex; gap:7px; align-items:center; color:var(--fg-2); font-weight:var(--fw-medium); }
.tform select, .tform input[type=text] { font-family:var(--font-sans); font-size:var(--fs-sm);
  padding:6px 10px; border-radius:var(--radius); border:1px solid var(--border);
  background:var(--bg-1); color:var(--fg-1); font-weight:var(--fw-medium); }
.tform select:focus, .tform input[type=text]:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.tform input[type=text] { min-width:220px; }
.tform input[type=checkbox] { accent-color:var(--accent); width:15px; height:15px; }
.tform-hint { font-size:var(--fs-xs); color:var(--fg-3); }
.warn-mark { color:var(--amber-600); font-weight:var(--fw-bold); }

/* Поиск подписчиков */
.search-box { display:flex; align-items:center; gap:8px; padding:7px 14px; background:var(--bg-1);
  border:1px solid var(--border); border-radius:var(--radius-pill); min-width:230px; }
.search-box:focus-within { border-color:var(--accent); box-shadow:var(--focus-ring); }
.search-box svg { width:14px; height:14px; color:var(--fg-4); flex-shrink:0; }
.search-input { border:none; outline:none; background:transparent; font-size:var(--fs-sm);
  color:var(--fg-1); flex:1; font-family:var(--font-sans); font-weight:var(--fw-medium); min-width:0; }
.search-input::placeholder { color:var(--fg-4); }

/* Карточки подписчиков v2 */
.subs { display:flex; flex-direction:column; gap:10px; }
.sub-card { background:var(--bg-1); border-radius:var(--radius-lg); box-shadow:var(--shadow-1); padding:12px 16px; }
.sub-head { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.sub-name { font-size:var(--fs-lg); font-weight:var(--fw-semibold); }
.sub-name.unnamed { color:var(--fg-4); font-style:italic; font-weight:var(--fw-medium); }
.sub-actions { display:flex; gap:6px; margin-left:auto; align-items:center; flex-wrap:wrap; }
.sub-kv { display:flex; gap:4px 14px; flex-wrap:wrap; font-size:var(--fs-xs); color:var(--fg-3); margin-top:5px; }
.sub-kv b { color:var(--fg-2); font-weight:var(--fw-medium); }

.case-row { display:flex; gap:8px; align-items:baseline; padding:5px 0; border-bottom:1px solid var(--divider);
  flex-wrap:wrap; font-size:var(--fs-sm); }
.case-row:last-child { border-bottom:0; }
.case-num { font-family:var(--font-code); font-weight:var(--fw-semibold); font-size:var(--fs-xs); color:var(--fg-1); white-space:nowrap; }
.case-alias { font-family:var(--font-code); color:var(--fg-4); font-size:var(--fs-2xs);
  background:var(--bg-3); padding:1px 6px; border-radius:var(--radius-xs); white-space:nowrap; }
.case-parties { color:var(--fg-2); }
.case-court { color:var(--fg-4); font-size:var(--fs-xs); }
.push-box { margin-top:6px; padding:9px 12px; background:var(--bg-2); border-radius:var(--radius-md);
  border-left:3px solid var(--accent); font-size:var(--fs-sm); }
.push-box.skip { border-left-color:var(--gray-400); opacity:0.75; }
.push-box.general { border-left-color:var(--amber-500); }
.push-box.broadcast { border-left-color:var(--blue-500); }
.push-title { font-weight:var(--fw-semibold); }
.push-body { color:var(--fg-2); margin-top:2px; font-size:var(--fs-xs); }
.push-meta { color:var(--fg-3); font-size:var(--fs-2xs); margin-top:4px; word-break:break-all; }
.push-meta a { color:var(--accent); text-decoration:none; }
.push-meta a:hover { text-decoration:underline; }

.empty { color:var(--fg-4); font-style:italic; font-size:var(--fs-sm); padding:4px 0; }
.error { color:var(--danger-fg); padding:12px; background:var(--danger-bg); border-radius:var(--radius-md); }
.loading { color:var(--fg-3); padding:24px; text-align:center; }

/* Модалка watchlist */
dialog.wl { border:1px solid var(--border); border-radius:var(--radius-lg); background:var(--bg-1);
  color:var(--fg-1); padding:18px; width:min(560px, calc(100vw - 32px)); max-height:86vh;
  box-shadow:var(--shadow-lg); }
dialog.wl::backdrop { background:rgba(13,17,22,0.45); }
.wl-head { font-weight:var(--fw-semibold); font-size:var(--fs-lg); margin-bottom:12px; }
.wl-search { width:100%; padding:8px 12px; border-radius:var(--radius-md); border:1px solid var(--border);
  background:var(--bg-2); color:var(--fg-1); font-family:var(--font-sans); font-size:var(--fs-sm); margin-bottom:10px; }
.wl-search:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.wl-list { max-height:40vh; overflow:auto; display:flex; flex-direction:column;
  border:1px solid var(--border); border-radius:var(--radius-md); padding:4px 12px; }
.wl-row { display:flex; gap:9px; align-items:baseline; padding:7px 0; font-size:var(--fs-sm);
  border-bottom:1px solid var(--divider); cursor:pointer; flex-wrap:wrap; }
.wl-row:last-child { border-bottom:0; }
.wl-row input { flex-shrink:0; position:relative; top:2px; accent-color:var(--accent); }
.wl-parties { color:var(--fg-2); font-size:var(--fs-xs); }
.wl-manual { display:flex; gap:6px; margin-top:10px; }
.wl-manual input { flex:1; padding:7px 11px; border-radius:var(--radius); border:1px solid var(--border);
  background:var(--bg-2); color:var(--fg-1); font-family:var(--font-sans); font-size:var(--fs-sm); min-width:0; }
.wl-manual input:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.wl-foot { display:flex; gap:10px; align-items:center; justify-content:flex-end; margin-top:14px; }
.wl-count { color:var(--fg-3); font-size:var(--fs-xs); margin-right:auto; }

/* ═══ Мобильная раскладка ═══ */
@media (max-width: 768px) {
  .app-header { padding:0 14px; }
  .header-inner { row-gap:4px; padding:8px 0 10px; }
  .header-meta { display:none; }
  .btn-refresh span { display:none; }
  .btn-refresh { padding:8px 10px; }
  .header-nav { order:10; flex-basis:100%; }
  .app-main { padding:14px 14px 40px; }
  .pult { grid-template-columns:repeat(2, 1fr); gap:8px; margin-bottom:22px; }
  .stat-card { padding:10px 12px; }
  .stat-value { font-size:var(--fs-xl); }
  .system-grid { grid-template-columns:1fr; }
  .section { scroll-margin-top:104px; }
  .sub-actions { margin-left:0; flex-basis:100%; margin-top:8px; }
  .health-name { max-width:44vw; }
  .tform input[type=text] { min-width:0; flex:1; }
  .search-box { min-width:0; flex:1; }
}
@media (min-width: 769px) and (max-width: 1024px) {
  .system-grid { grid-template-columns:1fr; }
}

/* ═══ Роли: operator не видит owner-блоки (реальный запрет — 403 на API) ═══ */
html[data-role="operator"] [data-owner-only] { display:none !important; }
html[data-role="operator"] .run-ext { display:none !important; }

/* ═══ Импорт дел (капчёвые суды) ═══ */
/* Секция скрыта inline-атрибутом style (не CSS-правилом: JS показывает её
   через style.display="", что снимает именно inline-стиль). */
.imp-form { display:flex; flex-direction:column; gap:12px; }
.imp-row { display:flex; gap:8px 18px; flex-wrap:wrap; align-items:center; font-size:var(--fs-sm); }
.imp-row label { display:flex; gap:7px; align-items:center; color:var(--fg-2); font-weight:var(--fw-medium); }
.imp-row select, .imp-row input[type=text] { font-family:var(--font-sans); font-size:var(--fs-sm);
  padding:6px 10px; border-radius:var(--radius); border:1px solid var(--border);
  background:var(--bg-1); color:var(--fg-1); font-weight:var(--fw-medium); max-width:100%; }
.imp-row select:focus, .imp-row input[type=text]:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.imp-paste { min-height:110px; max-height:260px; overflow:auto; padding:10px 12px;
  border:1.5px dashed var(--border-strong); border-radius:var(--radius-md);
  background:var(--bg-2); font-size:var(--fs-xs); color:var(--fg-2); }
.imp-paste:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.imp-paste:empty::before { content:attr(data-placeholder); color:var(--fg-4); font-style:italic; }
.imp-paste table { max-width:100%; font-size:var(--fs-2xs); }
.imp-hint { font-size:var(--fs-xs); color:var(--fg-3); }
.imp-status { font-size:var(--fs-sm); }
.imp-status .badge { vertical-align:baseline; }
.imp-report { margin-top:6px; }
.imp-hist-row { display:flex; gap:8px; align-items:baseline; padding:6px 0;
  border-bottom:1px solid var(--divider); font-size:var(--fs-sm); flex-wrap:wrap; }
.imp-hist-row:last-child { border-bottom:0; }
.imp-hist-court { color:var(--fg-2); }
.imp-hist-meta { color:var(--fg-3); font-size:var(--fs-xs); }
</style>
</head><body>

<header class="app-header">
  <div class="header-inner">
    <div class="header-brand">
      <span class="header-logo">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2"/><path d="M16.24 7.76a6 6 0 0 1 0 8.49M7.76 16.24a6 6 0 0 1 0-8.49M19.07 4.93a10 10 0 0 1 0 14.14M4.93 19.07a10 10 0 0 1 0-14.14"/></svg>
      </span>
      <div>
        <div class="header-title">Админка</div>
        <div class="header-sub">мониторинг дел Сбера</div>
      </div>
    </div>
    <nav class="header-nav" id="nav">
      <a class="chip-btn active" href="#system">Система</a>
      <a class="chip-btn" href="#import" id="nav-import" style="display:none;">Импорт</a>
      <a class="chip-btn" href="#llm" data-owner-only>LLM</a>
      <a class="chip-btn" href="#subs" data-owner-only>Подписчики <span class="chip-count" id="nav-subs-count">…</span></a>
    </nav>
    <div class="header-actions">
      <div class="header-meta" id="summary" data-owner-only>…</div>
      <button class="theme-toggle" id="theme-toggle" onclick="toggleTheme()" aria-label="Тема">
        <svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
        <svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
        <svg class="icon-auto" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor" stroke="none"/></svg>
      </button>
      <button class="btn-refresh" onclick="refreshAll()" title="Обновить данные">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>
        <span>Обновить</span>
      </button>
    </div>
  </div>
</header>

<main class="app-main">

  <div class="pult">
    <button class="stat-card" data-accent="gray" data-goto="#system">
      <div class="stat-label">Последний прогон</div>
      <div class="stat-value" id="tile-run-value">…</div>
      <div class="stat-sub" id="tile-run-sub"></div>
    </button>
    <button class="stat-card" data-accent="blue" data-goto="#system">
      <div class="stat-label">Дайджест</div>
      <div class="stat-value" id="tile-digest-value">…</div>
      <div class="stat-sub" id="tile-digest-sub"></div>
    </button>
    <button class="stat-card" data-accent="gray" data-goto="#system">
      <div class="stat-label">Парсеры</div>
      <div class="stat-value" id="tile-health-value">…</div>
      <div class="stat-sub" id="tile-health-sub"></div>
    </button>
    <button class="stat-card" data-accent="gray" data-goto="#system">
      <div class="stat-label">Автозапуск</div>
      <div class="stat-value" id="tile-cron-value">…</div>
      <div class="stat-sub" id="tile-cron-sub"></div>
    </button>
  </div>

  <section class="section" id="system">
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      </span>
      <h2 class="section-title">Система</h2>
    </div>
    <div class="system-grid">
      <div class="card">
        <div class="card-head">
          <span class="card-title">Прогоны GitHub Actions</span>
          <span class="run-meta" id="runs-next"></span>
          <span class="spacer"></span>
          <button class="btn-primary" id="btn-run-main" data-owner-only>
            <svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><polygon points="6 3 20 12 6 21 6 3"/></svg>
            Полный прогон
          </button>
          <button class="btn-outline" id="btn-run-std" data-owner-only title="Как ежедневный автозапуск: smart-skip — пропуск дел с известной будущей датой и нерабочих дней">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="6 3 20 12 6 21 6 3"/></svg>
            Стандартный прогон
          </button>
          <span class="action-flash" id="runs-flash"></span>
        </div>
        <div id="runs-list" class="loading">Загрузка…</div>
        <div class="mac-live" id="mac-live" style="display:none;">
          <div class="mac-live-head">
            <span class="mac-live-title" id="mac-live-title">Прогон</span>
            <span class="mac-live-state" id="mac-live-state"></span>
            <span class="run-meta" id="mac-live-meta"></span>
            <a class="run-ext" id="mac-live-link" target="_blank" rel="noopener noreferrer" style="display:none;" title="Открыть прогон на GitHub"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></a>
          </div>
          <div class="log-groups" id="mac-live-log"></div>
          <details class="fold" id="mac-prev" style="display:none;">
            <summary id="mac-prev-sum">Предыдущий прогон</summary>
            <div class="fold-body"><div class="log-groups" id="mac-prev-log"></div></div>
          </details>
        </div>
        <details class="fold" id="mac-stale" style="display:none;">
          <summary id="mac-stale-sum">Последний прогон</summary>
          <div class="fold-body"><div class="log-groups" id="mac-stale-log"></div></div>
        </details>
      </div>
      <div class="card">
        <div class="card-head">
          <span class="card-title">Здоровье парсеров</span>
          <span class="spacer"></span>
          <span id="health-badges"></span>
        </div>
        <div id="health-list" class="loading">Загрузка…</div>
        <div class="health-more" id="health-updated"></div>
      </div>
    </div>
  </section>

  <section class="section" id="import" style="display:none;">
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
      </span>
      <h2 class="section-title">Импорт дел</h2>
      <span class="section-counter" id="imp-court-count"></span>
    </div>
    <div class="card">
      <div class="imp-form">
        <div class="imp-hint">Поиск этих судов закрыт проверочным кодом, поэтому дела заводятся вручную:
          решите код на сайте суда, найдите дела по слову «Сбербанк», <b>скопируйте выделение страницы
          результатов</b> (или сохраните её как «только HTML» и приложите файл) и вставьте ниже.
          Вставка простым текстом не годится — теряются ссылки на карточки дел.</div>
        <div class="imp-row">
          <label>Суд
            <select id="imp-court"></select>
          </label>
          <a class="chip-btn" id="imp-court-link" href="#" target="_blank" rel="noopener noreferrer">Открыть сайт суда</a>
          <label>Ваше имя
            <input type="text" id="imp-name" maxlength="60" placeholder="как вас записать в журнале">
          </label>
        </div>
        <div class="imp-paste" id="imp-paste" contenteditable="true"
          data-placeholder="Вставьте сюда скопированную страницу выдачи (Ctrl+V / ⌘V)…"></div>
        <div class="imp-row">
          <label class="imp-hint">или файл «только HTML»: <input type="file" id="imp-file" accept=".html,.htm,text/html"></label>
        </div>
        <div class="imp-row">
          <button class="btn-primary" id="imp-send">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            Отправить на импорт
          </button>
          <span class="imp-status" id="imp-status"></span>
        </div>
        <div class="imp-report" id="imp-report"></div>
      </div>
      <details class="fold" id="imp-fresh-fold">
        <summary>Свежесть по судам <span id="imp-fresh-badges"></span></summary>
        <div class="fold-body">
          <div class="imp-hint" style="margin-bottom:6px;">Регламент — импорт каждого суда раз в неделю: зелёный ≤ 7 дней, жёлтый 8–14, красный дольше или ни разу. Просроченные — сверху.</div>
          <div id="imp-freshness" class="empty">Загрузка…</div>
        </div>
      </details>
      <details class="fold" id="imp-hist-fold">
        <summary>История импортов <span class="run-meta" id="imp-hist-count"></span></summary>
        <div class="fold-body"><div id="imp-history" class="empty">Загрузка…</div></div>
      </details>
    </div>
  </section>

  <section class="section" id="llm" data-owner-only>
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3"/></svg>
      </span>
      <h2 class="section-title">LLM · тест дайджеста</h2>
    </div>
    <div class="card">
      <div class="card-head">
        <span class="card-title">Топ бесплатных моделей OpenRouter — что стоит за «топ-N»</span>
      </div>
      <div id="llm-top-body" class="loading">Загрузка…</div>
      <div class="llm-updated" id="llm-updated"></div>
      <div class="tform" id="tf">
        <div class="tform-row">
          <label>Провайдер
            <select id="tf-provider">
              <option value="claude" selected>claude</option>
              <option value="gigachat">gigachat</option>
              <option value="openrouter">openrouter</option>
            </select>
          </label>
          <label id="tf-claude-wrap">Модель
            <select id="tf-claude">
              <option value="haiku" selected>Haiku 4.5 (эталон)</option>
              <option value="sonnet">Sonnet 5</option>
              <option value="opus">Opus 4.8</option>
            </select>
          </label>
          <label id="tf-effort-wrap" style="display:none;">Усилия
            <select id="tf-effort">
              <option value="default" selected>по умолчанию (high)</option>
              <option value="low">low</option>
              <option value="medium">medium</option>
              <option value="high">high</option>
              <option value="xhigh">xhigh</option>
              <option value="max">max</option>
            </select>
          </label>
          <label id="tf-giga-wrap" style="display:none;">Модель
            <select id="tf-giga">
              <option value="GigaChat-2-Pro" selected>GigaChat-2-Pro</option>
              <option value="GigaChat-2">GigaChat-2</option>
              <option value="GigaChat-2-Max">GigaChat-2-Max</option>
              <option value="GigaChat-3-Ultra">GigaChat-3-Ultra (freemium)</option>
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
          <label id="tf-model-wrap">Точная модель <input type="text" id="tf-model" placeholder="пусто = по выбору выше"></label>
          <span class="tform-hint" id="tf-claude-note">haiku — боевой эталон дайджеста; sonnet/opus дороже, только для сравнения. Усилия (глубина размышлений) — только у sonnet/opus</span>
        </div>
        <div class="tform-row">
          <label><input type="checkbox" id="tf-to-group"> в корп. группу <span class="warn-mark">⚠︎</span></label>
          <label><input type="checkbox" id="tf-push-all"> push всем <span class="warn-mark">⚠︎</span></label>
          <label><input type="checkbox" id="tf-full-llm"> полный LLM (старый режим)</label>
          <label><input type="checkbox" id="tf-commit"> опубликовать результаты</label>
        </div>
        <div class="tform-row">
          <button class="btn-primary" id="tf-run">
            <svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><polygon points="6 3 20 12 6 21 6 3"/></svg>
            Запустить тест дайджеста
          </button>
          <span class="action-flash" id="tf-flash"></span>
        </div>
        <div class="tform-hint">Без галок безопасно: Telegram только в личный чат, без публикации на дашборд и без push. «Push всем» работает только вместе с «опубликовать».</div>
      </div>
    </div>
  </section>

  <section class="section" id="subs" data-owner-only>
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      </span>
      <h2 class="section-title">Подписчики</h2>
      <span class="section-counter" id="subs-count">…</span>
      <span class="spacer"></span>
      <div class="search-box">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
        <input class="search-input" id="subs-search" placeholder="Имя, устройство или дело…">
      </div>
    </div>
    <div id="root" class="loading">Загрузка…</div>
  </section>

</main>

<dialog class="wl" id="wl-modal" data-owner-only>
  <div class="wl-head">Watchlist: <span id="wl-who"></span></div>
  <input class="wl-search" id="wl-search" type="text" placeholder="Поиск: номер дела, сторона или суд…">
  <div class="wl-list" id="wl-list"></div>
  <div id="wl-extras"></div>
  <div class="wl-manual">
    <input type="text" id="wl-manual-input" placeholder="Добавить номер вручную (напр. 2-123/2026)">
    <button class="btn-outline" type="button" id="wl-manual-add">Добавить</button>
  </div>
  <div class="wl-foot">
    <span class="wl-count" id="wl-count"></span>
    <button class="btn-outline" type="button" id="wl-cancel">Отмена</button>
    <button class="btn-primary" type="button" id="wl-save">Сохранить</button>
  </div>
</dialog>

<script>
const SECRET = ${JSON.stringify(secret)};
// Роль страницы: "owner" | "operator". Скрытие блоков — UX; реальный запрет
// operator-роли — на эндпоинтах Worker'а (403).
const ROLE = ${JSON.stringify(role)};
const IS_OWNER = ROLE === "owner";
// URL данных территории — из wrangler.toml форка (CASES_DATA_URL), не хардкод:
// иначе админка Урала показывала бы дела и здоровье ХМАО.
const CASES_URL = ${JSON.stringify(CFG.casesUrl)};
const ARCHIVE_URL = ${JSON.stringify(CFG.archiveUrl)};
const PUSHES_URL = ${JSON.stringify(CFG.pushesUrl)};
const DIGEST_URL = ${JSON.stringify(CFG.digestUrl)};
const HEALTH_URL = ${JSON.stringify(CFG.healthUrl)};
const DASHBOARD_URL = ${JSON.stringify(CFG.dashboardUrl)};
const SITE_BASE = ${JSON.stringify(CFG.siteBase)};
const GH_REPO = ${JSON.stringify(CFG.ghRepo)};

const SVG_EXT = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>';
const SVG_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';

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
// Бейдж стадии — та же семантика, что stageBadgeHtml на дашборде (app.js):
// переходные стадии показываются как та инстанция, куда дело движется.
function stageBadge(stage) {
  if (stage === "first_instance") return '<span class="badge badge-fi">1 инст.</span>';
  if (stage === "appeal" || stage === "awaiting_appeal" || stage === "cassation_watch" || stage === "cassation_pending")
    return '<span class="badge badge-appeal">Апелляция</span>';
  if (stage === "cassation" || stage === "awaiting_relink")
    return '<span class="badge badge-cassation">Кассация</span>';
  return "";
}
// Плитка пульта: значение/подпись/акцент. valueHtml приходит из наших же
// рендеров (не из сырых данных) — вставляется как HTML.
function setTile(name, accent, valueHtml, subHtml) {
  const v = document.getElementById("tile-" + name + "-value");
  const s = document.getElementById("tile-" + name + "-sub");
  if (v) { v.innerHTML = valueHtml; v.closest(".stat-card").setAttribute("data-accent", accent); }
  if (s) s.innerHTML = subHtml || "";
}

// Состояние, разделяемое между блоками (обновляется в render()).
let casesMapGlobal = new Map();
let activeCasesGlobal = [];
let subsByEp = new Map();
let allSubs = [];
let lastPushesMap = new Map();
let lastPushesGeneratedAt = "";

// ── Секция «Система»: прогоны GitHub Actions ─────────────────────────────────
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
function runDot(run) {
  if (run.status !== "completed") return '<span class="dot dot-amber dot-pulse"></span>';
  if (run.conclusion === "success") return '<span class="dot dot-green"></span>';
  if (run.conclusion === "failure" || run.conclusion === "startup_failure"
      || run.conclusion === "timed_out") return '<span class="dot dot-red"></span>';
  return '<span class="dot dot-gray"></span>';
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
        const txt = new Date(t).toLocaleString("ru-RU",
          { weekday: "short", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
        document.getElementById("runs-next").textContent = "автозапуск: " + txt;
        setTile("cron", "gray", escHtml(txt), document.getElementById("tile-cron-sub").innerHTML);
      }
    }
    if (!r.ok) {
      const txt = String(d.error || "") + " " + String(d.detail || "");
      const hint = txt.indexOf("403") >= 0 ? " — похоже, у GITHUB_PAT нет прав actions:read" : "";
      listEl.className = "";
      listEl.innerHTML = '<div class="empty">GitHub API недоступен: '
        + escHtml(d.error || ("HTTP " + r.status)) + escHtml(hint) + '</div>';
      setTile("run", "gray", "—", "GitHub недоступен");
      return;
    }
    const runs = (d.runs || []).slice(0, 8);
    listEl.className = "";
    if (!runs.length) {
      listEl.innerHTML = '<div class="empty">Прогонов пока нет</div>';
      setTile("run", "gray", "—", "");
      return;
    }
    let hasActive = false;
    listEl.innerHTML = runs.map(function (run) {
      const active = run.status !== "completed";
      if (active) hasActive = true;
      const dur = fmtDur(run.run_started_at, active ? null : run.updated_at);
      // Оператору ссылки на GitHub-run'ы не показываем (только статусы).
      const nameHtml = IS_OWNER
        ? '<a class="run-name" href="' + escHtml(run.html_url) + '" target="_blank" rel="noopener noreferrer">'
          + escHtml(wfShortName(run)) + '</a>'
        : '<span class="run-name">' + escHtml(wfShortName(run)) + '</span>';
      return '<div class="run-row">' + runDot(run)
        + nameHtml
        + '<span class="run-meta">#' + escHtml(String(run.run_number || "?"))
        + ' · ' + escHtml(relTime(run.run_started_at))
        + (dur ? " · " + escHtml(dur) + (active ? " (идёт)" : "") : "")
        + '</span>'
        + '<a class="run-ext" href="' + escHtml(run.html_url) + '" target="_blank" rel="noopener noreferrer" title="Открыть в GitHub">' + SVG_EXT + '</a>'
        + '</div>';
    }).join("");
    // Плитка «Последний прогон» — по последнему запуску основного workflow.
    // Сервер отдаёт его отдельным полем main_run (в общем списке его могут
    // вытеснить пары «Тесты+Pages»); фолбэк — поиск по списку.
    const main = d.main_run || (d.runs || []).find(function (run) {
      return String(run.path || "").indexOf("update_cases.yml") >= 0;
    });
    if (main) {
      const active = main.status !== "completed";
      const dur = fmtDur(main.run_started_at, active ? null : main.updated_at);
      if (active) {
        setTile("run", "amber", '<span class="dot dot-amber dot-pulse"></span>идёт · ' + escHtml(dur),
          "старт " + escHtml(relTime(main.run_started_at)));
      } else if (main.conclusion === "success") {
        setTile("run", "green", '<span class="dot dot-green"></span>ok · ' + escHtml(dur),
          escHtml(relTime(main.run_started_at)) + " · #" + escHtml(String(main.run_number || "")));
      } else {
        setTile("run", "red", '<span class="dot dot-red"></span>' + escHtml(main.conclusion || "сбой"),
          escHtml(relTime(main.run_started_at)) + " · #" + escHtml(String(main.run_number || "")));
      }
    } else {
      setTile("run", "gray", "—", "основной прогон не найден");
    }
    // Пока есть живой прогон — обновляемся сами, чтобы видеть исход без F5.
    if (hasActive) ghTimer = setTimeout(loadGhRuns, 15000);
  } catch (e) {
    listEl.className = "";
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
document.getElementById("btn-run-std").addEventListener("click", function () {
  if (!confirm("Запустить стандартный прогон (как ежедневный автозапуск)?\\n\\nSmart-skip: пропуск дел с известной будущей датой и нерабочих дней РФ. В выходной/праздник прогон сразу завершится строкой «нерабочий день РФ, парсинг пропущен» — это ожидаемо.")) return;
  dispatchWorkflow("update_cases.yml", { smart_skip: "true" }, document.getElementById("runs-flash"));
});

// ── Живой лог прогона (GitHub Actions / Mac-резерв): живой крупно, старый — свёрнуто
function progressAgo(iso) {
  const t = parseIso(iso);
  if (isNaN(t)) return "";
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 90) return Math.round(s) + " сек назад";
  if (s < 5400) return Math.round(s / 60) + " мин назад";
  return new Date(t).toLocaleString("ru-RU");
}
// Свёртка лога по фазам. Маркер — строка log_phase (runs.py):
// «HH:MM:SS [INFO] — [3/9] Заголовок —». Формат — контракт: его же ловит
// Mac-пушер (KEY_RE) и фиксирует тест scripts/tests/test_gh_progress_pusher.py.
var LOG_PHASE_RE = /— \\[(\\d+)\\/(\\d+)\\] (.+?) —\\s*$/;
function splitLogPhases(lines) {
  var out = { pre: [], phases: [], summary: [] };
  var list = (lines || []).map(function (x) { return String(x); });
  // Финальную сводку (log_run_summary, рамка «====») выносим наружу —
  // итог прогона виден без разворачивания фаз.
  var si = -1;
  for (var i = 0; i < list.length; i++) {
    if (list[i].indexOf("Сводка прогона") >= 0) { si = i; break; }
  }
  if (si >= 0) {
    var start = (si > 0 && list[si - 1].indexOf("====") >= 0) ? si - 1 : si;
    out.summary = list.slice(start);
    list = list.slice(0, start);
  }
  var cur = null;
  list.forEach(function (line) {
    var m = line.match(LOG_PHASE_RE);
    if (m) {
      cur = { num: m[1], total: m[2], title: m[3], lines: [], warns: 0, errs: 0 };
      out.phases.push(cur);
      return;
    }
    if (!cur) { out.pre.push(line); return; }
    cur.lines.push(line);
    if (line.indexOf("[ERROR]") >= 0) cur.errs++;
    else if (line.indexOf("[WARNING]") >= 0) cur.warns++;
  });
  return out;
}
function logLineHtml(line) {
  var esc = escHtml(line);
  if (line.indexOf("[ERROR]") >= 0) return '<span class="log-line-err">' + esc + '</span>';
  if (line.indexOf("[WARNING]") >= 0) return '<span class="log-line-warn">' + esc + '</span>';
  return esc;
}
function renderLogGroups(el, lines, live) {
  var g = splitLogPhases(lines);
  // Открытые ВРУЧНУЮ фазы переживают 5-секундный ререндер; автооткрытая
  // последняя (data-auto) не переносится — иначе к концу прогона остались бы
  // открытыми все фазы, по которым прошёл «курсор» живого прогона.
  var openSet = {};
  el.querySelectorAll("details[data-phase]").forEach(function (d) {
    if (d.open && !d.hasAttribute("data-auto")) openSet[d.getAttribute("data-phase")] = true;
  });
  var html = "";
  if (g.pre.length) html += '<pre class="log-pre">' + g.pre.map(logLineHtml).join("\\n") + '</pre>';
  g.phases.forEach(function (ph, idx) {
    var autoOpen = live && idx === g.phases.length - 1;
    var open = openSet[ph.num] || autoOpen;
    var badges = (ph.errs ? ' <span class="log-err-badge">✖ ' + ph.errs + '</span>' : '')
      + (ph.warns ? ' <span class="log-warn-badge">⚠ ' + ph.warns + '</span>' : '');
    html += '<details class="fold" data-phase="' + escHtml(ph.num) + '"'
      + (autoOpen && !openSet[ph.num] ? ' data-auto="1"' : '') + (open ? ' open' : '')
      + '><summary><span class="log-phase-n">[' + escHtml(ph.num) + '/' + escHtml(ph.total) + ']</span> '
      + escHtml(ph.title) + ' <span class="run-meta">' + ph.lines.length + ' стр.</span>' + badges + '</summary>'
      + '<div class="fold-body"><pre class="log-pre">' + ph.lines.map(logLineHtml).join("\\n") + '</pre></div>'
      + '</details>';
  });
  if (g.summary.length) html += '<pre class="log-pre log-summary">' + g.summary.map(logLineHtml).join("\\n") + '</pre>';
  el.innerHTML = html || '<pre class="log-pre">…</pre>';
}
function progressSourceTitle(rec) {
  // Старые записи Mac-пушера поля source не имеют → ветка Mac.
  return rec && rec.source === "github" ? "Прогон (GitHub Actions)" : "Парсинг на Mac (резерв)";
}
let progressTimer = null;
var lastProgressRenderKey = "";
var lastPrevRenderKey = "";
async function loadProgress() {
  try {
    const r = await fetch("/admin/run-progress?secret=" + encodeURIComponent(SECRET));
    if (!r.ok) return;
    const d = await r.json();
    const live = document.getElementById("mac-live");
    const stale = document.getElementById("mac-stale");
    const cur = d.current;
    if (!cur) { live.style.display = "none"; stale.style.display = "none"; return; }
    const running = cur.done !== true;
    // Mac — спящий резерв: завершённый прогон старше суток не заслуживает
    // большого блока, сворачиваем в details-строку.
    const isStale = !running && (Date.now() - parseIso(cur.updated_at)) > 24 * 3600 * 1000;
    if (isStale) {
      live.style.display = "none";
      stale.style.display = "";
      document.getElementById("mac-stale-sum").textContent =
        progressSourceTitle(cur) + " — завершён " + fullDate(cur.updated_at);
      renderLogGroups(document.getElementById("mac-stale-log"), cur.lines || [], false);
      return;
    }
    stale.style.display = "none";
    live.style.display = "";
    document.getElementById("mac-live-title").textContent = progressSourceTitle(cur);
    var lk = document.getElementById("mac-live-link");
    if (cur.link) { lk.href = cur.link; lk.style.display = ""; }
    else { lk.style.display = "none"; }
    const st = document.getElementById("mac-live-state");
    st.textContent = running ? "идёт" : "завершён";
    st.className = "mac-live-state " + (running ? "running" : "done");
    document.getElementById("mac-live-meta").textContent =
      "обновлено " + progressAgo(cur.updated_at) + " · старт " + progressAgo(cur.started_at);
    const logEl = document.getElementById("mac-live-log");
    const atBottom = logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 40;
    // Ререндер только при новых строках/смене прогона: открытые details и
    // скролл не дёргаются впустую (мета «обновлено…» обновляется всегда).
    var renderKey = String(cur.run_id) + ":" + (cur.lines || []).length + ":" + running;
    if (renderKey !== lastProgressRenderKey) {
      lastProgressRenderKey = renderKey;
      renderLogGroups(logEl, cur.lines || [], running);
      if (atBottom) logEl.scrollTop = logEl.scrollHeight;
    }
    if (d.prev && Array.isArray(d.prev.lines) && d.prev.lines.length) {
      document.getElementById("mac-prev").style.display = "";
      document.getElementById("mac-prev-sum").textContent =
        "Предыдущий прогон" + (d.prev.source === "github" ? " (GitHub Actions)" : " (Mac)");
      var prevKey = String(d.prev.run_id) + ":" + d.prev.lines.length;
      if (prevKey !== lastPrevRenderKey) {
        lastPrevRenderKey = prevKey;
        renderLogGroups(document.getElementById("mac-prev-log"), d.prev.lines, false);
      }
    }
    clearTimeout(progressTimer);
    if (running) progressTimer = setTimeout(loadProgress, 5000);
  } catch (e) { /* сеть мигнула — не мешаем остальной админке */ }
}

// ── Секция «Система»: здоровье парсеров (parse_health.json) ──────────────────
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
  const last = (counts || []).slice(-10);
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
// 2 = красный, 1 = жёлтый, 0 = зелёный.
function healthLevel(s) {
  if ((s.fail_streak || 0) >= 3 || s.alerted_zero) return 2;
  if ((s.fail_streak || 0) >= 1 || ((s.zero_streak || 0) >= 1 && healthMedian(s.counts) >= 1)) return 1;
  return 0;
}
function healthNote(s) {
  const parts = [];
  if ((s.fail_streak || 0) > 0) parts.push("HTTP-фейл ×" + s.fail_streak);
  if ((s.zero_streak || 0) > 0 && healthMedian(s.counts) >= 1) parts.push("нулей подряд: " + s.zero_streak);
  return parts.join(" · ");
}
async function loadHealth() {
  const listEl = document.getElementById("health-list");
  try {
    const r = await fetch(HEALTH_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    const sources = d.sources || {};
    const items = Object.keys(sources).map(function (k) {
      const s = sources[k] || {};
      // Имя источника — из самого журнала (label пишет health.py с 15.07.2026);
      // COURT_NAMES — фолбэк для записей до появления label (удалить, когда
      // все источники обзаведутся label после пары прогонов).
      return { key: k, s: s, level: healthLevel(s), name: s.label || COURT_NAMES[k] || k };
    });
    if (!items.length) {
      listEl.className = "";
      listEl.innerHTML = '<div class="empty">Журнал пуст</div>';
      return;
    }
    // Проблемные вверх, дальше — по числу результатов (живые источники выше).
    items.sort(function (a, b) {
      if (a.level !== b.level) return b.level - a.level;
      return (b.s.last_count || 0) - (a.s.last_count || 0);
    });
    const nRed = items.filter(function (x) { return x.level === 2; }).length;
    const nYellow = items.filter(function (x) { return x.level === 1; }).length;
    const nGreen = items.length - nRed - nYellow;
    function rowHtml(x) {
      const dotCls = x.level === 2 ? "dot-red" : x.level === 1 ? "dot-amber" : "dot-green";
      const note = x.level > 0 ? healthNote(x.s) : "";
      return '<div class="health-row"><span class="dot ' + dotCls + '"></span>'
        + '<span class="health-name">' + escHtml(x.name) + '</span>'
        + (note ? '<span class="health-note">' + escHtml(note) + '</span>' : '')
        + '<span class="health-spark">' + healthSpark(x.s.counts) + '</span>'
        + '<span class="health-count">' + escHtml(String(x.s.last_count ?? "—")) + '</span>'
        + '</div>';
    }
    const VISIBLE = 8;
    const head = items.slice(0, VISIBLE).map(rowHtml).join("");
    const rest = items.slice(VISIBLE);
    const restHtml = rest.length
      ? '<details class="fold"><summary>Остальные ' + rest.length + ' источников'
        + (nRed + nYellow === 0 || rest.every(function (x) { return x.level === 0; }) ? " — все ok" : "")
        + '</summary><div class="fold-body">' + rest.map(rowHtml).join("") + '</div></details>'
      : "";
    listEl.className = "";
    listEl.innerHTML = head + restHtml;
    document.getElementById("health-badges").innerHTML =
      (nRed ? '<span class="badge badge-fail">' + nRed + ' сбой</span> ' : "")
      + (nYellow ? '<span class="badge badge-run">' + nYellow + ' ⚠︎</span> ' : "")
      + '<span class="badge badge-ok">' + nGreen + ' ok</span>';
    document.getElementById("health-updated").textContent =
      "число результатов поиска по прогонам · обновлено " + relTime(d.updated_at);
    // Плитка «Парсеры».
    if (nRed) {
      setTile("health", "red", nRed + ' <span class="warn-mark">✕</span> из ' + items.length,
        escHtml(items[0].name + (healthNote(items[0].s) ? " · " + healthNote(items[0].s) : "")));
    } else if (nYellow) {
      setTile("health", "amber", nYellow + ' <span class="warn-mark">⚠︎</span> из ' + items.length,
        escHtml(items[0].name + (healthNote(items[0].s) ? " · " + healthNote(items[0].s) : "")));
    } else {
      setTile("health", "green", '<span class="dot dot-green"></span>все ' + items.length + " ok",
        "обновлено " + escHtml(relTime(d.updated_at)));
    }
  } catch (e) {
    listEl.className = "";
    listEl.innerHTML = '<div class="empty">Не удалось загрузить parse_health.json: ' + escHtml(String(e)) + '</div>';
    setTile("health", "gray", "—", "нет данных");
  }
}

// ── Секция «LLM»: рейтинг shir-man + мини-форма теста ────────────────────────
// Рейтинг грузим сразу (секция всегда развёрнута в новой вёрстке).
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
      return '<div class="llm-row"><span class="llm-rank">топ-' + (i + 1) + '</span>'
        + '<span class="llm-id">' + escHtml(m.id || "?") + '</span>'
        + (m.contextLength ? '<span class="llm-ctx">' + Math.round(m.contextLength / 1024) + 'k контекст' + (i === 0 ? " · модель дня" : "") + '</span>' : '')
        + '</div>';
    }).join("");
    document.getElementById("llm-updated").innerHTML = 'Рейтинг shir-man обновлён '
      + (d.updatedAt ? escHtml(relTime(d.updatedAt)) : "?")
      + ' · <a href="https://github.com/' + escHtml(GH_REPO) + '/actions/workflows/test_digest.yml" target="_blank" rel="noopener noreferrer">форма в GitHub UI</a>';
    // Подписи «топ-N» в селекте обогащаем конкретными моделями (value не трогаем).
    const orSel = document.getElementById("tf-or");
    models.forEach(function (m, i) {
      if (orSel.options[i] && m.id) {
        orSel.options[i].textContent = orSel.options[i].value + " · " + m.id;
      }
    });
  } catch (e) {
    el.className = "";
    el.textContent = "Не удалось загрузить рейтинг: " + e;
    llmTopLoaded = false; // повторная попытка при следующем refreshAll
  }
}
function tfUpdateEffortVisibility() {
  // Селектор усилий (output_config.effort) есть только у моделей нового
  // поколения — sonnet/opus. Для haiku (боевой эталон) API его не принимает,
  // бэкенд значение игнорирует — прячем, чтобы не путать.
  const isClaude = document.getElementById("tf-provider").value === "claude";
  const model = document.getElementById("tf-claude").value;
  const show = isClaude && model !== "haiku";
  document.getElementById("tf-effort-wrap").style.display = show ? "" : "none";
}
document.getElementById("tf-provider").addEventListener("change", function () {
  // Каждый провайдер показывает свой список моделей; «Точную модель» (llm_model)
  // оставляем видимой для всех — она перебивает список. У Claude заметка про
  // эталон/цену показывается только в его ветке.
  const isClaude = this.value === "claude";
  document.getElementById("tf-claude-wrap").style.display = isClaude ? "" : "none";
  document.getElementById("tf-giga-wrap").style.display = this.value === "gigachat" ? "" : "none";
  document.getElementById("tf-or-wrap").style.display = this.value === "openrouter" ? "" : "none";
  document.getElementById("tf-claude-note").style.display = isClaude ? "" : "none";
  tfUpdateEffortVisibility();
});
document.getElementById("tf-claude").addEventListener("change", tfUpdateEffortVisibility);
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
  if (provider === "claude") {
    inputs.claude_model = document.getElementById("tf-claude").value;
    // Усилия шлём только для sonnet/opus и только если выбрано не «default»
    // (haiku эффорт не поддерживает; default = не отправлять параметр).
    const effort = document.getElementById("tf-effort").value;
    if (inputs.claude_model !== "haiku" && effort !== "default") {
      inputs.claude_effort = effort;
    }
  }
  if (provider === "gigachat") inputs.gigachat_model = document.getElementById("tf-giga").value;
  if (provider === "openrouter") inputs.openrouter_model = document.getElementById("tf-or").value;
  // «Точная модель» (llm_model) перебивает список любого провайдера, включая
  // Claude (точный id вроде claude-opus-4-8 пройдёт через config-резолвер).
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

// ── Данные подписок ──────────────────────────────────────────────────────────
async function fetchAll() {
  const results = await Promise.all([
    fetch("/admin/data?secret=" + encodeURIComponent(SECRET)),
    fetch(CASES_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(PUSHES_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(DIGEST_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(ARCHIVE_URL, { cache: "no-cache" }).catch(function () { return null; }),
  ]);
  const subsRes = results[0];
  if (!subsRes.ok) throw new Error("HTTP " + subsRes.status + " /admin/data");
  const subs = await subsRes.json();
  const casesMap = new Map();
  const activeCases = [];
  // Все номера дела — под один payload: канонический ID первым (он же дефолт
  // для алиаса), затем FI / апелл. / касс. (касс. бывает в двух полях —
  // case_number и cassation_number), material_number — М-предок дела (Этап 3:
  // когда юрист звёздит материал, а парсер потом промоутит его в 2-XXX, эта
  // связь сохраняется и звезда не теряется), плюс предыдущие номера из
  // hybrid-ID '2-208/2026 (2-1148/2025;)'. addAlias не перезатирает ключи,
  // поэтому порядок добавления = приоритет.
  function addCaseAliases(c, payload) {
    addAlias(casesMap, c.id, payload);
    addAlias(casesMap, c.first_instance?.case_number, payload);
    addAlias(casesMap, c.first_instance?.material_number, payload);
    addAlias(casesMap, c.appeal?.case_number, payload);
    addAlias(casesMap, c.cassation?.case_number, payload);
    addAlias(casesMap, c.cassation?.cassation_number, payload);
    for (const prev of extractParenNumbers(c.id)) {
      addAlias(casesMap, prev, payload);
    }
  }
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
          stage: payload.stage,
        });
        addCaseAliases(c, payload);
      }
    }
  } catch (e) {
    console.warn("cases.json не загружен:", e);
  }
  // Горячий архив — те же алиасы, но с пометкой archived: звезда на
  // завершённом деле показывается со сторонами и бейджем «в архиве», а не
  // «пустой» строкой (дашборд архив грузит, админка раньше — нет). Архив
  // добавляется ПОСЛЕ активных дел: при коллизии номера активное побеждает.
  try {
    const archRes = results[4];
    if (archRes && archRes.ok) {
      const archJson = await archRes.json();
      const list = Array.isArray(archJson?.cases) ? archJson.cases : [];
      for (const c of list) {
        if (!bareCaseNumber(c.id)) continue;
        addCaseAliases(c, {
          plaintiff: c.plaintiff || "",
          defendant: c.defendant || "",
          court: c.first_instance?.court || c.appeal?.court || "",
          stage: c.current_stage || "",
          canonical_id: bareCaseNumber(c.id),
          archived: true,
          archived_at: c.archived_at || "",
        });
      }
    }
  } catch (e) {
    console.warn("cases_archive.json не загружен:", e);
  }
  // Журнал последней push-рассылки: endpoint → запись.
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

// Плитка «Дайджест» + push-агрегат в плитке «Автозапуск».
function renderDigestTile(digest, pushesMap, pushesGeneratedAt) {
  if (digest && digest.generated_at) {
    const m = String(digest.summary || "").match(/\\d+/);
    const value = digest.is_empty ? "пусто" : (m ? m[0] + " изменений" : escHtml(digest.summary || "—"));
    setTile("digest", "blue", value,
      escHtml(relTime(digest.generated_at))
      + ' · <a href="' + DASHBOARD_URL + '?digest=open" target="_blank" rel="noopener noreferrer">на дашборд</a>');
  } else {
    setTile("digest", "gray", "—", "last_digest.json недоступен");
  }
  const stats = {};
  pushesMap.forEach(function (item) {
    const v = item.variant || "?";
    stats[v] = (stats[v] || 0) + 1;
  });
  const parts = ["personal", "general", "broadcast", "skip"]
    .filter(function (k) { return stats[k]; })
    .map(function (k) { return stats[k] + " " + k; });
  const sub = parts.length
    ? "push: " + parts.join(" · ") + (pushesGeneratedAt ? " (" + relTime(pushesGeneratedAt) + ")" : "")
    : "";
  document.getElementById("tile-cron-sub").textContent = sub;
}

// ── Карточки подписчиков v2 ──────────────────────────────────────────────────
function renderLastPush(item, generatedAt) {
  if (!item) {
    return generatedAt
      ? '<div class="empty">Нет записи в журнале последней рассылки (' + escHtml(relTime(generatedAt)) + ')</div>'
      : '<div class="empty">Журнал push-рассылок пока пуст</div>';
  }
  const skipped = item.variant === "skip";
  const title = skipped
    ? "Push не отправлен — нет событий по watchlist"
    : (item.title || "—");
  const body = !skipped && item.body
    ? '<div class="push-body">' + escHtml(item.body) + '</div>'
    : "";
  const click = !skipped && item.click_url
    ? '<div class="push-meta">click_url: <a href="' + escHtml(SITE_BASE)
        + escHtml(item.click_url) + '" target="_blank" rel="noopener noreferrer">'
        + escHtml(item.click_url) + '</a></div>'
    : "";
  const ts = generatedAt
    ? '<div class="push-meta">Рассылка: ' + escHtml(relTime(generatedAt)) + '</div>'
    : "";
  return '<div class="push-box ' + escHtml(item.variant || "") + '">'
    + '<div class="push-title">' + escHtml(title) + '</div>'
    + body + click + ts
    + '</div>';
}
function pushVariantBadge(item) {
  if (!item) return "";
  const v = item.variant || "?";
  const cls = v === "personal" ? "badge-ok" : v === "skip" ? "badge-skip"
    : v === "general" ? "badge-run" : "badge-watch";
  return '<span class="badge ' + cls + '">' + escHtml(v) + '</span>';
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
  return '<span class="badge badge-expiry">⏳ ' + txt + '</span>';
}
function caseRowHtml(num, casesMap) {
  const bare = bareCaseNumber(num);
  const c = casesMap.get(bare);
  if (!c) {
    // Номер-сирота: дела нет ни в активных, ни в архиве (удалено вручную или
    // переименовано до Этапа 3, когда М-алиасы ещё не сохранялись). Держать
    // его в watchlist бессмысленно — даём убрать прямо из карточки.
    return '<div class="case-row"><span class="case-num">' + escHtml(num) + '</span>'
      + '<span class="badge badge-run" title="Дело удалено или переименовано без алиаса — push по этому номеру никогда не сработает">нигде не найдено</span>'
      + '<button class="btn-icon" type="button" data-action="wldel" data-wl-num="' + escHtml(num) + '" title="Убрать номер из watchlist">✕</button>'
      + '</div>';
  }
  const parties = (c.plaintiff && c.defendant)
    ? escHtml(c.plaintiff) + ' — ' + escHtml(c.defendant)
    : escHtml(c.plaintiff || c.defendant || "");
  // Алиас-плашка: ★ стоит на номере, который отличается от канонического ID
  // дела (звезда выставлена по апел./касс./hybrid-предку).
  const aliasNote = (c.canonical_id && c.canonical_id !== bare)
    ? '<span class="case-alias">→ ' + escHtml(c.canonical_id) + '</span>'
    : '';
  const archNote = c.archived
    ? '<span class="badge badge-archive" title="Дело завершено и лежит в cases_archive.json' + (c.archived_at ? ' с ' + escHtml(c.archived_at) : '') + '. Звезда снова заработает при реактивации.">в архиве</span>'
    : '';
  return '<div class="case-row"><span class="case-num">' + escHtml(num) + '</span>'
    + aliasNote
    + stageBadge(c.stage)
    + archNote
    + '<span class="case-parties">' + parties + '</span>'
    + (c.court ? '<span class="case-court">' + escHtml(c.court) + '</span>' : '')
    + '</div>';
}
function renderCard(sub, casesMap, lastPush, pushesGeneratedAt) {
  const epAttr = escHtml(sub.endpoint || "");
  const wl = Array.isArray(sub.watchlist) ? sub.watchlist : [];
  const nameHtml = sub.label
    ? '<span class="sub-name">' + escHtml(sub.label) + '</span>'
    : '<span class="sub-name unnamed">без имени</span>';
  const cases = wl.length
    ? wl.map(function (num) { return caseRowHtml(num, casesMap); }).join("")
    : '<div class="empty">Юрист не отслеживает ни одно дело</div>';
  return '<div class="sub-card" data-endpoint="' + epAttr + '">'
    + '<div class="sub-head">'
    +   nameHtml
    +   '<span class="badge badge-device">' + escHtml(detectDevice(sub.user_agent)) + '</span>'
    +   (sub.is_owner ? '<span class="badge badge-owner">★ owner</span>' : "")
    +   expiryBadge(sub)
    +   '<div class="sub-actions">'
    +     '<button class="btn-outline" data-action="rename">✏ Имя</button>'
    +     '<button class="btn-outline" data-action="watchlist">Watchlist</button>'
    +     '<button class="btn-outline" data-action="testpush">Тест push</button>'
    +     '<button class="btn-icon" data-action="copyep" title="Копировать endpoint (…' + escHtml((sub.endpoint || "").slice(-24)) + ')">' + SVG_COPY + '</button>'
    +     '<button class="btn-outline btn-danger" data-action="delete">Удалить</button>'
    +     '<span class="action-flash"></span>'
    +   '</div>'
    + '</div>'
    + '<div class="sub-kv">'
    +   '<span>Создана <b>' + escHtml(relTime(sub.created_at)) + '</b></span>'
    +   '<span>Вход <b>' + escHtml(relTime(sub.last_seen_at)) + '</b> <span title="' + escHtml(fullDate(sub.last_seen_at)) + '"></span></span>'
    +   '<span>Watchlist <b>' + escHtml(relTime(sub.last_watchlist_update_at)) + '</b></span>'
    +   '<span>Дел: <b>' + wl.length + '</b></span>'
    + '</div>'
    + '<details class="fold">'
    +   '<summary>Последний push ' + pushVariantBadge(lastPush) + '</summary>'
    +   '<div class="fold-body">' + renderLastPush(lastPush, pushesGeneratedAt) + '</div>'
    + '</details>'
    + '<details class="fold"' + (wl.length && wl.length <= 10 ? " open" : "") + '>'
    +   '<summary>Дела (' + wl.length + ')</summary>'
    +   '<div class="fold-body">' + cases + '</div>'
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
      ? c.plaintiff + " — " + c.defendant
      : (c.plaintiff || c.defendant || "");
    return '<label class="wl-row"><input type="checkbox" data-case-id="' + escHtml(c.id) + '"'
      + (wlState.selected.has(c.id) ? " checked" : "") + '>'
      + '<span class="case-num">' + escHtml(c.id) + '</span>'
      + stageBadge(c.stage)
      + '<span class="wl-parties">' + escHtml(parties)
      + (c.court ? ' · ' + escHtml(c.court) : '') + '</span>'
      + '</label>';
  });
  // Архивные звёзды: дело уже в cases_archive.json, в списке активных его
  // нет — но галку надо показать, иначе такую звезду в модалке не видно и
  // снять её нечем (при этом в selected она не теряется и уходит при
  // сохранении как есть).
  const archRows = [];
  wlState.selected.forEach(function (id) {
    const c = casesMapGlobal.get(bareCaseNumber(id));
    if (!c || !c.archived) return;
    if (q && (id + " " + c.plaintiff + " " + c.defendant + " " + c.court).toLowerCase().indexOf(q) < 0) return;
    const parties = (c.plaintiff && c.defendant)
      ? c.plaintiff + " — " + c.defendant
      : (c.plaintiff || c.defendant || "");
    archRows.push('<label class="wl-row"><input type="checkbox" data-case-id="' + escHtml(id) + '" checked>'
      + '<span class="case-num">' + escHtml(id) + '</span>'
      + '<span class="badge badge-archive">в архиве</span>'
      + '<span class="wl-parties">' + escHtml(parties)
      + (c.court ? ' · ' + escHtml(c.court) : '') + '</span>'
      + '</label>');
  });
  document.getElementById("wl-list").innerHTML =
    rows.concat(archRows).join("") || '<div class="empty">Ничего не найдено</div>';
}
function renderWlExtras() {
  const el = document.getElementById("wl-extras");
  if (!wlState || !wlState.extras.length) { el.innerHTML = ""; return; }
  el.innerHTML = '<div class="wl-count" style="margin-top:8px;">Номера не из активных дел (уйдут как есть):</div>'
    + wlState.extras.map(function (n) {
      return '<div class="case-row"><span class="case-num">' + escHtml(n) + '</span>'
        + '<button class="btn-icon" type="button" data-extra-del="' + escHtml(n) + '" title="Убрать">✕</button></div>';
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
  btn.textContent = "Сохранить";
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
async function handleAction(card, action, currentSub, btn) {
  const endpoint = card.getAttribute("data-endpoint");
  if (!endpoint) return;
  if (action === "wldel") {
    // Убрать из watchlist номер-сироту (нет ни в активных, ни в архиве).
    const num = btn ? (btn.getAttribute("data-wl-num") || "") : "";
    if (!num) return;
    if (!confirm("Убрать «" + num + "» из watchlist? Такого дела нет ни в активных, ни в архиве — push по нему никогда не сработает.")) return;
    flash(card, "убираю…", "");
    const wl = (Array.isArray(currentSub.watchlist) ? currentSub.watchlist : [])
      .filter(function (x) { return x !== num; });
    const res = await postAdmin("/admin/watchlist", { endpoint, watchlist: wl });
    if (res.ok) { flash(card, "✓ убрано", "ok"); render(true); }
    else { flash(card, "× ошибка", "err"); }
    return;
  }
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
  } else if (action === "copyep") {
    try {
      await navigator.clipboard.writeText(endpoint);
      flash(card, "✓ endpoint скопирован", "ok");
    } catch (e) {
      flash(card, "× буфер обмена недоступен", "err");
    }
  }
}

// Поиск по подпискам: имя, устройство, номера дел и стороны из watchlist.
function subMatches(sub, q) {
  if (!q) return true;
  let hay = (sub.label || "") + " " + detectDevice(sub.user_agent) + " " + (sub.endpoint || "").slice(-32);
  for (const num of (Array.isArray(sub.watchlist) ? sub.watchlist : [])) {
    hay += " " + num;
    const c = casesMapGlobal.get(bareCaseNumber(num));
    if (c) hay += " " + c.plaintiff + " " + c.defendant + " " + c.court;
  }
  return hay.toLowerCase().indexOf(q) >= 0;
}
function renderSubsList() {
  const root = document.getElementById("root");
  const q = document.getElementById("subs-search").value.trim().toLowerCase();
  const visible = allSubs.filter(function (s) { return subMatches(s, q); });
  root.className = "subs";
  root.innerHTML = visible.map(function (s) {
    return renderCard(s, casesMapGlobal, lastPushesMap.get(s.endpoint), lastPushesGeneratedAt);
  }).join("");
  if (!visible.length) {
    root.innerHTML = '<div class="empty">' + (q ? "Ничего не найдено по запросу" : "Подписок нет.") + '</div>';
  }
  document.getElementById("subs-count").textContent =
    q ? visible.length + " из " + allSubs.length : String(allSubs.length);
}

async function render(force) {
  const root = document.getElementById("root");
  if (force && !allSubs.length) { root.className = "loading"; root.textContent = "Загрузка…"; }
  try {
    const all = await fetchAll();
    casesMapGlobal = all.casesMap;
    activeCasesGlobal = all.activeCases;
    lastPushesMap = all.pushesMap;
    lastPushesGeneratedAt = all.pushesGeneratedAt;
    renderDigestTile(all.digest, all.pushesMap, all.pushesGeneratedAt);
    const subs = all.subs;
    const owners = subs.filter((s) => s.is_owner).length;
    const totalWl = subs.reduce((a, s) => a + (s.watchlist?.length || 0), 0);
    // Сироты: номера, которых нет ни в активных делах, ни в архиве —
    // сигнал, что watchlist пора почистить (крестик в строке дела).
    let orphanWl = 0;
    for (const s of subs) {
      for (const n of (Array.isArray(s.watchlist) ? s.watchlist : [])) {
        if (!casesMapGlobal.get(bareCaseNumber(n))) orphanWl++;
      }
    }
    document.getElementById("summary").innerHTML =
      "<b>" + subs.length + "</b> подписок · <b>" + owners + "</b> owner<br>"
      + totalWl + " дел в watchlist'ах"
      + (orphanWl ? " · <b>⚠ " + orphanWl + " нигде не найдено</b>" : "");
    document.getElementById("nav-subs-count").textContent = String(subs.length);
    // Сортируем: owner вверх, затем по последнему входу (свежие первыми).
    subs.sort((a, b) => {
      if (a.is_owner !== b.is_owner) return a.is_owner ? -1 : 1;
      const ta = parseIso(a.last_seen_at) || 0;
      const tb = parseIso(b.last_seen_at) || 0;
      return tb - ta;
    });
    allSubs = subs;
    subsByEp = new Map(subs.map((s) => [s.endpoint, s]));
    renderSubsList();
  } catch (e) {
    root.className = "error";
    root.textContent = "Ошибка: " + e.message;
  }
}

// Делегированный клик по кнопкам действий — вешается ОДИН раз (иначе
// обработчики множатся после каждого обновления списка).
document.getElementById("root").addEventListener("click", (e) => {
  const btn = e.target.closest("[data-action]");
  if (!btn) return;
  const card = btn.closest(".sub-card");
  if (!card) return;
  const sub = subsByEp.get(card.getAttribute("data-endpoint"));
  if (!sub) return;
  handleAction(card, btn.getAttribute("data-action"), sub, btn);
});
document.getElementById("subs-search").addEventListener("input", renderSubsList);

// ── Чипы-якоря и плитки: прокрутка + подсветка активной секции ───────────────
(function () {
  const chips = Array.prototype.slice.call(document.querySelectorAll("#nav .chip-btn"));
  function setActive(id) {
    chips.forEach(function (c) {
      c.classList.toggle("active", c.getAttribute("href") === "#" + id);
    });
  }
  chips.forEach(function (c) {
    c.addEventListener("click", function (e) {
      e.preventDefault();
      const el = document.querySelector(c.getAttribute("href"));
      if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
      setActive(c.getAttribute("href").slice(1));
    });
  });
  const sections = Array.prototype.slice.call(document.querySelectorAll("main .section"));
  if ("IntersectionObserver" in window) {
    const visible = {};
    const io = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) { visible[en.target.id] = en.isIntersecting; });
      for (let i = 0; i < sections.length; i++) {
        if (visible[sections[i].id]) { setActive(sections[i].id); break; }
      }
    }, { rootMargin: "-90px 0px -55% 0px", threshold: 0 });
    sections.forEach(function (s) { io.observe(s); });
  }
  document.querySelectorAll(".stat-card[data-goto]").forEach(function (t) {
    t.addEventListener("click", function () {
      const el = document.querySelector(t.getAttribute("data-goto"));
      if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });
})();

// ── Секция «Импорт дел» (капчёвые суды; обе роли) ────────────────────────────
// Источник dropdown'а — region.fi_courts из cases.json (search_gated=True).
// Секция скрыта, если gated-судов в регионе нет (у ХМАО прячется сама).
var impCourts = [];            // [{name, domain, search_gated, srv_num}]
var impCourtNameByDomain = {}; // домен → короткое имя (для журнала)
var impPollTimer = null;
function impCourtLink(domain) {
  return "https://" + domain + "/modules.php?name=sud_delo";
}
async function loadImportCourts() {
  try {
    const r = await fetch(CASES_URL, { cache: "no-cache" });
    if (!r.ok) return;
    const j = await r.json();
    const fi = (j && j.region && Array.isArray(j.region.fi_courts)) ? j.region.fi_courts : [];
    const gated = fi.filter(function (c) { return c && c.search_gated && c.domain; });
    fi.forEach(function (c) {
      if (c && c.domain && !impCourtNameByDomain[c.domain]) impCourtNameByDomain[c.domain] = c.name || c.domain;
    });
    if (!gated.length) return; // регион без капчёвых судов — секция не нужна
    // Дедуп по домену: вторые площадки («сервер 2») делят домен с первой,
    // а фактический сервер дела импортёр берёт из href дампа.
    const seen = {};
    impCourts = gated.filter(function (c) {
      if (seen[c.domain]) return false;
      seen[c.domain] = true;
      return true;
    });
    const sel = document.getElementById("imp-court");
    sel.innerHTML = impCourts.map(function (c) {
      return '<option value="' + escHtml(c.domain) + '">' + escHtml(c.name) + '</option>';
    }).join("");
    document.getElementById("imp-court-count").textContent = String(impCourts.length);
    function syncLink() {
      document.getElementById("imp-court-link").href = impCourtLink(sel.value);
    }
    sel.addEventListener("change", syncLink);
    syncLink();
    document.getElementById("import").style.display = "";
    document.getElementById("nav-import").style.display = "";
    loadImportLog();
  } catch (e) { /* cases.json недоступен — секция остаётся скрытой */ }
}
function impStatusBadge(status) {
  if (status === "done") return '<span class="badge badge-ok">готово</span>';
  if (status === "failed") return '<span class="badge badge-fail">сбой</span>';
  if (status === "started") return '<span class="badge badge-run">выполняется</span>';
  return '<span class="badge badge-skip">отправлено</span>';
}
function impResultText(item) {
  if (item.status === "done") {
    var parts = ["+" + (item.added || 0) + " добавлено"];
    if (item.promoted) parts.push(item.promoted + " материалов стали делами");
    if (item.already) parts.push(item.already + " уже в базе");
    if (item.skipped_role) parts.push(item.skipped_role + " не наша роль (банк не ответчик)");
    if (item.no_link) parts.push(item.no_link + " без ссылки");
    if (item.subsidiary) parts.push(item.subsidiary + " дочки");
    return parts.join(" · ");
  }
  if (item.status === "failed") return item.error || "ошибка — детали в журнале";
  return "";
}
function renderImportHistory(items) {
  const el = document.getElementById("imp-history");
  document.getElementById("imp-hist-count").textContent = items.length ? "(" + items.length + ")" : "";
  if (!items.length) {
    el.className = "empty";
    el.textContent = "Импортов ещё не было";
    return;
  }
  el.className = "";
  el.innerHTML = items.slice(0, 20).map(function (it) {
    const court = impCourtNameByDomain[it.court_domain] || it.court_domain || "?";
    return '<div class="imp-hist-row">' + impStatusBadge(it.status)
      + '<span class="imp-hist-court"><b>' + escHtml(court) + '</b></span>'
      + '<span>' + escHtml(it.operator || "без имени") + '</span>'
      + '<span class="imp-hist-meta">' + escHtml(relTime(it.ts)) + '</span>'
      + (impResultText(it) ? '<span class="imp-hist-meta">' + escHtml(impResultText(it)) + '</span>' : '')
      + '</div>';
  }).join("");
}
async function loadImportLog() {
  try {
    const r = await fetch("/admin/import-log?secret=" + encodeURIComponent(SECRET));
    if (!r.ok) return null;
    const d = await r.json();
    const items = Array.isArray(d.items) ? d.items : [];
    renderImportHistory(items);
    renderImportFreshness(items, d.last || {});
    return items;
  } catch (e) { return null; }
}
// Светофор свежести: когда каждый капчёвый суд импортировался в последний
// раз. Основной источник — карта last (вечные ключи import:last:<домен> на
// Worker'е); журнал (последние 50) подмешивается как фолбэк для импортов,
// прошедших до появления карты. Регламент — раз в неделю.
var IMP_FRESH_WARN_DAYS = 7;
var IMP_FRESH_STALE_DAYS = 14;
function renderImportFreshness(items, lastMap) {
  var el = document.getElementById("imp-freshness");
  if (!el || !impCourts.length) return;
  var byDomain = {};
  Object.keys(lastMap || {}).forEach(function (d) {
    var e = lastMap[d];
    var t = parseIso(e && e.ts);
    if (!isNaN(t)) byDomain[d] = { ts: t, operator: e.operator || "", added: e.added || 0 };
  });
  (items || []).forEach(function (it) {
    if (it.status !== "done" || !it.court_domain) return;
    var t = parseIso(it.updated_at || it.ts);
    if (isNaN(t)) return;
    if (!byDomain[it.court_domain] || byDomain[it.court_domain].ts < t) {
      byDomain[it.court_domain] = { ts: t, operator: it.operator || "", added: it.added || 0 };
    }
  });
  var rows = impCourts.map(function (c) {
    var e = byDomain[c.domain];
    var days = e ? (Date.now() - e.ts) / 86400000 : Infinity;
    var level = days <= IMP_FRESH_WARN_DAYS ? 0 : days <= IMP_FRESH_STALE_DAYS ? 1 : 2;
    return { court: c, e: e, days: days, level: level };
  });
  // Просроченные и «ни разу» сверху, внутри уровня — самые давние первыми.
  rows.sort(function (a, b) {
    if (a.level !== b.level) return b.level - a.level;
    return b.days - a.days;
  });
  var nRed = rows.filter(function (x) { return x.level === 2; }).length;
  var nYellow = rows.filter(function (x) { return x.level === 1; }).length;
  document.getElementById("imp-fresh-badges").innerHTML =
    (nRed ? '<span class="badge badge-fail">' + nRed + ' давно/ни разу</span> ' : "")
    + (nYellow ? '<span class="badge badge-run">' + nYellow + ' ⚠︎</span> ' : "")
    + '<span class="badge badge-ok">' + (rows.length - nRed - nYellow) + ' ok</span>';
  el.className = "";
  el.innerHTML = rows.map(function (x) {
    var dotCls = x.level === 2 ? "dot-red" : x.level === 1 ? "dot-amber" : "dot-green";
    var note = x.e
      ? relTime(new Date(x.e.ts).toISOString()) + (x.e.operator ? " · " + escHtml(x.e.operator) : "")
        + (x.e.added ? " · +" + x.e.added : "")
      : "ни разу не импортировался";
    return '<div class="health-row"><span class="dot ' + dotCls + '"></span>'
      + '<span class="health-name">' + escHtml(x.court.name) + '</span>'
      + '<span class="health-spark"></span>'
      + '<span class="run-meta">' + note + '</span>'
      + '</div>';
  }).join("");
}
function impSetStatus(html) {
  document.getElementById("imp-status").innerHTML = html;
}
// Поллинг журнала по key дампа: «отправлено → выполняется → +N добавлено».
// Таймаут ~5 мин: очередь GitHub держит 1 running + 1 pending — третий запуск
// вытесняет ожидающий, дамп при этом живёт в KV 24 ч (можно повторить).
function impPollResult(key, startedAt) {
  clearTimeout(impPollTimer);
  impPollTimer = setTimeout(async function () {
    const items = await loadImportLog();
    const mine = (items || []).find(function (it) { return it.uuid === key; });
    if (mine && (mine.status === "done" || mine.status === "failed")) {
      impSetStatus(impStatusBadge(mine.status) + " " + escHtml(impResultText(mine)));
      const rep = document.getElementById("imp-report");
      if (Array.isArray(mine.lines) && mine.lines.length) {
        rep.innerHTML = '<details class="fold" open><summary>Отчёт построчно ('
          + mine.lines.length + ')</summary><div class="fold-body"><pre class="log-pre">'
          + mine.lines.map(escHtml).join("\\n") + '</pre></div></details>';
      }
      document.getElementById("imp-send").disabled = false;
      return;
    }
    if (Date.now() - startedAt > 5 * 60 * 1000) {
      impSetStatus('<span class="badge badge-fail">нет ответа ~5 мин</span> '
        + 'Прогон мог быть вытеснен очередью GitHub — повторите отправку или сообщите владельцу.');
      document.getElementById("imp-send").disabled = false;
      return;
    }
    if (mine && mine.status === "started") impSetStatus(impStatusBadge("started") + " импорт запущен…");
    impPollResult(key, startedAt);
  }, 5000);
}
async function impReadFile(file) {
  // Файл «только HTML» с sudrf — win-1251; вставки/другие файлы — utf-8.
  const buf = await file.arrayBuffer();
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(buf);
  } catch (e) {
    return new TextDecoder("windows-1251").decode(buf);
  }
}
async function impSend() {
  const domain = document.getElementById("imp-court").value;
  const name = document.getElementById("imp-name").value.trim();
  try { localStorage.setItem("admin_operator_name", name); } catch (e) {}
  const fileEl = document.getElementById("imp-file");
  let html = "";
  if (fileEl.files && fileEl.files.length) {
    html = await impReadFile(fileEl.files[0]);
  } else {
    html = document.getElementById("imp-paste").innerHTML || "";
  }
  if (!domain) { impSetStatus('<span class="badge badge-fail">выберите суд</span>'); return; }
  if (!name) { impSetStatus('<span class="badge badge-fail">укажите ваше имя</span>'); return; }
  if (html.length < 1024) {
    impSetStatus('<span class="badge badge-fail">дамп пуст или слишком короткий</span> '
      + 'Скопируйте страницу выдачи целиком или приложите файл «только HTML».');
    return;
  }
  document.getElementById("imp-send").disabled = true;
  document.getElementById("imp-report").innerHTML = "";
  impSetStatus("отправляю дамп…");
  try {
    const r = await fetch("/admin/import-dump?secret=" + encodeURIComponent(SECRET), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ court_domain: domain, operator: name, html: html }),
    });
    const d = await r.json().catch(function () { return {}; });
    if (r.ok && d.ok) {
      impSetStatus(impStatusBadge("dispatched") + " дамп принят, импорт в очереди…");
      loadImportLog();
      impPollResult(d.key, Date.now());
    } else {
      impSetStatus('<span class="badge badge-fail">✕</span> ' + escHtml(d.error || ("HTTP " + r.status)));
      document.getElementById("imp-send").disabled = false;
    }
  } catch (e) {
    impSetStatus('<span class="badge badge-fail">✕ сеть</span> ' + escHtml(String(e)));
    document.getElementById("imp-send").disabled = false;
  }
}
document.getElementById("imp-send").addEventListener("click", impSend);
try {
  document.getElementById("imp-name").value = localStorage.getItem("admin_operator_name") || "";
} catch (e) {}

// Плитка «Дайджест» для оператора: полный render() ему недоступен
// (/admin/data → 403), а last_digest.json публичный.
async function loadDigestTileLite() {
  try {
    const r = await fetch(DIGEST_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    renderDigestTile(await r.json(), new Map(), "");
  } catch (e) {
    setTile("digest", "gray", "—", "last_digest.json недоступен");
  }
}

function refreshAll() {
  loadGhRuns();
  loadHealth();
  loadProgress();
  loadImportLog();
  if (IS_OWNER) {
    render(true);
    llmTopLoaded = false;
    loadLlmTop();
  } else {
    loadDigestTileLite();
  }
}

loadProgress();
loadGhRuns();
loadHealth();
loadImportCourts();
// Owner-данные (подписки, LLM-рейтинг) оператору не грузим: эндпоинты всё
// равно ответят 403, а секции скрыты.
if (IS_OWNER) {
  loadLlmTop();
  render();
} else {
  loadDigestTileLite();
}
</script>
</body></html>`;
}
