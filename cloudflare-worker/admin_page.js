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
//
// Вкладки (02.08.2026): чипы шапки — настоящие вкладки, показана ровно одна
// секция; пульт плиток остаётся сверху вне вкладок. Раньше страница была
// лентой на 3,6–5,7 экрана, а у оператора пульт с плиткой «Импорты» начинался
// ниже первого экрана (секция импорта 788px шла перед ним). Серверная
// перестановка IMPORT_SECTION по роли за ненадобностью убрана: порядок в DOM
// больше ничего не решает, решает активный чип (оператору «Импорт» первым
// чипом = вкладка по умолчанию).
//
// Три уровня скрытия секции складываются намеренно и без !important:
//   роль    — html[data-role="operator"] [data-owner-only] {display:none!important}
//   конфиг  — инлайновый style="display:none" у #import (нет капчёвых судов)
//   вкладка — .section / .section.is-tab-active
// Инлайн бьёт классы, !important бьёт всё: неактивная роль и отсутствующий
// конфиг не показываются, даже если вкладка активна.

export function renderAdminHtml(secret, role, cfg) {
  role = role === "operator" ? "operator" : "owner";
  // Производные URL территории: приходят из worker.js (adminPageConfig()).
  // Фолбэки — боевые значения ХМАО-инстанса (деплой без [vars] работает).
  const base = (cfg && cfg.siteBase) || "https://selivanovas.github.io/dashboard";
  const CFG = {
    casesUrl: (cfg && cfg.casesUrl) || base + "/data/cases.json",
    archiveUrl: (cfg && cfg.archiveUrl) || base + "/data/cases_archive.json",
    bankUrl: (cfg && cfg.bankUrl) || base + "/data/cases_bank.json",
    bankArchiveUrl: (cfg && cfg.bankArchiveUrl) || base + "/data/cases_bank_archive.json",
    pushesUrl: (cfg && cfg.pushesUrl) || base + "/data/last_personal_pushes.json",
    digestUrl: (cfg && cfg.digestUrl) || base + "/data/last_digest.json",
    healthUrl: (cfg && cfg.healthUrl) || base + "/data/parse_health.json",
    bankParseUrl: (cfg && cfg.bankParseUrl) || base + "/data/bank_parse_report.json",
    dashboardUrl: (cfg && cfg.dashboardUrl) || base + "/sberbank_dashboard.html",
    siteBase: base,
    ghRepo: (cfg && cfg.ghRepo) || "SelivanovAS/dashboard",
  };
  const isOperator = role === "operator";
  // Чип «Импорт» в nav: оператору — первым и активным сразу (его стартовая
  // секция). С появлением точечного добавления вкладка видна ОБЕИМ ролям
  // всегда (работает и без капчёвых судов — у ХМАО их нет); дамповая часть
  // внутри прячется сама, когда gated-судов в регионе нет (loadImportCourts).
  const IMPORT_CHIP = isOperator
    ? '<a class="chip-btn active" href="#import" id="nav-import" role="tab" aria-controls="import" aria-selected="true" tabindex="0">Импорт</a>'
    : '<a class="chip-btn" href="#import" id="nav-import" role="tab" aria-controls="import" aria-selected="false" tabindex="-1">Импорт</a>';
  // ── Секция «Импорт дел» ────────────────────────────────────────────────────
  // Порядок карточек решает РОЛЬ. У оператора еженедельная работа — дампы
  // капчёвых судов, и раньше она уезжала вниз под блок точечного добавления
  // (задача эпизодическая): рабочая очередь-светофор начиналась ниже первого
  // экрана. У владельца дампов нет вовсе (.imp-form прячет loadImportCourts),
  // поэтому ему прежний порядок.
  // Имя оператора — ОДНО поле в шапке секции. Их было два (в каждой карточке)
  // с двусторонней синхронизацией; поле обязано жить ВНЕ .imp-form, иначе на
  // территории без капчёвых судов оно скрылось бы вместе с ней.
  const AC_CARD = `<div class="card" id="ac-card">
      <details class="fold ac-fold"${isOperator ? "" : " open"}>
        <summary><b>Добавить дела точечно</b> — по номеру или ссылке</summary>
        <div class="fold-body">
      <div class="imp-hint" style="margin-bottom:8px;">По одному делу в строке, до 20 за раз:
        номер дела («2-1234/2026») или ссылка на карточку дела с сайта суда.
        Для судов с проверочным кодом работает только ссылка: откройте дело в
        браузере (код решается один раз) и скопируйте адрес карточки.</div>
      <textarea id="ac-input" rows="4" spellcheck="false"
        placeholder="2-1234/2026&#10;https://…sudrf.ru/modules.php?…name_op=case…"></textarea>
      <div class="imp-selection" id="ac-check"></div>
      <div class="imp-row">
        <label>Суд для номеров
          <select id="ac-court"><option value="">определить автоматически</option></select>
        </label>
      </div>
      <div class="imp-row">
        <button class="btn-primary" id="ac-send" disabled>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          Добавить дела
        </button>
        <span class="imp-hint" id="ac-send-hint">введите номер дела или ссылку на карточку</span>
        <span class="imp-status" id="ac-status" role="status" aria-live="polite"></span>
      </div>
      <div class="imp-report" id="ac-report"></div>
        </div>
      </details>
    </div>`;
  const DUMP_CARD = `<div class="card">
      <div class="imp-alert" id="imp-alert" style="display:none;"></div>
      <!-- На широком экране форма слева, рабочая очередь справа: иначе на
           1440px операторская — одна узкая колонка, и светофор «какой суд
           пора» не виден одновременно с формой. ≤1024 — обратно в одну. -->
      <div class="imp-grid">
      <div class="imp-form">
        <div class="imp-hint">Поиск этих судов закрыт проверочным кодом, поэтому дела заводятся вручную — выдачу копирует человек.</div>
        <!-- Шесть шагов нужны на первом импорте и мешают на двадцатом:
             открыты, пока оператор ни разу не довёл импорт до «готово». -->
        <details class="fold" id="imp-steps-fold">
          <summary>Как это делается — 6 шагов</summary>
          <div class="fold-body">
        <ol class="imp-steps">
          <li>выберите суд из списка;</li>
          <li>нажмите «Открыть поиск по суду»;</li>
          <li>на сайте решите проверочный код и найдите дела по слову «Сбербанк»;</li>
          <li>выделите страницу результатов и скопируйте её;</li>
          <li>вставьте скопированное в поле ниже (Ctrl+V / ⌘V) — простой текст не годится, теряются
            ссылки на дела; вместо вставки можно приложить файл «только HTML»;</li>
          <li>нажмите «Отправить на импорт».</li>
        </ol>
          </div>
        </details>
        <div class="imp-row">
          <label>Суд
            <select id="imp-court"><option value="">загружается…</option></select>
          </label>
          <a class="chip-btn" id="imp-court-link" href="#" target="_blank" rel="noopener noreferrer">Открыть поиск по суду</a>
        </div>
        <div class="imp-paste" id="imp-paste" contenteditable="true"
          data-placeholder="Вставьте сюда скопированную страницу результатов (Ctrl+V / ⌘V) или перетащите файл «только HTML»…"></div>
        <div class="imp-selection" id="imp-selection"></div>
        <div class="imp-row">
          <input type="file" id="imp-file" class="imp-file-native" accept=".html,.htm,text/html">
          <label class="btn-outline imp-file-btn" for="imp-file">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>
            Приложить файл «только HTML»
          </label>
          <span class="imp-hint">вместо вставки</span>
        </div>
        <div class="imp-row">
          <button class="btn-primary" id="imp-send" disabled>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            Отправить на импорт
          </button>
          <span class="imp-hint" id="imp-send-hint">вставьте страницу результатов или приложите файл</span>
          <span class="imp-status" id="imp-status" role="status" aria-live="polite"></span>
        </div>
        <div class="imp-report" id="imp-report"></div>
      </div>
      <div class="imp-side">
      <details class="fold" id="imp-fresh-fold" open>
        <summary>Свежесть по судам <span id="imp-fresh-badges"></span></summary>
        <div class="fold-body">
          <div class="imp-hint" style="margin-bottom:6px;">Регламент — импорт каждого суда раз в неделю: зелёный ≤ 7 дней, жёлтый 8–14, красный дольше или ни разу. Просроченные — сверху; клик по суду выбирает его в форме.</div>
          <div class="imp-my-bar" id="imp-my-bar"></div>
          <div id="imp-freshness" class="empty">Загрузка…</div>
        </div>
      </details>
      <details class="fold" id="imp-hist-fold">
        <summary>История импортов <span class="run-meta" id="imp-hist-count"></span></summary>
        <div class="fold-body"><div id="imp-history" class="empty">Загрузка…</div></div>
      </details>
      </div>
      </div>
    </div>`;
  const IMPORT_SECTION = `<section class="section${isOperator ? " is-tab-active" : ""}" id="import" role="tabpanel" aria-labelledby="nav-import">
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
      </span>
      <h2 class="section-title">Импорт дел</h2>
      <span class="section-counter" id="imp-court-count"></span>
      <span class="spacer"></span>
      <label class="imp-who">Вы:
        <input type="text" id="imp-name" maxlength="60" placeholder="как вас записать в журнале">
      </label>
    </div>
    ${isOperator ? DUMP_CARD + AC_CARD : AC_CARD + DUMP_CARD}
  </section>`;
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

  /* ⚠️ Расхождение с дашбордом (styles.css: 1800px) ОСОЗНАННОЕ: у админки
     другая плотность. Ручной сверке подлежат палитра и типографика, не это. */
  --content-max: 1440px;
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
/* Пока «Обновить» ждёт ответы — кнопка гаснет и иконка крутится: на мобильной
   сети иначе непонятно, нажалось ли (обновление занимает секунды). */
.btn-refresh[aria-busy="true"] { opacity:0.65; cursor:default; }
.btn-refresh[aria-busy="true"] svg { animation:spin 900ms linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .btn-refresh[aria-busy="true"] svg { animation:none; } }
.btn-refresh svg { width:15px; height:15px; }

/* ═══ Контент ═══ */
.app-main { max-width:var(--content-max); margin:0 auto; padding:20px 20px 48px; }

/* Пульт */
.pult { display:grid; grid-template-columns:repeat(4, 1fr); gap:12px; margin-bottom:26px; }
.pult.has-import { grid-template-columns:repeat(5, 1fr); } /* с плиткой «Импорты» (капчёвые суды) */
/* У оператора плиток три (дайджест и автозапуск — owner-only). Строго внутри
   min-width-медиа: специфичность html[data-role] .pult выше мобильного
   .pult, и голое правило перебило бы двухколоночный телефон. */
@media (min-width: 769px) {
  html[data-role="operator"] .pult,
  html[data-role="operator"] .pult.has-import { grid-template-columns:repeat(3, 1fr); }
}
.stat-card { background:var(--bg-1); border-radius:var(--radius-md); padding:12px 14px;
  box-shadow:var(--shadow-1); border-left:3px solid var(--border-strong);
  transition:box-shadow 150ms var(--ease-out); cursor:default; text-align:left;
  border-top:0; border-right:0; border-bottom:0; font-family:var(--font-sans);
  display:block; width:100%; min-width:0; }
/* Рука и ховер — только у плиток, которые куда-то ведут: у оператора «Последний
   прогон» лишается data-href (на GitHub его не пустят) и не должен обещать
   переход. Селектор общий с делегированием кликов ниже. */
.stat-card[data-goto], .stat-card[data-href] { cursor:pointer; }
.stat-card[data-goto]:hover, .stat-card[data-href]:hover { box-shadow:var(--shadow-md); }
.stat-card[data-accent="green"] { border-left-color:var(--accent); }
.stat-card[data-accent="red"]   { border-left-color:var(--red-500); }
.stat-card[data-accent="amber"] { border-left-color:var(--amber-500); }
.stat-card[data-accent="blue"]  { border-left-color:var(--blue-500); }
.stat-card[data-accent="gray"]  { border-left-color:var(--border-strong); }
.stat-label { font-size:var(--fs-2xs); color:var(--fg-3); font-weight:var(--fw-semibold);
  text-transform:uppercase; letter-spacing:0.05em; }
/* max-height — жёсткий потолок ряда пульта: длинная сводка дайджеста тянула
   ВЕСЬ ряд по самой высокой плитке (9 строк, инцидент 13.08.2026). 3.4em — это
   ДВА ряда .tile-part с запасом: на телефоне три числа переносятся, а ряд там
   ≈1.55em (inline-flex .tile-part с baseline-выравниванием растит строчный
   бокс — по line-height:1.15 не считать, подрежет). Штатные значения целы,
   третий ряд уже не поместится; текстовый фолбэк клампится сам, ниже. */
.stat-value { font-size:var(--fs-2xl); font-weight:var(--fw-bold); letter-spacing:-0.02em;
  line-height:1.15; color:var(--fg-1); margin-top:4px; display:flex; align-items:center; gap:8px; flex-wrap:wrap;
  max-height:3.4em; overflow:hidden; }
.stat-sub { font-size:var(--fs-xs); color:var(--fg-3); margin-top:3px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
/* Составное значение плитки: крупное число + мелкая подпись единицы рядом
   («4 новых · 6 изм.»). Даёт честные ДВА числа дайджеста в ширине плитки —
   раньше показывалось первое число под чужой подписью. */
.stat-value .tile-part { display:inline-flex; align-items:baseline; gap:4px; }
.stat-value .tile-part i { font-style:normal; font-size:var(--fs-xs); font-weight:var(--fw-semibold);
  color:var(--fg-3); letter-spacing:0; }
/* Текстовый фолбэк значения: сводку, которую не удалось разобрать на числа,
   печатаем мелко и в две строки с многоточием (полная — в title). Замер
   строк требует -webkit-box, поэтому это ОТДЕЛЬНЫЙ элемент внутри flex'а
   .stat-value, а не сам .stat-value; min-width:0 — чтобы flex его сжал. */
.stat-value .tile-text { flex:1 1 auto; min-width:0;
  font-size:var(--fs-sm); font-weight:var(--fw-semibold); line-height:1.3; letter-spacing:0;
  display:-webkit-box; -webkit-box-orient:vertical; -webkit-line-clamp:2; overflow:hidden; }

/* Секции */
/* Вкладки: показана ровно одна панель. Скрытие — классом, а НЕ инлайном:
   инлайновый display:none у #import — отдельный механизм («в регионе нет
   капчёвых судов»), он бьёт классы и перебивать его нельзя. */
.section { display:none; }
.section.is-tab-active { display:block; }
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

/* Полоса запуска — во всю ширину над сеткой (не карточка в сетке). */
.run-bar { margin-bottom:14px; }

/* Потолок длины строки прозы. На 1440 карточка одна на всю ширину, и
   инструкция/подписи растягивались бы на ~200 символов в строке. В ch, а не
   в px: держится за размер шрифта. */
.run-launch-note, .imp-hint, .imp-steps, .tform-hint, .health-more,
.push-body, .load-error { max-width:78ch; }
/* Ряды «имя слева — мета справа»: на всю ширину они разъезжаются по краям. */
#imp-freshness .health-row, #imp-history .imp-hist-row { max-width:1000px; }

/* Карточки данных. Число видимых — переменная: у оператора одна («Здоровье»),
   у владельца одна или две («Иски банка» скрыта на 404 — на Урале bank-трека
   нет). Фиксированные колонки при любом выборе дают дыру, поэтому auto-fit:
   пустые треки схлопываются, две карточки занимают ширину двух, а не «двух
   из трёх». Единственный auto-fit в проекте — оправдан именно этим. */
.system-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr));
  gap:14px; align-items:start; }
/* min-width:0 — иначе длинное имя суда растягивает трек. max-width — потолок
   ОБЩИЙ, не операторский: одиночная карточка бывает и у владельца, а
   :only-child не сработает — скрытые display:none соседи остаются в DOM.
   Двухколоночный случай потолок не трогает (~693px на 1440). */
.system-grid > .card { min-width:0; max-width:700px; }

/* Кнопки */
.btn-primary { display:inline-flex; align-items:center; gap:7px; padding:8px 16px; background:var(--accent);
  color:#fff; border:none; border-radius:var(--radius); font-size:var(--fs-sm); font-weight:var(--fw-semibold);
  cursor:pointer; font-family:var(--font-sans); transition:background 120ms var(--ease-out);
  white-space:nowrap; }
.btn-primary:hover { background:var(--accent-hover); }
.btn-primary:active { background:var(--accent-active); transform:scale(0.98); }
/* Неактивная кнопка должна выглядеть неактивной. Полупрозрачный зелёный
   («Отправить на импорт» до вставки страницы) читался как рабочий. */
.btn-primary:disabled { background:var(--bg-4); color:var(--fg-4); opacity:1; cursor:not-allowed; }
.btn-primary:disabled:hover { background:var(--bg-4); }
.btn-primary svg { width:13px; height:13px; }
.btn-outline { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; background:var(--bg-1);
  border:1px solid var(--border); border-radius:var(--radius); font-size:var(--fs-sm); cursor:pointer;
  color:var(--fg-1); font-weight:var(--fw-semibold); font-family:var(--font-sans); transition:all 120ms var(--ease-out);
  white-space:nowrap; }
.btn-outline:hover { border-color:var(--border-strong); background:var(--bg-3); }
.btn-outline:disabled { opacity:0.6; cursor:default; }
/* Размер иконки задаём явно: в inline-flex без него SVG растягивается по
   высоте строки контейнера (скрепка «Приложить файл» занимала пол-экрана). */
.btn-outline svg { width:14px; height:14px; flex-shrink:0; }
.btn-outline.btn-danger { color:var(--danger-fg); }
.btn-outline.btn-danger:hover { border-color:var(--red-500); background:var(--danger-bg); }
.btn-icon { display:inline-flex; align-items:center; justify-content:center; width:30px; height:30px;
  padding:0; border:1px solid var(--border); background:var(--bg-1); color:var(--fg-3);
  border-radius:var(--radius); cursor:pointer; transition:all 120ms var(--ease-out); }
.btn-icon:hover { background:var(--bg-3); color:var(--fg-1); border-color:var(--border-strong); }
.btn-icon svg { display:block; width:14px; height:14px; }

/* Бейджи */
.badge { display:inline-flex; align-items:center; gap:4px; padding:3px 9px; border-radius:var(--radius-pill);
  font-size:var(--fs-2xs); font-weight:var(--fw-semibold); white-space:nowrap; line-height:1.3; }
/* Владелец устройства — статус, а не тревога: янтарь оставлен тому, что
   требует действия («нигде не найдено», «истекает», «N парсеров ⚠»). Раньше
   ★ OWNER и «нигде не найдено» стояли рядом в одной карточке одним цветом. */
.badge-owner { background:var(--accent-bg-strong); color:var(--accent-active); font-weight:var(--fw-bold);
  letter-spacing:0.03em; text-transform:uppercase; }
:root[data-theme="dark"] .badge-owner { color:var(--accent); }
.badge-expiry { background:var(--warning-bg); color:var(--warning-fg); }
.badge-device { background:var(--bg-3); color:var(--fg-2); font-weight:var(--fw-medium); }
/* Профиль синхронизации звёзд: устройства одного юриста с общим watchlist. */
.badge-profile { background:var(--bg-3); color:var(--fg-2); font-family:var(--font-mono); }
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

/* Запуск прогонов */
.run-meta { color:var(--fg-3); font-size:var(--fs-xs); }
.action-flash { font-size:var(--fs-2xs); color:var(--fg-3); }
.action-flash.ok { color:var(--accent); }
.action-flash.err { color:var(--red-600); }
/* Крестик у вспышки-ошибки: сообщения об ошибке НЕ гасятся по таймеру
   (код ошибки нужен, чтобы повторить или показать), закрывает их юрист. */
/* vertical-align:-2px — замерено в браузере: кнопка встаёт ровно по центру
   строки сообщения (её собственная высота входит в строчный бокс, поэтому
   поправка не 1:1 и «на глаз» не подбирается). */
.flash-x { display:inline-flex; align-items:center; justify-content:center; width:18px; height:18px;
  vertical-align:-2px; border:0; background:none; color:inherit; cursor:pointer; padding:0; opacity:0.7; }
.flash-x svg { display:block; width:11px; height:11px; }
.flash-x:hover { opacity:1; }
:root[data-theme="dark"] .action-flash.err { color:var(--danger-fg); }

/* Здоровье */
.health-row { display:flex; align-items:baseline; gap:9px; padding:5px 0; font-size:var(--fs-sm);
  border-bottom:1px solid var(--divider); }
.health-row:last-child { border-bottom:0; }
.health-row .dot { align-self:center; width:8px; height:8px; }
.health-name { color:var(--fg-2); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
/* Мини-график: 10 столбиков фиксированной сетки, выравнены по низу. */
.health-spark { margin-left:auto; flex-shrink:0; display:inline-flex; align-items:flex-end;
  gap:2px; height:14px; align-self:center; }
.health-spark .hb { display:block; width:4px; min-height:2px; border-radius:1px;
  background:var(--fg-4); opacity:0.55; }
/* Ноль — не «самый низкий столбик», а отдельное состояние: раньше его было
   не отличить от «просто меньше остальных». */
.health-spark .hb-zero { height:100%; width:2px; background:var(--red-500); opacity:0.9; }
.health-count { color:var(--fg-1); font-weight:var(--fw-semibold); font-size:var(--fs-xs);
  min-width:22px; text-align:right; flex-shrink:0; }
.health-note { color:var(--warning-fg); font-size:var(--fs-2xs); flex-shrink:0; }
.health-more { color:var(--fg-3); font-size:var(--fs-xs); padding-top:8px; }

/* Закрытие дел, по которым ИЛ не нужен. Основной путь — ручная форма
   «суд + номер»; список ниже содержит только редкие подсказки суда, а не
   сотни строк всей очереди ожидания. */
.ww-row { display:flex; align-items:baseline; gap:8px; padding:5px 0;
          border-bottom:1px solid var(--border); font-size:var(--fs-sm); }
.ww-row:last-child { border-bottom:0; }
.ww-row.is-picked { background:var(--amber-50, rgba(245,158,11,.10)); }
/* Номер не сжимается (по нему юрист узнаёт дело), суд отдаёт место первым —
   ellipsis, как в .health-name. Срок и кнопка прижаты вправо. */
.ww-num { font-weight:var(--fw-semibold); flex-shrink:0; max-width:46%;
          overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ww-court { color:var(--fg-2); flex:1; min-width:0; overflow:hidden;
            text-overflow:ellipsis; white-space:nowrap; }
.ww-days { color:var(--fg-3); font-variant-numeric:tabular-nums; flex-shrink:0;
           font-size:var(--fs-xs); }
.ww-hint { color:var(--warning-fg); cursor:help; flex-shrink:0; }
.ww-hint-badge { color:var(--warning-fg); font-size:var(--fs-2xs); }
.ww-mark { flex-shrink:0; width:24px; height:24px; line-height:1; padding:0;
           border:1px solid var(--border); border-radius:var(--radius-md);
           background:var(--bg-1); color:var(--fg-2); cursor:pointer;
           font-size:var(--fs-sm); align-self:center; }
.ww-mark:hover { border-color:var(--accent); color:var(--accent); }
.ww-row.is-picked .ww-mark { border-color:var(--accent); color:var(--accent);
                             font-weight:var(--fw-semibold); }
.ww-row.is-waived .ww-court { color:var(--fg-3); }
.ww-actions { display:flex; align-items:center; gap:10px; padding-top:10px;
              flex-wrap:wrap; }
.ww-manual { padding:10px 0 12px; border-bottom:1px solid var(--divider); }
.ww-manual-grid { display:grid; grid-template-columns:minmax(190px,1.5fr)
                  minmax(130px,.9fr) minmax(180px,1.15fr) minmax(145px,.9fr);
                  gap:8px; margin-top:9px; align-items:end; }
.ww-manual-grid label { display:flex; flex-direction:column; gap:4px;
                        min-width:0; color:var(--fg-3); font-size:var(--fs-xs); }
.ww-manual-grid input, .ww-manual-grid select { width:100%; min-width:0; }
.ww-hints-title { color:var(--fg-2); font-size:var(--fs-sm);
                  font-weight:var(--fw-semibold); padding-top:11px; }
.ww-modal-case { color:var(--fg-2); font-size:var(--fs-sm); margin-bottom:10px; }
.ww-opt { display:block; padding:7px 0; font-size:var(--fs-sm); cursor:pointer; }
.ww-opt input { margin-right:8px; }
.ww-dlg { width:min(420px, calc(100vw - 32px)); }
@media (max-width:900px) {
  .ww-manual-grid { grid-template-columns:1fr 1fr; }
}
@media (max-width:560px) {
  .ww-manual-grid { grid-template-columns:1fr; }
  .ww-manual .btn-primary { width:100%; }
}

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

/* Карточка «Запуск прогона» и отчёт парсинга исков банка */
.run-launch-note { color:var(--fg-3); font-size:var(--fs-xs); }
.bp-row { display:flex; align-items:baseline; gap:8px; padding:5px 0;
  border-bottom:1px solid var(--divider); font-size:var(--fs-sm); flex-wrap:wrap; }
.bp-row:last-child { border-bottom:0; }
.bp-row .dot { align-self:center; flex-shrink:0; }
.bp-num { font-weight:var(--fw-semibold); font-family:var(--font-code);
  font-size:var(--fs-xs); flex-shrink:0; }
.bp-court { color:var(--fg-3); font-size:var(--fs-2xs); flex-shrink:0; }
.bp-why { color:var(--fg-2); font-size:var(--fs-2xs); }
.bp-meta { color:var(--fg-4); font-size:var(--fs-2xs); margin-left:auto; flex-shrink:0; }
.bp-group > summary .bp-group-n { color:var(--fg-4); font-size:var(--fs-2xs); }
.bp-more { margin-top:6px; }

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
/* Потолок ширины: на 1440 имя слева и «N дел» справа иначе разъезжаются по
   краям через ~1000px пустоты. */
.sub-card { background:var(--bg-1); border-radius:var(--radius-lg); box-shadow:var(--shadow-1);
  padding:12px 16px; max-width:1000px; }
/* Группа устройств одного профиля синхронизации: рамка вокруг карточек. */
.profile-group { border:1px dashed var(--border-strong); border-radius:var(--radius-lg);
  padding:10px 12px; max-width:1024px; display:flex; flex-direction:column; gap:10px; }
.profile-group-head { font-size:var(--fs-sm); color:var(--fg-2); font-weight:var(--fw-semibold); }
.profile-group-orphan .profile-group-head { color:var(--fg-3); font-weight:var(--fw-medium); }
/* Профиль без единого push-устройства — свёртка с составом набора. display:block
   перебивает flex базового .profile-group: у <details> флексовая раскладка
   ломает пару summary/тело. Треугольник и правила summary — как у .sub-card. */
details.profile-group-orphan { display:block; }
.profile-group-orphan > summary { display:flex; align-items:center; gap:8px; flex-wrap:wrap;
  cursor:pointer; list-style:none; user-select:none; }
.profile-group-orphan > summary::-webkit-details-marker { display:none; }
.profile-group-orphan > summary::before { content:''; width:0; height:0; border-left:5px solid var(--fg-4);
  border-top:4px solid transparent; border-bottom:4px solid transparent;
  transition:transform var(--dur-fast) var(--ease-out); flex-shrink:0; }
.profile-group-orphan[open] > summary::before { transform:rotate(90deg); }
.profile-group-orphan > summary:focus-visible { outline:2px solid var(--accent); outline-offset:2px;
  border-radius:var(--radius); }
.profile-group-orphan > summary .spacer { flex:1; }
.profile-body { padding-top:10px; margin-top:10px; border-top:1px solid var(--divider); }
.profile-why { color:var(--fg-3); font-size:var(--fs-xs); line-height:1.5; margin-bottom:8px; }
.profile-guess { color:var(--fg-2); font-size:var(--fs-xs); margin-bottom:8px; }
/* Свёрнутая строка подписки. Кнопок в summary НЕТ намеренно: клик по
   вложенной кнопке переключал бы свёртку. Треугольник — тот же приём, что у
   details.fold, но своим правилом (там селектор по прямому потомку .fold). */
.sub-card > summary { display:flex; align-items:center; gap:8px; flex-wrap:wrap;
  cursor:pointer; list-style:none; user-select:none; }
.sub-card > summary::-webkit-details-marker { display:none; }
.sub-card > summary::before { content:''; width:0; height:0; border-left:5px solid var(--fg-4);
  border-top:4px solid transparent; border-bottom:4px solid transparent;
  transition:transform var(--dur-fast) var(--ease-out); flex-shrink:0; }
.sub-card[open] > summary::before { transform:rotate(90deg); }
.sub-card > summary:focus-visible { outline:2px solid var(--accent); outline-offset:2px;
  border-radius:var(--radius); }
.sub-card[open] > summary { padding-bottom:10px; margin-bottom:10px;
  border-bottom:1px solid var(--divider); }
/* .spacer объявлен только внутри .section-head/.card-head — строке нужен свой. */
.sub-row .spacer { flex:1; }
.sub-count { color:var(--fg-3); font-size:var(--fs-xs); white-space:nowrap; }
.sub-name { font-size:var(--fs-md); font-weight:var(--fw-semibold); }
.sub-name.unnamed { color:var(--fg-4); font-style:italic; font-weight:var(--fw-medium); }
/* Кнопки теперь своей строкой в теле карточки — прижимать вправо не к чему. */
.sub-actions { display:flex; gap:6px; align-items:center; flex-wrap:wrap; }
/* Водораздел перед удалением: разделяет рабочие действия и необратимое. */
.sub-actions-sep { width:1px; align-self:stretch; background:var(--divider); margin:0 2px; }
.btn-icon-danger:hover { color:var(--danger-fg); border-color:var(--red-500); background:var(--danger-bg); }
.sub-kv { display:flex; gap:4px 14px; flex-wrap:wrap; font-size:var(--fs-xs); color:var(--fg-3); margin-top:5px; }
.sub-kv b { color:var(--fg-2); font-weight:var(--fw-medium); }

.case-row { display:flex; gap:8px; align-items:baseline; padding:5px 0; border-bottom:1px solid var(--divider);
  flex-wrap:wrap; font-size:var(--fs-sm); }
.case-row:last-child { border-bottom:0; }
/* Строка выровнена по базовой линии — это нужно тексту и бейджам, но
   кнопку 30×30 такой baseline уводит вниз целиком, мимо номера дела. */
.case-row .btn-icon { align-self:center; }
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
/* Единый блок «не загрузилось»: человеческий текст + «Повторить». Сырое
   исключение уходит в title и console — юристу оно ничего не говорит, а
   раньше печаталось прямо в карточку («TypeError: Failed to fetch»). */
.load-error { display:flex; align-items:center; gap:10px; flex-wrap:wrap; padding:6px 0;
  color:var(--fg-2); font-size:var(--fs-sm); }
.load-error .dot { flex-shrink:0; }
.btn-sm { padding:4px 10px; font-size:var(--fs-xs); }
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
  .pult.has-import { grid-template-columns:repeat(2, 1fr); } /* специфичность: иначе десктопные 5 колонок победят */
  /* У оператора плиток три — «Импорты» оставалась бы половинной с пустотой
     справа. Это его главный показатель: растягиваем на всю ширину. */
  html[data-role="operator"] .pult.has-import #tile-import-card { grid-column:1 / -1; }
  .stat-card { padding:10px 12px; }
  .stat-value { font-size:var(--fs-xl); }
  /* Подпись плитки переносим вместо обрезки: плитки идут в две колонки, и
     nowrap+ellipsis резал её на полуслове («из 6 судов · регламент раз в н…»,
     «push: 3 personal · 5 gener…»). Вторая строка сетку не ломает. */
  /* Перенос на телефоне осознанный (подписи плиток длиннее ширины колонки),
     но с потолком: причина отказа карточек — свободная фраза из
     fetch_fail_reason_ru, и на четыре строки она растягивала весь ряд пульта.
     Полный текст остаётся в title плитки. */
  .stat-sub { white-space:normal; overflow:hidden; text-overflow:clip;
    display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; }
  /* auto-fit сам даёт одну колонку ниже ~690px, но между 690 и 768 дал бы
     две — правило гарантирует уже проверенную мобильную раскладку. */
  .system-grid { grid-template-columns:1fr; }
  .system-grid > .card { max-width:none; }
  /* Ряд действий должен умещаться в одну строку: иначе корзина переносится
     отдельной строкой к левому краю — прямо под палец. */
  .sub-actions .btn-outline { padding:6px 9px; }
  .sub-actions .btn-icon { width:28px; height:28px; }
  /* Имя суда целиком, причина сбоя — своей строкой. Тот же приём, что уже
     применён к светофору импорта ниже: max-width:44vw обрезал названия на
     полуслове («Октябрьский районный с…»), а health-note с flex-shrink:0
     уезжал за правый край вместе с причиной сбоя — самым нужным текстом. */
  .health-row { flex-wrap:wrap; }
  /* flex-basis:0, а не auto: с auto длинное имя («Железнодорожный районный
     суд г. Екатеринбурга») не влезало рядом с точкой и уезжало на свою
     строку целиком, оставляя точку одну. */
  .health-name { max-width:none; white-space:normal; flex:1 1 0; min-width:0; }
  .health-note { flex-basis:100%; margin-left:17px; flex-shrink:1; }
  .health-spark { display:none; }
  .tform input[type=text] { min-width:0; flex:1; }
  .search-box { min-width:0; flex:1; }
  #imp-freshness .health-name { max-width:none; white-space:normal; }
  #imp-freshness .imp-fresh-meta { flex-basis:100%; margin-left:17px; } /* мета под именем, отступ = точка+gap */
}
@media (min-width: 769px) and (max-width: 1024px) {
  /* .system-grid тут больше не форсим в одну колонку: именно это правило
     делало страницу на 1000px ДЛИННЕЕ, чем на 1280 (сетка схлопывалась, а
     пульт оставался четырёхколоночным). auto-fit сам даёт две колонки. */
  .pult.has-import { grid-template-columns:repeat(3, 1fr); }
}

/* ═══ Роли: operator не видит owner-блоки (реальный запрет — 403 на API) ═══ */
html[data-role="operator"] [data-owner-only] { display:none !important; }

/* ═══ Импорт дел (капчёвые суды) ═══ */
/* Секция скрыта inline-атрибутом style (не CSS-правилом: JS показывает её
   через style.display="", что снимает именно inline-стиль). */
.imp-form { display:flex; flex-direction:column; gap:12px; }
.imp-row { display:flex; gap:8px 18px; flex-wrap:wrap; align-items:center; font-size:var(--fs-sm); }
/* min-width:0 обязателен: без него label-флекс принимает ширину самой длинной
   опции реестра судов («Верх-Исетский районный суд г. Екатеринбурга») и на
   узком экране уезжает за правый край карточки. */
.imp-row label { display:flex; gap:7px; align-items:center; color:var(--fg-2); font-weight:var(--fw-medium);
  min-width:0; max-width:100%; }
.imp-row select, .imp-row input[type=text] { font-family:var(--font-sans); font-size:var(--fs-sm);
  padding:6px 10px; border-radius:var(--radius); border:1px solid var(--border);
  background:var(--bg-1); color:var(--fg-1); font-weight:var(--fw-medium);
  min-width:0; max-width:100%; }
/* Файловый инпут прячем: он единственный нестилизованный контрол страницы и
   дублирует чип выбранного файла в индикаторе. Роль кнопки играет <label>. */
.imp-file-native { position:absolute; width:1px; height:1px; padding:0; margin:-1px;
  overflow:hidden; clip:rect(0 0 0 0); white-space:nowrap; border:0; }
.imp-file-native:focus-visible + .imp-file-btn { box-shadow:var(--focus-ring); }
.imp-file-btn { cursor:pointer; }
.imp-row select:focus, .imp-row input[type=text]:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.imp-paste { min-height:110px; max-height:260px; overflow:auto; padding:10px 12px;
  border:1.5px dashed var(--border-strong); border-radius:var(--radius-md);
  background:var(--bg-2); font-size:var(--fs-xs); color:var(--fg-2); }
.imp-paste:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
.imp-paste:empty::before { content:attr(data-placeholder); color:var(--fg-4); font-style:italic; }
.imp-paste table { max-width:100%; font-size:var(--fs-2xs); }
/* Точечное добавление: многострочное поле — тот же язык, что .imp-paste. */
#ac-input { width:100%; min-height:84px; max-height:220px; resize:vertical;
  padding:10px 12px; border:1.5px dashed var(--border-strong);
  border-radius:var(--radius-md); background:var(--bg-2);
  font:inherit; font-size:var(--fs-xs); color:var(--fg-1);
  margin-bottom:6px; box-sizing:border-box; }
#ac-input:focus { outline:none; border-color:var(--accent); box-shadow:var(--focus-ring); }
#ac-card .imp-row { margin-top:8px; }
.ac-err { color:var(--danger-fg, #c0392b); }
.imp-hint { font-size:var(--fs-xs); color:var(--fg-3); }
/* Статус занимает свою строку под кнопкой: в него приезжает многострочная
   сводка (вердикт + три группы), а в одном ряду с кнопкой и подсказкой она
   расталкивала бы раскладку. :empty — чтобы пустой статус не давал пустую
   строку во flex-ряду (тот же приём, что у .imp-selection). */
.imp-status { font-size:var(--fs-sm); flex-basis:100%; }
.imp-status:empty { display:none; }
.imp-status .badge { vertical-align:baseline; }
/* Вердикт — первое (а часто и единственное), что читает оператор после
   отправки: получилось / переделывать / пусто. Цвета — токенами: литеральный
   hex в правиле тёмного варианта не получает никогда. */
.imp-verdict { display:block; margin-top:6px; font-size:var(--fs-md);
  font-weight:var(--fw-semibold); line-height:1.35; }
.imp-verdict.is-ok { color:var(--success-fg); }
.imp-verdict.is-bad { color:var(--danger-fg); }
.imp-verdict.is-none { color:var(--fg-3); }
.imp-sum-line { margin-top:3px; font-size:var(--fs-xs); color:var(--fg-2);
  line-height:1.45; }
.imp-sum-line b { font-weight:var(--fw-semibold); color:var(--fg-3); }
.imp-sum-bad { color:var(--danger-fg); }
.imp-sum-bad b { color:var(--danger-fg); }
.imp-sum-dim { color:var(--fg-4); }
.imp-report { margin-top:6px; }
.imp-hist-item { border-bottom:1px solid var(--divider); }
.imp-hist-item:last-child { border-bottom:0; }
.imp-hist-item > details.fold { margin:0 0 6px; }
.imp-hist-row { display:flex; gap:8px; align-items:baseline; padding:6px 0;
  font-size:var(--fs-sm); flex-wrap:wrap; }
.imp-hist-court { color:var(--fg-2); }
.imp-hist-meta { color:var(--fg-3); font-size:var(--fs-xs); }
.imp-steps { margin:0; padding-left:20px; font-size:var(--fs-xs); color:var(--fg-3);
  display:flex; flex-direction:column; gap:3px; }
.imp-alert { display:flex; gap:10px; align-items:center; flex-wrap:wrap; padding:10px 12px;
  border-radius:var(--radius-md); background:var(--warning-bg); color:var(--warning-fg);
  font-size:var(--fs-sm); margin-bottom:12px; }
.imp-paste.imp-dragover { border-style:solid; border-color:var(--accent); background:var(--accent-bg-soft); }
/* Имя оператора — одно поле на вкладку, в шапке секции. */
.imp-who { display:flex; align-items:center; gap:6px; font-size:var(--fs-xs);
  color:var(--fg-3); white-space:nowrap; }
.imp-who input { font-size:var(--fs-sm); padding:4px 8px; width:170px;
  border:1px solid var(--border); border-radius:var(--radius-sm);
  background:var(--bg-1); color:var(--fg-1); }
.imp-who input:focus-visible { outline:none; box-shadow:var(--focus-ring); }
/* Точечное добавление у оператора свёрнуто (эпизодическая задача), у
   владельца раскрыто — это его единственный инструмент ввода. Маркер и
   поведение — от общего details.fold, здесь только кегль заголовка: это
   заголовок карточки, а не подпись внутри неё. */
.ac-fold { margin-top:0; }
.ac-fold > summary { color:var(--fg-2); font-size:var(--fs-md); }
/* Панель набора «мои суды» над светофором. .spacer объявлен только внутри
   .section-head/.card-head/.sub-row — строке нужен свой (та же грабля уже
   документирована выше). */
.imp-my-bar { display:flex; align-items:center; gap:8px; flex-wrap:wrap;
  margin-bottom:8px; padding-bottom:8px; border-bottom:1px solid var(--divider); }
.imp-my-bar .spacer { flex:1; }
.imp-my-bar:empty { display:none; }
/* Строка в режиме выбора: галка вместо цветной точки, курсор — рука. */
.imp-fresh-row.is-edit { cursor:pointer; gap:9px; align-items:center; }
.imp-fresh-row.is-edit input { margin:0; accent-color:var(--accent); }
.imp-fresh-row.is-edit .health-name { color:var(--fg-1); }
/* «Прочие суды» — не моя очередь, поэтому приглушены и свёрнуты. */
.imp-others { margin-top:8px; }
.imp-others .health-row { opacity:0.75; }
.imp-selection { font-size:var(--fs-xs); color:var(--fg-3); display:flex; gap:8px;
  align-items:center; flex-wrap:wrap; }
.imp-selection:empty { display:none; }
.imp-file-chip { display:inline-flex; align-items:center; gap:6px; padding:3px 10px;
  border-radius:var(--radius-pill); background:var(--info-bg); color:var(--info-fg);
  font-weight:var(--fw-medium); }
.imp-file-clear { display:inline-flex; align-items:center; justify-content:center; width:16px; height:16px;
  vertical-align:-3px; border:0; background:none; color:inherit; cursor:pointer; padding:0; }
.imp-file-clear svg { display:block; width:10px; height:10px; }
/* Вспышка формы после клика по суду в светофоре. */
.imp-flash { animation:impFlash 1.6s var(--ease-out) 1; border-radius:var(--radius-md); }
@keyframes impFlash { 0% { box-shadow:var(--focus-ring); } 100% { box-shadow:0 0 0 3px transparent; } }
@media (prefers-reduced-motion: reduce) { .imp-flash { animation:none; } }
/* Светофор свежести: строки-кнопки (клик = выбрать суд в форме). */
#imp-freshness .health-row { cursor:pointer; flex-wrap:wrap; }
#imp-freshness .health-row:hover .health-name { color:var(--accent); }
#imp-freshness .health-row:focus-visible { outline:2px solid var(--accent); outline-offset:-2px;
  border-radius:var(--radius); }
#imp-freshness .health-name { flex:1 1 0; min-width:0; }
.imp-fresh-meta { margin-left:auto; flex-shrink:0; } /* распорка вместо пустого health-spark */
/* Пометка про карточки — ВСЕГДА своей строкой под именем (отступ = точка+gap,
   как у мобильной меты). Ужать её в общую строку нельзя: .health-name
   схлопывается многоточием раньше, чем сработает перенос, и имя суда — то
   единственное, по чему оператор выбирает, куда идти, — исчезало первым. */
.imp-fresh-row { flex-wrap:wrap; }
/* Тихая, как мета: это ОБЪЯСНЕНИЕ красной точки, а не отдельная тревога.
   На боевом реестре пометку носят 10 строк из 54 (до четырёх подряд), и
   красным они превращали рабочую очередь в стену алярма — счётчик наверху
   («N судов не отдали») тревогу и так поднимает. */
.imp-fresh-warn { flex-basis:100%; margin-left:17px; color:var(--fg-3);
  font-size:var(--fs-xs); }

/* Форма слева, рабочая очередь справа. min-width:0 обеим колонкам: зона
   вставки с таблицей суда внутри иначе распирает свой трек. */
.imp-grid { display:grid; grid-template-columns:1fr; gap:14px; }
.imp-grid > * { min-width:0; }
.imp-side { display:flex; flex-direction:column; }
@media (min-width: 1200px) {
  .imp-grid { grid-template-columns:minmax(0, 7fr) minmax(0, 5fr); gap:22px; align-items:start; }
  /* Свёртки справа начинаются вровень с формой, без верхнего отступа fold. */
  .imp-side > details.fold:first-child { margin-top:0; }
}

/* Мобильная раскладка формы импорта. ⚠️ Своим медиа-блоком, а НЕ строкой в
   общем «═══ Мобильная раскладка ═══» выше: тот блок стоит в файле РАНЬШЕ
   правил .imp-row, и при равной специфичности базовые правила его перебивали
   (подпись «Суд» оставалась по центру от align-items:center, поля — 13px). */
@media (max-width: 768px) {
  .imp-row { gap:10px; }
  /* Подпись над полем, поле во всю ширину. */
  .imp-row label { flex-direction:column; align-items:stretch; gap:4px; flex-basis:100%; }
  /* 16px обязателен: iOS зумит страницу на любом поле мельче 16px, и оператор
     после каждого касания оказывался в увеличенной вёрстке (--fs-sm = 13px). */
  .imp-row select, .imp-row input[type=text] { width:100%; font-size:16px; }
  .imp-row .imp-file-btn, .imp-row #imp-send { width:100%; justify-content:center; }
  #imp-court-link { align-self:flex-start; }
  /* Точечное добавление: 16px обязателен — iOS зумит поля мельче. */
  #ac-input { font-size:16px; }
  /* Имя в шапке секции: своя строка во всю ширину, 16px против зума iOS. */
  .imp-who { flex-basis:100%; }
  .imp-who input { flex:1; width:auto; font-size:16px; }
  .imp-row #ac-send { width:100%; justify-content:center; }
}
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
    <nav class="header-nav" id="nav" role="tablist" aria-label="Разделы админки">
      ${isOperator ? IMPORT_CHIP : ""}
      <a class="chip-btn${isOperator ? "" : " active"}" href="#system" id="nav-system" role="tab" aria-controls="system" aria-selected="${isOperator ? "false" : "true"}" tabindex="${isOperator ? "-1" : "0"}">Система</a>
      ${isOperator ? "" : IMPORT_CHIP}
      <a class="chip-btn" href="#llm" id="nav-llm" role="tab" aria-controls="llm" aria-selected="false" tabindex="-1" data-owner-only>LLM</a>
      <a class="chip-btn" href="#subs" id="nav-subs" role="tab" aria-controls="subs" aria-selected="false" tabindex="-1" data-owner-only>Подписчики <span class="chip-count" id="nav-subs-count">…</span></a>
    </nav>
    <div class="header-actions">
      <div class="header-meta" id="summary" data-owner-only>…</div>
      <button class="theme-toggle" id="theme-toggle" onclick="toggleTheme()" aria-label="Тема">
        <svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
        <svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
        <svg class="icon-auto" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor" stroke="none"/></svg>
      </button>
      <button class="btn-refresh" onclick="refreshAll(this)" title="Обновить данные">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>
        <span>Обновить</span>
      </button>
    </div>
  </div>
</header>

<main class="app-main">
  <div class="pult">
    <!-- У оператора плитка НЕ ведёт на GitHub (доступа туда у него нет): без
         data-href она выпадает из делегирования кликов, disabled убирает её из
         фокуса, а ghRunSub не рисует стрелку ↗. -->
    <button class="stat-card" data-accent="gray"${isOperator ? " disabled" : ' data-href="run" title="Открыть лог прогона в GitHub Actions"'}>
      <div class="stat-label">Последний прогон</div>
      <div class="stat-value" id="tile-run-value">…</div>
      <div class="stat-sub" id="tile-run-sub"></div>
    </button>
    <!-- Дайджест и автозапуск — продукт и расписание владельца. Оператор
         диспатчит свой workflow сам и крона не ждёт: три плитки вместо пяти
         поднимают его форму импорта выше (на телефоне это было три ряда). -->
    <button class="stat-card" data-accent="blue" data-href="digest" title="Открыть дайджест на дашборде" data-owner-only>
      <div class="stat-label">Дайджест</div>
      <div class="stat-value" id="tile-digest-value">…</div>
      <div class="stat-sub" id="tile-digest-sub"></div>
    </button>
    <!-- Владельцу — здоровье автопоиска; оператору оно не про его работу:
         parse_health наполняется только по courts_for_search, а тот исключает
         search_gated, то есть ровно суды оператора (на Урале 56 из 69). Он
         читал «все N ok» как «мои суды в порядке». На их месте — состояние
         ЕГО канала: открываются ли карточки, считается по журналу импортов
         без единого лишнего запроса. -->
    ${isOperator ? `<button class="stat-card" data-accent="gray" data-goto="#import" title="Открывались ли карточки судов в последних импортах">
      <div class="stat-label">Карточки судов</div>
      <div class="stat-value" id="tile-cards-value">…</div>
      <div class="stat-sub" id="tile-cards-sub"></div>
    </button>` : `<button class="stat-card" data-accent="gray" data-goto="#system">
      <div class="stat-label">Парсеры</div>
      <div class="stat-value" id="tile-health-value">…</div>
      <div class="stat-sub" id="tile-health-sub"></div>
    </button>`}
    <button class="stat-card" data-accent="gray" data-href="cron" title="Расписание автозапуска и ручной прогон в GitHub Actions" data-owner-only>
      <div class="stat-label">Автозапуск</div>
      <div class="stat-value" id="tile-cron-value">…</div>
      <div class="stat-sub" id="tile-cron-sub"></div>
    </button>
    <button class="stat-card" data-accent="gray" data-goto="#import" id="tile-import-card" style="display:none;">
      <div class="stat-label">Импорты</div>
      <div class="stat-value" id="tile-import-value">…</div>
      <div class="stat-sub" id="tile-import-sub"></div>
    </button>
  </div>

  <section class="section${isOperator ? "" : " is-tab-active"}" id="system" role="tabpanel" aria-labelledby="nav-system">
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      </span>
      <h2 class="section-title">Система</h2>
    </div>
      <!-- Полоса запуска — ВНЕ сетки: после удаления «Полного прогона» тут
           одна кнопка с абзацем, и карточкой в 122px рядом с «Здоровьем» на
           367px она читалась обрубком. В сетке ниже остаются только карточки
           данных. Целиком owner-only: у оператора кнопок нет вовсе. -->
      <div class="card run-bar" data-owner-only>
        <div class="card-head">
          <span class="card-title">Запуск прогона</span>
          <span class="run-meta" id="runs-next"></span>
          <span class="spacer"></span>
          <button class="btn-primary" id="btn-run-std" data-owner-only title="Ровно то, что делает ежедневный автозапуск: smart-skip — пропуск дел с известной будущей датой и нерабочих дней">
            <svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><polygon points="6 3 20 12 6 21 6 3"/></svg>
            Запустить прогон
          </button>
          <span class="action-flash" id="runs-flash" role="status" aria-live="polite"></span>
        </div>
        <div class="run-launch-note">Кнопка делает ровно то же, что ежедневный автозапуск. Статусы и логи — на вкладке Actions в GitHub, плитка «Последний прогон» обновляется сама. Полный обход всех дел (без smart-skip) — там же, через Run workflow.</div>
      </div>
    <div class="system-grid">
      <div class="card">
        <div class="card-head">
          <span class="card-title">Здоровье парсеров</span>
          <span class="spacer"></span>
          <span id="health-badges"></span>
        </div>
        <div id="health-list" class="loading">Загрузка…</div>
        <div class="health-more" id="health-updated"></div>
      </div>
      <!-- Ход последнего прогона: вехи, которые пушер шлёт в KV (с Mac или из
           облака). Вернулся 20.08.2026 при флипе на Mac-резерв: парсинг больше
           не виден в GitHub Actions, а этот канал и так пишется каждым
           прогоном. БЕЗ поллинга — один GET при загрузке и по кнопке: лимиты
           Cloudflare бьют ЗАПИСИ (инцидент 17.07.2026), а не редкие чтения.
           Скрыта, пока в KV нет ни одного прогона. -->
      <div class="card" id="run-progress-card" style="display:none;" data-owner-only>
        <div class="card-head">
          <span class="card-title">Ход последнего прогона</span>
          <span class="run-meta" id="run-progress-meta"></span>
          <span class="spacer"></span>
          <button class="btn-outline btn-sm" type="button" onclick="loadRunProgress()">Обновить</button>
        </div>
        <div id="run-progress-body" class="loading">Загрузка…</div>
      </div>
      <!-- Не операторский трек (решение юриста 02.08.2026): data-owner-only —
           второй рубеж к тому, что loadStaticData ему файл вообще не тянет. -->
      <div class="card" id="bank-parse-card" style="display:none;" data-owner-only>
        <div class="card-head">
          <span class="card-title">Парсинг исков банка</span>
          <span class="run-meta" id="bank-parse-date"></span>
          <span class="spacer"></span>
          <span id="bank-parse-badges"></span>
        </div>
        <div id="bank-parse-list" class="loading">Загрузка…</div>
        <div class="health-more" id="bank-parse-note"></div>
      </div>
      <!-- Ручное закрытие дел, по которым ИЛ не нужен. Доступно обеим ролям:
           о добровольном погашении долга узнаёт тот, кто ведёт дело. Вся
           очередь больше не выводится: остаются только подсказки суда. -->
      <div class="card" id="ww-card" style="display:none;">
        <div class="card-head">
          <span class="card-title">Закрыть дело — ИЛ не нужен</span>
          <span class="run-meta" id="ww-meta"></span>
          <span class="spacer"></span>
          <span id="ww-badges"></span>
        </div>
        <div class="ww-manual">
          <div class="imp-hint">Выберите суд и введите номер дела. После подтверждения дело исчезнет из активной картотеки и перейдёт в архив; действие можно отменить ниже.</div>
          <div class="ww-manual-grid">
            <label>Суд
              <select id="ww-court"><option value="">загружается…</option></select>
            </label>
            <label>Номер дела
              <input type="text" id="ww-case" spellcheck="false" placeholder="2-1234/2026">
            </label>
            <label>Почему ИЛ не нужен
              <select id="ww-reason">
                <option value="debt_paid">долг погашен после решения</option>
                <option value="not_requested">лист решили не запрашивать</option>
                <option value="other">иное</option>
              </select>
            </label>
            <label>Кто закрывает
              <input type="text" id="ww-name" maxlength="60" placeholder="ваше имя">
            </label>
          </div>
          <div class="ww-actions">
            <button class="btn btn-primary" id="ww-manual-send" disabled>Закрыть и отправить в архив</button>
            <span class="run-meta" id="ww-manual-status" role="status" aria-live="polite"></span>
          </div>
        </div>
        <div class="ww-hints-title" id="ww-hints-title">Подсказки суда</div>
        <div id="ww-list" class="loading">Загрузка…</div>
        <div id="ww-waived-wrap"></div>
        <div class="ww-actions" id="ww-actions" style="display:none;">
          <button class="btn btn-primary" id="ww-send" disabled>Закрыть выбранные</button>
          <button class="btn" id="ww-reset">Очистить</button>
          <span class="run-meta" id="ww-status"></span>
        </div>
      </div>
    </div>
  </section>

${IMPORT_SECTION}

  <section class="section" id="llm" role="tabpanel" aria-labelledby="nav-llm" data-owner-only>
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3"/></svg>
      </span>
      <h2 class="section-title">LLM · тест дайджеста</h2>
    </div>
    <div class="card">
      <!-- Рейтинг — справочник к форме, а не статус: свёрнут и грузится по
           раскрытию (или при выборе провайдера openrouter). Раньше внешний
           запрос к shir-man уходил на каждый заход и на каждое «Обновить», а
           блок занимал треть мобильного экрана перед «Подписчиками». -->
      <details class="fold" id="llm-top-fold">
        <summary>Топ бесплатных моделей OpenRouter — что стоит за «топ-N»</summary>
        <div class="fold-body">
          <div id="llm-top-body" class="empty">рейтинг загрузится при раскрытии</div>
          <div class="llm-updated" id="llm-updated"></div>
        </div>
      </details>
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
          <span class="action-flash" id="tf-flash" role="status" aria-live="polite"></span>
        </div>
        <div class="tform-hint">Без галок безопасно: Telegram только в личный чат, без публикации на дашборд и без push. «Push всем» работает только вместе с «опубликовать».</div>
      </div>
    </div>
  </section>

  <section class="section" id="subs" role="tabpanel" aria-labelledby="nav-subs" data-owner-only>
    <div class="section-head">
      <span class="section-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      </span>
      <h2 class="section-title">Подписчики</h2>
      <span class="section-counter" id="subs-count">…</span>
      <span id="subs-orphans"></span>
      <span class="spacer"></span>
      <div class="search-box">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
        <input class="search-input" id="subs-search" placeholder="Имя, устройство или дело…">
      </div>
    </div>
    <div id="root" class="loading">Загрузка…</div>
  </section>

</main>

<!-- Причина пометки «лист не нужен». Модалка, а не контрол в строке:
     колонка сетки ~460px, широкий селект туда не влезает (первая версия
     схлопывала суд и обрезала список). data-owner-only НЕТ — помечает и
     оператор. -->
<dialog class="wl ww-dlg" id="ww-modal">
  <div class="wl-head">Закрыть дело — ИЛ не нужен</div>
  <div class="ww-modal-case" id="ww-modal-case"></div>
  <div id="ww-modal-reasons"></div>
  <div class="wl-foot">
    <button class="btn-outline" type="button" id="ww-modal-cancel">Отмена</button>
    <button class="btn-primary" type="button" id="ww-modal-ok">Добавить к закрытию</button>
  </div>
</dialog>
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
const BANK_URL = ${JSON.stringify(CFG.bankUrl)};
const BANK_ARCHIVE_URL = ${JSON.stringify(CFG.bankArchiveUrl)};
const PUSHES_URL = ${JSON.stringify(CFG.pushesUrl)};
const DIGEST_URL = ${JSON.stringify(CFG.digestUrl)};
const HEALTH_URL = ${JSON.stringify(CFG.healthUrl)};
const BANK_PARSE_URL = ${JSON.stringify(CFG.bankParseUrl)};
const DASHBOARD_URL = ${JSON.stringify(CFG.dashboardUrl)};
const SITE_BASE = ${JSON.stringify(CFG.siteBase)};
const GH_REPO = ${JSON.stringify(CFG.ghRepo)};

const SVG_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
const SVG_TRASH = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>';

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
// Крестик «закрыть»/«убрать» — только svg. Прежний текстовый «✕» (U+2715)
// в IBM Plex Sans отсутствует вовсе: браузер подставлял системный шрифт с
// чужими вертикальными метриками, и крестик стоял не по центру кнопки
// по-разному на разных ОС. Зеркало ICON_X из app.js.
const ICON_X = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><line x1="6" y1="6" x2="18" y2="18"/><line x1="6" y1="18" x2="18" y2="6"/></svg>';
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
// Блок «данные не загрузились» вместо сырого исключения. Юристу текст
// человеческий, само исключение — в title и console: «TypeError: Failed to
// fetch» в карточке не говорит ни что сломалось, ни что делать. retryKey
// разбирает делегированный слушатель ниже.
function loadErrorHtml(text, retryKey, err) {
  if (err) console.warn(retryKey + ":", err);
  return '<div class="load-error" title="' + escHtml(String(err || "")) + '">'
    + '<span class="dot dot-amber"></span><span>' + escHtml(text) + '</span>'
    + '<button class="btn-outline btn-sm" type="button" data-retry="' + retryKey + '">Повторить</button>'
    + '</div>';
}
// Плитка пульта: значение/подпись/акцент. valueHtml приходит из наших же
// рендеров (не из сырых данных) — вставляется как HTML.
// Склонение по числу: «1 карточка · 2 карточки · 5 карточек». Сводки импорта
// собирались без него и печатали «1 карточек дочитано» / «2 материалов стали
// делами» — мелочь, но оператор читает эти строки каждый рабочий день.
function plural(n, a, b, c) {
  var m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return a;
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return b;
  return c;
}
// «N карточек дочитано» одной сборкой: число + согласованная форма.
function nPlural(n, a, b, c) { return n + " " + plural(n, a, b, c); }

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
// Профили синхронизации звёзд (multi-device watchlist): устройства одного
// юриста связаны общим profile_id, их watchlist живёт в profile:<uuid> KV.
let allProfiles = [];
let profilesById = new Map();
let lastPushesMap = new Map();
let lastPushesGeneratedAt = "";
// Какие подписки юрист развернул. Живёт вне DOM: #root перерисовывается на
// каждое нажатие в поиске и после render(true) (переименование/удаление/
// watchlist) — иначе раскрытая карточка схлопывалась бы под руками.
let subsOpen = new Set();
// То же для карточек профилей без push-устройств (ключ — profile_id).
let profilesOpen = new Set();

// ── Секция «Система»: запуск прогонов GitHub Actions ─────────────────────────
// Список последних прогонов и живой лог убраны из админки (29.07.2026,
// решение юриста) — статусы и логи смотрятся на вкладке Actions в GitHub.
// GET /admin/gh-runs остался: им питаются плитки пульта «Последний прогон»
// и «Автозапуск» + метка следующего крона у кнопок запуска.
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
// Куда ведёт клик по плитке «Последний прогон»: лог именно этого прогона в
// GitHub. html_url приходит с /admin/gh-runs и раньше молча отбрасывался —
// при сбое нужный run приходилось искать глазами на вкладке Actions.
// Ссылку держим переменной, а не <a> внутри плитки: плитка сама <button>,
// вложенный интерактивный элемент ловил бы клик дважды.
let ghLastRunUrl = "";
function ghRunHref() {
  return ghLastRunUrl || ("https://github.com/" + GH_REPO + "/actions/workflows/update_cases.yml");
}
function ghRunSub(run) {
  const num = String(run.run_number || "");
  // Mac-прогон (replay после пуша «(Mac-парсинг)») помечаем источником: после
  // флипа 19.08.2026 это боевой путь, и без пометки плитка читалась как облако.
  const src = run.source === "mac" ? "Mac · " : "";
  // Стрелка ↗ — только владельцу: у оператора плитка не кликается (см. пульт).
  return src + escHtml(relTime(run.run_started_at))
    + (num ? " · #" + escHtml(num) + (IS_OWNER ? " ↗" : "") : "");
}
// Нерабочий ли сегодня день — считает Worker (isHoliday, тот же календарь, что
// у крона). Своей копии производственного календаря у страницы нет: их и так
// две (worker.js и textutil.py). null = сервер не ответил, спрашивать не о чем.
let todayNonWorking = null;
async function loadGhRuns() {
  clearTimeout(ghTimer);
  try {
    const r = await fetch("/admin/gh-runs?secret=" + encodeURIComponent(SECRET));
    const d = await r.json().catch(function () { return {}; });
    if (typeof d.today_non_working === "boolean") todayNonWorking = d.today_non_working;
    if (d.next_cron_at) {
      const t = parseIso(d.next_cron_at);
      if (!isNaN(t)) {
        const txt = new Date(t).toLocaleString("ru-RU",
          { weekday: "short", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
        document.getElementById("runs-next").textContent = "автозапуск: " + txt;
        setTile("cron", "gray", escHtml(txt), document.getElementById("tile-cron-sub").innerHTML);
      }
    } else if (d.next_cron_at === null && r.ok) {
      // Крон выключен (флип на Mac-резерв): плитка обязана это СКАЗАТЬ, а не
      // оставлять прежнее время — иначе она обещает запуск, которого не будет.
      document.getElementById("runs-next").textContent = "автозапуск выключен";
      setTile("cron", "gray", "выключен", "прогон делает Mac (резерв)");
    }
    if (!r.ok) {
      setTile("run", "gray", "—", "GitHub недоступен");
      return;
    }
    // Плитка «Последний прогон» — самый свежий из облачного (update_cases) и
    // Mac-прогона (replay_on_push после пуша «(Mac-парсинг)»): сервер отдаёт
    // его полем last_run (с 20.08.2026 — после флипа на Mac-резерв плитка
    // иначе показывала вчерашний облачный). Фолбэки — на старые поля: пока
    // Worker территории не передеплоен, страница живёт по-прежнему.
    const main = d.last_run || d.main_run || (d.runs || []).find(function (run) {
      return String(run.path || "").indexOf("update_cases.yml") >= 0;
    });
    let hasActive = (d.runs || []).some(function (run) { return run.status !== "completed"; });
    if (main) {
      if (/^https:\\/\\//.test(String(main.html_url || ""))) ghLastRunUrl = main.html_url;
      const active = main.status !== "completed";
      const dur = fmtDur(main.run_started_at, active ? null : main.updated_at);
      if (active) {
        setTile("run", "amber", '<span class="dot dot-amber dot-pulse"></span>идёт · ' + escHtml(dur),
          "старт " + escHtml(relTime(main.run_started_at)));
      } else if (main.conclusion === "success") {
        setTile("run", "green", '<span class="dot dot-green"></span>ok · ' + escHtml(dur), ghRunSub(main));
      } else {
        setTile("run", "red", '<span class="dot dot-red"></span>' + escHtml(main.conclusion || "сбой"), ghRunSub(main));
      }
    } else {
      setTile("run", "gray", "—", "основной прогон не найден");
    }
    // Пока есть живой прогон — обновляемся сами, чтобы плитка увидела исход
    // без F5 (список прогонов из админки убран, плитка — единственный статус).
    if (hasActive && !document.hidden) ghTimer = setTimeout(loadGhRuns, 15000);
  } catch (e) { /* сеть мигнула — плитка обновится следующим заходом */ }
}
// ── Карточка «Ход последнего прогона» (вехи пушера из KV) ────────────────────
// Данные пишет progress_pusher с Mac (или gh_progress_pusher из облака) — этот
// канал живёт с 13.07.2026 и не выключался; UI-читателя вернули 20.08.2026
// после флипа на Mac-резерв. Читаем РАЗОВО и по кнопке, без поллинга:
// KV-лимиты бьют записи, а не редкие чтения.
async function loadRunProgress() {
  const card = document.getElementById("run-progress-card");
  const body = document.getElementById("run-progress-body");
  const meta = document.getElementById("run-progress-meta");
  if (!card || !IS_OWNER) return;
  try {
    const r = await fetch("/admin/run-progress?secret=" + encodeURIComponent(SECRET));
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    // current — идущий/последний прогон, prev — предыдущий (см. worker.js).
    const run = (d && d.current && Array.isArray(d.current.lines) && d.current.lines.length)
      ? d.current
      : (d && d.prev && Array.isArray(d.prev.lines) && d.prev.lines.length ? d.prev : null);
    if (!run) { card.style.display = "none"; return; }
    card.style.display = "";
    const src = run.source === "github" ? "облако" : "Mac";
    // Финальная веха «ERROR:» = прогон закончился сбоем (алерт конца окна,
    // упавший парсинг) — подпись «завершён» читалась как успех (20.08.2026).
    const lastLine = String((run.lines || []).slice(-1)[0] || "");
    let state = lastLine.indexOf("ERROR:") >= 0 ? "сбой" : "завершён";
    if (!run.done) {
      const age = Date.now() - parseIso(run.updated_at || run.started_at || "");
      // Пушер шлёт батчи ~раз в минуту: полчаса тишины без done — прогон
      // оборвался (Mac уснул, скрипт убит), честнее сказать это, чем «идёт».
      state = (isNaN(age) || age > 30 * 60000) ? "не завершился" : "идёт";
    }
    meta.textContent = src
      + (run.started_at ? " · старт " + relTime(run.started_at) : "")
      + " · " + state;
    // ⚠️ В join обязателен ДВОЙНОЙ обратный слэш: страница — template literal,
    // и одинарный слэш-n становится НАСТОЯЩИМ переносом строки внутри
    // отдаваемого браузеру скрипта — разорванная строка убивает весь JS
    // админки (инцидент 20.08.2026 «админка пустая»; страж —
    // test_admin_page_inner_js_parses, конвенция файла — соседние join'ы).
    const tail = (run.lines || []).slice(-12).map(function (s) {
      return escHtml(String(s));
    }).join("\\n");
    body.className = "";
    body.innerHTML = '<details class="fold"><summary>последние шаги (всего строк: '
      + (run.lines || []).length + ')</summary>'
      + '<div class="fold-body"><pre class="log-pre">' + tail + '</pre></div></details>';
  } catch (e) {
    card.style.display = "";
    body.className = "";
    body.innerHTML = loadErrorHtml("Ход прогона не загрузился", "runprog", e);
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
      setFlash(flashEl, "✓ запущен — статус в плитке «Последний прогон»", "ok", 9000);
      // GitHub регистрирует run не мгновенно — обновим список дважды.
      setTimeout(loadGhRuns, 3000);
      setTimeout(loadGhRuns, 12000);
    } else {
      setFlash(flashEl, "× " + (d.error || d.detail || ("HTTP " + r.status)), "err");
    }
  } catch (e) {
    setFlash(flashEl, "× " + e, "err");
  }
}
// Единственная кнопка запуска (02.08.2026, решение юриста): «Полный прогон»
// (smart_skip=false) убран — тяжёлый разовый обход всех активных дел нужен
// редко и запускается из GitHub Actions (Run workflow → снять галку
// smart_skip). Осталось ровно то, что делает ежедневный крон.
document.getElementById("btn-run-std").addEventListener("click", function () {
  // В нерабочий день предлагаем прогнать «как крон, но мимо календаря»:
  // иначе прогон завершится за 20 секунд строкой «нерабочий день РФ».
  // Отказ = ничего не запускаем (раньше запускался холостой прогон).
  if (todayNonWorking) {
    if (!confirm("Сегодня нерабочий день РФ (выходной или праздник).\\n\\nПрогнать всё равно — как ежедневный автозапуск, но игнорируя календарь? Пропуск дел с известной будущей датой сохранится.")) return;
    dispatchWorkflow("update_cases.yml", { smart_skip: "true", ignore_calendar: "true" },
      document.getElementById("runs-flash"));
    return;
  }
  if (!confirm("Запустить прогон сейчас — как ежедневный автозапуск?\\n\\nПарсинг судов + дайджест + Telegram + push подписчикам. Smart-skip: пропуск дел с известной будущей датой.")) return;
  dispatchWorkflow("update_cases.yml", { smart_skip: "true" }, document.getElementById("runs-flash"));
});


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
// Мини-график последних прогонов. Столбики CSS, а не юникод-блоки: блоки
// нормировались по СОБСТВЕННОМУ максимуму строки, поэтому ровный ряд [23×10]
// и качающийся [21,20,19,…] выглядели одинаково, а «▁» означало и ноль, и
// «просто меньше остальных». Теперь ноль — отдельная красная риска, и он
// виден. Шкала по-прежнему своя у каждой строки: у судов разный порядок
// величин, общая шкала расплющила бы мелкие.
function healthSpark(counts) {
  const last = (counts || []).slice(-10);
  if (!last.length) return "";
  const max = Math.max.apply(null, last);
  return last.map(function (c) {
    const v = Number(c) || 0;
    if (v <= 0) return '<i class="hb hb-zero" title="0 результатов"></i>';
    const h = max > 0 ? Math.max(18, Math.round((v / max) * 100)) : 18;
    return '<i class="hb" style="height:' + h + '%" title="' + escHtml(String(v)) + '"></i>';
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
    // ⚠️ Охват называем вслух: parse_health.json наполняется только по
    // courts_for_search, а тот ИСКЛЮЧАЕТ search_gated — то есть все капчёвые
    // суды (на Урале 56 из 69). Без этой строки «все N ok» читалось как «на
    // территории всё в порядке», хотя про суды оператора карточка молчит.
    document.getElementById("health-updated").textContent =
      "число результатов поиска по прогонам · только суды с открытым поиском"
      + " (капчёвые сюда не входят — их канал ручной импорт) · обновлено "
      + relTime(d.updated_at);
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
    listEl.innerHTML = loadErrorHtml("Данные о здоровье парсеров не загрузились", "health", e);
    // Не «—/нет данных» и не янтарный: серый «?» отличает СБОЙ ЗАГРУЗКИ от
    // реального состояния парсеров, а янтарь занят под «N парсеров ⚠».
    setTile("health", "gray", "?", "не загрузилось · повторить");
  }
}

// ── Секция «Система»: отчёт парсинга исков банка (bank_parse_report.json) ────
// Пер-кейсовый итог последнего прогона по bank-треку: какие дела парсились,
// какие пропущены и почему. Файл пишет BankParseReport (фаза 7c main_json);
// нет файла (трек выключен / территория без bank-трека) — карточка скрыта.
// Группы: проблемные раскрыты, рутинные свёрнуты; внутри группы рендерим
// порциями по BP_CHUNK строк (на Урале дел будут тысячи — DOM не раздуваем).
var BP_CHUNK = 30;
var bpGroupsData = {};
var BP_GROUPS = [
  { key: "fail", title: "Ошибка загрузки карточки", dot: "dot-red", open: true },
  { key: "breaker", title: "Суд снят с обхода (предохранитель)", dot: "dot-red", open: false },
  { key: "nocard", title: "Без карточки: суд/ссылка", dot: "dot-amber", open: true },
  { key: "queue", title: "Вне очереди 1-й инстанции", dot: "dot-gray", open: true },
  { key: "intake", title: "Заведено авто-подхватом с выдачи", dot: "dot-green", open: true },
  { key: "parsed", title: "Спарсено", dot: "dot-green", open: false },
  { key: "writ", title: "Пропуск: недельный ритм ИЛ (решённые)", dot: "dot-gray", open: false },
  { key: "hearing", title: "Пропуск: заседание в будущем", dot: "dot-gray", open: false },
  { key: "othskip", title: "Пропуск: прочее (без движения и др.)", dot: "dot-gray", open: false },
];
function bpGroupKey(row) {
  var o = String(row.outcome || "");
  if (o === "parsed") return "parsed";
  if (o === "not_in_queue") return "queue";
  // Дела, заведённые авто-подхватом в этом же прогоне: карточку они уже
  // прочитали при приёме, в «спарсено» не попадают.
  if (o === "intake_new") return "intake";
  // Дела суда, снятого с обхода предохранителем (аутейдж портала): их могут
  // быть сотни разом — отдельная свёрнутая группа, чтобы не топить «fail».
  if (o === "court_breaker") return "breaker";
  if (o === "court_disabled" || o === "no_link" || o === "bad_link") return "nocard";
  if (o === "skip") {
    var reason = String(row.reason || "");
    if (reason.indexOf("writ_weekly") === 0) return "writ";
    if (reason.indexOf("future_hearing") === 0) return "hearing";
    return "othskip";
  }
  return "fail"; // fetch_* / empty_shell / unknown
}
function bpShortDate(iso) {
  // "2026-07-27" → "27.07"
  var p = String(iso || "").split("-");
  return p.length === 3 ? p[2] + "." + p[1] : String(iso || "");
}
// Разрез по судам. Отчёт сгруппирован по ИСХОДУ, и крупнейшие группы —
// рутина («заседание в будущем» — 199 дел из 344): если ляжет целый суд, его
// строки утонут среди сотен обычных. Здесь один ряд на суд, проблемные сверху.
function bpCourtsFoldHtml(rows) {
  var by = {};
  rows.forEach(function (row) {
    var name = String(row.court || row.court_domain || "—");
    var c = by[name] || (by[name] = { name: name, total: 0, bad: 0, parsed: 0, skip: 0 });
    c.total++;
    var g = bpGroupKey(row);
    if (g === "fail" || g === "breaker" || g === "nocard") c.bad++;
    else if (g === "parsed" || g === "intake") c.parsed++;
    else if (g === "writ" || g === "hearing" || g === "othskip") c.skip++;
  });
  var list = Object.keys(by).map(function (k) { return by[k]; });
  if (!list.length) return "";
  list.sort(function (a, b) {
    if (a.bad !== b.bad) return b.bad - a.bad;
    return b.total - a.total;
  });
  var anyBad = list.some(function (c) { return c.bad > 0; });
  var body = list.map(function (c) {
    var parts = [c.total + " дел"];
    if (c.parsed) parts.push(c.parsed + " спарсено");
    if (c.skip) parts.push(c.skip + " пропуск");
    return '<div class="health-row"><span class="dot ' + (c.bad ? "dot-red" : "dot-green") + '"></span>'
      + '<span class="health-name">' + escHtml(c.name) + '</span>'
      + (c.bad ? '<span class="health-note">' + c.bad + ' без карточки/сбой</span>' : '')
      + '<span class="bp-meta">' + escHtml(parts.join(" · ")) + '</span>'
      + '</div>';
  }).join("");
  return '<details class="fold"' + (anyBad ? " open" : "") + '>'
    + '<summary>По судам <span class="bp-group-n">(' + list.length + ')</span></summary>'
    + '<div class="fold-body">' + body + '</div></details>';
}
function bpRowHtml(row, dotCls, gkey) {
  var badges = "";
  // Статус карточки — только у спарсенных: там он про свежепрочитанное
  // состояние дела. В группах пропусков это данные прошлого прогона.
  if (gkey === "parsed" && row.case_status) {
    badges += ' <span class="badge badge-skip">' + escHtml(String(row.case_status)) + '</span>';
  }
  if (row.degraded) badges += ' <span class="badge badge-run">огрызок</span>';
  // Форс-парс — штатный механизм (дело давно не проверялось), не тревога.
  if (row.force_parsed) badges += ' <span class="badge badge-skip">форс-парс</span>';
  if (row.left_track) badges += ' <span class="badge badge-appeal">переезд в основной трек</span>';
  if (row.archived) badges += ' <span class="badge badge-archive">в архив трека</span>';
  if (row.events && row.events.length)
    badges += ' <span class="badge badge-ok">' + row.events.length + ' соб.</span>';
  else if (row.changed) badges += ' <span class="badge badge-ok">обновлено</span>';
  var why = String(row.reason_ru || row.detail || "");
  var checked = row.last_checked_at ? "провер. " + bpShortDate(row.last_checked_at) : "не проверялось";
  return '<div class="bp-row"><span class="dot ' + dotCls + '"></span>'
    + '<span class="bp-num">' + escHtml(String(row.number || row.key || "?")) + '</span>'
    + '<span class="bp-court">' + escHtml(String(row.court || "")) + '</span>'
    + (why ? '<span class="bp-why">' + escHtml(why) + '</span>' : '')
    + badges
    + '<span class="bp-meta">' + escHtml(checked) + '</span>'
    + '</div>';
}
function bpAppendRows(gkey) {
  var g = bpGroupsData[gkey];
  var box = document.getElementById("bp-rows-" + gkey);
  if (!g || !box) return;
  var next = g.rows.slice(g.rendered, g.rendered + BP_CHUNK);
  box.insertAdjacentHTML("beforeend", next.map(function (row) { return bpRowHtml(row, g.dot, gkey); }).join(""));
  g.rendered += next.length;
  var btn = document.getElementById("bp-more-" + gkey);
  if (btn) {
    if (g.rendered >= g.rows.length) btn.style.display = "none";
    else btn.textContent = "Показать ещё (" + (g.rows.length - g.rendered) + ")";
  }
}
async function loadBankParse() {
  var card = document.getElementById("bank-parse-card");
  var listEl = document.getElementById("bank-parse-list");
  try {
    var r = await fetch(BANK_PARSE_URL, { cache: "no-cache" });
    // Прячем карточку ТОЛЬКО на 404 — «территория без bank-трека». Раньше
    // прятали при любом !r.ok, и 502 от Pages выглядел так же: трека будто
    // нет вовсе, вместо честного «не загрузилось».
    if (r.status === 404) { card.style.display = "none"; return; }
    if (!r.ok) throw new Error("HTTP " + r.status);
    var d = await r.json();
    var rows = d.cases || [];
    var totals = d.totals || {};
    card.style.display = "";
    document.getElementById("bank-parse-date").textContent =
      "прогон " + bpShortDate(d.run_date) + "." + String(d.run_date || "").slice(0, 4)
      + (d.smart_skip === false ? " · полный (без smart-skip)" : "");
    document.getElementById("bank-parse-badges").innerHTML =
      (totals.failed ? '<span class="badge badge-fail">' + totals.failed + ' сбой</span> ' : "")
      + (totals.no_card ? '<span class="badge badge-run">' + totals.no_card + ' без карточки</span> ' : "")
      + '<span class="badge badge-ok">' + (totals.parsed || 0) + ' спарсено</span> '
      // Пропуск — рутина ритма опроса (заседание в будущем, недельный ритм
      // ИЛ), а не проблема: красить янтарём 292 дела из 344 значит утопить в
      // них настоящие «сбой» и «без карточки».
      + '<span class="badge badge-skip">' + (totals.skip || 0) + ' пропуск</span>'
      + (totals.not_in_queue ? ' <span class="badge badge-skip">' + totals.not_in_queue + ' вне очереди</span>' : "")
      + (totals.intake_new ? ' <span class="badge badge-ok">+' + totals.intake_new + ' подхвачено</span>' : "");
    bpGroupsData = {};
    var byGroup = {};
    rows.forEach(function (row) {
      var k = bpGroupKey(row);
      (byGroup[k] = byGroup[k] || []).push(row);
    });
    var html = bpCourtsFoldHtml(rows);
    BP_GROUPS.forEach(function (g) {
      var items = byGroup[g.key] || [];
      if (!items.length) return;
      bpGroupsData[g.key] = { rows: items, rendered: 0, dot: g.dot };
      html += '<details class="fold bp-group" data-bp="' + g.key + '"' + (g.open ? " open" : "") + '>'
        + '<summary><span class="dot ' + g.dot + '"></span> ' + escHtml(g.title)
        + ' <span class="bp-group-n">(' + items.length + ')</span></summary>'
        + '<div class="fold-body"><div id="bp-rows-' + g.key + '"></div>'
        + (items.length > BP_CHUNK
          ? '<button class="btn-outline bp-more" id="bp-more-' + g.key + '" data-bp-more="' + g.key + '">Показать ещё</button>'
          : '')
        + '</div></details>';
    });
    listEl.className = "";
    listEl.innerHTML = html || '<div class="empty">В отчёте нет дел</div>';
    // Первая порция строк — только раскрытым группам; свёрнутые дорендерятся
    // при первом открытии (toggle), «Показать ещё» — по клику (делегирование).
    BP_GROUPS.forEach(function (g) {
      if (g.open && bpGroupsData[g.key]) bpAppendRows(g.key);
    });
    listEl.querySelectorAll("details[data-bp]").forEach(function (det) {
      det.addEventListener("toggle", function () {
        var k = det.getAttribute("data-bp");
        if (det.open && bpGroupsData[k] && !bpGroupsData[k].rendered) bpAppendRows(k);
      });
    });
    // Свойство, не addEventListener: повторная загрузка (кнопка «Обновить»)
    // не должна плодить дубли обработчика.
    listEl.onclick = function (ev) {
      var btn = ev.target.closest ? ev.target.closest("[data-bp-more]") : null;
      if (btn) bpAppendRows(btn.getAttribute("data-bp-more"));
    };
    document.getElementById("bank-parse-note").textContent =
      "иски банка (1-я инст.): " + (totals.total || rows.length) + " дел · обновлено "
      + relTime(d.updated_at);
  } catch (e) {
    // Файл есть, но не распарсился/сеть мигнула — карточку не прячем зря.
    card.style.display = "";
    listEl.className = "";
    listEl.innerHTML = loadErrorHtml("Отчёт парсинга исков банка не загрузился", "bank", e);
  }
}

// ── Секция «LLM»: рейтинг shir-man + мини-форма теста ────────────────────────
// Рейтинг — внешний запрос к стороннему API. Грузим ЛЕНИВО: по раскрытию
// свёртки или при выборе провайдера openrouter (там подписи «топ-N» без
// рейтинга бессмысленны). Заход в админку за ним больше не ходит.
let llmTopLoaded = false;
async function loadLlmTop() {
  if (llmTopLoaded) return;
  llmTopLoaded = true;
  const el = document.getElementById("llm-top-body");
  el.className = "loading";
  el.textContent = "Загрузка…";
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
  // Подписи «топ-N» без рейтинга не говорят ничего — подтягиваем его тут.
  if (this.value === "openrouter") loadLlmTop();
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
    // Картотека «Иски банка»: composite-звёзды («домен|номер») без неё
    // показывались бы как «нигде не найдено». До пилота файлов нет — 404
    // глотается молча.
    fetch(BANK_URL, { cache: "no-cache" }).catch(function () { return null; }),
    fetch(BANK_ARCHIVE_URL, { cache: "no-cache" }).catch(function () { return null; }),
  ]);
  const subsRes = results[0];
  if (!subsRes.ok) throw new Error("HTTP " + subsRes.status + " /admin/data");
  const dataJson = await subsRes.json();
  // Новый Worker отдаёт {subs, profiles} (профили синхронизации звёзд);
  // старый (окно отката) — голый массив. Толерантность к обеим формам
  // обязательна: страница и Worker технически деплоятся врозь.
  const subs = Array.isArray(dataJson) ? dataJson : (dataJson.subs || []);
  const profiles = Array.isArray(dataJson) ? [] : (dataJson.profiles || []);
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
  // Bank-дела (активные + горячий архив): канон в карте — composite
  // «домен|номер» (номера не уникальны между судами, форма звёзд трека);
  // голый номер добавляется алиасом — ручной ввод в watchlist тоже находится.
  function addBankCases(res, archived) {
    if (!res || !res.ok) return Promise.resolve();
    return res.json().then(function (j) {
      const list = Array.isArray(j && j.cases) ? j.cases : [];
      for (const c of list) {
        const bare = bareCaseNumber(c.id);
        if (!bare) continue;
        const dom = String((c.first_instance && c.first_instance.court_domain) || "").trim();
        const comp = dom ? dom + "|" + bare : bare;
        const payload = {
          plaintiff: c.plaintiff || "",
          defendant: c.defendant || "",
          court: (c.first_instance && c.first_instance.court) || "",
          stage: c.current_stage || "",
          canonical_id: comp,
          bank: true,
        };
        if (archived) { payload.archived = true; payload.archived_at = c.archived_at || ""; }
        addAlias(casesMap, comp, payload);
        addAlias(casesMap, bare, payload);
        addAlias(casesMap, c.first_instance && c.first_instance.case_number, payload);
        // М-предок и composite-формы номеров — зеркало addCaseAliases (фикс
        // 11.08 дошёл только до основной ветки): промоушен М→2 переименовывает
        // дело, а звезда трека хранится composite «домен|М-…» — без этих
        // алиасов она показывалась «нигде не найдено» (инцидент 26.08.2026,
        // 2+2 дела на обеих территориях).
        const caseNum = bareCaseNumber(c.first_instance && c.first_instance.case_number);
        const mat = bareCaseNumber(c.first_instance && c.first_instance.material_number);
        if (mat) addAlias(casesMap, mat, payload);
        if (dom) {
          if (caseNum) addAlias(casesMap, dom + "|" + caseNum, payload);
          if (mat) addAlias(casesMap, dom + "|" + mat, payload);
        }
      }
    }).catch(function (e) { console.warn("cases_bank*.json не загружен:", e); });
  }
  await addBankCases(results[5], false);
  await addBankCases(results[6], true);
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
  return { subs, profiles, casesMap, activeCases, pushesMap, pushesGeneratedAt, digest };
}

// Разбор строки сводки дайджеста на ИМЕНОВАННЫЕ части.
// Боевой формат (runs.py): «🆕 Новых: 4 · 📋 Изменений: 6 · ➡️ Переходов: 2».
// Раньше плитка брала match(/\\d+/) — ПЕРВОЕ число строки — и подписывала его
// словом «изменений»: при «Новых: 4 · Изменений: 6» юрист каждое утро читал
// «4 изменений», то есть число новых дел под чужой подписью, а изменения не
// показывались вовсе. Пустой массив = формат не узнан (replay пишет свой) —
// вызывающий печатает summary как есть, он и так человекочитаемый.
function digestSummaryParts(summary) {
  const s = String(summary || "");
  const out = [];
  [["Новых", "новых"], ["Изменений", "изм."], ["Переходов", "перех."]].forEach(function (pair) {
    const m = s.match(new RegExp(pair[0] + ":\\\\s*(\\\\d+)"));
    if (m && Number(m[1]) > 0) out.push({ n: m[1], unit: pair[1] });
  });
  return out;
}
// Плитка «Дайджест» + push-агрегат в плитке «Автозапуск».
function renderDigestTile(digest, pushesMap, pushesGeneratedAt) {
  if (digest && digest.generated_at) {
    const parsed = digestSummaryParts(digest.summary);
    let value;
    if (digest.is_empty) value = "пусто";
    else if (parsed.length) {
      value = parsed.map(function (p) {
        return '<span class="tile-part">' + escHtml(p.n) + ' <i>' + escHtml(p.unit) + '</i></span>';
      }).join("");
    } else {
      // Сводку не разобрали на числа — печатаем текстом, но КЛАМПОМ: replay
      // (test_digest.yml с публикацией результатов) пишет в summary полную
      // сводку дайджеста на 9 частей, и голый текст раздувал ряд пульта.
      // Полная строка остаётся в title (у кнопки свой — вложенный побеждает).
      value = '<span class="tile-text" title="' + escHtml(digest.summary || "")
        + '">' + escHtml(digest.summary || "—") + '</span>';
    }
    // Подпись без ссылки «на дашборд»: кликабельна вся плитка (data-href),
    // ссылка внутри <button> была вложенным интерактивным элементом.
    setTile("digest", "blue", value, escHtml(relTime(digest.generated_at)) + " · открыть ↗");
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
  // general/broadcast — штатные варианты рассылки (подписчик без watchlist),
  // не тревога: янтарь тут только сбивал с толку.
  const cls = v === "personal" ? "badge-ok" : v === "skip" ? "badge-skip" : "badge-watch";
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
// opts.readOnly — рендер без кнопок действий: карточка профиля-сироты живёт
// без endpoint'а, а handleAction выходит на «if (!endpoint) return», то есть
// крестик там был бы мёртвой кнопкой.
function caseRowHtml(num, casesMap, opts) {
  const readOnly = !!(opts && opts.readOnly);
  const bare = bareCaseNumber(num);
  const c = casesMap.get(bare);
  // Composite-запись трека «Иски банка» («домен|номер») показываем юристу
  // номером, домен остаётся в тултипе — полная форма нужна только push'у.
  const isComposite = String(num).indexOf('|') > -1;
  const shownNum = isComposite ? String(num).split('|')[1] : num;
  if (!c) {
    // Номер-сирота: дела нет ни в активных, ни в архиве (удалено вручную или
    // переименовано до Этапа 3, когда М-алиасы ещё не сохранялись). Держать
    // его в watchlist бессмысленно — даём убрать прямо из карточки.
    return '<div class="case-row"><span class="case-num" title="' + escHtml(num) + '">' + escHtml(shownNum) + '</span>'
      + '<span class="badge badge-run" title="Дело удалено или переименовано без алиаса — push по этому номеру никогда не сработает">нигде не найдено</span>'
      + (readOnly ? '' : '<button class="btn-icon" type="button" data-action="wldel" data-wl-num="' + escHtml(num) + '" title="Убрать номер из watchlist" aria-label="Убрать номер из watchlist">' + ICON_X + '</button>')
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
  const bankNote = c.bank
    ? '<span class="badge badge-watch" title="Картотека «Иски банка» (cases_bank.json)">🏦</span>'
    : '';
  const archNote = c.archived
    ? '<span class="badge badge-archive" title="Дело завершено и лежит в ' + (c.bank ? 'cases_bank_archive.json' : 'cases_archive.json') + (c.archived_at ? ' с ' + escHtml(c.archived_at) : '') + '. Звезда снова заработает при реактивации.">в архиве</span>'
    : '';
  return '<div class="case-row"><span class="case-num" title="' + escHtml(num) + '">' + escHtml(shownNum) + '</span>'
    + aliasNote
    + bankNote
    + stageBadge(c.stage)
    + archNote
    + '<span class="case-parties">' + parties + '</span>'
    + (c.court ? '<span class="case-court">' + escHtml(c.court) + '</span>' : '')
    + '</div>';
}
// Сирот по КОНКРЕТНОЙ подписке — для свёрнутой строки. Глобальный счётчик в
// шапке отвечает «есть ли проблема вообще», этот — «у кого именно»: иначе
// свёрнутый список её прячет.
function subOrphanCount(wl, casesMap) {
  var n = 0;
  for (var i = 0; i < wl.length; i++) {
    if (!casesMap.get(bareCaseNumber(wl[i]))) n++;
  }
  return n;
}
// Карточка подписчика — свёртка (02.08.2026): раньше 8 подписок занимали
// 1983px, то есть 76% страницы. Свёрнуто — строка на подписку, развёрнуто —
// всё прежнее содержимое.
// ⚠️ Класс .sub-card остаётся на самом <details>, data-endpoint не переезжает:
// на них завязаны btn.closest(".sub-card") в делегировании и flash().
// ⚠️ В <summary> НЕТ ни одной кнопки — иначе клик по кнопке переключал бы
// свёртку, а <button> внутри <summary> ещё и вложенный интерактив.
function renderCard(sub, casesMap, lastPush, pushesGeneratedAt, isOpen, openCases) {
  const epAttr = escHtml(sub.endpoint || "");
  const wl = Array.isArray(sub.watchlist) ? sub.watchlist : [];
  const nameHtml = sub.label
    ? '<span class="sub-name">' + escHtml(sub.label) + '</span>'
    : '<span class="sub-name unnamed">без имени</span>';
  const cases = wl.length
    ? wl.map(function (num) { return caseRowHtml(num, casesMap); }).join("")
    : '<div class="empty">Юрист не отслеживает ни одно дело</div>';
  const orphans = subOrphanCount(wl, casesMap);
  return '<details class="sub-card" data-endpoint="' + epAttr + '"' + (isOpen ? " open" : "") + '>'
    + '<summary class="sub-row">'
    +   nameHtml
    +   '<span class="badge badge-device">' + escHtml(detectDevice(sub.user_agent)) + '</span>'
    +   (sub.profile_id ? '<span class="badge badge-profile" title="Watchlist общий — из профиля синхронизации устройств; правка отсюда меняет набор ВСЕХ связанных устройств">🔗 ' + escHtml(String(sub.profile_id).slice(0, 8)) + '</span>' : "")
    +   (sub.is_owner ? '<span class="badge badge-owner">★ owner</span>' : "")
    +   expiryBadge(sub)
    +   (orphans ? '<span class="badge badge-run" title="Номера, которых нет ни в активных делах, ни в архиве — push по ним не сработает">⚠ ' + orphans + '</span>' : "")
    +   '<span class="spacer"></span>'
    +   '<span class="sub-count">' + wl.length + ' дел</span>'
    +   pushVariantBadge(lastPush)
    + '</summary>'
    + '<div class="sub-body">'
    +   '<div class="sub-actions">'
    +     '<button class="btn-outline" data-action="rename">✏ Имя</button>'
    +     '<button class="btn-outline" data-action="watchlist">Watchlist</button>'
    +     '<button class="btn-outline" data-action="testpush">Тест push</button>'
    +     '<button class="btn-icon" data-action="copyep" title="Копировать endpoint (…' + escHtml((sub.endpoint || "").slice(-24)) + ')">' + SVG_COPY + '</button>'
    // Удаление — иконкой за разделителем: текстовая кнопка стояла в одном
    // ряду с четырьмя рабочими и отличалась только цветом текста, а на 390px
    // ряд переносился и «Удалить» оказывалась одна слева — прямо под палец.
    +     '<span class="sub-actions-sep"></span>'
    +     '<button class="btn-icon btn-icon-danger" data-action="delete" title="Удалить подписку">' + SVG_TRASH + '</button>'
    +     '<span class="action-flash" role="status" aria-live="polite"></span>'
    +   '</div>'
    +   '<div class="sub-kv">'
    +     '<span>Создана <b>' + escHtml(relTime(sub.created_at)) + '</b></span>'
    +     '<span>Вход <b>' + escHtml(relTime(sub.last_seen_at)) + '</b> <span title="' + escHtml(fullDate(sub.last_seen_at)) + '"></span></span>'
    +     '<span>Watchlist <b>' + escHtml(relTime(sub.last_watchlist_update_at)) + '</b></span>'
    +   '</div>'
    +   '<details class="fold">'
    +     '<summary>Последний push ' + pushVariantBadge(lastPush) + '</summary>'
    +     '<div class="fold-body">' + renderLastPush(lastPush, pushesGeneratedAt) + '</div>'
    +   '</details>'
    +   '<details class="fold"' + (openCases || (wl.length && wl.length <= 10) ? " open" : "") + '>'
    +     '<summary>Дела (' + wl.length + ')</summary>'
    +     '<div class="fold-body">' + cases + '</div>'
    +   '</details>'
    + '</div>'
    + '</details>';
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

// Вспышка результата действия. Успех гаснет сам, ОШИБКА остаётся до крестика:
// текст вроде «× endpoint мёртв (410)» — единственное место, где виден код
// сбоя, и стирать его через 5 секунд значит прятать причину.
function flash(card, text, kind) {
  const el = card.querySelector(".action-flash");
  if (!el) return;
  setFlash(el, text, kind);
}
function setFlash(el, text, kind, holdMs) {
  el.className = "action-flash " + (kind || "");
  if (kind === "err") {
    el.innerHTML = escHtml(text)
      + ' <button class="flash-x" type="button" data-flash-x title="Скрыть" aria-label="Скрыть">' + ICON_X + '</button>';
    return;
  }
  el.textContent = text;
  setTimeout(function () { el.textContent = ""; el.className = "action-flash"; }, holdMs || 5000);
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
  // Архивные и bank-звёзды: в списке активных основной картотеки их нет —
  // но галку надо показать, иначе такую звезду в модалке не видно и снять
  // её нечем (при этом в selected она не теряется и уходит при сохранении
  // как есть). Bank-дела целиком в список не льём (на Урале их сотни) —
  // только уже выбранные звёзды.
  const archRows = [];
  wlState.selected.forEach(function (id) {
    const c = casesMapGlobal.get(bareCaseNumber(id));
    if (!c || (!c.archived && !c.bank)) return;
    if (q && (id + " " + c.plaintiff + " " + c.defendant + " " + c.court).toLowerCase().indexOf(q) < 0) return;
    const parties = (c.plaintiff && c.defendant)
      ? c.plaintiff + " — " + c.defendant
      : (c.plaintiff || c.defendant || "");
    const badge = c.archived
      ? '<span class="badge badge-archive">в архиве</span>'
      : '<span class="badge badge-profile">иски банка</span>';
    archRows.push('<label class="wl-row"><input type="checkbox" data-case-id="' + escHtml(id) + '" checked>'
      + '<span class="case-num">' + escHtml(id) + '</span>'
      + badge
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
        + '<button class="btn-icon" type="button" data-extra-del="' + escHtml(n) + '" title="Убрать" aria-label="Убрать">' + ICON_X + '</button></div>';
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
  let hay = (sub.label || "") + " " + detectDevice(sub.user_agent) + " " + (sub.endpoint || "").slice(-32)
    + " " + (sub.profile_id || "").slice(0, 8);
  for (const num of (Array.isArray(sub.watchlist) ? sub.watchlist : [])) {
    hay += " " + num;
    const c = casesMapGlobal.get(bareCaseNumber(num));
    if (c) hay += " " + c.plaintiff + " " + c.defendant + " " + c.court;
  }
  return hay.toLowerCase().indexOf(q) >= 0;
}
// ── Профиль без push-устройств ───────────────────────────────────────────────
// Профиль-сирота = запись profile:<uuid>, на которую не ссылается ни одна живая
// подписка sub:*. Так выглядят: устройство с запрещёнными уведомлениями (★
// синхронизируются и без push), устройство, чья подписка истекла по TTL (60
// дней), и подписка на календарный фид — она заводит профиль вовсе без push.
// ⚠️ Устройств в профиле НЕТ: связь односторонняя (sub.profile_id → профиль),
// ни UA, ни истории там не хранится. Поэтому владельца опознаём косвенно —
// пересечением набора с живыми подписками и кнопкой 🔗 на самом устройстве
// (дашборд показывает там первые 8 символов profile_id).
function profileWlSet(p) {
  const out = new Set();
  const wl = Array.isArray(p.watchlist) ? p.watchlist : [];
  for (const n of wl) out.add(bareCaseNumber(n));
  return out;
}
// Поиск по профилю — зеркало subMatches: id, номера дел, стороны и суд.
function profileMatches(p, q) {
  if (!q) return true;
  let hay = String(p.profile_id || "");
  for (const num of (Array.isArray(p.watchlist) ? p.watchlist : [])) {
    hay += " " + num;
    const c = casesMapGlobal.get(bareCaseNumber(num));
    if (c) hay += " " + c.plaintiff + " " + c.defendant + " " + c.court;
  }
  return hay.toLowerCase().indexOf(q) >= 0;
}
// Кто ведёт набор: подписка с наибольшим пересечением. Порог в 2 совпадения —
// одно общее дело бывает случайным (наборы разных юристов пересекаются).
function profileLikelyOwner(p, subs) {
  const mine = profileWlSet(p);
  if (mine.size < 2) return null;
  let best = null;
  for (const s of subs) {
    let hits = 0;
    for (const n of (Array.isArray(s.watchlist) ? s.watchlist : [])) {
      if (mine.has(bareCaseNumber(n))) hits++;
    }
    if (hits >= 2 && (!best || hits > best.hits)) best = { sub: s, hits: hits };
  }
  return best;
}
// ⚠️ Кнопок в <summary> нет намеренно: клик по кнопке переключал бы свёртку.
// Строки дел — readOnly: у профиля нет endpoint'а, и крестик «убрать номер»
// был бы мёртвым (handleAction выходит на «if (!endpoint) return»).
function orphanProfileHtml(p, isOpen) {
  const wl = Array.isArray(p.watchlist) ? p.watchlist : [];
  const short = escHtml(String(p.profile_id).slice(0, 8));
  const orphans = subOrphanCount(wl, casesMapGlobal);
  const owner = profileLikelyOwner(p, allSubs);
  // updated_at профиля — миллисекунды эпохи (LWW-штамп набора), relTime ждёт ISO.
  const updIso = p.updated_at ? new Date(p.updated_at).toISOString() : "";
  const cases = wl.length
    ? wl.map(function (num) { return caseRowHtml(num, casesMapGlobal, { readOnly: true }); }).join("")
    : '<div class="empty">Набор пуст — профиль создан, но ★ ни разу не ставили</div>';
  return '<details class="profile-group profile-group-orphan" data-profile-id="' + escHtml(p.profile_id) + '"' + (isOpen ? " open" : "") + '>'
    + '<summary class="profile-group-head">'
    +   '🔗 Профиль ' + short + ' · без push-устройств'
    +   (p.has_feed ? '<span class="badge badge-watch" title="Профиль ведёт подписку календаря «Мои заседания» — она заводится и без push">📅 календарь</span>' : '')
    +   (orphans ? '<span class="badge badge-run" title="Номера, которых нет ни в активных делах, ни в архиве">⚠ ' + orphans + '</span>' : '')
    +   '<span class="spacer"></span>'
    +   '<span class="sub-count">' + nPlural(wl.length, "дело", "дела", "дел") + '</span>'
    + '</summary>'
    + '<div class="profile-body">'
    +   '<div class="sub-kv">'
    +     '<span>Создан <b>' + escHtml(relTime(p.created_at)) + '</b></span>'
    +     '<span>★ менялись <b>' + escHtml(relTime(updIso)) + '</b></span>'
    +     (p.has_feed ? '<span>Календарь <b>' + escHtml(relTime(p.feed_token_created_at)) + '</b></span>' : '')
    +   '</div>'
    +   '<div class="profile-why">Push-устройств нет: на устройстве запрещены уведомления '
    +     '(★ синхронизируются и без них), либо push-подписка истекла (60 дней без входа), '
    +     'либо профиль заведён подпиской на календарь. Чьё это устройство — видно на нём '
    +     'самом: дашборд → кнопка 🔗 → «профиль ' + short + '».</div>'
    +   (owner
      ? '<div class="profile-guess">Набор пересекается с подпиской «'
        + escHtml(owner.sub.label || detectDevice(owner.sub.user_agent))
        + '» — ' + owner.hits + ' из ' + wl.length + '</div>'
      : '')
    +   cases
    + '</div>'
    + '</details>';
}
function renderSubsList() {
  const root = document.getElementById("root");
  const q = document.getElementById("subs-search").value.trim().toLowerCase();
  const visible = allSubs.filter(function (s) { return subMatches(s, q); });
  // Профиль-сирота — на который не ссылается НИ ОДНА подписка вообще (allSubs,
  // не visible: иначе поиск, спрятавший устройства профиля, показал бы живой
  // профиль строкой «без push-устройств»). Совпавшие с запросом считаем ДО
  // autoOpen — они участвуют в пороге авто-раскрытия наравне с подписками.
  const linkedProfiles = new Set();
  for (const s of allSubs) { if (s.profile_id) linkedProfiles.add(s.profile_id); }
  const orphanMatches = allProfiles.filter(function (p) {
    return !linkedProfiles.has(p.profile_id) && profileMatches(p, q);
  });
  // Найденное раскрываем сами, но только когда поиск ДЕЙСТВИТЕЛЬНО сузил
  // список: иначе буква «а» развернёт всех и вернёт простыню. Ручное
  // состояние (subsOpen) не трогаем — очистка поиска возвращает то, что
  // юрист раскрыл сам.
  const autoOpen = !!q && (visible.length + orphanMatches.length) <= 3;
  function cardHtml(s) {
    return renderCard(s, casesMapGlobal, lastPushesMap.get(s.endpoint), lastPushesGeneratedAt,
      autoOpen || subsOpen.has(s.endpoint), autoOpen);
  }
  // Группировка по профилю синхронизации: устройства одного юриста — рядом,
  // в рамке с шапкой профиля. Порядок групп = первое вхождение подписки в
  // отсортированном списке (owner выше — сортировка из render() наследуется).
  const groups = new Map(); // profile_id → [подписки]
  const singles = [];
  for (const s of visible) {
    if (s.profile_id) {
      if (!groups.has(s.profile_id)) groups.set(s.profile_id, []);
      groups.get(s.profile_id).push(s);
    } else {
      singles.push(s);
    }
  }
  let html = "";
  const seenProfiles = new Set();
  for (const s of visible) {
    if (!s.profile_id || seenProfiles.has(s.profile_id)) continue;
    seenProfiles.add(s.profile_id);
    const members = groups.get(s.profile_id);
    const p = profilesById.get(s.profile_id);
    const wlLen = p ? (p.watchlist || []).length
      : (Array.isArray(members[0].watchlist) ? members[0].watchlist.length : 0);
    const updated = p && p.updated_at ? relTime(new Date(p.updated_at).toISOString()) : "";
    html += '<div class="profile-group" data-profile-id="' + escHtml(s.profile_id) + '">'
      + '<div class="profile-group-head">🔗 Профиль ' + escHtml(String(s.profile_id).slice(0, 8))
      + ' · ' + nPlural(members.length, "устройство", "устройства", "устройств")
      + ' · ' + nPlural(wlLen, "дело", "дела", "дел")
      + (updated ? ' · обновлён ' + escHtml(updated) : '')
      + '</div>'
      + members.map(cardHtml).join("")
      + '</div>';
  }
  html += singles.map(cardHtml).join("");
  // Профили без единой живой подписки (устройство без push-разрешения, истёкшая
  // подписка, подписка на календарь): read-only свёртка с составом набора.
  // Поиск их тоже находит — по id, номерам дел и сторонам (profileMatches):
  // иначе номер, который ведёт только такой профиль, в админке не искался.
  for (const p of orphanMatches) {
    html += orphanProfileHtml(p, autoOpen || profilesOpen.has(p.profile_id));
  }
  root.className = "subs";
  root.innerHTML = html;
  if (!html) {
    root.innerHTML = '<div class="empty">' + (q ? "Ничего не найдено по запросу" : "Подписок нет.") + '</div>';
  }
  // Счётчик считает ПОДПИСКИ, а поиск показывает ещё и профили без устройств —
  // без приписки выдача «0 из 1» при четырёх видимых карточках противоречит себе.
  document.getElementById("subs-count").textContent =
    (q ? visible.length + " из " + allSubs.length : String(allSubs.length))
    + (q && orphanMatches.length ? " · +" + nPlural(orphanMatches.length, "профиль", "профиля", "профилей") : "");
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
    allProfiles = all.profiles || [];
    profilesById = new Map(allProfiles.map((p) => [p.profile_id, p]));
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
      + (allProfiles.length ? " · <b>" + allProfiles.length + "</b> " + plural(allProfiles.length, "профиль", "профиля", "профилей") : "")
      + (orphanWl ? " · <b>⚠ " + orphanWl + " нигде не найдено</b>" : "");
    // Тот же счётчик — в заголовке секции: сводка в шапке скрыта на мобильном
    // (.header-meta{display:none} ≤768px), и с телефона сироты не видны вовсе.
    document.getElementById("subs-orphans").innerHTML = orphanWl
      ? '<span class="badge badge-run" title="Номера из watchlist\\'ов, которых нет ни в активных делах, ни в архиве. Push по ним никогда не сработает — крестик в строке дела убирает номер.">⚠ ' + orphanWl + ' нигде не найдено</span>'
      : "";
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
// Запоминаем раскрытые подписки по КЛИКУ на строку, а не по событию toggle.
// ⚠️ Chrome шлёт toggle и при парсинге <details open> — то есть каждое
// присваивание innerHTML рассылает его по всем карточкам, отрендеренным
// открытыми. Гард по таймеру ненадёжен: эти задачи дренируются позже
// setTimeout(0), и авто-раскрытые поиском карточки записывались в subsOpen
// как «раскрытые вручную» — после очистки запроса оставались открытыми.
// Клик по <summary> парсер не генерирует, а клавиатурная активация (Enter /
// Space) даёт его сама — обходимся одним слушателем.
// Состояние читаем в setTimeout(0): переключение open — это default action,
// на момент обработки события оно ещё не применено.
document.getElementById("root").addEventListener("click", function (e) {
  const s = e.target.closest ? e.target.closest("summary") : null;
  if (!s) return;
  const det = s.parentElement;
  // Фильтр обязателен: внутренние details.fold («Последний push», «Дела»)
  // иначе писали бы мусор.
  if (!det) return;
  if (det.classList.contains("profile-group-orphan")) {
    const pid = det.getAttribute("data-profile-id");
    if (!pid) return;
    setTimeout(function () {
      if (det.open) profilesOpen.add(pid); else profilesOpen.delete(pid);
    }, 0);
    return;
  }
  if (!det.classList.contains("sub-card")) return;
  const ep = det.getAttribute("data-endpoint");
  if (!ep) return;
  setTimeout(function () {
    if (det.open) subsOpen.add(ep); else subsOpen.delete(ep);
  }, 0);
});

// ── Вкладки шапки ────────────────────────────────────────────────────────────
// Чипы #nav — настоящие вкладки: показана ровно одна секция, пульт остаётся
// сверху вне вкладок. Состояние — в hash (secret живёт в query, hash его не
// трогает). history.replaceState, а не pushState: вкладка — фильтр вида, а не
// шаг навигации; иначе «назад» листал бы вкладки вместо ухода со страницы и
// плодил копии URL с секретом в истории.
var TAB_DEFAULT = IS_OWNER ? "system" : "import";
// ⚠️ В hash пишем "tab-<id>", а НЕ голый id секции. Иначе Chrome после
// replaceState видит в документе элемент с этим id и выполняет отложенный
// «прыжок к фрагменту» уже после load: страница уезжала вниз на высоту
// скрытых панелей, а липкая шапка оказывалась за верхним краем. Элемента
// с id="tab-…" на странице нет (чипы носят id="nav-…"), прыгать некуда.
// Старый формат (#import) читаем тоже — сохранённая ссылка не должна
// ломаться, showTab тут же нормализует hash.
function tabFromHash() {
  var h = String(location.hash || "").replace(/^#/, "");
  return h.indexOf("tab-") === 0 ? h.slice(4) : h;
}
function tabAllowed(id) {
  var panel = document.getElementById(id);
  var chip = document.getElementById("nav-" + id);
  if (!panel || !chip) return false;
  // Роль: #llm и #subs у оператора скрыты CSS (реальный запрет — 403 на API).
  if (!IS_OWNER && panel.hasAttribute("data-owner-only")) return false;
  // Вкладка «Импорт» с появлением точечного добавления видна всем всегда;
  // проверка инлайн-скрытия оставлена на будущее (сейчас не срабатывает).
  if (chip.style.display === "none") return false;
  return true;
}
function showTab(id, opts) {
  if (!tabAllowed(id)) id = TAB_DEFAULT;
  var chips = document.querySelectorAll("#nav .chip-btn");
  for (var i = 0; i < chips.length; i++) {
    var on = chips[i].getAttribute("href") === "#" + id;
    chips[i].classList.toggle("active", on);
    chips[i].setAttribute("aria-selected", on ? "true" : "false");
    chips[i].tabIndex = on ? 0 : -1;
  }
  var panels = document.querySelectorAll("main .section");
  for (var j = 0; j < panels.length; j++) {
    panels[j].classList.toggle("is-tab-active", panels[j].id === id);
  }
  // Присваивать location.hash нельзя: браузер прыгнет к якорю под шапку.
  try { history.replaceState(null, "", "#tab-" + id); } catch (e) {}
  if (!opts || opts.scroll !== false) window.scrollTo(0, 0);
  if (!opts || !opts.silent) onTabShown(id);
}
// Открытие вкладки — повод освежить статику Pages (KV не трогаем): админку
// держат открытой сутками. Порог тот же, что у visibilitychange.
function onTabShown(id) {
  if (id === "system" && Date.now() - lastStaticLoadAt > STATIC_STALE_MS) {
    loadStaticData(!IS_OWNER);
  }
}
function initTabs() {
  var nav = document.getElementById("nav");
  // Делегирование строго на #nav: класс .chip-btn носит ещё и ссылка
  // «Открыть поиск по суду» внутри формы импорта — глобальный слушатель
  // перехватил бы переход на сайт суда.
  nav.addEventListener("click", function (e) {
    var chip = e.target.closest ? e.target.closest(".chip-btn") : null;
    if (!chip) return;
    e.preventDefault();
    showTab(chip.getAttribute("href").slice(1));
  });
  // Стрелки/Home/End — обязательный минимум для объявленного role="tablist".
  nav.addEventListener("keydown", function (e) {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight"
        && e.key !== "Home" && e.key !== "End") return;
    var open = [];
    var chips = document.querySelectorAll("#nav .chip-btn");
    for (var i = 0; i < chips.length; i++) {
      if (tabAllowed(chips[i].getAttribute("href").slice(1))) open.push(chips[i]);
    }
    if (!open.length) return;
    var cur = open.indexOf(document.activeElement);
    var next = e.key === "Home" ? 0
      : e.key === "End" ? open.length - 1
      : (cur + (e.key === "ArrowRight" ? 1 : open.length - 1) + open.length) % open.length;
    e.preventDefault();
    open[next].focus();
    showTab(open[next].getAttribute("href").slice(1));
  });
  window.addEventListener("hashchange", function () {
    showTab(tabFromHash() || TAB_DEFAULT, { scroll: false });
  });
  showTab(tabFromHash() || TAB_DEFAULT, { scroll: false, silent: true });
}
// Плитки пульта: одни переключают вкладку (data-goto), другие ведут наружу
// (data-href) — у дайджеста, лога прогона и расписания крона своих секций на
// странице нет. Токен, а не готовый URL в разметке: URL — константы страницы,
// территории отличаются.
(function () {
  document.querySelectorAll(".stat-card[data-goto], .stat-card[data-href]").forEach(function (t) {
    t.addEventListener("click", function () {
      const href = t.getAttribute("data-href");
      if (href === "digest") {
        window.open(DASHBOARD_URL + "?digest=open", "_blank", "noopener");
        return;
      }
      if (href === "run") {
        window.open(ghRunHref(), "_blank", "noopener");
        return;
      }
      if (href === "cron") {
        // Расписание автозапуска и «Run workflow» для полного обхода — там же.
        window.open("https://github.com/" + GH_REPO + "/actions/workflows/update_cases.yml",
          "_blank", "noopener");
        return;
      }
      const sel = t.getAttribute("data-goto");
      if (sel) showTab(sel.slice(1));
    });
  });
})();

// ── Секция «Импорт дел» (капчёвые суды; обе роли) ────────────────────────────
// Источник dropdown'а — cases.json: region.appeal_courts + region.fi_courts со
// search_gated=True. Апелляция идёт ПЕРВОЙ и закреплена (pinned) — с
// 25.08.2026 код закрыл и её (Свердловский облсуд), а суд этот один на
// территорию. Секция скрыта, если капчёвых судов в регионе нет вовсе
// (у ХМАО прячется сама).
var impCourts = [];            // [{name, domain, search_gated, srv_num, delo_id, pinned}]
// Домены апел-судов территории: по ним сводка результата говорит про ДЕЛА
// АПЕЛЛЯЦИИ, а не про иски банка (счётчики у каналов общие, смысл — разный).
var impAppealDomains = {};
var impCourtNameByDomain = {}; // домен → короткое имя (для журнала)
var acRegion = null;           // весь region-блок cases.json — точечному добавлению
var impPollTimer = null;
var impSelectedFile = null;    // файл на отправку (из input или drag-n-drop)
var impSending = false;        // идёт отправка/импорт — кнопка заблокирована
var impDetectedHosts = [];     // sudrf-хосты текущей вставки/файла (автоопределение суда)
var impDetectSeq = 0;          // защита от гонки async-чтения файла
var impCourtTouched = false;   // оператор выбирал суд сам (select/светофор) — не переключать молча
var impLastFreshMap = {};      // кэш карты import:last:* (перерисовка светофора без KV)
var impLastLogItems = [];      // кэш последних записей журнала (то же — для «моих судов»)
var impFreshAutoPicked = false; // светофор уже подставил самый просроченный суд
var impDetectedCaseLinks = 0;  // ссылок на карточки дел во вставке/файле
// Ключ суда в форме импорта — «домен|srv_num», а не голый домен (14.08.2026).
// На одном домене живут ДВЕ площадки: сам районный суд и его постоянное
// судебное присутствие (Камышловский + Пышма, Красноуфимский + Ачит на
// Урале). Прежний дедуп по домену выкидывал присутствие из выпадающего списка
// и светофора — его дела не импортировал никто, хотя в реестре региона оно
// стояло с 16.07.2026. Сам ДАМП по-прежнему уходит на сервер с голым доменом:
// фактическую площадку дела импортёр берёт из href карточек (_stamp_court_ids),
// а хост и delo_id у площадок совпадают.
function impCourtKey(c) { return c.domain + "|" + String(c.srv_num || 1); }
// Оператор по подписи выбирает, какой раздел сайта открывать: у апелляции своя
// картотека (delo_id=5), и «Свердловский областной суд» без пометки читался бы
// как суд 1-й инстанции.
function impCourtLabel(c) {
  return (c && c.name ? c.name : "") + (c && c.pinned ? " — апелляция" : "");
}
function impDomainOf(key) { return String(key || "").split("|")[0]; }
// Ссылка «Открыть поиск по суду». srv_num обязателен: голая ссылка уводила
// оператора на первую площадку домена, а часть судов реестра заведена ТОЛЬКО
// как srv_num=2 (Железнодорожный ЕКБ — у него на первой площадке уголовная
// картотека). delo_id=1540005 — гражданские дела 1-й инстанции, name_op=sf —
// форма поиска (та самая, что закрыта проверочным кодом).
function impCourtLink(key) {
  var dom = impDomainOf(key);
  var c = null;
  for (var i = 0; i < impCourts.length; i++) {
    if (impCourtKey(impCourts[i]) === key) { c = impCourts[i]; break; }
    if (!c && impCourts[i].domain === dom) c = impCourts[i];
  }
  // delo_id — у суда, а не константой: 1540005 это гражданские дела 1-й
  // инстанции, а у апелляции раздел свой (5), и жёсткая константа уводила бы
  // оператора в чужую картотеку.
  return "https://" + dom + "/modules.php?name=sud_delo&srv_num="
    + encodeURIComponent(String((c && c.srv_num) || 1))
    + "&delo_id=" + encodeURIComponent(String((c && c.delo_id) || 1540005))
    + "&name_op=sf";
}
// Синхронизация ссылки «Открыть сайт суда» с выбранным судом. На верхнем
// уровне, а не внутри loadImportCourts: её зовут change селекта, клик по
// светофору и «Повторить», а слушатель вешается один раз в init-блоке ниже
// (иначе каждый retry плодил бы дубликаты подписок).
function syncImportCourtLink() {
  var sel = document.getElementById("imp-court");
  if (sel.value) document.getElementById("imp-court-link").href = impCourtLink(sel.value);
}
function impShowAlert(html) {
  var el = document.getElementById("imp-alert");
  el.innerHTML = html;
  el.style.display = "";
}
function impHideAlert() {
  document.getElementById("imp-alert").style.display = "none";
}
async function loadImportCourts() {
  try {
    impHideAlert();
    const r = await fetch(CASES_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const j = await r.json();
    // Весь блок региона — точечному добавлению: проверка ссылок против
    // реестра (апелляция/кассация/чужой регион) и селект судов для номеров.
    acRegion = (j && j.region) || null;
    const fi = (acRegion && Array.isArray(acRegion.fi_courts)) ? acRegion.fi_courts : [];
    wwSetRegionCourts(fi);
    const gated = fi.filter(function (c) { return c && c.search_gated && c.domain; });
    // Апелляция под проверочным кодом (Свердловский облсуд с 25.08.2026) —
    // такой же капчёвый суд, только раздел другой. Закреплена ПЕРВОЙ и вне
    // фильтра «мои суды» (решение юриста): апел-суд на территории один на
    // всех операторов, и попади он в чужую подсеть — дамп не сделал бы никто.
    // ⚠️ В fi_courts её подмешивать нельзя: тот же массив кормит точечное
    // добавление и пометку «лист не нужен», а они ссылки апелляции отвергают.
    const ap = (acRegion && Array.isArray(acRegion.appeal_courts)) ? acRegion.appeal_courts : [];
    const gatedAppeal = ap.filter(function (c) {
      return c && c.search_gated && c.domain;
    }).map(function (c) {
      return { name: c.name, domain: c.domain, srv_num: c.srv_num || 1,
               delo_id: c.delo_id, search_gated: true, pinned: true };
    });
    impAppealDomains = {};
    ap.forEach(function (c) { if (c && c.domain) impAppealDomains[c.domain] = true; });
    fi.concat(ap).forEach(function (c) {
      if (c && c.domain && !impCourtNameByDomain[c.domain]) impCourtNameByDomain[c.domain] = c.name || c.domain;
    });
    acFillCourts(fi);
    acUpdateState(); // ссылки могли ждать реестра для клиентской проверки
    if (!gated.length && !gatedAppeal.length) {
      // Регион без капчёвых судов (ХМАО): дамповая часть не нужна, но
      // вкладка живёт — точечное добавление и общая история работают всем.
      var form = document.querySelector("#import .imp-form");
      if (form) form.style.display = "none";
      var ff = document.getElementById("imp-fresh-fold");
      if (ff) ff.style.display = "none";
      var grid = document.querySelector("#import .imp-grid");
      if (grid) grid.style.display = "block"; // осталась одна колонка (история)
      // logonly: светофора здесь нет, второй KV-list по import:last:* ни к
      // чему (лимит lists общий на аккаунт — инцидент 17.07.2026). Ошибку
      // из «тихого» режима дорисовываем сами, иначе висело бы «Загрузка…».
      loadImportLog(true).then(function (items) {
        if (items === null) {
          var hist = document.getElementById("imp-history");
          if (hist) { hist.className = ""; hist.innerHTML = loadErrorHtml("Журнал импортов не загрузился", "implog", ""); }
        }
      });
      return;
    }
    // Дедупа по домену БОЛЬШЕ НЕТ (14.08.2026): он выкидывал из списка
    // постоянные судебные присутствия (Пышма у Камышловского, Ачит у
    // Красноуфимского) — отдельные площадки того же сайта со своей
    // картотекой, и их дела не импортировал никто. Ключ строки — «домен|srv».
    impCourts = gatedAppeal.concat(gated);
    const sel = document.getElementById("imp-court");
    sel.innerHTML = impCourts.map(function (c) {
      return '<option value="' + escHtml(impCourtKey(c)) + '">' + escHtml(impCourtLabel(c)) + '</option>';
    }).join("");
    document.getElementById("imp-court-count").textContent = String(impCourts.length);
    syncImportCourtLink();
    // Плитка «Импорты» и светофор — только про регламент дампов капчёвых
    // судов; сама вкладка видна всем и без них.
    document.getElementById("tile-import-card").style.display = "";
    document.querySelector(".pult").classList.add("has-import");
    loadImportLog();
  } catch (e) {
    // cases.json недоступен: точечное добавление продолжает работать (без
    // клиентской проверки ссылок — её сделает сервер), но об урезанном
    // режиме честно говорим обеим ролям.
    impShowAlert('Не удалось загрузить список судов (cases.json). '
      + '<button class="btn-refresh" type="button" id="imp-retry">Повторить</button>');
  }
}
// Дыра в реестре региона («суд ДОМЕН не найден в реестре») повтором НЕ
// лечится: локальная машина пойдёт тем же кодом и отвергнет строку так же.
// Обещать ей дочитку — врать; построчный отчёт это уже говорит («карточку не
// дочитает никто, проверьте реестр региона»), и сводка обязана не спорить с ним.
var IMP_NO_COURT_RE = /не найден в реестре/;
function impNoCourtReason(item) {
  return IMP_NO_COURT_RE.test(String(item.card_fail_reason || ""));
}
// Иск банка потерян целиком — вернуть строку может только повторный разбор
// ТОГО ЖЕ дампа (он лежит в KV сутки, очередь резерва его и берёт).
function impRetryPromise(item) {
  if (impNoCourtReason(item)) return "проверьте реестр региона (повтор не поможет)";
  return "повторит локальная машина в течение дня";
}
// Дело заведено card-blind: его дочитает и повторный импорт, и ближайший
// прогон — у записи нет last_checked_at, FI-цикл возьмёт её первой.
function impRefillPromise(item) {
  if (impNoCourtReason(item)) return "проверьте реестр региона (повтор не поможет)";
  return "дочитает локальная машина или ближайший прогон";
}
// Кто отработал запись. Метка появляется ТОЛЬКО у резерва: облако — дефолт,
// и подписывать каждую строку истории «сделано облаком» значило бы засорить
// список ради нулевой новости. Метка делает проверяемым обещание сводки
// «повторит локальная машина»: видно, бралась машина за запись или нет.
function impSourceLabel(item) {
  if (item.source !== "mac") return "";
  var when = relTime(item.updated_at || item.ts);
  return "🖥 повтор с локальной машины" + (when ? " · " + when : "");
}
// Сводка дампового импорта. Раньше это была одна цепочка из 17 корзин через
// « · », где заведения, штатный отсев и провалы, требующие повтора, стояли
// вперемешку — оператор не мог за секунду ответить себе «получилось или
// переделывать». Теперь корзины разложены по трём смыслам, а вердикт отвечает
// на этот вопрос словами.
//   parts    — что завелось. ⚠️ Имя массива и первое выражение держит страж
//              test_bank_counters_reach_operator: сквозная проводка счётчиков
//              рвётся молча в любом из трёх звеньев (jq → whitelist → сводка).
//   problems — то, ради чего оператор возвращается к суду.
//   skipped  — штатный отсев, читается как «так и должно быть».
// Дамп апелляции (капчёвый апел-суд): счётчики у каналов ОБЩИЕ, а смысл
// разный — «карточка не открылась» здесь значит «потеряно дело апелляции», а
// не «иск банка». Отличаем по домену записи: реестр апел-судов территории
// админка уже держит (impAppealDomains).
function impIsAppeal(item) {
  return !!(item && item.court_domain && impAppealDomains[item.court_domain]);
}
function impResultParts(item) {
  var parts = ["+" + (item.added || 0) + " в картотеку"];
  var problems = [];
  var skipped = [];
  var isAp = impIsAppeal(item);
  // Дело уехало наверх по УЖЕ известному нам делу 1-й инстанции: не новое,
  // но и не «уже в базе» — апелляция добавлена в существующую запись.
  if (item.linked) parts.push(nPlural(item.linked,
    "дело связано с 1-й инстанцией", "дела связаны с 1-й инстанцией",
    "дел связано с 1-й инстанцией"));
  if (item.added_bank) parts.push("+" + item.added_bank + " в иски банка");
  if (item.promoted) parts.push(nPlural(item.promoted,
    "материал стал делом", "материала стали делами", "материалов стали делами"));
  // Карточки основной картотеки (16.08.2026): суд может отдать блок-страницу
  // вместо карточки, и дело заводится пустышкой. Раньше это было видно только
  // в свёртке «Отчёт построчно». Ставим сразу после заведений: это про те же
  // дела, а не корзина отсева.
  if (item.refilled) parts.push(nPlural(item.refilled,
    "карточка дочитана", "карточки дочитаны", "карточек дочитано"));
  // Давно решённые дела против банка (18.08.2026): заведены тихо сразу в
  // архивное окно и «новым иском» не объявляются — в «+N в картотеку» не
  // входят, оператор обязан видеть их отдельной корзиной.
  if (item.resolved_old) parts.push(nPlural(item.resolved_old,
    "давно решённое дело", "давно решённых дела", "давно решённых дел")
    + " — сразу в архив");
  // Потеря исков банка — САМОЕ важное в сводке и раньше называлась мягче
  // всего: «12 карточка не открылась» звучало технической мелочью, а
  // означало двенадцать НЕзаведённых дел (разбор 16.08.2026 — блок ГАС).
  // Правила приёма в трек решаются только по карточке, поэтому без неё
  // строка выбрасывается целиком.
  // ⚠️ Что делать дальше — с 23.08.2026 говорим правду: повтор УЖЕ стоит в
  // очереди резерва (ops/mac-local-run/import_queue.jq берёт запись по
  // fetch_fail/card_failed, агент ходит будни 10:30–18:30). Прежнее
  // «повторите дамп, когда суд отвечает» писалось 16.08, до появления
  // очереди, и звало оператора делать работу, которая сделается сама.
  // Ручной запасной выход оставлен: Mac бывает выключен, а в пятницу вечером
  // ближайший слот — только в понедельник.
  if (item.fetch_fail) {
    problems.push("⛔ " + item.fetch_fail
      + (isAp ? " дел апелляции не заведено " : " исков банка не заведено ")
      + "(карточка не открылась) — " + impRetryPromise(item)
      + "; если к вечеру не появятся, вставьте дамп заново");
  }
  if (item.card_failed) {
    // Причина (403 / страница защиты / проверочный код / заглушка) важнее
    // самого факта: по ней отличают «нас блокируют по адресу» от «портал
    // лёг». Без неё все четыре беды выглядели одинаково.
    problems.push("⚠ " + item.card_failed + " без карточки: "
      + (item.card_fail_reason || "суд не ответил")
      + " — " + impRefillPromise(item));
  } else if (item.card_fail_reason) {
    // Ответчиков в дампе не было, а иски банка отвалились ([FETCH FAIL]).
    problems.push("⚠ карточки не читались: " + item.card_fail_reason
      + " — " + impRetryPromise(item));
  }
  if (item.already) skipped.push(item.already + " уже в базе");
  if (item.excluded_result) skipped.push(item.excluded_result + " отсеяно по итогу");
  if (item.excluded_writ) skipped.push(item.excluded_writ + " ИЛ уже выдан");
  // Две корзины, а не сумма «уже в треке»: seen_cached с 18.08.2026 общий
  // для обеих веток (карточные отказы ответчик-ветки тоже кэшируются), и
  // подпись «в треке» врала бы про дела против банка.
  if (item.already_spent) skipped.push(item.already_spent + " отработавших (иски банка)");
  if (item.seen_cached) skipped.push(item.seen_cached + " из кэша отказов");
  if (item.bank_capped) skipped.push(item.bank_capped + " не влезло в потолок");
  if (item.skipped_role) skipped.push(item.skipped_role + " не наша роль (банк не ответчик)");
  if (item.not_accepted) skipped.push(item.not_accepted + " к производству не принято");
  if (item.no_link) skipped.push(item.no_link + " без ссылки");
  if (item.subsidiary) skipped.push(nPlural(item.subsidiary,
    "дочка Сбера", "дочки Сбера", "дочек Сбера"));
  return { parts: parts, problems: problems, skipped: skipped };
}
// Вердикт одной фразой: получилось / переделывать / пусто. Это первое (а часто
// и единственное), что оператор читает после отправки.
function impVerdict(item) {
  var added = (item.added || 0) + (item.added_bank || 0);
  var lost = item.fetch_fail || 0;
  var unread = item.card_failed || 0;
  var got = "заведено " + nPlural(added, "дело", "дела", "дел");
  if (lost || unread) {
    // Вердикт называет ИСХОД и того, кто доделает. Прежний «нужен повтор
    // дампа» ставил задачу оператору, хотя запись уже стоит в очереди
    // резерва; у дыры в реестре повтор бесполезен — там задача и правда его.
    var tail = impNoCourtReason(item)
      ? "нужен повтор дампа, но сперва проверьте реестр региона"
      : "повтор подхватит локальная машина";
    return { kind: "bad", text: added
      ? "Заведено " + nPlural(added, "дело", "дела", "дел")
        + ", но карточки открылись не все — " + tail
      : "Ничего не заведено — карточки не открылись; " + tail };
  }
  if (added) return { kind: "ok", text: "Готово: " + got };
  return { kind: "none", text: "Готово: новых дел нет — всё уже в базе" };
}
function impStatusBadge(status) {
  if (status === "done") return '<span class="badge badge-ok">готово</span>';
  if (status === "failed") return '<span class="badge badge-fail">сбой</span>';
  if (status === "started") return '<span class="badge badge-run">выполняется</span>';
  return '<span class="badge badge-skip">отправлено</span>';
}
// Однострочная склейка — для компактных строк «Истории импортов». Порядок
// корзин прежний (заведения → проблемы → отсев), чтобы старые записи журнала
// читались ровно так же, как читались вчера.
function impResultText(item) {
  if (item.kind === "case") return acResultText(item);
  if (item.kind === "writ_waiver") return wwResultText(item);
  if (item.status === "done") {
    var g = impResultParts(item);
    return g.parts.concat(g.problems, g.skipped).join(" · ");
  }
  if (item.status === "failed") return item.error || "ошибка — детали в журнале";
  return "";
}
// Развёрнутая сводка — для живого статуса сразу после отправки, где у строки
// есть вся ширина карточки. Порядок чтения: итог → что делать → детали.
function impResultHtml(item) {
  // Метка резерва нужна ВСЕМ каналам: у пачек своя ветка сводки, а вопрос
  // «кто это сделал» у них тот же.
  var src = impSourceLabel(item)
    ? '<div class="imp-sum-line imp-sum-dim">' + escHtml(impSourceLabel(item)) + "</div>"
    : "";
  if (item.kind === "case" || item.kind === "writ_waiver" || item.status !== "done") {
    return escHtml(impResultText(item)) + src;
  }
  var g = impResultParts(item);
  var v = impVerdict(item);
  var html = '<div class="imp-verdict is-' + v.kind + '">' + escHtml(v.text) + "</div>" + src;
  function line(cls, title, arr) {
    if (!arr.length) return "";
    return '<div class="imp-sum-line ' + cls + '"><b>' + title + "</b> "
      + escHtml(arr.join(" · ")) + "</div>";
  }
  html += line("imp-sum-bad", "Проблемы:", g.problems);
  // Единственная корзина «+0 в картотеку» — не новость: вердикт уже сказал,
  // что заводить было нечего, и строка под ним читалась как противоречие.
  var nothing = g.parts.length === 1 && !(item.added || 0);
  html += nothing ? "" : line("", "Заведено:", g.parts);
  html += line("imp-sum-dim", "Пропущено:", g.skipped);
  return html;
}
// Сводка закрытий «лист не нужен» (kind:"writ_waiver") — общий журнал с
// дампами. До 23.08.2026 своей ветки не было вовсе: запись уходила в дамповую
// и печатала «+0 в картотеку», хотя счётчики waived/updated/cleared доезжали
// до KV полностью — рвалось только отображение.
function wwResultText(item) {
  if (item.status === "failed") return item.error || "ошибка — детали в журнале";
  if (item.status !== "done") return "";
  var parts = [];
  if (item.waived) parts.push(nPlural(item.waived,
    "дело закрыто", "дела закрыты", "дел закрыто"));
  if (item.updated) parts.push(item.updated + " закрытых с обновлённой причиной");
  if (item.cleared) parts.push(nPlural(item.cleared,
    "закрытие отменено", "закрытия отменены", "закрытий отменено"));
  if (item.not_found) parts.push(item.not_found + " не найдено в треке");
  if (item.refused) parts.push(item.refused + " отказано");
  return parts.length ? parts.join(" · ") : "без изменений";
}
// Сводка записи точечного добавления (kind:"case") — общий журнал с дампами.
function acResultText(item) {
  if (item.status === "failed") return item.error || "ошибка — детали в журнале";
  if (item.status !== "done") return "";
  var parts = [];
  var added = (item.added_main || 0) + (item.added_bank || 0);
  if (added) parts.push("+" + added + " добавлено");
  if (item.reactivated) parts.push(item.reactivated + " возвращено из архива");
  if (item.promoted) parts.push(item.promoted + " материалов стали делами");
  // ПОТЕРЯ — перед штатным отсевом: это единственная корзина, ради которой
  // оператор возвращается к делу. В ссылочном режиме (капчёвые суды)
  // непрочитанная карточка убивает строку целиком — роль банка решается
  // только по ней. Повтор пачки уже стоит в очереди резерва (пачка живёт в
  // KV сутки), поэтому обещаем машину, а не задачу оператору.
  if (item.fetch_error) {
    parts.push("⛔ " + nPlural(item.fetch_error,
      "карточка не открылась", "карточки не открылись", "карточек не открылось")
      + " — повторит локальная машина");
  }
  if (item.already) parts.push(item.already + " уже в базе");
  if (item.not_found) parts.push(item.not_found + " не найдено");
  if (item.refused) parts.push(item.refused + " отказано");
  return parts.length ? parts.join(" · ") : "без изменений";
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
    // Точечные добавления (kind:"case") живут в том же журнале: вместо суда —
    // маркер канала и размер пачки (суд у пачки может быть разный построчно).
    // Суда у пультовых операций нет (у точечной пачки он построчно разный, у
    // пометок его нет вовсе) — вместо него маркер канала и размер пачки.
    // Без этой ветки writ_waiver печатался как безымянный суд «?».
    const court = it.kind === "case"
      ? ("📌 точечно · " + (it.items_count || "?") + " стр.")
      : it.kind === "writ_waiver"
      ? ("🚫 лист не нужен · " + nPlural(it.items_count || 0, "дело", "дела", "дел"))
      : (impCourtNameByDomain[it.court_domain] || it.court_domain || "?");
    // Построчный отчёт импортёра ([ADDED]/[ALREADY]/[SKIPPED ROLE]/…) хранится
    // в записи журнала — показываем свёрткой, как в live-блоке после отправки.
    var linesHtml = "";
    if (Array.isArray(it.lines) && it.lines.length) {
      linesHtml = '<details class="fold"><summary>Отчёт построчно ('
        + it.lines.length + ')</summary><div class="fold-body"><pre class="log-pre">'
        + it.lines.map(escHtml).join("\\n") + '</pre></div></details>';
    }
    return '<div class="imp-hist-item"><div class="imp-hist-row">' + impStatusBadge(it.status)
      + '<span class="imp-hist-court"><b>' + escHtml(court) + '</b></span>'
      + '<span>' + escHtml(it.operator || "без имени") + '</span>'
      + '<span class="imp-hist-meta">' + escHtml(relTime(it.ts)) + '</span>'
      + (impSourceLabel(it) ? '<span class="imp-hist-meta">' + escHtml(impSourceLabel(it)) + '</span>' : '')
      + (impResultText(it) ? '<span class="imp-hist-meta">' + escHtml(impResultText(it)) + '</span>' : '')
      + '</div>' + linesHtml + '</div>';
  }).join("");
}
async function loadImportLog(logOnly) {
  try {
    // logOnly (горячий поллинг ожидания импорта): просим только журнал —
    // Worker пропускает второй KV-list по import:last:*. Светофор свежести
    // при этом НЕ перерисовываем (d.last пуст), он остаётся с прошлого
    // полного обновления — экономим KV lists+reads на каждом тике.
    const r = await fetch("/admin/import-log?secret=" + encodeURIComponent(SECRET)
      + (logOnly ? "&logonly=1" : ""));
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    const items = Array.isArray(d.items) ? d.items : [];
    impLastLogItems = items;
    renderImportHistory(items);
    if (!logOnly) {
      // Кэшируем карту вечных ключей import:last:*: по ней светофор можно
      // перерисовать после успешного импорта, не тратя второй KV-list.
      impLastFreshMap = d.last || {};
      renderImportFreshness(items, impLastFreshMap);
    }
    return items;
  } catch (e) {
    // Раньше сбой молча возвращал null, и светофор с историей навсегда
    // оставались с разметочным «Загрузка…» — оператор не мог отличить
    // «данные едут» от «журнал не пришёл». На горячем поллинге (logOnly)
    // молчим по-прежнему: там свой индикатор ожидания и свои ретраи.
    if (!logOnly) {
      const fresh = document.getElementById("imp-freshness");
      const hist = document.getElementById("imp-history");
      if (fresh) { fresh.className = ""; fresh.innerHTML = loadErrorHtml("Журнал импортов не загрузился", "implog", e); }
      if (hist) { hist.className = ""; hist.innerHTML = loadErrorHtml("Журнал импортов не загрузился", "implog", e); }
    }
    return null;
  }
}
// Светофор свежести: когда каждый капчёвый суд импортировался в последний
// раз. Основной источник — карта last (вечные ключи import:last:<домен> на
// Worker'е); журнал (последние 50) подмешивается как фолбэк для импортов,
// прошедших до появления карты. Регламент — раз в неделю.
var IMP_FRESH_WARN_DAYS = 7;
var IMP_FRESH_STALE_DAYS = 14;
// ── «Мои суды» ───────────────────────────────────────────────────────────────
// Секрет оператора ОДИН на всех сопровождающих, и до 23.08.2026 каждый видел
// общую очередь на все капчёвые суды территории (на Урале их 56), а
// автоподстановка выбирала глобально самый просроченный суд — почти наверняка
// чужой. Набор живёт в localStorage: админка у каждой территории на своём
// origin, поэтому неймспейс ключу не нужен (так же плоско лежит admin_theme).
// ПУСТОЙ набор = прежнее поведение, все суды: до первого выбора не меняется
// ничего.
var MY_COURTS_KEY = "admin_my_courts";
var impMyCourts = null;
var impMyEdit = false;
function myCourts() {
  if (impMyCourts) return impMyCourts;
  var raw = [];
  try { raw = JSON.parse(localStorage.getItem(MY_COURTS_KEY) || "[]"); } catch (e) { raw = []; }
  impMyCourts = {};
  (Array.isArray(raw) ? raw : []).forEach(function (k) { impMyCourts[String(k)] = true; });
  return impMyCourts;
}
function myCourtsCount() { return Object.keys(myCourts()).length; }
function saveMyCourts() {
  try {
    localStorage.setItem(MY_COURTS_KEY, JSON.stringify(Object.keys(myCourts())));
  } catch (e) {}
}
// ── Карточки судов: последняя ПОПЫТКА дампа по каждому суду ─────────────────
// Светофор свежести такие импорты не засчитывает (зеркало серверного гейта
// ниже), и красный суд, импортированный сегодня, выглядел бы необъяснимо.
// Отсюда же растёт операторская плитка «Карточки судов»: для капчёвого суда
// чтение карточек — весь его канал мониторинга, а видно оно было только
// внутри отчёта одного импорта.
var impCardTrouble = {};
function collectCardTrouble(items) {
  var byDom = {};
  (items || []).forEach(function (it) {
    if (it.status !== "done" || !it.court_domain) return;
    if (it.kind === "case" || it.kind === "writ_waiver") return;
    var t = parseIso(it.updated_at || it.ts);
    if (isNaN(t)) return;
    if (byDom[it.court_domain] && byDom[it.court_domain].ts >= t) return;
    byDom[it.court_domain] = {
      ts: t,
      unread: (it.fetch_fail || 0) + (it.card_failed || 0),
      reason: it.card_fail_reason || "",
    };
  });
  impCardTrouble = {};
  Object.keys(byDom).forEach(function (d) {
    if (byDom[d].unread > 0) impCardTrouble[d] = byDom[d];
  });
}
// Плитка пульта «Карточки судов» — только у оператора (у владельца на её месте
// «Парсеры»: у него открытый поиск и есть доступ к логам прогонов).
function renderCardsTile(domains) {
  if (!document.getElementById("tile-cards-value")) return;
  // Домены — из той же подсети, по которой считают бейджи и плитка «Импорты»:
  // чужой лежащий суд не моя забота (с пустым набором «моих» это все суды).
  var bad = Object.keys(impCardTrouble).filter(function (d) {
    return !domains || domains[d];
  });
  if (!bad.length) {
    setTile("cards", "green", '<span class="dot dot-green"></span>читаются',
      "по последним импортам журнала");
    return;
  }
  var latest = bad[0];
  bad.forEach(function (d) {
    if (impCardTrouble[d].ts > impCardTrouble[latest].ts) latest = d;
  });
  var reason = impCardTrouble[latest].reason || "карточки не открылись";
  setTile("cards", "red",
    nPlural(bad.length, "суд не отдал", "суда не отдали", "судов не отдали"),
    escHtml(reason));
  // Причина в подписи обрезается тремя строками — полную оставляем в
  // подсказке: по ней отличают «нас блокируют» от «портал лёг».
  var card = document.getElementById("tile-cards-value").closest(".stat-card");
  if (card) card.title = "Последний отказ: " + reason;
}
function renderImportFreshness(items, lastMap) {
  var el = document.getElementById("imp-freshness");
  if (!el || !impCourts.length) return;
  collectCardTrouble(items);
  var byDomain = {};
  Object.keys(lastMap || {}).forEach(function (d) {
    var e = lastMap[d];
    var t = parseIso(e && e.ts);
    // added — оба трека: страница, с которой ушли только истцовые дела,
    // показывала «+0 из 24» и читалась как неудачный импорт. added_bank
    // появился в вечном ключе 14.08.2026, у прежних записей его нет.
    if (!isNaN(t)) byDomain[d] = {
      ts: t, operator: e.operator || "",
      added: (e.added || 0) + (e.added_bank || 0), rows: e.rows || 0,
    };
  });
  (items || []).forEach(function (it) {
    // kind:"case"/"writ_waiver" — пультовые операции: свежесть ДАМПОВОГО
    // регламента они не подтверждают (зеркало серверного гейта import:last
    // в worker.js).
    if (it.status !== "done" || !it.court_domain) return;
    if (it.kind === "case" || it.kind === "writ_waiver") return;
    // ⚠️ Второе условие серверного гейта — карточки. Без него защита,
    // написанная после инцидента 16.08.2026 (дамп Верх-Исетского завёл НОЛЬ:
    // 12 исков банка отвалились по блок-странице ГАС), обходилась прямо
    // здесь: import:last Worker такому импорту не пишет, зато запись журнала
    // со status:"done" — новее, перекрывала карту, и суд красился зелёным
    // «импортирован сегодня». Работа сделана, только когда карточки читались.
    if ((it.fetch_fail || 0) + (it.card_failed || 0) > 0) return;
    var t = parseIso(it.updated_at || it.ts);
    if (isNaN(t)) return;
    if (!byDomain[it.court_domain] || byDomain[it.court_domain].ts < t) {
      byDomain[it.court_domain] = { ts: t, operator: it.operator || "", added: it.added || 0, rows: it.rows || 0 };
    }
  });
  // Свежесть — по ДОМЕНУ: вечный ключ import:last:* серверный и площадок не
  // различает, поэтому у суда и его присутствия дата общая. Для регламента
  // это честно (оператор берёт обе выдачи за один заход на сайт суда), а
  // строки в списке всё равно свои — иначе присутствие невидимо.
  var mine = myCourts();
  var hasMine = myCourtsCount() > 0;
  var rows = impCourts.map(function (c) {
    var e = byDomain[c.domain];
    var days = e ? (Date.now() - e.ts) / 86400000 : Infinity;
    var level = days <= IMP_FRESH_WARN_DAYS ? 0 : days <= IMP_FRESH_STALE_DAYS ? 1 : 2;
    var key = impCourtKey(c);
    return { court: c, key: key, e: e, days: days, level: level,
             // Закреплённый суд (апелляция) считается «моим» у КАЖДОГО
             // оператора: он один на территорию, и в чужой подсети остался бы
             // без дампа вовсе.
             mine: !!c.pinned || !hasMine || !!mine[key],
             trouble: impCardTrouble[c.domain] || null };
  });
  // Просроченные и «ни разу» сверху, внутри уровня — самые давние первыми.
  function byQueue(a, b) {
    // Закреплённые (апелляция) — всегда первыми, даже когда свежие: это
    // отдельный раздел сайта, и в хвосте очереди о нём просто забывали бы.
    if (!!a.court.pinned !== !!b.court.pinned) return a.court.pinned ? -1 : 1;
    if (a.level !== b.level) return b.level - a.level;
    return b.days - a.days;
  }
  rows.sort(byQueue);
  // Бейджи и плитка «Импорты» считают по МОЕЙ подсети: чужая просрочка — не
  // моя рабочая очередь, а пульт должен показывать именно её.
  var counted = hasMine ? rows.filter(function (x) { return x.mine; }) : rows;
  var nRed = counted.filter(function (x) { return x.level === 2; }).length;
  var nYellow = counted.filter(function (x) { return x.level === 1; }).length;
  document.getElementById("imp-fresh-badges").innerHTML =
    (nRed ? '<span class="badge badge-fail">' + nRed + ' давно/ни разу</span> ' : "")
    + (nYellow ? '<span class="badge badge-run">' + nYellow + ' ⚠︎</span> ' : "")
    + '<span class="badge badge-ok">' + (counted.length - nRed - nYellow) + ' ok</span>';
  // Плитка «Импорты» в пульте — из тех же подсчётов, без лишних запросов.
  var countedDomains = {};
  counted.forEach(function (x) { countedDomains[x.court.domain] = true; });
  renderCardsTile(countedDomains);
  var scope = hasMine ? "моих судов" : "судов";
  if (nRed) {
    // Подписи короткие: плитка узкая (пульт из 5 колонок), и на десктопе
    // .stat-sub по-прежнему режет длинный текст многоточием.
    setTile("import", "red", nRed + " просрочено", "из " + counted.length + " " + scope + " · раз в неделю");
  } else if (nYellow) {
    setTile("import", "amber", nYellow + " скоро срок", "из " + counted.length + " " + scope + " · 8–14 дней");
  } else {
    setTile("import", "green", '<span class="dot dot-green"></span>всё свежо',
      "все " + counted.length + " " + scope + " моложе 7 дней");
  }
  renderMyBar(rows);
  el.className = "";
  if (impMyEdit) {
    // Режим выбора: список идёт РЕЕСТРОМ (по алфавиту), а не рабочей очередью
    // — искать свой суд глазами проще по имени, чем по просрочке.
    var alpha = rows.slice().sort(function (a, b) {
      if (!!a.court.pinned !== !!b.court.pinned) return a.court.pinned ? -1 : 1;
      return a.court.name.localeCompare(b.court.name, "ru");
    });
    el.innerHTML = alpha.map(freshEditRow).join("");
    return;
  }
  if (!hasMine) {
    el.innerHTML = freshList(rows);
  } else {
    var mineRows = rows.filter(function (x) { return x.mine; });
    var others = rows.filter(function (x) { return !x.mine; });
    el.innerHTML = (mineRows.length ? freshList(mineRows)
        : '<div class="health-more">В «моих судах» пусто — нажмите «Изменить».</div>')
      + (others.length
        ? '<details class="fold imp-others"><summary>Прочие суды ('
          + others.length + ')</summary><div class="fold-body">'
          + others.map(freshRow).join("") + "</div></details>"
        : "");
  }
  // Первый рендер светофора выбирает в форме самый просроченный суд: список
  // отсортирован рабочей очередью, а селект до этого показывал первый суд
  // реестра — оператор каждый раз перевыбирал вручную. С «моими судами» —
  // самый просроченный ИЗ МОИХ, иначе подставлялся бы чужой.
  if (!impFreshAutoPicked && !impCourtTouched && counted.length) {
    impFreshAutoPicked = true;
    var sel = document.getElementById("imp-court");
    if (sel && counted[0].key !== sel.value) {
      sel.value = counted[0].key;
      syncImportCourtLink();
      impRenderSelection();
    }
  }
}
// Список — первые FRESH_VISIBLE строк рабочей очереди + свёртка с остальными
// (зеркало карточки «Здоровье парсеров»: там тот же приём с VISIBLE = 8).
// ⚠️ Без потолка на настоящем реестре Урала (54 капчёвых суда) вкладка на
// телефоне вырастала до 6,3 экрана, из них 4,3 — один список: оператор
// пролистывал всю территорию до истории импортов. Проверено на боевом
// реестре 23.08.2026; на фикстуре из 14 судов проблема не проявлялась.
var FRESH_VISIBLE = 12;
function freshList(arr) {
  var head = arr.slice(0, FRESH_VISIBLE).map(freshRow).join("");
  var rest = arr.slice(FRESH_VISIBLE);
  if (!rest.length) return head;
  return head + '<details class="fold imp-others"><summary>Остальные '
    + rest.length + " " + plural(rest.length, "суд", "суда", "судов")
    + '</summary><div class="fold-body">'
    + rest.map(freshRow).join("") + "</div></details>";
}
// Одна строка светофора.
function freshRow(x) {
  var dotCls = x.level === 2 ? "dot-red" : x.level === 1 ? "dot-amber" : "dot-green";
  // «+7 из 24» — сколько дел завели из скольких сберовских строк было на
  // странице. rows появился в вечном ключе 02.08.2026: у импортов до этого
  // его нет, поэтому падаем обратно на голое «+7».
  var added = x.e && x.e.added
    ? " · +" + x.e.added + (x.e.rows ? " из " + x.e.rows : "")
    : "";
  var note = x.e
    ? relTime(new Date(x.e.ts).toISOString()) + (x.e.operator ? " · " + escHtml(x.e.operator) : "") + added
    : "ни разу не импортировался";
  // Почему суд красный, хотя импорт был: карточки не открылись, и регламент
  // такой импорт не засчитывает (гейт cardsUnread). Без пометки красный цвет
  // читался как ошибка светофора.
  // ⚠️ Отдельным элементом, а не хвостом меты: мета говорит про последний
  // ЗАСЧИТАННЫЙ импорт, а пометка — про последнюю ПОПЫТКУ, и это разные дни.
  // Склейка в одну фразу читалась так, будто карточки не открылись тогда же.
  // ⚠️ Слово «попытка» обязательно: у суда, который ни разу не импортировался
  // успешно, мета говорит «ни разу не импортировался», и без него две строки
  // читались как противоречие («ни разу» + «16 дн назад карточки не
  // читались»). Оно же чинит фразу, когда relTime отдаёт не «N дн назад», а
  // абсолютную дату: «попытка 23.07.2026: …» вместо «23.07.2026 карточки…».
  var trouble = x.trouble
    ? '<span class="imp-fresh-warn" title="'
      + escHtml(x.trouble.reason || "суд не отдал карточки")
      + '">⚠ попытка ' + escHtml(relTime(new Date(x.trouble.ts).toISOString()))
      + ': карточки не читались</span>'
    : "";
  return '<div class="health-row imp-fresh-row" role="button" tabindex="0"'
    + ' title="Выбрать этот суд в форме импорта" data-domain="' + escHtml(x.key) + '">'
    + '<span class="dot ' + dotCls + '"></span>'
    + '<span class="health-name">' + escHtml(impCourtLabel(x.court)) + '</span>'
    + '<span class="run-meta imp-fresh-meta">' + note + '</span>'
    + trouble
    + '</div>';
}
// Та же строка в режиме выбора «моих судов»: label + checkbox, чтобы работало
// нативно (клик по имени переключает галку) и не конфликтовало с делегатом
// выбора суда — он в режиме правки выключен.
function freshEditRow(x) {
  // Закреплённый суд из набора не выключается: снявший его оператор перестал
  // бы видеть апелляцию совсем, а ведёт её территория, а не человек.
  if (x.court.pinned) {
    return '<div class="health-row imp-fresh-row is-edit">'
      + '<span class="dot dot-green"></span>'
      + '<span class="health-name">' + escHtml(impCourtLabel(x.court)) + '</span>'
      + '<span class="run-meta">всегда в очереди</span></div>';
  }
  return '<label class="health-row imp-fresh-row is-edit">'
    + '<input type="checkbox" class="imp-my-box" data-key="' + escHtml(x.key) + '"'
    + (myCourts()[x.key] ? " checked" : "") + ">"
    + '<span class="health-name">' + escHtml(x.court.name) + '</span>'
    + '</label>';
}
// Панель управления набором «моих судов» над списком. ⚠️ Не в <summary>
// свёртки: кнопка внутри summary переключала бы саму свёртку (и вложенный
// интерактив — та же грабля, что в карточках подписчиков).
function renderMyBar(rows) {
  var bar = document.getElementById("imp-my-bar");
  if (!bar) return;
  var n = myCourtsCount();
  if (impMyEdit) {
    bar.innerHTML = '<span class="imp-hint">Отметьте суды, которые ведёте вы —'
      + ' очередь и пульт будут считать по ним. Список хранится в этом браузере.</span>'
      + '<span class="spacer"></span>'
      + '<button class="btn-outline btn-sm" type="button" data-act="clear">Снять все</button>'
      + '<button class="btn-primary btn-sm" type="button" data-act="done">Готово</button>';
    return;
  }
  bar.innerHTML = (n
      ? '<span class="imp-hint"><b>Мои суды: ' + n + '</b> из ' + rows.length + '</span>'
      : '<span class="imp-hint">Судов на территории: ' + rows.length
        + '. Отметьте свои — очередь станет вашей.</span>')
    + '<span class="spacer"></span>'
    + '<button class="btn-outline btn-sm" type="button" data-act="edit">'
    + (n ? "Изменить мои суды" : "Отметить мои суды") + "</button>";
}
// Клик по строке светофора = выбрать суд в форме импорта: светофор работает
// рабочей очередью («какой суд пора обновить — тот и импортирую»). Слушатели —
// делегированием в init-блоке ниже (список ререндерится на каждом поллинге).
function impPickCourt(key) {
  if (!key) return;
  var sel = document.getElementById("imp-court");
  // Автоопределение по вставке даёт голый ДОМЕН (хост из href карточек) —
  // площадку по нему не различить, выбираем первую строку этого домена.
  // Светофор и ручной выбор передают полный ключ «домен|srv».
  if (key.indexOf("|") < 0) {
    for (var i = 0; i < impCourts.length; i++) {
      if (impCourts[i].domain === key) { key = impCourtKey(impCourts[i]); break; }
    }
  }
  sel.value = key;
  syncImportCourtLink();
  impRenderSelection(); // заметка автоопределения зависит от выбранного суда
  var row = sel.closest(".imp-row");
  row.scrollIntoView({ behavior: "smooth", block: "center" });
  row.classList.remove("imp-flash");
  void row.offsetWidth; // рестарт CSS-анимации при повторном клике
  row.classList.add("imp-flash");
  setTimeout(function () { row.classList.remove("imp-flash"); }, 1700);
}
function impSetStatus(html) {
  document.getElementById("imp-status").innerHTML = html;
}
// Поллинг журнала по key дампа: «отправлено → выполняется → +N добавлено».
// Таймаут ~5 мин: очередь GitHub держит 1 running + 1 pending — третий запуск
// вытесняет ожидающий, дамп при этом живёт в KV 24 ч (можно повторить).
// Интервал 30 с + ?logonly=1 (только журнал, 1 KV-list вместо 2): каждый тик
// стоит KV-операций, а лимит lists free-tier — 1000/день на аккаунт
// (инцидент 17.07.2026: отладка импорта сожгла 50% дневного лимита).
function impElapsedText(startedAt) {
  var s = Math.max(0, Math.round((Date.now() - startedAt) / 1000));
  var m = Math.floor(s / 60);
  return m ? m + " мин " + (s % 60) + " с" : s + " с";
}
// Ожидание импорта — секундный тикер поверх 30-секундного поллинга. Тикер
// НЕ ходит в сеть: он перерисовывает только строку прошедшего времени. Без
// него после «страница принята» статус стоял немым ровно 30 секунд до первого
// тика поллинга, и оператор не понимал, ушло ли вообще (реже 30 с опрашивать
// нельзя — каждый тик стоит KV-операций, лимит lists общий на аккаунт).
var impWaitState = { st: "dispatched", startedAt: 0 };
var impWaitTimer = null;
function impRenderWaiting() {
  var st = impWaitState.st;
  impSetStatus(impStatusBadge(st) + ' <span class="dot dot-amber dot-pulse"></span> '
    + (st === "started" ? "выполняется" : "в очереди") + " · "
    + impElapsedText(impWaitState.startedAt));
}
function impStartTicker(startedAt) {
  impStopTicker();
  impWaitState = { st: "dispatched", startedAt: startedAt };
  impRenderWaiting();
  impWaitTimer = setInterval(function () {
    if (!impSending) { impStopTicker(); return; }
    impRenderWaiting();
  }, 1000);
}
function impStopTicker() {
  if (impWaitTimer) { clearInterval(impWaitTimer); impWaitTimer = null; }
}
function impPollResult(key, startedAt) {
  clearTimeout(impPollTimer);
  impPollTimer = setTimeout(async function () {
    const items = await loadImportLog(true);
    const mine = (items || []).find(function (it) { return it.uuid === key; });
    if (mine && (mine.status === "done" || mine.status === "failed")) {
      impStopTicker();
      impSetStatus(impStatusBadge(mine.status) + " " + impResultHtml(mine));
      const rep = document.getElementById("imp-report");
      if (Array.isArray(mine.lines) && mine.lines.length) {
        rep.innerHTML = '<details class="fold" open><summary>Отчёт построчно ('
          + mine.lines.length + ')</summary><div class="fold-body"><pre class="log-pre">'
          + mine.lines.map(escHtml).join("\\n") + '</pre></div></details>';
      }
      // Успех — очищаем поле вставки и файл. Оператор идёт очередью судов, а
      // форма оставалась заполненной прошлым дампом: следующий Ctrl+V клеился
      // в конец предыдущего, автоопределение видело «ссылки нескольких судов»
      // и блокировало отправку, а кнопки «очистить» на странице нет.
      // При failed вставку НЕ трогаем — дамп нужен для повторной попытки.
      if (mine.status === "done") {
        // Импорт доведён до конца — инструкция больше не разворачивается сама.
        try { localStorage.setItem("admin_imp_steps_seen", "1"); } catch (e) {}
        document.getElementById("imp-paste").innerHTML = "";
        impSetFile(null);          // внутри — impRenderSelection()
        impRunDetect();            // сбросить impDetectedHosts и заметку суда
        // Светофор свежести перерисовываем из УЖЕ полученного журнала поверх
        // КЭША карты import:last:* — только что импортированный суд иначе
        // висел бы красным до «Обновить». Полного захода в /admin/import-log
        // не делаем: это лишний KV-list, а свежая запись и так в journal'е.
        renderImportFreshness(items, impLastFreshMap);
      }
      impSending = false;
      impUpdateSendState();
      return;
    }
    if (Date.now() - startedAt > 5 * 60 * 1000) {
      impStopTicker();
      impSetStatus('<span class="badge badge-fail">нет ответа ~5 мин</span> '
        + 'Прогон мог быть вытеснен очередью GitHub — повторите отправку или сообщите владельцу.');
      impSending = false;
      impUpdateSendState();
      return;
    }
    // Ожидание до 5 минут: живой статус с прошедшим временем.
    impWaitState.st = (mine && mine.status === "started") ? "started" : "dispatched";
    impRenderWaiting();
    impPollResult(key, startedAt);
  }, 30000);
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
// «Что уйдёт на импорт»: единый источник — impSelectedFile (файл из input
// или drag-n-drop побеждает вставку), индикатор под полем говорит об этом.
function impFmtSize(n) {
  if (n >= 1024 * 1024) return (Math.round(n / 1024 / 1024 * 10) / 10) + " МБ";
  return Math.max(1, Math.round(n / 1024)) + " КБ";
}
function impSetFile(f) {
  impSelectedFile = f || null;
  if (!impSelectedFile) {
    try { document.getElementById("imp-file").value = ""; } catch (e) {}
  }
  impRenderSelection(); // чип файла сразу, автоопределение догонит async
  impRunDetect();
}
// ── Автоопределение суда по вставке/файлу ───────────────────────────────────
// Rich-paste абсолютизирует href карточек (https://<суд>/modules.php?…
// name=sud_delo…), файл «только HTML» из Chrome несёт маркер «saved from
// url=…». Относительные ссылки хоста не несут — тогда список пуст и
// автоопределение молчит (серверные проверки Worker'а и импортёра — финальные).
function impDetectDomains(html) {
  var hosts = [];
  function add(h) { h = h.toLowerCase(); if (hosts.indexOf(h) === -1) hosts.push(h); }
  var re = /https?:\\/\\/([a-z0-9][a-z0-9.-]*\\.sudrf\\.ru)\\/modules\\.php\\?[^"'\\s<>]*name=sud_delo/gi;
  var m;
  while ((m = re.exec(html)) !== null) add(m[1]);
  m = /saved from url=\\(\\d+\\)https?:\\/\\/([a-z0-9][a-z0-9.-]*\\.sudrf\\.ru)(?=[\\/\\s])/i.exec(html);
  if (m) add(m[1]);
  hosts.sort();
  return hosts;
}
function impCourtInDropdown(domain) {
  return impCourts.some(function (c) { return c.domain === domain; });
}
async function impRunDetect() {
  var seq = ++impDetectSeq;
  var html = "";
  if (impSelectedFile) {
    try { html = await impReadFile(impSelectedFile); } catch (e) { html = ""; }
  } else {
    html = document.getElementById("imp-paste").innerHTML || "";
  }
  if (seq !== impDetectSeq) return; // источник сменился, пока читали файл
  impDetectedHosts = html ? impDetectDomains(html) : [];
  // Сколько на странице ссылок именно на КАРТОЧКИ дел. Раньше индикатор
  // считал querySelectorAll("a[href]") — то есть меню, «хлебные крошки» и
  // пейджер вместе с делами, и на обычной выдаче показывал «ссылок на дела:
  // 137» при десятке реальных. Признак name_op=case есть и в абсолютных, и в
  // относительных href, поэтому работает и для файла, и для rich-paste.
  impDetectedCaseLinks = html ? (html.match(/name_op=case/gi) || []).length : 0;
  // Ровно один суд из списка импорта: подставляем сами, пока оператор не
  // выбирал вручную — ловит главный сценарий «оставил суд по умолчанию,
  // вставил выдачу другого». Ручной выбор автоматика не перебивает.
  // Сравниваем по ДОМЕНУ: у площадок одного суда хост общий, и переключать
  // выбранное присутствие на первую площадку из-за этого нельзя.
  if (impDetectedHosts.length === 1 && impCourtInDropdown(impDetectedHosts[0])
      && !impCourtTouched
      && impDomainOf(document.getElementById("imp-court").value) !== impDetectedHosts[0]) {
    impPickCourt(impDetectedHosts[0]);
  }
  impRenderSelection();
}
// Заметка под полем: что автоопределение думает о вставке. Кнопка «выбрать
// этот суд» слушается делегированием на #imp-selection (init-блок ниже).
function impDetectNote() {
  if (!impDetectedHosts.length) return "";
  if (impDetectedHosts.length > 1) {
    return "<b>⚠ в странице ссылки нескольких судов (" + escHtml(impDetectedHosts.join(", "))
      + ") — вставьте выдачу одного суда</b>";
  }
  var h = impDetectedHosts[0];
  var name = impCourtNameByDomain[h] || h;
  if (!impCourtInDropdown(h)) {
    return "<b>⚠ ссылки ведут в «" + escHtml(name) + "» (" + escHtml(h)
      + ") — этого суда нет в списке импортируемых</b>";
  }
  if (impDomainOf(document.getElementById("imp-court").value) === h) {
    return "определён суд: <b>" + escHtml(name) + "</b>";
  }
  return "<b>⚠ ссылки ведут в «" + escHtml(name) + "», а выбран другой суд</b> "
    + '<button class="btn-refresh" type="button" id="imp-detected-pick">выбрать этот суд</button>';
}
function impRenderSelection() {
  var el = document.getElementById("imp-selection");
  var paste = document.getElementById("imp-paste");
  var det = impDetectNote();
  // Одна и та же формулировка для файла и для вставки: считаем карточки дел.
  var cases = impDetectedCaseLinks
    ? "дел на странице: " + impDetectedCaseLinks
    : "<b>дел на странице: 0 — похоже, вставился простой текст, скопируйте страницу заново</b>";
  if (impSelectedFile) {
    el.innerHTML = '<span class="imp-file-chip">файл: ' + escHtml(impSelectedFile.name)
      + " (" + impFmtSize(impSelectedFile.size)
      + ') <button class="imp-file-clear" type="button" id="imp-file-clear" title="Убрать файл" aria-label="Убрать файл">' + ICON_X + '</button></span>'
      + '<span>отправится файл — вставленное в поле не используется</span>'
      + "<span>" + cases + "</span>"
      + (det ? "<span>" + det + "</span>" : "");
  } else if (paste.innerHTML.length) {
    el.innerHTML = "вставлено " + paste.innerHTML.length + " симв. · " + cases
      + (det ? "<span>" + det + "</span>" : "");
  } else {
    el.innerHTML = "";
  }
  impUpdateSendState();
}
function impUpdateSendState() {
  var has = !!impSelectedFile || document.getElementById("imp-paste").innerHTML.length > 0;
  var off = impSending || !has;
  document.getElementById("imp-send").disabled = off;
  // Подсказка «чего не хватает» — только пока кнопка заблокирована и ничего
  // не отправляется: сама по себе выключенная кнопка причины не объясняет.
  var hint = document.getElementById("imp-send-hint");
  if (hint) hint.style.display = (!has && !impSending) ? "" : "none";
}
async function impSend() {
  // На сервер уходит голый ДОМЕН: площадку дела импортёр берёт из href
  // карточек дампа (_stamp_court_ids), а Worker и его белый список судов
  // работают по домену. Ключ селекта — «домен|srv» (см. impCourtKey).
  const domain = impDomainOf(document.getElementById("imp-court").value);
  const name = document.getElementById("imp-name").value.trim();
  try { localStorage.setItem("admin_operator_name", name); } catch (e) {}
  let html = "";
  if (impSelectedFile) {
    // Файл (input или drag-n-drop) побеждает вставку — об этом честно
    // говорит индикатор impRenderSelection под полем.
    html = await impReadFile(impSelectedFile);
  } else {
    html = document.getElementById("imp-paste").innerHTML || "";
  }
  if (!domain) { impSetStatus('<span class="badge badge-fail">выберите суд</span>'); return; }
  if (!name) { impSetStatus('<span class="badge badge-fail">укажите ваше имя</span>'); return; }
  if (html.length < 1024) {
    impSetStatus('<span class="badge badge-fail">страница не вставлена или слишком короткая</span> '
      + 'Скопируйте страницу результатов целиком или приложите файл «только HTML».');
    return;
  }
  // Главная ошибка операторов — вставка простым текстом: ссылки на карточки
  // дел теряются, импортёру нечего забирать. Ловим до отправки. Проверяем
  // именно ссылки на КАРТОЧКИ (name_op=case), а не любой <a>: страница суда
  // полна навигации, и голый тест на <a> пропускал вставку без единого дела.
  if (!/name_op=case/i.test(html)) {
    impSetStatus('<span class="badge badge-fail">нет ссылок на дела</span> '
      + 'Похоже, вставился простой текст или не та страница. Скопируйте страницу результатов заново (выделением) или приложите файл «только HTML».');
    return;
  }
  // Дамп чужого суда: хост в абсолютных ссылках карточек обязан совпадать с
  // выбранным судом (Worker и импортёр перепроверяют то же серверно; при
  // относительных ссылках хостов нет — проверка молчит).
  const dumpHosts = impDetectDomains(html);
  if (dumpHosts.length && (dumpHosts.length > 1 || dumpHosts[0] !== domain)) {
    const foundNames = dumpHosts.map(function (h) { return impCourtNameByDomain[h] || h; }).join(", ");
    impSetStatus('<span class="badge badge-fail">страница другого суда</span> '
      + "Ссылки ведут в «" + escHtml(foundNames) + "», а выбран «"
      + escHtml(impCourtNameByDomain[domain] || domain)
      + "». Выберите суд по ссылкам или вставьте выдачу выбранного суда.");
    return;
  }
  impSending = true;
  impUpdateSendState();
  document.getElementById("imp-report").innerHTML = "";
  impSetStatus("отправляю страницу…");
  try {
    const r = await fetch("/admin/import-dump?secret=" + encodeURIComponent(SECRET), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ court_domain: domain, operator: name, html: html }),
    });
    const d = await r.json().catch(function () { return {}; });
    if (r.ok && d.ok) {
      var startedAt = Date.now();
      impStartTicker(startedAt);
      loadImportLog();
      impPollResult(d.key, startedAt);
    } else {
      impStopTicker();
      impSetStatus('<span class="badge badge-fail">✕</span> ' + escHtml(d.error || ("HTTP " + r.status)));
      impSending = false;
      impUpdateSendState();
    }
  } catch (e) {
    impStopTicker();
    impSetStatus('<span class="badge badge-fail">✕ сеть</span> ' + escHtml(String(e)));
    impSending = false;
    impUpdateSendState();
  }
}
// Одноразовая инициализация секции импорта. Всё содержимое, которое
// ререндерится (светофор, индикатор выбора), слушается ТОЛЬКО делегированием
// на постоянные контейнеры — прямые слушатели не пережили бы поллинг.
document.getElementById("imp-send").addEventListener("click", impSend);
document.getElementById("imp-court").addEventListener("change", function () {
  impCourtTouched = true; // ручной выбор — автоопределение его не перебивает
  syncImportCourtLink();
  impRenderSelection();   // заметка «а выбран другой суд» зависит от выбора
});
(function () {
  var fresh = document.getElementById("imp-freshness");
  fresh.addEventListener("click", function (e) {
    // В режиме правки «моих судов» строка — это label с галкой: выбирать по
    // ней суд в форме нельзя, иначе клик делал бы сразу два дела.
    if (impMyEdit) return;
    var row = e.target.closest(".imp-fresh-row");
    if (row) { impCourtTouched = true; impPickCourt(row.getAttribute("data-domain")); }
  });
  fresh.addEventListener("keydown", function (e) {
    if (impMyEdit) return;
    if (e.key !== "Enter" && e.key !== " ") return;
    var row = e.target.closest(".imp-fresh-row");
    if (row) { e.preventDefault(); impCourtTouched = true; impPickCourt(row.getAttribute("data-domain")); }
  });
  // Галки набора «мои суды»: пишем сразу, без кнопки «сохранить» — «Готово»
  // только выходит из режима. Перерисовки списка на каждый клик нет намеренно
  // (56 строк, и она сбрасывала бы позицию прокрутки под рукой).
  fresh.addEventListener("change", function (e) {
    var box = e.target.closest(".imp-my-box");
    if (!box) return;
    var key = box.getAttribute("data-key");
    if (box.checked) myCourts()[key] = true; else delete myCourts()[key];
    saveMyCourts();
  });
  var myBar = document.getElementById("imp-my-bar");
  myBar.addEventListener("click", function (e) {
    var btn = e.target.closest("[data-act]");
    if (!btn) return;
    var act = btn.getAttribute("data-act");
    if (act === "clear") {
      impMyCourts = {};
      saveMyCourts();
    }
    impMyEdit = (act === "edit");
    // Набор изменился — автоподстановка суда обязана переиграться: на первом
    // рендере она выбрала самый просроченный из ВСЕХ, а теперь очередь своя.
    // Ручной выбор при этом остаётся в силе (impCourtTouched не трогаем).
    if (!impMyEdit) impFreshAutoPicked = false;
    // Перерисовываем из кэша журнала: свежих данных правка набора не требует,
    // а лишний /admin/import-log — это KV-list (лимит общий на аккаунт).
    renderImportFreshness(impLastLogItems, impLastFreshMap);
  });
  var paste = document.getElementById("imp-paste");
  paste.addEventListener("input", impRunDetect);
  paste.addEventListener("dragover", function (e) {
    // preventDefault только для файлов: перетаскивание выделенного текста
    // в contenteditable должно остаться штатным поведением браузера.
    var t = e.dataTransfer && e.dataTransfer.types;
    if (t && Array.prototype.indexOf.call(t, "Files") !== -1) {
      e.preventDefault();
      paste.classList.add("imp-dragover");
    }
  });
  paste.addEventListener("dragleave", function () { paste.classList.remove("imp-dragover"); });
  paste.addEventListener("drop", function (e) {
    paste.classList.remove("imp-dragover");
    if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length) {
      e.preventDefault();
      impSetFile(e.dataTransfer.files[0]);
    }
  });
  document.getElementById("imp-file").addEventListener("change", function () {
    impSetFile(this.files && this.files.length ? this.files[0] : null);
  });
  document.getElementById("imp-selection").addEventListener("click", function (e) {
    if (e.target.closest("#imp-file-clear")) impSetFile(null);
    if (e.target.closest("#imp-detected-pick") && impDetectedHosts.length === 1) {
      impCourtTouched = true; // осознанный клик «выбрать этот суд»
      impPickCourt(impDetectedHosts[0]);
    }
  });
  document.getElementById("imp-alert").addEventListener("click", function (e) {
    if (e.target.closest("#imp-retry")) loadImportCourts();
  });
})();
impUpdateSendState();
try {
  document.getElementById("imp-name").value = localStorage.getItem("admin_operator_name") || "";
} catch (e) {}
// Свёрнутость светофора помним: он открыт по умолчанию (рабочая очередь), но
// оператор, у которого все суды зелёные, вправе его закрыть — и не открывать
// заново на каждой перезагрузке.
(function () {
  var f = document.getElementById("imp-fresh-fold");
  if (!f) return;
  try {
    if (localStorage.getItem("admin_imp_fresh_open") === "0") f.open = false;
  } catch (e) {}
  f.addEventListener("toggle", function () {
    try { localStorage.setItem("admin_imp_fresh_open", f.open ? "1" : "0"); } catch (e) {}
  });
})();
// Инструкция из шести шагов раскрыта, пока оператор ни разу не довёл импорт до
// «готово»: на первом заходе она и есть регламент, на двадцатом — шум.
try {
  if (!localStorage.getItem("admin_imp_steps_seen")) {
    var sf = document.getElementById("imp-steps-fold");
    if (sf) sf.open = true;
  }
} catch (e) {}

// ── Точечное добавление дел (блок «Добавить дела», обе роли) ─────────────────
// По одному делу в строке: номер («2-1234/2026») или ссылка на карточку sudrf
// (для капчёвых судов — единственный путь: код закрывает поиск, карточки
// открыты). Построчные предпроверки — зеркало Worker'а (handleAdminAddCase)
// и скрипта (classify_input в targeted_add.py); содержательные отказы (роль,
// дубль, «не найдено») делает скрипт на раннере, сюда они приходят журналом.
var AC_MAX_ITEMS = 20;
// JS-зеркало _FI_CASE_NUM_RE: буквенный/цифровой префикс + опциональный
// средний сегмент постоянного присутствия (Покачи «2-2-279/2026»).
var AC_NUM_RE = /^(?:[А-ЯA-Z]+|\\d+)-(?:\\d+-)?\\d+\\/\\d{4}$/;
var acSending = false;
var acPollTimer = null;
// Пачка может честно идти дольше дампового импорта: до 20 номеров × все
// открытые суды региона + очередь cases-data-write за ночным прогоном.
// Тик 60 с (не 30, как у дампов): каждый тик — KV-list, а лимит list'ов
// free-tier общий на аккаунт (инцидент 17.07.2026); 40 мин × 60 с ≤ 40 шт.
var AC_POLL_TICK_MS = 60 * 1000;
var AC_POLL_GIVEUP_MS = 40 * 60 * 1000;

function acFillCourts(fi) {
  var sel = document.getElementById("ac-court");
  if (!sel) return;
  var opts = ['<option value="">определить автоматически</option>'];
  (fi || []).forEach(function (c) {
    if (!c || !c.domain || c.search_gated) return; // по капчёвым номер не ищется
    opts.push('<option value="' + escHtml(c.domain + "|" + (c.srv_num || 1))
      + '">' + escHtml(c.name || c.domain) + '</option>');
  });
  var prev = sel.value;
  sel.innerHTML = opts.join("");
  if (prev) sel.value = prev; // перерисовка списка не сбрасывает выбор
}

function acClassifyLine(raw) {
  var s = raw.trim();
  if (!s) return { kind: "" };
  if (s.indexOf("://") !== -1 || s.toLowerCase().indexOf(".sudrf.ru") !== -1) {
    return { kind: "link", value: s };
  }
  var n = s.replace(/\\u00a0/g, " ").replace(/^\\s*(?:№|N)\\s*/i, "");
  n = n.split("~")[0].split("(")[0].replace(/\\s+/g, "");
  return AC_NUM_RE.test(n) ? { kind: "number", value: n } : { kind: "" };
}

// Клиентская проверка ссылки против реестра региона (acRegion из cases.json).
// Реестр не загрузился — молчим, авторитетную проверку сделает скрипт.
function acCheckLink(url) {
  var s = url.replace(/&amp;/g, "&");
  if (s.indexOf("://") === -1) s = "https://" + s;
  var u;
  try { u = new URL(s); } catch (e) { return "не удалось разобрать ссылку"; }
  var host = u.hostname.toLowerCase();
  if (host.slice(-9) !== ".sudrf.ru") return "это не адрес сайта суда (sudrf.ru)";
  if (!/case_id=\\d+/.test(u.search)) {
    return "в ссылке нет case_id — откройте саму карточку дела, а не страницу поиска";
  }
  if (!acRegion) return "";
  var i;
  var appeals = acRegion.appeal_courts || [];
  for (i = 0; i < appeals.length; i++) {
    if ((appeals[i].domain || "").toLowerCase() === host) {
      return "это карточка апелляции (" + (appeals[i].name || host)
        + ") — добавьте ссылку на дело в суде первой инстанции, апелляция подтянется сама";
    }
  }
  var cass = acRegion.cassation || {};
  if ((cass.domain || "").toLowerCase() === host) {
    return "это карточка кассации — она отслеживается автоматически по делу 1-й инстанции";
  }
  var fi = acRegion.fi_courts || [];
  var court = null;
  for (i = 0; i < fi.length; i++) {
    if ((fi[i].domain || "").toLowerCase() === host) { court = fi[i]; break; }
  }
  if (!court) return "суд " + host + " не из нашего региона";
  var dm = /[?&]delo_id=(\\d+)/.exec(u.search);
  if (dm && parseInt(dm[1], 10) !== (court.delo_id || 1540005)) {
    return "ссылка ведёт в другой раздел судопроизводства — откройте карточку в разделе гражданских дел";
  }
  return "";
}

// Разобрать textarea: валидные строки + построчные ошибки (с номером строки).
function acLines() {
  var raw = document.getElementById("ac-input").value || "";
  var lines = raw.split("\\n");
  var items = [];
  var errors = [];
  for (var i = 0; i < lines.length; i++) {
    var s = lines[i].trim();
    if (!s) continue;
    var c = acClassifyLine(s);
    if (!c.kind) {
      errors.push("строка " + (i + 1) + ": не похоже ни на номер дела, ни на ссылку на карточку");
      continue;
    }
    if (c.kind === "link") {
      var why = acCheckLink(s);
      if (why) { errors.push("строка " + (i + 1) + ": " + why); continue; }
    }
    items.push(s);
  }
  return { items: items, errors: errors };
}

function acSetStatus(html) {
  var el = document.getElementById("ac-status");
  if (el) el.innerHTML = html;
}

function acUpdateState() {
  var check = document.getElementById("ac-check");
  var btn = document.getElementById("ac-send");
  var hint = document.getElementById("ac-send-hint");
  if (!check || !btn) return;
  var st = acLines();
  var over = st.items.length > AC_MAX_ITEMS;
  var bits = [];
  if (st.items.length) {
    bits.push("к добавлению: " + st.items.length
      + (st.items.length === 1 ? " дело" : st.items.length < 5 ? " дела" : " дел"));
  }
  if (over) {
    bits.push('<span class="ac-err">не больше ' + AC_MAX_ITEMS
      + ' за раз — разбейте на части</span>');
  }
  st.errors.forEach(function (e) {
    bits.push('<span class="ac-err">' + escHtml(e) + "</span>");
  });
  check.innerHTML = bits.join(" · ");
  var ready = st.items.length > 0 && !st.errors.length && !over && !acSending;
  btn.disabled = !ready;
  if (hint) {
    hint.textContent = acSending ? ""
      : st.errors.length || over ? "исправьте строки с ошибками"
      : st.items.length ? "" : "введите номер дела или ссылку на карточку";
  }
}


// ── Закрытие дела: исполнительный лист не нужен ─────────────────────────
// Основной путь — ручной ввод номера + выбор суда. Очередь ожидания нужна
// только для счётчика и редких подсказок court_archived_at; выводить все
// сотни строк нет смысла. Признак очереди по-прежнему читаем готовым штампом
// first_instance.writ_awaited_since — свою копию Python-правила не заводим.
//
// ⚠️ Всё, что ниже, живёт на ВЕРХНЕМ уровне и не зависит от роли: карточка
// доступна обеим (пометку ставит и оператор). Первая версия собирала очередь
// внутри fetchAll — пайплайна подписчиков, который у оператора не вызывается
// и падает на owner-only /admin/data; это стоило трёх заходов подряд.
var wwRows = [];      // ждут лист (на экран попадут только строки-подсказки)
var wwWaived = [];    // уже закрыты вручную, в т.ч. в bank-архиве
var wwCourts = {};    // «домен|srv_num» → подпись; только суды из bank-трека
var wwRegionCourts = {}; // тот же ключ → точная подпись реестра (включая п.п.)
var wwBasket = {};    // лоток: ключ «домен|номер» → код причины
var wwClearQueue = [];
var wwModalKey = "";  // дело, открытое в модалке
var WW_HINT_VISIBLE = 12;
var WW_NUM_RE = /^(?:[А-ЯA-Z]+|\\d+)-(?:\\d+-)?\\d+\\/\\d{4}$/;
var WW_REASONS = [
  ["debt_paid", "долг погашен после решения"],
  ["not_requested", "лист решили не запрашивать"],
  ["other", "иное"]
];
function wwReasonLabel(code) {
  var p = WW_REASONS.filter(function (x) { return x[0] === code; })[0];
  return p ? p[1] : "иное";
}
function wwDaysSince(iso) {
  if (!iso) return null;
  var d = new Date(iso + "T00:00:00");
  if (isNaN(d)) return null;
  var t = new Date(); t.setHours(0, 0, 0, 0);
  return Math.round((t - d) / 86400000);
}
function wwShortCourt(name) {
  return String(name || "")
    .replace(/\\s*районный суд\\s*/i, " р/с ")
    .replace(/\\s*городской суд\\s*/i, " гор. ")
    .replace(/\\s*город(ского|ской)\\s*/i, " ")
    .trim();
}
function wwSrv(fi) { return String((fi && fi.srv_num) || 1); }
function wwKey(r) { return r.domain + "|" + r.srv + "|" + r.id; }
function wwItemFromKey(k, reason) {
  var p = String(k || "").split("|");
  return {
    case: p.slice(2).join("|"), court_domain: p[0] || "",
    court_srv_num: p[1] || "1", reason: reason || ""
  };
}
function wwRowByKey(k) {
  return wwRows.filter(function (r) { return wwKey(r) === k; })[0] || null;
}
function wwRegisterCourt(row) {
  var key = row.domain + "|" + row.srv;
  if (!wwCourts[key]) wwCourts[key] = row.court || row.domain;
}
function wwSetRegionCourts(fi) {
  wwRegionCourts = {};
  (fi || []).forEach(function (c) {
    if (!c || !c.domain) return;
    wwRegionCourts[c.domain + "|" + String(c.srv_num || 1)] = c.name || c.domain;
  });
  wwFillCourts();
}
function wwFillCourts() {
  var sel = document.getElementById("ww-court");
  if (!sel) return;
  var prev = sel.value;
  var labels = {};
  Object.keys(wwCourts).forEach(function (k) { labels[k] = wwCourts[k]; });
  // Реестр точнее bank-записи: «Камышловский районный суд (п.п. Пышма)»
  // иначе выглядел бы точным дублем основной площадки того же домена.
  Object.keys(wwRegionCourts).forEach(function (k) { labels[k] = wwRegionCourts[k]; });
  var keys = Object.keys(labels).sort(function (a, b) {
    return String(labels[a]).localeCompare(String(labels[b]), "ru");
  });
  sel.innerHTML = '<option value="">выберите суд</option>' + keys.map(function (k) {
    return '<option value="' + escHtml(k) + '">' + escHtml(labels[k]) + "</option>";
  }).join("");
  if (prev && labels[prev]) sel.value = prev;
}
function collectWaitRow(c, fromArchive) {
  var fi = (c && c.first_instance) || {};
  var row = {
    id: c.id || "", domain: String(fi.court_domain || "").trim(),
    srv: wwSrv(fi), court: fi.court || "", archAt: fi.court_archived_at || "",
    archived: !!fromArchive
  };
  if (!row.id || !row.domain) return;
  wwRegisterCourt(row);
  if (fi.writ_waived && fi.writ_waived.reason) {
    row.reason = fi.writ_waived.reason;
    row.at = fi.writ_waived.at || "";
    row.by = fi.writ_waived.by || "";
    wwWaived.push(row);
    return;
  }
  if (fromArchive) return;
  // Штамп очереди ставит прогон. ФОЛБЭК нужен, пока прогон после деплоя ещё
  // не отработал: без него карточка пустая и прячется. Приближение грубее
  // штампа («нет ни одного листа» вместо «нет листа НА ИСПОЛНЕНИЕ») — дела с
  // обеспечительными листами могут показаться лишний раз; тащить
  // классификацию листа третьей копией в админку было бы хуже.
  var since = fi.writ_awaited_since;
  var approx = false;
  if (!since && fi.legal_force_est && fi.writ_expected !== false
      && String(fi.status || "") === "Решено"
      && !(fi.writs || []).length) {
    since = fi.legal_force_est;
    approx = true;
  }
  if (!since) return;
  row.days = wwDaysSince(since);
  row.approx = approx;
  if (row.days === null) return;
  wwRows.push(row);
}
// Своя загрузка картотеки — по образцу loadHealth(). Отдельный fetch, а не
// врезка в чужой пайплайн: у владельца тот же URL уже лежит в HTTP-кэше
// браузера, у оператора это единственный путь.
async function loadWritQueue() {
  wwRows = []; wwWaived = []; wwCourts = {};
  var list = document.getElementById("ww-list");
  try {
    var r = await fetch(BANK_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    var d = await r.json();
    (d.cases || []).forEach(function (c) { collectWaitRow(c, false); });
    // Закрытые вручную дела сразу переезжают в bank-архив. Грузим его для
    // прозрачного аудита и кнопки отмены; 404 допустим на новой территории.
    try {
      var ar = await fetch(BANK_ARCHIVE_URL, { cache: "no-cache" });
      if (ar.ok) {
        var ad = await ar.json();
        (ad.cases || []).forEach(function (c) { collectWaitRow(c, true); });
      }
    } catch (archiveError) {
      console.warn("ww-archive:", archiveError);
    }
  } catch (e) {
    // 404 = территория без трека исков банка: карточку просто не показываем.
    var card = document.getElementById("ww-card");
    if (card) card.style.display = "none";
    if (String(e && e.message) !== "HTTP 404") console.warn("ww-card:", e);
    return;
  }
  wwFillCourts();
  renderWaitCard();
}
function renderWaitCard() {
  var card = document.getElementById("ww-card");
  if (!card) return;
  card.style.display = "";
  wwRows.sort(function (a, b) { return b.days - a.days; });
  var hints = wwRows.filter(function (r) { return r.archAt; });
  var shown = hints.slice(0, WW_HINT_VISIBLE);
  var approxN = wwRows.filter(function (r) { return r.approx; }).length;
  document.getElementById("ww-meta").textContent = "ждут ИЛ: " + wwRows.length
    + (approxN === wwRows.length && approxN ? " (примерно — до прогона)" : "");
  document.getElementById("ww-badges").innerHTML = hints.length
    ? '<span class="ww-hint-badge" title="Суд сдал дело в архив, а листа нет — вероятно, его не запрашивали">' + hints.length + " " + plural(hints.length, "подсказка", "подсказки", "подсказок") + "</span>"
    : "";
  var title = document.getElementById("ww-hints-title");
  if (title) title.textContent = hints.length
    ? "Подсказки суда: дело передано в архив, а ИЛ не выдан"
    : "Подсказки суда";
  var html = shown.map(function (r) {
    var k = wwKey(r);
    var picked = wwBasket[k];
    var hint = r.archAt
      ? '<span class="ww-hint" title="Суд сдал дело в архив ' + escHtml(r.archAt) + ', а листа нет — вероятно, его и не будет">&#9888;</span>'
      : "";
    var btn = '<button class="ww-mark" data-k="' + escHtml(k) + '" title="'
      + (picked ? "Помечено: " + escHtml(wwReasonLabel(picked)) + ". Нажмите, чтобы изменить"
                : "Закрыть это дело: лист не нужен")
      + '">' + (picked ? "&#10003;" : "&#43;") + "</button>";
    return '<div class="ww-row' + (picked ? " is-picked" : "") + '">'
      + '<span class="ww-num" title="' + escHtml(r.id) + '">' + escHtml(r.id) + "</span>"
      + '<span class="ww-court" title="' + escHtml(r.court) + '">' + escHtml(wwShortCourt(r.court)) + "</span>"
      + hint
      + '<span class="ww-days">' + r.days + "&nbsp;дн.</span>"
      + btn + "</div>";
  }).join("");
  if (hints.length > shown.length) {
    html += '<div class="health-more">Показаны первые ' + shown.length
      + " из " + hints.length + " подсказок</div>";
  }
  var list = document.getElementById("ww-list");
  list.className = "";
  list.innerHTML = html || '<div class="health-more">Сейчас подсказок нет. Любое известное дело можно закрыть по номеру в форме выше.</div>';
  renderWaivedList();
  updateWaitActions();
  wwManualUpdate();
}
function renderWaivedList() {
  var wrap = document.getElementById("ww-waived-wrap");
  if (!wrap) return;
  if (!wwWaived.length) { wrap.innerHTML = ""; return; }
  wwWaived.sort(function (a, b) { return String(b.at).localeCompare(String(a.at)); });
  var rows = wwWaived.map(function (r) {
    return '<div class="ww-row is-waived">'
      + '<span class="ww-num" title="' + escHtml(r.id) + '">' + escHtml(r.id) + "</span>"
      + '<span class="ww-court" title="' + escHtml(r.by ? "отметил " + r.by : "") + '">'
      + escHtml(wwReasonLabel(r.reason)) + "</span>"
      + '<span class="ww-days">' + escHtml(r.at || "") + "</span>"
      + '<button class="ww-mark ww-clear" data-k="' + escHtml(wwKey(r)) + '" title="Отменить закрытие и вернуть дело в активные">&#8635;</button>'
      + "</div>";
  }).join("");
  wrap.innerHTML = '<details class="fold"><summary>Закрыто вручную — ИЛ не нужен ('
    + wwWaived.length + ')</summary><div class="fold-body">' + rows + "</div></details>";
}
function updateWaitActions() {
  var n = Object.keys(wwBasket).length;
  var box = document.getElementById("ww-actions");
  var btn = document.getElementById("ww-send");
  if (!box || !btn) return;
  box.style.display = n ? "" : "none";
  btn.disabled = !n;
  btn.textContent = n
    ? "Закрыть " + n + " " + plural(n, "дело", "дела", "дел")
    : "Закрыть выбранные";
}
// Модалка выбора причины — по образцу #wl-modal (вкладка подписчиков):
// широкий контрол в строку карточки не влезает (колонка сетки ~460px), да и
// причина заслуживает осознанного выбора, а не случайного клика в списке.
function wwOpenModal(k) {
  var row = wwRowByKey(k);
  if (!row) return;
  wwModalKey = k;
  var dlg = document.getElementById("ww-modal");
  if (!dlg) return;
  document.getElementById("ww-modal-case").textContent =
    row.id + " · " + wwShortCourt(row.court) + " · ждёт " + row.days + " дн.";
  var cur = wwBasket[k] || "";
  document.getElementById("ww-modal-reasons").innerHTML = WW_REASONS.map(function (p, i) {
    return '<label class="ww-opt"><input type="radio" name="ww-reason-radio" value="' + p[0] + '"'
      + (cur === p[0] || (!cur && i === 0) ? " checked" : "") + "> " + p[1] + "</label>";
  }).join("");
  dlg.showModal();
}
function wwApplyModal() {
  var dlg = document.getElementById("ww-modal");
  var sel = dlg ? dlg.querySelector('input[name="ww-reason-radio"]:checked') : null;
  if (sel && wwModalKey) wwBasket[wwModalKey] = sel.value;
  if (dlg) dlg.close();
  renderWaitCard();
}
// ── Отправка закрытий ───────────────────────────────────────────────────────
// Подсказки можно закрыть пачкой; ручная форма отправляет одно дело. Оба пути
// сходятся здесь и используют относительный endpoint текущего Worker'а.
// Именно ошибочный вызов через API + ... (переменная нигде не объявлена) давал Safari
// «Can't find variable: API» ещё до сетевого запроса.
function wwNormalizeNumber(raw) {
  return String(raw || "").replace(/\\u00a0/g, " ")
    .replace(/^\\s*(?:№|N)\\s*/i, "").split("(")[0].replace(/\\s+/g, "");
}
function wwOperatorName() {
  var el = document.getElementById("ww-name");
  var name = (el && el.value || "").trim();
  try { name = name || localStorage.getItem("admin_operator_name") || ""; } catch (e) {}
  return name.trim();
}
function wwRememberName() {
  var el = document.getElementById("ww-name");
  var name = (el && el.value || "").trim();
  try { localStorage.setItem("admin_operator_name", name); } catch (e) {}
  var shared = document.getElementById("imp-name");
  if (shared) shared.value = name;
  wwManualUpdate();
}
function wwManualUpdate() {
  var btn = document.getElementById("ww-manual-send");
  var numEl = document.getElementById("ww-case");
  var courtEl = document.getElementById("ww-court");
  if (!btn || !numEl || !courtEl) return;
  var num = wwNormalizeNumber(numEl.value);
  btn.disabled = !(WW_NUM_RE.test(num) && courtEl.value && wwOperatorName());
}
async function wwPost(action, items, st, btn) {
  if (!items.length) return false;
  var name = wwOperatorName();
  if (!name) {
    if (st) st.textContent = "Укажите, кто закрывает дело.";
    return false;
  }
  try { localStorage.setItem("admin_operator_name", name); } catch (e) {}
  if (btn) btn.disabled = true;
  if (st) st.textContent = "Отправляем…";
  try {
    var r = await fetch("/admin/writ-waiver?secret=" + encodeURIComponent(SECRET), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action || "set", operator: name, items: items }),
    });
    var j = await r.json().catch(function () { return {}; });
    if (!r.ok || !j.ok) throw new Error(j.error || ("HTTP " + r.status));
    if (st) st.textContent = action === "clear"
      ? "Принято. Закрытие будет отменено после завершения задания."
      : "Принято. Дело будет перенесено в архив; ход виден в журнале импортов.";
    loadImportLog(true);
    return true;
  } catch (e) {
    if (st) st.textContent = "Не отправилось: " + (e && e.message ? e.message : e);
    return false;
  } finally {
    if (btn) btn.disabled = false;
    wwManualUpdate();
  }
}
async function wwSend(action) {
  var items = action === "clear"
    ? wwClearQueue.map(function (k) { return wwItemFromKey(k, ""); })
    : Object.keys(wwBasket).map(function (k) { return wwItemFromKey(k, wwBasket[k]); });
  var st = document.getElementById("ww-status");
  var btn = document.getElementById("ww-send");
  if (await wwPost(action, items, st, btn)) {
    wwBasket = {};
    wwClearQueue = [];
    updateWaitActions();
  }
}
async function wwManualSend() {
  var court = document.getElementById("ww-court");
  var numEl = document.getElementById("ww-case");
  var reason = document.getElementById("ww-reason");
  var st = document.getElementById("ww-manual-status");
  var btn = document.getElementById("ww-manual-send");
  var num = wwNormalizeNumber(numEl && numEl.value);
  if (!court || !court.value || !WW_NUM_RE.test(num)) {
    if (st) st.textContent = "Выберите суд и проверьте номер дела.";
    return;
  }
  var p = court.value.split("|");
  var item = { case: num, court_domain: p[0], court_srv_num: p[1] || "1",
               reason: (reason && reason.value) || "debt_paid" };
  if (await wwPost("set", [item], st, btn)) {
    numEl.value = "";
    wwManualUpdate();
  }
}
// Делегирование: список перерисовывается целиком при каждом изменении лотка.
document.addEventListener("click", function (e) {
  var t = e.target;
  if (!t || !t.closest) return;
  if (t.closest("#ww-manual-send")) { wwManualSend(); return; }
  if (t.closest("#ww-send")) { wwSend("set"); return; }
  if (t.closest("#ww-modal-ok")) { wwApplyModal(); return; }
  if (t.closest("#ww-modal-cancel")) {
    var dlg = document.getElementById("ww-modal");
    if (dlg) dlg.close();
    return;
  }
  if (t.closest("#ww-reset")) {
    wwBasket = {};
    renderWaitCard();
    var st = document.getElementById("ww-status");
    if (st) st.textContent = "";
    return;
  }
  // ⚠️ .ww-clear проверяем ДО .ww-mark: у кнопки возврата оба класса, иначе
  // «вернуть в очередь» открывало бы модалку пометки.
  var clr = t.closest(".ww-clear");
  if (clr) {
    wwClearQueue = [clr.getAttribute("data-k")];
    wwSend("clear");
    return;
  }
  var mark = t.closest(".ww-mark");
  if (mark) wwOpenModal(mark.getAttribute("data-k"));
});

// Ручная форма доступна и owner, и operator. Имя синхронизируем с полем
// вкладки «Импорт», чтобы один человек не представлялся странице дважды.
(function () {
  var num = document.getElementById("ww-case");
  var court = document.getElementById("ww-court");
  var name = document.getElementById("ww-name");
  if (!num || !court || !name) return;
  try { name.value = localStorage.getItem("admin_operator_name") || ""; } catch (e) {}
  num.addEventListener("input", wwManualUpdate);
  num.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !document.getElementById("ww-manual-send").disabled) wwManualSend();
  });
  court.addEventListener("change", wwManualUpdate);
  name.addEventListener("input", wwRememberName);
  var shared = document.getElementById("imp-name");
  if (shared) shared.addEventListener("input", function () {
    name.value = shared.value;
    wwManualUpdate();
  });
  wwManualUpdate();
})();

async function acSend() {
  if (acSending) return;
  var st = acLines();
  if (!st.items.length || st.errors.length || st.items.length > AC_MAX_ITEMS) return;
  // Имя — одно на всю вкладку (шапка секции): раньше полей было два.
  var name = (document.getElementById("imp-name").value || "").trim();
  if (!name) {
    acSetStatus('<span class="badge badge-fail">укажите ваше имя</span>');
    return;
  }
  try { localStorage.setItem("admin_operator_name", name); } catch (e) {}
  var courtDomain = "";
  var courtSrv = "";
  var sel = document.getElementById("ac-court");
  if (sel && sel.value) {
    var p = sel.value.split("|");
    courtDomain = p[0];
    courtSrv = p[1] || "";
  }
  acSending = true;
  acUpdateState();
  document.getElementById("ac-report").innerHTML = "";
  acSetStatus("отправляю…");
  try {
    var r = await fetch("/admin/add-case?secret=" + encodeURIComponent(SECRET), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        items: st.items, court_domain: courtDomain,
        court_srv_num: courtSrv, operator: name,
      }),
    });
    var d = await r.json().catch(function () { return {}; });
    if (r.ok && d.ok) {
      acSetStatus(impStatusBadge("dispatched") + " пачка принята, обработка в очереди…");
      acPollResult(d.key, Date.now());
    } else {
      acSetStatus('<span class="badge badge-fail">✕</span> '
        + escHtml((d && d.error) || ("HTTP " + r.status)));
      acSending = false;
      acUpdateState();
    }
  } catch (e) {
    acSetStatus('<span class="badge badge-fail">✕ сеть</span> ' + escHtml(String(e)));
    acSending = false;
    acUpdateState();
  }
}

function acPollResult(key, startedAt) {
  clearTimeout(acPollTimer);
  acPollTimer = setTimeout(async function () {
    var items = await loadImportLog(true);
    var mine = (items || []).find(function (it) { return it.uuid === key; });
    if (mine && (mine.status === "done" || mine.status === "failed")) {
      acSetStatus(impStatusBadge(mine.status) + " " + escHtml(acResultText(mine)));
      var rep = document.getElementById("ac-report");
      if (rep && Array.isArray(mine.lines) && mine.lines.length) {
        rep.innerHTML = '<details class="fold" open><summary>Отчёт построчно ('
          + mine.lines.length + ')</summary><div class="fold-body"><pre class="log-pre">'
          + mine.lines.map(escHtml).join("\\n") + '</pre></div></details>';
      }
      // Успех — чистим поле (следующая пачка не должна клеиться к прошлой);
      // при failed ввод сохраняем для повтора.
      if (mine.status === "done") {
        document.getElementById("ac-input").value = "";
      }
      acSending = false;
      acUpdateState();
      return;
    }
    if (Date.now() - startedAt > AC_POLL_GIVEUP_MS) {
      acSetStatus('<span class="badge badge-fail">нет ответа ~40 мин</span> '
        + 'Итог появится в «Истории импортов» — обновите страницу позже. '
        + 'Если его там нет, отправьте пачку заново: уже добавленные дела система отсеет сама.');
      acSending = false;
      acUpdateState();
      return;
    }
    var st = (mine && mine.status === "started") ? "started" : "dispatched";
    acSetStatus(impStatusBadge(st) + ' <span class="dot dot-amber dot-pulse"></span> '
      + (st === "started" ? "выполняется" : "в очереди") + " · " + impElapsedText(startedAt));
    acPollResult(key, startedAt);
  }, AC_POLL_TICK_MS);
}

// Инициализация блока точечного добавления.
(function () {
  var input = document.getElementById("ac-input");
  if (!input) return;
  input.addEventListener("input", acUpdateState);
  document.getElementById("ac-court").addEventListener("change", acUpdateState);
  document.getElementById("ac-send").addEventListener("click", acSend);
  acUpdateState();
})();

// Плитка «Дайджест» из публичного last_digest.json. Оператору это
// единственный путь (полный render() ему недоступен, /admin/data → 403);
// владельцу — точечное обновление плитки без похода в KV. Push-агрегат
// берём из последнего успешного render() (глобали), иначе перерисовка
// затёрла бы подпись плитки «Автозапуск» пустым значением.
async function loadDigestTile() {
  try {
    const r = await fetch(DIGEST_URL, { cache: "no-cache" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    renderDigestTile(await r.json(), lastPushesMap, lastPushesGeneratedAt);
  } catch (e) {
    setTile("digest", "gray", "—", "last_digest.json недоступен");
  }
}

// Когда последний раз тянули статику GitHub Pages (health / bank / digest).
// По этой метке решаем, стоит ли освежать данные при возврате на вкладку.
let lastStaticLoadAt = 0;
const STATIC_STALE_MS = 10 * 60 * 1000;

// Статика Pages: ни одной KV-операции, поэтому её безопасно перезагружать и
// по возврату на вкладку. /admin/data и /admin/import-log сюда НЕ входят —
// они ходят в KV (инцидент 17.07.2026, лимит бесплатного тарифа общий).
// withDigest=false там, где плитку дайджеста и так перерисует render()
// (владелец): иначе гонка двух рендеров затирала бы push-агрегат в подписи
// плитки «Автозапуск» пустым значением.
function loadStaticData(withDigest) {
  lastStaticLoadAt = Date.now();
  // ⚠️ loadWritQueue — БЕЗ гарда IS_OWNER: пометку «лист не нужен» ставит и
  // оператор (о погашении долга узнаёт тот, кто ведёт дело). Раньше очередь
  // собиралась внутри fetchAll — пайплайна ПОДПИСЧИКОВ, который у оператора
  // не вызывается вовсе и вдобавок падает на owner-only /admin/data. Это
  // стоило трёх заходов подряд; теперь карточка ни от чего чужого не зависит.
  const jobs = [loadHealth(), loadWritQueue()];
  // Иски банка — не операторский трек: карточка у него скрыта, и файл (на
  // Урале — сотни килобайт) не запрашиваем вовсе, а не «грузим и прячем».
  if (IS_OWNER) jobs.push(loadBankParse());
  // Плитка «Дайджест» у оператора тоже скрыта — тянуть last_digest.json ему
  // не за чем.
  if (withDigest && IS_OWNER) jobs.push(loadDigestTile());
  return jobs;
}

async function refreshAll(btn) {
  if (btn) { btn.disabled = true; btn.setAttribute("aria-busy", "true"); }
  const wlModal = document.getElementById("wl-modal");
  const renderable = IS_OWNER && !(wlModal && wlModal.open);
  const jobs = [loadGhRuns(), loadRunProgress()].concat(loadStaticData(!renderable));
  // «Обновить» чинит неудачную первую загрузку списка судов (для региона
  // без капчёвых судов это лишний fetch cases.json — безвредно).
  // Журнал импортов — только там, где секция вообще есть: два KV-list на
  // территории без капчёвых судов (ХМАО) были бы платой ни за что.
  if (impCourts.length) jobs.push(loadImportLog());
  else jobs.push(loadImportCourts());
  // render() перерисовывает #root целиком: при открытой модалке watchlist
  // это выбило бы правки из-под рук — тогда плитку дайджеста тянем статикой.
  if (renderable) {
    jobs.push(render(true));
    // Рейтинг перезапрашиваем только если он уже открыт: свёрнутый блок
    // тянуть с внешнего API незачем.
    const llmFold = document.getElementById("llm-top-fold");
    if (llmFold && llmFold.open) { llmTopLoaded = false; jobs.push(loadLlmTop()); }
  }
  try {
    await Promise.allSettled(jobs);
  } finally {
    if (btn) { btn.disabled = false; btn.removeAttribute("aria-busy"); }
  }
}

// Делегирование на документ: и «Повторить» в блоках ошибок, и крестик у
// вспышки-ошибки живут в узлах, которые постоянно перерисовываются.
document.addEventListener("click", function (e) {
  if (!e.target.closest) return;
  const x = e.target.closest("[data-flash-x]");
  if (x) {
    const f = x.closest(".action-flash");
    if (f) { f.textContent = ""; f.className = "action-flash"; }
    return;
  }
  const b = e.target.closest("[data-retry]");
  if (!b) return;
  const k = b.getAttribute("data-retry");
  if (k === "health") loadHealth();
  else if (k === "bank") loadBankParse();
  else if (k === "runprog") loadRunProgress();
  // Полный (не logonly) заход осознанно: сбой первой загрузки оставляет без
  // данных И светофор свежести, а его чинит только карта import:last:*.
  // Это редкий ручной клик, а не тик поллера — два KV-list допустимы.
  else if (k === "implog") loadImportLog();
});

// ⚠️ initTabs() именно здесь, а не на месте определения: onTabShown читает
// lastStaticLoadAt (let ниже по файлу) — вызов раньше объявления упал бы в TDZ.
// Скрипт синхронный и стоит в конце body, коррекция по hash успевает до
// первой отрисовки.
initTabs();
loadGhRuns();
loadRunProgress();    // разовый GET, внутри сам выходит у оператора
loadImportCourts();   // журнал импортов тянет он сам — только если есть gated-суды
// Плитку дайджеста владельцу рисует render(), оператору она не нужна вовсе.
loadStaticData(false);
// Owner-данные (подписки) оператору не грузим: эндпоинты всё равно ответят
// 403, а секции скрыты. render() заодно рисует плитку дайджеста вместе с
// push-агрегатом — потому её и не тянем статикой выше.
// Рейтинг LLM не грузим вовсе: он ленивый (раскрытие свёртки / выбор
// openrouter в форме) — внешнему API незачем отвечать на каждый заход.
if (IS_OWNER) {
  const llmFold = document.getElementById("llm-top-fold");
  if (llmFold) llmFold.addEventListener("toggle", function () { if (llmFold.open) loadLlmTop(); });
  render();
}
// Свёрнутая/фоновая вкладка не должна поллить: гасим самоперевзводящийся
// поллер gh-runs при уходе со вкладки, будим при возврате. Забытая открытая
// админка иначе тихо жгла бы Worker-инвокации/GitHub PAT сутками. Браузер
// такие setTimeout лишь троттлит, но не гасит.
document.addEventListener("visibilitychange", function () {
  if (document.hidden) {
    clearTimeout(ghTimer);
    return;
  }
  loadGhRuns();
  // Вкладку часто оставляют открытой на ночь: раньше возврат обновлял ТОЛЬКО
  // плитку прогона, а здоровье парсеров, дайджест и отчёт по искам банка
  // оставались вчерашними — вместе с метками «5 ч назад», посчитанными в
  // момент загрузки. Тянем только статику Pages (KV не трогаем).
  if (Date.now() - lastStaticLoadAt > STATIC_STALE_MS) loadStaticData(true);
});
</script>
</body></html>`;
}
