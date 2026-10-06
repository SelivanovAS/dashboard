"""
Стражи офлайн-режима PWA (service-worker.js + app.js + HTML + шрифты).

Инцидент 15.08.2026. Юрист без сети открывал PWA: долгий белый экран, затем
4 демо-дела вместо сохранённых. Три независимых дефекта:
1. Кэш данных был версионированным (CACHE_NAME) — правка фронта = обязательный
   бамп CACHE_VERSION (39 за 30 дней), и activate сносил данные вместе с кодом.
2. Ни одного дедлайна в SW + render-blocking Google Fonts + оба экрана скрыты
   в разметке → до запуска app.js буквально белый лист.
3. loadFromSheet на любую ошибку подставлял DEMO_CSV: из 8 демо-дел четыре
   старше ARCHIVE_DAYS и скрыты фильтром «all» — на экране ровно 4 чужих дела.

Что охраняем (правка 15.08.2026):
- DATA_CACHE не версионируется и переживает деплой; ownVersion его щадит,
  как и кэши соседней территории (ХМАО и Урал на одном origin github.io);
- activate мигрирует данные из старых кэшей ДО их удаления;
- у каждой стратегии есть дедлайн (гонка промисов, не AbortController —
  fetch(request,{signal}) пересобирает navigate-запрос в same-origin);
- cache.put живёт только в cachePutSafe: отказ записи (квота) не должен
  превращать валидный сетевой 200 в синтетический 503;
- демо-данные не подставляются автоматически — офлайн без кэша даёт честный
  экран «Нет сохранённых данных»;
- спиннер виден с первого кадра, сторож белого экрана снимается show*-функциями;
- шрифты свои (woff2 в fonts/), Google Fonts не запрашивается;
- окно умеет читать Cache Storage напрямую (страховка от 503 SW), имя кэша —
  дословное зеркало формулы SCOPE_NS из SW.

Запуск: python3 -m pytest scripts/tests/test_frontend_offline.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))

NODE = shutil.which("node")


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"(?:async\s+)?function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _strip_line_comments(src: str) -> str:
    """Убирает //-комментарии: проверки «паттерн есть/нет в КОДЕ» не должны
    срабатывать на пояснительный текст (комментарии в проекте подробные и
    называют вещи своими именами)."""
    return re.sub(r"^\s*//.*$|(?<=[;{})\s])//[^\n]*", "", src, flags=re.M)


# ===== 1. Жизненные циклы кэшей разведены =====

def test_data_cache_is_not_versioned():
    sw = _read("service-worker.js")
    m = re.search(r"const DATA_CACHE = ([^;]+);", sw)
    assert m, "DATA_CACHE не объявлен."
    assert "CACHE_VERSION" not in m.group(1), (
        "DATA_CACHE версионирован — каждый бамп CACHE_VERSION снова будет "
        "сносить офлайн-данные юриста (инцидент 15.08.2026: 39 бампов за "
        "30 дней = обнуление датасета чаще раза в день)."
    )
    code = re.search(r"const CACHE_NAME = ([^;]+);", sw)
    assert code and "CACHE_VERSION" in code.group(1), (
        "CACHE_NAME обязан остаться версионированным — код обновляется атомарно."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_own_version_predicate_spares_data_and_neighbours():
    """Предикат ownVersion удаляет старые версионированные кэши СВОЕЙ
    территории и не трогает ни неверсионированный DATA_CACHE, ни кэши
    соседней территории (общий origin github.io)."""
    sw = _read("service-worker.js")
    m = re.search(r"const ownVersion = \(k\) => \{[\s\S]*?\n  \};", sw)
    assert m, "ownVersion не найден (или сменил форму — обнови регулярку)."
    body = m.group(0)
    script = """
    const SCOPE_NS = 'dashboard';
    %s
    const cases = {
      'sber-jurist-dashboard-data': false,            // свои данные — жить
      'sber-jurist-dashboard-v164': true,             // свой старый код — удалить
      'sber-jurist-fonts-dashboard-v164': true,       // свой старый шрифтовой — удалить
      'sber-jurist-dashboard-ural-data': false,       // сосед: данные — жить
      'sber-jurist-dashboard-ural-v107': false,       // сосед: код — жить
      'sber-jurist-fonts-dashboard-ural-v99': false,  // сосед: шрифты — жить
    };
    const out = {};
    for (const k of Object.keys(cases)) out[k] = ownVersion(k);
    console.log(JSON.stringify({out, expected: cases}));
    """ % body
    res = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    data = json.loads(res.stdout)
    assert data["out"] == data["expected"], (
        "ownVersion ведёт себя не так: либо сносит данные/соседа, либо "
        f"перестал чистить своё старьё. Разница: {data}"
    )


def test_activate_migrates_before_delete():
    sw = _read("service-worker.js")
    act = re.search(r"addEventListener\('activate',[\s\S]*?\n\}\);", sw)
    assert act, "Обработчик activate не найден."
    body = act.group(0)
    mig = body.find("migrateDataCache")
    delete = body.find("caches.delete")
    assert mig != -1, (
        "activate больше не мигрирует данные из старых кэшей — у уже "
        "установленных PWA офлайн-датасет пропадёт при переходе."
    )
    assert delete != -1 and mig < delete, (
        "Миграция обязана идти ДО удаления старых кэшей."
    )
    mig_fn = _fn_src(sw, "migrateDataCache")
    assert "SCOPE_PATH" in mig_fn, (
        "Миграция без гарда SCOPE_PATH утащит записи соседней территории "
        "(легаси-кэши до v107 общие на весь origin)."
    )
    assert "isDataRequest" in mig_fn
    assert re.search(r"if \(await data\.match\(req\)\) continue;", mig_fn), (
        "Миграция перетирает свежие записи DATA_CACHE старьём."
    )


# ===== 2. Дедлайны и безопасная запись =====

def test_every_strategy_has_deadline():
    sw = _read("service-worker.js")
    for fn in ("networkFirst", "staleWhileRevalidate", "cacheFirst"):
        src = _fn_src(sw, fn)
        assert "withDeadline(" in src, (
            f"{fn} снова ждёт fetch без дедлайна — при «сеть есть, интернета "
            "нет» это десятки секунд белого экрана."
        )
    # Дедлайн навигации — короткий: кэшированному HTML ждать нечего.
    m = re.search(r"const NAV_TIMEOUT_MS = (\d+);", sw)
    assert m and int(m.group(1)) <= 5000
    # Гонка промисов, не AbortController: signal пересобирает navigate-запрос.
    assert "AbortController" not in _strip_line_comments(sw), (
        "AbortController в SW ломает навигационные запросы "
        "(mode:'navigate' схлопывается в 'same-origin')."
    )


def test_cache_put_only_through_safe_wrapper():
    sw = _strip_line_comments(_read("service-worker.js"))
    puts = re.findall(r"cache\.put\(", sw)
    assert len(puts) == 1, (
        f"cache.put встречается {len(puts)} раз — все записи обязаны идти "
        "через cachePutSafe, иначе отказ по квоте снова превратит валидный "
        "ответ в 503 (и демо-дела на экране)."
    )
    safe = _fn_src(sw, "cachePutSafe")
    assert "cache.put(" in safe and "try" in safe and "catch" in safe
    # data.put миграции — отдельный вход, он под своим try/catch.
    mig = _fn_src(sw, "migrateDataCache")
    assert "data.put(" in mig and "catch" in mig


def test_swr_notifies_only_on_successful_put():
    sw = _read("service-worker.js")
    swr = _fn_src(sw, "staleWhileRevalidate")
    assert re.search(r"if \(changed && stored\)", swr), (
        "Сигнал data-updated при упавшей записи заставит страницу перечитать "
        "СТАРЫЙ кэш и показать тост «Данные обновлены» поверх старых данных."
    )


def test_install_precache_bypasses_http_cache():
    """GitHub Pages отдаёт max-age=600: в 10-минутном окне после деплоя
    cache.add(url) мог взять ПРОШЛУЮ версию файла из HTTP-кэша браузера —
    офлайн отдавал бы старый JS под новый HTML."""
    sw = _read("service-worker.js")
    inst = re.search(r"addEventListener\('install',[\s\S]*?\n\}\);", sw)
    assert inst and "cache: 'no-cache'" in inst.group(0)


def test_versioned_asset_falls_back_to_bare():
    """APP_SHELL хранит голые './app.js'/'./styles.css', HTML просит ?v=N —
    без ignoreSearch офлайн-старт сразу после деплоя падал на
    «HTML из кэша есть, кода нет»."""
    sw = _read("service-worker.js")
    cf = _fn_src(sw, "cacheFirst")
    assert "ignoreSearch: true" in cf


# ===== 3. Демо не подставляется автоматически =====

def test_no_demo_fallback_on_load_failure():
    app = _read("app.js")
    load = _strip_line_comments(_fn_src(app, "loadFromSheet"))
    assert "DEMO_CSV" not in load, (
        "loadFromSheet снова подставляет демо-данные при ошибке — юрист "
        "офлайн увидит 4 чужих дела и решит, что потерял свои (жалоба "
        "15.08.2026)."
    )
    assert "showNoData(" in load
    # Демо остаётся доступным ЯВНО: функция и ссылки в разметке.
    assert "function loadDemo()" in app
    html = _read("sberbank_dashboard.html")
    assert html.count("loadDemo()") >= 2, (
        "Ссылка «посмотреть демо-данные» должна быть и на setup-экране, "
        "и на экране «Нет сохранённых данных»."
    )
    assert 'id="nodata-screen"' in html


# ===== 4. Старт без белого экрана =====

def test_loading_screen_visible_in_markup():
    html = _read("sberbank_dashboard.html")
    m = re.search(r'<div id="loading-screen"[^>]*>', html)
    assert m and "display:none" not in m.group(0), (
        "Экран загрузки снова скрыт в разметке — до исполнения app.js "
        "пользователь смотрит в белый лист."
    )
    for hidden in ("setup-screen", "nodata-screen", "app"):
        el = re.search(r'<div id="%s"[^>]*>' % hidden, html)
        assert el and "display:none" in el.group(0), (
            f"#{hidden} обязан стартовать скрытым."
        )


def test_boot_watchdog_present_and_cleared():
    html = _read("sberbank_dashboard.html")
    assert "__bootWatchdog" in html and "setTimeout" in html
    app = _read("app.js")
    assert "clearTimeout(window.__bootWatchdog)" in app
    # Сторож снимает ЛЮБОЙ показанный экран — все show* идут через _showScreens.
    for fn in ("showSetup", "showLoading", "showApp", "showNoData"):
        src = _fn_src(app, fn)
        assert "_showScreens(" in src, (
            f"{fn} обходит _showScreens — сторож белого экрана не снимется."
        )


# ===== 5. Шрифты свои =====

def test_fonts_are_self_hosted():
    html = _read("sberbank_dashboard.html")
    assert "fonts.googleapis.com" not in html and "fonts.gstatic.com" not in html, (
        "Google Fonts вернулся в HTML — render-blocking `<link>` офлайн "
        "держит белый экран до сетевого таймаута."
    )
    css = _read("styles.css")
    faces = re.findall(r"@font-face \{[\s\S]*?\}", css)
    assert len(faces) >= 4, "Ожидались @font-face на 4 подмножества IBM Plex Sans."
    for face in faces:
        assert "url('fonts/" in face and "unicode-range" in face
    for f in re.findall(r"url\('(fonts/[^']+)'\)", css):
        assert os.path.exists(os.path.join(ROOT, f)), f"Файл шрифта {f} не найден."
    sw = _read("service-worker.js")
    for f in re.findall(r"url\('fonts/([^']+)'\)", css):
        assert f"./fonts/{f}" in sw, (
            f"Шрифт {f} не в APP_SHELL — офлайн после чистки кэша останется "
            "без него."
        )


# ===== 6. Окно читает кэш напрямую =====

def test_window_falls_back_to_cache_storage():
    app = _read("app.js")
    fetch_fn = _fn_src(app, "fetchJsonCases")
    assert "readJsonFromCache" in fetch_fn, (
        "fetchJsonCases потерял фолбэк на Cache Storage — синтетический 503 "
        "от SW снова приведёт на экран «нет данных» при живом кэше."
    )
    reader = _fn_src(app, "readJsonFromCache")
    assert "caches.open(dataCacheName())" in reader
    assert "caches.match" in reader  # фолбэк для немигрировавших устройств


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_data_cache_name_mirrors_sw_scope_ns():
    """Формулы имени data-кэша в SW и в окне обязаны давать один результат —
    промах уводит чтение в чужой (пустой) кэш, и офлайн-фолбэк молча умирает."""
    sw = _read("service-worker.js")
    app = _read("app.js")
    ns_line = re.search(r"const SCOPE_NS = ([^;]+);", sw)
    data_line = re.search(r"const DATA_CACHE = ([^;]+);", sw)
    assert ns_line and data_line
    fn = _fn_src(app, "dataCacheName")
    script = """
    const paths = [
      ['/dashboard/service-worker.js', '/dashboard/sberbank_dashboard.html'],
      ['/dashboard-ural/service-worker.js', '/dashboard-ural/sberbank_dashboard.html'],
      ['/dashboard/service-worker.js', '/dashboard/'],
      ['/service-worker.js', '/sberbank_dashboard.html'],
    ];
    const out = paths.map(([swPath, pagePath]) => {
      const self = { location: { pathname: swPath } };
      const SCOPE_NS = %s;
      const DATA_CACHE = %s;
      const location = { pathname: pagePath };
      %s
      return [DATA_CACHE, dataCacheName()];
    });
    console.log(JSON.stringify(out));
    """ % (ns_line.group(1), data_line.group(1), fn)
    res = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    for sw_name, app_name in json.loads(res.stdout):
        assert sw_name == app_name, (
            f"Имена разошлись: SW={sw_name}, окно={app_name} — офлайн-фолбэк "
            "будет читать чужой кэш."
        )


# ===== 7. Метка офлайна и без голых fetch =====

def test_offline_badge_reuses_run_stamp():
    app = _read("app.js")
    meta = _fn_src(app, "renderMeta")
    assert "meta-offline" in meta and "_dataFromCache" in meta
    css = _read("styles.css")
    assert ".meta-offline" in css
    # Возврат сети сбрасывает метку и тихо освежает датасет.
    assert re.search(r"addEventListener\('online',", app)
    assert "_dataFromCache=false" in app


def test_no_bare_data_fetch_without_timeout():
    """Голый fetch без таймаута офлайн висит до таймаута ОС."""
    app = _read("app.js")
    for m in re.finditer(r"await fetch\('\./data/[^']+'", app):
        line = app[:m.start()].count("\n") + 1
        raise AssertionError(
            f"app.js:{line}: голый fetch('./data/…') без fetchWithTimeout."
        )
    # HEAD-проба (мимо SW) обязана резаться AbortController'ом.
    probe = _fn_src(app, "probeBankFile")
    assert "AbortController" in probe


def test_storage_persist_requested_once():
    app = _read("app.js")
    assert "navigator.storage.persist" in app
    assert "storage_persist_asked_v1" in app
    assert re.search(r"lsKey\('storage_persist_asked_v1'\)", app), (
        "Ключ гарда обязан идти через lsKey — иначе территории на одном "
        "origin перебьют друг друга."
    )
