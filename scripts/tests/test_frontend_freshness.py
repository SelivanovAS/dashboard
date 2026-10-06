"""
Стражи свежести данных на фронте (app.js + service-worker.js).

Инцидент 03.08.2026. Юрист открыл дашборд после утреннего прогона и увидел
дело 2-592/2025 в АКТИВНОМ списке картотеки «Иски банка» — хотя тот же прогон
двумя часами раньше увёл его в архив трека (archived_at 2026-08-03). Данные
были ни при чём: SW отдаёт все data/*.json по stale-while-revalidate, то есть
кэш мгновенно, а сеть — в фоне «на следующее открытие». Свежий блок дайджеста
рядом (last_digest.json идёт network-first) делал картину совсем убедительной:
дайджест сегодняшний, картотека вчерашняя. Шапка при этом писала
«Обновлено: <текущее время>» — время РЕНДЕРА, а не прогона, так что отличить
вчерашний снимок было нечем.

Что охраняем:
1. SW сравнивает версию ответа (ETag/Last-Modified) и, обновив кэш, сообщает
   об этом окнам — иначе перерисовывать будет некому.
2. app.js слушает сигнал и перечитывает затронутый датасет.
3. Перерисовка откладывается, пока открыт drawer, и происходит по закрытию.
4. Шапка показывает время ПРОГОНА (updated_at файла), а не new Date().
5. Naive-ISO без «Z» читается как UTC (Python на UTC-раннере пишет без него).

JS-инструментария в проекте нет: чистые функции исполняются в node, остальное
проверяется grep'ом по исходнику — тем же приёмом, что test_frontend_writs.py.

Запуск: python3 -m pytest scripts/tests/test_frontend_freshness.py
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
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


# ===== 1. Service worker сообщает об обновлении кэша =====

def test_sw_notifies_clients_on_fresh_data():
    sw = _read("service-worker.js")
    swr = _fn_src(sw, "staleWhileRevalidate")
    assert "notifyDataUpdated" in swr, (
        "staleWhileRevalidate больше не сообщает окнам об обновлении кэша — "
        "страница снова будет показывать снимок предыдущего прогона до "
        "следующего открытия (кейс 2-592/2025)."
    )
    notify = _fn_src(sw, "notifyDataUpdated")
    assert "postMessage" in notify and "data-updated" in notify
    assert "clients.matchAll" in notify


def test_sw_compares_response_version():
    """Сообщаем только о РЕАЛЬНОМ изменении: иначе каждая ревалидация дёргала
    бы холостую перерисовку на ровном месте."""
    sw = _read("service-worker.js")
    tag = _fn_src(sw, "responseTag")
    assert "ETag" in tag and "Last-Modified" in tag, (
        "responseTag должен смотреть на ETag (GitHub Pages) с фолбэком на "
        "Last-Modified (локальный http.server)."
    )
    swr = _fn_src(sw, "staleWhileRevalidate")
    assert "responseTag(cached) !== responseTag(res)" in swr
    # Первая загрузка (кэша не было) сигнала не даёт — данные и так свежие.
    assert "cached &&" in swr


def test_data_json_still_stale_while_revalidate():
    """Network-first для тяжёлых файлов не годится: cases.json ~2 МБ,
    cases_bank.json ~1.4 МБ — первый экран встал бы на мобильной сети."""
    sw = _read("service-worker.js")
    m = re.search(r"if \(isDataRequest\(url\)\) \{\s*event\.respondWith\((\w+)",
                  sw)
    assert m and m.group(1) == "staleWhileRevalidate", (
        "data/*.json ушли с stale-while-revalidate — проверьте, что первый "
        "экран не ждёт сети."
    )


def test_revalidation_survives_sw_sleep():
    """Отдав ответ из кэша, браузер вправе усыпить SW — без waitUntil фоновая
    дозагрузка умрёт вместе с ним, и сигнала о свежих данных не будет
    (на телефоне SW усыпляют агрессивнее всего)."""
    sw = _read("service-worker.js")
    swr = _fn_src(sw, "staleWhileRevalidate")
    assert "event.waitUntil(network)" in swr, (
        "Фоновая ревалидация больше не продлевает жизнь SW — свежие данные "
        "могут не доехать до кэша и до страницы."
    )
    # С 15.08.2026 данные живут в неверсионированном DATA_CACHE (переживает
    # бамп CACHE_VERSION) — см. test_frontend_offline.py.
    assert re.search(r"event\.respondWith\(staleWhileRevalidate\("
                     r"request, DATA_CACHE, event\)\)", sw), (
        "В staleWhileRevalidate не передаётся event — waitUntil звать нечем."
    )


# ===== 2-3. Приём сигнала и отложенная перерисовка =====

def test_app_listens_to_data_updated():
    app = _read("app.js")
    assert "data.type === 'data-updated'" in app, (
        "app.js не слушает сигнал SW — перерисовки не будет."
    )
    assert "onDataUpdated(data.url)" in app


def test_refresh_deferred_while_drawer_open():
    app = _read("app.js")
    busy = _fn_src(app, "uiBusyForRefresh")
    assert "activeCaseNumber" in busy, (
        "Перерисовка на открытом drawer выдернет карточку из-под пальца."
    )
    apply_fn = _fn_src(app, "applyPendingDataRefresh")
    assert "uiBusyForRefresh()" in apply_fn
    # Набор ждущих url'ов не должен теряться при отложенном проходе.
    assert re.search(r"if\(uiBusyForRefresh\(\)\)return;\s*//", apply_fn), (
        "Отложенный проход обязан ВЕРНУТЬСЯ, не обнуляя _pendingDataUrls."
    )
    close_drawer = _fn_src(app, "closeDrawer")
    assert "applyPendingDataRefresh()" in close_drawer, (
        "Закрытие drawer больше не запускает отложенное обновление — оно "
        "повиснет до следующего сигнала SW."
    )


def test_background_refresh_is_quiet():
    """Фоновое обновление не должно мигать экраном загрузки."""
    app = _read("app.js")
    apply_fn = _fn_src(app, "applyPendingDataRefresh")
    assert "{quiet:true}" in apply_fn
    load = _fn_src(app, "loadFromSheet")
    assert "if(!(opts&&opts.quiet))showLoading();" in load


def test_background_refresh_is_silent():
    """Тоста «Данные обновлены» нет (03.09.2026, решение юриста): он срабатывал
    на каждый утренний заход и двоился по файлам. Сигнал свежести один —
    штамп «Данные от: …» в шапке."""
    app = _read("app.js")
    apply_fn = _fn_src(app, "applyPendingDataRefresh")
    assert "showToast" not in apply_fn, "Тост фонового обновления вернулся."
    # Строка тоста не должна вернуться НИ в одном вызове (комментарии не в счёт).
    code = "\n".join(l for l in app.splitlines() if not l.lstrip().startswith("//"))
    assert "showToast('Данные обновлены" not in code


def test_background_refresh_passes_do_not_overlap():
    """Три файла прогона докачиваются с разницей больше дебаунса: пока проход
    в полёте, новые сигналы копятся, а хвост дочитывается ОДНИМ проходом после —
    иначе два loadFromSheet бегут параллельно и дважды зовут renderAll."""
    app = _read("app.js")
    apply_fn = _fn_src(app, "applyPendingDataRefresh")
    assert "if(_dataRefreshInFlight)return;" in apply_fn
    assert "_dataRefreshInFlight=Promise.all(jobs)" in apply_fn
    assert "_dataRefreshInFlight=null;" in apply_fn
    # Хвостовой проход зовётся ПОСЛЕ снятия гарда — иначе он вернётся сразу.
    assert re.search(r"_dataRefreshInFlight=null;\s*//[^\n]*\n\s*applyPendingDataRefresh\(\);", apply_fn)
    assert re.search(r"const DATA_REFRESH_DEBOUNCE_MS=(\d+);", app)
    assert int(re.search(r"const DATA_REFRESH_DEBOUNCE_MS=(\d+);", app).group(1)) >= 1000


# ===== 4. Шапка показывает время прогона =====

def test_header_shows_run_stamp_not_render_time():
    app = _read("app.js")
    meta = _fn_src(app, "renderMeta")
    assert "currentDataStamp()" in meta, (
        "Шапка снова подписывает данные временем рендера страницы — "
        "вчерашний снимок из кэша выглядит сегодняшним."
    )
    assert "Данные от: " in meta
    stamp = _fn_src(app, "currentDataStamp")
    assert "resolveSheetUrl()" in stamp and "bankJsonUrl()" in stamp, (
        "Штамп берётся из основного cases.json с фолбэком на файл трека "
        "(вход сразу в картотеку банка)."
    )


def test_updated_at_captured_on_load():
    app = _read("app.js")
    fetch_fn = _fn_src(app, "fetchJsonCases")
    assert "_dataUpdatedAt[url]" in fetch_fn and "data.updated_at" in fetch_fn


# ===== 5. Поведение чистых функций =====

@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_naive_iso_parsed_as_utc():
    """Python на UTC-раннере пишет updated_at без «Z». Без явного суффикса
    браузер прочитал бы его как локальное время и показал прогон на пять
    часов раньше (ХМАО = UTC+5)."""
    app = _read("app.js")
    script = _fn_src(app, "parseIsoUtc") + """
    const naive = parseIsoUtc('2026-08-03T05:53:43');
    const zulu = parseIsoUtc('2026-08-03T05:53:43Z');
    const offset = parseIsoUtc('2026-08-03T10:53:43+05:00');
    console.log(JSON.stringify({
      naive: naive.toISOString(),
      zulu: zulu.toISOString(),
      offset: offset.toISOString(),
      empty: parseIsoUtc('') === null,
      garbage: parseIsoUtc('не дата') === null,
    }));
    """
    out = subprocess.run([NODE, "-e", script], capture_output=True,
                         text=True, check=True)
    res = json.loads(out.stdout)
    assert res["naive"] == "2026-08-03T05:53:43.000Z", (
        "Naive-ISO прочитан не как UTC — время прогона в шапке уедет."
    )
    assert res["zulu"] == res["naive"] == res["offset"]
    assert res["empty"] and res["garbage"]


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_data_file_kind_routing():
    """Сигнал о каждом файле должен попасть в свой датасет — и никуда больше
    (last_digest_context.json перерисовку картотеки не запускает)."""
    app = _read("app.js")
    script = _fn_src(app, "dataFileKind") + """
    const urls = [
      'https://x.io/dashboard/data/cases.json',
      'https://x.io/dashboard/data/cases_archive.json',
      'https://x.io/dashboard/data/cases_bank.json',
      'https://x.io/dashboard/data/cases_bank_archive.json',
      'https://x.io/dashboard/data/cases_bank_events.json',
      'https://x.io/dashboard/data/cases_bank_archive_events.json',
      'https://x.io/dashboard/data/last_digest_context.json',
      'https://x.io/dashboard/data/parse_health.json',
      '',
    ];
    console.log(JSON.stringify(urls.map(dataFileKind)));
    """
    out = subprocess.run([NODE, "-e", script], capture_output=True,
                         text=True, check=True)
    assert json.loads(out.stdout) == [
        "main", "main", "bank", "bank", "bank", "bank", "", "", "",
    ]
