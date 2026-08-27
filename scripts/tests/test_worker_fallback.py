# -*- coding: utf-8 -*-
"""Стражи устойчивости фронта к блокировкам Worker'а (27.08.2026).

Контекст: часть операторов связи режет *.workers.dev по имени (SNI) — с их
сетей не работали синк подписок, push и админка. Worker переехал на свой
домен (api-hmao.delosud.ru), workers.dev остался фолбэком. Решения, которые
стерегут тесты:

1. ВСЕ запросы фронта к Worker'у идут через workerFetch: таймаут на каждый
   адрес (блокировка «вешает» соединение — голый fetch ждал бы вечно) +
   перебор адресов из WORKER_HOSTS, sticky на последнем ответившем.
2. Неидемпотентные пути связывания НЕ ретраятся на второй адрес:
   /profile/link-code без profile_id создаёт НОВЫЙ профиль на каждый вызов
   (повтор = профиль-сирота в KV), /profile/link сжигает код. Для них —
   failover:false + предварительный ensureWorkerHost (дешёвый GET /).
3. Семантика «пустой PUSH_WORKER_URL = синк выключен» сохранена: гарды на
   месте, WORKER_HOSTS при пустом URL пуст, фолбэки не спасают (иначе форк
   без Worker'а перемешал бы подписчиков с чужим KV).
4. Дорассылка dirty-буфера на online/visibilitychange — ТОЛЬКО у связанных
   устройств (getProfileId): без профиля dirty не снимается никогда, и
   каждый возврат сети давал бы холостой POST /watchlist (KV-writes).
5. Индикация «не отправлено»: точка на 🔗 (updateSyncButton), строка в
   шторке синка, тост РОВНО один раз за сессию.
6. wrangler.toml: custom domain + ЯВНЫЙ workers_dev = true (при routes
   wrangler молча гасит workers.dev-адрес, а он нужен как фолбэк).
"""

from __future__ import annotations

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


def _app_js() -> str:
    return _read("app.js")


def _fn_src(src: str, name: str) -> str:
    # (?:async\s+)? — async-префикс обязан войти в вырезку: без него node-тест
    # получает функцию с await внутри не-async тела (SyntaxError).
    m = re.search(
        r"(?:async\s+)?function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src
    )
    assert m, f"Функция {name} не найдена."
    return m.group(0)


# ── workerFetch: обёртка всех запросов к Worker'у ───────────────────────────

class TestWorkerFetch:
    def test_no_bare_worker_fetches(self):
        """Голых fetch(PUSH_WORKER_URL + …) не осталось — все 9 вызовов через
        workerFetch (иначе запрос без таймаута виснет при блокировке)."""
        assert "fetch(PUSH_WORKER_URL" not in _app_js()

    def test_worker_fetch_has_timeout_and_failover(self):
        js = _app_js()
        one = _fn_src(js, "_workerFetchOne")
        assert "AbortController" in one and "abort()" in one, (
            "Таймаут через AbortController обязателен: блокировка оператора "
            "«вешает» соединение, без abort перебор адресов не начнётся."
        )
        assert "clearTimeout" in one
        wf = _fn_src(js, "workerFetch")
        assert "_workerHostIdx" in wf, "sticky-индекс ответившего адреса"
        assert "WORKER_HOSTS" in wf
        assert "failover === false" in wf

    def test_empty_url_keeps_sync_disabled(self):
        """Пустой PUSH_WORKER_URL = синк выключен: WORKER_HOSTS пуст,
        фолбэки не используются (иначе форк без Worker'а перемешал бы
        подписчиков с ХМАО-KV)."""
        js = _app_js()
        m = re.search(r"const WORKER_HOSTS = PUSH_WORKER_URL\s*\n?\s*\?", js)
        assert m, "WORKER_HOSTS обязан строиться тернарником от PUSH_WORKER_URL"
        assert ": [];" in js[m.start():m.start() + 400]

    def test_push_worker_url_guards_alive(self):
        """Гарды «Worker'а нет → тихий выход» не тронуты миграцией."""
        js = _app_js()
        # 8 гардов вида !PUSH_WORKER_URL (у loadProfileWatchlist — «!pid || !PUSH_WORKER_URL»)
        assert js.count("!PUSH_WORKER_URL)") >= 8
        assert "PUSH_WORKER_URL" in _fn_src(js, "unlinkThisDevice")

    @pytest.mark.skipif(NODE is None, reason="node недоступен")
    def test_failover_behaviour_node(self):
        """Функционально: мёртвый адрес A → ответ с B, sticky переезжает,
        failover:false ходит только на sticky."""
        js = _app_js()
        script = (
            "const FETCH_TIMEOUT_MS = 1000;\n"
            "const WORKER_HOSTS = ['https://a.test','https://b.test'];\n"
            "let _workerHostIdx = 0;\n"
            "let _workerHostProbed = false;\n"
            + _fn_src(js, "_workerFetchOne") + "\n"
            + _fn_src(js, "workerFetch") + "\n"
            + "global.fetch = async (url, init) => {\n"
            + "  if (url.startsWith('https://a.test')) throw new Error('net');\n"
            + "  return { ok: true, url: url, status: 200 };\n"
            + "};\n"
            + "(async () => {\n"
            + "  const r = await workerFetch('/x', { method: 'GET' });\n"
            + "  if (r.url !== 'https://b.test/x') throw new Error('не тот адрес: ' + r.url);\n"
            + "  if (_workerHostIdx !== 1) throw new Error('sticky не переехал');\n"
            + "  if (!_workerHostProbed) throw new Error('probed не выставлен');\n"
            + "  const r2 = await workerFetch('/y', {}, { failover: false });\n"
            + "  if (r2.url !== 'https://b.test/y') throw new Error('failover:false обязан ходить на sticky');\n"
            + "  console.log('OK');\n"
            + "})().catch((e) => { console.error(e && e.message); process.exit(1); });\n"
        )
        r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
        assert r.returncode == 0, f"node упал:\n{r.stderr}"
        assert "OK" in r.stdout


# ── Неидемпотентные пути связывания ─────────────────────────────────────────

class TestUnsafeEndpointsNoFailover:
    def test_link_code_no_failover(self):
        fn = _fn_src(_app_js(), "requestPairCode")
        assert "'/profile/link-code'" in fn
        assert "failover: false" in fn, (
            "/profile/link-code без profile_id создаёт НОВЫЙ профиль на каждый "
            "вызов — ретрай на второй адрес плодит сирот в KV."
        )
        assert "ensureWorkerHost()" in fn

    def test_link_no_failover(self):
        fn = _fn_src(_app_js(), "submitPairCode")
        assert "'/profile/link'" in fn
        assert "failover: false" in fn, (
            "/profile/link сжигает код при успехе — ретрай даёт "
            "code_not_found на уже связанном профиле."
        )
        assert "ensureWorkerHost()" in fn

    def test_ensure_worker_host_is_cheap_get(self):
        fn = _fn_src(_app_js(), "ensureWorkerHost")
        assert "_workerHostProbed" in fn, "одноразовость на сессию"
        assert "'GET'" in fn and "workerFetch('/'" in fn


# ── Дорассылка dirty-буфера ─────────────────────────────────────────────────

class TestDirtyFlush:
    def test_online_flushes_dirty(self):
        js = _app_js()
        m = re.search(r"addEventListener\('online',[\s\S]*?\n\}\);", js)
        assert m, "online-слушатель не найден"
        assert "isProfileDirty()" in m.group(0)
        assert "scheduleWatchlistSync()" in m.group(0)
        assert "getProfileId()" in m.group(0), (
            "Гейт по профилю обязателен: без него dirty несвязанного "
            "устройства (не снимается никогда) даёт холостые KV-writes."
        )

    def test_visibility_flushes_dirty(self):
        js = _app_js()
        m = re.search(r"addEventListener\('visibilitychange',[\s\S]*?\n\}\);", js)
        assert m, "visibilitychange-слушатель не найден (сценарий «перешёл на Wi-Fi» online не даёт)"
        assert "isProfileDirty()" in m.group(0)
        assert "getProfileId()" in m.group(0)
        assert "scheduleWatchlistSync()" in m.group(0)


# ── Индикация «изменения не отправлены» ─────────────────────────────────────

class TestPendingIndication:
    def test_sync_button_shows_pending(self):
        fn = _fn_src(_app_js(), "updateSyncButton")
        assert "'pending'" in fn
        assert "isProfileDirty()" in fn

    def test_dirty_markers_update_button(self):
        js = _app_js()
        assert "updateSyncButton()" in _fn_src(js, "markProfileDirty")
        assert "updateSyncButton()" in _fn_src(js, "clearProfileDirty")

    def test_sync_sheet_shows_pending_line(self):
        assert "sync-status-pending" in _fn_src(_app_js(), "renderSyncSheet")

    def test_styles_have_pending_classes(self):
        css = _read("styles.css")
        assert ".sync-status-pending" in css
        assert "#btn-sync.pending::after" in css

    def test_background_fail_toasts_once(self):
        js = _app_js()
        assert "_syncFailToastShown" in js
        fn = _fn_src(js, "_notifySyncFailOnce")
        assert "showToast" in fn
        assert js.count("_notifySyncFailOnce()") >= 2, (
            "Обе ветки провала фонового синка (!r.ok и catch) обязаны звать тост"
        )


# ── Конфигурация адресов (region_front.js + wrangler.toml) ──────────────────

class TestAddressConfig:
    def test_region_front_has_fallbacks(self):
        rf = _read("region_front.js")
        assert "PUSH_WORKER_FALLBACKS" in rf
        assert re.search(r"PUSH_WORKER_FALLBACKS:\s*\[[^\]]*workers\.dev", rf), (
            "workers.dev-адрес обязан остаться в фолбэках: он живёт без зоны "
            "и спасает при проблемах кастомного домена."
        )

    def test_wrangler_custom_domain_and_workers_dev(self):
        toml = _read("cloudflare-worker/wrangler.toml")
        assert "custom_domain = true" in toml
        assert re.search(r"^workers_dev = true", toml, re.M), (
            "workers_dev = true обязан стоять ЯВНО: при появлении routes "
            "wrangler молча гасит workers.dev-адрес, а он нужен как фолбэк."
        )
