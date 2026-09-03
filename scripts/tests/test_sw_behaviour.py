"""
Поведенческие тесты service-worker.js в node с моками Cache API.

Зачем отдельно от test_frontend_offline.py: тот стережёт ПРИСУТСТВИЕ паттернов
в исходнике (grep), а здесь код реально ИСПОЛНЯЕТСЯ на подставных caches/fetch.
Правка 15.08.2026 (офлайн-режим) трогает ядро офлайна, а проверить её в
браузере на этой машине нельзя: встроенный превью-браузер не регистрирует ни
один service worker (проверено пустым SW и версией из HEAD — падают одинаково).

Что исполняем:
1. migrateDataCache — переносит data/*.json из старых кэшей, не тащит чужую
   территорию, не перетирает свежее, переживает сбойную запись.
2. staleWhileRevalidate — кэш отдаётся мгновенно; сигнал об обновлении только
   при успешной записи; отказ cache.put не превращает валидный ответ в 503.
3. networkFirst — при висящей сети отдаёт кэш по дедлайну, а не ждёт.
4. cacheFirst — ignoreSearch-фолбэк: app.js?v=166 берётся из голого './app.js'.

Запуск: python3 -m pytest scripts/tests/test_sw_behaviour.py
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
pytestmark = pytest.mark.skipif(NODE is None, reason="node недоступен")

# Заглушки браузерного окружения + мок Cache Storage. Ответы — обычные объекты
# с headers.get и clone(): Response в node нет, а SW от него нужно немногое.
HARNESS = r"""
const listeners = {};
const self = {
  location: { pathname: '/dashboard/service-worker.js', origin: 'https://x.io' },
  addEventListener: (t, f) => { listeners[t] = f; },
  skipWaiting: async () => {},
  clients: { claim: async () => {}, matchAll: async () => MOCK.windows },
  registration: { showNotification: async () => {} },
};
const clients = self.clients;
const console_ = console;

function mkRes(body, { tag = null, ok = true, status = 200 } = {}) {
  return {
    ok, status, body,
    headers: { get: (h) => (/^etag$/i.test(h) ? tag : null) },
    clone() { return mkRes(body, { tag, ok, status }); },
  };
}

class MockCache {
  constructor(name) { this.name = name; this.map = new Map(); this.failPut = false; }
  async match(req, opts) {
    const url = typeof req === 'string' ? req : req.url;
    if (this.map.has(url)) return this.map.get(url);
    if (opts && opts.ignoreSearch) {
      const bare = url.split('?')[0];
      for (const [k, v] of this.map) if (k.split('?')[0] === bare) return v;
    }
    return undefined;
  }
  async put(req, res) {
    if (this.failPut) throw new Error('QuotaExceededError');
    this.map.set(typeof req === 'string' ? req : req.url, res);
  }
  async add() { throw new Error('add не используется в тестах'); }
  async keys() { return [...this.map.keys()].map((u) => ({ url: u })); }
}

const MOCK = { caches: new Map(), windows: [], posted: [], fetchImpl: null };
const caches = {
  async open(name) {
    if (!MOCK.caches.has(name)) MOCK.caches.set(name, new MockCache(name));
    return MOCK.caches.get(name);
  },
  async keys() { return [...MOCK.caches.keys()]; },
  async delete(name) { return MOCK.caches.delete(name); },
  async match(url) {
    for (const c of MOCK.caches.values()) { const r = await c.match(url); if (r) return r; }
    return undefined;
  },
};
function fetch(req) { return MOCK.fetchImpl(typeof req === 'string' ? req : req.url); }
function Request(url, init) { return { url, mode: 'no-cors', headers: { get: () => '' }, init }; }
function Response(body, init) { return mkRes(body, { ok: !init || !init.status || init.status < 400, status: (init && init.status) || 200 }); }
const setTimeoutOrig = setTimeout;
const mkEvent = () => { const p = []; return { waitUntil: (x) => p.push(x), _pending: p }; };
"""


def _sw_source() -> str:
    """Исходник SW без обработчиков верхнего уровня — их заменяет харнесс."""
    with open(os.path.join(ROOT, "service-worker.js"), encoding="utf-8") as f:
        return f.read()


def _run(script: str) -> dict:
    src = HARNESS + "\n" + _sw_source() + "\n" + script
    res = subprocess.run([NODE, "-e", src], capture_output=True, text=True)
    assert res.returncode == 0, f"node упал:\n{res.stderr[-2000:]}"
    return json.loads(res.stdout.strip().splitlines()[-1])


# ===== 1. Миграция данных =====

def test_migration_moves_own_data_only():
    """Переносим свои data/*.json; чужую территорию и код не трогаем."""
    out = _run(r"""
    (async () => {
      const old = await caches.open('sber-jurist-dashboard-v164');
      await old.put({url:'https://x.io/dashboard/data/cases.json'}, mkRes('свои дела'));
      await old.put({url:'https://x.io/dashboard/data/cases_bank.json'}, mkRes('свой банк'));
      await old.put({url:'https://x.io/dashboard/app.js'}, mkRes('код'));
      const legacy = await caches.open('sber-jurist-v106');
      await legacy.put({url:'https://x.io/dashboard-ural/data/cases.json'}, mkRes('ЧУЖИЕ дела'));
      await migrateDataCache(['sber-jurist-dashboard-v164', 'sber-jurist-v106']);
      const data = await caches.open(DATA_CACHE);
      const keys = (await data.keys()).map(k => k.url);
      console.log(JSON.stringify({ keys, name: DATA_CACHE }));
    })();
    """)
    assert out["name"] == "sber-jurist-dashboard-data"
    assert "https://x.io/dashboard/data/cases.json" in out["keys"]
    assert "https://x.io/dashboard/data/cases_bank.json" in out["keys"]
    assert all("app.js" not in k for k in out["keys"]), "Код не должен ехать в data-кэш."
    assert all("dashboard-ural" not in k for k in out["keys"]), (
        "Утащили данные СОСЕДНЕЙ территории — легаси-кэш общий на origin."
    )


def test_migration_keeps_fresher_copy_and_survives_broken_entry():
    out = _run(r"""
    (async () => {
      const data = await caches.open(DATA_CACHE);
      await data.put({url:'https://x.io/dashboard/data/cases.json'}, mkRes('СВЕЖЕЕ'));
      const old = await caches.open('sber-jurist-dashboard-v164');
      await old.put({url:'https://x.io/dashboard/data/cases.json'}, mkRes('старьё'));
      await old.put({url:'не-url'}, mkRes('битая запись'));
      await old.put({url:'https://x.io/dashboard/data/cases_archive.json'}, mkRes('архив'));
      await migrateDataCache(['sber-jurist-dashboard-v164']);
      const main = await data.match('https://x.io/dashboard/data/cases.json');
      const arch = await data.match('https://x.io/dashboard/data/cases_archive.json');
      console.log(JSON.stringify({ main: main.body, archMigrated: !!arch }));
    })();
    """)
    assert out["main"] == "СВЕЖЕЕ", "Миграция перетёрла свежий снимок старьём."
    assert out["archMigrated"], "Битая запись оборвала миграцию остальных."


# ===== 2. stale-while-revalidate =====

def test_swr_serves_cache_and_signals_only_on_successful_put():
    out = _run(r"""
    (async () => {
      const cache = await caches.open(DATA_CACHE);
      const req = { url: 'https://x.io/dashboard/data/cases.json', headers:{get:()=>''} };
      await cache.put(req, mkRes('вчера', { tag: 'W/"1"' }));
      MOCK.fetchImpl = async () => mkRes('сегодня', { tag: 'W/"2"' });
      MOCK.windows = [{ postMessage: (m) => MOCK.posted.push(m.type) }];

      const ev1 = mkEvent();
      const r1 = await staleWhileRevalidate(req, DATA_CACHE, ev1);
      await Promise.all(ev1._pending);
      const served = r1.body;
      const afterOk = { posted: [...MOCK.posted], stored: (await cache.match(req)).body };

      // Теперь запись падает по квоте: сигнала быть не должно, ответ — валиден.
      MOCK.posted.length = 0;
      cache.failPut = true;
      MOCK.fetchImpl = async () => mkRes('послезавтра', { tag: 'W/"3"' });
      const ev2 = mkEvent();
      const r2 = await staleWhileRevalidate(req, DATA_CACHE, ev2);
      await Promise.all(ev2._pending);
      console.log(JSON.stringify({ served, afterOk, quietOnFail: MOCK.posted,
                                   secondServed: r2.body, secondOk: r2.ok }));
    })();
    """)
    assert out["served"] == "вчера", "SWR обязан отдать кэш мгновенно."
    assert out["afterOk"]["posted"] == ["data-updated"], "Сигнал об обновлении не ушёл."
    assert out["afterOk"]["stored"] == "сегодня", "Кэш не обновился в фоне."
    assert out["quietOnFail"] == [], (
        "При упавшей записи ушёл сигнал — страница впустую перечитает "
        "СТАРЫЙ кэш и перерисуется поверх тех же данных."
    )
    # Вторая фаза: в кэше уже лежит «сегодня» (положен первой фазой), сеть даёт
    # «послезавтра», но запись падает по квоте. Ответ обязан остаться валидным.
    assert out["secondServed"] == "сегодня" and out["secondOk"], (
        "Отказ cache.put не должен превращать ответ в ошибку."
    )


def test_swr_without_cache_returns_network_even_if_put_fails():
    """Ключевой регресс: пустой кэш + отказ записи давали синтетический 503,
    а app.js трактовал его как фатальную ошибку → демо-дела на экране."""
    out = _run(r"""
    (async () => {
      const cache = await caches.open(DATA_CACHE);
      cache.failPut = true;
      const req = { url: 'https://x.io/dashboard/data/cases.json', headers:{get:()=>''} };
      MOCK.fetchImpl = async () => mkRes('свежие дела', { tag: 'W/"9"' });
      const ev = mkEvent();
      const r = await staleWhileRevalidate(req, DATA_CACHE, ev);
      console.log(JSON.stringify({ status: r.status, body: r.body, ok: r.ok }));
    })();
    """)
    assert out["ok"] and out["status"] == 200, (
        f"Валидный сетевой ответ выродился в {out['status']} из-за отказа записи."
    )
    assert out["body"] == "свежие дела"


# ===== 3. Дедлайны =====

def test_network_first_falls_back_to_cache_on_hanging_network():
    """«Сеть есть, интернета нет»: fetch не падает, а висит. Без дедлайна
    это был белый экран на десятки секунд."""
    out = _run(r"""
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      const req = { url: 'https://x.io/dashboard/sberbank_dashboard.html',
                    mode: 'navigate', headers: { get: () => 'text/html' } };
      await cache.put(req, mkRes('кэшированный HTML'));
      MOCK.fetchImpl = () => new Promise(() => {});   // висит вечно
      const started = Date.now();
      const ev = mkEvent();
      const r = await networkFirst(req, CACHE_NAME, ev, 300);
      console.log(JSON.stringify({ body: r.body, ms: Date.now() - started }));
    })();
    """)
    assert out["body"] == "кэшированный HTML"
    assert out["ms"] < 2000, f"Ждали дольше дедлайна: {out['ms']} мс."


def test_network_first_offline_without_cache_gives_offline_page():
    out = _run(r"""
    (async () => {
      const req = { url: 'https://x.io/dashboard/sberbank_dashboard.html',
                    mode: 'navigate', headers: { get: () => 'text/html' } };
      MOCK.fetchImpl = async () => { throw new Error('offline'); };
      const r = await networkFirst(req, CACHE_NAME, mkEvent(), 300);
      console.log(JSON.stringify({ status: r.status, hasOffline: String(r.body).includes('Нет связи') }));
    })();
    """)
    assert out["status"] == 200 and out["hasOffline"]


# ===== 4. Версионированный ассет из голого прекэша =====

def test_cache_first_falls_back_to_bare_url():
    """APP_SHELL кладёт './app.js', HTML просит 'app.js?v=166'. Без
    ignoreSearch офлайн-старт сразу после деплоя падал на «кода нет»."""
    out = _run(r"""
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      await cache.put({ url: 'https://x.io/dashboard/app.js' }, mkRes('код приложения'));
      MOCK.fetchImpl = async () => { throw new Error('offline'); };
      const req = { url: 'https://x.io/dashboard/app.js?v=166', headers: { get: () => '' } };
      const r = await cacheFirst(req, CACHE_NAME, mkEvent(), 300);
      console.log(JSON.stringify({ body: r.body }));
    })();
    """)
    assert out["body"] == "код приложения"
