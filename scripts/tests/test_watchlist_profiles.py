# -*- coding: utf-8 -*-
"""Стражи синхронизации звёзд между устройствами (профили, 25.08.2026).

Концепция (утверждена юристом): устройства одного юриста связываются коротким
кодом в профиль `profile:<uuid>` в KV Worker'а — watchlist общий, снятие
звезды зеркалится. Ключевые решения, которые стерегут тесты:

1. Профиль НЕ зависит от push-подписки: профильный путь синка работает и на
   устройстве, где уведомления не разрешены (главный страж —
   test_profile_sync_has_no_push_guards).
2. LWW по таймстампу набора: updated_at ставит только Worker; устаревший
   base_ts клиента → 409 с серверным набором БЕЗ записи, клиент накатывает
   тоглы текущей сессии и повторяет РОВНО один раз.
3. Профиль пишется без expirationTtl (KV get TTL не продлевает, «продление»
   фоновыми writes запрещено free-tier'ом).
4. delivery.py не знает о профилях ВООБЩЕ: Worker резолвит профильный
   watchlist в выдачах /subscriptions и /admin/data (TestDeliveryFrozen).
5. /admin/data отдаёт обёртку {subs, profiles} — ключ «subs» обязан
   называться так: второй потребитель scripts/audit_watchlists.py понимает
   именно его.
6. Ленивая миграция: устройство без профиля живёт прежним endpoint-путём.
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


def _worker() -> str:
    return _read("cloudflare-worker/worker.js")


def _admin() -> str:
    return _read("cloudflare-worker/admin_page.js")


def _app_js() -> str:
    return _read("app.js")


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _const_src(src: str, name: str) -> str:
    m = re.search(r"^const\s+" + re.escape(name) + r"\s*=.*$", src, re.M)
    assert m, f"Константа {name} не найдена."
    return m.group(0) + "\n"


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# ── Worker: контракт эндпоинтов и KV-схемы ──────────────────────────────────

class TestWorkerContract:
    def test_router_has_profile_routes(self):
        js = _worker()
        for route in (
            '"/profile/link-code"',
            '"/profile/link"',
            '"/profile/get"',
            '"/profile/watchlist"',
            '"/profile/unlink"',
        ):
            assert route in js, f"Роут {route} не заведён в fetch-роутере."

    def test_paircode_constants(self):
        js = _worker()
        m = re.search(r'const PAIR_CODE_ALPHABET = "([^"]+)"', js)
        assert m, "PAIR_CODE_ALPHABET не найден."
        # Код — ТОЛЬКО цифры, 6 знаков (решение юриста 26.08.2026: цифровая
        # клавиатура, короткий ввод; компенсация малого пространства — TTL
        # 10 мин, одноразовость и лимит Workers free).
        assert m.group(1) == "0123456789"
        assert "const PAIR_CODE_LEN = 6" in js
        assert "const PAIR_CODE_TTL_SEC = 600" in js
        assert "crypto.getRandomValues" in _fn_src(js, "genPairCode")

    def test_profile_put_has_no_ttl(self):
        # Профиль бессрочный: KV get TTL не продлевает, а «продление» фоновыми
        # writes запрещено (free-tier 1000/день). Протухший профиль снёс бы
        # звёзды всех устройств юриста, вернувшегося из отпуска.
        body = _fn_src(_worker(), "putProfile")
        code_only = re.sub(r"//[^\n]*", "", body)  # слово живёт в комментарии
        assert "expirationTtl" not in code_only

    def test_sub_ttl_named_constant(self):
        js = _worker()
        assert "const KV_SUB_TTL_SEC" in js
        # Единственное вхождение выражения — само объявление константы.
        assert js.count("60 * 24 * 3600") == 1, (
            "Инлайновый TTL подписок пережил замену на KV_SUB_TTL_SEC — "
            "новый put с «магическим» сроком разъедется при следующей правке."
        )

    def test_profile_watchlist_sanitizes_and_canonicalizes(self):
        body = _fn_src(_worker(), "handleProfileSetWatchlist")
        assert "sanitizeWatchlistInput" in body
        assert "canonicalizeWatchlistArr" in body

    def test_lww_conflict_contract(self):
        body = _fn_src(_worker(), "handleProfileSetWatchlist")
        assert "status: 409" in body
        assert '"conflict"' in body
        assert "baseTs < profile.updated_at" in body
        # В конфликтной ветке записи НЕТ: 409 отвечает ДО канонизации и put.
        pre409 = body.split("status: 409", 1)[0]
        assert "writeProfileWatchlist" not in pre409

    def test_link_does_union_and_burns_code(self):
        body = _fn_src(_worker(), "handleProfileLink")
        assert "unionWatchlists" in body, "Первое связывание обязано сливать наборы."
        assert ".delete(paircodeKey(" in body, "Код одноразовый — сжигается при обмене."

    def test_subscribe_carries_profile_id(self):
        body = _fn_src(_worker(), "handleSubscribe")
        # Старый фронт (без profile_id в body) не рвёт связку…
        assert "prev.profile_id" in body
        # …и не пробивает 12-часовой гейт экономии writes; смена привязки —
        # пробивает (после переноса из prev неравенство возможно только при ней).
        assert "(prev.profile_id || null) === (sub.profile_id || null)" in body
        # Обе ветки ответа (ранний выход гейта и полная) резолвят профиль —
        # иначе гидратация при открытии PWA отдаёт замороженный снимок.
        assert body.count("subscribeResponseBody(") == 2

    def test_legacy_watchlist_redirects_to_profile(self):
        body = _fn_src(_worker(), "handleSetWatchlist")
        assert "sub.profile_id" in body
        assert "writeProfileWatchlist" in body
        assert 'target: "profile"' in body

    def test_admin_watchlist_redirects_and_skips_equal(self):
        body = _fn_src(_worker(), "handleAdminWatchlist")
        assert "r.sub.profile_id" in body
        assert "writeProfileWatchlist" in body
        # skip-if-equal: canonicalize_kv_watchlists шлёт по КАЖДОЙ подписке
        # профиля — без сравнения N устройств давали бы N одинаковых writes.
        assert ".sort()" in body and "JSON.stringify" in body

    def test_subscriptions_and_admin_data_resolve_profiles(self):
        js = _worker()
        assert "resolveProfilesInto" in _fn_src(js, "handleListSubscriptions")
        assert "resolveProfilesInto" in _fn_src(js, "handleAdminData")

    def test_admin_data_shape_matches_audit_contract(self):
        body = _fn_src(_worker(), "handleAdminData")
        # Ключ «subs» — контракт scripts/audit_watchlists.py (fetch_subscriptions
        # понимает {subs: [...]}); переименование молча уронит аудит.
        assert "subs: safe" in body
        assert "profiles: profileRows" in body
        assert "profile_id:" in body, "profile_id обязан войти в safe-проекцию."

    def test_error_contract_json(self):
        js = _worker()
        assert js.count('"profile_not_found"') >= 3
        assert '"code_not_found"' in js


@pytest.mark.skipif(NODE is None, reason="node недоступен")
class TestWorkerPure:
    def test_gen_pair_code(self):
        js = _worker()
        script = (
            _const_src(js, "PAIR_CODE_ALPHABET")
            + _const_src(js, "PAIR_CODE_LEN")
            + _fn_src(js, "genPairCode")
            + """
const seen = new Set();
for (let i = 0; i < 200; i++) {
  const c = genPairCode();
  if (c.length !== 6) { console.log("BAD_LEN " + c); process.exit(0); }
  if (!/^[0-9]+$/.test(c)) { console.log("BAD_CHAR " + c); process.exit(0); }
  seen.add(c);
}
console.log("OK " + (seen.size > 150 ? "diverse" : "suspect"));
"""
        )
        out = _node(script)
        assert out == "OK diverse", out

    def test_normalize_pair_code(self):
        script = (
            _fn_src(_worker(), "normalizePairCode")
            + "console.log(normalizePairCode(' 12-3 456 '));"
        )
        assert _node(script) == "123456"

    def test_union_watchlists(self):
        script = (
            _fn_src(_worker(), "unionWatchlists")
            + "console.log(JSON.stringify(unionWatchlists(['a','b'], ['b','c','a'])));"
        )
        assert _node(script) == '["a","b","c"]'


# ── Фронт app.js ────────────────────────────────────────────────────────────

class TestAppJs:
    def test_profile_sync_has_no_push_guards(self):
        # Пункт 3 концепции: профильный синк работает БЕЗ push-подписки.
        # Появление любого из этих гардов молча вернёт «остров» устройствам
        # с запрещёнными уведомлениями.
        body = _fn_src(_app_js(), "syncWatchlistToProfile")
        for marker in ("PushManager", "serviceWorker", "getSubscription"):
            assert marker not in body, f"Профильный путь не должен требовать push ({marker})."

    def test_dispatcher_prefers_profile(self):
        body = _fn_src(_app_js(), "syncWatchlistToWorker")
        assert "getProfileId()" in body
        assert "syncWatchlistToProfile(pid)" in body
        assert "syncWatchlistToWorkerLegacy()" in body
        # Push-гарды живут только в легаси-пути.
        assert "PushManager" not in body

    def test_profile_keys_via_lskey(self):
        js = _app_js()
        for key in ("lsKey('profile_id')", "lsKey('profile_base_ts')", "lsKey('profile_dirty')"):
            assert key in js, (
                f"{key}: ключи профиля обязаны идти через lsKey — оба фронта "
                "(ХМАО и Урал) живут на одном origin github.io."
            )

    def test_anti_cycle_preserved(self):
        js = _app_js()
        # Приём серверного набора не планирует новый sync (анти-цикл v98)…
        assert "scheduleWatchlistSync" not in _fn_src(js, "_adoptServerWatchlist")
        assert "scheduleWatchlistSync" not in _fn_src(js, "loadProfileWatchlist")
        # …а 409-повтор ограничен одним заходом.
        sync = _fn_src(js, "syncWatchlistToProfile")
        assert "!isRetry" in sync
        assert "syncWatchlistToProfile(profileId, true)" in sync

    def test_subscribe_sends_profile_id(self):
        js = _app_js()
        assert "profile_id" in _fn_src(js, "buildSubscribeBody")
        # Объявление + два вызова: subscribeToPush и existing-ветка
        # setupPushNotifications.
        assert js.count("buildSubscribeBody(") >= 3

    def test_reconcile_skips_when_linked(self):
        # Эндпоинт-reconcile не должен воскрешать замороженный снимок
        # sub.watchlist на связанном устройстве.
        m = re.search(
            r"function reconcileWatchlistWithServer[\s\S]{0,500}?if \(getProfileId\(\)\) return;",
            _app_js(),
        )
        assert m, "reconcileWatchlistWithServer обязан начинаться с гарда профиля."

    def test_sync_sheet_markup_and_handlers(self):
        html = _read("sberbank_dashboard.html")
        for marker in ('id="sync-sheet"', 'id="sync-scrim"', 'id="btn-sync"'):
            assert marker in html, marker
        # Имя фичи — «Синхронизация подписок» (решение юриста 26.08.2026,
        # раннее «Синхронизация звёзд» переименовано).
        assert "Синхронизация подписок" in html
        assert "Синхронизация звёзд" not in html
        js = _app_js()
        for fn in (
            "function openSyncSheet",
            "function closeSyncSheet",
            "function renderSyncSheet",
            "async function requestPairCode",
            "async function submitPairCode",
            "async function unlinkThisDevice",
        ):
            assert fn in js, fn
        assert 'id="sync-code-input"' in js
        # Маска поля: дефис вводить не нужно — нецифры вычищаются на вводе,
        # включая вставку «123-456» из буфера (maxlength с запасом).
        sheet = _fn_src(js, "renderSyncSheet")
        assert "replace(/\\\\D/g" in sheet and "slice(0,6)" in sheet
        assert "confirm(" in _fn_src(js, "unlinkThisDevice"), (
            "Отвязка — разрушающее действие, обязана спрашивать."
        )

    def test_sync_sheet_desktop_popup(self):
        # На десктопе шторка — мини-окно по центру, а не bottom-sheet во всю
        # ширину: базовый .filters-sheet прибит к низу, переопределение живёт
        # в media (min-width:769px) и ведёт появление opacity+pointer-events.
        css = _read("styles.css")
        # Селектор групповой (#sync-sheet, #whatsnew-sheet — общее мини-окно).
        m = re.search(
            r"@media \(min-width: 769px\) \{\s*#sync-sheet,\s*#whatsnew-sheet \{([\s\S]*?)\}",
            css,
        )
        assert m, "Десктопное переопределение #sync-sheet не найдено."
        rules = m.group(1)
        assert "pointer-events:none" in rules
        assert "translateX(-50%)" in rules

    def test_qr_wiring(self):
        # QR несёт deep-link «?pair=<код>» — его читает СИСТЕМНАЯ камера
        # телефона (встроенный сканер в PWA не строим: BarcodeDetector нет в
        # iOS Safari). Генератор — vendored qrcode-gen.js, без CDN.
        assert os.path.exists(os.path.join(ROOT, "qrcode-gen.js")), (
            "qrcode-gen.js (vendored qrcode-generator, MIT) обязан лежать в корне."
        )
        html = _read("sberbank_dashboard.html")
        assert "qrcode-gen.js" in html
        sw = _read("service-worker.js")
        assert "./qrcode-gen.js" in sw, "QR-генератор обязан попасть в APP_SHELL прекэша."
        js = _app_js()
        qr_fn = _fn_src(js, "renderSyncQr")
        assert "?pair=" in qr_fn
        assert "typeof qrcode !== 'function'" in qr_fn, (
            "Без загрузившейся библиотеки QR-блок обязан тихо прятаться."
        )
        pair_fn = _fn_src(js, "maybeHandlePairParam")
        assert "replaceState" in pair_fn, "?pair= вычищается из URL (код одноразовый)."
        assert "submitPairCode" in pair_fn

    def test_scanner_wiring(self):
        # Сканер QR внутри приложения — ради PWA на iOS: хранилище PWA
        # отдельно от Safari, deep-link из системной камеры связал бы не то.
        assert os.path.exists(os.path.join(ROOT, "jsqr.js")), (
            "jsqr.js (vendored jsQR, MIT) обязан лежать в корне."
        )
        html = _read("sberbank_dashboard.html")
        assert "jsqr.js" not in html, (
            "jsqr.js грузится ЛЕНИВО из loadJsQr — статический <script> "
            "возил бы 256 КБ каждому на старте."
        )
        js = _app_js()
        for fn in ("function canScanQr", "function stopSyncScan",
                   "async function startSyncScan", "function extractPairCode"):
            assert fn in js, fn
        assert "jsqr.js?v=" in _fn_src(js, "loadJsQr")
        # Камера не переживает ни закрытие окна, ни пересборку тела.
        assert "stopSyncScan()" in _fn_src(js, "closeSyncSheet")
        assert "stopSyncScan()" in _fn_src(js, "renderSyncSheet")
        # iOS-PWA: без playsinline видео уходит в fullscreen.
        assert "playsinline" in _fn_src(js, "renderSyncScanView")

    @pytest.mark.skipif(NODE is None, reason="node недоступен")
    def test_extract_pair_code(self):
        script = (
            _fn_src(_app_js(), "extractPairCode")
            + """
console.log(JSON.stringify([
  extractPairCode('https://x.io/dash.html?pair=1234-5678'),
  extractPairCode('http://a/b?x=1&pair=12345678'),
  extractPairCode('1234-5678'),
  extractPairCode(' 12345678 '),
  extractPairCode('https://evil.example/'),
]));
"""
        )
        out = _node(script)
        assert out == '["12345678","12345678","12345678","12345678",null]', out

    def test_sync_sheet_blocks_background_refresh(self):
        # Юрист читает/вводит код — фоновая перерисовка не должна дёргать DOM.
        assert "sync-sheet" in _fn_src(_app_js(), "uiBusyForRefresh")


# ── Админка ─────────────────────────────────────────────────────────────────

class TestAdminPage:
    def test_groups_by_profile(self):
        js = _admin()
        body = _fn_src(js, "renderSubsList")
        assert "profile-group" in body
        assert "data-profile-id" in body
        assert "badge-profile" in _fn_src(js, "renderCard")

    def test_fetchall_tolerates_both_shapes(self):
        # Окно отката Worker'а: старый /admin/data отдаёт голый массив.
        assert "Array.isArray(dataJson)" in _fn_src(_admin(), "fetchAll")

    def test_no_template_literals_in_new_code(self):
        # Вся страница — один template literal: backtick во внутреннем JS
        # обрывает его и убивает админку целиком.
        js = _admin()
        for fn in ("renderSubsList", "renderCard", "fetchAll"):
            assert "`" not in _fn_src(js, fn), f"Backtick в {fn}."

    def test_search_matches_profile_id(self):
        assert "profile_id" in _fn_src(_admin(), "subMatches")


# ── Python: контракт «delivery.py не знает о профилях» ──────────────────────

class TestDeliveryFrozen:
    def test_delivery_untouched(self):
        src = _read("scripts/court_monitor/delivery.py")
        # Прежний контракт жив…
        assert 'sub.get("watchlist")' in src
        assert "/subscriptions" in src
        assert "/admin/watchlist" in src
        # …а профильной логики в Python НЕТ: резолв профилей — обязанность
        # Worker'а (/subscriptions и /admin/data отдают watchlist готовым).
        # Появление слова «profile» здесь — сигнал нарушения дизайна.
        assert "profile" not in src.lower()


# ── Сквозная проводка ───────────────────────────────────────────────────────

class TestWiring:
    def test_profile_id_travels_end_to_end(self):
        # фронт шлёт profile_id в /subscribe → Worker переносит/резолвит →
        # /admin/data отдаёт profile_id → админка группирует.
        assert "profile_id" in _fn_src(_app_js(), "buildSubscribeBody")
        worker = _worker()
        assert "prev.profile_id" in _fn_src(worker, "handleSubscribe")
        assert "profile_id:" in _fn_src(worker, "handleAdminData")
        assert "profile-group" in _fn_src(_admin(), "renderSubsList")
