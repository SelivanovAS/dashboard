# -*- coding: utf-8 -*-
"""Стражи календарного фида «Мои заседания» (webcal/.ics, 29.08.2026).

Концепция (утверждена юристом): коллега подписывается на персональный
iCalendar-фид один раз — календарь телефона/Outlook поллит
GET /calendar/<token>.ics сам, события синхронизируются по стабильным UID
(переносы обновляют, отпавшие исчезают — дублей не существует по построению).
Ключевые решения, которые стерегут тесты:

1. profile_id — bearer-секрет и в URL ему не место: у фида СВОЙ производный
   read-only токен (второй uuid) + индекс calfeed:<token> → profile_id.
   Компрометация ссылки = только чтение расписания, лечится перевыпуском.
2. feed_token НЕ трогает profile.updated_at (LWW-штамп watchlist'а):
   запись строго через putProfile, не writeProfileWatchlist.
3. Индекс calfeed:* — без expirationTtl: ссылка живёт в календаре месяцами,
   отзыв — только перевыпуском.
4. Пустой watchlist → валидный ПУСТОЙ календарь (200), не 404: подписка у
   клиента не должна считаться битой.
5. Недоступный cases.json → 503 + Retry-After, НЕ пустой календарь: пустой
   ответ стёр бы события у подписчика (клиенты хранят последнюю копию).
6. UID = <canon>--<stage>@<host>: без даты (перенос обновляет событие),
   со стадией (FI и апелляция сосуществуют), canon стабилен между стадиями.
7. RFC 5545: CRLF-переводы, свёртка строк по 75 ОКТЕТОВ без разреза
   code point (кириллица — 2 байта/буква — главный источник битых .ics).
8. Фронт строит ссылку от ОСНОВНОГО адреса территории (PUSH_WORKER_URL),
   не от sticky-фолбэка workerFetch: ссылка живёт месяцами.
9. Вызов /profile/calendar-token без profile_id неидемпотентен (создаёт
   профиль) → ensureWorkerHost + failover:false (иначе профили-сироты в KV).
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


def _app_js() -> str:
    return _read("app.js")


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# Сборка чистых функций генерации ICS из worker.js для node-тестов.
_ICS_FNS = [
    "icsEscape", "icsFold", "calDateLocal", "calTimeLocal", "calTodayYmd",
    "calSelectHearing", "calHearingPlace", "calCaseIncluded",
    "calBuildCourtLink", "calDtstampUtc", "buildVevent", "buildIcs",
    "wnBareCaseNumber",
]

_ICS_STUBS = """
function cfgVar(n, f) { return f; }
function calTzid() { return "Asia/Yekaterinburg"; }
function calTzOffsetMin() { return 300; }
function siteBaseUrl() { return "https://example.github.io/dashboard"; }
"""


def _ics_bundle() -> str:
    js = _worker()
    return "\n".join(_fn_src(js, n) for n in _ICS_FNS) + _ICS_STUBS


# ── Worker: контракт эндпоинтов и KV-схемы ──────────────────────────────────

class TestWorkerContract:
    def test_router_has_calendar_routes(self):
        js = _worker()
        assert '"/profile/calendar-token"' in js, "Роут выдачи токена не заведён."
        assert re.search(r"/\^\\/calendar\\/.+\.ics", js) or "/calendar/" in js, \
            "Роут фида /calendar/<token>.ics не заведён."
        assert "handleCalendarFeed(request, env, calMatch[1])" in js

    def test_feed_token_does_not_touch_lww(self):
        # Решение 2: токен не трогает updated_at (LWW-штамп watchlist'а).
        body = _fn_src(_worker(), "handleProfileCalendarToken")
        assert "writeProfileWatchlist(" not in body, \
            "Токен фида не должен писаться через writeProfileWatchlist (сдвинет LWW)."
        assert not re.search(r"profile\.updated_at\s*=(?!=)", body.split("} else {")[-1]), \
            "Выдача токена существующему профилю не должна менять updated_at."
        assert "putProfile" in body

    def test_calfeed_index_without_ttl(self):
        # Решение 3: индекс вечный, отзыв — только перевыпуском.
        body = _fn_src(_worker(), "handleProfileCalendarToken")
        m = re.search(r"put\(\s*\n?\s*feedTokenKey\(token\)[\s\S]*?\)", body)
        assert m, "Запись calfeed:<token> не найдена."
        assert "expirationTtl" not in m.group(0), \
            "Индекс calfeed:* должен писаться БЕЗ expirationTtl."

    def test_regenerate_deletes_old_index(self):
        body = _fn_src(_worker(), "handleProfileCalendarToken")
        assert "delete(feedTokenKey(oldToken))" in body, \
            "Перевыпуск обязан удалять старый индекс — иначе старая ссылка живёт вечно."

    def test_feed_headers(self):
        body = _fn_src(_worker(), "handleCalendarFeed")
        assert "text/calendar; charset=utf-8" in body
        assert re.search(r'"Cache-Control":\s*"private', body), \
            "Ответ с токеном в URL нельзя класть в shared-кэши (private обязателен)."

    def test_empty_watchlist_returns_valid_calendar(self):
        # Решение 4: пустой набор — пустой календарь, не ошибка.
        body = _fn_src(_worker(), "handleCalendarFeed")
        m = re.search(r"if \(!watch\.length\) \{[\s\S]*?\}", body)
        assert m, "Ветка пустого watchlist не найдена."
        assert "buildIcs" in m.group(0), \
            "Пустой watchlist обязан отдавать валидный VCALENDAR (200)."

    def test_unavailable_cases_returns_503(self):
        # Решение 5: без данных фид не притворяется пустым.
        body = _fn_src(_worker(), "handleCalendarFeed")
        m = re.search(r"if \(!casesJson\) \{[\s\S]*?\}\s*\)", body)
        assert m, "Ветка недоступного cases.json не найдена."
        assert "503" in m.group(0) and "Retry-After" in m.group(0), \
            "Недоступный cases.json → 503 + Retry-After (клиент хранит копию)."

    def test_no_full_token_in_logs(self):
        # Решение 1: полные секреты в логи не попадают.
        js = _worker()
        for fn in ("handleProfileCalendarToken", "handleCalendarFeed"):
            body = _fn_src(js, fn)
            for m in re.finditer(r"console\.(log|warn)\([\s\S]*?\);", body):
                call = m.group(0)
                if "token" in call:
                    assert "slice(0, 8)" in call, \
                        f"{fn}: токен в логе должен быть срезан до 8 символов."
                if "profileId" in call:
                    assert "shortProfileId" in call, \
                        f"{fn}: profile_id в логе только через shortProfileId."

    def test_feed_checks_token_matches_profile(self):
        # Страховка от недоудалённого старого индекса.
        body = _fn_src(_worker(), "handleCalendarFeed")
        assert "profile.feed_token !== token" in body

    def test_alias_map_uses_shared_fetch(self):
        # Рефакторинг: канонизация и фид едят один загрузчик с edge-кэшем.
        js = _worker()
        assert "fetchJsonCached" in _fn_src(js, "getAliasMapCached")
        assert "cacheTtl: 300" in _fn_src(js, "fetchJsonCached")


# ── Генерация ICS: RFC 5545 на настоящем движке ─────────────────────────────

@pytest.mark.skipif(NODE is None, reason="node недоступен")
class TestIcsGeneration:
    _CASE = """
const c = {id: "2-2182/2026", current_stage: "first_instance", bank_role: "Ответчик",
  plaintiff: "Иванов Иван Иванович", defendant: "ПАО Сбербанк",
  category: "Иски о защите прав потребителей",
  first_instance: {hearing_date: "29.09.2026", hearing_time: "17:20",
    court: "Сургутский районный суд", court_domain: "surgray--hmao.sudrf.ru",
    judge: "Тюленев В.В.", link: "281703271|ffc04471-99cd-477c-ae8a-61a98a61cdbd",
    srv_num: 1, delo_id: 1540005,
    last_event: "Подготовка дела (собеседование). 17:20. каб. №304. 27.08.2026",
    events: [{date: "29.09.2026", time: "17:20", place: "каб. №304"}]}};
"""

    def _run(self, tail: str) -> str:
        return _node(_ics_bundle() + self._CASE + tail)

    def test_ics_structure_and_crlf(self):
        out = self._run("""
const sel = calSelectHearing(c);
const ics = buildIcs(buildVevent(sel, c, "h.example", "Asia/Yekaterinburg"),
  "Asia/Yekaterinburg", "Мои заседания");
console.log(JSON.stringify({
  crlf: ics.includes("\\r\\n") && !/[^\\r]\\n/.test(ics),
  ver: ics.includes("VERSION:2.0"),
  tz: ics.includes("TZOFFSETTO:+0500"),
  name: ics.includes("X-WR-CALNAME:Мои заседания"),
  refresh: ics.includes("REFRESH-INTERVAL;VALUE=DURATION:PT6H"),
}));""")
        assert out == '{"crlf":true,"ver":true,"tz":true,"name":true,"refresh":true}'

    def test_fold_75_octets_no_broken_codepoints(self):
        # Решение 7: свёртка по октетам, кириллица не режется посреди буквы.
        out = self._run("""
const sel = calSelectHearing(c);
const ics = buildIcs(buildVevent(sel, c, "h.example", "Asia/Yekaterinburg"),
  "Asia/Yekaterinburg", "Мои заседания");
const enc = new TextEncoder();
const lines = ics.split("\\r\\n");
const tooLong = lines.filter((l) => enc.encode(l).length > 75).length;
const broken = lines.filter((l) => l.includes("\\uFFFD")).length;
// восстановленный текст после развёртки должен совпасть с исходным SUMMARY
const unfolded = ics.replace(/\\r\\n /g, "");
console.log(JSON.stringify({tooLong, broken,
  summary: unfolded.includes("SUMMARY:Заседание 2-2182/2026 — Иванов Иван Иванович")}));""")
        assert out == '{"tooLong":0,"broken":0,"summary":true}'

    def test_uid_stable_across_reschedule(self):
        # Решение 6: перенос заседания НЕ меняет UID — событие обновляется.
        out = self._run("""
const sel1 = calSelectHearing(c);
const c2 = JSON.parse(JSON.stringify(c));
c2.first_instance.hearing_date = "15.10.2026";
const sel2 = calSelectHearing(c2);
const uid = (lines) => lines.find((l) => l.startsWith("UID:"));
console.log(JSON.stringify({
  same: uid(buildVevent(sel1, c, "h", "T")) === uid(buildVevent(sel2, c2, "h", "T")),
  val: uid(buildVevent(sel1, c, "h", "T")),
}));""")
        assert '"same":true' in out
        assert '"val":"UID:2-2182/2026--first_instance@h"' in out

    def test_all_day_without_time(self):
        out = self._run("""
c.first_instance.hearing_time = "";
const ev = buildVevent(calSelectHearing(c), c, "h", "Asia/Yekaterinburg");
console.log(JSON.stringify(ev.filter((l) => l.startsWith("DTSTART") || l.startsWith("DTEND"))));""")
        assert out == '["DTSTART;VALUE=DATE:20260929","DTEND;VALUE=DATE:20260930"]'

    def test_timed_event_plus_hour(self):
        out = self._run("""
const ev = buildVevent(calSelectHearing(c), c, "h", "Asia/Yekaterinburg");
console.log(JSON.stringify(ev.filter((l) => l.startsWith("DTSTART") || l.startsWith("DTEND"))));""")
        assert out == ('["DTSTART;TZID=Asia/Yekaterinburg:20260929T172000",'
                       '"DTEND;TZID=Asia/Yekaterinburg:20260929T182000"]')

    def test_case_filters(self):
        out = self._run("""
const sel = calSelectHearing(c);
const past = calCaseIncluded(sel, "20261001");
const today = calCaseIncluded(sel, "20260929");
const future = calCaseIncluded(sel, "20260801");
c.first_instance.last_event = "Производство приостановлено (экспертиза)";
const suspended = calCaseIncluded(calSelectHearing(c), "20260801");
c.first_instance.last_event = "Оставлено без движения до 15.10.2026";
const nomove = calCaseIncluded(calSelectHearing(c), "20260801");
console.log(JSON.stringify({past, today, future, suspended, nomove}));""")
        assert out == '{"past":false,"today":true,"future":true,"suspended":false,"nomove":false}'

    def test_ics_escape(self):
        out = self._run("""
console.log(icsEscape("а;б,в\\\\г\\nд"));""")
        assert out == "а\\;б\\,в\\\\г\\nд"

    def test_appeal_block_selection(self):
        # Зеркало jsonToCase: в cassation_watch активный блок — апелляция.
        out = self._run("""
c.current_stage = "cassation_watch";
c.appeal = {case_number: "33-1234/2026", hearing_date: "10.10.2026",
  hearing_time: "10:00", court: "Суд ХМАО"};
const sel = calSelectHearing(c);
console.log(JSON.stringify({stage: sel.stage, canon: sel.canon}));""")
        assert out == '{"stage":"appeal","canon":"2-2182/2026"}'


# ── Фронт: контракт кнопки подписки ─────────────────────────────────────────

class TestFrontendContract:
    def test_feed_url_from_primary_host(self):
        # Решение 8: ссылка — от канонического адреса территории.
        body = _fn_src(_app_js(), "calFeedHttpsUrl")
        assert "PUSH_WORKER_URL" in body
        assert "WORKER_HOSTS" not in body, \
            "Ссылка фида не должна зависеть от sticky-фолбэка workerFetch."

    def test_request_is_guarded_like_link_code(self):
        # Решение 9: неидемпотентный вызов — только на проверенный адрес.
        body = _fn_src(_app_js(), "requestCalendarFeed")
        assert "ensureWorkerHost" in body
        assert "failover: false" in body

    def test_clear_profile_clears_feed_token(self):
        assert "clearCalFeedToken" in _fn_src(_app_js(), "clearProfileLink")

    def test_profile_switch_clears_feed_token(self):
        # Связка кодом переводит устройство на ДРУГОЙ профиль: кэш токена
        # принадлежит покинутому — без сброса модалка показывала бы рабочую
        # ссылку на календарь со старым watchlist.
        body = _fn_src(_app_js(), "setProfileLink")
        assert "clearCalFeedToken" in body
        assert re.search(r"prev\s*&&\s*prev\s*!==", body), \
            "Сброс — только при смене id (тот же профиль кэш не трогает)."

    def test_webcal_scheme(self):
        body = _fn_src(_app_js(), "calFeedWebcalUrl")
        assert "webcal:" in body

    def test_sync_sheet_has_calendar_block(self):
        js = _app_js()
        body = _fn_src(js, "renderSyncSheet")
        assert body.count("calFeedBlockHtml()") >= 2, \
            "Блок календаря должен быть и у связанных, и у несвязанных устройств."
        assert "subscribeCalendar()" in _fn_src(js, "calFeedBlockHtml"), \
            "Главная кнопка блока — умная subscribeCalendar (один тап)."

    def test_subscribe_is_one_tap_and_platform_aware(self):
        # «Минимум действий»: кнопка сама добывает токен и сразу открывает
        # календарь. Apple — webcal:, остальные — Google «добавить по URL».
        js = _app_js()
        body = _fn_src(js, "subscribeCalendar")
        assert "requestCalendarFeed" in body, "Токен добывается на клике сам."
        assert "calIsApplePlatform()" in body
        assert "calFeedWebcalUrl" in body
        assert "calFeedGoogleUrl" in body
        # Попап-блокер после await: window.open с фолбэком location.href.
        assert "window.open" in body and "location.href = url" in body
        assert "calendar.google.com/calendar/render?cid=" in _fn_src(js, "calFeedGoogleUrl")

    def test_request_returns_token(self):
        # subscribeCalendar/copyCalFeedUrl ждут токен ВОЗВРАТОМ, не через кэш.
        body = _fn_src(_app_js(), "requestCalendarFeed")
        assert "return data.token" in body
        assert "return ''" in body, "Ошибочные ветки обязаны возвращать ''."
