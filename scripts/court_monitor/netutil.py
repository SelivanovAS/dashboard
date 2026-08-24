# -*- coding: utf-8 -*-
"""Сетевой слой: общая requests-сессия, вежливая задержка, загрузка страниц
судов (win-1251) с ретраями. Счётчики пишутся в config.METRICS.
"""

from __future__ import annotations

import errno
import math
import random
import re
import socket
import time
from http.client import RemoteDisconnected
from urllib.parse import urlsplit

import requests

from court_monitor import config, telemetry
from court_monitor.config import log

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
})




def polite_delay():
    """Случайная задержка между запросами."""
    time.sleep(random.uniform(*config.REQUEST_DELAY))


def _set_diag(kind: str, url: str, **extra) -> None:
    """Записать класс последнего ответа в config.FETCH_DIAG (одна точка).

    Диагноз перезаписывается КАЖДЫМ запросом — читать его надо сразу после
    вызова fetch_*, пока следующий не затёр. Поэтому же здесь, и только здесь,
    ведётся НАКОПИТЕЛЬНЫЙ счёт отказов по классам: FETCH_DIAG живёт до
    следующего запроса, а сводка прогона должна пережить весь обход.
    """
    config.FETCH_DIAG.clear()
    config.FETCH_DIAG.update(
        {"kind": kind, "host": urlsplit(url).netloc or url, **extra})
    if kind != "ok":
        config.FETCH_FAIL_KINDS[kind] = config.FETCH_FAIL_KINDS.get(kind, 0) + 1
        elapsed = extra.get("elapsed")
        if isinstance(elapsed, (int, float)):
            config.FETCH_FAIL_TIMINGS.setdefault(kind, []).append(float(elapsed))


def _exception_chain(exc: BaseException):
    """Обойти реальные вложенные исключения requests/urllib3 без парсинга текста."""
    pending = [exc]
    seen: set[int] = set()
    while pending:
        cur = pending.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        yield cur
        for linked in (getattr(cur, "__cause__", None),
                       getattr(cur, "__context__", None)):
            if isinstance(linked, BaseException):
                pending.append(linked)
        # urllib3 прячет первопричину не только в args/cause: MaxRetryError
        # использует `.reason`, NameResolutionError — `._reason`. Без этого
        # реальные DNS/reset-цепочки requests схлопывались в connection_error,
        # хотя упрощённые unit-исключения классифицировались правильно.
        for attr in ("reason", "_reason", "original_error"):
            linked = getattr(cur, attr, None)
            if isinstance(linked, BaseException):
                pending.append(linked)
        for arg in getattr(cur, "args", ()):
            if isinstance(arg, BaseException):
                pending.append(arg)


def transport_fail_kind(exc: requests.RequestException) -> str:
    """Единственная классификация транспортного отказа fetch_page.

    Порядок load-bearing: специализированные requests-классы проверяются до
    общих Timeout/ConnectionError, затем разбирается цепочка urllib3/socket.
    Downstream получает готовый kind и ничего не классифицирует повторно.
    """
    if isinstance(exc, requests.ReadTimeout):
        return "read_timeout"
    if isinstance(exc, requests.ConnectTimeout):
        return "connect_timeout"
    if isinstance(exc, requests.exceptions.SSLError):
        return "tls_error"
    if isinstance(exc, requests.exceptions.ProxyError):
        return "proxy_error"
    if isinstance(exc, requests.exceptions.TooManyRedirects):
        return "redirect_error"
    chain = list(_exception_chain(exc))
    if any(isinstance(x, socket.gaierror) for x in chain):
        return "dns_error"
    reset_errnos = {errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE}
    if any(
        isinstance(x, (ConnectionResetError, ConnectionAbortedError,
                       BrokenPipeError, RemoteDisconnected))
        or (isinstance(x, OSError) and getattr(x, "errno", None) in reset_errnos)
        for x in chain
    ):
        return "connection_reset"
    # ChunkedEncodingError commonly wraps urllib3.ProtocolError and the real
    # ConnectionResetError. Only an unqualified corrupt/truncated body is the
    # generic response_error; a proved reset gets the selective fast retry.
    if isinstance(exc, (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ContentDecodingError,
    )):
        return "response_error"
    if isinstance(exc, requests.ConnectionError):
        return "connection_error"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    return "request_error"


_FAST_RETRY_KINDS = frozenset({
    "connection_reset",
    "http_500", "http_502", "http_503", "http_504",
})


def should_retry_fetch(kind: str, elapsed: float, attempt: int) -> bool:
    """Повторять только быстрый, plausibly transient отказ.

    ``FETCH_MAX_RETRIES`` — потолок, а не приказ повторять всё подряд.
    Долгий ReadTimeout, connect-timeout, TLS/DNS, 4xx, CAPTCHA и семантическая
    заглушка завершаются с первой попытки. Мгновенный reset и быстрый 5xx
    можно безопасно добрать: именно reset мигал по тем же хостам 21.08.2026.
    Порог elapsed страхует от reset после долгой частичной передачи ответа.
    """
    return (
        attempt < config.FETCH_MAX_RETRIES
        and kind in _FAST_RETRY_KINDS
        and 0 <= float(elapsed) <= config.FETCH_RETRY_FAST_MAX_SECONDS
    )


def _latency_summary(values: list[float]) -> dict:
    """Nearest-rank percentiles; корректно и для чётного размера выборки."""
    xs = sorted(values)
    if not xs:
        return {}

    def pct(p: float) -> float:
        idx = max(0, math.ceil(p * len(xs)) - 1)
        return xs[min(len(xs) - 1, idx)]

    return {
        "n": len(xs),
        "p50": round(pct(0.50), 1),
        "p90": round(pct(0.90), 1),
        "max": round(xs[-1], 1),
    }


def fetch_latency_summary() -> dict:
    """Персентили времени ответа за прогон (пусто — если замеров не было).

    Считаем по УСПЕШНЫМ ответам: у отказа времени нет — есть только потолок
    таймаута, и подмешивать его значило бы мерить свою настройку, а не портал.
    """
    return _latency_summary(config.FETCH_TIMINGS)


def fetch_failure_latency_summary() -> dict:
    """Перцентили времени отказа по точному классу за текущий прогон."""
    return {
        kind: summary
        for kind, values in sorted(config.FETCH_FAIL_TIMINGS.items())
        if (summary := _latency_summary(values))
    }


# Страница защиты ГАС «Правосудие» печатает в теле наш адрес и букву правила:
# «… (B) : ip: 43.245.226.66 Host: … Australia». Это ровно тот факт, ради
# которого пробу и заводили, — вытаскиваем оба.
_BLOCK_IP_RE = re.compile(r"ip:\s*([0-9a-fA-F.:]{7,45})")
_BLOCK_RULE_RE = re.compile(r"\(([A-Z])\)")


def block_page_marks(html: str) -> dict:
    """Наш IP и буква правила со страницы защиты (что нашлось), иначе {}.

    Тело приходит в win-1251, но нужные куски — ASCII, кодировка не мешает.
    """
    out = {}
    if not html:
        return out
    m = _BLOCK_IP_RE.search(html)
    if m:
        out["ip"] = m.group(1)
    m = _BLOCK_RULE_RE.search(html)
    if m:
        out["rule"] = m.group(1)
    return out


# Человеческие формулировки классов отказа — ОДНО место на все каналы
# (импорт дампа, точечное добавление, отчёт парсинга трека).
_FAIL_REASON_RU = {
    "captcha": "карточка закрыта проверочным кодом",
    "blocked": "суд заблокировал запрос (страница защиты ГАС)",
    "breaker": "суд снят с обхода после нескольких неудач подряд",
    "empty": "суд вернул пустой ответ",
    "empty_shell": "вместо карточки пришла страница без данных",
    "empty_act": "страница судебного акта не содержит текста",
    "captcha_search": "поиск суда закрыт проверочным кодом",
    "outage_search": "вместо поиска суд вернул заглушку портала",
    "empty_search": "поиск суда вернул страницу без распознанных дел",
    "degraded_card": "суд вернул неполную карточку без движения дела",
    "unparsed_card": "карточка суда не распознана парсером",
    "read_timeout": "суд не отдал данные до таймаута чтения",
    "connect_timeout": "соединение с судом не установилось вовремя",
    "connection_reset": "суд оборвал соединение",
    "connection_error": "соединение с судом не установлено",
    "dns_error": "адрес суда не разрешился через DNS",
    "tls_error": "ошибка защищённого соединения TLS",
    "proxy_error": "ошибка прокси или сетевого маршрута",
    "redirect_error": "суд зациклил перенаправления",
    "response_error": "суд оборвал или повредил передачу ответа",
    "timeout": "сетевой таймаут",
    "request_error": "сетевая ошибка запроса",
    # Старые сохранённые диагнозы/тестовые фикстуры остаются читаемыми.
    "network": "сеть недоступна или таймаут",
}


def fetch_fail_reason_ru(diag: dict | None = None) -> str:
    """Причина отказа по-русски для отчёта оператору.

    diag=None — берём текущий config.FETCH_DIAG. Неизвестный класс не выдумываем:
    отдаём пустую строку, вызыватель оставит прежнюю формулировку.
    """
    d = config.FETCH_DIAG if diag is None else diag
    kind = (d or {}).get("kind", "")
    if kind.startswith("http_"):
        code = kind[5:]
        # 403 — самый частый и самый информативный случай: это не «сайт лёг»,
        # а «нас не пускают». Остальные коды называем как есть.
        tail = " — адрес заблокирован" if code == "403" else ""
        text = f"суд отвечает HTTP {code}{tail}"
    else:
        text = _FAIL_REASON_RU.get(kind, "")
    if not text:
        return ""
    ip = (d or {}).get("ip")
    return f"{text} (наш адрес {ip})" if ip else text


def mark_last_fetch_semantic(
    kind: str,
    url: str,
    *,
    context: str | None = None,
    **details,
) -> None:
    """Уточнить semantic verdict последнего fetch_page без нового запроса.

    Поиски кассации/апелляции/FI валидируются уже в runs.py: только после
    parse_* и детекторов видно, был HTTP 200 настоящей выдачей, CAPTCHA или
    заглушкой. Берём request_id из единого FETCH_DIAG сразу после fetch_page;
    telemetry.classify_semantic заменяет verdict, но не двигает transport
    counters. Полного URL и HTML в checkpoint не передаём.
    """
    host = urlsplit(url).netloc or url
    diag = config.FETCH_DIAG
    request_id = (
        str(diag.get("request_id") or "")
        if str(diag.get("host") or "") == host
        else ""
    )
    # Semantic-отказ должен быть виден не только в локальном checkpoint, но и
    # в публичном last_run.fail_kinds/failure_latency. Валидный ответ оставляет
    # transport-diag ``ok``; ошибочный заменяет его тем же request_id и
    # исходным elapsed, не создавая второй HTTP-запрос.
    if kind not in (
        "ok", "valid", "valid_card", "valid_search", "valid_act",
    ):
        preserved = {
            key: diag[key]
            for key in ("status", "elapsed", "attempt", "request_id")
            if key in diag
        }
        _set_diag(kind, url, context=context, **preserved, **details)
    telemetry.classify_semantic(
        request_id or None,
        kind,
        host=host,
        context=context or "",
        **details,
    )


def fetch_page(url: str, *, context: str | None = None) -> str:
    """Скачать страницу с сайта суда (win-1251) с повторными попытками.

    context — короткая метка «что грузим» (номер дела, имя суда, «поиск
    апелляции»): попадает и в WARNING ретрая, и в финальный ERROR, чтобы
    ошибка сети сразу привязывалась к делу/суду одной строкой.

    Побочно заполняет config.FETCH_DIAG — класс ответа для отчёта оператору
    (HTTP-код здесь единственное место, где вообще виден: наружу
    raise_for_status отдаёт только исключение).
    """
    ctx = f" ({context})" if context else ""
    host = urlsplit(url).netloc or url
    # Один id на логический fetch_page; все физические ретраи передают его
    # дальше. При выключенной локальной телеметрии begin_fetch — no-op и
    # возвращает пустую строку.
    request_id = ""
    for attempt in range(1, config.FETCH_MAX_RETRIES + 1):
        request_id = telemetry.begin_fetch(
            host,
            context or "",
            attempt=attempt,
            max_attempts=config.FETCH_MAX_RETRIES,
            request_id=request_id or None,
        )
        try:
            # ⚠️ Таймаут РАЗДЕЛЬНЫЙ (connect, read) — это разные события, и
            # одна мерка на оба неверна. Соединение с судом встаёт за 0,05 с,
            # так что 10 с — с запасом, и мёртвая сеть видна быстро. А вот
            # ОТВЕТ портал готовит долго: замер 24.08.2026 дал 26–58 с на
            # sud_delo при 0,2 с на корень сайта, и прежние общие 30 с рубили
            # верхнюю половину распределения — прогон 06:00 прочитал 20
            # карточек из 287, потеряв всё, что отвечало медленнее 30 с.
            # Верхнюю границу задаёт САМ сервер: проба висела 60,26 с, после
            # чего портал разорвал соединение без ответа, — ждать дольше
            # нечего, дожидаться уже некого. Отсюда 65: весь наблюдавшийся
            # разброс плюс поля, и ни секунды сверх предела портала.
            # Значения — в config (FETCH_TIMEOUT_CONNECT/READ, env-рычаг):
            # в такое утро крутить их правкой кода с коммитом — не вариант.
            # На здоровом дне правка невидима: таймаут — потолок, а не
            # задержка, быстрый ответ возвращается быстро.
            _t0 = time.monotonic()
            r = session.get(url, timeout=(config.FETCH_TIMEOUT_CONNECT,
                                          config.FETCH_TIMEOUT_READ))
            r.raise_for_status()
            elapsed = time.monotonic() - _t0
            config.FETCH_TIMINGS.append(elapsed)
            config.METRICS["requests_ok"] += 1
            if attempt > 1:
                config.METRICS["requests_retried"] += 1
            text = r.content.decode("windows-1251", errors="replace")
            if not text:
                # HTTP 200 с пустым телом — аномалия: без лога такой ответ
                # исчезал бы бесследно (вызыватели молча скипают "").
                host = urlsplit(url).netloc or url
                log.warning(f"Пустой ответ (HTTP {r.status_code}): {host}{ctx}")
            _set_diag(
                "ok" if text else "empty", url, status=r.status_code,
                elapsed=elapsed, context=context, attempt=attempt,
                request_id=request_id,
            )
            telemetry.finish_fetch_transport(
                request_id,
                f"http_{r.status_code}",
                elapsed,
                attempt=attempt,
                status=r.status_code,
            )
            if not text:
                telemetry.classify_semantic(
                    request_id, "empty", host=host, context=context or ""
                )
            return text
        except requests.RequestException as e:
            elapsed = time.monotonic() - _t0
            status = getattr(getattr(e, "response", None), "status_code", None)
            kind = f"http_{status}" if status else transport_fail_kind(e)
            will_retry = should_retry_fetch(kind, elapsed, attempt)
            _set_diag(kind, url,
                      status=status, error=type(e).__name__, elapsed=elapsed,
                      context=context, attempt=attempt, request_id=request_id,
                      **(block_page_marks(getattr(e.response, "text", ""))
                         if status else {}))
            telemetry.finish_fetch_transport(
                request_id,
                kind,
                elapsed,
                attempt=attempt,
                will_retry=will_retry,
                status=status,
                error=type(e).__name__,
            )
            if will_retry:
                # Промежуточная попытка: хост + контекст + класс ошибки, без
                # простыни с полным URL (он уйдёт в финальный ERROR, если все
                # попытки исчерпаются).
                wait = attempt * 5
                log.warning(
                    f"Попытка {attempt}/{config.FETCH_MAX_RETRIES}: {host}{ctx} — "
                    f"{kind} за {elapsed:.1f}с, повтор через {wait}с..."
                )
                time.sleep(wait)
            else:
                config.METRICS["requests_failed"] += 1
                no_retry = (
                    f"; класс {kind} не ретраим"
                    if attempt < config.FETCH_MAX_RETRIES else ""
                )
                log.error(
                    f"Ошибка загрузки {url}{ctx} "
                    f"после {attempt} попыток{no_retry}: {e}"
                )
                break
    return ""


# ── Пер-суд предохранитель карточек (circuit breaker) ────────────────────────
# Аутейдж Сургутского городского 29.07.2026: суд отдавал заглушку на каждой
# карточке, а прогон впустую молотил polite_delay + HTTP по всем его делам.
# Состояние — config.CARD_BREAKER (живёт один прогон, сброс в _metrics_reset),
# ключ — хост суда из URL карточки: одна точка в fetch_card_checked накрывает
# все вызовы (FI-цикл, апелляция, кассация 7kas, тексты актов, бэкфиллы,
# ручные скрипты). Мутируют состояние ТОЛЬКО хелперы ниже.


def _card_breaker_entry(host: str) -> dict:
    return config.CARD_BREAKER.setdefault(host, {
        "fails": 0, "open": False, "reason": "",
        "skipped": 0, "probes": 0, "preopened": False,
    })


def card_breaker_open(host: str) -> bool:
    """READ-ONLY: True, если предохранитель хоста открыт (суд отключён)."""
    return bool(config.CARD_BREAKER.get(host, {}).get("open"))


def card_breaker_allows(host: str) -> bool:
    """МУТИРУЮЩИЙ гейт «фетчить карточку или пропустить».

    Ровно ОДИН вызов на попытку карточки: либо внутри fetch_card_checked
    (breaker_gate=True, дефолт), либо пре-чеком горячего цикла ДО
    polite_delay (тогда fetch зовётся с breaker_gate=False) — двойной вызов
    задваивал бы счётчик пропусков и ломал каденс half-open проб.
    Закрытый предохранитель → True. Открытый: пропуск с инкрементом
    счётчиков, кроме каждой K-й карточки (config.CARD_BREAKER_PROBE_EVERY) —
    она пропускается как проба: успех закроет предохранитель
    (_card_breaker_ok), и хвост суда дочитается этим же прогоном.
    """
    if not config.CARD_BREAKER_THRESHOLD:
        return True
    entry = config.CARD_BREAKER.get(host)
    if not entry or not entry.get("open"):
        return True
    entry["skipped"] += 1
    every = config.CARD_BREAKER_PROBE_EVERY
    if every and entry["skipped"] % every == 0:
        entry["probes"] += 1
        log.debug(f"Предохранитель {host}: half-open проба #{entry['probes']}")
        return True
    config.METRICS["cards_breaker_skipped"] += 1
    return False


def _card_breaker_fail(host: str, reason: str) -> None:
    """Учесть не прочитанную карточку хоста; на пороге — отключить суд."""
    if not config.CARD_BREAKER_THRESHOLD or not host:
        return
    entry = _card_breaker_entry(host)
    entry["fails"] += 1
    entry["reason"] = reason
    if not entry["open"] and entry["fails"] >= config.CARD_BREAKER_THRESHOLD:
        entry["open"] = True
        probe_note = (
            f", проба каждые {config.CARD_BREAKER_PROBE_EVERY} карточек"
            if config.CARD_BREAKER_PROBE_EVERY else ""
        )
        log.warning(
            f"Суд {host}: {entry['fails']} карточек подряд не прочитано "
            f"({reason}) — обход приостановлен до конца прогона{probe_note}"
        )


def _card_breaker_ok(host: str) -> None:
    """Успешная карточка: сброс счётчика; открытый предохранитель — закрыть."""
    entry = config.CARD_BREAKER.get(host)
    if not entry:
        return
    entry["fails"] = 0
    if entry.get("open"):
        entry["open"] = False
        log.info(f"Суд {host}: снова отдаёт карточки — обход возобновлён")


def card_breaker_preopen(host: str, reason: str) -> None:
    """Канарейка: открыть предохранитель заранее, не потратив ни карточки.

    Зовётся из фазы поиска main_json: страница ПОИСКА суда грузится раньше
    обхода карточек, и заглушка на ней (looks_like_outage_page) означает
    «портал лежит». ⚠️ Только заглушка — капча на поиске предохранитель НЕ
    открывает: это штатный режим капчёвых судов (search_gated: поиск закрыт,
    карточки живут и мониторятся). Если карточки вопреки канарейке живы —
    первая же half-open проба (card_breaker_allows) вернёт суд в обход.
    """
    if not config.CARD_BREAKER_THRESHOLD or not host:
        return
    entry = _card_breaker_entry(host)
    if entry["open"]:
        return
    entry["open"] = True
    entry["preopened"] = True
    entry["reason"] = reason
    probe_note = (
        f", проба каждые {config.CARD_BREAKER_PROBE_EVERY} карточек"
        if config.CARD_BREAKER_PROBE_EVERY else ""
    )
    log.warning(f"Суд {host}: {reason} — карточки не запрашиваем{probe_note}")


def fetch_card_checked(url: str, *, context: str | None = None,
                       breaker_gate: bool = True) -> str:
    """Скачать КАРТОЧКУ дела с проверкой «а не заглушка ли это вместо неё».

    Два класса не-карточных ответов, оба с HTTP 200:
    - проверочный код (Свердловск, замер 15.07.2026): WARNING + счётчик
      METRICS["cards_captcha"];
    - заглушка недоступности / антибот-блок (аутейдж sudrf 20.07.2026
      «Информация временно недоступна…»): WARNING + METRICS["cards_blocked"].
    Без классификации parse_case_card молча распарсил бы такую страницу как
    пустой «огрызок», а FI-цикл засчитал бы её успешной проверкой (бумп
    last_checked_at). Оба случая → "" — дело пропускается этим прогоном,
    его данные не портятся, следующий прогон перечитает.

    Пер-суд предохранитель: не прочитанные карточки (заглушка/код/сеть)
    копят счётчик хоста (_card_breaker_fail), успех сбрасывает
    (_card_breaker_ok); отключённый суд гейтится card_breaker_allows.
    breaker_gate=False — вызыватель уже спросил card_breaker_allows сам
    (пре-чек горячего цикла до polite_delay), второй раз не спрашиваем.

    READ-ONLY: код не читаем и не решаем — только распознаём страницу
    (см. detect_captcha_challenge_card / looks_like_non_card_page).
    """
    host = urlsplit(url).netloc
    if breaker_gate and not card_breaker_allows(host):
        # Запроса не было вовсе — иначе оператор прочитал бы в отчёте диагноз
        # ПРЕДЫДУЩЕЙ карточки и решил, что этой суд ответил сам.
        _set_diag("breaker", url)
        telemetry.classify_semantic(
            None, "breaker", host=host, context=context or ""
        )
        return ""
    html = fetch_page(url, context=context)
    if not html:
        _card_breaker_fail(host, "сеть/пустой ответ")
        return ""
    # fetch_page оставляет id/elapsed последней HTTP-попытки в едином
    # FETCH_DIAG. Semantic verdict ниже уточняет ЭТОТ ответ и не создаёт
    # вторую попытку в checkpoint.
    request_id = str(config.FETCH_DIAG.get("request_id") or "")
    elapsed = config.FETCH_DIAG.get("elapsed")
    # Ленивый импорт: netutil — низкоуровневый слой, тащить parsing (courts,
    # tables) на уровень модуля значило бы завязать сеть на парсеры.
    from court_monitor.parsing.search import (
        detect_captcha_challenge_card,
        looks_like_non_card_page,
    )
    ctx = f" ({context})" if context else ""
    if detect_captcha_challenge_card(html):
        config.METRICS["cards_captcha"] += 1
        log.warning(f"Карточка закрыта проверочным кодом{ctx}: {url}")
        _set_diag(
            "captcha", url, status=200, request_id=request_id,
            elapsed=elapsed, context=context,
        )
        telemetry.classify_semantic(
            request_id or None, "captcha", host=host, context=context or ""
        )
        _card_breaker_fail(host, "проверочный код")
        return ""
    if looks_like_non_card_page(html, url):
        config.METRICS["cards_blocked"] += 1
        # Страница защиты ГАС приходит с HTTP 200 — по коду её не отличить от
        # успеха, диагноз ставится по телу. Наш адрес из него забираем: он и
        # объясняет, почему тот же URL с другой машины открывается.
        marks = block_page_marks(html)
        log.warning(
            f"Карточка не получена — портал недоступен/заглушка{ctx}: {url}"
            + (f" (наш адрес {marks['ip']})" if marks.get("ip") else "")
        )
        _set_diag(
            "blocked", url, status=200, request_id=request_id,
            elapsed=elapsed, context=context, **marks,
        )
        telemetry.classify_semantic(
            request_id or None,
            "blocked",
            host=host,
            context=context or "",
            **marks,
        )
        _card_breaker_fail(host, "заглушка/блок портала")
        return ""
    _card_breaker_ok(host)
    telemetry.classify_semantic(
        request_id or None, "valid_card", host=host, context=context or ""
    )
    return html
