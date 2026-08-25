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
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from http.client import RemoteDisconnected
from typing import Any, Iterable
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

_RUN_DEADLINE_AT = 0.0
_RUN_DEADLINE_REPORTED = False


def start_run_deadline(seconds: float | None = None) -> None:
    """Начать общий monotonic-бюджет; 0 выключает ограничение."""
    global _RUN_DEADLINE_AT, _RUN_DEADLINE_REPORTED
    budget = config.RUN_DEADLINE_SECONDS if seconds is None else float(seconds)
    _RUN_DEADLINE_AT = time.monotonic() + budget if budget > 0 else 0.0
    _RUN_DEADLINE_REPORTED = False


def run_deadline_remaining() -> float | None:
    if _RUN_DEADLINE_AT <= 0:
        return None
    return max(_RUN_DEADLINE_AT - time.monotonic(), 0.0)


def run_deadline_reached() -> bool:
    return _RUN_DEADLINE_AT > 0 and time.monotonic() >= _RUN_DEADLINE_AT


def _report_run_deadline_once() -> None:
    global _RUN_DEADLINE_REPORTED
    if _RUN_DEADLINE_REPORTED:
        return
    _RUN_DEADLINE_REPORTED = True
    log.warning(
        f"Общий лимит прогона {config.RUN_DEADLINE_SECONDS:.0f} с исчерпан: "
        "новые запросы не начинаем, сохраняем уже прочитанное"
    )




def polite_delay():
    """Случайная задержка между запросами."""
    if run_deadline_reached():
        _report_run_deadline_once()
        return
    delay = random.uniform(*config.REQUEST_DELAY)
    remaining = run_deadline_remaining()
    if remaining is not None:
        # Не тратим последние секунды бюджета только на courtesy sleep.
        delay = min(delay, max(remaining - 0.1, 0.0))
    if delay > 0:
        time.sleep(delay)


def _set_diag(kind: str, url: str, *, record_failure: bool = True, **extra) -> None:
    """Записать класс последнего ответа в config.FETCH_DIAG (одна точка).

    Диагноз перезаписывается КАЖДЫМ запросом — читать его надо сразу после
    вызова fetch_*, пока следующий не затёр. Поэтому же здесь, и только здесь,
    ведётся НАКОПИТЕЛЬНЫЙ счёт отказов по классам: FETCH_DIAG живёт до
    следующего запроса, а сводка прогона должна пережить весь обход.
    """
    config.FETCH_DIAG.clear()
    config.FETCH_DIAG.update(
        {"kind": kind, "host": urlsplit(url).netloc or url, **extra})
    if kind != "ok" and record_failure:
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
    "captcha": "карточка закрыта проверочным кодом",  # legacy
    "captcha_card": "карточка закрыта проверочным кодом",
    "blocked": "суд заблокировал запрос (страница защиты ГАС)",  # legacy
    "waf_block": "суд заблокировал запрос (страница защиты ГАС)",
    "waf_search": "поиск суда заблокирован защитой ГАС",
    "portal_placeholder": "вместо карточки пришла заглушка портала",
    "non_card_page": "вместо карточки пришла неопознанная служебная страница",
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
    "run_deadline": "общий лимит времени прогона исчерпан",
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
        remaining = run_deadline_remaining()
        if remaining is not None and remaining <= 0:
            _report_run_deadline_once()
            # Запроса не было: это не transport failure суда и не должно
            # открывать breaker. Диагноз нужен вызывающему, чтобы тоже не
            # приписать пропуск хосту.
            _set_diag(
                "run_deadline", url, record_failure=False,
                context=context, attempt=attempt,
            )
            return ""
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
            connect_timeout = config.FETCH_TIMEOUT_CONNECT
            read_timeout = config.FETCH_TIMEOUT_READ
            if remaining is not None and remaining < connect_timeout + read_timeout:
                # requests не имеет total-timeout. Делим остаток так, чтобы
                # худший connect+read не вышел далеко за общий дедлайн.
                connect_timeout = min(connect_timeout, max(remaining - 0.1, 0.1))
                read_timeout = min(
                    read_timeout,
                    max(remaining - connect_timeout, 0.1),
                )
            r = session.get(url, timeout=(connect_timeout, read_timeout))
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
                remaining = run_deadline_remaining()
                if remaining is not None:
                    wait = min(wait, max(remaining, 0.0))
                if wait > 0:
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
# ключ — хост суда. ``count`` оставлен batch-импортам; полный прогон использует
# ``time`` и откладывает карточки хоста до next_probe_at, продолжая другие суды.

_BREAKER_FAST_KINDS = frozenset({
    "connection_reset", "response_error",
    "http_500", "http_502", "http_503", "http_504",
})
_BREAKER_SLOW_KINDS = frozenset({
    "read_timeout", "connect_timeout", "timeout", "connection_error",
    "dns_error", "tls_error", "proxy_error", "redirect_error",
    "request_error", "empty",
})
_BREAKER_OUTAGE_KINDS = frozenset({
    "portal_placeholder", "outage_search", "non_card_page",
})
_BREAKER_BLOCK_KINDS = frozenset({
    "waf_block", "waf_search", "http_403", "captcha_card",
})
_BREAKER_PARSER_KINDS = frozenset({
    "empty_shell", "empty_search", "unparsed_card", "degraded_card",
    "captcha_search",
})


def card_breaker_time_mode() -> bool:
    """True только для полного time-based профиля; неизвестное = count-safe."""
    return str(config.CARD_BREAKER_MODE or "").strip().lower() == "time"


def card_breaker_policy(kind: str) -> dict[str, Any]:
    """Одна таблица operational-policy по уже готовому точному ``kind``.

    Классификация транспорта остаётся в transport_fail_kind, семантики — в
    parsing.search; здесь нет анализа текста исключений/HTML, только политика.
    """
    if kind in _BREAKER_PARSER_KINDS:
        return {"family": "parser_quality", "threshold": 0, "cooldown": 0.0}
    if kind in _BREAKER_FAST_KINDS or kind.startswith("http_5"):
        return {
            "family": "fast_transient",
            "threshold": config.CARD_BREAKER_FAST_THRESHOLD,
            "cooldown": config.CARD_BREAKER_FAST_COOLDOWN_SECONDS,
        }
    if kind in _BREAKER_OUTAGE_KINDS:
        return {
            "family": "portal_outage",
            "threshold": config.CARD_BREAKER_OUTAGE_THRESHOLD,
            "cooldown": config.CARD_BREAKER_OUTAGE_COOLDOWN_SECONDS,
        }
    if kind in _BREAKER_BLOCK_KINDS:
        return {
            "family": "access_block",
            "threshold": config.CARD_BREAKER_BLOCK_THRESHOLD,
            "cooldown": config.CARD_BREAKER_BLOCK_COOLDOWN_SECONDS,
        }
    if kind in _BREAKER_SLOW_KINDS:
        return {
            "family": "slow_unavailable",
            "threshold": config.CARD_BREAKER_SLOW_THRESHOLD,
            "cooldown": config.CARD_BREAKER_SLOW_COOLDOWN_SECONDS,
        }
    return {
        "family": "unknown",
        "threshold": config.CARD_BREAKER_THRESHOLD,
        "cooldown": config.CARD_BREAKER_SLOW_COOLDOWN_SECONDS,
    }


_BREAKER_ENTRY_DEFAULTS = {
    "fails": 0,
    "open": False,
    "state": "closed",
    "kind": "",
    "family": "",
    "reason": "",
    "skipped": 0,
    "gate_hits": 0,
    "probes": 0,
    "probe_successes": 0,
    "probe_failures": 0,
    "preopened": False,
    "opened_count": 0,
    "recoveries": 0,
    "opened_at": 0.0,
    "next_probe_at": 0.0,
    "cooldown_seconds": 0.0,
    "deferred_total": 0,
    "deferred_remaining": 0,
    "deferred_recovered": 0,
    "opened_kinds": None,
}


def _card_breaker_entry(host: str) -> dict:
    entry = config.CARD_BREAKER.setdefault(host, {})
    for key, value in _BREAKER_ENTRY_DEFAULTS.items():
        if key not in entry:
            entry[key] = {} if key == "opened_kinds" else value
    # Старые/тестовые сиды состояния не знают накопительной разбивки. None
    # здесь допустим как значение дефолта, но наружу всегда отдаём словарь.
    if not isinstance(entry.get("opened_kinds"), dict):
        entry["opened_kinds"] = {}
    return entry


def card_breaker_open(host: str) -> bool:
    """READ-ONLY: True, если предохранитель хоста открыт/half-open."""
    return bool(config.CARD_BREAKER.get(host, {}).get("open"))


def card_breaker_probe_ready(host: str, *, now: float | None = None) -> bool:
    """READ-ONLY: срок единственной time-based half-open пробы наступил."""
    if not card_breaker_time_mode():
        return False
    entry = config.CARD_BREAKER.get(host) or {}
    if not entry.get("open") or entry.get("state", "open") != "open":
        return False
    clock = time.monotonic() if now is None else float(now)
    return clock >= float(entry.get("next_probe_at") or 0.0)


def card_breaker_summary() -> dict[str, Any]:
    """Санитизированная сводка состояния для checkpoint/last_run."""
    now = time.monotonic()
    hosts: list[dict[str, Any]] = []
    by_kind: dict[str, int] = {}
    for host, raw in sorted(config.CARD_BREAKER.items()):
        entry = _card_breaker_entry(host)
        for kind, count in (entry.get("opened_kinds") or {}).items():
            by_kind[kind] = by_kind.get(kind, 0) + int(count or 0)
        if not (entry.get("opened_count") or entry.get("preopened")
                or entry.get("deferred_total") or entry.get("probes")):
            continue
        next_in = 0.0
        if entry.get("open") and card_breaker_time_mode():
            next_in = max(float(entry.get("next_probe_at") or 0.0) - now, 0.0)
        hosts.append({
            "host": host,
            "state": entry.get("state") or ("open" if entry.get("open") else "closed"),
            "kind": entry.get("kind") or "",
            "family": entry.get("family") or "",
            "reason": entry.get("reason") or "",
            "preopened": bool(entry.get("preopened")),
            "skipped": int(entry.get("skipped") or 0),
            "probes": int(entry.get("probes") or 0),
            "deferred_total": int(entry.get("deferred_total") or 0),
            "deferred_remaining": int(entry.get("deferred_remaining") or 0),
            "deferred_recovered": int(entry.get("deferred_recovered") or 0),
            "cooldown_seconds": float(entry.get("cooldown_seconds") or 0.0),
            "next_probe_in_seconds": round(next_in, 1),
        })
    return {
        "mode": "time" if card_breaker_time_mode() else "count",
        "opened_hosts": sum(
            1 for e in config.CARD_BREAKER.values()
            if e.get("opened_count") or e.get("preopened")
        ),
        "open_hosts": sum(1 for e in config.CARD_BREAKER.values() if e.get("open")),
        "recovered_hosts": sum(
            1 for e in config.CARD_BREAKER.values() if e.get("recoveries")
        ),
        "deferred_total": sum(
            int(e.get("deferred_total") or 0) for e in config.CARD_BREAKER.values()
        ),
        "deferred_remaining": sum(
            int(e.get("deferred_remaining") or 0) for e in config.CARD_BREAKER.values()
        ),
        "deferred_recovered": sum(
            int(e.get("deferred_recovered") or 0) for e in config.CARD_BREAKER.values()
        ),
        "probes": sum(int(e.get("probes") or 0) for e in config.CARD_BREAKER.values()),
        "probe_successes": sum(
            int(e.get("probe_successes") or 0) for e in config.CARD_BREAKER.values()
        ),
        "probe_failures": sum(
            int(e.get("probe_failures") or 0) for e in config.CARD_BREAKER.values()
        ),
        "by_kind": dict(sorted(by_kind.items())),
        "hosts": hosts,
    }


def _publish_breaker_snapshot() -> None:
    # События open/probe/recovery редки. Пер-карточные defer здесь намеренно
    # не fsync'аем: сотни пропусков не должны превратить telemetry в bottleneck.
    telemetry.set_breaker_snapshot(card_breaker_summary())


def _open_card_breaker(host: str, entry: dict, kind: str, *,
                       preopened: bool = False, reason: str = "",
                       reopened: bool = False) -> None:
    policy = card_breaker_policy(kind)
    now = time.monotonic()
    entry["open"] = True
    entry["state"] = "open"
    entry["kind"] = kind
    entry["family"] = policy["family"]
    entry["reason"] = reason or fetch_fail_reason_ru({"kind": kind}) or kind
    entry["cooldown_seconds"] = float(policy["cooldown"])
    entry["opened_at"] = now
    entry["next_probe_at"] = (
        now + float(policy["cooldown"]) if card_breaker_time_mode() else 0.0
    )
    entry["preopened"] = bool(entry.get("preopened") or preopened)
    if not reopened:
        entry["opened_count"] = int(entry.get("opened_count") or 0) + 1
    opened_kinds = entry.setdefault("opened_kinds", {})
    opened_kinds[kind] = int(opened_kinds.get(kind, 0)) + 1
    _publish_breaker_snapshot()


def card_breaker_allows(host: str, *, allow_half_open: bool = True) -> bool:
    """МУТИРУЮЩИЙ гейт «фетчить карточку или отложить/пропустить».

    В ``time``-профиле разрешает ровно одну пробу после ``next_probe_at``;
    очередь основного прогона держит остальные карточки хоста. Конкретная
    ``DeferredCardQueue`` передаёт ``allow_half_open=False`` после первой
    неудачной пробы этого хоста в своей фазе — иначе множество лежащих хостов
    могло само переждать cooldown и бесконечно запускать новые круги проб.
    В ``count`` сохраняется прежняя K-я проба коротких batch-импортов.
    """
    if not config.CARD_BREAKER_THRESHOLD:
        return True
    entry = config.CARD_BREAKER.get(host)
    if not entry or not entry.get("open"):
        return True
    entry = _card_breaker_entry(host)
    entry["gate_hits"] += 1

    allow_probe = False
    if card_breaker_time_mode():
        allow_probe = allow_half_open and card_breaker_probe_ready(host)
    else:
        every = config.CARD_BREAKER_PROBE_EVERY
        allow_probe = bool(every and entry["gate_hits"] % every == 0)

    if allow_probe:
        entry["state"] = "half_open"
        entry["probes"] += 1
        log.debug(f"Предохранитель {host}: half-open проба #{entry['probes']}")
        _publish_breaker_snapshot()
        return True

    entry["skipped"] += 1
    config.METRICS["cards_breaker_skipped"] += 1
    return False


def _card_breaker_fail(host: str, kind: str, *, reason: str = "") -> None:
    """Учесть один финальный logical failure карточки по точному классу."""
    if not config.CARD_BREAKER_THRESHOLD or not host:
        return
    policy = card_breaker_policy(kind)
    threshold = (
        int(policy["threshold"])
        if card_breaker_time_mode() else config.CARD_BREAKER_THRESHOLD
    )
    if threshold <= 0:
        return
    entry = _card_breaker_entry(host)

    # Провальная half-open проба (или явный ungated fetch при уже открытом
    # breaker) запускает новый cooldown. Не смешиваем её с новым threshold.
    if entry.get("open"):
        if entry.get("state") == "half_open":
            entry["probe_failures"] += 1
        entry["fails"] = max(int(entry.get("fails") or 0), threshold)
        _open_card_breaker(
            host, entry, kind, reopened=True, reason=reason,
            preopened=bool(entry.get("preopened")),
        )
        log.warning(
            f"Суд {host}: half-open проба не прошла ({entry['reason']}) — "
            + (f"следующая через {entry['cooldown_seconds']:.0f}с"
               if card_breaker_time_mode() else "суд остаётся снят с обхода")
        )
        return

    # В полном time-профиле разные operational families не складываются:
    # CAPTCHA после reset не должна внезапно достигать timeout-порога и
    # получать чужой cooldown. Count-профиль импортов намеренно сохраняет
    # прежнее правило «любые N непрочитанных карточек подряд» (5/3).
    if (card_breaker_time_mode() and entry.get("family")
            and entry.get("family") != policy["family"]):
        entry["fails"] = 0
    entry["family"] = policy["family"]
    entry["kind"] = kind
    entry["fails"] += 1
    entry["reason"] = reason or fetch_fail_reason_ru({"kind": kind}) or kind
    if entry["fails"] < threshold:
        return

    _open_card_breaker(host, entry, kind, reason=entry["reason"])
    probe_note = (
        f", half-open через {entry['cooldown_seconds']:.0f}с"
        if card_breaker_time_mode()
        else (f", проба каждые {config.CARD_BREAKER_PROBE_EVERY} карточек"
              if config.CARD_BREAKER_PROBE_EVERY else "")
    )
    log.warning(
        f"Суд {host}: {entry['fails']} карточек подряд не прочитано "
        f"({entry['reason']}) — обход приостановлен{probe_note}"
    )


def card_breaker_note_failure(host: str, kind: str, *, reason: str = "") -> None:
    """Публичная проводка точного отказа поиска в тот же breaker-policy."""
    if kind == "run_deadline":
        return  # запроса не было — суд не виноват и breaker не открываем
    _card_breaker_fail(host, kind, reason=reason)


def _card_breaker_ok(host: str) -> None:
    """Успешная карточка: сброс серии; half-open закрывает предохранитель."""
    entry = config.CARD_BREAKER.get(host)
    if not entry:
        return
    entry = _card_breaker_entry(host)
    was_open = bool(entry.get("open"))
    was_probe = entry.get("state") == "half_open"
    entry["fails"] = 0
    entry["open"] = False
    entry["state"] = "closed"
    entry["next_probe_at"] = 0.0
    if was_open:
        entry["recoveries"] += 1
        if was_probe:
            entry["probe_successes"] += 1
        log.info(f"Суд {host}: снова отдаёт карточки — обход возобновлён")
        _publish_breaker_snapshot()


def card_breaker_preopen(host: str, kind: str, *, reason: str = "") -> None:
    """Канарейка поиска: открыть breaker точным outage/WAF-классом."""
    if not config.CARD_BREAKER_THRESHOLD or not host:
        return
    entry = _card_breaker_entry(host)
    if entry["open"]:
        return
    _open_card_breaker(host, entry, kind, preopened=True, reason=reason)
    probe_note = (
        f", half-open через {entry['cooldown_seconds']:.0f}с"
        if card_breaker_time_mode()
        else (f", проба каждые {config.CARD_BREAKER_PROBE_EVERY} карточек"
              if config.CARD_BREAKER_PROBE_EVERY else "")
    )
    log.warning(
        f"Суд {host}: {entry['reason']} — карточки пока не запрашиваем{probe_note}"
    )


def card_breaker_note_deferred(host: str) -> None:
    entry = _card_breaker_entry(host)
    entry["deferred_total"] += 1
    entry["deferred_remaining"] += 1


def card_breaker_note_deferred_finished(host: str, *, recovered: bool) -> None:
    entry = _card_breaker_entry(host)
    entry["deferred_remaining"] = max(entry["deferred_remaining"] - 1, 0)
    if recovered:
        entry["deferred_recovered"] += 1
        config.METRICS["cards_breaker_recovered"] += 1


@dataclass
class DeferredCardWork:
    """Одна business-единица карточной очереди без судебного содержимого."""

    value: Any
    visits: int = 0
    host: str = ""
    ever_deferred: bool = False
    pending_deferred: bool = False
    queued: bool = False
    requested: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def first_visit(self) -> bool:
        return self.visits == 1


class DeferredCardQueue:
    """No-sleep очередь: после основного прохода даёт только due half-open.

    Значение ``value`` непрозрачно для сети. Вызывающий обязан отметить
    реальный HTTP через ``mark_attempted`` и закончить вынутую карточку через
    ``finish`` либо вернуть её ``defer``. Поэтому одна и та же business-логика
    используется и на первом проходе, и после восстановления суда.
    """

    def __init__(self, values: Iterable[Any], *, stage: str = ""):
        self.stage = stage
        self._ready = deque(DeferredCardWork(v) for v in values)
        self._deferred: OrderedDict[str, deque[DeferredCardWork]] = OrderedDict()
        # ``probe_failures`` живёт весь прогон, а лимит нужен на ФАЗУ. Снимок
        # при создании очереди отделяет провал предыдущей фазы от нового.
        self._probe_failures_at_start = {
            host: int(entry.get("probe_failures") or 0)
            for host, entry in config.CARD_BREAKER.items()
        }

    def __iter__(self):
        return self

    def __next__(self) -> DeferredCardWork:
        if run_deadline_reached():
            _report_run_deadline_once()
            raise StopIteration
        if not self._ready:
            self._schedule_recoverable()
        if not self._ready:
            raise StopIteration
        work = self._ready.popleft()
        work.visits += 1
        return work

    def _failed_probe_in_phase(self, host: str) -> bool:
        entry = config.CARD_BREAKER.get(host) or {}
        baseline = self._probe_failures_at_start.setdefault(
            host, int(entry.get("probe_failures") or 0)
        )
        return int(entry.get("probe_failures") or 0) > baseline

    def allows(self, host: str) -> bool:
        """Гейт очереди: не более одной НЕУДАЧНОЙ half-open пробы за фазу.

        Успех не увеличивает ``probe_failures`` и сразу закрывает breaker, так
        что восстановившийся хост по-прежнему дочитывается целиком.
        """
        return card_breaker_allows(
            host, allow_half_open=not self._failed_probe_in_phase(host)
        )

    def _schedule_recoverable(self) -> None:
        if not card_breaker_time_mode():
            return
        for host in list(self._deferred):
            queue = self._deferred.get(host)
            if not queue:
                self._deferred.pop(host, None)
                continue
            if not card_breaker_open(host):
                self._deferred.pop(host, None)
                while queue:
                    work = queue.popleft()
                    work.queued = False
                    self._ready.append(work)
                return
            # Провальная проба исчерпала бюджет этого хоста в данной фазе.
            # Даже если обход других хостов уже пережил новый cooldown, второй
            # круг не запускаем: остаток честно уйдёт в следующий прогон.
            if self._failed_probe_in_phase(host):
                continue
            if card_breaker_probe_ready(host):
                work = queue.popleft()
                work.queued = False
                if not queue:
                    self._deferred.pop(host, None)
                self._ready.append(work)
                return

    def defer(self, work: DeferredCardWork, host: str) -> bool:
        """Вернуть работу в очередь. False в count-профиле = старый skip."""
        if not card_breaker_time_mode():
            return False
        work.host = host
        if not work.pending_deferred:
            work.pending_deferred = True
            work.ever_deferred = True
            card_breaker_note_deferred(host)
        if not work.queued:
            self._deferred.setdefault(host, deque()).append(work)
            work.queued = True
        return True

    @staticmethod
    def mark_attempted(work: DeferredCardWork) -> None:
        work.requested = True

    @staticmethod
    def finish(work: DeferredCardWork, *, recovered: bool) -> None:
        if not work.pending_deferred:
            return
        work.pending_deferred = False
        work.queued = False
        card_breaker_note_deferred_finished(work.host, recovered=recovered)

    def unresolved(self) -> list[DeferredCardWork]:
        return [work for queue in self._deferred.values() for work in queue]

    def unresolved_unrequested(self) -> list[DeferredCardWork]:
        return [work for work in self.unresolved() if not work.requested]

    def checkpoint(self) -> None:
        _publish_breaker_snapshot()


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
        if config.FETCH_DIAG.get("kind") == "run_deadline":
            return ""
        # fetch_page уже записал точный transport-kind; читаем СРАЗУ, пока
        # следующий запрос не перезаписал единственный FETCH_DIAG.
        _card_breaker_fail(
            host, str(config.FETCH_DIAG.get("kind") or "request_error")
        )
        return ""
    # fetch_page оставляет id/elapsed последней HTTP-попытки в едином
    # FETCH_DIAG. Semantic verdict ниже уточняет ЭТОТ ответ и не создаёт
    # вторую попытку в checkpoint.
    request_id = str(config.FETCH_DIAG.get("request_id") or "")
    elapsed = config.FETCH_DIAG.get("elapsed")
    # Ленивый импорт: netutil — низкоуровневый слой, тащить parsing (courts,
    # tables) на уровень модуля значило бы завязать сеть на парсеры.
    from court_monitor.parsing.search import (
        classify_non_card_page,
        detect_captcha_challenge_card,
    )
    ctx = f" ({context})" if context else ""
    if detect_captcha_challenge_card(html):
        config.METRICS["cards_captcha"] += 1
        log.warning(f"Карточка закрыта проверочным кодом{ctx}: {url}")
        _set_diag(
            "captcha_card", url, status=200, request_id=request_id,
            elapsed=elapsed, context=context,
        )
        telemetry.classify_semantic(
            request_id or None, "captcha_card", host=host, context=context or ""
        )
        _card_breaker_fail(host, "captcha_card")
        return ""
    non_card_kind = classify_non_card_page(html, url)
    if non_card_kind:
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
            non_card_kind, url, status=200, request_id=request_id,
            elapsed=elapsed, context=context, **marks,
        )
        telemetry.classify_semantic(
            request_id or None,
            non_card_kind,
            host=host,
            context=context or "",
            **marks,
        )
        _card_breaker_fail(host, non_card_kind)
        return ""
    _card_breaker_ok(host)
    telemetry.classify_semantic(
        request_id or None, "valid_card", host=host, context=context or ""
    )
    return html
