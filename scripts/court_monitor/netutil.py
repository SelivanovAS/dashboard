# -*- coding: utf-8 -*-
"""Сетевой слой: общая requests-сессия, вежливая задержка, загрузка страниц
судов (win-1251) с ретраями. Счётчики пишутся в config.METRICS.
"""

from __future__ import annotations

import random
import re
import time
from urllib.parse import urlsplit

import requests

from court_monitor import config
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
    вызова fetch_*, пока следующий не затёр.
    """
    config.FETCH_DIAG.clear()
    config.FETCH_DIAG.update(
        {"kind": kind, "host": urlsplit(url).netloc or url, **extra})


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
    for attempt in range(1, config.FETCH_MAX_RETRIES + 1):
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
            # На здоровом дне правка невидима: таймаут — потолок, а не
            # задержка, быстрый ответ возвращается быстро.
            r = session.get(url, timeout=(10, 65))
            r.raise_for_status()
            config.METRICS["requests_ok"] += 1
            if attempt > 1:
                config.METRICS["requests_retried"] += 1
            text = r.content.decode("windows-1251", errors="replace")
            if not text:
                # HTTP 200 с пустым телом — аномалия: без лога такой ответ
                # исчезал бы бесследно (вызыватели молча скипают "").
                host = urlsplit(url).netloc or url
                log.warning(f"Пустой ответ (HTTP {r.status_code}): {host}{ctx}")
            _set_diag("ok" if text else "empty", url, status=r.status_code)
            return text
        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            _set_diag(f"http_{status}" if status else "network", url,
                      status=status, error=type(e).__name__,
                      **(block_page_marks(getattr(e.response, "text", ""))
                         if status else {}))
            if attempt < config.FETCH_MAX_RETRIES:
                # Промежуточная попытка: хост + контекст + класс ошибки, без
                # простыни с полным URL (он уйдёт в финальный ERROR, если все
                # попытки исчерпаются).
                wait = attempt * 5
                host = urlsplit(url).netloc or url
                log.warning(
                    f"Попытка {attempt}/{config.FETCH_MAX_RETRIES}: {host}{ctx} — "
                    f"{type(e).__name__}, повтор через {wait}с..."
                )
                time.sleep(wait)
            else:
                config.METRICS["requests_failed"] += 1
                log.error(
                    f"Ошибка загрузки {url}{ctx} "
                    f"после {config.FETCH_MAX_RETRIES} попыток: {e}"
                )
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
        return ""
    html = fetch_page(url, context=context)
    if not html:
        _card_breaker_fail(host, "сеть/пустой ответ")
        return ""
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
        _set_diag("captcha", url, status=200)
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
        _set_diag("blocked", url, status=200, **marks)
        _card_breaker_fail(host, "заглушка/блок портала")
        return ""
    _card_breaker_ok(host)
    return html
