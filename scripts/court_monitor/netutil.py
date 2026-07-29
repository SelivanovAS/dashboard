# -*- coding: utf-8 -*-
"""Сетевой слой: общая requests-сессия, вежливая задержка, загрузка страниц
судов (win-1251) с ретраями. Счётчики пишутся в config.METRICS.
"""

from __future__ import annotations

import random
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


def fetch_page(url: str, *, context: str | None = None) -> str:
    """Скачать страницу с сайта суда (win-1251) с повторными попытками.

    context — короткая метка «что грузим» (номер дела, имя суда, «поиск
    апелляции»): попадает и в WARNING ретрая, и в финальный ERROR, чтобы
    ошибка сети сразу привязывалась к делу/суду одной строкой.
    """
    ctx = f" ({context})" if context else ""
    for attempt in range(1, config.FETCH_MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=30)
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
            return text
        except requests.RequestException as e:
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
        _card_breaker_fail(host, "проверочный код")
        return ""
    if looks_like_non_card_page(html, url):
        config.METRICS["cards_blocked"] += 1
        log.warning(
            f"Карточка не получена — портал недоступен/заглушка{ctx}: {url}"
        )
        _card_breaker_fail(host, "заглушка/блок портала")
        return ""
    _card_breaker_ok(host)
    return html
