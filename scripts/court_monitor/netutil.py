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


def fetch_card_checked(url: str, *, context: str | None = None) -> str:
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

    READ-ONLY: код не читаем и не решаем — только распознаём страницу
    (см. detect_captcha_challenge_card / looks_like_non_card_page).
    """
    html = fetch_page(url, context=context)
    if not html:
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
        return ""
    if looks_like_non_card_page(html, url):
        config.METRICS["cards_blocked"] += 1
        log.warning(
            f"Карточка не получена — портал недоступен/заглушка{ctx}: {url}"
        )
        return ""
    return html
