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
            return r.content.decode("windows-1251", errors="replace")
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
