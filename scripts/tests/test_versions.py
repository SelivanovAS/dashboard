"""
Сверка версий кэш-бастинга фронта.

Три счётчика должны совпадать, иначе после деплоя пользователи PWA
зависнут на старой версии (service worker отдаст закэшированный файл):
- styles.css?v=N   в sberbank_dashboard.html
- app.js?v=N       в sberbank_dashboard.html
- CACHE_VERSION='vN' в service-worker.js

При любой правке фронта поднимать все три на один и тот же номер.
Запуск: python -m pytest scripts/tests/ -v
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def test_versions_in_sync():
    html = _read("sberbank_dashboard.html")
    sw = _read("service-worker.js")

    css_m = re.search(r"styles\.css\?v=(\d+)", html)
    js_m = re.search(r"app\.js\?v=(\d+)", html)
    sw_m = re.search(r"CACHE_VERSION\s*=\s*'v(\d+)'", sw)

    assert css_m, "В sberbank_dashboard.html не найден styles.css?v=N"
    assert js_m, "В sberbank_dashboard.html не найден app.js?v=N"
    assert sw_m, "В service-worker.js не найден CACHE_VERSION = 'vN'"

    css_v, js_v, sw_v = css_m.group(1), js_m.group(1), sw_m.group(1)
    assert css_v == js_v == sw_v, (
        f"Версии рассинхронизированы: styles.css?v={css_v}, app.js?v={js_v}, "
        f"CACHE_VERSION=v{sw_v} — при правке фронта поднимай все три на один номер."
    )
