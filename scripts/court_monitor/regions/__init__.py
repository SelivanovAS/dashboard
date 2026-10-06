# -*- coding: utf-8 -*-
"""Реестры регионов мониторинга (по одному модулю на территорию).

Каждый модуль региона (hmao.py, sverdlovsk_yanao.py, …) обязан определить
константу REGION: RegionConfig (см. regions/base.py). Активный регион
выбирается env-переменной REGION (читается как config.REGION, дефолт "hmao");
форк территории задаёт её в GitHub Actions Variables и НЕ правит код.

Загрузка ленивая (importlib) — regions/__init__ не импортирует модули регионов
на уровне модуля, поэтому цикла с courts.py (фасад активного региона) нет.
"""

from __future__ import annotations

import importlib

from court_monitor.regions.base import CourtConfig, RegionConfig  # noqa: F401


def get_region(code: str | None = None) -> RegionConfig:
    """RegionConfig по коду; без кода — активный регион (config.REGION).

    config.REGION читается на каждый вызов (config.X-инвариант: тесты патчат
    monkeypatch.setattr(config, "REGION", ...) — и следующий вызов увидит
    подмену). Неизвестный код — громкий ValueError, а не молчаливый дефолт.
    """
    from court_monitor import config
    resolved = (code or getattr(config, "REGION", "") or "hmao").strip().lower()
    try:
        mod = importlib.import_module(f"court_monitor.regions.{resolved}")
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"Неизвестный регион {resolved!r} (env REGION): нет модуля "
            f"scripts/court_monitor/regions/{resolved}.py"
        ) from exc
    region = getattr(mod, "REGION", None)
    if not isinstance(region, RegionConfig):
        raise ValueError(
            f"Модуль региона {resolved!r} не определяет константу "
            f"REGION: RegionConfig"
        )
    return region
