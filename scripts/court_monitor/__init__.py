# -*- coding: utf-8 -*-
"""Пакет court_monitor — модули мониторинга судебных дел.

Распил монолита scripts/update_cases.py (см. docs/technical/01-обзор-и-архитектура.md).
Фасад scripts/update_cases.py остаётся точкой входа CLI и сохраняет
используемые тестами имена; новый код и правки — в модулях этого пакета.

Импортировать только как `court_monitor` (scripts/ уже в sys.path у всех
точек входа); импорт как `scripts.court_monitor` создаст второй экземпляр
модулей-синглтонов (config.METRICS, netutil.session) — нельзя.
"""
