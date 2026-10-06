# -*- coding: utf-8 -*-
"""Пакет court_monitor — модули мониторинга судебных дел.

Распил монолита scripts/update_cases.py (см. docs/Распил_монолита_контекст.md).
Фасад scripts/update_cases.py ре-экспортирует прежние имена и остаётся
точкой входа CLI; новый код и правки — в модулях этого пакета.

Импортировать только как `court_monitor` (scripts/ уже в sys.path у всех
точек входа); импорт как `scripts.court_monitor` создаст второй экземпляр
модулей-синглтонов (config.METRICS, netutil.session) — нельзя.
"""
