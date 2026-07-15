# -*- coding: utf-8 -*-
"""Общий conftest всего pytest-набора.

Тестовый набор написан в контексте региона hmao (реальные суды/фикстуры
ХМАО). Форк территории задаёт свой регион файлом REGION в корне репо —
без этой фиксации pytest в форке грузил бы чужой реестр и падал. pytest
импортирует conftest раньше тестовых модулей (и, следовательно, раньше
court_monitor.config), поэтому env-переменная успевает до чтения.

Тесты, которым нужен другой регион, передают его явно
(match_region_first_instance(name, region), get_region(code)) или патчат
config.REGION monkeypatch'ем.
"""

import os

os.environ["REGION"] = "hmao"
