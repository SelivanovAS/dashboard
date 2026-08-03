# -*- coding: utf-8 -*-
"""Даты фикстур, пересаженные на сегодняшний день.

Карточки-фикстуры датированы намертво (февраль-2026), а часть правил меряет
ВОЗРАСТ дела — прежде всего гейт приёма в трек исков банка
(`bank_intake.entry_is_spent`, 03.08.2026): карточка с полугодовым решением
для него «дело уже отработало». E2e-наборы сборщика и импортёра проверяют
пейджер, фильтры строк и запись в хранилище — не архивные окна, — поэтому им
нужна свежая карточка, а не вечно стареющая. Окна проверяются отдельными
тестами с явными датами.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "fixtures")

# Каноническая карточка 1-й инстанции: дата в фикстуре → «сколько дней назад»
# она должна оказаться. Порядок событий сохранён (регистрация → передача
# судье → решение → мотивировка → последнее движение).
_FI_CARD_DATE_AGES = {
    "08.10.2025": 40,
    "09.10.2025": 39,
    "12.02.2026": 10,
    "19.02.2026": 8,
    "20.03.2026": 3,
}


def days_ago(days: int) -> str:
    """«N дней назад» в формате дат карточки (ДД.ММ.ГГГГ)."""
    return (date.today() - timedelta(days=days)).strftime("%d.%m.%Y")


def recent_fi_card_html() -> str:
    """case_card_first_instance.html с датами «на этой неделе»."""
    with open(os.path.join(FIXTURES_DIR, "case_card_first_instance.html"),
              encoding="utf-8") as f:
        html = f.read()
    for old, age in _FI_CARD_DATE_AGES.items():
        html = html.replace(old, days_ago(age))
    return html
