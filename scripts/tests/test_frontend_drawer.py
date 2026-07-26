"""
Стражи раскладки шапки drawer'а (hero) во фронте.

JS-инструментария в проекте нет, поэтому инварианты проверяются grep'ом по
исходнику — тем же приёмом, что и test_frontend_writs.py / test_versions.py.

Что охраняем: метка стороны («ИСТЕЦ» / «ОТВЕТ.») не наезжает на имя.
Механизм бага: у flex-элемента автоминимум (min-width:auto) не даёт сжаться
ниже содержимого, но ЯВНЫЙ min-width его отменяет. Стояло min-width:22px при
нужных «ОТВЕТ.» 39px — тег сжимался, и текст вылезал на имя ответчика.
У такого же тега в таблице (.party-tag) flex-shrink:0 стоял с самого начала.

Запуск: python3 -m pytest scripts/tests/test_frontend_drawer.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _styles() -> str:
    with open(os.path.join(ROOT, "styles.css"), encoding="utf-8") as f:
        return f.read()


def _rule(selector_tail: str) -> str:
    """Тело CSS-правила, чей селектор оканчивается на selector_tail."""
    css = _styles()
    m = re.search(r"[^\n{}]*" + re.escape(selector_tail) + r"\s*\{([^}]*)\}", css)
    assert m, f"В styles.css нет правила для {selector_tail}."
    return m.group(1)


def test_party_tag_does_not_shrink():
    """Метка стороны не сжимается ниже своего текста."""
    правило = _rule(".p-tag")
    assert re.search(r"flex-shrink\s*:\s*0", правило), (
        "У .p-tag пропал flex-shrink:0 — явный min-width отменяет автоминимум "
        "flex-элемента, и «ОТВЕТ.» (нужно 39px) снова сожмётся до min-width, "
        "наехав на имя ответчика."
    )


def test_party_tag_min_width_scales_with_font():
    """Ширина колонки метки задана в em, а не в жёстких пикселях.

    Метки выравниваются в одну колонку («ИСТЕЦ» и «ОТВЕТ.» разной длины), и
    значение в px пришлось бы подбирать заново при смене --fs-2xs или шрифта.
    """
    правило = _rule(".p-tag")
    m = re.search(r"min-width\s*:\s*([\d.]+)(em|rem|ch|px)", правило)
    assert m, "У .p-tag нет min-width — метки перестанут выравниваться."
    assert m.group(2) != "px", (
        f"min-width у .p-tag снова в пикселях ({m.group(0)}) — при смене "
        "размера шрифта метка либо разъедется с соседней, либо обрежется."
    )


def test_table_party_tag_keeps_shrink_guard():
    """Тег стороны в таблице — эталон, с которого списан drawer."""
    правило = _rule(".party-tag")
    assert re.search(r"flex-shrink\s*:\s*0", правило), (
        "У .party-tag в таблице пропал flex-shrink:0 — там та же связка "
        "«явный min-width + длинное имя стороны»."
    )
