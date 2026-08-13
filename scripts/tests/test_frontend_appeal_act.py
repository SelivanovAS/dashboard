"""
Стражи блока «Текст определения (полный)» на вкладке апелляции drawer'а.

Поле appeal.act_text персистится с 13.08.2026 (только новые акты) — до этого
текст апел. определения жил один прогон и drawer его не показывал, в отличие
от 1-й инстанции и кассации. JS-инструментария в проекте нет — инварианты
проверяются grep'ом по исходнику (приём test_frontend_writs.py).

Запуск: python3 -m pytest scripts/tests/test_frontend_appeal_act.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _ap_branch() -> str:
    """Кусок app.js от ветки drawerStage==='ap' до ветки 'cs'."""
    js = _app_js()
    m = re.search(
        r"drawerStage==='ap'&&stageData\)\{(.*?)drawerStage==='cs'&&stageData",
        js, re.S,
    )
    assert m, "В app.js не нашлась ветка drawerStage==='ap' — разметка уехала."
    return m.group(1)


def test_appeal_act_text_block_present():
    """Вкладка апелляции показывает полный текст определения свёрткой."""
    ветка = _ap_branch()
    assert "ap.act_text" in ветка, (
        "Ветка апелляции drawer'а не читает ap.act_text — блок полного "
        "текста определения пропал (персист поля появился 13.08.2026)."
    )
    assert "Текст определения (полный)" in ветка, (
        "Пропал заголовок свёртки «Текст определения (полный)» на вкладке "
        "апелляции."
    )


def test_appeal_act_text_marks_truncation():
    """Обрезка на 8000 помечается явно — иначе юрист решит, что так написал
    суд (то же правило, что у текста решения 1-й инстанции)."""
    ветка = _ap_branch()
    assert re.search(r"ap\.act_text\.length>=8000", ветка), (
        "Пропала проверка length>=8000 у ap.act_text — обрезанный текст "
        "перестал помечаться."
    )
    assert "Текст обрезан при загрузке" in ветка
