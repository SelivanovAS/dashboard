"""
Стражи drawer'а во фронте: раскладка hero и дата проверки сайта суда.

Инварианты раскладки проверяются по исходнику, а чистые функции даты и HTML —
исполнением в node, как в test_frontend_writs.py.

Что охраняем: метка стороны («ИСТЕЦ» / «ОТВЕТ.») не наезжает на имя.
Механизм бага: у flex-элемента автоминимум (min-width:auto) не даёт сжаться
ниже содержимого, но ЯВНЫЙ min-width его отменяет. Стояло min-width:22px при
нужных «ОТВЕТ.» 39px — тег сжимался, и текст вылезал на имя ответчика.
У такого же тега в таблице (.party-tag) flex-shrink:0 стоял с самого начала.

Запуск: python3 -m pytest scripts/tests/test_frontend_drawer.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))
NODE = shutil.which("node")


def _styles() -> str:
    with open(os.path.join(ROOT, "styles.css"), encoding="utf-8") as f:
        return f.read()


def _app() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _fn_src(name: str) -> str:
    """Вырезать многострочную или однострочную function declaration."""
    src = _app()
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    if m:
        return m.group(0)
    m = re.search(r"^function\s+" + re.escape(name) + r"\s*\(.*\}$", src, re.M)
    assert m, f"В app.js нет функции {name}."
    return m.group(0)


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


def test_freshness_uses_selected_stage_and_sits_before_key_dates():
    """Плашка читает выбранную инстанцию и стоит сразу под её вкладками."""
    src = _fn_src("renderDrawer")
    assert "const stageData=drawerStage==='fi'?c._fi" in src
    call = "${drawerFreshnessHtml(stageData)}"
    assert call in src, (
        "renderDrawer не передаёт выбранный _fi/_ap/_cs в плашку проверки."
    )
    key_dates = '<div class="drawer-section-title">Ключевые даты</div>'
    assert src.index("${tabsHtml}") < src.index(call) < src.index(key_dates), (
        "Плашка должна оставаться между вкладками инстанций и «Ключевыми датами»."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_freshness_html_known_and_missing_dates():
    """Дата форматируется без TZ, а отсутствие штампа не маскируется общей датой."""
    deps = "\n".join(_fn_src(n) for n in (
        "parseDate", "courtCheckDate", "drawerFreshnessHtml",
    ))
    script = deps + "\nprocess.stdout.write(JSON.stringify([" \
        "drawerFreshnessHtml({last_checked_at:'2026-08-21'})," \
        "drawerFreshnessHtml({}),drawerFreshnessHtml({last_checked_at:'2026-02-31'})]));"
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    known, missing, invalid = json.loads(out.stdout)

    assert "Проверено на сайте суда" in known and "21.08.2026" in known
    assert "is-missing" not in known and "m8 12 2.5 2.5L16 9" in known
    for html in (missing, invalid):
        assert "is-missing" in html
        assert "Проверка сайта суда" in html
        assert "не зафиксирована" in html
        assert "updated_at" not in html


def test_freshness_is_visible_compact_and_theme_safe():
    """Плашка заметна, но остаётся однострочной на обеих темах."""
    правило = _rule(".drawer-freshness")
    for ожидаемое in (
        r"display\s*:\s*flex", r"min-height\s*:\s*38px",
        r"background\s*:\s*var\(--freshness-bg\)",
    ):
        assert re.search(ожидаемое, правило), ожидаемое

    дата = _rule(".drawer-freshness-date")
    assert re.search(r"white-space\s*:\s*nowrap", дата)
    css = _styles()
    assert css.count("--freshness-bg:") == 2, (
        "Токен поверхности должен быть определён и в light, и в dark теме."
    )
    assert css.count("--freshness-border:") == 2
    узкий = re.search(r"@media\s*\(max-width\s*:\s*350px\)\s*\{([\s\S]*?)\n\}", css)
    assert узкий and ".drawer-freshness-long { display:none; }" in узкий.group(1)
    assert ".drawer-freshness-short { display:inline; }" in узкий.group(1)
