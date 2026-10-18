"""Стражи drawer'а во фронте: раскладка hero и дата проверки сайта суда."""

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
