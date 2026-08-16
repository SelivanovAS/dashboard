"""Стражи трёх разделов и раздельного состояния фильтров (v167)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NODE = shutil.which("node")


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _fn(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена"
    return m.group(0)


def test_scope_switch_precedes_toolbar_and_has_three_sections():
    html = _read("sberbank_dashboard.html")
    assert html.index('id="dataset-switch"') < html.index('class="toolbar"'), (
        "Выбор раздела снова оказался ниже поиска/фильтров."
    )
    assert 'id="toolbar-mine-btn"' in html, (
        "Пропал мобильный быстрый вход в подписки рядом с поиском."
    )
    assert re.search(r'id="toolbar-mine-btn"[\s\S]*?\bhidden\b', html)
    assert 'onclick="toggleMobileMine()"' in html
    fn = _fn(_read("app.js"), "renderDatasetSwitch")
    for value, label in (("main", "Основные"), ("bank", "Иски банка"),
                         ("mine", "Мои")):
        assert f"setDatasetView('{value}')" in fn
        assert label in fn
    assert "scopeNavIcon('main')" not in fn
    assert "scopeNavIcon('bank')" not in fn
    assert "scopeMineIcon()" in fn
    assert "watchlist.size>0" not in fn, "«Мои» снова скрываются при нуле."


def test_common_status_and_context_filters_are_separate():
    html = _read("sberbank_dashboard.html")
    status = re.search(
        r'<select[^>]+id="filter-status"[\s\S]*?</select>', html
    ).group(0)
    assert 'value="writs"' not in status
    assert 'value="awaiting_writ"' not in status
    for filter_id in (
        "filter-bank-control", "filter-mine-source", "filter-mine-role",
        "filter-mine-stage", "filter-role", "filter-stage",
    ):
        assert f'id="{filter_id}"' in html

    apply = _fn(_read("app.js"), "applyFilters")
    assert "const scope=activeScope()" in apply
    assert "scope==='bank'" in apply and "filter-bank-control" in apply
    assert "scope==='mine'" in apply and "filter-mine-source" in apply
    assert "mineOn&&!isWatchedCase(c)" in apply
    assert "mineOn&&!q" not in apply


def test_rendered_context_groups_match_scope():
    fn = _fn(_read("app.js"), "renderChipBar")
    for label in ("Роль", "Инстанция", "Контроль", "Источник"):
        assert label in fn
    for setter in (
        "setRoleFilter", "setStageFilter", "setBankControlFilter",
        "setMineSourceFilter", "setMineRoleFilter", "setMineStageFilter",
    ):
        assert setter in fn
    assert "setStatusFilter('writs')" not in fn
    assert "setStatusFilter('awaiting_writ')" not in fn
    assert "stageGroupHtml(stg,'setMineStageFilter',mineSrc,true)" in fn, (
        "Группа инстанций в «Моих» не должна исчезать при пустом watchlist."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_bank_control_predicate_covers_three_queues():
    src = _read("app.js")
    script = """
function caseArchived(c){return !!c.archived;}
function hasEnforcementWrit(c){return !!c.enforcement;}
function awaitsWrit(c){return !!c.awaiting;}
function hasInterimWrit(c){return !!c.interim;}
""" + _fn(src, "bankControlMatches") + """
const c={enforcement:true,awaiting:false,interim:true};
console.log(JSON.stringify([
  bankControlMatches(c,'all'),
  bankControlMatches(c,'writs'),
  bankControlMatches(c,'awaiting_writ'),
  bankControlMatches(c,'interim'),
  bankControlMatches({...c,archived:true},'writs')
]));
"""
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[true,true,false,true,false]"


def test_scope_layout_has_labels_and_stable_width():
    css = _read("styles.css")
    assert ".filter-group-label" in css
    assert re.search(r"\.dataset-switch\s*\{[^}]*flex:\s*0\s+1\s+520px", css)
    assert ".scope-switch .seg-btn" in css
    assert ".scope-nav-icon" in css
    assert "background:var(--scope-nav-bg)" in css
    assert "box-shadow:var(--scope-nav-active-shadow)" in css
    assert ".scope-switch .seg-btn:not(:last-child)" in css
    dark_start = css.index('[data-theme="dark"]')
    assert "--scope-nav-active-bg" in css[:dark_start]
    assert "--scope-nav-active-bg" in css[dark_start:]
    assert ".filter-group-bank-control .seg-ctrl" in css
    assert "grid-template-columns:repeat(2, minmax(0, 1fr))" in css
    assert "font-size:clamp(12px" in css, (
        "Кегль верхнего переключателя больше не подстраивается под ширину телефона."
    )


def test_mobile_mine_shortcut_hides_without_subscriptions_and_syncs():
    src = _read("app.js")
    sync = _fn(src, "syncMobileMineButton")
    assert "btn.hidden=watchlist.size===0" in sync.replace(" ", "")
    assert "activeScope()==='mine'" in sync.replace(" ", "")
    refresh = _fn(src, "refreshDigestModeVisibility")
    assert "el.hidden = !visible" in refresh
    css = _read("styles.css")
    assert ".toolbar-mine-btn[hidden]" in css
