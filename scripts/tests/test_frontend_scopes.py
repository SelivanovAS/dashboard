"""Стражи трёх разделов и раздельного состояния фильтров (v167)."""

from __future__ import annotations

from .js_support import read_source as _read

import re
import shutil
import subprocess

import pytest


NODE = shutil.which("node")


def _fn(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена"
    return m.group(0)


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


def test_mobile_mine_shortcut_hides_without_subscriptions_and_syncs():
    src = _read("app.js")
    sync = _fn(src, "syncMobileMineButton")
    assert "btn.hidden=watchlist.size===0" in sync.replace(" ", "")
    assert "activeScope()==='mine'" in sync.replace(" ", "")
    refresh = _fn(src, "refreshDigestModeVisibility")
    assert "el.hidden = !visible" in refresh
    css = _read("styles.css")
    assert ".toolbar-mine-btn[hidden]" in css
