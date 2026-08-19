"""Стражи фильтра «Суд» на дашборде (v172).

Ключ фильтра — нормализованное ИМЯ суда 1-й инстанции, НЕ court_domain и
НЕ srv_num: srv_num в данных у одного суда записан то числом, то null
(Сургутский городской раскалывался бы на два пункта), а у дел, заведённых
с выдачи апелляции, домена нет вовсе. Подробности — комментарий у
courtFilterNorm в app.js.
"""

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


def _court_script(src: str) -> str:
    return "\n".join(
        _fn(src, name)
        for name in ("shortCourt", "courtFilterNorm", "courtFilterKey",
                     "courtFilterEntries")
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_court_key_normalizes_eyo_spaces_and_keeps_pokachi_separate():
    script = _court_script(_read("app.js")) + """
console.log(JSON.stringify([
  courtFilterKey({firstInstanceCourt:'Берёзовский районный суд'})
    ===courtFilterKey({firstInstanceCourt:'Березовский  районный суд '}),
  courtFilterKey({firstInstanceCourt:''}),
  courtFilterKey(null),
  courtFilterKey({firstInstanceCourt:'Нижневартовский районный суд'})
    !==courtFilterKey({firstInstanceCourt:'Нижневартовский районный суд (г. Покачи)'})
]));
"""
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == '[true,"","",true]'


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_court_entries_count_label_sort_and_skip_empty():
    # Победа «ё»-подписи проверяется при ОБОИХ порядках подачи: детерминизм
    # не должен зависеть от порядка дел в датасете.
    script = _court_script(_read("app.js")) + """
const mk=n=>({firstInstanceCourt:n});
const a=courtFilterEntries([
  mk('Сургутский городской суд'), mk('Сургутский городской суд'),
  mk('Березовский районный суд'), mk('Берёзовский районный суд'),
  mk(''), {},
]);
const b=courtFilterEntries([
  mk('Берёзовский районный суд'), mk('Березовский районный суд'),
]);
console.log(JSON.stringify({
  n:a.length,
  labels:a.map(e=>e.label),
  shorts:a.map(e=>e.short),
  counts:a.map(e=>e.count),
  reversed:b[0].label,
}));
"""
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    import json

    data = json.loads(result.stdout)
    assert data["n"] == 2, "Дела без суда не должны давать пункт списка"
    assert data["labels"] == ["Берёзовский районный суд", "Сургутский городской суд"]
    assert data["shorts"] == ["Берёзовский р-ный суд", "Сургутский гор. суд"]
    assert data["counts"] == [2, 2]
    assert data["reversed"] == "Берёзовский районный суд"


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_court_predicate_passes_own_key_and_cuts_others():
    script = _court_script(_read("app.js")) + """
const c={firstInstanceCourt:'Сургутский городской суд'};
const key=courtFilterKey(c);
const pred=(c,court)=>!(court!=='all'&&courtFilterKey(c)!==court);
console.log(JSON.stringify([
  pred(c,'all'),
  pred(c,key),
  pred(c,'чужой суд'),
  pred({firstInstanceCourt:''},key)
]));
"""
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[true,true,false,false]"


def test_hidden_select_and_wiring():
    html = _read("sberbank_dashboard.html")
    court = re.search(r'<select[^>]+id="filter-court"[\s\S]*?</select>', html)
    assert court, "Скрытый #filter-court пропал из .toolbar-hidden-selects"
    assert 'value="all">Все суды' in court.group(0)

    src = _read("app.js")
    apply_fn = _fn(src, "applyFilters")
    assert "filter-court" in apply_fn
    assert "courtFilterKey(c)!==court" in apply_fn
    assert "setCourtFilter" in src
    assert "filter-court" in _fn(src, "resetFilters"), (
        "«Сбросить» обязан снимать и фильтр по суду"
    )
    assert "courtFilterEntries" in _fn(src, "populateFilterOptions")


def test_visible_select_has_no_id_and_counts_toward_filters_btn():
    fn = _fn(_read("app.js"), "renderChipBar")
    assert "setCourtFilter(this.value)" in fn
    assert "Суд" in fn
    # Видимый select уходит и в ряд сегментов, и в мобильную шторку одним
    # HTML — id на нём дал бы дубль id в документе.
    visible = re.search(r"<select[^>]*court-select[^>]*>", fn)
    assert visible, "Видимый селект «Суд» не найден в renderChipBar"
    assert " id=" not in visible.group(0)
    assert "courtVal&&courtVal!=='all'" in fn, (
        "Счётчик кнопки «Фильтры» не учитывает фильтр по суду"
    )


def test_lazy_loads_refresh_options_before_refilter():
    apply_fn = _fn(_read("app.js"), "applyFilters")
    # Догруженный bank-архив/список меняет состав судов — перед пересчётом
    # фильтров опции обязаны освежиться, иначе суд из архива не попадёт
    # в выпадашку.
    assert apply_fn.count("populateFilterOptions();applyFilters();") >= 3


def test_css_rules_for_both_layouts():
    css = _read("styles.css")
    assert ".chip-bar-segments .court-select" in css
    assert ".sheet-body .court-select" in css


def test_cache_versions_in_sync():
    html = _read("sberbank_dashboard.html")
    sw = _read("service-worker.js")
    html_versions = set(re.findall(r'(?:app\.js|styles\.css)\?v=(\d+)', html))
    assert len(html_versions) == 1, "app.js и styles.css с разными ?v="
    sw_version = re.search(r"CACHE_VERSION = 'v(\d+)'", sw).group(1)
    assert html_versions == {sw_version}, (
        "CACHE_VERSION service-worker разошёлся с ?v= в HTML"
    )
