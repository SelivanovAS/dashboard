"""Стражи глобального поиска между разделами и липкой полосы списка (app.js)."""

from __future__ import annotations

from .js_support import read_source as _read, extract_function as _fn_src

import json
import re
import shutil
import subprocess

import pytest


NODE = shutil.which("node")


def _app_js() -> str:
    return _read("app.js")


def _const_src(src: str, name: str) -> str:
    """Строка объявления const — чистые функции часто читают такие пороги."""
    m = re.search(r"^const\s+" + re.escape(name) + r"\s*=.*$", src, re.M)
    assert m, f"Константа {name} не найдена."
    return m.group(0) + "\n"


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# ===== 2. Единый поисковый блоб =====


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_cross_search_counts():
    """collectSearchMatches: находит по блобу, не берёт архив ОБЕИХ картотек."""
    src = _app_js()
    script = (
        _fn_src(src, "caseSearchBlob")
        + _fn_src(src, "caseArchived")
        + _fn_src(src, "collectSearchMatches")
        + _fn_src(src, "countSearchMatches")
        + """
const list=[
  {computed:{archived:false,searchBlob:'2-518/2026 макарова сбербанк сургут'}},
  {computed:{archived:true, searchBlob:'2-999/2020 сбербанк старое'}},
  {_bankTrack:true,_bankArchived:false,computed:{searchBlob:'2-100/2026 иванов сбербанк'}},
  {_bankTrack:true,_bankArchived:true, computed:{searchBlob:'2-200/2026 иванов сбербанк'}},
];
console.log(JSON.stringify([
  countSearchMatches(list,'иванов'),
  countSearchMatches(list,'сбербанк'),
  countSearchMatches(list,'2-518'),
  countSearchMatches(list,'нет-такого'),
  // Список и счётчик обязаны сходиться: счётчик — обёртка над списком.
  collectSearchMatches(list,'сбербанк').length,
]));
"""
    )
    got = json.loads(_node(script))
    assert got == [1, 2, 1, 0, 2], (
        f"countSearchMatches: {got}, ожидалось [1, 2, 1, 0, 2] — архивные записи "
        "обеих картотек (computed.archived и _bankArchived) не берём, а счётчик "
        "обязан быть обёрткой над collectSearchMatches."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_global_search_priority_and_dedup():
    """Текущий раздел уже впереди; хвост: Мои → Основные → Иски банка."""
    src = _app_js()
    script = (
        "function caseArchived(c){return !!c.archived;}\n"
        + _fn_src(src, "caseSearchBlob")
        + _fn_src(src, "collectSearchMatches")
        + _const_src(src, "CROSS_MIN_QUERY")
        + _const_src(src, "SEARCH_SCOPE_PRIORITY")
        + _const_src(src, "SEARCH_SCOPE_LABELS")
        + _fn_src(src, "buildGlobalSearchTail")
        + """
const item=id=>({id,computed:{archived:false,searchBlob:'сбербанк '+id}});
const mainCurrent=item('main-current');
const mineMain=item('mine-main');
const mineBank=item('mine-bank');
const bankOnly=item('bank-only');
const mainOnly=item('main-only');
const lists={
  mine:[mineMain,mineBank],
  main:[mainCurrent,mineMain,mainOnly],
  bank:[mineBank,bankOnly],
};
const fromMain=buildGlobalSearchTail('main','сбербанк',lists,[mainCurrent]);
const fromMine=buildGlobalSearchTail('mine','сбербанк',lists,[mineMain]);
const short=buildGlobalSearchTail('main','с',lists,[mainCurrent]);
console.log(JSON.stringify({
  mainItems:fromMain.items.map(x=>x.id),
  mainGroups:fromMain.groups.map(g=>[g.scope,g.count,g.offset]),
  mineItems:fromMine.items.map(x=>x.id),
  mineGroups:fromMine.groups.map(g=>[g.scope,g.count,g.offset]),
  short,
}));
"""
    )
    got = json.loads(_node(script))
    assert got == {
        "mainItems": ["mine-main", "mine-bank", "bank-only"],
        "mainGroups": [["mine", 2, 0], ["bank", 1, 2]],
        "mineItems": ["main-current", "main-only", "mine-bank", "bank-only"],
        "mineGroups": [["main", 2, 0], ["bank", 2, 2]],
        "short": {"items": [], "groups": []},
    }


def test_cross_tail_wiring():
    """Группы дописываются в filteredCases, счётчик не путает их с текущей."""
    src = _app_js()
    fn = _fn_src(src, "applyFilters")
    compact = fn.replace(" ", "").replace("\n", "")
    assert "crossCount=globalTail.items.length" in compact, (
        "applyFilters не выставляет crossCount — граница своей выдачи и хвоста "
        "потеряна, счётчик и заголовок группы разъедутся."
    )
    assert "filteredCases.concat(globalTail.items)" in fn, (
        "Хвост кросс-поиска больше не дописывается в filteredCases: на этом "
        "массиве висят стрелки drawer, фокус клавиатуры и пагинация."
    )
    assert "searchGroups=globalTail.groups.map" in compact
    counter = _fn_src(src, "renderCounter")
    assert "crossStartIdx()" in counter, (
        "renderCounter снова считает по filteredCases.length — в «Показано N» "
        "попадут дела других разделов, а знаменатель остался свой."
    )
    for имя in ("renderTable", "renderMobileCards"):
        rendered = _fn_src(src, имя)
        assert "crossStartIdx()" in rendered and "searchGroupAt(idx)" in rendered, (
            f"{имя} не рисует отдельные заголовки разделов глобального поиска."
        )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_case_search_blob_fallback():
    """Дело без computed (не должно случаться, но фолбэк обязан работать)."""
    src = _app_js()
    script = _fn_src(src, "caseSearchBlob") + """
const c={caseNumber:'2-1/2026',plaintiff:'ПАО Сбербанк',defendant:'Иванов И.И.',
  category:'кредит',firstInstanceCourt:'Сургутский',lastEvent:'',notes:''};
console.log(JSON.stringify([caseSearchBlob(c).includes('иванов'),
  caseSearchBlob(c).includes('2-1/2026')]));
"""
    assert json.loads(_node(script)) == [True, True]


def test_cross_search_lazy_guard():
    """Догрузка bank-списка из кросс-поиска — тройной гард + флаг неудачи."""
    src = _app_js()
    fn = _fn_src(src, "renderSearchCrossHint")
    assert "renderSearchCrossHint()" in _fn_src(src, "applyFilters"), (
        "renderSearchCrossHint выпал из хвоста applyFilters."
    )
    assert 'id="search-cross-hint"' in _read("sberbank_dashboard.html"), (
        "Контейнер #search-cross-hint исчез из HTML."
    )
    assert "bankFileExists&&!bankLoaded&&!bankListLoading&&!_crossHintLoadFailed" in fn, (
        "Гард фоновой догрузки ослаб: без bankListLoading параллельные вызовы "
        "плодят fetch'и, без _crossHintLoadFailed каждый ввод в поиск ретраит "
        "мёртвую сеть (loadBankDataset ошибку глотает)."
    )
    assert "mineModeOn()" not in fn, (
        "Глобальный поиск снова отключён в разделе «Мои»."
    )
    assert "document.getElementById('search-input').value" in fn.split("loadBankDataset", 1)[1], (
        "После фоновой загрузки нет проверки, что поиск ещё не очищен — "
        "applyFilters дёрнется впустую."
    )
    for запрет in ("ensureBankArchive", "ensureBankEvents"):
        assert запрет not in fn, (
            f"Кросс-поиск зовёт {запрет} — глубина ленивой цепочки не должна "
            "расти: список догружается, архив и events ждут своих триггеров."
        )


# ===== 3. «★ Мои»: честные KPI и сегменты =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_main_kpi_counts():
    src = _app_js()
    script = "function getResultFavor(c){return c._favor||'none';}\n" + _fn_src(
        src, "mainKpiCounts"
    ) + """
const list=[
  {status:'active'},
  {status:'decided',_favor:'favorable',hasPublishedActs:true,actDate:'2099-01-01'},
  {status:'decided',_favor:'favorable'},
  {status:'decided',_favor:'unfavorable'},
];
console.log(JSON.stringify(mainKpiCounts(list)));
"""
    got = json.loads(_node(script))
    assert got == {
        "active": 1, "won": 2, "lost": 1,
        "meaningful": 3, "winRate": 67, "freshActs": 1,
    }, f"mainKpiCounts: {got}"


def test_mine_kpi_and_segments_use_mine_set():
    """KPI и сегменты в mine-режиме — по mine-набору, предикат = applyFilters."""
    src = _app_js()
    stats = _fn_src(src, "renderStats")
    assert "mineModeOn()?scopedDataset():allCases" in stats, (
        "renderStats в «★ Мои» обязан считать KPI по mine-набору обеих картотек "
        "(до v132 плитки показывали цифры всей основной картотеки)."
    )
    assert "mainKpiCounts(src)" in stats, (
        "Подсчёты KPI обязаны идти через mainKpiCounts — чистую функцию "
        "гоняет node-тест."
    )
    chip = _fn_src(src, "renderChipBar")
    assert "const mineSrc=mineDataset()" in chip, (
        "Контекстные сегменты «Моих» считают не watchlist обеих картотек."
    )
    apply = _fn_src(src, "applyFilters")
    assert "mineOn&&!isWatchedCase(c)" in apply, (
        "«Мои» снова допускают дела без звезды."
    )
    assert "mineOn&&!q" not in apply, (
        "Поиск снова отключает ограничение watchlist и уходит во всю базу."
    )
    assert "isWatchedCase(c)||isNewCase(c)" not in stats + chip + apply, (
        "Новые дела без звезды снова автоматически подмешиваются в «Мои»."
    )


# ===== 4. resetFilters и счётчик кнопки «Фильтры» =====

def test_reset_and_btn_count():
    src = _app_js()
    reset = _fn_src(src, "resetFilters")
    assert "filter-category" in reset, (
        "«Сбросить» снова не сбрасывает категорию — юрист не поймёт, почему "
        "список неполон (видимого сеттера у категории нет)."
    )
    chip = _fn_src(src, "renderChipBar")
    for scope in ("scope==='main'", "scope==='bank'", "filter-mine-source",
                  "filter-mine-role", "filter-mine-stage", "filter-bank-control"):
        assert scope in chip, f"Счётчик фильтров не учитывает контекст {scope}."
    apply = _fn_src(src, "applyFilters")
    assert "const scope=activeScope()" in apply
    assert "scope==='bank'" in apply and "filter-bank-control" in apply
    assert "scope==='mine'" in apply and "filter-mine-source" in apply
    m = re.search(r"filters-btn-count[\s\S]*?display='none'", chip)
    assert m and "filter-category" in m.group(0), (
        "Счётчик кнопки «Фильтры» не учитывает категорию — активная категория "
        "не видна как фильтр."
    )


# ===== 5. Гард archived_count =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_is_bank_list_url():
    src = _app_js()
    script = _fn_src(src, "isBankListUrl") + """
console.log(JSON.stringify([
  isBankListUrl('data/cases_bank.json'),
  isBankListUrl('https://x.github.io/dashboard/data/cases_bank.json?v=3'),
  isBankListUrl('data/cases.json'),
  isBankListUrl('data/cases_bank_archive.json'),
  isBankListUrl('data/cases_bank_events.json'),
  isBankListUrl(''),
]));
"""
    got = json.loads(_node(script))
    assert got == [True, True, False, False, False, False], f"isBankListUrl: {got}"


def test_bank_meta_url_guard_wired():
    src = _app_js()
    fetch_fn = _fn_src(src, "fetchJsonCases")
    m = re.search(r"[^\n]*bankArchivedMeta=data\.archived_count[^\n]*", fetch_fn)
    assert m, "Присвоение bankArchivedMeta из archived_count исчезло из fetchJsonCases."
    assert "isBankListUrl(url)" in m.group(0), (
        "bankArchivedMeta пишется без гарда isBankListUrl — archived_count из "
        "чужого файла молча испортит счётчик архива банка."
    )


# ===== 8. Знаменатели — активные дела =====

def test_counters_active_denominator():
    src = _app_js()
    ds = _fn_src(src, "renderDatasetSwitch")
    assert "allCases.filter(c=>!caseArchived(c)).length" in ds, (
        "Сегмент «Основные» снова считает с архивом (allCases.length): "
        "асимметрия с bank-сегментом, который считается по активным."
    )
    counter = _fn_src(src, "renderCounter")
    assert "allCases.length-archivedCount" in counter, (
        "Знаменатель счётчика основной картотеки снова включает архив — "
        "инвариант «знаменатели = активные, архив хвостом» нарушен."
    )
