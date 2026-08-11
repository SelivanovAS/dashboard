"""
Стражи кросс-поиска между картотеками и полировки фильтров (v132, app.js).

Контекст 10.08.2026. Юрист спросил, зачем картотеки «Основные» и «Иски банка»
разделены и нельзя ли заменить это ролевыми фильтрами. Разбор показал: граница
проходит не по роли, а по жизненному циклу (58 из 59 «истцовых» дел основной
картотеки — иски банка, ушедшие на обжалование и ПЕРЕЕХАВШИЕ из трека), и
слияние отвергнуто. Вместо него — кросс-поиск по соседней картотеке при
нулевой выдаче и честные счётчики. (Баннер-мостик при фильтре «Истец» был
построен и УДАЛЁН тем же днём решением юриста: он дублировал переключатель
картотек, стоящий прямо над ним, и дёргал раскладку — не возвращать.)

Что охраняем:
1. Поисковый блоб один на предикат applyFilters и кросс-поиск (caseSearchBlob);
   кросс-подсчёт не считает архив обеих картотек.
2. Ленивая догрузка bank-списка из кросс-поиска — под тройным гардом + флагом
   неудачи (loadBankDataset ошибку глотает, без флага каждый ввод в поиск
   ретраил бы мёртвую сеть).
3. KPI и счётчики сегментов в «★ Мои» считаются по mine-набору (тот же
   предикат isWatchedCase||isNewCase, что и в mine-ветке applyFilters) — до
   v132 плитки показывали цифры всей основной картотеки.
4. «Сбросить» сбрасывает и категорию; счётчик кнопки «Фильтры» учитывает
   категорию и не считает роль/инстанцию в bank-режиме (там они игнорируются).
5. archived_count принимается только из cases_bank.json (isBankListUrl) —
   иначе поле из чужого файла молча портило бы счётчик архива банка.
6. Знаменатели счётчиков картотек — АКТИВНЫЕ дела, архив — отдельным хвостом
   (до v132 «Основные 225» считались с архивом, «Иски банка 499» — без).

JS-инструментария в проекте нет: чистые функции исполняются в node, проводка
проверяется grep'ом по исходнику — тем же приёмом, что test_frontend_writs.py.

Запуск: python3 -m pytest scripts/tests/test_frontend_bridges.py
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


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _app_js() -> str:
    return _read("app.js")


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# ===== 1. Баннер-мостик удалён решением юриста (10.08.2026) =====

def test_bridge_banner_stays_removed():
    """Баннер при фильтре «Истец» дублировал переключатель картотек — удалён."""
    src = _app_js()
    for след in ("renderContextBridge", "plaintiffBridgeVisible", "context-bridge"):
        assert след not in src, (
            f"«{след}» вернулся в app.js — баннер-мостик удалён решением юриста "
            "(дублировал #dataset-switch прямо над собой и дёргал раскладку); "
            "навигацию между картотеками несёт кросс-поиск."
        )
    assert 'id="context-bridge"' not in _read("sberbank_dashboard.html")


# ===== 2. Единый поисковый блоб =====

def test_search_blob_single_source():
    src = _app_js()
    assert src.count("c.computed?c.computed.searchBlob") == 1, (
        "Фолбэк-склейка поискового блоба обязана жить только в caseSearchBlob: "
        "вторая копия в applyFilters разъедется с кросс-поиском молча."
    )
    fn = _fn_src(src, "applyFilters")
    assert "caseSearchBlob(c).includes(q)" in fn, (
        "Предикат поиска в applyFilters не использует caseSearchBlob — "
        "кросс-поиск будет искать не тем блобом, что основная выдача."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_cross_search_counts():
    """countSearchMatches: находит по блобу, не считает архив ОБЕИХ картотек."""
    src = _app_js()
    script = (
        _fn_src(src, "caseSearchBlob")
        + _fn_src(src, "caseArchived")
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
]));
"""
    )
    got = json.loads(_node(script))
    assert got == [1, 2, 1, 0], (
        f"countSearchMatches: {got}, ожидалось [1, 2, 1, 0] — архивные записи "
        "обеих картотек (computed.archived и _bankArchived) не считаются."
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
    mine_pred = "isWatchedCase(c)||isNewCase(c)"
    stats = _fn_src(src, "renderStats")
    assert f"mineModeOn()?activeDataset().filter(c=>{mine_pred}):allCases" in stats, (
        "renderStats в «★ Мои» обязан считать KPI по mine-набору обеих картотек "
        "(до v132 плитки показывали цифры всей основной картотеки)."
    )
    assert "mainKpiCounts(src)" in stats, (
        "Подсчёты KPI обязаны идти через mainKpiCounts — чистую функцию "
        "гоняет node-тест."
    )
    chip = _fn_src(src, "renderChipBar")
    assert f"mineModeOn()?activeDataset().filter(c=>{mine_pred}):allCases" in chip, (
        "Счётчики сегментов стадий в «★ Мои» обязаны считаться по mine-набору: "
        "иначе сегмент «Апелляция» показывается при заведомо пустой выдаче "
        "(все bank-звёзды — 1-я инстанция)."
    )
    for корзина in ("first_instance", "appeal", "cassation"):
        assert f"segSrc.filter(c=>stageGroup(c)==='{корзина}')" in chip, (
            f"Счётчик сегмента «{корзина}» ушёл с segSrc/stageGroup."
        )
    # Страж синхронности: mine-ветка предиката applyFilters использует тот же
    # предикат (в отрицании) — если её переименуют, править оба места.
    assert "!isWatchedCase(c)&&!isNewCase(c)" in _fn_src(src, "applyFilters"), (
        "Mine-ветка applyFilters изменилась — синхронизировать предикат "
        "с renderStats/renderChipBar (KPI обязаны считать тот же набор)."
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
    assert "!bankViewActive&&rl&&rl!=='all'" in chip, (
        "Счётчик кнопки «Фильтры» считает роль в bank-режиме, где applyFilters "
        "её игнорирует — кнопка врёт."
    )
    assert "!bankViewActive&&stg&&stg!=='all'" in chip, (
        "Счётчик кнопки «Фильтры» считает инстанцию в bank-режиме."
    )
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


# ===== 6. Hover — только устройства с курсором (скрин юриста 10.08.2026) =====

def test_hover_rules_touch_safe():
    """Залипший тач-hover iOS красил АКТИВНЫЙ сегмент в бледный --bg-3 при
    белом тексте («Иски банка»/«Истец» пропадали). Hover-фон .chip-btn/.seg-btn
    обязан жить в @media (hover:hover) и не трогать .active."""
    css = _read("styles.css")
    for селектор in (".chip-btn:hover", ".seg-btn:hover"):
        for m in re.finditer(re.escape(селектор) + r"[^{]*\{[^}]*background", css):
            # Правило с фоном обязано быть внутри @media (hover:hover)...
            before = css[: m.start()]
            блок = before.rfind("@media (hover:hover)")
            закрытий = before[блок:].count("}") if блок >= 0 else 0
            открытий = before[блок:].count("{") if блок >= 0 else 0
            assert блок >= 0 and открытий > закрытий, (
                f"{селектор} с background вне @media (hover:hover): на iOS "
                "hover залипает после касания и перекрашивает кнопку."
            )
            # ...и нести гард :not(.active): специфичность :hover:not(...) выше
            # .seg-btn.active, бледный фон победил бы зелёный у активной кнопки.
            assert ":not(.active)" in m.group(0), (
                f"{селектор} без :not(.active): при наведении/тапе активная "
                "кнопка теряет зелёный фон, оставаясь с белым текстом."
            )


# ===== 7. «Ближайшие заседания»: бейдж роли только у 3-го лица =====

def test_upcoming_role_badge_third_party_only():
    """Решение юриста 11.08.2026: «Истец»/«Ответчик» в карточках заседаний —
    шум (роль видна из строки сторон: Сбербанк подсвечен, порядок «истец vs
    ответчик»). Бейдж остаётся ТОЛЬКО у 3-го лица — там банка в сторонах нет
    вовсе. Тот же принцип, что у roleBadge в hero drawer'а."""
    src = _app_js()
    fn = _fn_src(src, "renderAnalytics")
    assert "ROLE_LABELS" not in fn, (
        "renderAnalytics снова печатает подпись роли из ROLE_LABELS — "
        "«Истец»/«Ответчик» в карточках заседаний юрист велел убрать."
    )
    assert "sberbankRole==='third_party'" in fn and "Сбер 3-е лицо" in fn, (
        "Бейдж «Сбер 3-е лицо» пропал из «Ближайших заседаний»: у дел 3-го "
        "лица банка в строке сторон нет, без бейджа связь с банком невидима."
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
