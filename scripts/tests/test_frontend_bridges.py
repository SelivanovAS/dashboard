"""
Стражи глобального поиска между разделами и липкой полосы списка (app.js).

Контекст 10.08.2026. Юрист спросил, зачем картотеки «Основные» и «Иски банка»
разделены и нельзя ли заменить это ролевыми фильтрами. Разбор показал: граница
проходит не по роли, а по жизненному циклу (истцовые дела основной картотеки —
иски банка, ушедшие на обжалование и покинувшие трек), и слияние отвергнуто.
(Баннер-мостик при фильтре «Истец» был построен и УДАЛЁН тем же днём решением
юриста: он дублировал переключатель картотек, стоящий прямо над ним, и дёргал
раскладку — не возвращать.)

Замер 13.08.2026 подтвердил цену слияния на актуальных данных: 41 дело из 509
ушло бы в архив по обычному 60-дневному окну, а 21 исполнительный лист из 39
выдан ПОЗЖЕ 60-го дня (лаг решение → ИЛ: 43-104 дня) — трек живёт ровно ради
этих листов. Плюс +157 карточек за прогон и 87.5% строк дайджеста стали бы
трековыми.

В v155 переделан сам кросс-поиск. Было: подсказка «Найдено N в
картотеке X» с кнопкой «Показать», и ТОЛЬКО при нулевой выдаче — при 3 своих
и 2 соседских делах про соседские не узнавал никто. Стало: дела соседней
картотеки дописываются в filteredCases ХВОСТОМ под заголовком группы, без
переключения вида. Отдельный бейдж принадлежности не нужен — роль читается
из подсветки ПАО Сбербанк (решение юриста 11.08.2026, см.
test_bank_track_badge_stays_removed). Тогда же появилась липкая полоса списка
(капсула картотек + счётчик одной строкой) и вскрылось, что overflow-x:hidden
у body много месяцев ломал липкость самой .app-header. В v167 поиск стал
глобальным: текущий раздел первый, затем «Мои → Основные → Иски банка» без
повтора тех же starred-объектов.

Что охраняем:
1. Поисковый блоб один на предикат applyFilters и глобальный поиск (caseSearchBlob);
   отбор совпадений — один на список и счётчик (collectSearchMatches);
   архив обеих картотек в кросс-поиск не попадает.
2. Ленивая догрузка bank-списка из глобального поиска — под тройным гардом + флагом
   неудачи (loadBankDataset ошибку глотает, без флага каждый ввод в поиск
   ретраил бы мёртвую сеть); хвост не строится на запросе короче двух
   символов — иначе первая же буква тянула бы 1.6 МБ cases_bank.json.
2b. Липкая полоса: только в мобильном медиа-блоке, top:0 (с 14.08.2026 шапка
   на телефоне статична и уезжает с контентом — липнуть под неё нечему, а
   прежний top:var(--header-h) открыл бы над полосой щель в высоту шапки),
   класс is-sticky во всех трёх разделах, и ГЛАВНОЕ — у body нет overflow-x:hidden
   (он делает body скролл-контейнером и убивает sticky у всех потомков,
   включая десктопную шапку).
3. KPI и счётчики сегментов в «★ Мои» считаются по строгому watchlist-набору
   (`isWatchedCase`, без автоматического добавления новых дел) — до
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


def _const_src(src: str, name: str) -> str:
    """Строка объявления const — чистые функции часто читают такие пороги."""
    m = re.search(r"^const\s+" + re.escape(name) + r"\s*=.*$", src, re.M)
    assert m, f"Константа {name} не найдена."
    return m.group(0) + "\n"


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


def test_count_search_matches_wraps_collect():
    """Вторая реализация отбора разъехалась бы со счётчиком молча."""
    src = _app_js()
    fn = _fn_src(src, "countSearchMatches")
    assert "collectSearchMatches(list,q).length" in fn.replace(" ", ""), (
        "countSearchMatches перестал быть обёрткой над collectSearchMatches: "
        "два независимых фильтра дадут разное число в счётчике и в выдаче."
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


# ===== 2b. Липкая полоса списка (v155) =====

def test_list_bar_sticky_only_on_mobile():
    """Липкость объявлена в мобильном медиа-блоке и нигде больше.

    На десктопе список скроллится внутри .table-scroll, а не страницей —
    липнуть там нечему (решение юриста 13.08.2026: «только телефон»).
    """
    css = _read("styles.css")
    правила = re.findall(r"\.list-bar\.is-sticky\s*\{[^}]*\}", css)
    assert len(правила) == 1, (
        f"Правил .list-bar.is-sticky найдено {len(правила)}, ожидалось одно."
    )
    assert "position:sticky" in правила[0].replace(" ", ""), ".list-bar.is-sticky не липкая."
    # Ближайший ОТКРЫТЫЙ выше @media должен быть мобильным: блоков
    # max-width:768px в файле несколько, поэтому ищем именно предшествующий.
    # Именно ПРАВИЛО, а не упоминание в комментарии выше по файлу.
    до = css[: re.search(r"\.list-bar\.is-sticky\s*\{", css).start()]
    последний_media = до.rindex("@media")
    шапка = до[последний_media : последний_media + 40].replace(" ", "")
    assert "max-width:768px" in шапка, (
        f"Липкость полосы объявлена вне мобильного медиа-блока (ближайший выше "
        f"— «{шапка.strip()}»): на десктопе список скроллится внутри "
        ".table-scroll, липнуть там нечему."
    )
    assert re.search(r"top\s*:\s*0", правила[0]), (
        "Полоса липнет не к верху экрана. С 14.08.2026 шапка на телефоне "
        "статична и уезжает вместе с контентом — липнуть под неё нечему; "
        "top:var(--header-h) откроет над полосой щель в высоту шапки, "
        "сквозь которую виден уезжающий контент."
    )


def test_body_has_no_overflow_x():
    """overflow-x:hidden у body убивает position:sticky у ВСЕХ потомков.

    По спецификации overflow корневого элемента распространяется на вьюпорт,
    а body с собственным overflow становится скролл-контейнером — и sticky
    липнет к его боксу, который сам не скроллится. Из-за этого молча не
    липла .app-header (при scrollY=896 стояла на -827px). Правило снято
    13.08.2026; вернуть его — снова сломать обе липкости разом.
    """
    css = _read("styles.css")
    assert not re.search(r"^html,\s*body\s*\{[^}]*overflow-x\s*:\s*hidden", css, re.M), (
        "overflow-x:hidden вернулся на body — position:sticky у .app-header и "
        ".list-bar перестанет работать (обрезка вылета живёт на html)."
    )
    assert re.search(r"^html\s*\{[^}]*overflow-x\s*:\s*hidden", css, re.M), (
        "Пропала обрезка горизонтального вылета у html — появится "
        "горизонтальный скролл страницы на узких экранах."
    )
    мобильный = css[css.rindex("@media (max-width: 768px)") if "@media (max-width: 768px)" in css else css.rindex("@media (max-width:768px)"):]
    assert not re.search(r"\.app-main\s*\{[^}]*overflow-x\s*:\s*hidden", мобильный), (
        ".app-main {overflow-x:hidden} вернулся в мобильный блок — он делает "
        ".app-main скроллпортом, и липкая полоса на телефоне мертва."
    )


def test_list_bar_sticky_keeps_all_three_scopes_available():
    """После повышения «Моих» до раздела верхняя навигация не исчезает."""
    src = _app_js()
    fn = _fn_src(src, "renderDatasetSwitch")
    сжато = fn.replace(" ", "")
    assert "classList.add('is-sticky')" in сжато.replace('"', "'"), (
        "Верхняя навигация перестала быть липкой в одном из разделов — "
        "Основные / Иски банка / Мои должны оставаться равноправными."
    )
    for scope in ("main", "bank", "mine"):
        assert f"setDatasetView('{scope}')" in fn, (
            f"В верхнем переключателе пропал раздел {scope}."
        )
    assert "mineModeOn()){box.hidden=true" not in сжато, (
        "Переключатель снова скрывается внутри «Моих» — выйти из раздела "
        "без прокрутки/перезагрузки будет невозможно."
    )
    assert 'id="list-bar"' in _read("sberbank_dashboard.html"), (
        "Обёртка #list-bar исчезла из разметки — капсула и счётчик снова "
        "разъедутся на два ряда, и липнуть будет нечему."
    )


def test_counter_fit_ladder():
    """Счётчик разворачивается настолько, насколько хватает места.

    Решение юриста 14.08.2026: «когда места хватает — можно тут же более
    развёрнуто писать». fitCounter отрезает по одной наименее ценной части,
    пока текст не влезет рядом с капсулой картотек.
    """
    src = _app_js()
    fn = _fn_src(src, "fitCounter")
    assert "scrollWidth" in fn, (
        "fitCounter больше не меряет ширину — ступень перестанет зависеть от "
        "реального места, и на узком экране счётчик снова разорвёт полосу."
    )
    assert "is-sticky" in fn, (
        "fitCounter обязан отступать на десктопе (полоса не липкая, счётчик "
        "стоит своим рядом — там места вдоволь и резать нечего)."
    )
    assert "fitCounter()" in _fn_src(src, "renderCounter"), (
        "renderCounter не зовёт fitCounter — ступень застынет на прошлой."
    )
    assert re.search(r"_TC_LEVELS\s*=\s*\['lead','nouns','tails','slash'\]", src), (
        "Порядок ступеней изменился. Он не случаен: сперва уходит служебное "
        "«Показано», потом существительные, потом хвосты про архив и другие "
        "разделы, и только в конце «из» уступает косой черте."
    )
    # nowrap обязателен: иначе scrollWidth меряет уже перенесённый текст.
    css = _read("styles.css")
    assert re.search(r"\.list-bar\.is-sticky\s+\.table-counter\s*\{[^}]*white-space:\s*nowrap", css), (
        "Пропал white-space:nowrap у счётчика в липкой полосе — замер "
        "scrollWidth станет бессмысленным, и лестница ступеней сломается."
    )


def test_counter_hidden_when_it_cannot_fit():
    """Не влез даже кратчайшей формой — счётчик скрыт, а не перенесён.

    Решение юриста 14.08.2026: «на 320 вообще не надо счётчик писать».
    Перенос на вторую строку удваивал высоту ЛИПКОЙ полосы (50→75px) ради
    цифры, без которой можно жить.
    """
    fn = _fn_src(_app_js(), "fitCounter")
    assert "classList.add('is-squeezed')" in fn.replace('"', "'"), (
        "fitCounter больше не прячет счётчик — на узких экранах полоса снова "
        "станет двухстрочной."
    )
    assert fn.index("classList.remove('is-squeezed')".replace('"', "'")) < fn.index(
        "classList.add('is-squeezed')".replace('"', "'")
    ), (
        "Класс .is-squeezed обязан сниматься ДО замера: у спрятанного счётчика "
        "scrollWidth равен нулю, он «влезал» бы всегда и не возвращался."
    )
    css = _read("styles.css")
    assert re.search(
        r"\.list-bar\.is-sticky\s+\.table-counter\.is-squeezed\s*\{[^}]*display:\s*none", css
    ), (
        "Правило скрытия счётчика ослабло или уехало из липкой полосы — на "
        "десктопе счётчик прятать нельзя, там своя строка."
    )


def test_counter_separator_survives_word_trim():
    """«из» живёт в своём классе, иначе счётчик читается как «1166»."""
    src = _app_js()
    assert 'class="tc-of"' in src, (
        "Разделитель «из» снова в .tc-wordy: ступень «убрать слова» унесёт его "
        "вместе с существительными, и «1 из 166» схлопнется в «1166»."
    )
    css = _read("styles.css")
    assert re.search(r'\[data-fit="slash"\]\s*\.tc-of\s*\{[^}]*display:\s*none', css), (
        "На последней ступени «из» обязан уступать месту косой черте."
    )
    assert re.search(r'\[data-fit="slash"\]\s*\.tc-slash\s*\{[^}]*display:\s*inline', css), (
        "Косая черта не показывается на последней ступени — числа слипнутся."
    )


def test_header_sticky_desktop_only():
    """Шапка липкая на десктопе и статична на телефоне.

    Решение юриста 14.08.2026: на телефоне липкая шапка занимала ~73px
    первого экрана навсегда, а работа идёт в списке ниже. На десктопе
    липкость осталась — там она места почти не отнимает, а таблица и так
    скроллится внутри .table-scroll.
    """
    css = _read("styles.css")
    # Правил .app-header в файле три: базовое, мобильный оверрайд и fallback
    # для движков без backdrop-filter (позицию он не трогает). Берём те, что
    # вообще объявляют position.
    позиционные = [
        m
        for m in re.finditer(r"\.app-header\s*\{[^}]*\}", css)
        if "position:" in m.group(0).replace(" ", "")
    ]
    assert len(позиционные) == 2, (
        f"Правил .app-header с position найдено {len(позиционные)}, ожидалось "
        "два (базовое липкое + мобильный оверрайд со static)."
    )
    базовое, мобильное = позиционные[0], позиционные[1]
    assert "position:sticky" in базовое.group(0).replace(" ", ""), (
        "Базовое правило .app-header перестало быть липким — на десктопе "
        "шапка уедет вместе с контентом (юрист просил открепить только телефон)."
    )
    assert "position:static" in мобильное.group(0).replace(" ", ""), (
        "Мобильный оверрайд .app-header не задаёт position:static — шапка "
        "снова липнет на телефоне и съедает ~73px первого экрана."
    )
    до = css[: мобильное.start()]
    шапка_медиа = до[до.rindex("@media") : до.rindex("@media") + 40].replace(" ", "")
    assert "max-width:768px" in шапка_медиа, (
        f"Открепление шапки объявлено вне мобильного медиа-блока (ближайший "
        f"выше — «{шапка_медиа.strip()}»)."
    )
    # Комментарии срезаем: в них селектор упомянут намеренно — объяснением,
    # почему сжатие шапки при прокрутке не возвращать.
    без_комментариев = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    assert ".app-header.scrolled" not in без_комментариев, (
        "Вернулись правила .app-header.scrolled: у статичной шапки сжатие на "
        "scrollY≈30 укорачивает документ, и контент под ней прыгает на 4px."
    )
    # В комментариях app.js оба имени упомянуты намеренно — объяснением,
    # почему механику не возвращать; смотрим только на живой код.
    код = "\n".join(
        строка
        for строка in _app_js().splitlines()
        if not строка.lstrip().startswith("//")
    )
    for мёртвое in ("--header-h", "syncHeaderHeight"):
        assert мёртвое not in код, (
            f"{мёртвое} снова живёт в коде: потребителей у него нет (полоса "
            "липнет к top:0), а обработчик висел бы на каждом scroll."
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


# ===== 7. Бейдж «🏦 Иск банка» удалён решением юриста (11.08.2026) =====

def test_bank_track_badge_stays_removed():
    """В «★ Мои» карточки bank-трека носили бейдж «🏦 Иск банка» — юрист счёл
    его лишним: роль банка и так видна из строки сторон (ПАО Сбербанк
    подсвечен истцом), а принадлежность к внутренней картотеке в mine-списке
    не нужна. (Бейджи ролей в «Ближайших заседаниях» при этом ОСТАЮТСЯ —
    их снятие 11.08 юрист отменил.)"""
    src = _app_js()
    for след in ("bankTrackBadge", "Иск банка", "badge-bank-track"):
        assert след not in src, (
            f"«{след}» вернулся в app.js — бейдж «🏦 Иск банка» удалён "
            "решением юриста 11.08.2026, роль банка видна из строки сторон."
        )
    assert ".badge-bank-track" not in _read("styles.css"), (
        "Мёртвый CSS .badge-bank-track вернулся в styles.css."
    )
    # Контроль отмены: бейдж роли в «Ближайших заседаниях» жив.
    assert "ROLE_LABELS[c.sberbankRole]" in _fn_src(src, "renderAnalytics"), (
        "Бейдж роли пропал из «Ближайших заседаний» — юрист отменил его "
        "снятие (v134 → откат в v135), должен рендериться."
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
