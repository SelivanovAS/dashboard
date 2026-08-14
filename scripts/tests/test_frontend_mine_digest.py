"""
Стражи mine-версии дайджеста на фронте (app.js): секция «🏦 ИСКИ БАНКА».

Контекст 13.08.2026. Юрист: «при выборе моих дел в дайджесте должны быть
только мои дела + новые дела, поступившие в суд; по искам банка в первой
инстанции это не работает». Разбор нашёл две независимые поломки:

A. Регекспы секций не знали заголовков «🏦 ИСКИ БАНКА», «📑 Касс. события» и
   «⚖️🔬 КАССАЦИЯ» — незнакомый заголовок не сбрасывает состояние машины
   filterGeneralHtmlByMine, и секция банка наследовала 'new' от «📥 Новые
   дела» апелляции, то есть сохранялась ЦЕЛИКОМ. Тот же класс бага чинили
   12.08.2026 на Python-стороне (`_DIGEST_HEADER_RE`, postprocess.py): наборы
   эмодзи держать наравне. Альтернатива ⚖️🔬 обязана стоять ПЕРЕД одиночным
   ⚖ — после него идёт 🔬, а не <b>.
B. Звезда иска банка хранится composite-формой «домен|номер» (номера не
   уникальны между судами), а фильтр сравнивал голый номер из HTML. Починка
   одних регекспов выкинула бы из mine-версии ВСЕ звёздные дела банка.
   Матчинг зеркалит `_fi_change_matches` (delivery.py).

Что охраняем:
1. Наборы эмодзи в SECTION_*_RE (grep) — 🏦/📑 фильтруются, ⚖️🔬 распознаётся,
   📌 (футер) закрывает секцию, «Новые касс» общесистемны.
2. Composite-матчинг банк-строк по домену из href (mineRefMatches).
3. Новые иски банка (fi_bank_claim_registered) проходят как «новые дела» —
   без звезды. Push этим НЕ затронут: там они по-прежнему по watchlist
   (решение юриста 13.08.2026).
4. Счётчик found в buildMineHtml считается тем же предикатом — иначе
   bank-only совпадение давало ложный фолбэк «показан общий дайджест».
5. Заголовки-сироты (группа, у которой всё выкинуто) убираются, футер — нет.

JS-инструментария в проекте нет: чистые функции исполняются в node, проводка
проверяется grep'ом — тем же приёмом, что test_frontend_bridges.py.

Запуск: python3 -m pytest scripts/tests/test_frontend_mine_digest.py
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


def _app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _const_src(src: str, name: str) -> str:
    m = re.search(r"^const\s+" + re.escape(name) + r"\s*=.*?;$", src, re.M)
    assert m, f"Константа {name} не найдена."
    return m.group(0)


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


_CONSTS = (
    "SECTION_NEW_RE",
    "SECTION_HEADER_RE",
    "SECTION_FILTERED_RE",
    "SECTION_GROUPING_RE",
    "SECTION_FOOTER_RE",
    "SECTION_BANK_RE",
    "MINE_CASE_RE",
    "MINE_HREF_DOMAIN_RE",
)

_FNS = (
    "bareCaseNumber",
    "canonCaseNumber",
    "caseRefsInFragment",
    "mineRefMatches",
    "retitleSectionHeader",
    "filterGeneralHtmlByMine",
    "collectNewCaseNumbers",
)


def _harness(src: str, extra_fns: tuple[str, ...] = ()) -> str:
    """Изолированный кусок app.js: константы секций + чистые функции фильтра."""
    parts = [_const_src(src, c) for c in _CONSTS]
    parts.append("let watchCanonMap = new Map();")
    parts += [_fn_src(src, f) for f in _FNS + extra_fns]
    return "\n".join(parts) + "\n"


# Миниатюра боевого дайджеста (структура секций — как в data/last_digest.json
# от 13.08.2026): группы 🏛/⚖️/⚖️🔬, фильтруемые 📅/📑/🏦, общесистемная 📥,
# разделители «⸻» внутри банк-секции и футер 📌.
DIGEST = "\n\n".join([
    "📊 <b>Мониторинг дел Сбербанка ХМАО-Югра — 13.08.2026</b>\n📋 <b>Сводка</b>",
    "🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>",
    "📅 <b>Изменения (2):</b>",
    '<a href="https://surggor--hmao.sudrf.ru/x?name_op=case"><b>2-8373/2026</b></a> — 📅 заседание',
    '<a href="https://uganskray--hmao.sudrf.ru/x?name_op=case"><b>9-178/2026</b></a> — 🔚 возвращён',
    "⚖️ <b>АПЕЛЛЯЦИЯ</b>",
    "📥 <b>Новые дела (1):</b>",
    '<a href="https://oblsud--hmao.sudrf.ru/x?name_op=case"><b>33-5715/2026</b></a> — поступило',
    "⚖️🔬 <b>КАССАЦИЯ</b>",
    "📑 <b>Касс. события (1):</b>",
    '<a href="https://7kas.sudrf.ru/x?name_op=case"><b>2-1000/2025</b></a> — 8Г-11469/2026',
    "🏦 <b>ИСКИ БАНКА (4):</b>",
    '<a href="https://vartovgor--hmao.sudrf.ru/x?name_op=case"><b>2-6736/2026</b></a> — 📅 заседание',
    "⸻",
    '<a href="https://surggor--hmao.sudrf.ru/x?name_op=case"><b>2-6736/2026</b></a> — 🧾 ИЛ выдан',
    '<a href="https://surggor--hmao.sudrf.ru/x?name_op=case"><b>М-7220/2026</b></a> — 🆕 иск банка',
    '<a href="https://megion--hmao.sudrf.ru/x?name_op=case"><b>2-500/2026</b></a> — 📅 заседание',
    "📌 <b>В производстве: всего 79</b>\n<a href=\"https://example.test/d\">📊 Дашборд</a>",
])

# Контекст: одно новое дело апелляции + один новый иск банка (авто-подхват).
CTX = {
    "fi_new_cases": [],
    "new_cases": [{"Номер дела": "33-5715/2026"}],
    "fi_changes": [
        {
            "case": "М-7220/2026",
            "type": ["fi_bank_claim_registered"],
            "track": "plaintiff_light",
            "details": {"court_domain": "surggor--hmao.sudrf.ru"},
        },
        {
            "case": "2-500/2026",
            "type": ["fi_hearing_postponed"],
            "track": "plaintiff_light",
            "details": {"court_domain": "megion--hmao.sudrf.ru"},
        },
    ],
}


def _run_filter(stars: list[str]) -> list[str]:
    """Прогнать фильтр в node, вернуть первые строки сохранённых параграфов."""
    src = _app_js()
    script = _harness(src) + """
const DIGEST = %s, CTX = %s, STARS = %s;
const mine = new Set(STARS.map(canonCaseNumber));
for (const n of collectNewCaseNumbers(CTX)) mine.add(canonCaseNumber(n));
const out = filterGeneralHtmlByMine(DIGEST, mine);
console.log(JSON.stringify(out.split(/\\n{2,}/).filter(Boolean)
  .map((p) => p.split('\\n')[0])));
""" % (json.dumps(DIGEST), json.dumps(CTX), json.dumps(stars))
    return json.loads(_node(script))


def _numbers(lines: list[str]) -> list[str]:
    out = []
    for line in lines:
        m = re.search(r"<b>([^<]+)</b></a>", line)
        if m:
            out.append(m.group(1))
    return out


# ===== 1. Наборы эмодзи в регекспах секций =====

def test_bank_and_cassation_sections_are_filterable():
    """🏦 и 📑 — списки дел: без них секции наследуют режим предыдущей."""
    src = _app_js()
    filtered = _const_src(src, "SECTION_FILTERED_RE")
    for эмодзи, имя in (("1F3E6", "🏦 ИСКИ БАНКА"), ("1F4D1", "📑 Касс. события")):
        assert эмодзи in filtered.upper(), (
            f"{имя} выпала из SECTION_FILTERED_RE — секция снова будет "
            "показываться в «★ Мои» целиком, мимо watchlist."
        )
    header = _const_src(src, "SECTION_HEADER_RE")
    for эмодзи, зачем in (
        ("1F3E6", "🏦 — заголовок секции банка"),
        ("1F4D1", "📑 — заголовок касс. событий"),
        ("1F4CC", "📌 — футер обязан закрывать секцию банка, иначе выпадет"),
    ):
        assert эмодзи in header.upper(), f"{зачем}: пропал из SECTION_HEADER_RE."


def test_cassation_grouping_header_recognized():
    """«⚖️🔬 КАССАЦИЯ»: альтернатива обязана стоять ПЕРЕД одиночным ⚖."""
    src = _app_js()
    for имя in ("SECTION_HEADER_RE", "SECTION_GROUPING_RE"):
        rx = _const_src(src, имя).upper()
        assert "1F52C" in rx, (
            f"{имя} не знает «⚖️🔬 КАССАЦИЯ»: после ⚖ идёт 🔬, а не <b>, "
            "и класс символов такую строку не матчит."
        )
        assert rx.index("1F52C") < rx.rindex("2696"), (
            f"{имя}: альтернатива ⚖️🔬 обязана идти ПЕРВОЙ — иначе одиночный "
            "⚖ откусит начало строки (тот же порядок в postprocess.py)."
        )


def test_new_cassation_cases_stay_systemwide():
    """«📥 Новые касс. дела» (discovery) не фильтруются — как в delivery.py."""
    rx = _const_src(_app_js(), "SECTION_NEW_RE")
    assert "касс" in rx, (
        "SECTION_NEW_RE не считает «Новые касс. дела» общесистемной секцией, "
        "хотя cass_discovered в delivery.py не фильтруется по watchlist."
    )


# ===== 2-3. Поведение фильтра =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_bank_section_filtered_without_stars():
    """Без звёзд в секции банка остаются только новые иски банка."""
    lines = _run_filter([])
    nums = _numbers(lines)
    assert nums == ["33-5715/2026", "М-7220/2026"], (
        f"Осталось {nums}: ожидались только общесистемные — новое дело "
        "апелляции и новый иск банка (fi_bank_claim_registered)."
    )
    assert any(s.startswith("🏦") for s in lines), "Заголовок секции банка пропал."


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_bank_star_matches_by_composite_domain():
    """Звезда «домен|номер» оставляет своё дело — и только его."""
    nums = _numbers(_run_filter(["vartovgor--hmao.sudrf.ru|2-6736/2026"]))
    assert "2-6736/2026" in nums, (
        "Звёздное дело банка выпало: composite-матчинг по домену из href не "
        "работает (звёзды bank-дел в другой форме не хранятся)."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_same_number_in_two_courts_not_confused():
    """Номера банк-дел не уникальны между судами — домен обязан разводить."""
    src = _app_js()
    script = _harness(src) + """
const mine = new Set(['vartovgor--hmao.sudrf.ru|2-6736/2026']);
const свой = {bare: '2-6736/2026', dom: 'vartovgor--hmao.sudrf.ru'};
const чужой = {bare: '2-6736/2026', dom: 'surggor--hmao.sudrf.ru'};
console.log(JSON.stringify([mineRefMatches(свой, mine), mineRefMatches(чужой, mine)]));
"""
    assert json.loads(_node(script)) == [True, False], (
        "mineRefMatches путает дела с одинаковым номером в разных судах — "
        "именно ради этого звезда банка хранится composite-формой."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_section_counter_matches_kept_rows():
    """Счётчик заголовка пересчитан по факту: «(4)» при четырёх строках."""
    lines = _run_filter(["vartovgor--hmao.sudrf.ru|2-6736/2026"])
    заголовок = next(s for s in lines if s.startswith("🏦"))
    assert "(2)" in заголовок, (
        f"Заголовок «{заголовок}» несёт счётчик общего дайджеста — в "
        "mine-версии юрист видит «(4)» над двумя строками и решает, что "
        "фильтр потерял его дела."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_orphan_group_headers_dropped_footer_kept():
    """Пустые группы убираются, футер «📌 В производстве…» остаётся."""
    lines = _run_filter([])
    assert not any(s.startswith("🏛") for s in lines), (
        "«🏛 ПЕРВАЯ ИНСТАНЦИЯ» осталась сиротой — все её дела выкинуты."
    )
    assert not any(s.startswith("⚖️🔬") for s in lines), (
        "«⚖️🔬 КАССАЦИЯ» осталась сиротой: секция «🏦 ИСКИ БАНКА» — раздел "
        "верхнего уровня, содержимым кассации она не является."
    )
    assert any(s.startswith("📌") for s in lines), (
        "Футер выпал: он попал в фильтруемую секцию банка как «параграф без "
        "номера дела» — 📌 обязан закрывать секцию (SECTION_HEADER_RE)."
    )
    assert not any(s.strip() == "⸻" for s in lines), "Разделитель-сирота остался."


# ===== 4. Счётчик found в buildMineHtml =====

def test_found_counted_by_same_predicate():
    """buildMineHtml считает дела тем же mineRefMatches, что и фильтр."""
    src = _app_js()
    fn = _fn_src(src, "buildMineHtml")
    assert "caseRefsInFragment(filtered).filter((ref) => mineRefMatches(ref, mineSet))" in fn, (
        "Счётчик found разошёлся с фильтром: по голым номерам совпадение "
        "только в секции банка даёт 0 и ложный фолбэк «показан общий "
        "дайджест» при живых делах юриста."
    )
    assert "casesInFragment" not in src, (
        "Вернулась casesInFragment (без домена) — у исков банка она теряет "
        "composite-совпадение; единственный источник — caseRefsInFragment."
    )


def test_bank_new_claims_pass_as_new_cases():
    """fi_bank_claim_registered — «дело поступило в суд», идёт без звезды."""
    fn = _fn_src(_app_js(), "collectNewCaseNumbers")
    assert "fi_bank_claim_registered" in fn, (
        "Новые иски банка снова требуют звезду: авто-подхват заводит их сам, "
        "звезды на них по определению ещё нет."
    )
    assert "court_domain" in fn, (
        "Новый иск банка кладётся голым номером — в mine-набор он обязан "
        "попасть composite-ключом «домен|номер» (номера не уникальны)."
    )


def test_push_filter_untouched():
    """Push-фильтр новые иски банка по-прежнему шлёт только по watchlist."""
    path = os.path.join(ROOT, "scripts", "court_monitor", "delivery.py")
    with open(path, encoding="utf-8") as f:
        src = f.read()
    assert "fi_bank_claim_registered" not in src, (
        "В delivery.py появилось исключение для новых исков банка. Решение "
        "юриста 13.08.2026: послабление сделано ТОЛЬКО на дашборде — на "
        "Урале авто-подхват заводит десятки исков за прогон, в push это шум."
    )


# ===== 5. Свёрнутая строка «заведено N новых исков банка» =====
# Разгон Урала 14.08.2026: при >25 заведениях за прогон рендер печатает одну
# строку без номера дела (config.BANK_INTAKE_DIGEST_FOLD). Для mine-версии
# это «параграф без номера» внутри фильтруемой секции — он обязан выпадать
# (звёзд у только что заведённых дел нет), не ломая ни машину состояний, ни
# пересчёт счётчика заголовка.

FOLDED_LINE = "🆕 <b>заведено 116 новых исков банка</b> в 12 судах — список на дашборде"

DIGEST_FOLDED = DIGEST.replace(
    '<a href="https://surggor--hmao.sudrf.ru/x?name_op=case"><b>М-7220/2026</b></a> — 🆕 иск банка',
    FOLDED_LINE,
)


def _run_filter_html(html: str, stars: list[str], ctx: dict) -> list[str]:
    src = _app_js()
    script = _harness(src) + """
const DIGEST = %s, CTX = %s, STARS = %s;
const mine = new Set(STARS.map(canonCaseNumber));
for (const n of collectNewCaseNumbers(CTX)) mine.add(canonCaseNumber(n));
const out = filterGeneralHtmlByMine(DIGEST, mine);
console.log(JSON.stringify(out.split(/\\n{2,}/).filter(Boolean)
  .map((p) => p.split('\\n')[0])));
""" % (json.dumps(html), json.dumps(ctx), json.dumps(stars))
    return json.loads(_node(script))


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_folded_line_dropped_in_mine():
    """Свёрнутая строка выпадает, звёздное дело банка остаётся."""
    ctx = {**CTX, "fi_changes": [CTX["fi_changes"][1]]}
    lines = _run_filter_html(
        DIGEST_FOLDED, ["vartovgor--hmao.sudrf.ru|2-6736/2026"], ctx)
    assert not any("заведено 116" in s for s in lines), (
        "Свёрнутая строка осталась в mine-версии — у только что заведённых "
        "дел звёзд нет, юрист видит чужие дела в «★ Мои»."
    )
    assert any("2-6736/2026" in s for s in lines), (
        "Звёздное дело банка выпало вместе со свёрнутой строкой."
    )


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_folded_line_does_not_break_section_machine():
    """Строка не заголовок: футер и соседние секции не должны пострадать."""
    lines = _run_filter_html(
        DIGEST_FOLDED, ["vartovgor--hmao.sudrf.ru|2-6736/2026"], CTX)
    assert any(s.startswith("📌") for s in lines), (
        "Футер выпал — значит свёрнутая строка сбила состояние машины "
        "секций (тот же класс бага, что 12.08.2026 с 🏦 в _DIGEST_HEADER_RE)."
    )
    заголовок = next((s for s in lines if s.startswith("🏦")), "")
    assert "(1)" in заголовок, (
        f"Счётчик заголовка «{заголовок}» не пересчитан по факту строк."
    )
