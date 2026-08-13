"""
Стражи крестиков «закрыть» и сокращения городов Урала (v152).

Контекст 13.08.2026. Разбор интерфейса юристом дал две претензии.

1. «В некоторых местах крестик не ровно по середине». Разбор: из пяти
   крестиков дашборда честно центрирован был только один — в маячке
   дайджеста, он нарисован svg. Остальные несли текстовый «×» (U+00D7,
   знак умножения). Глиф сидит на математической оси шрифта, то есть
   всегда выше оптического центра строки, и flex его не спасает: flex
   центрирует строчный бокс, а не чернила глифа. Сверх того у баннера
   новых дел и шторки фильтров центрирования не было вовсе (padding +
   line-height), а глобального `button { font-family: inherit }` в
   проекте нет — крестик рисовался СИСТЕМНЫМ шрифтом, и сдвиг был
   разным на разных ОС. В админке та же болезнь и символ другой («✕»
   U+2715), которого в IBM Plex Sans нет вовсе.
   Лечение: везде svg + общий контракт .btn-x.

2. Имена судов Урала с городом («Ленинский районный суд
   г. Екатеринбурга») на мобильной карточке переносились в 2-3 строки,
   в колонке «Суд» (130px) уходили в многоточие. Лечение: правила
   городов в shortCourt — ЕКБ, Н. Тагила, Каменска-Ур.
   Питоновский shorten_court_name (дайджест в Telegram) юрист велел НЕ
   трогать — там правила свои и другие.

Что охраняем:
1. shortCourt сокращает три города Урала (и не задевает ХМАО, «Городской
   суд г. Лесного» и «Свердловский областной суд»).
2. Ни одна кнопка закрытия не содержит текстового крестика — только svg.
3. Контракт .btn-x жив (flex-центрирование + display:block у svg) и стоит
   ДО правил с display:none, иначе .search-clear станет всегда видимой.
4. .case-row .btn-icon { align-self:center } в админке — без него кнопка
   30×30 уезжает по базовой линии строки.
5. Bust кэша согласован: ?v=N у styles.css и app.js == CACHE_VERSION.

JS-инструментария в проекте нет: чистые функции исполняются в node,
проводка проверяется grep'ом по исходнику — приём test_frontend_bridges.py.

Запуск: python3 -m pytest scripts/tests/test_frontend_icons.py
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


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# ===== 1. Сокращение городов Урала =====

# Длинная форма («… Свердловской области») приезжает с карточек апелляции
# и КСОЮ — в реестре региона её нет, но на фронт она попадает.
SHORT_COURT_CASES = [
    ("Ленинский районный суд г. Екатеринбурга", "Ленинский р-ный суд ЕКБ"),
    ("Академический районный суд г. Екатеринбурга", "Академический р-ный суд ЕКБ"),
    (
        "Октябрьский районный суд г. Екатеринбурга Свердловской области",
        "Октябрьский р-ный суд ЕКБ",
    ),
    ("Верх-Исетский районный суд г. Екатеринбурга", "Верх-Исетский р-ный суд ЕКБ"),
    ("Дзержинский районный суд г. Нижний Тагил", "Дзержинский р-ный суд Н. Тагила"),
    (
        "Тагилстроевский районный суд г. Нижний Тагил Свердловской области",
        "Тагилстроевский р-ный суд Н. Тагила",
    ),
    (
        "Синарский районный суд г. Каменск-Уральского",
        "Синарский р-ный суд Каменска-Ур.",
    ),
    # Негативные: города в правилах нет — имя не трогаем.
    ("Сургутский городской суд", "Сургутский гор. суд"),
    ("Нижневартовский районный суд", "Нижневартовский р-ный суд"),
    ("Городской суд г. Лесного", "Городской суд г. Лесного"),
    # Правило города не должно задеть апелляции территорий.
    ("Свердловский областной суд", "Свердловский обл. суд"),
    ("Суд Ямало-Ненецкого автономного округа", "Суд ЯНАО"),
    (
        "Суд Ханты-Мансийского автономного округа - Югры",
        "Суд ХМАО-Югры",
    ),
]


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_short_court_cities():
    src = _fn_src(_read("app.js"), "shortCourt")
    payload = json.dumps([c[0] for c in SHORT_COURT_CASES], ensure_ascii=False)
    out = _node(
        src
        + "\nconsole.log(JSON.stringify("
        + payload
        + ".map(shortCourt)));"
    )
    got = json.loads(out)
    for (src_name, expected), actual in zip(SHORT_COURT_CASES, got):
        assert actual == expected, f"shortCourt({src_name!r}) → {actual!r}, ждали {expected!r}"


def test_shorten_court_name_python_untouched():
    """Дайджест в Telegram юрист велел не трогать — правил городов там нет."""
    src = _read("scripts/court_monitor/textutil.py")
    fn = src[src.index("def shorten_court_name"):]
    fn = fn[: fn.index("\n\n\n")]
    assert "Екатеринбург" not in fn, (
        "Правило города просочилось в shorten_court_name: решением юриста "
        "13.08.2026 сокращение городов — только на фронте, дайджест как есть."
    )


# ===== 2. Ни одного текстового крестика в кнопках =====

# Символы, которых в кнопке закрытия быть не должно: × U+00D7, ✕ U+2715,
# ✖ U+2716, ✗ U+2717 и HTML-мнемоника.
BAD_GLYPHS = ("&times;", "×", "✕", "✖", "✗")

# Кнопки закрытия/удаления по опорному куску их разметки.
CLOSE_BUTTON_MARKERS = [
    ("sberbank_dashboard.html", 'class="dismiss btn-x"'),
    ("sberbank_dashboard.html", 'class="search-clear btn-x"'),
    ("sberbank_dashboard.html", 'class="sheet-close btn-x"'),
    ("sberbank_dashboard.html", 'class="digest-close btn-x"'),
    ("app.js", 'class="drawer-close btn-x"'),
    ("cloudflare-worker/admin_page.js", 'class="flash-x"'),
    ("cloudflare-worker/admin_page.js", 'data-action="wldel"'),
    ("cloudflare-worker/admin_page.js", "data-extra-del="),
    ("cloudflare-worker/admin_page.js", 'class="imp-file-clear"'),
]


@pytest.mark.parametrize("path,marker", CLOSE_BUTTON_MARKERS)
def test_close_button_uses_svg(path, marker):
    """Крестик рисуется svg — ни текстового глифа, ни мнемоники."""
    src = _read(path)
    idx = src.find(marker)
    assert idx >= 0, f"Кнопка «{marker}» пропала из {path} — проверь, не переименована ли."
    # Хвост от маркера до конца открывающего/закрывающего тега кнопки:
    # содержимое лежит между ним и </button>.
    tail = src[idx : src.find("</button>", idx)]
    assert "<svg" in tail or "ICON_X" in tail, (
        f"Кнопка «{marker}» в {path} снова рисует крестик текстом. "
        "Текстовый × сидит на математической оси шрифта — он всегда выше "
        "оптического центра, и flex этого не исправляет (см. .btn-x)."
    )
    for glyph in BAD_GLYPHS:
        assert glyph not in tail, (
            f"В кнопке «{marker}» ({path}) снова текстовый крестик {glyph!r}."
        )


def test_icon_x_constant_is_single_source():
    """Разметка крестика — одна константа на файл, а не копии по шаблонам."""
    app = _read("app.js")
    assert "const ICON_X=" in app, "Константа ICON_X пропала из app.js."
    admin = _read("cloudflare-worker/admin_page.js")
    assert "const ICON_X = " in admin, "Константа ICON_X пропала из admin_page.js."
    # Админка целиком — один template literal: backtick и ${ внутри неё
    # оборвали бы строку (см. предупреждение в CLAUDE.md).
    icon_line = next(l for l in admin.splitlines() if l.startswith("const ICON_X = "))
    assert "`" not in icon_line and "${" not in icon_line, (
        "ICON_X админки содержит backtick или ${ — страница собирается одним "
        "template literal, такая вставка её порвёт."
    )


def test_status_icons_untouched():
    """✕ статус-иконок и знаков исходов — не крестик закрытия, не трогать."""
    admin = _read("cloudflare-worker/admin_page.js")
    assert '<span class="warn-mark">✕</span>' in admin, (
        "Знак сбоя в плитке здоровья пропал — это статус-иконка, а не кнопка."
    )
    app = _read("app.js")
    assert "reversed:'✕'" in app, (
        "Знак исхода «отменено» пропал из RESULT_ICONS — это бейдж, не кнопка."
    )


# ===== 3. Контракт .btn-x =====

def test_btn_x_contract():
    css = _read("styles.css")
    m = re.search(r"^\.btn-x \{([^}]*)\}", css, re.M)
    assert m, "Правило .btn-x пропало из styles.css."
    body = m.group(1)
    for prop in ("display:inline-flex", "align-items:center", "justify-content:center"):
        assert prop in body, f".btn-x потерял {prop} — крестик перестанет центрироваться."
    m_svg = re.search(r"^\.btn-x svg \{([^}]*)\}", css, re.M)
    assert m_svg and "display:block" in m_svg.group(1), (
        ".btn-x svg потерял display:block: инлайновый svg тянет строчный бокс "
        "с подстрочным просветом и ломает центровку заново."
    )


def test_btn_x_precedes_display_none_rules():
    """.btn-x обязан стоять ВЫШЕ правил с display:none.

    У .search-clear и .digest-close свой display:none (показываются по
    классу). При равной специфичности выигрывает тот, кто ниже в файле —
    стой .btn-x после них, обе кнопки были бы видны всегда.
    """
    css = _read("styles.css")
    pos_btn_x = css.index(".btn-x {")
    for later in (".search-clear {", ".digest-close {"):
        assert pos_btn_x < css.index(later), (
            f"Правило .btn-x оказалось ниже {later} — его display:inline-flex "
            "перебьёт display:none, и кнопка станет видимой всегда."
        )


def test_dashboard_close_buttons_carry_btn_x():
    html = _read("sberbank_dashboard.html")
    for cls in ("dismiss", "search-clear", "sheet-close", "digest-close"):
        assert f'class="{cls} btn-x"' in html, (
            f"Кнопка .{cls} потеряла класс btn-x — центрирование отвалится."
        )
    assert 'class="drawer-close btn-x"' in _read("app.js")


def test_admin_case_row_button_centered():
    admin = _read("cloudflare-worker/admin_page.js")
    m = re.search(r"\.case-row \.btn-icon \{([^}]*)\}", admin)
    assert m and "align-self:center" in m.group(1), (
        "Пропало .case-row .btn-icon { align-self:center }: строка выровнена "
        "по базовой линии (нужно тексту и бейджам), и кнопка 30×30 уезжает "
        "вниз целиком, мимо номера дела."
    )


# ===== 4. Bust кэша =====

def test_cache_bust_in_sync():
    html = _read("sberbank_dashboard.html")
    css_v = re.search(r"styles\.css\?v=(\d+)", html)
    app_v = re.search(r"app\.js\?v=(\d+)", html)
    sw_v = re.search(r"CACHE_VERSION = 'v(\d+)'", _read("service-worker.js"))
    assert css_v and app_v and sw_v, "Не нашёл номера версий — проверь разметку."
    assert css_v.group(1) == app_v.group(1) == sw_v.group(1), (
        f"Версии разошлись: styles.css?v={css_v.group(1)}, "
        f"app.js?v={app_v.group(1)}, CACHE_VERSION=v{sw_v.group(1)}. "
        "PWA юриста покажет старую версию из cache-first."
    )
