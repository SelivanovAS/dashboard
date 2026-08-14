"""
Стражи пульта админки: кламп значения плитки и операторские ссылки.

Контекст 13.08.2026. Разбор админки юристом дал две претензии.

1. «Карточка раздулась». Плитка «Дайджест» заняла девять строк и растянула
   весь ряд пульта (grid тянется по самой высокой плитке). Причина не в
   вёрстке как таковой: `digestSummaryParts` разбирает сводку по ИМЕНОВАННЫМ
   частям («Новых:/Изменений:/Переходов:»), которые пишет боевой крон, а
   replay (`test_digest.yml` с публикацией результатов) кладёт в summary
   полную сводку дайджеста — там таких слов нет вовсе. Разбор возвращал
   пусто, фолбэк печатал строку целиком, а ограничения высоты у .stat-value
   не было. Лечение: текстовый фолбэк — отдельный .tile-text с клампом в две
   строки (полная строка в title) + жёсткий max-height у самого .stat-value,
   чтобы ряд не растянуло НИКАКОЕ будущее значение.

2. «Убрать ссылки на этих карточках в операторской». У оператора плитка
   «Последний прогон» вела в лог GitHub Actions, куда его не пустят.
   Внутренние переходы по вкладкам («Парсеры», «Импорты») юрист велел
   оставить — это навигация по самой админке.

Что охраняем:
1. Текстовый фолбэк плитки клампится (.tile-text с -webkit-line-clamp) и
   у .stat-value есть max-height.
2. Плитка «Дайджест» рисует фолбэк через .tile-text, а не голым escHtml.
3. Рука и ховер-тень — только у кликабельных плиток ([data-href]/[data-goto]),
   иначе неинтерактивная плитка оператора притворяется ссылкой.
4. data-href плитки прогона гейтится ролью, а стрелка ↗ в подписи — IS_OWNER.

JS-инструментария в проекте нет: проводка проверяется grep'ом по исходнику —
приём test_frontend_bridges.py / test_frontend_icons.py.

Запуск: python3 -m pytest scripts/tests/test_admin_pult.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))
ADMIN = os.path.join(ROOT, "cloudflare-worker", "admin_page.js")


def _admin() -> str:
    with open(ADMIN, encoding="utf-8") as f:
        return f.read()


# ===== 1. Кламп значения плитки =====

def test_stat_value_has_height_ceiling():
    m = re.search(r"\.stat-value \{([^}]*)\}", _admin())
    assert m, "Не нашёл правило .stat-value — проверь стили пульта."
    body = m.group(1)
    assert "max-height" in body and "overflow:hidden" in body, (
        "У .stat-value пропал потолок высоты. Длинная сводка (replay пишет её "
        "на 9 частей) снова растянет ВЕСЬ ряд пульта по самой высокой плитке."
    )


def test_tile_text_is_clamped():
    m = re.search(r"\.stat-value \.tile-text \{([^}]*)\}", _admin())
    assert m, (
        "Пропал класс .tile-text — текстовый фолбэк плитки печатается без "
        "ограничения строк."
    )
    body = m.group(1)
    assert "-webkit-line-clamp" in body, ".tile-text больше не клампится по строкам."
    assert "min-width:0" in body, (
        "Без min-width:0 flex-элемент .tile-text не сжимается и кламп "
        "не спасает от распирания плитки."
    )


def test_digest_tile_fallback_uses_tile_text():
    src = _admin()
    m = re.search(r"function renderDigestTile\(.*?\n\}", src, re.S)
    assert m, "Не нашёл renderDigestTile."
    body = m.group(0)
    assert 'class="tile-text"' in body, (
        "Фолбэк плитки «Дайджест» снова печатает сводку голым текстом — "
        "именно так карточка раздулась 13.08.2026."
    )
    assert "title=" in body, (
        "Полная сводка должна остаться в title фолбэка: в плитке её режет кламп."
    )


# ===== 2. Ссылки у оператора =====

def test_pointer_only_on_clickable_tiles():
    src = _admin()
    m = re.search(r"\.stat-card \{([^}]*)\}", src)
    assert m, "Не нашёл правило .stat-card."
    assert "cursor:pointer" not in m.group(1), (
        "cursor:pointer вернулся на ВСЕ .stat-card — неинтерактивная плитка "
        "оператора («Последний прогон») снова притворяется ссылкой."
    )
    assert ".stat-card[data-goto], .stat-card[data-href] { cursor:pointer; }" in src, (
        "Пропало правило руки для кликабельных плиток."
    )
    assert re.search(
        r"\.stat-card\[data-goto\]:hover, \.stat-card\[data-href\]:hover", src
    ), "Ховер-тень должна быть только у кликабельных плиток."


def test_run_tile_href_gated_by_role():
    src = _admin()
    m = re.search(r'<button class="stat-card" data-accent="gray"\$\{isOperator[^\n]*', src)
    assert m, (
        "Плитка «Последний прогон» потеряла гейт по роли: у оператора снова "
        "появится data-href на лог GitHub Actions."
    )
    line = m.group(0)
    assert "disabled" in line and 'data-href="run"' in line, (
        "Ожидаю ветку: оператору — disabled, владельцу — data-href=\"run\"."
    )


def test_run_sub_arrow_gated_by_owner():
    m = re.search(r"function ghRunSub\(.*?\n\}", _admin(), re.S)
    assert m, "Не нашёл ghRunSub."
    assert "IS_OWNER" in m.group(0), (
        "Стрелка ↗ в подписи плитки прогона снова безусловна — она обещает "
        "оператору переход, которого нет."
    )


# ── Постоянные судебные присутствия в форме импорта (14.08.2026) ─────────────
# Скан площадок нашёл на Урале два реальных присутствия: Пышма у Камышловского
# и Ачит у Красноуфимского (обе площадки в реестре с 16.07.2026). В админку они
# не попадали: список судов дедуплицировался ПО ДОМЕНУ, и вторая площадка
# выпадала из выпадающего списка и светофора — её дела не импортировал никто.

def test_import_courts_not_deduped_by_domain():
    """Дедуп по домену снова съел бы присутствия."""
    src = _admin()
    m = re.search(r"impCourts = gated[^\n;]*", src)
    assert m, "Не нашёл сборку impCourts."
    assert "filter" not in m.group(0), (
        "impCourts снова фильтруется при сборке — если это дедуп по домену, "
        "постоянные судебные присутствия (Пышма, Ачит) исчезнут из формы "
        "импорта и светофора."
    )


def test_court_select_value_is_domain_plus_srv():
    """Значение строки — «домен|srv»: голый домен не различает площадки."""
    src = _admin()
    assert re.search(r"function impCourtKey\(c\)[^\n]*domain \+ \"\|\"", src), (
        "impCourtKey должен собирать ключ «домен|srv_num»."
    )
    m = re.search(r'sel\.innerHTML = impCourts\.map\([\s\S]{0,200}?\)\.join\(""\);', src)
    assert m and "impCourtKey(c)" in m.group(0), (
        "У <option> значением снова стоит голый домен — площадки склеятся."
    )


def test_dump_post_sends_bare_domain():
    """На сервер уходит домен: Worker и его белый список судов — по домену,
    а фактическую площадку дела импортёр берёт из href карточек дампа."""
    src = _admin()
    m = re.search(r"async function impSend\(\)[\s\S]{0,400}", src)
    assert m and 'impDomainOf(document.getElementById("imp-court").value)' in m.group(0), (
        "impSend отправляет значение селекта как есть — на сервер уедет "
        "«домен|srv», которого нет в белом списке Worker'а."
    )


def test_detect_compares_by_domain():
    """Автоопределение суда по вставке сравнивает ДОМЕНЫ: у площадок одного
    суда хост общий, и переключать выбранное присутствие на первую площадку
    из-за совпадения хоста нельзя."""
    src = _admin()
    for anchor in ('!== impDetectedHosts[0]', '=== h) {'):
        idx = src.find(anchor)
        assert idx > 0, anchor
        assert "impDomainOf" in src[idx - 120:idx], (
            f"Сравнение у «{anchor}» идёт без impDomainOf — присутствие будет "
            "молча перевыбираться на первую площадку домена."
        )
