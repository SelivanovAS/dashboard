# -*- coding: utf-8 -*-
"""Стражи анонса «Что нового» (26.08.2026, к раскатке синка подписок).

Одноразовое окно при первом открытии после обновления: анонсирует дату
сверки с судом (существующая плашка drawer-freshness — с живой демонстрацией
подсветкой) и синхронизацию подписок (кнопка ведёт в sync-sheet). Маркер
показа — id анонса в lsKey('whatsnew_seen'): следующий анонс = новый
WHATSNEW_ID, механизм переиспользуется.
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def test_markup():
    html = _read("sberbank_dashboard.html")
    for marker in ('id="whatsnew-sheet"', 'id="whatsnew-scrim"', "Что нового"):
        assert marker in html, marker
    # Обе кнопки действий ведут в демонстрации, а не просто закрывают.
    assert "whatsNewShowFreshness()" in html
    assert "whatsNewOpenSync()" in html
    # Дата обновления в анонсе (решение юриста 26.08.2026).
    assert re.search(r'class="wn-date">Обновление от \d{2}\.\d{2}\.\d{4}<', html)
    # Пункт про синк ПОКАЗЫВАЕТ кнопку шапки: мини-копия иконки в тексте,
    # чтобы пользователь знал, ЧТО искать в шапке. Слово «кнопка» и иконка
    # обёрнуты в nowrap — перенос разлучал их (разрыв строки допустим вокруг
    # атомарного inline-flex даже через &nbsp;).
    assert 'class="wn-inline-btn"' in html
    assert re.search(r'появилась <span class="wn-nowrap">кнопка&nbsp;', html)


def test_seen_marker_via_lskey():
    js = _read("app.js")
    assert "lsKey('whatsnew_seen')" in js, (
        "Маркер показа обязан идти через lsKey — фронты территорий живут "
        "на одном origin github.io."
    )
    # id анонса — константа: следующий анонс бампает её, не механизм.
    assert re.search(r"const WHATSNEW_ID = '[^']+'", js)
    # Закрытие ЛЮБЫМ путём помечает показ — иначе анонс лез бы каждый раз.
    assert "localStorage.setItem(WHATSNEW_KEY, WHATSNEW_ID)" in _fn_src(js, "closeWhatsNew")


def test_demo_spotlights_existing_freshness_row():
    js = _read("app.js")
    demo = _fn_src(js, "whatsNewShowFreshness")
    assert "openDrawer(" in demo
    assert "drawer-freshness" in demo, (
        "Демонстрация подсвечивает СУЩЕСТВУЮЩУЮ плашку «Проверено на сайте "
        "суда» (drawerFreshnessHtml), а не собственную копию."
    )
    assert "spotlight" in demo
    css = _read("styles.css")
    assert ".drawer-freshness.spotlight" in css
    assert "wn-spotlight" in css


def test_desktop_popup_shared_with_sync_sheet():
    # Оба одноразовых окна используют одно десктопное переопределение —
    # расхождение стилей двух одинаковых окон было бы молчаливым.
    css = _read("styles.css")
    m = re.search(r"#sync-sheet,\s*#whatsnew-sheet \{", css)
    assert m, "Десктопное мини-окно должно быть общим для sync и whatsnew."


def test_whatsnew_blocks_background_refresh():
    js = _read("app.js")
    assert "whatsnew-sheet" in _fn_src(js, "uiBusyForRefresh")


def test_whatsnew_yields_to_pair_deeplink():
    # Переход по QR (?pair=) занят связыванием — анонс не должен спорить
    # с открытой шторкой синка; он дождётся следующего открытия.
    js = _read("app.js")
    m = re.search(r"maybeShowWhatsNew\(\)", js)
    assert m
    tail = js[max(0, m.start() - 400):m.start()]
    assert "sync-sheet" in tail and "open" in tail
