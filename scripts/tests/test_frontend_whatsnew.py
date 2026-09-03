# -*- coding: utf-8 -*-
"""Стражи анонса «Что нового» (26.08.2026; текущий выпуск — 03.09.2026).

Одноразовое окно при первом открытии после обновления. Выпуск 03.09.2026
анонсирует шторку «Настройки» за ⚙ (туда переехали уведомления, синк 🔗 и
календарь; кнопка ведёт в settings-sheet) и «Скачать файл (.ics)» для
корпоративного OWA. Маркер показа — id анонса в lsKey('whatsnew_seen'):
следующий анонс = новый WHATSNEW_ID, механизм переиспользуется; повторный
показ — кнопка в разделе «О приложении» (showWhatsNewAgain).
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
    # Кнопка действия ведёт в шторку настроек, а не просто закрывает.
    assert "whatsNewOpenSettings()" in html
    for gone in ("whatsNewShowFreshness()", "whatsNewOpenSync()"):
        assert gone not in html, f"{gone}: прошлый анонс (26.08) снят вместе с кодом."
    # Дата обновления в анонсе (решение юриста 26.08.2026).
    assert re.search(r'class="wn-date">Обновление от \d{2}\.\d{2}\.\d{4}<', html)
    # Пункт про настройки ПОКАЗЫВАЕТ кнопку шапки: мини-копия иконки в тексте,
    # чтобы пользователь знал, ЧТО искать в шапке. Слово «кнопка» и иконка
    # обёрнуты в nowrap — перенос разлучал их (разрыв строки допустим вокруг
    # атомарного inline-flex даже через &nbsp;).
    assert 'class="wn-inline-btn"' in html
    assert re.search(r'появилась <span class="wn-nowrap">кнопка&nbsp;', html)
    assert "Из файла" in html, "Второй пункт — путь через файл для OWA."


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


def test_open_settings_and_show_again():
    js = _read("app.js")
    body = _fn_src(js, "whatsNewOpenSettings")
    assert "closeWhatsNew()" in body and "openSettingsSheet()" in body
    again = _fn_src(js, "showWhatsNewAgain")
    assert "localStorage.removeItem(WHATSNEW_KEY)" in again
    assert "maybeShowWhatsNew()" in again
    assert "showWhatsNewAgain()" in _fn_src(js, "settingsAboutSectionHtml")


def test_desktop_popup_shared_with_sync_sheet():
    # Все три окна используют одно десктопное переопределение —
    # расхождение стилей одинаковых окон было бы молчаливым.
    css = _read("styles.css")
    m = re.search(r"#sync-sheet,\s*#whatsnew-sheet,\s*#settings-sheet \{", css)
    assert m, "Десктопное мини-окно должно быть общим для sync, whatsnew и settings."


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
