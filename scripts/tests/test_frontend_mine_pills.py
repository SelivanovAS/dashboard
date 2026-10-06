"""
Стражи пилюли «★ Мои» в шапках дайджеста и «Ближайших заседаний» (17.08.2026).

Выбран раздел «★ Мои» — а обе верхние карточки об этом молчали: «Ближайшие
заседания» подписывались разделом только в «Исках банка» («· иски банка»), а
шапка дайджеста всегда была «Дайджест [дата]», хотя тело уже отфильтровано по
звёздам. Признак в дайджесте когда-то был (.digest-mine-pill, коммит 9159346)
и умер, когда рядом появился тоггл «★ Мой»; тоггл прячется при пустом
watchlist, и подписи не осталось вовсе.

Что охраняем:
1. Пилюля в ОБЕИХ шапках и одним классом .mine-scope-pill — второй класс
   разъедется с CSS молча.
2. Проводка заголовка дайджеста: renderDigestTitle зовётся и из
   loadLastDigest, и из setDigestView. Только загрузка — пилюля появлялась бы
   лишь после перезагрузки страницы: раздел переключается позже.
3. Пилюля идёт по РЕЖИМУ, а не по числу найденных дел: при пустом watchlist
   дайджест честно откатывается на общий с плашкой, но раздел выбран, и шапка
   обязана его называть (у юриста сейчас ноль звёзд — гейт по находкам
   выглядел бы поломкой).
4. Значок — inline-SVG из scopeMineIcon() на currentColor, без «★» и эмодзи:
   правило проекта (эмодзи в бейдже рисуется системным цветным шрифтом мимо
   палитры и в тёмной теме выпадает пятном).
5. Цвет пилюли — токенами, объявленными в ОБЕИХ темах; литеральный фолбэк
   var(--x,#hex) — класс дефектов, чиненный 14.08.2026.
6. Мёртвого .digest-mine-pill не осталось ни в CSS, ни в JS.
7. Bust кэша: ?v= у app.js/styles.css равны CACHE_VERSION service-worker.js
   (забывается чаще всего — инцидент 0b70826).

JS-инструментария в проекте нет: проводка проверяется grep'ом по исходнику —
тем же приёмом, что test_frontend_bridges.py.

Запуск: python3 -m pytest scripts/tests/test_frontend_mine_pills.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))

PILL_CLASS = "mine-scope-pill"


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


class TestMineScopePill:
    def test_pill_helper_uses_scope_icon(self):
        """Один хелпер на обе карточки, значок — SVG чипа раздела."""
        src = _fn_src(_read("app.js"), "mineScopePillHtml")
        assert PILL_CLASS in src, "пилюля рисуется чужим классом"
        assert "scopeMineIcon()" in src, "значок не переиспользует SVG чипа"
        assert "★" not in src and "⭐" not in src, (
            "звезда в пилюле обязана быть inline-SVG на currentColor")

    def test_upcoming_title_has_pill_in_mine(self):
        """«Ближайшие заседания» подписываются разделом, и только в «Моих»."""
        src = _fn_src(_read("app.js"), "renderAnalytics")
        m = re.search(r"const upMinePill\s*=\s*(.+?);", src)
        assert m, "в шапке «Ближайших заседаний» нет пилюли раздела"
        assert "mineMode" in m.group(1) and "mineScopePillHtml()" in m.group(1)
        assert "${upMinePill}" in src, "пилюля собрана, но не вставлена в шапку"

    def test_digest_title_pill_follows_mode(self):
        """Заголовок дайджеста несёт пилюлю по режиму и сохраняет бейдж даты."""
        src = _fn_src(_read("app.js"), "renderDigestTitle")
        assert "mineScopePillHtml()" in src, "в шапке дайджеста нет пилюли"
        assert "_digestViewMode === 'mine'" in src, (
            "пилюля обязана следовать режиму, а не числу найденных дел")
        assert "digest-date-pill" in src, "потерян зелёный бейдж даты"

    def test_digest_title_rendered_on_mode_switch(self):
        """Проводка: без вызова из setDigestView пилюля появлялась бы только
        после перезагрузки страницы — раздел переключается позже загрузки."""
        app = _read("app.js")
        assert "renderDigestTitle();" in _fn_src(app, "setDigestView")
        assert "renderDigestTitle();" in _fn_src(app, "loadLastDigest")

    def test_pill_style_declared_with_tokens(self):
        css = _read("styles.css")
        m = re.search(r"\." + PILL_CLASS + r"\s*\{[^}]*\}", css)
        assert m, "стиль пилюли не объявлен"
        body = m.group(0)
        assert "var(--amber-700)" in body, "цвет пилюли — не токеном"
        assert not re.search(r"var\(--[a-z0-9-]+\s*,\s*#", body), (
            "литеральный фолбэк var(--x,#hex) — в тёмной теме молча "
            "подставится светлый цвет (дефект 14.08.2026)")

    def test_dead_digest_mine_pill_class_is_gone(self):
        """Старое имя не должно остаться: следующая правка чинила бы не тот
        класс, а мёртвое правило CSS живёт вечно."""
        for name in ("app.js", "sberbank_dashboard.html"):
            assert "digest-mine-pill" not in _read(name), name
        css = _read("styles.css")
        assert not re.search(r"^\s*\.digest-mine-pill\s*[,{]", css, re.M), (
            "правило .digest-mine-pill осталось в styles.css")

    def test_cache_bust_versions_are_in_sync(self):
        html = _read("sberbank_dashboard.html")
        versions = set(re.findall(r"(?:app\.js|styles\.css)\?v=(\d+)", html))
        assert len(versions) == 1, f"?v= разъехались: {sorted(versions)}"
        sw = re.search(r"CACHE_VERSION\s*=\s*'v(\d+)'", _read("service-worker.js"))
        assert sw, "CACHE_VERSION не найден"
        assert sw.group(1) == versions.pop(), (
            "CACHE_VERSION и ?v= разошлись — у юриста PWA отдаст старый код")
