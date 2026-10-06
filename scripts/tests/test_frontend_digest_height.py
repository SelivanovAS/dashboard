"""Страж высоты раскрытого дайджеста (разбор 21.08.2026).

Юрист: «Очень длинный дайджест по ЕКБ. Даже не помещается в мобильной
версии». Помимо объёма нашлась буквальная обрезка: у `.digest-body` в
базовом правиле стоит `overflow: hidden`, а в `.digest-block.expanded` был
`max-height: 10000px` — на телефоне (16px × line-height 1.6 = 25.6px на
строку) это ~390 визуальных строк. Замер того дня: у Урала 19 285 символов
видимого текста (~820 строк) — видно было меньше половины, БЕЗ скролла и без
индикатора, у ХМАО 5 754 влезало впритык и ломалось при втором выпуске дня
(«➕ Дополнение», digest/core.py).

Дефект прятался за beacon-режимом: свежий дайджест открывается именно в нём
(app.js, isFreshDigest), а там потолок снят с самого начала. Обрезка била при
ПОВТОРНОМ ручном раскрытии — то есть у юриста, вернувшегося к дайджесту днём.

Запуск: python3 -m pytest scripts/tests/test_frontend_digest_height.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _rule(css: str, selector: str) -> str:
    """Тело правила по точному селектору, БЕЗ комментариев.

    Комментарии режем обязательно: в правиле дайджеста прежнее значение
    `max-height:10000px` упомянуто в пояснении к правке, и поиск по сырому
    тексту нашёл бы именно его.
    """
    css = re.sub(r"/\*.*?\*/", " ", css, flags=re.S)
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert m, f"Правило {selector} исчезло из styles.css"
    return m.group(1)


def test_expanded_digest_has_no_height_ceiling():
    """У раскрытого дайджеста не должно быть КОНЕЧНОГО max-height."""
    body = _rule(_read("styles.css"), ".digest-block.expanded .digest-body")
    m = re.search(r"max-height\s*:\s*([^;]+);", body)
    assert m, (
        "У .digest-block.expanded .digest-body пропал max-height — базовое "
        "правило .digest-body задаёт max-height: 0, и дайджест не раскроется "
        "вовсе."
    )
    value = m.group(1).strip()
    assert value == "none", (
        f"max-height: {value} снова режет хвост дайджеста. Базовое правило "
        ".digest-body несёт overflow: hidden, поэтому любой конечный потолок "
        "обрезает текст молча — без скролла и без индикатора. Ставить "
        "max-height: none и overflow: visible, как в beacon-режиме."
    )


def test_expanded_digest_restores_overflow():
    """`overflow: hidden` базового правила обязан быть переопределён.

    Именно связка «конечный потолок + hidden» и делала обрезку невидимой:
    со `visible` пользователь хотя бы увидел бы вылезающий текст.
    """
    body = _rule(_read("styles.css"), ".digest-block.expanded .digest-body")
    m = re.search(r"overflow\s*:\s*([^;]+);", body)
    assert m and m.group(1).strip() == "visible", (
        "В .digest-block.expanded .digest-body нет overflow: visible — "
        "наследуется hidden из .digest-body, и хвост дайджеста снова "
        "обрежется без следа."
    )


def test_beacon_reference_rule_intact():
    """Beacon — эталон, с которого списана правка: он не должен разъехаться."""
    body = _rule(_read("styles.css"), ".digest-block.beacon .digest-body")
    assert re.search(r"max-height\s*:\s*none", body), (
        "В beacon-режиме появился потолок высоты — там дайджест обязан "
        "открываться целиком (его скроллит сам .digest-block)."
    )
