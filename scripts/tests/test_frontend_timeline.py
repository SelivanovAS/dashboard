"""
Стражи хронологии drawer'а во фронте (app.js).

JS-инструментария в проекте нет, поэтому инварианты проверяются grep'ом по
исходнику — тем же приёмом, что и сверка версий в test_versions.py.

Что охраняем:
1. Обрезки событий больше нет. `cleanTimelineText` вырезала «Зал N», время и
   дату размещения — из-за неё юрист не видел в drawer'е часть карточки суда
   (а drawer нужен именно тогда, когда сайты судов недоступны).
2. Разбор форматов события живёт в ОДНОЙ функции `normalizeTlEvent`. Форматов
   три и это навсегда: карточки дел в стадиях appeal/cassation/awaiting_relink
   больше не перепарсиваются (should_parse_fi_card), архив не парсится вовсе.
3. Хронология строится по инстанциям: `buildTimeline` принимает вторым
   аргументом стадию и вызывается с ней.

Запуск: python3 -m pytest scripts/tests/test_frontend_timeline.py
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def test_clean_timeline_text_removed():
    """Функция-обрезка удалена целиком, вместе со всеми вызовами."""
    assert "cleanTimelineText" not in _app_js(), (
        "cleanTimelineText вернулась в app.js — она срезает «Зал N», время и "
        "дату размещения, из-за чего drawer перестаёт быть полным зеркалом "
        "карточки суда."
    )


def test_normalize_tl_event_exists():
    """Единая точка знания о трёх форматах события."""
    assert re.search(r"function\s+normalizeTlEvent\s*\(", _app_js()), (
        "В app.js нет normalizeTlEvent — разбор форматов события (структурный "
        "/ legacy / «Движение жалобы») должен жить в одной функции."
    )


def test_build_timeline_is_per_instance():
    """buildTimeline объявлена со стадией и вызывается со стадией."""
    src = _app_js()
    assert re.search(r"function\s+buildTimeline\s*\(\s*c\s*,\s*\S+\s*\)", src), (
        "buildTimeline должна принимать вторым аргументом стадию — иначе "
        "хронология снова станет общей для всех инстанций."
    )
    # Вызов без второго аргумента вернул бы общий список по всем инстанциям —
    # ровно тот регресс, который эта правка и устраняет.
    bare = re.findall(r"buildTimeline\s*\(\s*c\s*\)", src)
    assert not bare, (
        f"Найден вызов buildTimeline(c) без стадии ({len(bare)} шт.) — "
        "хронология в drawer'е перестанет разделяться по инстанциям."
    )
