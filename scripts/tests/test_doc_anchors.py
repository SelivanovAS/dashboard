"""
Страж якорей документации: ссылки на строки кода не должны протухать.

Техдоки (docs/technical/*.md, CLAUDE.md) ссылаются на конкретные строки
исходников. Номера уезжают при любой правке кода, и молча: ссылка остаётся
кликабельной, просто ведёт не туда. К 26.07.2026 так уехали ВСЕ 50 якорей во
фронт и Worker — refresh_doc_anchors.py знал только про Python, а
05-конвейер-обновления.md вообще был в списке «править руками».

Тест гоняет сам переанкеровщик в режиме dry-run и требует, чтобы ему нечего
было менять. Чинится одной командой:

    python3 scripts/refresh_doc_anchors.py --write

Запуск: python3 -m pytest scripts/tests/test_doc_anchors.py
"""

from __future__ import annotations

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import refresh_doc_anchors as rda  # noqa: E402

# Ссылки, где ближайший бэктик — не символ кода, а имя поля JSON или стадии
# из прозы. Якорь у них позиционный (место в коде, а не `def`), проверить
# автоматически нельзя — фиксируем список, чтобы он не рос незаметно.
ИЗВЕСТНЫЕ_ПОЗИЦИОННЫЕ = {
    "05-конвейер-обновления.md: `cases`",
    "05-конвейер-обновления.md: `cassation_watch`",
    "05-конвейер-обновления.md: `first_instance`",
    "05-конвейер-обновления.md: `stage_transitions`",
}


def _прогон() -> tuple[int, list[str]]:
    tables = {
        "py": rda.build_symbol_table(rda.PY_FILES),
        "js": rda.build_symbol_table(rda.JS_FILES),
        "calls": rda.build_call_table(
            os.path.join(rda.ROOT, rda.CALL_SITE_HOST), rda.CALL_SITE_FUNC),
    }
    total = 0
    unresolved: list[str] = []
    import glob
    for pattern in rda.DOC_GLOBS:
        for path in sorted(glob.glob(pattern)):
            if os.path.basename(path) in rda.SKIP_FILES:
                continue
            fixed, unres = rda.refresh_file(path, tables, write=False)
            total += fixed
            unresolved.extend(unres)
    return total, unresolved


def test_no_stale_anchors():
    """Ни один якорь не уехал."""
    total, _ = _прогон()
    assert total == 0, (
        f"{total} якорей документации указывают не на те строки. "
        "Починить: python3 scripts/refresh_doc_anchors.py --write"
    )


def test_unresolved_list_does_not_grow():
    """Список нераспознанных ссылок не растёт.

    Новая запись здесь означает либо переименованный/удалённый символ (ссылка
    ведёт в никуда — как было с `expandWatchlistAliases`, которого в app.js не
    существует с v98), либо новую ссылку, которую переанкеровщик не понимает.
    """
    _, unresolved = _прогон()
    ключи = {u.split(" → ")[0] for u in unresolved}
    новые = ключи - ИЗВЕСТНЫЕ_ПОЗИЦИОННЫЕ
    assert not новые, (
        "Появились нераспознанные ссылки на код: " + ", ".join(sorted(новые))
        + ". Либо символ переименован/удалён (тогда править текст дока), либо "
        "ссылка позиционная — добавить в ИЗВЕСТНЫЕ_ПОЗИЦИОННЫЕ."
    )


def test_frontend_and_worker_are_covered():
    """Фронт и Worker остаются в зоне ответственности переанкеровщика.

    Именно их отсутствие в SRC_FILES и дало молчаливый дрейф всех 50 якорей.
    """
    имена = {os.path.basename(p) for p in rda.JS_FILES}
    assert {"app.js", "worker.js"} <= имена
    for путь in rda.JS_FILES:
        assert os.path.exists(путь), f"{путь} из JS_FILES не существует"
    таблица = rda.build_symbol_table(rda.JS_FILES)
    # Контрольные символы разных форм объявления: function / const / метод
    # default-экспорта Worker'а.
    for символ in ("openDrawer", "buildWritsSectionHtml", "renderAdminHtml",
                   "CACHE_VERSION", "scheduled"):
        assert символ in таблица, (
            f"JS-символ {символ!r} не распознан — сломался разбор объявлений."
        )
