#!/usr/bin/env python3
"""Переанкеровка ссылок на строки монолита в технической документации.

Документация (docs/technical/*.md, CLAUDE.md) ссылается на код паттерном:

    `symbol` … [762](../../scripts/update_cases.py#L762)
    `symbol(...)` … [Строка 762](../../scripts/update_cases.py#L762)

После правок update_cases.py номера строк уезжают. Скрипт находит такие
ссылки, где в пределах 60 символов ПЕРЕД ссылкой стоит `symbol` в бэктиках,
ищет актуальную строку `def symbol` / `class symbol` / `SYMBOL =` в монолите
и переписывает и текст ссылки, и #L-якорь. Ссылки без распознанного символа
(диапазоны «строки 100–200», якоря на баннеры-комментарии) не трогает —
печатает списком, чтобы поправить руками.

Запуск из корня репозитория:
    python3 scripts/refresh_doc_anchors.py          # показать план (dry-run)
    python3 scripts/refresh_doc_anchors.py --write  # применить
"""
from __future__ import annotations

import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "scripts", "update_cases.py")
DOC_GLOBS = [
    os.path.join(ROOT, "docs", "technical", "*.md"),
    os.path.join(ROOT, "CLAUDE.md"),
]
# 05-конвейер якорит места ВЫЗОВОВ внутри main_json (а не def функций) —
# переанкеровка по def их сломала бы. Обновлять руками вместе с main_json.
SKIP_FILES = {"05-конвейер-обновления.md"}

# `symbol` или `symbol(...)` в бэктиках, затем ≤60 символов БЕЗ бэктиков
# (перенос строки допустим — ссылка бывает на следующей строке), затем
# markdown-ссылка [762] / [Строка 762] на update_cases.py#L762. Запрет
# бэктиков в середине гарантирует, что ссылка привязывается к БЛИЖАЙШЕМУ
# символу, а не к случайному имени поля из прозы левее.
LINK_RX = re.compile(
    r"`(?P<sym>[A-Za-z_]\w*)(?:\([^)`]*\))?`"
    r"(?P<mid>[^`]{0,60}?)"
    r"\[(?P<word>[Сс]трока\s+)?(?P<disp>\d+)\]"
    r"\((?P<path>(?:\.\./)+scripts/update_cases\.py|scripts/update_cases\.py)"
    r"#L(?P<line>\d+)\)"
)


def build_symbol_table(path: str) -> dict[str, int]:
    """{имя → номер строки} для def/class/КОНСТАНТ верхнего уровня.

    Фолбэк: методы классов (def с отступом) добавляются, только если имя
    встречается в файле ровно один раз — например, `CourtConfig.search_url`.
    Неоднозначные имена методов в таблицу не попадают (лучше «не распознан»,
    чем неверный якорь)."""
    table: dict[str, int] = {}
    indented: dict[str, list[int]] = {}
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            m = re.match(r"^(?:def|class)\s+([A-Za-z_]\w*)", line)
            if not m:
                m = re.match(r"^([A-Za-z_]\w*)\s*(?::[^=]+)?=", line)
            if m:
                table.setdefault(m.group(1), i)
                continue
            m = re.match(r"^\s+def\s+([A-Za-z_]\w*)", line)
            if m:
                indented.setdefault(m.group(1), []).append(i)
    for name, lines in indented.items():
        if name not in table and len(lines) == 1:
            table[name] = lines[0]
    return table


def refresh_file(path: str, table: dict[str, int], write: bool) -> tuple[int, list[str]]:
    """Обновить якоря в одном файле. Возвращает (сколько поправлено,
    список нераспознанных ссылок для ручной правки)."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    out: list[str] = []
    pos = 0
    fixed = 0
    unresolved: list[str] = []
    for m in LINK_RX.finditer(text):
        sym = m.group("sym")
        new_line = table.get(sym)
        if new_line is None:
            unresolved.append(
                f"{os.path.basename(path)}: `{sym}` → #L{m.group('line')} (символ не найден)"
            )
            continue
        if int(m.group("line")) == new_line and m.group("disp") == str(new_line):
            continue
        out.append(text[pos:m.start("disp")])
        out.append(str(new_line))
        out.append(text[m.end("disp"):m.start("line")])
        out.append(str(new_line))
        pos = m.end("line")
        fixed += 1
    out.append(text[pos:])
    new_text = "".join(out)

    if write and fixed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_text)
    return fixed, unresolved


def main() -> None:
    write = "--write" in sys.argv
    table = build_symbol_table(SRC)
    total = 0
    all_unresolved: list[str] = []
    for pattern in DOC_GLOBS:
        for path in sorted(glob.glob(pattern)):
            if os.path.basename(path) in SKIP_FILES:
                continue
            fixed, unresolved = refresh_file(path, table, write)
            all_unresolved.extend(unresolved)
            if fixed:
                print(f"{'✏' if write else '→'} {os.path.relpath(path, ROOT)}: {fixed} якорей")
                total += fixed
    print(f"\nИтого {'обновлено' if write else 'к обновлению'}: {total}")
    if all_unresolved:
        print("\nНе распознаны (поправить руками):")
        for u in sorted(set(all_unresolved)):
            print("  •", u)
    if not write and total:
        print("\nПрименить: python3 scripts/refresh_doc_anchors.py --write")


if __name__ == "__main__":
    main()
