#!/usr/bin/env python3
"""Переанкеровка ссылок на строки кода в технической документации.

Документация (docs/technical/*.md, CLAUDE.md) ссылается на код паттернами:

    `symbol` … [762](../../scripts/court_monitor/health.py#L762)
    `symbol(...)` … [Строка 762](../../scripts/court_monitor/health.py#L762)
    `symbol` … [scripts/court_monitor/health.py:762](scripts/court_monitor/health.py:762)

После правок кода номера строк уезжают, а после переносов между модулями
пакета court_monitor устаревает и путь. Скрипт находит такие ссылки, где в
пределах 60 символов ПЕРЕД ссылкой стоит `symbol` в бэктиках, ищет
актуальные файл и строку `def symbol` / `class symbol` / `SYMBOL =` по всем
модулям пакета (scripts/court_monitor/**/*.py + фасад scripts/update_cases.py)
и переписывает текст ссылки, путь и #L-якорь. Ссылки без распознанного
символа (диапазоны «строки 100–200», якоря на места вызовов) не трогает —
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
# Фасад + все модули пакета. Порядок важен: при коллизии имён побеждает
# ПЕРВЫЙ файл, поэтому фасад (только ре-экспорты, реальных def нет) — в конце.
SRC_FILES = sorted(
    glob.glob(os.path.join(ROOT, "scripts", "court_monitor", "**", "*.py"),
              recursive=True)
) + [os.path.join(ROOT, "scripts", "update_cases.py")]
DOC_GLOBS = [
    os.path.join(ROOT, "docs", "technical", "*.md"),
    os.path.join(ROOT, "CLAUDE.md"),
]
# 05-конвейер якорит места ВЫЗОВОВ внутри main_json (а не def функций) —
# переанкеровка по def их сломала бы. Обновлять руками вместе с runs.main_json.
SKIP_FILES = {"05-конвейер-обновления.md"}

# `symbol` или `symbol(...)` в бэктиках, затем ≤60 символов БЕЗ бэктиков
# (перенос строки допустим — ссылка бывает на следующей строке), затем
# markdown-ссылка на код: [762](…#L762), [Строка 762](…#L762) или
# [scripts/...py:762](scripts/...py:762). Запрет бэктиков в середине
# гарантирует привязку к БЛИЖАЙШЕМУ символу, а не к имени из прозы левее.
_CODE_PATH = r"scripts/(?:court_monitor/[\w/]+\.py|update_cases\.py|add_cases_manually\.py)"
LINK_RX = re.compile(
    r"`(?P<sym>[A-Za-z_]\w*)(?:\([^)`]*\))?`"
    r"(?P<mid>[^`]{0,60}?)"
    r"\[(?P<label>(?:[Сс]трока\s+)?\d+|" + _CODE_PATH + r":\d+)\]"
    r"\((?P<prefix>(?:\.\./)*)(?P<path>" + _CODE_PATH + r")"
    r"(?P<sep>#L|:)(?P<line>\d+)\)"
)


def build_symbol_table(paths: list[str]) -> dict[str, tuple[str, int]]:
    """{имя → (путь_от_корня, номер строки)} для def/class/КОНСТАНТ
    верхнего уровня по всем файлам пакета.

    Импорты (в т.ч. ре-экспорты фасада) не учитываются — матчится только
    определение. Фолбэк: методы классов (def с отступом) добавляются, только
    если имя встречается во всех файлах ровно один раз."""
    table: dict[str, tuple[str, int]] = {}
    indented: dict[str, list[tuple[str, int]]] = {}
    for path in paths:
        rel = os.path.relpath(path, ROOT)
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                m = re.match(r"^(?:def|class)\s+([A-Za-z_]\w*)", line)
                if not m:
                    m = re.match(r"^([A-Za-z_]\w*)\s*(?::[^=]+)?=", line)
                if m:
                    table.setdefault(m.group(1), (rel, i))
                    continue
                m = re.match(r"^\s+def\s+([A-Za-z_]\w*)", line)
                if m:
                    indented.setdefault(m.group(1), []).append((rel, i))
    for name, locs in indented.items():
        if name not in table and len(locs) == 1:
            table[name] = locs[0]
    return table


def refresh_file(path: str, table: dict[str, tuple[str, int]],
                 write: bool) -> tuple[int, list[str]]:
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
        loc = table.get(sym)
        if loc is None:
            unresolved.append(
                f"{os.path.basename(path)}: `{sym}` → {m.group('path')}"
                f"{m.group('sep')}{m.group('line')} (символ не найден)"
            )
            continue
        new_path, new_line = loc
        if (m.group("path") == new_path
                and int(m.group("line")) == new_line
                and (m.group("label").endswith(str(new_line)))):
            continue
        # подпись: число / «Строка N» / «путь:N» — сохраняем стиль
        label = m.group("label")
        if re.fullmatch(r"(?:[Сс]трока\s+)?\d+", label):
            new_label = re.sub(r"\d+$", str(new_line), label)
        else:
            new_label = f"{new_path}:{new_line}"
        out.append(text[pos:m.start("label")])
        out.append(new_label)
        out.append(text[m.end("label"):m.start("path")])
        out.append(new_path + m.group("sep") + str(new_line))
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
    table = build_symbol_table(SRC_FILES)
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
