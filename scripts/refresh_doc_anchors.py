#!/usr/bin/env python3
"""Переанкеровка ссылок на строки кода в технической документации.

Документация (docs/technical/*.md, CLAUDE.md) ссылается на код паттернами:

    `symbol` … [762](../../scripts/court_monitor/health.py#L762)
    `symbol(...)` … [Строка 762](../../scripts/court_monitor/health.py#L762)
    `symbol` … [scripts/court_monitor/health.py:762](scripts/court_monitor/health.py:762)

После правок кода номера строк уезжают, а после переносов между модулями
устаревает и путь. Скрипт находит такие ссылки, где в пределах 60 символов
ПЕРЕД ссылкой (или внутри её метки) стоит `symbol` в бэктиках, ищет актуальные
файл и строку определения и переписывает текст ссылки, путь и #L-якорь.

Покрытие:
- Python — весь пакет scripts/court_monitor/**/*.py + фасад update_cases.py
  (`def` / `class` / `КОНСТАНТА =`, фолбэком — методы классов);
- JS — app.js, service-worker.js, cloudflare-worker/{worker,admin_page}.js
  (`function` / `const|let|var` / `class`, фолбэком — методы default-экспорта
  Worker'а). Без этого 08-фронтенд.md и 09-cloudflare-worker.md дрейфовали
  молча: к 26.07.2026 мимо цели били все 50 их якорей;
- 05-конвейер-обновления.md описывает ПОРЯДОК шагов main_json, поэтому его
  ссылки на runs.py резолвятся в места ВЫЗОВОВ внутри main_json, а из
  нескольких вызовов одного символа берётся следующий по ходу повествования.

Ссылки без распознанного символа (диапазоны «строки 100–200», позиционные
якоря на прозу вроде `first_instance`) не трогает — печатает списком.
Актуальность якорей стережёт scripts/tests/test_doc_anchors.py.

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
PY_FILES = sorted(
    glob.glob(os.path.join(ROOT, "scripts", "court_monitor", "**", "*.py"),
              recursive=True)
) + [os.path.join(ROOT, "scripts", "update_cases.py")]
# Фронт и Worker. Без них 08-фронтенд.md и 09-cloudflare-worker.md дрейфовали
# молча: скрипт знал только про Python, а якорей туда — полсотни, и к
# 26.07.2026 мимо цели били ВСЕ.
JS_FILES = [
    os.path.join(ROOT, "app.js"),
    os.path.join(ROOT, "service-worker.js"),
    os.path.join(ROOT, "cloudflare-worker", "worker.js"),
    os.path.join(ROOT, "cloudflare-worker", "admin_page.js"),
]
DOC_GLOBS = [
    os.path.join(ROOT, "docs", "technical", "*.md"),
    os.path.join(ROOT, "CLAUDE.md"),
]
# 05-конвейер якорит места ВЫЗОВОВ внутри main_json (а не def функций), поэтому
# для ссылок на runs.py у него отдельная таблица — первый вызов символа в теле
# main_json. Раньше файл просто пропускался «править руками», и к 26.07.2026
# все его якоря уехали. Ссылки на другие модули (linking.py, lifecycle.py)
# резолвятся как везде — по определению.
CALL_SITE_FILES = {"05-конвейер-обновления.md"}
CALL_SITE_HOST = os.path.join("scripts", "court_monitor", "runs.py")
CALL_SITE_FUNC = "main_json"
SKIP_FILES: set[str] = set()

# `symbol` или `symbol(...)` в бэктиках, затем ≤60 символов БЕЗ бэктиков
# (перенос строки допустим — ссылка бывает на следующей строке), затем
# markdown-ссылка на код: [762](…#L762), [Строка 762](…#L762) или
# [scripts/...py:762](scripts/...py:762). Запрет бэктиков в середине
# гарантирует привязку к БЛИЖАЙШЕМУ символу, а не к имени из прозы левее.
_PY_PATH = r"scripts/(?:court_monitor/[\w/]+\.py|update_cases\.py|add_cases_manually\.py)"
_JS_PATH = r"(?:app\.js|service-worker\.js|cloudflare-worker/(?:worker|admin_page)\.js)"
_CODE_PATH = r"(?:" + _PY_PATH + r"|" + _JS_PATH + r")"
# Подпись ссылки: «762» / «Строка 762» / «путь:762». Путь в подписи бывает и
# коротким именем файла («worker.js:1006») — стиль сохраняется при перезаписи.
_LABEL = r"(?:[Сс]трока\s+)?\d+|[\w./-]+\.(?:py|js):\d+"
# Две формы записи. Первая — символ ПЕРЕД ссылкой, вторая — символ ВНУТРИ
# метки («[`DEFAULT_SHEET_URL`, app.js:2](../../app.js#L2)»). Обе требуют
# скобку `[` ровно в одном месте: без этого требования подпись-диапазон
# «[Строки 300–330](…)» матчилась бы хвостом «330]» и была бы переписана.
_SYM = r"[A-Za-z_$][\w$]*(?:\([^)`]*\))?"
LINK_RX = re.compile(
    r"(?:"
    r"\[`(?P<sym_in>" + _SYM + r")`(?P<mid_in>[^`\[\]]{0,60}?)"
    r"|"
    r"`(?P<sym>" + _SYM + r")`(?P<mid>[^`\[\]]{0,60}?)\["
    r")"
    r"(?P<label>" + _LABEL + r")\]"
    r"\((?P<prefix>(?:\.\./)*)(?P<path>" + _CODE_PATH + r")"
    r"(?P<sep>#L|:)(?P<line>\d+)\)"
)


# Определения верхнего уровня. Питон: def/class/КОНСТАНТА. JS: function
# (в т.ч. async/export) / const|let|var / class.
_TOP_PY = (
    re.compile(r"^(?:def|class)\s+([A-Za-z_]\w*)"),
    re.compile(r"^([A-Za-z_]\w*)\s*(?::[^=]+)?="),
)
_TOP_JS = (
    re.compile(r"^(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+([A-Za-z_$]\w*)"),
    re.compile(r"^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$]\w*)\s*="),
    re.compile(r"^(?:export\s+)?class\s+([A-Za-z_$]\w*)"),
)
# Вложенные определения — фолбэк, берётся только при единственном совпадении
# по всем файлам. У JS так живут методы default-экспорта Worker'а
# (`async scheduled(event, env) {`, `async fetch(request, env) {`).
_NESTED_PY = re.compile(r"^\s+def\s+([A-Za-z_]\w*)")
_NESTED_JS = re.compile(r"^\s+(?:async\s+)?([A-Za-z_$]\w*)\s*\([^)]*\)\s*\{\s*$")


def build_symbol_table(paths: list[str]) -> dict[str, tuple[str, int]]:
    """{имя → (путь_от_корня, номер строки)} для определений верхнего уровня.

    Импорты (в т.ч. ре-экспорты фасада) не учитываются — матчится только
    определение. Фолбэк: вложенные определения (метод класса в Python, метод
    default-экспорта в JS) добавляются, только если имя встречается во всех
    файлах ровно один раз. Язык выбирается по расширению файла, поэтому
    таблицы .py и .js собираются независимо и не воруют имена друг у друга."""
    table: dict[str, tuple[str, int]] = {}
    nested: dict[str, list[tuple[str, int]]] = {}
    for path in paths:
        rel = os.path.relpath(path, ROOT)
        js = path.endswith(".js")
        top_rx, nested_rx = (_TOP_JS, _NESTED_JS) if js else (_TOP_PY, _NESTED_PY)
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                for rx in top_rx:
                    m = rx.match(line)
                    if m:
                        table.setdefault(m.group(1), (rel, i))
                        break
                else:
                    m = nested_rx.match(line)
                    if m:
                        nested.setdefault(m.group(1), []).append((rel, i))
    for name, locs in nested.items():
        if name not in table and len(locs) == 1:
            table[name] = locs[0]
    return table


def build_call_table(host: str, func: str) -> dict[str, list[tuple[str, int]]]:
    """{имя → [(путь, строка вызова), …]} внутри тела функции func.

    Нужна 05-конвейеру: он описывает порядок шагов main_json, поэтому его
    ссылки указывают на вызовы, а не на определения. Тело функции — от её
    `def` до следующего определения нулевого уровня.

    Возвращаются ВСЕ вызовы: один символ вызывается в разных фазах (например,
    send_telegram — и раннее предупреждение, и рассылка дайджеста), а нужный
    выбирается по порядку повествования (см. refresh_file)."""
    rel = os.path.relpath(host, ROOT)
    with open(host, encoding="utf-8") as f:
        lines = f.readlines()
    start = end = None
    for i, line in enumerate(lines, 1):
        if start is None:
            if re.match(r"^def\s+" + re.escape(func) + r"\s*\(", line):
                start = i
        elif re.match(r"^(?:def|class|@)\s*\w", line):
            end = i
            break
    if start is None:
        return {}
    table: dict[str, list[tuple[str, int]]] = {}
    for i in range(start, (end or len(lines) + 1)):
        # Комментарий отсекаем: имя из пояснительного текста («…отличается от
        # stage_transitions(…)») — не место вызова, и якорь уехал бы в прозу.
        код = lines[i - 1].split("#", 1)[0]
        for name in re.findall(r"\b([A-Za-z_]\w*)\s*\(", код):
            table.setdefault(name, []).append((rel, i))
    return table


def refresh_file(path: str, tables: dict[str, dict[str, tuple[str, int]]],
                 write: bool) -> tuple[int, list[str]]:
    """Обновить якоря в одном файле. Возвращает (сколько поправлено,
    список нераспознанных ссылок для ручной правки).

    Таблица выбирается по расширению файла, на который ссылка уже указывает:
    иначе одноимённый Python-символ увёл бы JS-якорь в другой файл (и наоборот).
    Перенос символа между модулями ОДНОГО языка по-прежнему отслеживается."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    out: list[str] = []
    pos = 0
    fixed = 0
    unresolved: list[str] = []
    вызовы = (os.path.basename(path) in CALL_SITE_FILES
              and tables.get("calls") or {})
    # Курсор повествования: документ идёт по конвейеру сверху вниз, поэтому из
    # нескольких вызовов одного символа берём первый ПОСЛЕ предыдущего якоря.
    курсор = 0
    for m in LINK_RX.finditer(text):
        sym = (m.group("sym") or m.group("sym_in")).split("(")[0]
        table = tables["js" if m.group("path").endswith(".js") else "py"]
        # Ссылка 05-конвейера на runs.py — это шаг конвейера, т.е. ВЫЗОВ
        # внутри main_json; на прочие модули — обычное определение.
        loc = None
        if (m.group("path").replace("/", os.sep)
                == os.path.relpath(CALL_SITE_HOST, ROOT)):
            места = вызовы.get(sym) or []
            loc = next((p for p in места if p[1] >= курсор), места[0] if места else None)
            if loc:
                курсор = loc[1]
        if loc is None:
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
        # подпись: число / «Строка N» / «путь:N» — сохраняем стиль, включая
        # короткое имя файла («worker.js:1006»), которым подписана часть ссылок.
        label = m.group("label")
        if re.fullmatch(r"(?:[Сс]трока\s+)?\d+", label):
            new_label = re.sub(r"\d+$", str(new_line), label)
        else:
            старый_путь = label.rsplit(":", 1)[0]
            краткий = "/" not in старый_путь
            new_label = f"{os.path.basename(new_path) if краткий else new_path}:{new_line}"
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
    tables = {"py": build_symbol_table(PY_FILES),
              "js": build_symbol_table(JS_FILES),
              "calls": build_call_table(
                  os.path.join(ROOT, CALL_SITE_HOST), CALL_SITE_FUNC)}
    total = 0
    all_unresolved: list[str] = []
    for pattern in DOC_GLOBS:
        for path in sorted(glob.glob(pattern)):
            if os.path.basename(path) in SKIP_FILES:
                continue
            fixed, unresolved = refresh_file(path, tables, write)
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
