#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Точечное добавление дел через админку (канал «Добавить дела») — CLI.

Обёртка над court_monitor/targeted_add.py для workflow add_cases.yml:
оператор/владелец вставляет в админке до 20 строк (номера дел и/или ссылки
на карточки sudrf), Worker кладёт задание в KV (`import:case:<uuid>`) и
диспатчит workflow; тот скачивает job-файл и запускает этот скрипт.

Формат job-файла (JSON):
    {"items": ["2-1234/2026", "https://…sudrf.ru/modules.php?…name_op=case…"],
     "court_domain": "",     // опционально: суд для ВСЕЙ пачки номеров
     "court_srv_num": 0,     // опционально: площадка двухсерверного домена
     "operator": "Иванова"}

Ввод оператора идёт файлом, а не аргументом командной строки — строки пачки
произвольны и не должны касаться shell (тот же принцип, что у дампового
импортёра: см. предупреждение в import_cases.yml).

Отказ одной строки НЕ валит пачку: каждый исход считается в счётчики и
попадает построчно в lines. Файлы данных сохраняются ОДИН раз в конце
(только изменённые). JSON-сводка — в $GITHUB_OUTPUT (ключ summary) и в файл
$IMPORT_SUMMARY_PATH: workflow читает файл jq'ом и постит на /import-result
Worker'а БЕЗ интерполяции ${{ }} в shell.

Коды выхода: 0 — пачка обработана (в т.ч. «все строки отказаны» — продуктовый
итог виден в счётчиках, статус журнала done); 4 — тотальный сетевой сбой
(ни одна строка не прошла из-за сети/капчи); 5 — job-файл нечитаем или пуст.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import targeted_add as ta  # noqa: E402
from court_monitor.config import log  # noqa: E402
from court_monitor.courts import fi_court_by_domain  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

# Контракт add_cases.yml: ненулевой код = failed в журнале админки.
EXIT_OK = 0
EXIT_NETWORK = 4
EXIT_BAD_JOB = 5

# Какие исходы строк в какой счётчик сводки идут. Всё прочее (refused,
# fetch_error, нераспознанный ввод) считается «отказано» — построчная причина
# видна в lines.
_COUNTER_STATUSES = (
    ta.ST_ADDED_MAIN, ta.ST_ADDED_BANK, ta.ST_REACTIVATED,
    ta.ST_PROMOTED, ta.ST_ALREADY, ta.ST_NOT_FOUND,
)


def write_github_output(summary: dict) -> None:
    """JSON-сводка для add_cases.yml — двойник write_github_output дампового
    импортёра (scripts/import_search_dump.py): одной строкой в $GITHUB_OUTPUT
    (ключ summary) и файлом $IMPORT_SUMMARY_PATH (основной канал)."""
    payload = json.dumps(summary, ensure_ascii=False)
    out_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if out_path:
        try:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write("summary=" + payload + "\n")
        except OSError as e:
            log.warning(f"GITHUB_OUTPUT недоступен: {e}")
    file_path = os.environ.get("IMPORT_SUMMARY_PATH", "").strip()
    if file_path:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(payload + "\n")
        except OSError as e:
            log.warning(f"IMPORT_SUMMARY_PATH недоступен: {e}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Точечное добавление дел (канал админки)")
    ap.add_argument("--job", required=True,
                    help="JSON-файл задания (items/court_domain/operator)")
    ap.add_argument("--dry-run", action="store_true",
                    help="обработать и отчитаться, но файлы данных не менять")
    args = ap.parse_args(argv)

    region = get_region()
    summary: dict = {
        "kind": "case", "operator": "", "region": region.code,
        "dry_run": bool(args.dry_run),
        "items": 0, "added_main": 0, "added_bank": 0, "reactivated": 0,
        "promoted": 0, "already": 0, "refused": 0, "not_found": 0,
        "lines": [],
    }

    try:
        with open(args.job, encoding="utf-8") as f:
            job = json.load(f)
        raw_items = job.get("items") if isinstance(job, dict) else None
        items = [str(x).strip() for x in (raw_items or []) if str(x).strip()]
    except (OSError, json.JSONDecodeError) as e:
        summary["error"] = f"job-файл нечитаем: {e}"
        log.error(summary["error"])
        write_github_output(summary)
        return EXIT_BAD_JOB
    if not items:
        summary["error"] = "в задании нет ни одной строки"
        log.error(summary["error"])
        write_github_output(summary)
        return EXIT_BAD_JOB
    if len(items) > ta.MAX_ITEMS:
        # Worker режет пачку раньше нас; здесь — предохранитель от ручных job.
        log.warning(f"Строк больше {ta.MAX_ITEMS} — лишние отброшены")
        items = items[:ta.MAX_ITEMS]

    operator = str(job.get("operator") or "").strip()[:60]
    summary["operator"] = operator
    summary["items"] = len(items)

    # Суд для всей пачки номеров (опция «выберите суд» при неоднозначности).
    court_override = None
    dom = str(job.get("court_domain") or "").strip().lower()
    if dom:
        srv_raw = str(job.get("court_srv_num") or "").strip()
        srv = int(srv_raw) if srv_raw.isdigit() and int(srv_raw) > 0 else None
        court_override = fi_court_by_domain(dom, srv)
        if court_override is None:
            summary["error"] = (
                f"суд {dom} не найден в реестре региона {region.code!r}")
            log.error(summary["error"])
            write_github_output(summary)
            return EXIT_BAD_JOB

    state = ta.load_tracked_state()
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    results: list[dict] = []
    for i, raw in enumerate(items, 1):
        log.info(f"[{i}/{len(items)}] {raw[:100]}")
        res = ta.process_item(state, raw, operator, now_iso, court_override)
        results.append(res)
        log.info("  " + res["line"])
        summary["lines"].append(res["line"])

    for res in results:
        st = res["status"]
        if st in _COUNTER_STATUSES:
            summary[st] += 1
        else:
            summary["refused"] += 1

    if args.dry_run:
        log.info("DRY-RUN: файлы данных не изменены")
    else:
        saved = ta.save_state(state)
        if saved:
            log.info("Сохранено: " + ", ".join(saved))
        else:
            log.info("Изменений нет — файлы данных не тронуты")

    summary["lines"] = summary["lines"][:100]
    log.info("=" * 60)
    log.info(
        "Точечное добавление (оператор %s): строк %d | +%d основная | "
        "+%d иски банка | %d из архива | %d промоушенов | %d уже в базе | "
        "%d не найдено | %d отказано%s",
        operator or "—", summary["items"], summary["added_main"],
        summary["added_bank"], summary["reactivated"], summary["promoted"],
        summary["already"], summary["not_found"], summary["refused"],
        " | DRY-RUN" if args.dry_run else "",
    )

    if results and all(r["status"] == ta.ST_FETCH_ERROR for r in results):
        # Тотальный сбой сети — это НЕ продуктовый итог: журнал должен
        # показать failed, чтобы оператор повторил пачку позже.
        summary["error"] = (
            "ни одна строка не обработана — суды недоступны (сеть или "
            "проверочный код), повторите позже")
        write_github_output(summary)
        return EXIT_NETWORK
    write_github_output(summary)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
