#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Сборщик реестра судов региона — read-only разведка перед добавлением
территории в regions/ (этап 1 тиражирования, шаг 1.1).

Вход: CSV-файл (см. --input), по строке на суд:
    тип;Название суда;URL
где тип — fi (первая инстанция) или appeal. Разделитель — «;».

Для каждого суда делает два вежливых GET (задержка 2.5–4 с):
1) страница модуля «Судебное делопроизводство» (modules.php?name=sud_delo) —
   какие delo_id вообще есть на сайте (+ подписи разделов): проверяем, что
   ожидаемый delo_id гражданских дел (1540005 у 1-й инст., 5 у апелляции)
   существует, а не угадан;
2) боевой поиск по «Сбербанк» (name_op=r с дефолтными параметрами типа) —
   классификация ответа: RESULTS(N) / EMPTY(«данных не обнаружено») /
   CAPTCHA / FAIL.

Выход: готовые строки CourtConfig для regions/<код>.py + сводная таблица.
Ничего не пишет и не коммитит — только читает сайты судов.

Запуск:  python3 scripts/build_region_registry.py --input suds.csv
"""

from __future__ import annotations

import argparse
import html as html_mod
import os
import random
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from court_monitor.netutil import fetch_page, session  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    detect_captcha_challenge, _find_results_table,
)
from court_monitor.parsing.tables import extract_tables  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402

# Ожидаемые delo_id гражданских дел по типу суда (эталон ХМАО; скрипт
# ПРОВЕРЯЕТ их наличие на каждом сайте, а не предполагает).
EXPECTED_DELO_ID = {"fi": 1540005, "appeal": 5}

_DELO_LINK_RE = re.compile(
    r"<a[^>]*delo_id=(\d+)[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL
)
_TAG_RE = re.compile(r"<[^>]+>")
_NO_DATA = "данных по запросу не обнаружено"


def _polite():
    time.sleep(random.uniform(2.5, 4.0))


def _domain(url: str) -> str:
    m = re.match(r"https?://([^/]+)", url.strip())
    return m.group(1) if m else url.strip()


def survey_delo_ids(domain: str) -> dict[int, str]:
    """{delo_id: подпись ссылки} со страницы модуля sud_delo."""
    _polite()
    page = fetch_page(f"https://{domain}/modules.php?name=sud_delo",
                      context=f"обзор {domain}")
    found: dict[int, str] = {}
    if not page:
        return found
    for did, text in _DELO_LINK_RE.findall(page):
        label = html_mod.unescape(_TAG_RE.sub("", text)).strip()
        label = re.sub(r"\s+", " ", label)
        did_i = int(did)
        # Первая непустая подпись выигрывает (дальше идут дубли-вкладки).
        if did_i not in found or (not found[did_i] and label):
            found[did_i] = label[:80]
    return found


def live_search_check(court: CourtConfig) -> str:
    """Классификация боевого поиска: RESULTS(N) / EMPTY / CAPTCHA / FAIL."""
    _polite()
    page = fetch_page(court.search_url(), context=f"поиск {court.domain}")
    if not page:
        return "FAIL (страница не загрузилась)"
    if _NO_DATA in page.lower():
        return "EMPTY (данных по запросу не обнаружено — параметры приняты)"
    if detect_captcha_challenge(page):
        return "CAPTCHA (поиск закрыт проверочным кодом)"
    tbl = _find_results_table(extract_tables(page))
    if tbl:
        return f"RESULTS ({len(tbl) - 1} строк на стр. 1)"
    return "UNKNOWN (ни результатов, ни «нет данных», ни кода — смотреть глазами)"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True,
                    help="CSV: тип;Название;URL (тип = fi | appeal)")
    args = ap.parse_args()

    rows: list[tuple[str, str, str]] = []
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) != 3 or parts[0] not in EXPECTED_DELO_ID:
                print(f"⚠ пропускаю строку (ожидаю «fi|appeal;Название;URL»): {line}")
                continue
            rows.append((parts[0], parts[1], parts[2]))

    print(f"Судов на проверку: {len(rows)} (по 2 запроса на суд, ~3.5 с пауза)\n")
    config_lines: list[str] = []
    summary: list[str] = []

    for i, (ctype, name, url) in enumerate(rows, 1):
        dom = _domain(url)
        expected = EXPECTED_DELO_ID[ctype]
        print(f"[{i}/{len(rows)}] {name} — {dom}")

        ids = survey_delo_ids(dom)
        if not ids:
            verdict_ids = "FAIL (страница sud_delo не загрузилась)"
        elif expected in ids:
            verdict_ids = f"ok: delo_id={expected} найден («{ids[expected]}»)"
        else:
            civ = {d: t for d, t in ids.items() if "гражданск" in t.lower()}
            verdict_ids = (
                f"⚠ delo_id={expected} НЕ найден; кандидаты с «гражданск»: "
                f"{civ or ids}"
            )
        print(f"    разделы: {verdict_ids}")

        court_type = "first_instance" if ctype == "fi" else "appeal"
        court = CourtConfig(name, dom, expected, court_type)
        verdict_live = live_search_check(court)
        print(f"    поиск:   {verdict_live}\n")

        summary.append(f"{name:55.55} | {verdict_ids:60.60} | {verdict_live}")
        if expected in ids and not verdict_live.startswith(("FAIL", "CAPTCHA", "UNKNOWN")):
            config_lines.append(
                f'    CourtConfig("{name}", "{dom}", {expected}, "{court_type}"),'
            )

    print("=" * 100)
    print("СВОДКА:")
    for s in summary:
        print("  " + s)
    print("\nГотовые строки CourtConfig (только суды, прошедшие обе проверки):")
    for line in config_lines:
        print(line)


if __name__ == "__main__":
    main()
