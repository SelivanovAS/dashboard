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

Второй режим — `--scan-servers` (с 13.08.2026, разгон Урала): разведка
СУДЕБНЫХ ПРИСУТСТВИЙ / вторых площадок. Судебное присутствие живёт на домене
районного суда отдельным сервером (`srv_num=2+`: Покачи на vartovray в ХМАО,
Камышловский/Красноуфимский на Урале), и суд без записи в конфиге НЕВИДИМ
ЦЕЛИКОМ — обычная проба по CSV ходит только на сервер 1 и площадок не
находит. Режим делает 1 GET страницы модуля sud_delo на каждый уникальный
домен 1-й инстанции региона (env REGION; `--input` добавляет домены-кандидаты
не из конфига), разбирает селектор площадок (ссылки с `srv_num=`, фолбэк —
union всех srv_num в HTML) и сверяет с конфигом: «⚠ НОВАЯ ПЛОЩАДКА» + готовая
строка CourtConfig (search_gated наследуется от площадок домена).

Запуск:  REGION=sverdlovsk_yanao python3 scripts/build_region_registry.py --scan-servers
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
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig, RegionConfig  # noqa: E402

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


# ── Разведка судебных присутствий (вторых площадок, --scan-servers) ──────────

_SRV_LINK_RE = re.compile(
    r"<a[^>]*srv_num=(\d+)[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL
)
_SRV_ANY_RE = re.compile(r"srv_num=(\d+)")


def parse_server_options(page: str) -> dict[int, str]:
    """{srv_num: подпись} со страницы модуля sud_delo.

    Многосерверный домен встречает селектором площадок — ссылками с
    srv_num=N; на односерверном srv_num=1 сидит в ссылках разделов, так что
    словарь честно схлопывается до {1: …}. Фолбэк — union всех srv_num= в
    HTML (селектор может быть свёрстан не ссылками, тогда подписи пустые);
    совсем пусто → {1: ""} (площадка одна, параметр сайту не нужен).
    """
    found: dict[int, str] = {}
    for num, text in _SRV_LINK_RE.findall(page or ""):
        label = html_mod.unescape(_TAG_RE.sub("", text)).strip()
        label = re.sub(r"\s+", " ", label)
        n = int(num)
        # Первая непустая подпись выигрывает (дальше идут дубли-вкладки).
        if n not in found or (not found[n] and label):
            found[n] = label[:80]
    if not found:
        for num in _SRV_ANY_RE.findall(page or ""):
            found.setdefault(int(num), "")
    return found or {1: ""}


def _region_fi_index(region: RegionConfig):
    """(площадки, капча, имя) по доменам 1-й инстанции конфига региона."""
    configured: dict[str, set[int]] = {}
    gated: dict[str, bool] = {}
    names: dict[str, str] = {}
    for c in region.first_instance_courts:
        d = c.domain.lower()
        configured.setdefault(d, set()).add(c.srv_num)
        gated[d] = gated.get(d, False) or c.search_gated
        names.setdefault(d, c.name)
    return configured, gated, names


def compare_servers(domain: str, name: str, found: dict[int, str],
                    region: RegionConfig) -> tuple[str, list[str]]:
    """Сверка найденных площадок домена с конфигом региона.

    Возвращает (строка сводки, готовые CourtConfig-строки новых площадок).
    Для домена не из конфига новыми считаются ВСЕ найденные площадки.
    """
    configured, gated, names = _region_fi_index(region)
    dom = domain.lower()
    known = configured.get(dom, set())
    missing = sorted(set(found) - known)
    gone = sorted(known - set(found))
    base_name = names.get(dom, name)

    parts = [f"найдено: {sorted(found)}", f"в конфиге: {sorted(known) or '—'}"]
    if missing:
        labels = "; ".join(
            f"{n}: {found[n] or 'без подписи'}" for n in missing)
        parts.append(f"⚠ НОВАЯ ПЛОЩАДКА → {labels}")
    if gone:
        # Конфиг знает площадку, которой нет на странице — чаще это значит,
        # что селектор не распарсился, а не что присутствие закрыли.
        parts.append(f"⚠ в конфиге, но не на странице: {gone}")

    config_lines = []
    for n in missing:
        gated_part = ", search_gated=True" if gated.get(dom) else ""
        label = found[n] or f"сервер {n}"
        config_lines.append(
            f'    CourtConfig("{base_name} ({label})", "{dom}", 1540005, '
            f'"first_instance"{gated_part}, srv_num={n}),'
        )
    return " | ".join(parts), config_lines


def scan_servers_mode(extra: list[tuple[str, str]]) -> None:
    """Обойти домены 1-й инстанции региона (плюс кандидатов из --input),
    1 GET на домен, сверить площадки с конфигом."""
    region = get_region()
    _, _, names = _region_fi_index(region)
    targets: list[tuple[str, str]] = [
        (d, n) for d, n in names.items()
    ]
    seen_domains = {d for d, _ in targets}
    for dom, name in extra:
        if dom.lower() not in seen_domains:
            targets.append((dom.lower(), name))
            seen_domains.add(dom.lower())

    print(f"Регион {region.code}: доменов на скан площадок — {len(targets)} "
          "(1 запрос на домен, ~3.5 с пауза)\n")
    summary: list[str] = []
    config_lines: list[str] = []
    for i, (dom, name) in enumerate(targets, 1):
        print(f"[{i}/{len(targets)}] {name} — {dom}")
        _polite()
        page = fetch_page(f"https://{dom}/modules.php?name=sud_delo",
                          context=f"площадки {dom}")
        if not page:
            verdict = "FAIL (страница sud_delo не загрузилась)"
        else:
            found = parse_server_options(page)
            verdict, lines = compare_servers(dom, name, found, region)
            config_lines.extend(lines)
        print(f"    {verdict}\n")
        summary.append(f"{name:45.45} | {verdict}")

    print("=" * 100)
    print("СВОДКА ПЛОЩАДОК:")
    for s in summary:
        print("  " + s)
    if config_lines:
        print("\n⚠ Найдены площадки вне конфига — готовые строки CourtConfig "
              "(проверить глазами подпись/капчу перед добавлением):")
        for line in config_lines:
            print(line)
    else:
        print("\nНовых площадок не найдено — конфиг региона полон.")


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


def _read_input_rows(path: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) != 3 or parts[0] not in EXPECTED_DELO_ID:
                print(f"⚠ пропускаю строку (ожидаю «fi|appeal;Название;URL»): {line}")
                continue
            rows.append((parts[0], parts[1], parts[2]))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",
                    help="CSV: тип;Название;URL (тип = fi | appeal); "
                         "обязателен без --scan-servers")
    ap.add_argument("--scan-servers", action="store_true",
                    help="разведка судебных присутствий: домены 1-й инст. "
                         "региона (env REGION) + fi-строки --input, 1 GET на "
                         "домен, сверка srv_num с конфигом")
    args = ap.parse_args()

    if args.scan_servers:
        extra = []
        if args.input:
            extra = [(_domain(url), name)
                     for ctype, name, url in _read_input_rows(args.input)
                     if ctype == "fi"]
        scan_servers_mode(extra)
        return

    if not args.input:
        ap.error("--input обязателен (или используйте --scan-servers)")
    rows = _read_input_rows(args.input)

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
