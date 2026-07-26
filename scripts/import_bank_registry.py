#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Импортёр реестра исков банка (банк — истец) в data/cases_bank.json.

Канал ввода пула исков банка: автопоиск их завести не может (видит только
первую страницу выдачи + фильтр «банк-ответчик»), поэтому список дел приходит
реестром из внутренних систем банка.

Вход — CSV-файл (см. --input), по строке на дело:
    court_domain;case_number
например:
    surggor--hmao.sudrf.ru;2-4440/2025
Разделитель — «;». Пустые строки и строки с «#» в начале игнорируются,
шапка «court_domain;case_number» — тоже.

Поток на дело (2 вежливых HTTP-запроса, как в add_cases_manually):
1. Целевой поиск по номеру (CourtConfig.search_by_number_url,
   G1_CASE__CASE_NUMBERSS) → строка выдачи без фильтра роли (parse_search_row).
2. Карточка дела (parse_case_card) → build_json_entry.
3. Запись получает маркер track="plaintiff_light" и служебный блок
   import{operator, at, source, announced:true} — announced сразу True:
   иски банка в дайджесте НЕ анонсируются (решение юриста 25.07.2026).
4. Уже решённые дела получают fi.resolved_emitted=True — старые решения
   задним числом в дайджест не льются (защита от паводка, ср. 07.07.2026).

Отчёт (метки в логе):
- [ADDED]          — добавлено в cases_bank.json
- [ALREADY]        — уже отслеживается (активные + архивы + bank-файлы)
- [NOT PLAINTIFF]  — банк в деле не истец (ответчик/третье лицо/не сторона)
- [SUBSIDIARY]     — упомянута только дочка Сбера
- [NO LINK]        — в выдаче нет case_id|case_uid — карточку не открыть
- [NOT FOUND]      — поиск не вернул нужное дело
- [FETCH FAIL]     — сбой загрузки страницы суда
- [UNKNOWN COURT]  — домен не из реестра судов активного региона

Идемпотентен: повторный запуск пропустит уже добавленные дела ([ALREADY]).
Порционный импорт: --limit N добавляет не больше N дел за запуск (лимит
считается по УСПЕШНЫМ добавлениям; пропуски лимит не тратят).

Запуск: python3 scripts/import_bank_registry.py --input ops/bank_registry/registry.csv --limit 200
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.config import log  # noqa: E402
from court_monitor.linking import (  # noqa: E402
    collect_fi_dedup_index,
    is_fi_number_tracked,
)
from court_monitor.netutil import fetch_card_checked, fetch_page, polite_delay  # noqa: E402
from court_monitor.parsing import is_subsidiary_only_case, parse_case_card  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.storage import (  # noqa: E402
    load_json, load_bank_json, save_bank_json,
)
from court_monitor.target_search import (  # noqa: E402
    build_json_entry,
    determine_bank_role,
    parse_search_row,
)

EXIT_OK = 0
EXIT_NO_INPUT = 3


def read_registry(path: str) -> list[tuple[str, str]]:
    """Прочитать реестр «court_domain;case_number», отсеяв мусор и шапку."""
    pairs: list[tuple[str, str]] = []
    with open(path, encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 2 or not parts[0] or not parts[1]:
                log.warning(f"Строка реестра пропущена (не 2 поля): {line!r}")
                continue
            domain, num = parts[0].lower(), parts[1]
            if "sudrf" not in domain and "." not in domain:
                # Шапка «court_domain;case_number» и прочий не-доменный мусор.
                continue
            pairs.append((domain, num))
    return pairs


def load_all_tracked() -> list[dict]:
    """Все известные дела для дедупа: активные + горячий и холодные архивы +
    оба bank-файла. Паттерн main_json/import_search_dump. Для дедупа хватает
    list-файлов (без events) — id и номера лежат в списке."""
    import glob

    bank_cold = [
        p for p in sorted(glob.glob(config.bank_cold_archive_glob()))
        if config.is_bank_cold_archive_file(p)
    ]
    tracked: list[dict] = []
    for path in (
        config.JSON_PATH,
        config.JSON_ARCHIVE_PATH,
        config.JSON_BANK_PATH,
        config.JSON_BANK_ARCHIVE_PATH,
        *sorted(glob.glob(config.cold_archive_glob())),
        *bank_cold,
    ):
        if os.path.exists(path):
            tracked.extend(load_json(path).get("cases", []))
    return tracked


def load_bank_file() -> dict:
    """cases_bank.json (создаётся при первом импорте). Грузим СКЛЕЕННЫМ
    (события из cases_bank_events.json подставлены в записи): save_bank_json
    перезаписывает events-файл целиком, и без склейки события существующих
    дел потерялись бы при дозаписи новых."""
    if os.path.exists(config.JSON_BANK_PATH):
        return load_bank_json(config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    return {"version": 1, "track": "plaintiff_light", "cases": []}


def make_bank_entry(fi_row: dict, card_info: dict, operator: str,
                    now_iso: str, source: str = "bank_registry") -> dict:
    """JSON-запись трека «Иски банка» из поисковой строки + карточки.

    build_json_entry + маркеры трека: track="plaintiff_light",
    import{announced:true} — иски банка в дайджесте не анонсируются
    (решение юриста 25.07.2026); уже решённые получают resolved_emitted=True —
    старые решения задним числом в дайджест не льются. Общая для импортёра
    реестра и разового сборщика выдачи (scripts/collect_bank_claims.py).
    """
    entry = build_json_entry(fi_row, card_info)
    entry["track"] = "plaintiff_light"
    entry["initial_bank_role"] = fi_row.get("bank_role", "Истец")
    entry["import"] = {
        "operator": operator, "at": now_iso,
        "source": source, "announced": True,
    }
    fi = entry["first_instance"]
    if (fi.get("status") or "").strip() in ("Решено", "Возвращено"):
        fi["resolved_emitted"] = True
    # Уже выданные листы переносим в запись сразу — тот же принцип, что
    # resolved_emitted: первый прогон не должен объявить старые ИЛ «новыми»
    # (без переноса FI-цикл эмитнул бы fi_writ_issued задним числом по всем
    # решённым делам пула). События пойдут только на листы, появившиеся
    # ПОСЛЕ постановки на мониторинг.
    if card_info.get("_writs"):
        fi["writs"] = card_info["_writs"]
    return entry


def import_registry(pairs: list[tuple[str, str]], limit: int, operator: str) -> dict:
    """Провести импорт, вернуть счётчики. Сохраняет cases_bank.json сам."""
    courts_by_domain = {c.domain: c for c in get_region().first_instance_courts}
    dedup_exact, dedup_wildcard = collect_fi_dedup_index(load_all_tracked())

    counters = {
        "added": 0, "already": 0, "not_plaintiff": 0, "subsidiary": 0,
        "no_link": 0, "not_found": 0, "fetch_fail": 0, "unknown_court": 0,
    }
    new_entries: list[dict] = []
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    total = len(pairs)
    for i, (domain, case_num) in enumerate(pairs, 1):
        if limit and counters["added"] >= limit:
            log.info(f"Лимит {limit} добавлений достигнут — остальное следующим запуском")
            break
        log.info(f"[{i}/{total}] {domain} / {case_num}")

        court = courts_by_domain.get(domain)
        if not court:
            log.warning("  [UNKNOWN COURT] домен не из реестра судов региона")
            counters["unknown_court"] += 1
            continue

        if is_fi_number_tracked(case_num, domain, dedup_exact, dedup_wildcard):
            log.info("  [ALREADY] уже отслеживается")
            counters["already"] += 1
            continue

        polite_delay()
        html = fetch_page(court.search_by_number_url(case_num), context=case_num)
        if not html:
            log.warning("  [FETCH FAIL] поисковая страница")
            counters["fetch_fail"] += 1
            continue

        fi_row = parse_search_row(html, court, case_num)
        if not fi_row:
            log.warning("  [NOT FOUND] дело не найдено в выдаче")
            counters["not_found"] += 1
            continue

        if is_subsidiary_only_case(fi_row["plaintiff"], fi_row["defendant"]):
            log.info("  [SUBSIDIARY] упомянута только дочка Сбера — пропуск")
            counters["subsidiary"] += 1
            continue

        role = determine_bank_role(fi_row["plaintiff"], fi_row["defendant"])
        if role != "Истец":
            # Трек ведёт только иски самого банка. Ответчик-дела заводит
            # автопоиск, третьи лица и «не сторона» — вне охвата реестра.
            log.info(
                f"  [NOT PLAINTIFF] банк — {role or 'не сторона'}: "
                "в трек исков банка не берём"
            )
            counters["not_plaintiff"] += 1
            continue
        fi_row["bank_role"] = role

        link = fi_row["link"]
        if not link or "|" not in link:
            log.warning("  [NO LINK] нет case_id|case_uid — дело немониторимо")
            counters["no_link"] += 1
            continue
        cid, _, cuid = link.partition("|")

        polite_delay()
        card_html = fetch_card_checked(court.card_url(cid, cuid), context=case_num)
        if not card_html:
            log.warning("  [FETCH FAIL] карточка дела")
            counters["fetch_fail"] += 1
            continue
        card_info = parse_case_card(card_html, court.base_url)

        entry = make_bank_entry(fi_row, card_info, operator, now_iso)
        fi = entry["first_instance"]
        new_entries.append(entry)
        dedup_exact.add((domain, case_num))
        counters["added"] += 1
        log.info(
            f"  [ADDED] статус={fi.get('status', '?')} "
            f"заседание={fi.get('hearing_date') or '—'} "
            f"судья={(fi.get('judge') or '')[:40]!r}"
        )

    if new_entries:
        bank = load_bank_file()
        bank["cases"] = new_entries + bank.get("cases", [])
        save_bank_json(bank, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    else:
        log.info("Нечего добавлять — cases_bank.json не изменён")

    log.info("=" * 60)
    log.info(
        f"Итого: +{counters['added']} новых | {counters['already']} уже в базе | "
        f"{counters['not_plaintiff']} не истец | {counters['subsidiary']} дочки | "
        f"{counters['no_link']} без ссылки | {counters['not_found']} не найдено | "
        f"{counters['fetch_fail']} сбоев | {counters['unknown_court']} неизв. суд"
    )
    return counters


def main() -> int:
    ap = argparse.ArgumentParser(description="Импорт реестра исков банка")
    ap.add_argument("--input", default="ops/bank_registry/registry.csv",
                    help="CSV «court_domain;case_number»")
    ap.add_argument("--limit", type=int, default=0,
                    help="максимум добавлений за запуск (0 — без лимита)")
    ap.add_argument("--operator", default=os.environ.get("GITHUB_ACTOR", "manual"),
                    help="кто запускает импорт (для блока import)")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        log.error(f"Файл реестра не найден: {args.input}")
        return EXIT_NO_INPUT
    pairs = read_registry(args.input)
    if not pairs:
        log.error(f"Реестр пуст: {args.input}")
        return EXIT_NO_INPUT

    log.info(f"Реестр: {len(pairs)} дел, лимит на запуск: {args.limit or 'нет'}")
    import_registry(pairs, args.limit, args.operator)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
