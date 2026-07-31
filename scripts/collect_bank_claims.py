#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Разовый сборщик исков банка с выдачи поиска суда (банк — истец).

Исторический добор пула вглубь выдачи: свежие иски с первой страницы прогон
подхватывает сам (блок 3b фазы 3), этот сборщик берёт уехавшее дальше.
Альтернатива реестру из внутренних систем банка (import_bank_registry.py):
проходит первые N страниц выдачи поиска по «Сбербанк» на сайте одного суда
(пилот — Сургутский городской) и заводит в data/cases_bank.json только иски,
где банк — истец. Исключаются дела с итогами «оставлено без рассмотрения»,
«передано по подсудности», «возвращено», «прекращено» (решение юриста
26.07.2026; «отказано» вносится — по нему возможна апелляция банка).
Также не берутся дела с признаком апелляции/кассации в карточке (первый же
прогон увёл бы их из лёгкого трека в основной cases.json) и дела с уже
выданным ИЛ на исполнение решения — цикл трека по ним пройден; обеспечительные
листы не считаются (решение юриста 30.07.2026, сбор по Нижневартовскому
городскому). Тип листа якорится на дату РЕШЕНИЯ из событий карточки
(fi_decision_date_from_events): «Дата заседания» промахивается в обе стороны —
см. комментарий у фильтра.

Пагинация: формата пейджера sudrf в кодовой базе раньше не было — ссылки
страниц ОБНАРУЖИВАЮТСЯ в HTML выдачи (href с sud_delo и page=N); если
пейджер не найден, пробуем сконструировать &page=N сами. Стоп-защиты:
пустая выдача, сбой загрузки или страница, повторившая предыдущую (сервер
проигнорировал page=), останавливают обход с WARNING.

Строка выдачи уже несёт ссылку на карточку (case_id|case_uid) — целевой
поиск по номеру не нужен: 1 HTTP-запрос на добавляемое дело (карточка).

Метки отчёта:
- [ADDED]            — добавлено в cases_bank.json
- [ALREADY]          — уже отслеживается (активные + архивы + bank-файлы)
- [SKIPPED ROLE]     — банк в деле не истец
- [EXCLUDED RESULT]  — итог из списка исключений юриста
- [EXCLUDED APPEAL]  — жалоба/направление в апелляцию/кассацию — дело трек покинет
- [EXCLUDED WRIT]    — выдан ИЛ на исполнение решения — цикл трека уже пройден
- [NO LINK]          — в выдаче нет case_id|case_uid — карточку не открыть
- [FETCH FAIL]       — сбой загрузки карточки

Запуск:
    python3 scripts/collect_bank_claims.py --court surggor--hmao.sudrf.ru \
        --pages 10 [--limit N] [--dry-run]

Суд резолвится парой (домен, srv_num): на одном домене может жить два суда —
Нижневартовский районный (srv 1) и его постоянное присутствие в Покачи (srv 2).
Для второго обязателен --srv 2, иначе выберется первый суд домена.
"""

from __future__ import annotations

import argparse
import html as html_mod
import os
import re
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.bank_intake import (  # noqa: E402,F401 — ре-экспорт правил приёма
    _EXCLUDED_RESULT_RX,
    card_rejects,
    row_passes,
)
from court_monitor.config import log  # noqa: E402
from court_monitor.linking import (  # noqa: E402
    collect_fi_dedup_index,
    is_fi_number_tracked,
)
from court_monitor.netutil import fetch_card_checked, fetch_page, polite_delay  # noqa: E402
from court_monitor.parsing import parse_case_card  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    detect_captcha_challenge,
    parse_first_instance_search,
)
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.storage import save_bank_json  # noqa: E402
from import_bank_registry import (  # noqa: E402
    load_all_tracked,
    load_bank_file,
    make_bank_entry,
)

EXIT_OK = 0
EXIT_CAPTCHA = 2
EXIT_NO_COURT = 3
EXIT_NO_RESULTS = 4

# Правила приёма (роль, исключаемые итоги, карточные фильтры) — общие для всех
# каналов ввода трека, живут в court_monitor/bank_intake.py; здесь только
# ре-экспорт (см. импорт выше) + пагинация выдачи, которая нужна лишь этому
# разовому сборщику.
_HREF_RX = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
_PAGE_PARAM_RX = re.compile(r"[?&]page=(\d+)")


def discover_page_urls(html: str, base_url: str) -> dict[int, str]:
    """{номер страницы: абсолютный URL} из ссылок пейджера выдачи.

    Берём все href, ведущие в модуль sud_delo с параметром page=N
    (html-сущности разэкранируются: в разметке ссылки идут через &amp;).
    Первая встреченная ссылка на номер побеждает — пейджер повторяется
    сверху и снизу таблицы.
    """
    pages: dict[int, str] = {}
    for m in _HREF_RX.finditer(html):
        href = html_mod.unescape(m.group(1))
        if "sud_delo" not in href:
            continue
        pm = _PAGE_PARAM_RX.search(href)
        if not pm:
            continue
        n = int(pm.group(1))
        if href.startswith("http"):
            url = href
        elif href.startswith("/"):
            url = base_url.rstrip("/") + href
        else:
            url = base_url.rstrip("/") + "/" + href
        pages.setdefault(n, url)
    return pages


def fetch_search_rows(court, pages_limit: int) -> tuple[list[dict], int]:
    """Собрать строки первых pages_limit страниц выдачи. (строки, страниц)."""
    polite_delay()
    html = fetch_page(court.search_url(), context=f"выдача {court.name}, стр. 1")
    if not html:
        return [], 0
    if detect_captcha_challenge(html):
        log.error("Выдача закрыта проверочным кодом — сбор невозможен")
        sys.exit(EXIT_CAPTCHA)

    stats: dict = {}
    rows = parse_first_instance_search(html, court, stats=stats, keep_all_roles=True)
    log.info(
        f"Стр. 1: строк {len(rows)} (сберовских {stats.get('sber_rows', 0)}, "
        f"дочек отсеяно {stats.get('subsidiary_rows', 0)})"
    )
    all_rows = list(rows)
    pager = discover_page_urls(html, court.base_url)
    if pager:
        log.info(f"Пейджер обнаружен: страницы {sorted(pager)}")
    else:
        log.warning(
            "Пейджер в HTML не найден — пробуем конструировать &page=N "
            "(если сервер игнорирует параметр, стоп-защита остановит обход)"
        )
    prev_nums = [r["case_number"] for r in rows]
    pages_done = 1
    for n in range(2, pages_limit + 1):
        url = pager.get(n) or (court.search_url() + f"&page={n}")
        polite_delay()
        page_html = fetch_page(url, context=f"выдача {court.name}, стр. {n}")
        if not page_html:
            log.warning(f"Стр. {n}: не загрузилась — обход остановлен")
            break
        page_rows = parse_first_instance_search(
            page_html, court, keep_all_roles=True
        )
        if not page_rows:
            log.info(f"Стр. {n}: пусто — конец выдачи")
            break
        nums = [r["case_number"] for r in page_rows]
        if nums == prev_nums:
            log.warning(
                f"Стр. {n} повторила предыдущую — сервер игнорирует page=, стоп"
            )
            break
        # Пейджер может показывать «окно» номеров — дособираем ссылки
        # дальних страниц по мере продвижения.
        for k, u in discover_page_urls(page_html, court.base_url).items():
            pager.setdefault(k, u)
        log.info(f"Стр. {n}: строк {len(page_rows)}")
        all_rows.extend(page_rows)
        prev_nums = nums
        pages_done = n

    # Дедуп внутри выдачи: выдача сортируется по дате поступления и может
    # сдвинуться между запросами страниц — одно дело всплывает дважды.
    uniq: dict[str, dict] = {}
    for r in all_rows:
        uniq.setdefault(r["case_number"], r)
    return list(uniq.values()), pages_done


def collect(court, pages_limit: int, limit: int, dry_run: bool, operator: str) -> dict:
    """Обойти выдачу, отфильтровать и завести истцовые дела. Счётчики — в return."""
    rows, pages_done = fetch_search_rows(court, pages_limit)
    counters = {
        "pages": pages_done, "rows": len(rows), "added": 0, "already": 0,
        "role": 0, "excluded_result": 0, "excluded_appeal": 0,
        "excluded_writ": 0, "no_link": 0, "fetch_fail": 0,
    }
    if not rows:
        return counters

    dedup_exact, dedup_wildcard = collect_fi_dedup_index(load_all_tracked())
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    new_entries: list[dict] = []

    for i, r in enumerate(rows, 1):
        num = r["case_number"]
        if limit and counters["added"] >= limit:
            log.info(f"Лимит {limit} добавлений достигнут — остальное следующим запуском")
            break
        ok, why = row_passes(r)
        if not ok:
            counters[why] += 1
            label = {
                "role": f"[SKIPPED ROLE] банк — {r.get('bank_role') or 'не сторона'}",
                "excluded_result": f"[EXCLUDED RESULT] {(r.get('result') or '')[:60]}",
                "no_link": "[NO LINK] нет case_id|case_uid",
            }[why]
            log.info(f"[{i}/{len(rows)}] {num} — {label}")
            continue
        if is_fi_number_tracked(num, court.domain, dedup_exact, dedup_wildcard):
            counters["already"] += 1
            log.info(f"[{i}/{len(rows)}] {num} — [ALREADY] уже отслеживается")
            continue

        cid, _, cuid = r["link"].partition("|")
        polite_delay()
        card_html = fetch_card_checked(court.card_url(cid, cuid), context=num)
        if not card_html:
            counters["fetch_fail"] += 1
            log.warning(f"[{i}/{len(rows)}] {num} — [FETCH FAIL] карточка")
            continue
        card_info = parse_case_card(card_html, court.base_url)
        # Карточные фильтры — общие правила приёма (court_monitor/bank_intake.py):
        # исключаемый итог из карточки, признак апелляции/кассации (исторический
        # сбор такие дела не берёт — в треке они побыли бы мусорным транзитом,
        # решение юриста 30.07.2026), уже выданный ИЛ на исполнение решения.
        why_card = card_rejects(card_info, skip_appeal=True)
        if why_card:
            counters[why_card] += 1
            label = {
                "excluded_result": (
                    "[EXCLUDED RESULT] "
                    f"{(card_info.get('Результат') or '')[:60]} (итог из карточки)"
                ),
                "excluded_appeal": (
                    "[EXCLUDED APPEAL] жалоба/направление в вышестоящую инстанцию"
                ),
                "excluded_writ": "[EXCLUDED WRIT] выдан ИЛ на исполнение решения",
            }[why_card]
            log.info(f"[{i}/{len(rows)}] {num} — {label}")
            continue

        entry = make_bank_entry(r, card_info, operator, now_iso,
                                source="search_sweep", court=court)
        new_entries.append(entry)
        dedup_exact.add((court.domain, num))
        counters["added"] += 1
        fi = entry["first_instance"]
        log.info(
            f"[{i}/{len(rows)}] {num} — [ADDED] статус={fi.get('status', '?')} "
            f"итог={(fi.get('result') or '—')[:40]!r}"
        )

    if new_entries and not dry_run:
        bank = load_bank_file()
        bank["cases"] = new_entries + bank.get("cases", [])
        save_bank_json(bank, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    elif new_entries:
        log.info(f"DRY-RUN: {len(new_entries)} дел НЕ записаны (снимите --dry-run)")

    log.info("=" * 60)
    log.info(
        f"Итого: страниц {counters['pages']} | уникальных строк {counters['rows']} | "
        f"+{counters['added']} добавлено{' (dry-run, без записи)' if dry_run else ''} | "
        f"{counters['already']} уже в базе | {counters['role']} не истец | "
        f"{counters['excluded_result']} исключено по итогу | "
        f"{counters['excluded_appeal']} ушло в апелляцию/кассацию | "
        f"{counters['excluded_writ']} с ИЛ на исполнение | "
        f"{counters['no_link']} без ссылки | {counters['fetch_fail']} сбоев карточек"
    )
    return counters


def resolve_court(domain: str, srv: int):
    """CourtConfig суда 1-й инст. по паре (домен, srv_num); None — не нашёлся.

    Пара, а не один домен: на vartovray--hmao.sudrf.ru живут два суда —
    Нижневартовский районный (srv 1) и его постоянное присутствие в Покачи
    (srv 2), — и резолв по домену всегда отдавал первый, т.е. Покачи собрать
    было нельзя.
    """
    d = (domain or "").strip().lower()
    return next(
        (c for c in get_region().first_instance_courts
         if c.domain == d and c.srv_num == srv),
        None,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Разовый сбор исков банка с выдачи суда")
    ap.add_argument("--court", default="surggor--hmao.sudrf.ru",
                    help="домен суда 1-й инстанции (default: Сургутский городской)")
    ap.add_argument("--srv", type=int, default=1,
                    help="номер сервера суда (двухсерверные домены: Покачи на "
                         "vartovray--hmao.sudrf.ru — srv 2; default 1)")
    ap.add_argument("--pages", type=int, default=10,
                    help="сколько страниц выдачи обойти (default 10)")
    ap.add_argument("--limit", type=int, default=0,
                    help="максимум добавлений за запуск (0 — без лимита)")
    ap.add_argument("--dry-run", action="store_true",
                    help="только отчёт, cases_bank.json не пишется")
    ap.add_argument("--operator", default=os.environ.get("GITHUB_ACTOR", "manual"))
    args = ap.parse_args()

    domain = args.court.strip().lower()
    court = resolve_court(domain, args.srv)
    if not court:
        same_domain = [c for c in get_region().first_instance_courts
                       if c.domain == domain]
        if same_domain:
            log.error(
                f"На домене {domain} нет суда с srv_num={args.srv}; "
                f"доступны: {', '.join(f'{c.srv_num} — {c.name}' for c in same_domain)}"
            )
        else:
            log.error(f"Домен не из реестра судов региона: {args.court}")
        return EXIT_NO_COURT

    log.info(
        f"Сбор исков банка: {court.name} (srv {court.srv_num}), страниц ≤{args.pages}, "
        f"лимит {args.limit or 'нет'}{', DRY-RUN' if args.dry_run else ''}"
    )
    counters = collect(court, args.pages, args.limit, args.dry_run, args.operator)
    if counters["rows"] == 0:
        log.error("Выдача не загрузилась или пуста — сбор не состоялся")
        return EXIT_NO_RESULTS
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
