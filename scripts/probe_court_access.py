#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проба доступа к КАРТОЧКАМ дел с раннера: пускают нас суды или нет.

Зачем отдельно от probe_courts.yml. Прежняя проба дёргала только страницу
ПОИСКА и только захардкоженные хосты ХМАО (env REGION в неё не передавался —
на форке территории она проверяла чужой регион), а вывод печатала в лог рана,
куда без admin-прав не заглянуть. Для капчёвых судов Свердловской области
поиск закрыт ПО ПРОЕКТУ (search_gated), и его отказ вообще не новость: весь
канал мониторинга там — карточки. Спрашивать надо именно их.

Повод (16.08.2026): три импорта подряд — Ленинский, Верх-Исетский и
Академический районные суды ЕКБ — не прочитали ни одной карточки, при том что
с машины юриста те же карточки открываются целиком. Отличить «нас блокируют по
адресу» от «портал лёг» было нечем: 403, страница защиты ГАС «Правосудие» с
HTTP 200, проверочный код и заглушка давали одинаковое «карточка не
прочиталась». Если режут адрес раннера — под ударом весь основной прогон, и это
сценарий «процедуры флипа» на Mac-резерв из CLAUDE.md.

Как устроено:
- цели берутся из ЖИВЫХ данных (data/cases.json активного региона): по одному
  делу на суд, ссылка `case_id|case_uid` — та же, по которой ходит прогон.
  Хардкода доменов нет: реестр территорий меняется, а проба должна следовать
  за ним. Первыми идут суды со search_gated=True — у них карточки и есть весь
  канал;
- запрос — напрямую через netutil.session, НЕ через fetch_page: нужен
  HTTP-код, а raise_for_status отдаёт наружу только исключение;
- классификация — теми же функциями, что и боевой код
  (detect_captcha_challenge_card / looks_like_non_card_page /
  card_is_empty_shell): второй копии правил не заводим;
- со страницы защиты снимается наш адрес и буква правила — ровно тот факт,
  ради которого пробу и делали.

Код выхода ВСЕГДА 0: это диагностика, а не гейт. Отчёт workflow кладёт в
ops/court_probe/report.txt и коммитит — его читают локально через git.

Запуск:
    REGION=sverdlovsk_yanao python3 scripts/probe_court_access.py [--limit 8]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import urlsplit

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.courts import (  # noqa: E402
    APPEAL_COURTS, CASSATION_COURT, fi_court_by_domain,
)
from court_monitor.netutil import block_page_marks, polite_delay, session  # noqa: E402
from court_monitor.parsing.cards import card_is_empty_shell  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    detect_captcha_challenge_card, looks_like_non_card_page,
)
from court_monitor.parsing import parse_case_card  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

DEFAULT_LIMIT = 8

# Вердикты одной цели.
OK = "OK"                    # пришла настоящая карточка
BLOCKED = "БЛОК"             # страница защиты / 403 — нас не пускают
CAPTCHA = "КОД"              # проверочный код на карточке
OUTAGE = "ЗАГЛУШКА"          # портал недоступен
EMPTY = "ОГРЫЗОК"            # карточка без данных (0 таблиц у парсера)
FAIL = "СЕТЬ"                # соединение не состоялось


def collect_targets(cases_path: str, limit: int = DEFAULT_LIMIT) -> list[dict]:
    """Цели пробы из cases.json активного региона: по одному делу на суд.

    Порядок: сначала капчёвые суды (search_gated — у них карточки единственный
    канал), потом обычные. Дело берём первое встреченное: проба спрашивает
    «пускает ли нас домен», конкретика дела значения не имеет. Записи с
    неполной ссылкой (нет `case_id|case_uid`) пропускаем — по ним URL карточки
    не собрать.
    """
    try:
        with open(cases_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []

    by_domain: dict[str, dict] = {}
    for case in data.get("cases", []):
        fi = case.get("first_instance") or {}
        domain = (fi.get("court_domain") or "").strip().lower()
        link = fi.get("link") or ""
        if not domain or "|" not in link or domain in by_domain:
            continue
        court = fi_court_by_domain(domain, fi.get("srv_num"))
        if court is None:                 # дело чужого региона (форк/эталон)
            continue
        cid, _, cuid = link.partition("|")
        if not cid or not cuid:
            continue
        by_domain[domain] = {
            "label": court.name,
            "url": court.card_url(cid, cuid),
            "gated": bool(court.search_gated),
            "case": case.get("id", ""),
        }

    targets = sorted(by_domain.values(), key=lambda t: (not t["gated"], t["label"]))
    return targets[:limit]


def appeal_and_cassation_targets(cases_path: str) -> list[dict]:
    """Апелляция и кассация: та же проба по одной живой карточке каждой.

    Блок стадии может быть пуст (в картотеке нет обжалуемых дел) — тогда цель
    просто не появится, отчёт от этого не ломается.
    """
    try:
        with open(cases_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []

    by_domain = {c.domain.lower(): c for c in APPEAL_COURTS}
    out: list[dict] = []
    seen: set[str] = set()
    for case in data.get("cases", []):
        for block, court in (
            ("appeal", by_domain.get(
                ((case.get("appeal") or {}).get("court_domain") or "").lower())),
            ("cassation", CASSATION_COURT),
        ):
            data_block = case.get(block) or {}
            link = data_block.get("link") or ""
            if court is None or "|" not in link or court.domain in seen:
                continue
            cid, _, cuid = link.partition("|")
            if not cid or not cuid:
                continue
            seen.add(court.domain)
            out.append({
                "label": f"{court.name} ({'апелляция' if block == 'appeal' else 'кассация'})",
                "url": court.card_url(cid, cuid),
                "gated": bool(court.search_gated),
                "case": case.get("id", ""),
            })
    return out


def classify_response(status: int | None, html: str, url: str) -> tuple[str, dict]:
    """Вердикт по ответу + приметы блокировки. Правила — боевые, не свои.

    Порядок проверок важен: 403 отдаётся вместе с телом страницы защиты, и
    приметы (наш адрес, буква правила) надо снять до всякой классификации
    содержимого.
    """
    marks = block_page_marks(html)
    if status is None:
        return FAIL, marks
    if status != 200:
        return BLOCKED if status in (401, 403, 429) else FAIL, marks
    if detect_captcha_challenge_card(html):
        return CAPTCHA, marks
    if looks_like_non_card_page(html, url):
        # Страница защиты и штатная заглушка sudrf идут одним детектором —
        # различает их только текст: у защиты в теле есть НАШ АДРЕС. Именно
        # ip, а не любой mark: _BLOCK_RULE_RE матчит первую латинскую букву в
        # скобках, и «(C) 2006» из футера обычной заглушки перекрасил бы её в
        # БЛОК — а это главный вердикт отчёта (ревью Fable 16.08.2026).
        return (BLOCKED if marks.get("ip") else OUTAGE), marks
    if card_is_empty_shell(parse_case_card(html, url)):
        return EMPTY, marks
    return OK, marks


def probe_target(target: dict) -> dict:
    """Один запрос карточки. Ошибки сети не роняют пробу — это её предмет."""
    status: int | None = None
    html = ""
    try:
        r = session.get(target["url"], timeout=30)
        status = r.status_code
        html = r.content.decode("windows-1251", errors="replace")
    except Exception as e:                       # noqa: BLE001 — любой сбой
        target = {**target, "error": f"{type(e).__name__}: {e}"[:120]}
    verdict, marks = classify_response(status, html, target["url"])
    return {**target, "status": status, "bytes": len(html),
            "verdict": verdict, **marks}


def overall_verdict(results: list[dict]) -> str:
    """Итог пробы одним словом — что докладывать про основной прогон."""
    if not results:
        return "НЕТ ЦЕЛЕЙ"
    verdicts = {r["verdict"] for r in results}
    if verdicts == {OK}:
        return "OK — суды пускают раннер, прогон будет читать карточки"
    if OK not in verdicts:
        if BLOCKED in verdicts:
            return "BLOCKED — суды режут адрес раннера, прогон прочитает НИЧЕГО"
        if CAPTCHA in verdicts:
            return "CAPTCHA — карточки закрыты проверочным кодом"
        if OUTAGE in verdicts:
            return "OUTAGE — портал недоступен (заглушка)"
        return "FAIL — соединение не состоялось"
    return "MIXED — часть судов отвечает, часть нет (смотреть построчно)"


def render_report(results: list[dict], region_code: str) -> str:
    lines = [
        "=" * 72,
        f"Проба доступа к карточкам дел с раннера · регион {region_code}",
        "=" * 72,
    ]
    for r in results:
        gate = " [капчёвый]" if r.get("gated") else ""
        status = r.get("status")
        head = (f"  {r['verdict']:<9} HTTP {status if status else '—':<4} "
                f"{r.get('bytes', 0):>7} б  {r['label']}{gate}")
        lines.append(head)
        detail = []
        if r.get("case"):
            detail.append(f"дело {r['case']}")
        if r.get("ip"):
            detail.append(f"наш адрес {r['ip']}")
        if r.get("rule"):
            detail.append(f"правило ({r['rule']})")
        if r.get("error"):
            detail.append(r["error"])
        if detail:
            lines.append("             " + " · ".join(detail))
        lines.append(f"             {urlsplit(r['url']).netloc}")
    ok = sum(1 for r in results if r["verdict"] == OK)
    lines += [
        "-" * 72,
        f"Карточки читаются: {ok}/{len(results)}",
        f"ИТОГ: {overall_verdict(results)}",
        "",
        ">>> BLOCKED — блок вернулся: см. «Процедура флипа» в CLAUDE.md",
        ">>> OK — суды пускают, отказы импорта были кратковременными",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Проба доступа к карточкам дел")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                    help=f"сколько судов 1-й инст. проверять (дефолт {DEFAULT_LIMIT})")
    ap.add_argument("--cases", default=config.JSON_PATH,
                    help="путь к cases.json (дефолт — боевой)")
    args = ap.parse_args(argv)

    region = get_region()
    targets = (collect_targets(args.cases, args.limit)
               + appeal_and_cassation_targets(args.cases))
    if not targets:
        print(f"Нет целей: в {args.cases} нет дел региона {region.code} "
              "со ссылкой на карточку.")
        return 0

    results = []
    for t in targets:
        polite_delay()
        results.append(probe_target(t))
    print(render_report(results, region.code))
    return 0


if __name__ == "__main__":
    sys.exit(main())
