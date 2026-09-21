# -*- coding: utf-8 -*-
"""Поиск открытых президиумов: находки для общей кассационной связки.

Данные не сохраняются здесь: main_json передаёт находки link_cassation_cases,
а затем публикует их и включает изменения в обычный дайджест.
"""
from datetime import datetime, date

from court_monitor import config, telemetry
from court_monitor.config import log
from court_monitor.courts import canon_sudrf_domain
from court_monitor.lifecycle import should_skip_case
from court_monitor.netutil import (
    DeferredCardQueue, fetch_page, fetch_card_checked, polite_delay,
    mark_last_fetch_semantic, card_breaker_open, card_breaker_preopen,
)
from court_monitor.parsing import parse_cassation_search_page, parse_cassation_card
from court_monitor.parsing.search import detect_captcha_challenge, classify_outage_page

PRESIDIUM_SINCE = "01.05.2026"


def before_presidium_since(filing_date: str) -> bool:
    """Та же отсечка исторического раздела, что у ручного импорта."""
    try:
        return datetime.strptime((filing_date or "").strip(), "%d.%m.%Y") < datetime.strptime(
            PRESIDIUM_SINCE, "%d.%m.%Y"
        )
    except ValueError:
        return False


def collect_presidium_finds(court, cases, archived_cases, successful_today,
                           health_obs, health_labels, health_captcha, stats=None):
    """Первая страница и карточки; судебный участок не обязан быть в FI-реестре.

    Очередь и её полный знаменатель фиксируются до первого запроса карточки.
    При сбое поиска известные карточки остаются в обычной фазе refresh.
    """
    finds = []
    if stats is None:
        stats = {}
    stats.update(planned=0, parsed=0)
    if not court.enabled or court.search_gated or court.search_disabled:
        return finds
    key = f"cassation:presidium:{court.domain}:total"
    if key in successful_today:
        return finds
    health_labels[key] = court.name
    polite_delay()
    url = court.search_url()
    html = fetch_page(url, context=f"поиск {court.name}")
    if not html:
        if config.FETCH_DIAG.get("kind") != "run_deadline":
            health_obs[key] = None
        return finds
    rows = parse_cassation_search_page(html)
    captcha = not rows and detect_captcha_challenge(html)
    outage = classify_outage_page(html) if not rows else ""
    kind = ("captcha_search" if captcha else
            "waf_search" if outage == "waf_block" else
            "outage_search" if outage else
            "valid_search" if rows else "empty_search")
    mark_last_fetch_semantic(kind, url, context=f"поиск {court.name}", rows=len(rows))
    health_obs[key] = len(rows)
    if captcha:
        health_captcha[key] = court.domain
    if outage:
        card_breaker_preopen(court.domain, kind, reason=str(outage))
    if not rows:
        return finds

    existing = {}
    for case in list(archived_cases) + list(cases):
        blocks = [(h or {}).get("cassation") or {} for h in case.get("history") or []]
        blocks.append(case.get("cassation") or {})
        for block in blocks:
            existing[(canon_sudrf_domain(block.get("court_domain")),
                      block.get("case_number"))] = case
    plan, seen = [], set()
    for row in rows:
        number = row.get("cassation_internal_number")
        identity = (court.domain, number)
        if not number or identity in seen or before_presidium_since(row.get("filing_date")):
            continue
        seen.add(identity)
        known = existing.get(identity)
        if known and (known.get("current_stage") != "cassation"
                      or should_skip_case(known, date.today())[0]):
            continue
        plan.append(row)

    stage = f"presidium_search:{court.domain}"
    parsed = 0
    stats["planned"] = len(plan)
    telemetry.set_coverage(stage, 0, len(plan), processed=0)
    telemetry.register_planned_case_ids(
        "cassation", (f"{court.domain}|{r['cassation_internal_number']}" for r in plan)
    )
    queue = DeferredCardQueue(plan, stage=stage)
    try:
        for work in queue:
            row = work.value
            if not queue.allows(court.domain):
                queue.defer(work, court.domain)
                continue
            number = row["cassation_internal_number"]
            card_url = court.card_url(row["case_id"], row["case_uid"])
            polite_delay()
            queue.mark_attempted(work)
            card_html = fetch_card_checked(card_url, context=number, breaker_gate=False)
            if not card_html:
                if card_breaker_open(court.domain) and queue.defer(work, court.domain):
                    continue
                queue.finish(work, recovered=False)
                continue
            info = parse_cassation_card(card_html, court.base_url)
            if not info:
                mark_last_fetch_semantic("unparsed_card", card_url, context=number)
                queue.finish(work, recovered=False)
                continue
            parsed += 1
            stats["parsed"] = parsed
            queue.finish(work, recovered=True)
            telemetry.mark_case_read("cassation", f"{court.domain}|{number}")
            if not info.get("sber_present"):
                log.info("%s: %s — в участниках нет ПАО Сбербанк", court.name, number)
                continue
            info.update(link=f"{row['case_id']}|{row['case_uid']}",
                        cassation_internal_number=number, court_domain=court.domain)
            for field in ("cassation_number", "fi_case_number", "fi_court_long",
                          "fi_judge", "cassator", "category", "filing_date", "result_text"):
                if not info.get(field) and row.get(field):
                    info[field] = row[field]
            if row.get("fi_magistrate"):
                info["fi_magistrate"] = True
            finds.append(info)
    finally:
        queue.checkpoint()
        telemetry.set_coverage(stage, parsed, len(plan), processed=len(plan),
                               breaker_skipped=len(queue.unresolved_unrequested()))
        log.info("%s: выдача %s строк, карточки %s/%s, находок банка %s",
                 court.name, len(rows), parsed, len(plan), len(finds))
    return finds
