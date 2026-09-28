"""Точный дослинк кассации в существующем прогоне (без отдельного исполнителя)."""
from __future__ import annotations

from datetime import date
from urllib.parse import urlencode

from court_monitor.complaints import (record_verification, stamp_complaint_tracking,
                                      verification_due, remember_cassation_resolution,
                                      needs_upper_card_lookup)
from court_monitor.courts import CASSATION_COURT, canon_sudrf_domain, fi_card_url
from court_monitor.fi_identity import compare_fi_identity
from court_monitor.linking import link_cassation_cases, retain_historical_cassation, _cassation_card_to_block, _cass_key
from court_monitor.netutil import fetch_card_checked, fetch_page, polite_delay
from court_monitor.parsing.cassation import parse_cassation_card, parse_cassation_search_page
from court_monitor.parsing.search import classify_outage_page, detect_captcha_challenge, is_no_data_page
from court_monitor.textutil import _bare_case_number, parse_date


def identity_search_url(court, *, uid="", number=""):
    params = {"name": "sud_delo", "srv_num": court.srv_num, "name_op": "r",
              "delo_id": court.delo_id, "case_type": 0, "new": court._new_param,
              "delo_table": court._delo_table, "Submit": "Найти"}
    # Имена сверены с формой 7kas: регистр отличается у двух полей.
    params["g33_case__JUDICIAL_UIDSS" if uid else "G33_CASE__CASE_NUMBER_ISS"] = uid or number
    return court.base_url + "/modules.php?" + urlencode(params, encoding="cp1251")


def card_matches_case(case, info, row):
    """Сквозной УИД + конкретное производство, суд и хронология."""
    fi = case.get("first_instance") or {}
    court = info.get("fi_court_config")
    if not court or not info.get("sber_present"):
        return False
    if info.get("page_case_number") != row.get("cassation_internal_number"):
        return False
    incoming = {"court_domain": court.domain, "srv_num": court.srv_num,
                "court": court.name}
    if compare_fi_identity(incoming, fi) != "same":
        return False
    numbers = {_bare_case_number((c.get("first_instance") or {}).get("case_number", ""))
               for c in [case] + (case.get("history") or [])} - {""}
    if _bare_case_number(info.get("fi_case_number", "")) not in numbers:
        return False
    uid = fi.get("judicial_uid") or (case.get("appeal") or {}).get("judicial_uid")
    if uid and info.get("judicial_uid") and uid != info["judicial_uid"]:
        return False
    filed, ended = parse_date(info.get("filing_date") or ""), parse_date(info.get("decision_date") or "")
    if filed and ended and ended < filed:
        return False
    lower = parse_date(info.get("fi_decision_date") or "")
    if filed and lower and lower > filed:
        return False
    # При том же номере дела разбор другого акта не заменяет текущую
    # кассацию: более старые карточки linker сохраняет только в истории.
    return bool(filed or ended)


def lookup_missing_cassations(cases, today=None, court=None):
    today, court = today or date.today(), court or CASSATION_COURT
    stats = {"planned": 0, "parsed": 0, "searched": 0, "linked": 0}
    changes = []
    if not court.enabled or court.search_gated or court.search_disabled:
        return changes, stats
    for case in cases:
        if not verification_due(case, "cassation", today):
            continue
        if not needs_upper_card_lookup(case, "cassation"):
            continue
        fi = case.get("first_instance") or {}
        number = _bare_case_number(fi.get("case_number", ""))
        uid = fi.get("judicial_uid") or (case.get("appeal") or {}).get("judicial_uid")
        queries = ([identity_search_url(court, uid=uid)] if uid else [])
        if number:
            queries.append(identity_search_url(court, number=number))
        if not queries:
            continue
        stats["searched"] += 1
        links = [u for u in [fi_card_url(fi)] if u]
        errors, found, seen = [], False, set()
        for url in queries:
            links.append(url)
            polite_delay()
            html = fetch_page(url, context=f"точный поиск кассации {case.get('id')}")
            rows = parse_cassation_search_page(html) if html else []
            if (not html or detect_captcha_challenge(html) or classify_outage_page(html)
                    or (not rows and not is_no_data_page(html))):
                errors.append("Поиск суда недоступен или ответ не распознан")
                continue
            for row in rows:
                cfg = row.get("fi_court_config")
                if not cfg or canon_sudrf_domain(cfg.domain) != canon_sudrf_domain(fi.get("court_domain")):
                    continue
                link = f"{row['case_id']}|{row['case_uid']}"
                if link in seen:
                    continue
                seen.add(link)
                card_url = court.card_url(row["case_id"], row["case_uid"])
                links.append(card_url)
                stats["planned"] += 1
                polite_delay()
                card_html = fetch_card_checked(card_url, context=row["cassation_internal_number"])
                info = parse_cassation_card(card_html, court.base_url) if card_html else None
                if not info:
                    errors.append("Карточка найденной кассации не прочитана")
                    continue
                stats["parsed"] += 1
                if not card_matches_case(case, info, row):
                    continue
                info.update(link=link, court_domain=court.domain,
                            cassation_internal_number=row["cassation_internal_number"])
                block = _cassation_card_to_block(info)
                key = _cass_key(block["court_domain"], block["case_number"])
                owners = [c for c in cases if c is not case
                          and _cass_key((c.get("cassation") or {}).get("court_domain"),
                                        (c.get("cassation") or {}).get("case_number")) == key
                          and card_matches_case(c, info, row)]
                if owners and block.get("outcome"):
                    if not retain_historical_cassation(case, block):
                        remember_cassation_resolution(case, block, owners[0]["id"])
                    found = True
                    stats["linked"] += 1
                    continue
                # Локально с одним подтверждённым кандидатом: УИД может быть
                # общим у нескольких апелляционных производств в каталоге.
                _, delta, discovered = link_cassation_cases([case], [info])
                if info.get("_link_status") == "needs_review" or discovered:
                    errors.append("Неоднозначная связка производства")
                    continue
                changes.extend(delta)
                found = True
                stats["linked"] += 1
            stamp_complaint_tracking(case)
            state = case["complaint_tracking"]["cassation"]
            if found and state["state"] not in ("active", "resolving"):
                break
        unresolved = any(ep["state"] == "resolving"
                         for ep in case["complaint_tracking"]["cassation"]["episodes"])
        record_verification(case, "cassation", today, success=found and not unresolved,
                            links=links, error="; ".join(dict.fromkeys(errors)) if not found or unresolved else "")
    return changes, stats
