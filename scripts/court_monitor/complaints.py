"""Текущее обжалование отдельно от исторических флагов подачи.

Состояние выводится из отдельных жалоб и дат соответствующего рассмотрения.
Проверки неизвестного результата хранятся между запусками; сетевой отказ
не считается отрицательным результатом поиска.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, timedelta

from court_monitor.textutil import parse_date

_FILED = re.compile(r"регистрац\w*\s+жалоб|дата поступления жалобы", re.I)
_SENT = re.compile(r"направлен\w+.{0,40}(?:вышестоящ|кассационн|апелляционн)", re.I)
_CLOSED = re.compile(r"дата рассмотрения жалобы|жалоб\w*.{0,30}рассмотрен|рассмотрен\w*.{0,30}жалоб|возвращен\w*.{0,25}в суд", re.I)
_WITHDRAWN = re.compile(r"жалоб\w*.{0,40}(?:возвращен|отозван)|(?:возвращен|отозван)\w*.{0,40}жалоб|прекращен\w*.{0,30}производств", re.I)
_RETURNED = re.compile(r"возвращен\w*.{0,20}из вышестоящ", re.I)


def _d(value):
    return parse_date(value or "")


def _latest(values):
    return max((v for v in values if _d(v)), key=_d, default="")


def _episodes(fi, kind):
    groups = {}
    events = fi.get(kind + "_events") or []
    for ev in events:
        groups.setdefault(ev.get("complaint_id") or "legacy", []).append(ev)
    if not groups and any(fi.get(k) for k in (
        kind + "_filed", kind + "_filed_date", "sent_to_" + kind,
        "sent_to_" + kind + "_date",
    )):
        groups["legacy"] = []
    out = []
    for identity, rows in groups.items():
        filed = _latest(ev.get("date") for ev in rows if _FILED.search(ev.get("text", "")))
        sent = _latest(ev.get("date") for ev in rows if _SENT.search(ev.get("text", "")))
        if identity == "legacy":
            filed = filed or fi.get(kind + "_filed_date", "")
            sent = sent or fi.get("sent_to_" + kind + "_date", "")
        closed = _latest(ev.get("date") for ev in rows if _CLOSED.search(ev.get("text", "")))
        returned = _latest(ev.get("date") for ev in rows if _RETURNED.search(ev.get("text", "")))
        withdrawn = _latest(ev.get("date") for ev in rows if _WITHDRAWN.search(ev.get("text", "")))
        # Старые данные без номера жалобы: более поздняя регистрация означает
        # новое обжалование. Закрытие предыдущей жалобы его не гасит.
        if _d(filed) and _d(closed) and _d(closed) < _d(filed):
            closed = ""
        if _d(filed) and _d(withdrawn) and _d(withdrawn) < _d(filed):
            withdrawn = ""
        if _d(filed) and _d(returned) and _d(returned) < _d(filed):
            returned = ""
        out.append({"id": identity, "filed_date": filed, "sent_date": sent,
                    "completed_date": closed or withdrawn or returned,
                    "completion_source": "returned" if not (closed or withdrawn) and returned else "decision",
                    "withdrawn": bool(withdrawn)})
    return out


def _upper_matches(ep, block, kind):
    """Одного УИД недостаточно: результат должен относиться к этой жалобе."""
    if not block.get("case_number"):
        return False
    received = _d(block.get("filing_date"))
    filed = _d(ep["filed_date"] or ep["sent_date"])
    finished = _d(block.get("decision_date") or block.get("hearing_date"))
    closed = _d(ep["completed_date"])
    if filed and received and received < filed:
        return False
    if filed and finished and finished < filed:
        return False
    if closed and finished and (finished > closed or (ep.get("completion_source") != "returned" and closed != finished)):
        return False
    # Без дат нельзя погасить новую жалобу старой карточкой.
    return bool((received and filed) or (closed and finished))


def complaint_state(case, kind):
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}
    episodes = _episodes(fi, kind)
    references = ((case.get("complaint_tracking") or {}).get(kind) or {}).get("resolved_cases") or []
    upper = [case.get(kind) or {}] + [h.get(kind) or {} for h in case.get("history") or []] + references
    anchor = _d(ap.get("filing_date") or ap.get("hearing_date")) if kind == "cassation" else None
    cancellation = fi.get("default_cancellation") or {}
    cancelled = _d(cancellation.get("outcome_date")) if cancellation.get("outcome") == "cancelled" else None
    for ep in episodes:
        filed = _d(ep["filed_date"] or ep["sent_date"])
        completed = _d(ep["completed_date"])
        ep["state"] = "active"
        if ep["withdrawn"]:
            ep["state"] = "completed"
        elif completed:
            ep["state"] = "resolving"
        if kind == "appeal" and cancelled and filed and filed <= cancelled:
            ep["state"] = "historical"
        for block in upper:
            if not _upper_matches(ep, block, kind):
                continue
            terminal = block.get("outcome") not in (None, "", "cassation_other") if kind == "cassation" else bool(block.get("result"))
            if terminal:
                ep["state"] = "completed"
                ep["completed_date"] = block.get("decision_date") or block.get("hearing_date") or ep["completed_date"]
                ep["case_number"] = block["case_number"]
                ep["outcome"] = block.get("outcome") or block.get("result")
                break
        if kind == "appeal" and ap.get("case_number"):
            appeal_received = _d(ap.get("filing_date"))
            if completed and appeal_received and completed < appeal_received:
                ep["state"] = "historical"
                ep["historical"] = True
        # Жалоба подана/рассмотрена до поступления НОВОЙ апелляции.
        # Результат старой кассации ещё можно уточнять, но бейдж новой
        # апелляции уже не должен утверждать, что её акт обжалован.
        if anchor and ((completed and completed < anchor) or (filed and filed < anchor)):
            ep["historical"] = True
            if ep["state"] in ("active", "completed"):
                ep["state"] = "historical"
    states = {ep["state"] for ep in episodes}
    state = next((s for s in ("active", "resolving", "completed", "historical") if s in states), "none")
    # Отпечаток только судебных сведений, без даты нашего чтения и попыток.
    evidence_events = [{k: ev.get(k) for k in ("date", "text", "result_event", "complaint_id")}
                       for ev in fi.get(kind + "_events") or []]
    evidence_upper = [{k: b.get(k) for k in ("case_number", "filing_date", "decision_date", "review_result", "outcome")}
                      for b in upper if b.get("case_number")]
    evidence = json.dumps([kind, episodes, ap.get("case_number"), ap.get("filing_date"),
                           fi.get("decision_date"), cancellation, evidence_events, evidence_upper], ensure_ascii=False, sort_keys=True)
    return {"state": state, "episodes": episodes,
            "evidence_key": hashlib.sha256(evidence.encode()).hexdigest()[:20]}


def stamp_complaint_tracking(case):
    old = case.get("complaint_tracking") or {}
    new = {}
    for kind in ("appeal", "cassation"):
        item = complaint_state(case, kind)
        previous = old.get(kind) or {}
        if previous.get("resolved_cases"):
            item["resolved_cases"] = previous["resolved_cases"]
        if previous.get("evidence_key") == item["evidence_key"] and previous.get("verification"):
            item["verification"] = previous["verification"]
            if item["state"] == "resolving" and previous["verification"].get("attempts", 0) >= 3:
                item["state"] = "needs_review"
        new[kind] = item
    if new == old or (not old and not any(x["episodes"] for x in new.values())):
        return False
    case["complaint_tracking"] = new
    return True


def remember_cassation_resolution(case, block, source_case_id):
    """Ссылка на уже учтённое производство общего дела, без смены апелляции."""
    from court_monitor.courts import cassation_card_url
    target = case.setdefault("complaint_tracking", {}).setdefault("cassation", {})
    refs = target.setdefault("resolved_cases", [])
    item = {k: block.get(k, "") for k in ("case_number", "court_domain", "link", "filing_date", "decision_date", "outcome")}
    item.update(source_case_id=source_case_id, url=cassation_card_url(block))
    refs[:] = [r for r in refs if (r.get("court_domain"), r.get("case_number"))
               != (item["court_domain"], item["case_number"])] + [item]
    stamp_complaint_tracking(case)


def has_current_complaint(case, kind):
    # Чистый расчёт нужен и до миграции/приёма данных; sticky-флаги не стираем.
    return complaint_state(case, kind)["state"] in ("active", "resolving")


def has_unresolved_complaint(case, kind):
    """В архив нельзя скрыть и жалобу прошлого рассмотрения без итога."""
    item = complaint_state(case, kind)
    return item["state"] in ("active", "resolving") or (
        kind == "cassation" and any(ep["state"] == "historical"
                                    and not ep.get("outcome") and not ep.get("withdrawn")
                                    for ep in item["episodes"]))


def needs_upper_card_lookup(case, kind):
    """Известная карточка одной жалобы не закрывает поиск другой."""
    item = complaint_state(case, kind)
    references = ((case.get("complaint_tracking") or {}).get(kind) or {}).get("resolved_cases") or []
    blocks = [case.get(kind) or {}] + [h.get(kind) or {} for h in case.get("history") or []] + references
    for ep in item["episodes"]:
        if ep.get("outcome") or ep.get("withdrawn") or (kind == "appeal" and ep["state"] == "historical"):
            continue
        if ep["state"] == "resolving" or not any(_upper_matches(ep, b, kind) for b in blocks):
            return True
    return False


def verification_due(case, kind, today):
    stamp_complaint_tracking(case)
    item = (case.get("complaint_tracking") or {}).get(kind)
    if not item:
        return False
    if not item["episodes"] or item["state"] in ("none", "completed", "needs_review"):
        return False
    if all(ep.get("outcome") or (ep["state"] == "historical" and kind == "appeal")
           or ep.get("withdrawn") for ep in item["episodes"]):
        return False
    check = item.get("verification") or {}
    next_at = check.get("next_attempt_at")
    return not next_at or today.isoformat() >= next_at


def record_verification(case, kind, today, *, success, links, error=""):
    stamp_complaint_tracking(case)
    item = case["complaint_tracking"][kind]
    check = item.setdefault("verification", {"attempts": 0})
    check.update(last_attempt_at=today.isoformat(), links=list(dict.fromkeys(links)))
    if error:
        check["last_error"] = error
        check["next_attempt_at"] = (today + timedelta(days=1)).isoformat()
        return
    check.pop("last_error", None)
    if success:
        check.update(reason="", resolved=True)
        check.pop("next_attempt_at", None)
        return
    # Три попытки ограничивают только подтверждённо ЗАВЕРШЁННУЮ жалобу.
    # Пока жалоба рассматривается, отсутствие карточки не означает отказ.
    if any(ep["state"] == "resolving" for ep in item["episodes"]):
        check["attempts"] = min(check["attempts"] + 1, 3)
        check["reason"] = "Жалоба рассмотрена, результат вышестоящего суда не подтверждён"
        if check["attempts"] >= 3 and item["state"] != "active":
            item["state"] = "needs_review"
            check.pop("next_attempt_at", None)
            return
    check["next_attempt_at"] = (today + timedelta(days=7)).isoformat()
