# -*- coding: utf-8 -*-
"""Сохраняемые кандидаты с неоднозначным судом, площадкой или УИД.

Файл региональный, записывается под общим lock исполнителя. Повторный поиск
обновляет ту же запись; подтверждённое добавление/совпадение закрывает её.
Сохраняется только строка дела, без HTML, транспортных токенов и ответов сети.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime

from court_monitor import config
from court_monitor.fi_identity import resolve_fi_identity
from court_monitor.storage import load_json, save_json

REVIEW_REASON = "Не подтверждено совпадение суда, площадки или УИД; требуется проверка"
_ROW_FIELDS = (
    "case_number", "material_number", "court", "court_domain", "court_srv_num",
    "href_srv_num", "srv_num", "judicial_uid", "link", "plaintiff", "defendant", "bank_role",
    "result", "result_date", "status", "judge", "filing_date", "hearing_date", "category",
    "court_delo_id", "delo_id",
)
_pending_resolutions: dict[tuple[str, str], tuple[dict, str]] = {}


def row_identity(row: dict, court=None, card_info: dict | None = None) -> dict:
    fi = {k: row[k] for k in _ROW_FIELDS if row.get(k) not in (None, "")}
    if court is not None:
        fi.setdefault("court_domain", court.domain)
        fi.setdefault("court", court.name)
        fi.setdefault("court_srv_num", fi.get("srv_num", court.srv_num))
    if row.get("href_srv_num") is not None:
        fi["court_srv_num"] = row["href_srv_num"]
    if "court_srv_num" in fi:
        fi["srv_num"] = fi["court_srv_num"]
    if card_info and card_info.get("УИД"):
        fi["judicial_uid"] = card_info["УИД"]
    if row.get("_identity_conflict"):
        fi["_identity_conflict"] = True
    return fi


def _key(row: dict) -> str:
    # УИД может появиться при дочитке; он не меняет идентификатор кандидата.
    ident = resolve_fi_identity(row)
    parts = [config.REGION, ident.domain or row.get("court_domain", ""),
             ident.srv_num, (row.get("case_number") or "").split("(")[0].strip()]
    if not parts[1]:
        parts.append(row.get("court", ""))
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode()).hexdigest()[:24]


def remember_identity_review(row: dict, *, source: str, reason: str = REVIEW_REASON,
                             dry_run: bool = False) -> None:
    if dry_run:
        return
    path = config.FI_IDENTITY_REVIEW_PATH
    data = load_json(path) if os.path.exists(path) else {"version": 1, "items": []}
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    key = _key(row)
    _pending_resolutions.pop((path, key), None)
    item = next((i for i in data.setdefault("items", []) if i.get("key") == key), None)
    if item is None:
        item = {"key": key, "first_seen_at": now}
        data["items"].append(item)
    item.update(status="needs_review", last_seen_at=now, reason=reason,
                source=source, region=config.REGION,
                candidate={**item.get("candidate", {}),
                           **{k: row[k] for k in _ROW_FIELDS if k in row}})
    item.pop("resolved_at", None)
    item.pop("outcome", None)
    save_json(data, path)


def resolve_identity_review(row: dict, *, outcome: str, dry_run: bool = False,
                            persisted: bool = False) -> None:
    path = config.FI_IDENTITY_REVIEW_PATH
    if dry_run or not os.path.exists(path):
        return
    data = load_json(path)
    key = _key(row)
    item = next((i for i in data.get("items", []) if i.get("key") == key), None)
    if item is None or item.get("status") == "resolved":
        return
    pending_key = (path, key)
    if not persisted:
        if outcome != "tracked":
            _pending_resolutions[pending_key] = (dict(row), outcome)
            return
        if pending_key in _pending_resolutions:
            # Повтор строки той же пачки видит индекс ещё не записанного дела.
            return
    item.update(status="resolved", outcome=outcome,
                resolved_at=datetime.now().astimezone().isoformat(timespec="seconds"))
    save_json(data, path)


def flush_identity_reviews() -> None:
    """Закрыть кандидатов только после успешной записи всех картотек пачки."""
    path = config.FI_IDENTITY_REVIEW_PATH
    for key, (row, outcome) in list(_pending_resolutions.items()):
        if key[0] != path:
            continue
        resolve_identity_review(row, outcome=outcome, persisted=True)
        _pending_resolutions.pop(key, None)


def row_tracking_status(row: dict, exact: set, wildcard: set, *, source: str,
                        court=None, card_info: dict | None = None,
                        dry_run: bool = False) -> str:
    from court_monitor.linking import fi_number_tracking_status
    fi = row_identity(row, court, card_info)
    ident = resolve_fi_identity(fi)
    status = ("needs_review" if ident.status != "resolved" else
              fi_number_tracking_status(fi.get("case_number", ""), ident.domain,
                                        exact, wildcard, srv_num=ident.srv_num,
                                        judicial_uid=ident.judicial_uid))
    material = fi.get("material_number")
    if material and material != fi.get("case_number") and status == "free":
        material_status = fi_number_tracking_status(
            material, ident.domain, exact, wildcard,
            srv_num=ident.srv_num, judicial_uid=ident.judicial_uid)
        if material_status == "needs_review":
            status = "needs_review"
    if row.get("_promotion_needs_review"):
        status = "needs_review"
    if status == "needs_review":
        remember_identity_review(fi, source=source, dry_run=dry_run)
    elif status == "tracked":
        resolve_identity_review(fi, outcome="tracked", dry_run=dry_run)
    return status


def add_row_to_index(exact: set, row: dict, *, court=None) -> None:
    fi = row_identity(row, court)
    ident = resolve_fi_identity(fi)
    for number in {fi.get("case_number", ""), fi.get("material_number", "")}:
        if not number:
            continue
        for form in {number, number.split("(")[0].strip()}:
            if hasattr(exact, "add_identity"):
                exact.add_identity(ident.domain, form, fi)
            else:
                exact.add((ident.domain, form))
