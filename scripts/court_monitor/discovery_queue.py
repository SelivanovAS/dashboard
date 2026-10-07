"""Долговечная дочитка новых строк выдачи; не заменяет правила приёма дел.

Очередь живёт в metadata cases.json. Карточку читает вызывающий код своим
обычным парсером и проверяет роль банка, территорию и допустимость приёма.
Известные производства в очередь не ставятся. Pending не имеет TTL: уход
строки с первой страницы не является причиной забыть найденное дело.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import json
from zoneinfo import ZoneInfo

from court_monitor import config
from court_monitor.courts import canon_sudrf_domain
from court_monitor.regions import get_region
from court_monitor.storage import load_json, save_json
from court_monitor.textutil import case_id_uid

FIELD = "discovery_queue"
VERSION = 1
KINDS = frozenset({"bank", "cassation", "presidium"})
RETRY_SECONDS = 1800
MAX_DAILY_ATTEMPTS = 2
ACCEPTED_RETENTION_DAYS = 30


def _now(now=None):
    value = now or datetime.now(ZoneInfo(get_region().timezone))
    return value if value.tzinfo else value.replace(tzinfo=ZoneInfo(get_region().timezone))


def _stamp(now=None):
    return _now(now).isoformat(timespec="seconds")


def tasks(data):
    state = data.setdefault(FIELD, {"version": VERSION, "items": {}})
    return state.setdefault("items", {})


def scope(kind, court):
    if kind not in KINDS:
        raise ValueError("Неизвестный вид очереди обнаружения")
    return (config.REGION, kind, canon_sudrf_domain(court.domain),
            int(court.delo_id), int(court.srv_num or 1))


def key(kind, court, row):
    identity = list(scope(kind, court))
    identity[4] = int(row.get("href_srv_num") or row.get("court_srv_num")
                      or row.get("srv_num") or identity[4])
    number = str(row.get("case_number") or row.get("cassation_internal_number") or "").strip()
    _, uid = case_id_uid(row.get("link") or "")
    uid = str(row.get("case_uid") or uid or "").strip()
    if not number and not uid:
        raise ValueError("У кандидата отсутствуют номер и UID карточки")
    return json.dumps(identity + [number, uid], ensure_ascii=False, separators=(",", ":"))


def _row_dump(row):
    result = deepcopy({k: v for k, v in row.items() if k != "fi_court_config"})
    fi = row.get("fi_court_config")
    if fi:
        result["_queue_fi_court"] = {"domain": fi.domain, "srv_num": fi.srv_num}
    # Fail closed: непереносимое поле нельзя молча выбросить и потерять
    # часть доказательств территории/приёма при следующем запуске.
    json.dumps(result, ensure_ascii=False)
    return result


def restore_row(task):
    row = deepcopy(task["row"])
    saved = row.pop("_queue_fi_court", None)
    if saved:
        region = get_region()
        courts = list(region.first_instance_courts) + list(region.appeal_courts)
        row["fi_court_config"] = next((c for c in courts
            if canon_sudrf_domain(c.domain) == canon_sudrf_domain(saved["domain"])
            and int(c.srv_num or 1) == int(saved.get("srv_num") or 1)), None)
    return row


def _belongs(task, kind, court):
    expected = scope(kind, court)
    return (task.get("region"), task.get("kind"), task.get("court_domain"),
            task.get("delo_id"), task.get("source_srv_num", task.get("srv_num"))) == expected


def target_court(kind, court, row):
    """Площадка href — адрес карточки, source court — владелец её очереди."""
    region = get_region()
    candidates = (list(region.first_instance_courts) if kind == "bank" else
                  [region.cassation_court] + list(region.presidium_courts))
    target_srv = int(row.get("href_srv_num") or row.get("court_srv_num")
                     or row.get("srv_num") or court.srv_num or 1)
    return next((candidate for candidate in candidates
        if candidate.enabled and candidate.delo_id == court.delo_id
        and canon_sudrf_domain(candidate.domain) == canon_sudrf_domain(court.domain)
        and int(candidate.srv_num or 1) == target_srv), None)


def checkpoint(data):
    """Пишет только очередь: объединённый bank-трек не попадает в cases.json."""
    disk = load_json(config.JSON_PATH)
    disk[FIELD] = deepcopy(data.get(FIELD) or {"version": VERSION, "items": {}})
    save_json(disk, config.JSON_PATH)


def enqueue(data, kind, court, rows, *, known=None, now=None, persist=checkpoint):
    """Сохранить все неизвестные строки ДО первого HTTP/проверки лимита.

    known(row) проверяет существующие дела и архивы по судебной идентичности.
    Территориальный фильтр кассационной выдачи выполняется ДО этого вызова.
    """
    items, stamp = tasks(data), _stamp(now)
    added, changed = [], False
    for row in rows:
        task_key = key(kind, court, row)
        old = items.get(task_key)
        if known and known(row):
            if old and old.get("status") == "pending":
                old.update(status="accepted", reason="already_tracked", updated_at=stamp,
                           completed_at=stamp)
                changed = True
            continue
        if old:
            old["last_seen_at"] = stamp
            # Обновлённая выдача может исправить ссылку/реквизиты, но не
            # снимает cooldown и не оживляет терминальный отказ.
            if old.get("status") == "pending":
                old["row"] = _row_dump(row)
            changed = True
            continue
        region, source, domain, delo_id, srv = scope(kind, court)
        srv = int(row.get("href_srv_num") or row.get("court_srv_num")
                  or row.get("srv_num") or srv)
        items[task_key] = dict(key=task_key, region=region, kind=source,
            court_domain=domain, delo_id=delo_id, srv_num=srv,
            source_srv_num=int(court.srv_num or 1),
            row=_row_dump(row), status="pending", reason="discovered",
            created_at=stamp, last_seen_at=stamp, updated_at=stamp,
            attempt_day="", attempt_count=0, next_attempt_at="")
        added.append(task_key)
        changed = True
    if changed:
        persist(data)
    return added


def reconcile(data, kind, court, known, *, now=None, persist=checkpoint):
    """Восстановить accepted, если карточка не пережила финальный save.

    Вызывать по загруженным существующим делам/архивам перед обработкой
    очереди. accepted хранит исходную строку, поэтому авария между приёмом
    в память и сохранением баз не теряет кандидата навсегда.
    """
    changed, stamp = False, _stamp(now)
    for task_key, task in list(tasks(data).items()):
        if not _belongs(task, kind, court) or task.get("status") == "rejected":
            continue
        exists = known(restore_row(task))
        if exists and task.get("status") == "pending":
            task.update(status="accepted", reason="already_tracked", updated_at=stamp,
                        completed_at=stamp)
            changed = True
        elif not exists and task.get("status") == "accepted":
            task.update(status="pending", reason="admission_not_committed", updated_at=stamp)
            changed = True
        elif exists and task.get("status") == "accepted":
            # Удалять разрешено лишь после новой проверки реальной базы.
            # Неподтверждённый accepted должен быть восстановлен, а pending
            # и терминальные отказы не очищаются по возрасту.
            completed = task.get("completed_at") or task.get("updated_at")
            try:
                age = (_now(now) - _now(datetime.fromisoformat(completed))).days
            except (ValueError, TypeError):
                age = 0
            if age >= ACCEPTED_RETENTION_DAYS:
                del tasks(data)[task_key]
                changed = True
    if changed:
        persist(data)


def retry_reason(task, now=None):
    now = _now(now)
    if task.get("status") != "pending":
        return task.get("status") or "invalid_status"
    if task.get("attempt_day") == now.date().isoformat():
        if int(task.get("attempt_count") or 0) >= MAX_DAILY_ATTEMPTS:
            return "daily_retry_limit"
    raw = task.get("next_attempt_at")
    if raw:
        try:
            if _now(datetime.fromisoformat(raw)) > now:
                return "retry_cooldown"
        except (ValueError, TypeError):
            return "invalid_retry_date"
    return ""


def has_candidates(data, kind, court):
    return any(_belongs(t, kind, court) and t.get("status") != "rejected"
               for t in tasks(data).values())


def due_rows(data, kind, court, *, now=None, persist=checkpoint):
    if not court.enabled:
        return []
    result = []
    changed = False
    for task in sorted(tasks(data).values(), key=lambda t: (t.get("created_at", ""), t["key"])):
        if _belongs(task, kind, court) and not retry_reason(task, now):
            row = restore_row(task)
            if target_court(kind, court, row) is None:
                task.update(status="rejected", reason="unknown_court_site", updated_at=_stamp(now))
                changed = True
                continue
            # Сменившийся реестр не превращает ранее региональную строку
            # кассации в разрешение импортировать чужое дело.
            if kind == "cassation" and not row.get("fi_court_config"):
                task.update(status="rejected", reason="territory_unresolved",
                            updated_at=_stamp(now))
                changed = True
                continue
            result.append(row)
    if changed:
        persist(data)
    return result


def begin_attempt(data, task_key, *, now=None, persist=checkpoint):
    task = tasks(data).get(task_key)
    if task is None:
        return True  # известные карточки не подчиняются discovery-очереди
    if retry_reason(task, now):
        return False
    now = _now(now)
    day = now.date().isoformat()
    count = int(task.get("attempt_count") or 0) if task.get("attempt_day") == day else 0
    task.update(attempt_day=day, attempt_count=count + 1,
        last_attempt_at=_stamp(now), updated_at=_stamp(now), reason="attempting",
        next_attempt_at=_stamp(now + timedelta(seconds=RETRY_SECONDS)))
    persist(data)
    return True


def defer(data, task_key, reason, *, now=None, persist=checkpoint):
    task = tasks(data).get(task_key)
    if task is None or task.get("status") != "pending":
        return
    task.update(reason=reason, updated_at=_stamp(now))
    persist(data)


def finish(data, task_key, *, accepted, reason, now=None, persist=checkpoint):
    task = tasks(data).get(task_key)
    if task is None:
        return
    status = "accepted" if accepted else "rejected"
    if task.get("status") == status:
        return  # не теряем конкретную причину отказа из-за cached_rejection
    task.update(status=status, reason=reason,
                updated_at=_stamp(now), completed_at=_stamp(now))
    persist(data)


def known_cassation(row, court, cases):
    number = row.get("cassation_internal_number") or row.get("case_number")
    domain = canon_sudrf_domain(court.domain)
    for case in cases:
        for production in [case] + list(case.get("history") or []):
            block = (production or {}).get("cassation") or {}
            if (block.get("case_number") == number
                    and canon_sudrf_domain(block.get("court_domain")) == domain
                    and int(block.get("srv_num") or 1) == int(court.srv_num or 1)):
                return True
    return False


def finish_cassation_finds(data, kind, court, finds, cases, *, persist=checkpoint):
    for info in finds:
        task_key = info.get("_discovery_queue_key")
        if not task_key:
            continue
        if known_cassation(info, court, cases):
            finish(data, task_key, accepted=True, reason="linked", persist=persist)
        elif info.get("_link_status") in ("needs_review", "historical"):
            finish(data, task_key, accepted=False,
                   reason=info["_link_status"], persist=persist)
        else:
            defer(data, task_key, "not_linked", persist=persist)


def report(data, today=None, *, now=None):
    """Сводка очереди без сторон/содержимого актов для существующего health."""
    moment = _now(now)
    if today is not None and moment.date() != today:
        moment = moment.replace(year=today.year, month=today.month, day=today.day)
    result = dict(date=moment.date().isoformat(), region=config.REGION,
        pending=0, due=0, accepted=0, rejected=0, needs_review=0,
        missing_link=0, capped=0, budget_blocked=0, daily_limited=0, items=[])
    review_reasons = {"needs_review", "territory_unresolved", "unknown_court_site", "not_linked"}
    for task in sorted(tasks(data).values(), key=lambda t: (t.get("created_at", ""), t["key"])):
        if task.get("region") != config.REGION:
            continue
        row, status = task.get("row") or {}, task.get("status") or "pending"
        reason = task.get("reason") or ""
        blocked_by = retry_reason(task, moment) if status == "pending" else ""
        cid, uid = case_id_uid(row.get("link") or "")
        has_link = bool((cid or row.get("case_id")) and (uid or row.get("case_uid")))
        if status in ("pending", "accepted", "rejected"):
            result[status] += 1
        if status == "pending":
            result["due"] += int(not blocked_by and has_link)
            result["missing_link"] += int(not has_link)
            result["capped"] += int(reason in ("run_intake_cap", "per_court_intake_cap"))
            result["budget_blocked"] += int(reason in ("run_deadline", "run_budget"))
            result["daily_limited"] += int(blocked_by == "daily_retry_limit")
        result["needs_review"] += int(reason in review_reasons and status != "accepted")
        result["items"].append(dict(key=task["key"], kind=task.get("kind"),
            court=task.get("court_domain"), delo_id=task.get("delo_id"),
            srv_num=task.get("srv_num"), source_srv_num=task.get("source_srv_num", task.get("srv_num")),
            number=row.get("case_number") or row.get("cassation_internal_number") or "",
            status=status, reason=reason, blocked_by=blocked_by,
            missing_link=not has_link, attempt_count=task.get("attempt_count", 0),
            attempt_day=task.get("attempt_day", ""), last_attempt_at=task.get("last_attempt_at", ""),
            next_attempt_at=task.get("next_attempt_at", "")))
    return result
