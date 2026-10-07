"""Поздние тексты первой инстанции независимо от стадии, трека и архива.

Первое наблюдение новой очереди — исходное состояние, а не новость. Старый
act_absent_checked_at не подтверждает отсутствие текста после её внедрения.
Событие живёт на диске до записи соответствующего контекста дайджеста.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import glob
import json
import os
import re
from urllib.parse import urlencode, urlparse

from court_monitor import act_publication, act_watch_policy as policy, config, telemetry
from court_monitor.courts import FIRST_INSTANCE_COURTS
from court_monitor.lifecycle import (
    bank_side_outcome_fi, classify_verdict_fi, fi_decision_date_from_events,
)
from court_monitor.parsing.cards import parse_case_card, card_is_empty_shell, _warn_if_card_degraded
from court_monitor.storage import load_json, save_json
from court_monitor.textutil import case_id_uid, parse_date

FIELD = 'fi_act_watch'
PENDING = 'pending_fi_act_changes'
BASELINE_FIELD = 'fi_act_watch_baseline_version'
BASELINE_VERSION = 1
ACT_CHECKED_FIELD = 'fi_act_checked_at'
PUBLICATION_FIELDS = ('act_text', 'act_published', 'act_date', BASELINE_FIELD,
                      ACT_CHECKED_FIELD) + act_publication.FIELDS


def number(block):
    match = re.match(r'\s*([\w-]+/\d{4})', block.get('case_number') or '')
    return match.group(1) if match else (block.get('case_number') or '').strip()


def decision_date(block):
    return (block.get('decision_date') or block.get('act_decision_date')
            or fi_decision_date_from_events(block.get('events') or []) or '')


def identity(block):
    """Новый акт того же номера/карточки — отдельное обязательство."""
    day = parse_date(decision_date(block))
    court = _court(block)
    return '|'.join((
        (block.get('court_domain') or '').strip().lower(),
        str(block.get('delo_id') or (court.delo_id if court else '')),
        str(block.get('srv_num') or 1),
        number(block), day.date().isoformat() if day else 'undated',
    ))


def has_text(block):
    return bool((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source'))


def pending(block):
    return bool(not has_text(block) and (decision_date(block)
                or block.get('status') == 'Решено' or block.get('act_summary_needs_source')))


def productions(cases):
    for case in cases:
        yield case, case
        for episode in case.get('history') or []:
            if isinstance(episode, dict):
                yield case, episode


def _court(block):
    domain = (block.get('court_domain') or '').strip().lower()
    srv = str(block.get('srv_num') or 1)
    return next((c for c in FIRST_INSTANCE_COURTS
                 if c.domain == domain and str(c.srv_num) == srv), None)


def card_url(block):
    """Суд и площадка должны принадлежать реестру территории."""
    court = _court(block)
    cid, uid = case_id_uid(block.get('link') or '')
    if court is None or not cid or not uid:
        return ''
    delo = block.get('delo_id') or court.delo_id
    return court.base_url + '/modules.php?' + urlencode(dict(
        name='sud_delo', srv_num=block.get('srv_num') or 1, name_op='case', case_id=cid,
        case_uid=uid, delo_id=delo, new=getattr(court, 'new', 0) or 0))


def _reset_legacy_absence(block):
    if block.get(BASELINE_FIELD) != BASELINE_VERSION:
        return block.pop('act_absent_checked_at', None)
    return None


def observe(block, text, today, *, present, confirmed_date='', source_url=''):
    """Вход и для основного FI-обхода: только свежее отсутствие даёт новость."""
    _reset_legacy_absence(block)
    notify = act_publication.observe(block, text, today, present=present,
                                     confirmed_date=confirmed_date, source_url=source_url)
    # Вызывается исключительно после успешного чтения, включая чтение без
    # документа. Ссылка без доступного текста отрицательного наблюдения не даёт.
    block[BASELINE_FIELD] = BASELINE_VERSION
    block[ACT_CHECKED_FIELD] = today.isoformat()
    return notify


def _checked_at(block):
    # Полное чтение FI включает проверку текста, но чтение только текста
    # не подтверждает обработку движения дела, жалоб и исполнительных листов.
    return max(str(block.get('last_checked_at') or ''),
               str(block.get(ACT_CHECKED_FIELD) or ''))


def next_check(block, today):
    clock_block = dict(block, last_checked_at=_checked_at(block))
    return policy.next_check(clock_block, today, decision_date, identity(block))


def _prepare(task, today, key):
    policy.prepare(task, today, decision_date, key)
    if task['status'] == 'waiting':
        due = next_check(task['block'], today)
        task['next_check_at'] = due.isoformat() if due else ''


def _queue_event(data, task):
    if task.get('previously_announced'):
        return
    change = event(task)
    pending_changes = data.setdefault(PENDING, [])
    if change not in pending_changes:
        pending_changes.append(change)


def sync(data, cases, today):
    tasks = data.setdefault(FIELD, {})
    for parent, episode in productions(cases):
        block = episode.get('first_instance') or {}
        if not block:
            continue
        key = identity(block)
        task = tasks.get(key)
        if task is None and parse_date(decision_date(block)):
            # Дата появилась у прежде неполной записи. Это та же задача,
            # а не новое обязательство рядом с вечным missing_decision_date.
            undated = key.rsplit('|', 1)[0] + '|undated'
            candidate = tasks.get(undated)
            if candidate and candidate.get('reason') == 'missing_decision_date':
                task = tasks[key] = tasks.pop(undated)
                task.update(status='waiting', reason='')
                task['block']['decision_date'] = decision_date(block)
        # Не доверяем старым отрицательным наблюдениям даже у ещё нерешённых
        # дел: основной обход может впервые увидеть решение уже с текстом.
        legacy_absent = _reset_legacy_absence(block) if not has_text(block) else None
        if task is None and not pending(block):
            continue
        if task is None:
            task = tasks[key] = {
                'status': 'waiting', 'created_at': today.isoformat(),
                'previously_announced': bool((block.get('act_text') or '').strip()),
                'block': deepcopy(block),
                'parent': {k: deepcopy(episode.get(k, parent.get(k))) for k in
                           ('id', 'plaintiff', 'defendant', 'bank_role', 'category', 'track', 'track_origin')},
            }
            if legacy_absent:
                task['legacy_absent_checked_at'] = legacy_absent
        old = task['block']
        if any(old.get(k) and block.get(k) and old[k] != block[k]
               for k in ('link', 'judicial_uid')):
            task.update(status='needs_review', reason='identity_conflict')
            continue
        if task['status'] == 'needs_review':
            # Дозаполнение адреса/даты разрешает только недостаток реквизитов,
            # но не снимает установленный конфликт идентичности.
            if task.get('reason') not in ('missing_card_link', 'missing_case_number', 'missing_court_domain'):
                continue
            if not number(block) or not card_url(block):
                continue
            task.update(status='waiting', reason='')
        if block.get('act_summary_needs_source'):
            if (task['status'] == 'complete' and not old.get('act_summary_needs_source')
                    and act_publication.summary_source(old.get('act_text') or '')):
                for field in PUBLICATION_FIELDS:
                    if field in old:
                        block[field] = deepcopy(old[field])
                block['act_summary_needs_source'] = False
            else:
                # Уже известный, но неполный текст догружаем без повторной
                # новости, включая задачи, завершённые до аудита источника.
                task.update(status='waiting', block=deepcopy(block), previously_announced=True)
                old = task['block']
        if task['status'] == 'complete' and not has_text(block):
            for field in PUBLICATION_FIELDS:
                if field in old:
                    block[field] = deepcopy(old[field])
        elif has_text(block):
            newly_complete = task['status'] != 'complete'
            task.update(status='complete', block=deepcopy(block))
            if (newly_complete and block.get(BASELINE_FIELD) == BASELINE_VERSION
                    and block.get('act_notification_kind') == 'new_publication'):
                _queue_event(data, task)
        else:
            # Последнее достоверное наблюдение может прийти из обычного обхода
            # или из сохранённой задачи после прерванной записи картотеки.
            if (str(block.get('last_checked_at') or '') > str(old.get('last_checked_at') or '')
                    or (block.get(BASELINE_FIELD) == BASELINE_VERSION
                        and old.get(BASELINE_FIELD) != BASELINE_VERSION
                        and str(block.get('last_checked_at') or '') >= str(old.get('last_checked_at') or ''))):
                task['block'] = deepcopy(block)
            else:
                for field in PUBLICATION_FIELDS:
                    if field in old and field not in block:
                        block[field] = deepcopy(old[field])
                # Реквизиты, ранее отсутствовавшие, можно безопасно дозаполнить.
                for field in ('link', 'judicial_uid', 'court', 'court_domain', 'srv_num', 'delo_id'):
                    if block.get(field) and not old.get(field):
                        old[field] = deepcopy(block[field])
            old = task['block']
        # В картотеку переносим исключительно часы проверки акта. Нельзя
        # двигать last_checked_at: иначе ежедневное ожидание текста навсегда
        # откладывает недельную проверку жалоб у завершённого/архивного дела.
        checked = max(str(task['block'].get(ACT_CHECKED_FIELD) or ''),
                      str(block.get(ACT_CHECKED_FIELD) or ''))
        if checked:
            task['block'][ACT_CHECKED_FIELD] = block[ACT_CHECKED_FIELD] = checked
        if task['status'] != 'complete':
            if not number(task['block']):
                task.update(status='needs_review', reason='missing_case_number')
            elif not task['block'].get('court_domain'):
                task.update(status='needs_review', reason='missing_court_domain')
        _prepare(task, today, key)
    return tasks


def event(task):
    block, parent = task['block'], task['parent']
    verdict = classify_verdict_fi(block.get('result') or '')
    change = {
        'case': number(block), 'court': block.get('court') or block.get('court_domain') or '',
        **{k: parent.get(k) or '' for k in ('plaintiff', 'defendant', 'bank_role', 'category')},
        'type': ['fi_act_text_published'],
        'details': {
            **{k: block.get(k) or '' for k in ('link', 'court_domain', 'srv_num', 'delo_id',
                                               'judicial_uid', 'act_text', 'act_date', 'last_event')},
            'decision_date': decision_date(block), 'verdict_label': verdict,
            'raw_result': block.get('result') or '', 'category': parent.get('category') or '',
            'bank_outcome': bank_side_outcome_fi(parent.get('bank_role') or '', verdict),
            **act_publication.dates(block),
            'fi_act_watch_key': identity(block),
        },
    }
    if parent.get('track') == 'plaintiff_light':
        change['track'] = 'plaintiff_light'
    return change


def checkpoint(data):
    disk = load_json(config.JSON_PATH)
    disk[FIELD] = deepcopy(data[FIELD])
    disk[PENDING] = deepcopy(data.get(PENDING) or [])
    if 'act_watch_budget' in data:
        disk['act_watch_budget'] = deepcopy(data['act_watch_budget'])
    save_json(disk, config.JSON_PATH)


def refresh(data, cases, today, fetch, persist=checkpoint, *, force=False,
            fetch_text=None, budget=None, phase='all', now=None):
    from court_monitor.netutil import limit_run_deadline, run_deadline_remaining
    tasks = sync(data, cases, today)
    budget = budget or policy.Budget(data, today)
    now = now or datetime.combine(today, datetime.now().time())
    report = dict(date=today.isoformat(), region=config.REGION, waiting=0,
                  due=0, planned=0, read=0, published=0, backfilled=0, unplanned=0, items=[])
    persist(data)
    for key, task in sorted(tasks.items(), key=lambda kv: (
            policy.phase(kv[1]) == 'backfill', kv[1].get('next_check_at', ''), kv[0])):
        _prepare(task, today, key)
        if task['status'] == 'complete' or (phase != 'all' and policy.phase(task) != phase):
            continue
        block = task['block']
        item = dict(key=key, number=number(block), court=block.get('court_domain') or '',
                    status=task['status'], last_checked_at=_checked_at(block),
                    normal_last_checked_at=block.get('last_checked_at') or '',
                    fi_act_checked_at=block.get(ACT_CHECKED_FIELD) or '',
                    next_check_at=task.get('next_check_at') or '',
                    over_90_days=(policy.age(block, today, decision_date) or 0) > 90)
        report['items'].append(item)
        if task['status'] in ('expired', 'needs_review'):
            item['reason'] = task.get('reason') or 'age_limit'
            report['unplanned'] += task['status'] == 'needs_review'
            continue
        due = next_check(block, today)
        if not force and due and due > today:
            item['reason'] = 'scheduled'
            continue
        report['due'] += 1
        url = card_url(block)
        if not url:
            task.update(status='needs_review', reason='missing_card_link')
            item.update(status='needs_review', reason='missing_card_link')
            report['unplanned'] += 1
            continue
        report['planned'] += 1
        card_id = block['court_domain'] + '|' + number(block)
        telemetry.register_planned_case_ids('first_instance', [card_id])
        kind = policy.phase(task)
        reason = '' if force else policy.retry_reason(task, now)
        reason = reason or budget.reason(kind, run_remaining=run_deadline_remaining())
        if reason:
            item['reason'] = reason
            continue
        left = budget.remaining(kind)
        policy.attempt(task, now)
        started = budget.begin(kind)
        persist(data)
        try:
            with limit_run_deadline(left):
                html = fetch(url, context=number(block))
                if not html and (config.FETCH_DIAG or {}).get('kind') == 'invalid_card_request':
                    task.update(status='needs_review', reason='invalid_card_request')
                    item.update(status='needs_review', reason='invalid_card_request')
                    continue
                info = parse_case_card(html or '', 'https://' + block['court_domain'])
                if (card_is_empty_shell(info) or
                        _warn_if_card_degraded(info, number(block), case_block=block) == 'degraded'):
                    item.update(reason='unread_card', failure_kind=(config.FETCH_DIAG or {}).get('kind', ''))
                    continue
                if (number({'case_number': info.get('Номер дела (карточка)') or ''}) != number(block)
                        or (block.get('judicial_uid') and info.get('УИД') != block['judicial_uid'])):
                    task.update(status='needs_review', reason='identity_mismatch')
                    item.update(status='needs_review', reason='identity_mismatch')
                    continue
                text = (info.get('act_text') or '').strip()
                source = info.get('_act_url') or url
                if not text and info.get('_act_url'):
                    if urlparse(source).hostname != block['court_domain']:
                        task.update(status='needs_review', reason='identity_mismatch')
                        item.update(status='needs_review', reason='identity_mismatch')
                        continue
                    text = fetch_text(source, context=number(block)) if fetch_text else ''
                    if not text:
                        item['reason'] = 'act_text_unread'
                        continue
                card_day = (fi_decision_date_from_events(info.get('_events') or [])
                            or info.get('Дата рассмотрения (карточка)') or '')
                text_day = act_publication.document_date(text) if text else ''
                expected = parse_date(decision_date(block))
                # Дата самого документа точнее последнего движения той же
                # карточки (новый круг/расходы). Без даты в шапке используем
                # подтверждённое решение из карточки, но не дату заседания.
                observed = parse_date(text_day) or parse_date(card_day)
                if ((observed and observed != expected) or (text and not observed)):
                    reason = 'act_identity_mismatch' if observed else 'missing_act_decision_date'
                    task.update(status='needs_review', reason=reason)
                    item.update(status='needs_review', reason=reason)
                    continue
            report['read'] += 1
            telemetry.mark_case_read('first_instance', card_id)
            notify = observe(block, text, today, present=bool(text or info.get('_act_url')),
                             confirmed_date=text_day or card_day, source_url=source)
            if has_text(block):
                task.update(status='complete', completed_at=today.isoformat())
                if notify and not task.get('previously_announced'):
                    _queue_event(data, task)
                    report['published'] += 1
                    item['reason'] = 'published'
                else:
                    report['backfilled'] += 1
                    item['reason'] = 'backfilled'
            else:
                item['reason'] = 'source_incomplete' if text else 'text_not_published'
            due = next_check(block, today)
            task['next_check_at'] = due.isoformat() if due else ''
            item['next_check_at'] = task['next_check_at']
        except Exception as exc:
            item.update(reason='fetch_error', error=type(exc).__name__)
        finally:
            budget.finish(kind, started)
            persist(data)
    sync(data, cases, today)
    report['waiting'] = sum(t['status'] in ('waiting', 'needs_review') for t in tasks.values())
    report['expired'] = sum(t['status'] == 'expired' for t in tasks.values())
    report['unread'] = report['planned'] - report['read']
    report['long_wait'] = sum(i['over_90_days'] for i in report['items']
                              if i.get('reason') not in ('published', 'backfilled', 'age_limit'))
    persist(data)
    return report


def persist_archives(data, today):
    from court_monitor.storage import load_bank_json, save_bank_json
    paths = [config.JSON_ARCHIVE_PATH] + glob.glob(config.cold_archive_glob())
    if config.BANK_TRACK:
        paths += [config.JSON_BANK_ARCHIVE_PATH]
        paths += [p for p in glob.glob(config.bank_cold_archive_glob())
                  if config.is_bank_cold_archive_file(p)]
    for path in dict.fromkeys(paths):
        if not os.path.exists(path):
            continue
        bank_hot = path == config.JSON_BANK_ARCHIVE_PATH
        doc = (load_bank_json(path, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
               if bank_hot else load_json(path))
        before = json.dumps(doc, sort_keys=True, ensure_ascii=False)
        sync(data, doc.get('cases') or [], today)
        if json.dumps(doc, sort_keys=True, ensure_ascii=False) != before:
            if bank_hot:
                save_bank_json(doc, path, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
            else:
                save_json(doc, path)


def _change_key(change):
    details = change.get('details') or {}
    return details.get('fi_act_watch_key') or identity(dict(details, case_number=change.get('case') or ''))


def merge_changes(data, changes):
    # Сливаем только сообщения об одном тексте; иные FI-события и иные круги
    # того же дела не объединяем по голому номеру.
    result, indexes = [], {}
    for change in list(data.get(PENDING) or []) + list(changes):
        if 'fi_act_text_published' not in (change.get('type') or []):
            result.append(deepcopy(change))
            continue
        key = _change_key(change)
        if key not in indexes:
            indexes[key] = len(result)
            result.append(deepcopy(change))
        else:
            old = result[indexes[key]]
            old['type'] = list(dict.fromkeys(old['type'] + change['type']))
            old['details'].update({k: v for k, v in (change.get('details') or {}).items()
                                   if v not in ('', None)})
    return result


def acknowledge(data, changes, issue_key):
    pending_changes = data.get(PENDING)
    if not pending_changes:
        return
    context = load_json(config.LAST_DIGEST_CONTEXT_PATH)
    saved = context.get('fi_changes') or []
    signatures = {json.dumps(ch, sort_keys=True, ensure_ascii=False) for ch in saved}
    publication_keys = {_change_key(ch) for ch in saved
                        if 'fi_act_text_published' in (ch.get('type') or [])}
    if (context.get('issue_key') != issue_key
            or not all(json.dumps(ch, sort_keys=True, ensure_ascii=False) in signatures for ch in changes)
            or not all(_change_key(ch) in publication_keys for ch in pending_changes)):
        raise RuntimeError('поздние акты первой инстанции не сохранены в контексте дайджеста')
    data.pop(PENDING)
    try:
        save_json(data, config.JSON_PATH)
    except Exception:
        data[PENDING] = pending_changes
        raise
