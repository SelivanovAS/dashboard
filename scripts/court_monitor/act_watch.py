"""Ожидание кассационного текста независимо от стадии и места хранения дела.

Задачи и неотправленные события сохраняются вместе в cases.json. Успех
публикации не расходуется до этой записи; сбой транспорта оставляет повтор.
"""
from __future__ import annotations

from copy import deepcopy
import glob
import json
import os
from urllib.parse import urlparse
from datetime import date, datetime, timedelta

from court_monitor import config, telemetry, act_publication, act_watch_policy as policy
from court_monitor.courts import cassation_card_url
from court_monitor.parsing.cassation import parse_cassation_card
from court_monitor.storage import load_json, save_json, load_cassation_acts, _cassation_act_key
from court_monitor.textutil import parse_date, extract_motive_part

FIELD = 'cassation_act_watch'


def identity(block):
    domain = (block.get('court_domain') or '').lower().strip()
    number = (block.get('case_number') or '').strip()
    return f'{domain}|{block.get("delo_id") or 2800001}|{number}' if domain and number else ''


def productions(cases):
    for case in cases:
        yield case, case
        for episode in case.get('history') or []:
            if isinstance(episode, dict):
                yield case, episode


def next_check(block, today):
    return policy.next_check(block, today, lambda b: b.get('decision_date') or '', identity(block))


def sync(data, cases, today):
    tasks = data.setdefault(FIELD, {})
    for parent, episode in productions(cases):
        block = episode.get('cassation') or {}
        key = identity(block)
        if not key:
            continue
        if key not in tasks:
            if ((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source')) or not block.get('decision_date'):
                continue
            tasks[key] = {
                'status': 'waiting', 'created_at': today.isoformat(),
                'previously_announced': _cassation_act_key(block) in load_cassation_acts(),
                'block': deepcopy(block),
                'parent': {k: deepcopy(parent.get(k)) for k in
                           ('id', 'plaintiff', 'defendant', 'bank_role', 'category')},
                'first_instance': deepcopy(episode.get('first_instance') or {}),
            }
        task = tasks[key]
        old = task['block']
        # Производство с тем же номером, но другой карточкой/УИД не замещаем.
        if any(old.get(k) and block.get(k) and old[k] != block[k]
               for k in ('link', 'judicial_uid')):
            task['status'] = 'needs_review'
            task['reason'] = 'identity_conflict'
            continue
        if task['status'] == 'needs_review':
            continue
        if (block.get('act_summary_needs_source') and task['status'] == 'complete'
                and old.get('act_summary_needs_source') is False
                and act_publication.summary_source(old.get('act_text') or '')):
            for field in ('act_text', 'act_published', 'act_date', 'cassation_number', 'last_checked_at') + act_publication.FIELDS:
                if field in old:
                    block[field] = deepcopy(old[field])
        if block.get('act_summary_needs_source'):
            # Дочитка уже известного акта, даже если прежняя задача завершена.
            # Старый снимок задачи не должен затереть запрос полного источника.
            task.update(status='waiting', block=deepcopy(block), previously_announced=True)
        if ((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source')):
            newly_complete = task['status'] != 'complete'
            task['status'] = 'complete'
            task['block'] = deepcopy(block)
            if (newly_complete and not task.get('previously_announced')
                    and block.get('act_notification_kind') == 'new_publication'):
                data.setdefault('pending_cassation_changes', []).append(event(task))
        elif task['status'] == 'complete':
            # Восстановление после сбоя между записью задачи и архива.
            for k in ('act_published', 'act_text', 'act_date', 'cassation_number', 'last_checked_at') + act_publication.FIELDS:
                if k in old:
                    block[k] = deepcopy(old[k])
        elif str(block.get('last_checked_at') or '') > str(old.get('last_checked_at') or ''):
            task['block'] = deepcopy(block)
        policy.prepare(task, today, lambda b: b.get('decision_date') or '', key)
        if str(task['block'].get('last_checked_at') or '') > str(block.get('last_checked_at') or ''):
            block['last_checked_at'] = task['block']['last_checked_at']
        for field in act_publication.FIELDS:
            if task['block'].get(field) and not block.get(field):
                block[field] = task['block'][field]
    return tasks


def event(task):
    block = task['block']
    details = deepcopy(block)
    details.update(act_publication.dates(block))
    details['act_text'] = block.get('act_text') or ''
    return {
        'case': task['first_instance'].get('case_number') or task['parent'].get('id', ''),
        'cassation_internal_number': block['case_number'],
        'type': ['new_act'],
        'details': details,
        # Прежнее производство может уже жить в истории или холодном архиве.
        'publication_parent': dict(deepcopy(task['parent']),
                                   first_instance=deepcopy(task['first_instance']),
                                   cassation={k: block.get(k) for k in ('case_number', 'court_domain', 'decision_date')}),
    }


def checkpoint(data):
    # Остальные поля на диске принадлежат основному прогону. В частности,
    # объединённый в памяти bank-трек нельзя сохранить в основной cases.json.
    disk = load_json(config.JSON_PATH)
    disk[FIELD] = deepcopy(data[FIELD])
    if 'act_watch_budget' in data:
        disk['act_watch_budget'] = deepcopy(data['act_watch_budget'])
    disk['pending_cassation_changes'] = deepcopy(data.get('pending_cassation_changes') or [])
    save_json(disk, config.JSON_PATH)


def refresh(data, cases, today, fetch, persist=checkpoint, *, force=False,
            fetch_text=None, budget=None, phase='all', now=None):
    from court_monitor.netutil import limit_run_deadline, run_deadline_remaining
    tasks = sync(data, cases, today)
    budget = budget or policy.Budget(data, today)
    now = now or datetime.combine(today, datetime.now().time())
    report = {'date': today.isoformat(), 'region': config.REGION,
              'waiting': 0, 'due': 0, 'planned': 0, 'read': 0,
              'published': 0, 'backfilled': 0, 'unplanned': 0, 'items': []}
    persist(data)
    for key, task in sorted(tasks.items(), key=lambda kv: (policy.phase(kv[1]) == 'backfill', kv[1].get('next_check_at', ''), kv[0])):
        policy.prepare(task, today, lambda b: b.get('decision_date') or '', key)
        if task['status'] == 'complete' or (phase != 'all' and policy.phase(task) != phase):
            continue
        block = task['block']
        item = {'key': key, 'number': block['case_number'], 'court': block['court_domain'],
                'status': task['status'], 'last_checked_at': block.get('last_checked_at', ''),
                'next_check_at': task.get('next_check_at', ''),
                'over_90_days': (policy.age(block, today, lambda b: b.get('decision_date') or '') or 0) > 90}
        report['items'].append(item)
        if task['status'] == 'expired':
            item['reason'] = 'age_limit'
            continue
        if task['status'] == 'needs_review':
            item['reason'] = task.get('reason', 'identity_conflict')
            report['unplanned'] += 1
            continue
        due = next_check(block, today)
        if not force and due and due > today:
            item['reason'] = 'scheduled'
            continue
        report['due'] += 1
        url = cassation_card_url(block)
        if not url or urlparse(url).hostname != block['court_domain']:
            task.update(status='needs_review', reason='missing_card_link')
            item['reason'] = 'missing_card_link'
            report['unplanned'] += 1
            continue
        report['planned'] += 1
        telemetry.register_planned_case_ids('cassation', [block['court_domain'] + '|' + block['case_number']])
        reason = '' if force else policy.retry_reason(task, now)
        kind = policy.phase(task)
        left = budget.remaining(kind)
        run_left = run_deadline_remaining()
        if reason or left < 1 or (run_left is not None and run_left < 1):
            item['reason'] = reason or budget.reason(kind, run_remaining=run_left)
            continue
        policy.attempt(task, now)
        started = budget.begin(kind)
        persist(data)
        try:
            with limit_run_deadline(left):
                html = fetch(url, context=block['case_number'])
                if not html and (config.FETCH_DIAG or {}).get('kind') == 'invalid_card_request':
                    task.update(status='needs_review', reason='invalid_card_request')
                    item['reason'] = 'invalid_card_request'
                    continue
                info = parse_cassation_card(html or '', 'https://' + block['court_domain'])
                if not info or not info.get('decision_date'):
                    item['reason'] = 'unread_card'
                    item['failure_kind'] = (config.FETCH_DIAG or {}).get('kind', '')
                    continue
                if (info.get('page_case_number') != block['case_number'] or
                        (block.get('judicial_uid') and info.get('judicial_uid') != block['judicial_uid'])):
                    task.update(status='needs_review', reason='identity_mismatch')
                    item['reason'] = 'identity_mismatch'
                    continue
                text = info.get('act_text') or ''
                present = bool(info.get('act_published'))
                confirmed_date = info.get('decision_date') or ''
                if info.get('cassation_number'):
                    block['cassation_number'] = info['cassation_number']
            report['read'] += 1
            telemetry.mark_case_read('cassation', block['court_domain'] + '|' + block['case_number'])
            block['last_checked_at'] = today.isoformat()
            notify = act_publication.observe(block, text, today, present=present, confirmed_date=confirmed_date, source_url=url)
            if text and not block.get('act_summary_needs_source'):
                task.update(status='complete', completed_at=today.isoformat())
                if notify and not task.get('previously_announced'):
                    change = event(task)
                    pending = data.setdefault('pending_cassation_changes', [])
                    if change not in pending:
                        pending.append(change)
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
            item['reason'] = 'fetch_error'
            item['error'] = type(exc).__name__
        finally:
            budget.finish(kind, started)
            persist(data)  # Ошибка диска не перехватывается как сетевая.
    sync(data, cases, today)
    report['waiting'] = sum(t['status'] in ('waiting', 'needs_review') for t in tasks.values())
    report['expired'] = sum(t['status'] == 'expired' for t in tasks.values())
    report['unread'] = report['planned'] - report['read']
    report['long_wait'] = sum(i['over_90_days'] for i in report['items'] if i.get('reason') not in ('published', 'backfilled', 'age_limit'))
    persist(data)
    return report


def persist_archives(data, today):
    """Применить завершённые задачи к архивам без перемещения дел между файлами."""
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


def pending(block):
    """Публикацию терминального производства обслуживает отдельная очередь."""
    return bool(block.get('decision_date') and not ((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source')))
