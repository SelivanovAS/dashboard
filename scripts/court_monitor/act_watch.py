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
from datetime import date, timedelta

from court_monitor import config, telemetry
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
    decision_dt = parse_date(block.get('decision_date') or '')
    decision = decision_dt.date() if decision_dt else None
    last = str(block.get('last_checked_at') or '')[:10]
    try:
        checked = date.fromisoformat(last)
    except ValueError:
        checked = None
    age = (today - decision).days if decision else 0
    days = 1 if age <= 30 else 7 if age <= 90 else 30
    due = checked + timedelta(days=days) if checked else today
    if days == 1:
        while due.weekday() >= 5:
            due += timedelta(days=1)
    return due


def sync(data, cases, today):
    tasks = data.setdefault(FIELD, {})
    for parent, episode in productions(cases):
        block = episode.get('cassation') or {}
        key = identity(block)
        if not key:
            continue
        if key not in tasks:
            if block.get('act_published') or not block.get('decision_date'):
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
        if block.get('act_published'):
            newly_complete = task['status'] != 'complete'
            task['status'] = 'complete'
            task['block'] = deepcopy(block)
            if newly_complete and not task.get('previously_announced'):
                data.setdefault('pending_cassation_changes', []).append(event(task))
        elif task['status'] == 'complete':
            # Восстановление после сбоя между записью задачи и архива.
            for k in ('act_published', 'act_text', 'act_date', 'cassation_number', 'last_checked_at'):
                if k in old:
                    block[k] = deepcopy(old[k])
        elif str(block.get('last_checked_at') or '') > str(old.get('last_checked_at') or ''):
            task['block'] = deepcopy(block)
        task['next_check_at'] = next_check(task['block'], today).isoformat()
    return tasks


def event(task):
    block = task['block']
    details = deepcopy(block)
    details['act_text'] = extract_motive_part(block.get('act_text') or '', 1800)
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
    disk['pending_cassation_changes'] = deepcopy(data.get('pending_cassation_changes') or [])
    save_json(disk, config.JSON_PATH)


def refresh(data, cases, today, fetch, persist=checkpoint, *, force=False):
    tasks = sync(data, cases, today)
    report = {'date': today.isoformat(), 'region': config.REGION,
              'waiting': 0, 'due': 0, 'planned': 0, 'read': 0,
              'published': 0, 'unplanned': 0, 'items': []}
    persist(data)  # Обязательство переживает прерванный прогон и смену стадии.
    telemetry.register_planned_case_ids('cassation', [
        t['block']['court_domain'] + '|' + t['block']['case_number']
        for t in tasks.values() if t['status'] == 'waiting'
        and (force or next_check(t['block'], today) <= today)
    ])
    for key, task in tasks.items():
        if task['status'] == 'complete':
            continue
        report['waiting'] += 1
        block = task['block']
        decision_dt = parse_date(block.get('decision_date') or '')
        decision = decision_dt.date() if decision_dt else None
        item = {'key': key, 'number': block['case_number'], 'court': block['court_domain'],
                'status': task['status'], 'last_checked_at': block.get('last_checked_at', ''),
                'next_check_at': task.get('next_check_at', ''),
                'over_90_days': bool(decision and (today - decision).days > 90)}
        report['items'].append(item)
        if task['status'] == 'needs_review':
            item['reason'] = task.get('reason', 'identity_conflict')
            report['unplanned'] += 1
            continue
        if not force and next_check(block, today) > today:
            item['reason'] = 'scheduled'
            continue
        report['due'] += 1
        url = cassation_card_url(block)
        if not url or urlparse(url).hostname != block['court_domain']:
            report['unplanned'] += 1
            item['reason'] = 'missing_card_link'
            continue
        report['planned'] += 1
        try:
            html = fetch(url, context=block['case_number'])
            info = parse_cassation_card(html or '', 'https://' + block['court_domain'])
            if not info or not info.get('decision_date'):
                item['reason'] = 'unread_card'
                item['failure_kind'] = (config.FETCH_DIAG or {}).get('kind', '')
                continue
            # Не принимать HTTP 200/оболочку/другую карточку за успех.
            if (not info.get('page_case_number') or info['page_case_number'] != block['case_number']
                    or (block.get('judicial_uid') and info.get('judicial_uid') != block['judicial_uid'])):
                item['reason'] = 'identity_mismatch'
                continue
            report['read'] += 1
            telemetry.mark_case_read('cassation', block['court_domain'] + '|' + block['case_number'])
            block['last_checked_at'] = today.isoformat()
            if info.get('act_published') and info.get('act_text'):
                block.update(act_published=True, act_text=info['act_text'],
                             act_date=block.get('decision_date') or '',
                             cassation_number=info.get('cassation_number') or block.get('cassation_number', ''))
                task['status'] = 'complete'
                task['completed_at'] = today.isoformat()
                change = event(task)
                pending = data.setdefault('pending_cassation_changes', [])
                if not task.get('previously_announced') and change not in pending:
                    pending.append(change)
                report['published'] += 1
                item['reason'] = 'published'
            else:
                item['reason'] = 'text_not_published'
            task['next_check_at'] = next_check(block, today).isoformat()
            item['next_check_at'] = task['next_check_at']
        except Exception as exc:
            item['reason'] = 'fetch_error'
            item['error'] = type(exc).__name__
            continue
        # Сохранение вне сетевого try: ошибку диска нельзя скрывать как отказ суда.
        persist(data)
    sync(data, cases, today)
    report['waiting'] = sum(t['status'] != 'complete' for t in tasks.values())
    report['unread'] = report['planned'] - report['read']
    report['long_wait'] = sum(i['over_90_days'] for i in report['items'] if i['reason'] != 'published')
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
    return bool(block.get('decision_date') and not block.get('act_published'))
