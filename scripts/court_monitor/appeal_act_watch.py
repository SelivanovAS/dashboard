"""Ожидание апелляционного текста независимо от стадии и места хранения дела.

Задачи и неотправленные события сохраняются вместе в cases.json. Успех
публикации не расходуется до этой записи; сбой транспорта оставляет повтор.
"""
from __future__ import annotations

from copy import deepcopy
import glob
import json
import os
import re
from urllib.parse import urlparse
from datetime import date, timedelta

from court_monitor import config, telemetry
from court_monitor.courts import APPEAL_COURTS
from court_monitor.lifecycle import classify_verdict, bank_side_outcome
from court_monitor.parsing.cards import parse_case_card, card_is_empty_shell, _warn_if_card_degraded
from court_monitor.storage import load_json, save_json, load_digested_acts
from court_monitor.textutil import parse_date, extract_motive_part, case_id_uid

FIELD = 'appeal_act_watch'


def number(block):
    match = re.match(r"\s*(\d+-(?:\d+-)?\d+/\d{4})", block.get('case_number') or '')
    return match.group(1) if match else ''


def identity(block):
    domain = (block.get('court_domain') or '').lower().strip()
    num = number(block)
    return f'{domain}|{block.get("delo_id") or 5}|{block.get("srv_num") or 1}|{num}' if domain and num else ''


def decision_date(block):
    return block.get('act_date') or block.get('hearing_date') or block.get('event_date') or ''


def has_text(block):
    return bool((block.get('act_text') or '').strip())


def pending(block):
    return bool(block.get('status') == 'Решено' and parse_date(decision_date(block)) and not has_text(block))


def card_url(block):
    court = next((c for c in APPEAL_COURTS if c.domain == block.get('court_domain')), None)
    cid, uid = case_id_uid(block.get('link') or '')
    if not court or not cid or not uid:
        return ''
    from urllib.parse import urlencode
    return court.base_url + '/modules.php?' + urlencode(dict(name='sud_delo',
        srv_num=block.get('srv_num') or 1, name_op='case', case_id=cid, case_uid=uid,
        delo_id=block.get('delo_id') or 5, new=block.get('delo_id') or 5))


def productions(cases):
    for case in cases:
        yield case, case
        for episode in case.get('history') or []:
            if isinstance(episode, dict):
                yield case, episode


def next_check(block, today):
    decision_dt = parse_date(decision_date(block))
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
    announced = load_digested_acts()
    domains = {}
    for _, episode in productions(cases):
        ap = episode.get('appeal') or {}
        domains.setdefault(number(ap), set()).add(ap.get('court_domain') or '')
    for task in tasks.values():
        ap = task['block']
        domains.setdefault(number(ap), set()).add(ap.get('court_domain') or '')
    for parent, episode in productions(cases):
        block = episode.get('appeal') or {}
        key = identity(block)
        if not key:
            continue
        if key not in tasks:
            if not pending(block):
                continue
            tasks[key] = {
                'status': 'waiting', 'created_at': today.isoformat(),
                'previously_announced': block.get('case_number') in announced or number(block) in announced,
                'block': deepcopy(block),
                'parent': {k: deepcopy(episode.get(k, parent.get(k))) for k in
                           ('id', 'plaintiff', 'defendant', 'bank_role', 'category')},
                'first_instance': deepcopy(episode.get('first_instance') or {}),
                'judicial_uid': (episode.get('first_instance') or {}).get('judicial_uid') or block.get('judicial_uid') or '',
            }
        task = tasks[key]
        old = task['block']
        if task.get('previously_announced') and len(domains.get(number(block), set())) > 1:
            task['status'] = 'needs_review'
            task['reason'] = 'legacy_announcement_ambiguous'
            continue
        # Производство с тем же номером, но другой карточкой/УИД не замещаем.
        episode_uid = (episode.get('first_instance') or {}).get('judicial_uid') or block.get('judicial_uid')
        if (task.get('judicial_uid') and episode_uid and task['judicial_uid'] != episode_uid) or any(old.get(k) and block.get(k) and old[k] != block[k]
               for k in ('link', 'judicial_uid')):
            task['status'] = 'needs_review'
            task['reason'] = 'identity_conflict'
            continue
        if task['status'] == 'needs_review':
            continue
        if has_text(block):
            newly_complete = task['status'] != 'complete'
            task['status'] = 'complete'
            task['block'] = deepcopy(block)
            if newly_complete and not task.get('previously_announced'):
                data.setdefault('pending_appeal_act_changes', []).append(event(task))
        elif task['status'] == 'complete':
            # Восстановление после сбоя между записью задачи и архива.
            for k in ('act_published', 'act_text', 'act_date', 'last_checked_at'):
                if k in old:
                    block[k] = deepcopy(old[k])
        elif str(block.get('last_checked_at') or '') > str(old.get('last_checked_at') or ''):
            task['block'] = deepcopy(block)
        task['next_check_at'] = next_check(task['block'], today).isoformat()
    return tasks


def event(task):
    block, parent = task['block'], task['parent']
    verdict = classify_verdict(block.get('result') or '', block.get('last_event') or '')
    appellant = ('Банк' if block.get('appellant_is_bank') else 'Иное лицо') if block.get('appellant_is_bank') is not None else ''
    return {'case': block['case_number'], 'type': ['new_act'], 'details': {
        'court_domain': block['court_domain'], 'case_url': card_url(block),
        'plaintiff': parent.get('plaintiff') or '', 'defendant': parent.get('defendant') or '',
        'role': parent.get('bank_role') or '', 'category': parent.get('category') or '',
        'appellant': appellant,
        'bank_outcome': bank_side_outcome(parent.get('bank_role') or '', appellant, verdict),
        'appeal_court': block.get('court') or block['court_domain'],
        'appellant_name': block.get('appellant') or '', 'appellant_role': block.get('appellant_status') or '',
        'hearing_date': block.get('hearing_date') or '', 'act_date': block.get('act_date') or '',
        'act_text': extract_motive_part(block.get('act_text') or '', 1800),
        'act_verdict_raw': block.get('result') or '',
        'act_verdict_label': verdict,
    }}


def checkpoint(data):
    # Остальные поля на диске принадлежат основному прогону. В частности,
    # объединённый в памяти bank-трек нельзя сохранить в основной cases.json.
    disk = load_json(config.JSON_PATH)
    disk[FIELD] = deepcopy(data[FIELD])
    disk['pending_appeal_act_changes'] = deepcopy(data.get('pending_appeal_act_changes') or [])
    save_json(disk, config.JSON_PATH)


def refresh(data, cases, today, fetch, persist=checkpoint, *, force=False, fetch_text=None):
    tasks = sync(data, cases, today)
    report = {'date': today.isoformat(), 'region': config.REGION,
              'waiting': 0, 'due': 0, 'planned': 0, 'read': 0,
              'published': 0, 'unplanned': 0, 'items': []}
    persist(data)  # Обязательство переживает прерванный прогон и смену стадии.
    telemetry.register_planned_case_ids('appeal', [
        t['block']['court_domain'] + '|' + t['block']['case_number']
        for t in tasks.values() if t['status'] == 'waiting'
        and (force or next_check(t['block'], today) <= today)
    ])
    for key, task in tasks.items():
        if task['status'] == 'complete':
            continue
        report['waiting'] += 1
        block = task['block']
        decision_dt = parse_date(decision_date(block))
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
        url = card_url(block)
        if not url or urlparse(url).hostname != block['court_domain']:
            report['unplanned'] += 1
            item['reason'] = 'missing_card_link'
            continue
        report['planned'] += 1
        try:
            html = fetch(url, context=block['case_number'])
            info = parse_case_card(html or '', 'https://' + block['court_domain'])
            if card_is_empty_shell(info) or _warn_if_card_degraded(info, block['case_number'], case_block=block) == 'degraded':
                item['reason'] = 'unread_card'
                item['failure_kind'] = (config.FETCH_DIAG or {}).get('kind', '')
                continue
            # Не принимать HTTP 200/оболочку/другую карточку за успех.
            if (info.get('Номер дела (карточка)') != number(block)
                    or (task.get('judicial_uid') and info.get('УИД') != task['judicial_uid'])):
                item['reason'] = 'identity_mismatch'
                continue
            text = info.get('act_text') or ''
            if not text and info.get('_act_url') and fetch_text:
                if urlparse(info['_act_url']).hostname != block['court_domain']:
                    item['reason'] = 'identity_mismatch'
                    continue
                text = fetch_text(info['_act_url'], context=block['case_number']) or ''
                if not text:
                    item['reason'] = 'act_text_unread'
                    continue  # Не считать успешным чтение карточки с недоступным текстом.
            report['read'] += 1
            telemetry.mark_case_read('appeal', block['court_domain'] + '|' + block['case_number'])
            block['last_checked_at'] = today.isoformat()
            if text:
                block.update(act_published=True, act_text=text[:8000],
                             act_date=info.get('Дата публикации акта') or block.get('act_date') or '')
                task['status'] = 'complete'
                task['completed_at'] = today.isoformat()
                change = event(task)
                pending = data.setdefault('pending_appeal_act_changes', [])
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



def merge_changes(data, changes):
    merged = {}
    for index, change in enumerate(list(data.get('pending_appeal_act_changes') or []) + list(changes)):
        details = change.get('details') or {}
        key = (details.get('court_domain') or urlparse(details.get('case_url') or '').hostname,
               number({'case_number': change.get('case') or ''}))
        if not key[1]:
            key = ('unkeyed', index)
        if key not in merged:
            merged[key] = deepcopy(change)
        else:
            old = merged[key]
            old['type'] = list(dict.fromkeys(old['type'] + change['type']))
            old['details'].update({k: v for k, v in details.items() if v not in ('', None)})
    return list(merged.values())


def acknowledge(data, changes, issue_key):
    pending = data.get('pending_appeal_act_changes')
    if not pending:
        return
    context = load_json(config.LAST_DIGEST_CONTEXT_PATH)
    signatures = {json.dumps(ch, sort_keys=True, ensure_ascii=False) for ch in context.get('changes', [])}
    if context.get('issue_key') != issue_key or not all(
            json.dumps(ch, sort_keys=True, ensure_ascii=False) in signatures for ch in changes):
        raise RuntimeError('поздние апелляционные акты не сохранены в контексте дайджеста')
    data.pop('pending_appeal_act_changes')
    try:
        save_json(data, config.JSON_PATH)
    except Exception:
        data['pending_appeal_act_changes'] = pending
        raise
