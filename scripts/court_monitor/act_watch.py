"""Ожидание кассационного текста независимо от стадии и места хранения дела.

Задачи и неотправленные события сохраняются вместе в cases.json. Успех
публикации не расходуется до этой записи; сбой транспорта оставляет повтор.

Чтение карточек и запись полных снимков в архивы выполняет card_refresh.
"""
from __future__ import annotations

from copy import deepcopy

from court_monitor import config, act_publication, act_watch_policy as policy
from court_monitor.storage import load_json, save_json, load_cassation_acts, _cassation_act_key

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


def pending(block):
    """Публикацию терминального производства обслуживает отдельная очередь."""
    return bool(block.get('decision_date') and not ((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source')))
