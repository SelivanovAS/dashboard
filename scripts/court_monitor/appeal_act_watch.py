"""Ожидание апелляционного текста независимо от стадии и места хранения дела.

Задачи и неотправленные события сохраняются вместе в cases.json. Успех
публикации не расходуется до этой записи; сбой транспорта оставляет повтор.

Чтение карточек и запись полных снимков в архивы выполняет card_refresh.
"""
from __future__ import annotations

from copy import deepcopy
import json
import re
from urllib.parse import urlparse

from court_monitor import config, act_publication, act_watch_policy as policy
from court_monitor.courts import APPEAL_COURTS
from court_monitor.lifecycle import classify_verdict, bank_side_outcome
from court_monitor.storage import load_json, save_json, load_digested_acts
from court_monitor.textutil import parse_date, case_id_uid

FIELD = 'appeal_act_watch'


def number(block):
    match = re.match(r"\s*(\d+-(?:\d+-)?\d+/\d{4})", block.get('case_number') or '')
    return match.group(1) if match else ''


def identity(block):
    domain = (block.get('court_domain') or '').lower().strip()
    num = number(block)
    return f'{domain}|{block.get("delo_id") or 5}|{block.get("srv_num") or 1}|{num}' if domain and num else ''


def decision_date(block):
    return block.get('act_decision_date') or block.get('hearing_date') or block.get('event_date') or ''


def has_text(block):
    return bool(((block.get('act_text') or '').strip() and not block.get('act_summary_needs_source')))


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
    return policy.next_check(block, today, decision_date, identity(block))


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
        if block.get('act_source_review_reason'):
            task.update(status='needs_review', reason=block['act_source_review_reason'])
            continue
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
        if (block.get('act_summary_needs_source') and task['status'] == 'complete'
                and old.get('act_summary_needs_source') is False
                and act_publication.summary_source(old.get('act_text') or '')):
            for field in ('act_text', 'act_published', 'act_date', 'last_checked_at') + act_publication.FIELDS:
                if field in old:
                    block[field] = deepcopy(old[field])
        if block.get('act_summary_needs_source'):
            task.update(status='waiting', block=deepcopy(block), previously_announced=True)
        if has_text(block):
            newly_complete = task['status'] != 'complete'
            task['status'] = 'complete'
            task['block'] = deepcopy(block)
            if (newly_complete and not task.get('previously_announced')
                    and block.get('act_notification_kind') == 'new_publication'):
                data.setdefault('pending_appeal_act_changes', []).append(event(task))
        elif task['status'] == 'complete':
            # Восстановление после сбоя между записью задачи и архива.
            for k in ('act_published', 'act_text', 'act_date', 'last_checked_at') + act_publication.FIELDS:
                if k in old:
                    block[k] = deepcopy(old[k])
        elif str(block.get('last_checked_at') or '') > str(old.get('last_checked_at') or ''):
            task['block'] = deepcopy(block)
        policy.prepare(task, today, decision_date, key)
        if str(task['block'].get('last_checked_at') or '') > str(block.get('last_checked_at') or ''):
            block['last_checked_at'] = task['block']['last_checked_at']
        for field in act_publication.FIELDS:
            if task['block'].get(field) and not block.get(field):
                block[field] = task['block'][field]
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
        'act_text': block.get('act_text') or '',
        **act_publication.dates(block),
        'act_verdict_raw': block.get('result') or '',
        'act_verdict_label': verdict,
    }}


def checkpoint(data):
    # Остальные поля на диске принадлежат основному прогону. В частности,
    # объединённый в памяти bank-трек нельзя сохранить в основной cases.json.
    disk = load_json(config.JSON_PATH)
    disk[FIELD] = deepcopy(data[FIELD])
    if 'act_watch_budget' in data:
        disk['act_watch_budget'] = deepcopy(data['act_watch_budget'])
    disk['pending_appeal_act_changes'] = deepcopy(data.get('pending_appeal_act_changes') or [])
    save_json(disk, config.JSON_PATH)



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
