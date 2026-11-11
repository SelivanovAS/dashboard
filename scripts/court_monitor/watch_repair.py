"""Узкое восстановление очередей по уже сохранённым производствам, без сети.

Не снимает конфликт текста, отмену решения или отзыв листа. Номер без суда
и площадки не является доказательством. Каждое изменение оставляет основание.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date

from court_monitor import act_anchor
from court_monitor.fi_identity import resolve_fi_identity
from court_monitor.textutil import _bare_case_number, parse_date

IDENTIFIERS = ('court_domain', 'srv_num', 'delo_id', 'case_number', 'link', 'judicial_uid')
GAPS = {'missing_card_link', 'missing_case_number', 'missing_court_domain'}


def record(task, today, action, **evidence):
    entry = dict(date=today.isoformat(), action=action, **deepcopy(evidence))
    history = task.setdefault('repair_history', [])
    # Повтор той же проверки не создаёт искусственную историю исправлений.
    if not any({k: v for k, v in old.items() if k != 'date'} ==
               {k: v for k, v in entry.items() if k != 'date'} for old in history):
        history.append(entry)


def _same(left, right):
    a, b = resolve_fi_identity(left), resolve_fi_identity(right)
    if (a.status != 'resolved' or b.status != 'resolved' or not a.court or not b.court
            or not a.domain or a.domain != b.domain or a.srv_num != b.srv_num):
        return False
    for name in ('link', 'judicial_uid', 'delo_id'):
        if left.get(name) and right.get(name) and str(left[name]) != str(right[name]):
            return False
    ln, rn = (_bare_case_number(x.get('case_number') or '') for x in (left, right))
    if ln and rn and ln != rn:
        return False
    ld, rd = (parse_date(act_anchor.date(x)) for x in (left, right))
    if ld and rd and ld != rd:
        return False
    same_link = bool(left.get('link') and left.get('link') == right.get('link'))
    same_uid = bool(left.get('judicial_uid') and left.get('judicial_uid') == right.get('judicial_uid'))
    return same_link or bool(ln and ln == rn and (same_uid or (ld and ld == rd)))


def _signature(block):
    return tuple(str(block.get(f) or '') for f in IDENTIFIERS)


def _created(task):
    try:
        return date.fromisoformat(str(task.get('created_at') or '')[:10])
    except ValueError:
        return None


def recoverable_date(task, block):
    """Не переносить старое бездатное ожидание на решение нового круга."""
    old = task.get('block') or {}
    if not _same(old, block) or block.get('act_source_review_reason'):
        return False
    day = parse_date(act_anchor.date(block))
    own = parse_date(act_anchor.date(old))
    created = _created(task)
    return bool(day and (own or (created and day.date() <= created)))


def reconcile(data, cases, today):
    """Изменить только доказанные пробелы и дубли; вернуть список действий."""
    from court_monitor import fi_act_watch, writ_watch
    entries = []
    for parent in cases:
        for episode in [parent] + list(parent.get('history') or []):
            block = episode.get('first_instance') or {}
            if block:
                entries.append((parent, episode, block))
    # Большая картотека не должна повторно обходить весь реестр судов для
    # каждой задачи: сначала узкий отбор по ссылке/номеру, затем полная сверка.
    by_link, by_number = {}, {}
    for entry in entries:
        block = entry[2]
        if block.get('link'):
            by_link.setdefault(block['link'], []).append(entry)
        num = _bare_case_number(block.get('case_number') or '')
        if num:
            by_number.setdefault(num, []).append(entry)

    def matching(block):
        pool = by_link.get(block.get('link'), []) + by_number.get(
            _bare_case_number(block.get('case_number') or ''), [])
        seen = set()
        for entry in pool:
            if id(entry[2]) not in seen:
                seen.add(id(entry[2]))
                if _same(block, entry[2]):
                    yield entry

    actions = []
    tasks = data.get(fi_act_watch.FIELD) or {}
    for old_key, task in list(tasks.items()):
        if task.get('status') != 'needs_review' or task.get('reason') != 'missing_decision_date':
            continue
        old = task['block']
        # Берём событие из живого/архивного производства; одного оторванного
        # от картотеки снимка задачи недостаточно для возобновления чтения.
        candidates = [b for _, _, b in matching(old) if act_anchor.evidence(b)]
        anchors = {(act_anchor.date(b), act_anchor.evidence(b).get('kind')) for b in candidates}
        if len(anchors) != 1:
            continue
        donor = candidates[0]
        if not recoverable_date(task, donor):
            continue
        if not fi_act_watch.card_url(donor):
            continue
        repaired = deepcopy(donor)
        new_key = fi_act_watch.identity(repaired)
        evidence = act_anchor.evidence(repaired)
        existing = tasks.get(new_key) if new_key != old_key else None
        if existing:
            if not _same(repaired, existing['block']):
                continue
            record(task, today, 'duplicate_act_task_closed', previous_key=old_key,
                   previous_reason=task['reason'], replacement_key=new_key, evidence=evidence)
            task.update(status='closed', reason='duplicate_dated_task', replacement_key=new_key,
                        next_check_at='')
            action = 'duplicate_act_task_closed'
        else:
            record(task, today, 'act_date_recovered', previous_key=old_key,
                   previous_reason=task['reason'], evidence=evidence)
            task.update(block=repaired, status='waiting', reason='', act_anchor=evidence)
            if fi_act_watch.has_text(repaired):
                task.update(status='complete', reason='known_text_in_snapshot', previously_announced=True)
            else:
                fi_act_watch._prepare(task, today, new_key)
            if new_key != old_key:
                tasks[new_key] = tasks.pop(old_key)
            action = 'act_date_recovered'
        actions.append({'queue': fi_act_watch.FIELD, 'key': new_key,
                        'previous_key': old_key, 'action': action})

    for old_key, task in list(tasks.items()):
        if task.get('status') != 'needs_review' or task.get('reason') not in GAPS:
            continue
        old = task['block']
        candidates = [b for _, _, b in matching(old) if fi_act_watch.card_url(b)]
        signatures = {_signature(b) for b in candidates}
        if len(signatures) != 1:
            continue
        donor = candidates[0]
        patch = {f: donor[f] for f in IDENTIFIERS if not old.get(f) and donor.get(f)}
        if not patch:
            continue
        repaired = dict(old, **patch)
        if not fi_act_watch.card_url(repaired) or not fi_act_watch.number(repaired):
            continue
        new_key = fi_act_watch.identity(repaired)
        if new_key != old_key and new_key in tasks:
            continue  # Слияние конфликтующих ожиданий требует отдельного доказательства.
        # Дозаполняем только совместимые снимки того же производства.
        targets = [b for _, _, b in matching(old)]
        for block in targets:
            for field, value in patch.items():
                if not block.get(field):
                    block[field] = value
        old.update(patch)
        record(task, today, 'identifiers_recovered', previous_key=old_key,
               previous_reason=task['reason'], fields=patch,
               source={f: donor.get(f) for f in IDENTIFIERS})
        task.update(status='waiting', reason='')
        fi_act_watch._prepare(task, today, new_key)
        if new_key != old_key:
            tasks[new_key] = tasks.pop(old_key)
        actions.append({'queue': fi_act_watch.FIELD, 'key': new_key,
                        'action': 'identifiers_recovered'})

    # Старые задачи с пустой датой иногда оставались рядом с уже созданной
    # датированной задачей того же листа. Закрываем дубль, а не вопрос ИЛ.
    tasks = data.get(writ_watch.FIELD) or {}
    for key, task in list(tasks.items()):
        old = task.get('block') or {}
        if (task.get('status') != 'needs_review' or old.get('decision_date')
                or old.get('decision_date_vacated') or not old.get('link')):
            continue
        candidates = []
        for parent, episode, block in matching(old):
            if episode is not parent or not block.get('decision_date'):
                continue
            new_key = writ_watch.identity(block)
            target = tasks.get(new_key)
            anchor = parse_date(block.get('decision_date'))
            created = _created(task)
            if (new_key != key and target and target.get('reason') == task.get('reason')
                    and _same(old, target['block']) and anchor and created
                    and anchor.date() <= created):
                candidates.append(new_key)
        if len(set(candidates)) != 1:
            continue
        replacement = candidates[0]
        record(task, today, 'duplicate_writ_task_closed', previous_reason=task['reason'],
               replacement_key=replacement,
               evidence={f: tasks[replacement]['block'].get(f) for f in IDENTIFIERS + ('decision_date',)})
        task.update(status='closed', reason='duplicate_dated_task', replacement_key=replacement,
                    next_check_at='')
        actions.append({'queue': writ_watch.FIELD, 'key': key,
                        'action': 'duplicate_writ_task_closed', 'replacement_key': replacement})

    for key, task in tasks.items():
        if task.get('status') != 'needs_review' or task.get('reason') in writ_watch.FROZEN_REASONS:
            continue
        matches = [(p, b) for p, e, b in matching(task['block']) if e is p
                   and writ_watch.identity(b) == key]
        states = {writ_watch.state(parent, today) for parent, _ in matches}
        if len(states) != 1 or next(iter(states))[0] == 'needs_review':
            continue
        # Противоречащие актуальные снимки и защитный флаг не обходятся.
        if any(b.get('writ_source_review_reason') for _, b in matches):
            continue
        status, reason = next(iter(states))
        if status != 'closed':
            continue  # Положительное основание исполнения требует отдельной сверки.
        record(task, today, 'writ_obligation_closed', previous_reason=task['reason'],
               evidence={'reason': reason, 'case_ids': sorted({p.get('id') or '' for p, _ in matches})})
        task.update(status=status, reason=reason, next_check_at='')
        actions.append({'queue': writ_watch.FIELD, 'key': key, 'action': 'writ_obligation_closed'})
    return actions
