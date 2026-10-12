"""Единый дополнительный план карточек: ИЛ, ожидаемый текст, пробел в базе.

Причины определяют срок и приоритет, но не состав обновляемых полей. Один URL
читается и разбирается один раз. Обработчики те же, что у основного обхода.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
import glob
import hashlib
import json
import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from court_monitor import (act_watch, appeal_act_watch, fi_act_watch, writ_watch,
                           act_watch_policy as policy, config, telemetry)
from court_monitor.courts import cassation_card_url
from court_monitor.netutil import limit_run_deadline, run_deadline_remaining, mark_last_fetch_semantic
from court_monitor.parsing.cards import parse_case_card, card_is_empty_shell, _warn_if_card_degraded
from court_monitor.parsing.cassation import parse_cassation_card
from court_monitor.storage import load_json, save_json, load_bank_json, save_bank_json
from court_monitor.textutil import _bare_case_number, parse_date

JOURNAL = 'pending_card_refresh'
STAGES = {act_watch.FIELD: 'cassation', appeal_act_watch.FIELD: 'appeal',
          fi_act_watch.FIELD: 'first_instance', writ_watch.FIELD: 'first_instance'}
MODULES = (act_watch, appeal_act_watch, fi_act_watch, writ_watch)
CONTEXT = {'cassation': 'cass_changes', 'appeal': 'changes', 'first_instance': 'fi_changes'}
META = ('plaintiff', 'defendant', 'bank_role', 'category', 'track', 'track_origin')
_SEEN = ContextVar('full_card_reads', default=None)


@contextmanager
def card_session():
    token = _SEEN.set(set())
    try:
        yield
    finally:
        _SEEN.reset(token)


def note_read(stage, url):
    seen = _SEEN.get()
    if seen is not None and url:
        seen.add(card_key(stage, url))



def card_key(stage, url):
    """Порядок query-параметров не меняет карточку; суд/раздел/площадка меняют."""
    u = urlsplit(url)
    return stage + '|' + urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path,
                                    urlencode(sorted(parse_qsl(u.query, keep_blank_values=True))), ''))


def productions(cases):
    for parent in cases:
        yield parent, parent
        for episode in parent.get('history') or []:
            if isinstance(episode, dict):
                yield parent, episode


def _url(stage, block):
    if stage == 'first_instance':
        return fi_act_watch.card_url(block)
    if stage == 'appeal':
        return appeal_act_watch.card_url(block)
    return cassation_card_url(block)


def check_document_source(block, info):
    """Чужую ссылку не скачиваем, остальные сведения карточки обновляем."""
    info = dict(info)
    source = info.get('_act_url')
    if source and urlsplit(source).hostname != block.get('court_domain'):
        block['act_source_review_reason'] = 'document_host_mismatch'
        info['_act_source_blocked'] = True
        info['act_text'] = ''
    return info


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _identity(block):
    return tuple(str(block.get(k) or '') for k in
                 ('court_domain', 'srv_num', 'link', 'case_number', 'judicial_uid'))


def checkpoint(data):
    disk = load_json(config.JSON_PATH)
    for field in (JOURNAL, 'act_watch_budget', 'pending_cassation_changes',
                  'pending_appeal_act_changes', fi_act_watch.PENDING, writ_watch.PENDING) + tuple(m.FIELD for m in MODULES):
        if field in data:
            disk[field] = deepcopy(data[field])
    save_json(disk, config.JSON_PATH)


def restore(data, cases):
    """Дописать полный снимок после прерывания, только поверх исходной версии.

    Не откатываем более свежую карточку и не переносим старый круг в новый.
    События живут в том же журнале до подтверждения контекста выпуска.
    """
    index = {}
    for parent, episode in productions(cases):
        for stage in CONTEXT:
            block = episode.get(stage) or {}
            index.setdefault((stage, _identity(block)), []).append((parent, episode))
    changed = False
    for record in data.get(JOURNAL) or []:
        if record.get('region') != config.REGION:
            continue
        stage = record['stage']
        for parent, episode in index.get((stage, tuple(record['identity'])), []):
            block = episode[stage]
            already_applied = _fingerprint(block) == _fingerprint(record['block'])
            if not already_applied:
                if _fingerprint(block) != record['before']:
                    # sync может добавить служебные поля до checkpoint, но
                    # более свежую полную проверку заменять нельзя.
                    checked = str(block.get('last_checked_at') or '')
                    if not (checked < str(record['block'].get('last_checked_at') or '')
                            and checked <= record.get('previous_checked_at', '')
                            and (block.get('decision_date') or '') == record.get('previous_decision_date', '')):
                        continue
                block.clear()
                block.update(deepcopy(record['block']))
                changed = True
            for key, patch in record.get('side_updates', {}).items():
                if _fingerprint(episode.get(key)) == patch['before']:
                    episode[key] = deepcopy(patch['value'])
                    changed = True
            # Событие уже записано рядом со снимком. Старый синхронизатор
            # ожиданий после перезапуска не должен создать его второй раз.
            for module in MODULES:
                if module == writ_watch or STAGES[module.FIELD] != stage:
                    continue
                task = (data.get(module.FIELD) or {}).get(module.identity(block))
                if task is not None and block.get('act_text') and not block.get('act_summary_needs_source'):
                    task.update(block=deepcopy(block), status='complete',
                                completed_at=block.get('last_checked_at') or '')
    return changed


def persist_archives(data):
    paths = [config.JSON_ARCHIVE_PATH] + glob.glob(config.cold_archive_glob())
    if config.BANK_TRACK:
        paths += [config.JSON_BANK_ARCHIVE_PATH] + [p for p in glob.glob(config.bank_cold_archive_glob())
                                                   if config.is_bank_cold_archive_file(p)]
    for path in dict.fromkeys(paths):
        if not os.path.exists(path):
            continue
        bank = path == config.JSON_BANK_ARCHIVE_PATH
        doc = load_bank_json(path, config.JSON_BANK_ARCHIVE_EVENTS_PATH) if bank else load_json(path)
        if restore(data, doc.get('cases') or []):
            if bank:
                save_bank_json(doc, path, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
            else:
                save_json(doc, path)


def pending_context(data):
    result = {field: [] for field in CONTEXT.values()}
    for record in data.get(JOURNAL) or []:
        if record.get('region') == config.REGION:
            result[CONTEXT[record['stage']]].extend(deepcopy(record.get('changes') or []))
    return result


def acknowledge(data, context, issue_key=None):
    if not data.get(JOURNAL):
        return
    if any(r.get('region') != config.REGION for r in data[JOURNAL]):
        raise RuntimeError('журнал карточек относится к другой территории')
    if issue_key is not None and context.get('issue_key') != issue_key:
        raise RuntimeError('полное обновление карточек не подтверждено выпуском')
    def included(expected, actual):
        # Связка может дополнить событие (например, номером дела-приёмника),
        # а накопление — объединить типы. Подтверждаем весь исходный смысл,
        # не требуя байтового равенства до и после такого обогащения.
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(
                included(value, actual.get(key)) for key, value in expected.items()
                if value not in ('', None, [], {}))
        if isinstance(expected, list):
            return isinstance(actual, list) and all(
                any(included(value, candidate) for candidate in actual) for value in expected)
        return expected == actual
    expected = pending_context(data)
    if any(not any(included(event, saved) for saved in context.get(field, []))
           for field, events in expected.items() for event in events):
        raise RuntimeError('полное обновление карточек не сохранено в контексте выпуска')
    previous = data.pop(JOURNAL, None)
    try:
        save_json(data, config.JSON_PATH)
    except Exception:
        if previous is not None:
            data[JOURNAL] = previous
        raise


def _report(today):
    return dict(date=today.isoformat(), region=config.REGION, waiting=0, due=0,
                planned=0, read=0, unread=0, published=0, backfilled=0,
                unplanned=0, needs_review=0, expired=0, long_wait=0, items=[])


def plan(data, cases, today, *, force=False, now=None):
    now = now or datetime.combine(today, datetime.now().time())
    refs = {}
    for parent, episode in productions(cases):
        for module in MODULES:
            stage = STAGES[module.FIELD]
            block = episode.get(stage) or {}
            key = module.identity(block)
            if key:
                refs.setdefault((module.FIELD, key), []).append((parent, episode))
    reports = {m.FIELD: _report(today) for m in MODULES}
    cards = {}
    for module in MODULES:
        tasks = module.sync(data, cases, today)
        report = reports[module.FIELD]
        stage = STAGES[module.FIELD]
        for key, task in tasks.items():
            status = task.get('status')
            if status in ('complete', 'closed'):
                continue
            report['waiting'] += status in ('waiting', 'monitoring')
            report['needs_review'] += status == 'needs_review'
            report['expired'] += status == 'expired'
            item = dict(key=key, number=task['block'].get('case_number'),
                        next_check_at=task.get('next_check_at'), reason=task.get('reason') or '')
            report['items'].append(item)
            if status == 'expired':
                item['reason'] = 'age_limit'
                continue
            frozen = status == 'needs_review' and (module != writ_watch or task.get('reason') in
                writ_watch.FROZEN_REASONS + ('basis_changed', 'decision_cancelled'))
            if frozen:
                report['unplanned'] += 1
                continue
            due = task.get('next_check_at') or ''
            if not due or (not force and due > today.isoformat()):
                item['reason'] = 'scheduled'
                continue
            report['due'] += 1
            if module == writ_watch:
                court = fi_act_watch._court(task['block'])
                if court is None or not court.enabled:
                    item['reason'] = 'unsupported_court_site'
                    report['unplanned'] += 1
                    continue
            targets = refs.get((module.FIELD, key), [])
            url = _url(stage, task['block'])
            if not targets or not url:
                item['reason'] = 'missing_production' if not targets else 'missing_card_link'
                report['unplanned'] += 1
                continue
            if card_key(stage, url) in (_SEEN.get() or set()):
                item['reason'] = 'checked_this_run'
                continue
            report['planned'] += 1
            kind = 'waiting' if module == writ_watch else policy.phase(task)
            priority = 0 if module == writ_watch else 1 if kind == 'waiting' else 2
            identity = card_key(stage, url)
            card = cards.setdefault(identity, dict(key=identity, stage=stage, url=url,
                priority=priority, kind=kind, targets=[], obligations=[], reasons=[]))
            card['priority'] = min(priority, card['priority'])
            if kind == 'waiting':
                card['kind'] = 'waiting'
            reason = 'writ' if module == writ_watch else 'text_waiting' if kind == 'waiting' else 'text_missing'
            if reason not in card['reasons']:
                card['reasons'].append(reason)
            for target in targets:
                if not any(target[1] is old[1] for old in card['targets']):
                    card['targets'].append(target)
            retry = '' if force else policy.retry_reason(task, now)
            card['obligations'].append((module, task, item, report, retry))
    # Сохраняем дневную ротацию инстанций внутри равного приоритета.
    rotation = ['cassation', 'appeal', 'first_instance']
    offset = today.toordinal() % 3
    rotation = rotation[offset:] + rotation[:offset]
    return sorted(cards.values(), key=lambda c: (c['priority'], rotation.index(c['stage']), c['key'])), reports


def _validated_info(card, html):
    url, stage = card['url'], card['stage']
    if not html:
        return None, (config.FETCH_DIAG or {}).get('kind') or 'fetch_error'
    base = 'https://' + urlsplit(url).hostname
    info = parse_cassation_card(html, base) if stage == 'cassation' else parse_case_card(html, base)
    if not info or (stage != 'cassation' and card_is_empty_shell(info)):
        mark_last_fetch_semantic('empty_shell', url)
        return None, 'empty_shell'
    for _, episode in card['targets']:
        block = episode[stage]
        number = info.get('page_case_number') if stage == 'cassation' else info.get('Номер дела (карточка)')
        uid = info.get('judicial_uid') if stage == 'cassation' else info.get('УИД')
        if (_bare_case_number(number or '') != _bare_case_number(block.get('case_number') or '')
                or (block.get('judicial_uid') and uid != block['judicial_uid'])):
            return None, 'identity_mismatch'
        if stage != 'cassation' and _warn_if_card_degraded(info, number, case_block=block) == 'degraded':
            mark_last_fetch_semantic('degraded_card', url)
            return None, 'degraded_card'
    return info, ''


def refresh(data, cases, today, fetch, apply, *, budget, force=False, persist=checkpoint, now=None):
    """apply(stage, episode_view, info, today, url, initial_snapshot=...) -> changes."""
    now = now or datetime.combine(today, datetime.now().time())
    cards, reports = plan(data, cases, today, force=force, now=now)
    result = dict(date=today.isoformat(), region=config.REGION, planned=len(cards), read=0, items=[],
                  instances={stage: dict(planned=0, read=0) for stage in CONTEXT})
    for card in cards:
        result['instances'][card['stage']]['planned'] += 1
        block = card['targets'][0][1][card['stage']]
        telemetry.register_planned_case_ids(card['stage'], [block['court_domain'] + '|' + block['case_number']])
    persist(data)
    for card in cards:
        outcome = dict(key=card['key'], stage=card['stage'], reasons=card['reasons'], reason='')
        result['items'].append(outcome)
        active = [o for o in card['obligations'] if not o[4]]
        reason = budget.reason(card['kind'], run_remaining=run_deadline_remaining())
        if not active:
            reason = card['obligations'][0][4]
        if reason:
            outcome['reason'] = reason
            for _, _, item, _, retry in card['obligations']:
                item['reason'] = retry or reason
            continue
        for _, task, _, _, _ in active:
            policy.attempt(task, now)
        started = budget.begin(card['kind'])
        persist(data)
        try:
            with limit_run_deadline(budget.remaining(card['kind'])):
                html = fetch(card['url'], context='; '.join(card['reasons']))
                info, reason = _validated_info(card, html)
                if reason:
                    outcome['reason'] = reason
                    for _, task, item, _, _ in card['obligations']:
                        item['reason'] = reason
                        if reason == 'identity_mismatch':
                            task.update(status='needs_review', reason=reason)
                    continue
                records = []
                for parent, episode in card['targets']:
                    stage = card['stage']
                    old = episode[stage]
                    view = deepcopy(episode)
                    for key in META:
                        if key not in view and key in parent:
                            view[key] = deepcopy(parent[key])
                    # Исторический круг остаётся отдельным производством.
                    view.pop('history', None)
                    view.setdefault('id', old.get('case_number'))
                    observed = (info.get('decision_date') if stage == 'cassation' else
                                fi_act_watch.fi_decision_date_from_events(info.get('_events') or [])
                                or info.get('Дата рассмотрения (карточка)') or '')
                    expected = fi_act_watch.decision_date(old) if stage == 'first_instance' else old.get('decision_date')
                    if episode is not parent and parse_date(expected) and parse_date(observed) and parse_date(expected) != parse_date(observed):
                        outcome['reason'] = 'production_changed'
                        for _, task, item, _, _ in card['obligations']:
                            task.update(status='needs_review', reason='production_changed')
                            item['reason'] = 'production_changed'
                        records = []
                        break
                    changes = apply(stage, view, deepcopy(info), today, card['url'],
                                    initial_snapshot=(not bool(old.get('last_checked_at')) or 'text_missing' in card['reasons']))
                    if stage == 'first_instance' and not config.BANK_DIGEST_ROUTINE:
                        from court_monitor.lifecycle import filter_bank_routine_events
                        changes = filter_bank_routine_events(changes)
                    records.append(dict(region=config.REGION, stage=stage, identity=list(_identity(old)),
                        before=_fingerprint(old), previous_checked_at=str(old.get('last_checked_at') or ''),
                        previous_decision_date=old.get('decision_date') or '', block=deepcopy(view[stage]),
                        side_updates={k: {'before': _fingerprint(episode.get(k)), 'value': deepcopy(v)}
                                      for k, v in view.items()
                                      if k not in (stage, 'current_stage', 'history', 'round', 'id')
                                      and v != episode.get(k)}, changes=changes))
                if not records:
                    continue
                # Сначала фиксируем снимок и события. До этого момента исходные
                # карточки не изменяются даже при исключении в обработчике.
                data.setdefault(JOURNAL, []).extend(records)
                persist(data)
                restore(data, cases)
            result['read'] += 1
            result['instances'][card['stage']]['read'] += 1
            outcome['reason'] = 'checked'
            block = card['targets'][0][1][card['stage']]
            telemetry.mark_case_read(card['stage'], block['court_domain'] + '|' + block['case_number'])
            for module, task, item, report, _ in card['obligations']:
                report['read'] += 1
                item['reason'] = 'checked'
                # Обязательство хранит полный блок, а не только текст/ИЛ.
                target = next((episode for _, episode in card['targets']
                               if module.identity(episode[card['stage']]) == module.identity(task['block'])), None)
                if target is not None:
                    task['block'] = deepcopy(target[card['stage']])
                    if module != writ_watch and task['block'].get('act_text') and not task['block'].get('act_summary_needs_source'):
                        task.update(status='complete', completed_at=today.isoformat())
                        kind = task['block'].get('act_notification_kind')
                        report['published' if kind == 'new_publication' and not task.get('previously_announced') else 'backfilled'] += 1
        finally:
            budget.finish(card['kind'], started)
            persist(data)
    for module in MODULES:
        tasks = module.sync(data, cases, today)
        report = reports[module.FIELD]
        report['waiting'] = sum(t.get('status') in ('waiting', 'monitoring') for t in tasks.values())
        report['needs_review'] = sum(t.get('status') == 'needs_review' for t in tasks.values())
        report['unread'] = report['planned'] - report['read']
    result['unread'] = result['planned'] - result['read']
    persist(data)
    return result, reports
