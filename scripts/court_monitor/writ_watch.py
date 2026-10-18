"""Контроль листов по требованиям банка независимо от текущей инстанции.

Задача не устанавливает юридическую силу изменённого вышестоящим судом
решения по старому расчёту. Неясное основание остаётся видимым для проверки.

Чтение карточек и запись полных снимков в архивы выполняет card_refresh.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta

from court_monitor import config, lifecycle
from court_monitor.storage import load_json, save_json
from court_monitor.textutil import parse_date

FIELD = 'writ_watch'
PENDING = 'pending_writ_changes'
FROZEN_REASONS = ('identity_conflict', 'decision_date_changed', 'fi_production_reopened')


def bank_claim(case):
    role = (case.get('bank_role') or '').strip().lower()
    if role and role != 'истец':
        return False
    return role == 'истец' or case.get('track') == 'plaintiff_light' or case.get('track_origin') == 'plaintiff_light'


def identity(fi):
    domain, num = fi.get('court_domain'), fi.get('case_number')
    if not domain or not num:
        return ''
    return '|'.join(str(v or '') for v in (domain, fi.get('srv_num') or 1, num,
                                         fi.get('decision_date') or fi.get('decision_date_vacated')))


def state(case, today):
    fi = case.get('first_instance') or {}
    if not bank_claim(case):
        return 'closed', 'bank_not_plaintiff'
    if case.get('current_stage') == 'awaiting_relink' or lifecycle.default_cancellation_state(fi)['outcome'] == 'cancelled':
        return 'needs_review', 'decision_cancelled'
    # Изменение/отмена в любой вышестоящей инстанции требует нового основания.
    ap = case.get('appeal') or {}
    cass = case.get('cassation') or {}
    outcome = (cass.get('outcome') or '').lower()
    ap_result = (ap.get('result') or '').lower()
    if outcome in ('cassation_remanded', 'cassation_reversed', 'cassation_modified') or 'отмен' in ap_result or 'изменено' in ap_result:
        return 'needs_review', 'basis_changed'
    if not lifecycle.bank_writ_awaited(fi):
        return 'closed', 'writ_not_required'
    if fi.get('status') != 'Решено':
        return 'closed', 'no_decision'
    enforcement = [w for w in fi.get('writs') or [] if lifecycle.classify_writ_kind(w, fi) == 'enforcement']
    if enforcement:
        if any(any(word in (w.get('status') or '').lower() for word in ('отозв', 'возвращ')) for w in enforcement):
            return 'needs_review', 'writ_recalled_or_returned'
        issued = [parse_date(w.get('issue_date') or '') for w in enforcement]
        issued = [d.date() for d in issued if d]
        window = config.BANK_DEFAULT_WRIT_ARCHIVE_DAYS if lifecycle.bank_default_judgment_info(fi).get('default_judgment') else config.BANK_WRIT_ARCHIVE_DAYS
        if issued and (today - max(issued)).days >= window:
            return 'complete', 'issued_observation_finished'
        return 'monitoring', 'issued_status_observation'
    if case.get('current_stage') in ('appeal', 'awaiting_appeal'):
        return 'waiting', 'appeal_pending'
    return 'waiting', 'awaiting_writ'


def sync(data, cases, today):
    tasks = data.setdefault(FIELD, {})
    for case in cases:
        fi = case.get('first_instance') or {}
        key = identity(fi)
        if not key or (not bank_claim(case) and key not in tasks):
            continue
        status, reason = state(case, today)
        if key not in tasks and status == 'closed':
            continue
        task = tasks.setdefault(key, {'created_at': today.isoformat(), 'block': deepcopy(fi)})
        if fi.get('writ_source_review_reason') in FROZEN_REASONS and status != 'closed':
            task.update(status='needs_review', reason=fi['writ_source_review_reason'])
        if task.get('reason') in FROZEN_REASONS and status != 'closed':
            fi['writ_watch_status'] = 'needs_review'
            fi['writ_watch_reason'] = task['reason']
            fi.pop('legal_force_est', None)
            fi.pop('writ_awaited_since', None)
            continue
        old = task['block']
        if any(old.get(k) and fi.get(k) and old[k] != fi[k] for k in ('link', 'judicial_uid')):
            task.update(status='needs_review', reason='identity_conflict')
            continue
        # Задача может быть новее карточки после остановки между checkpoint и save.
        observed_at = max(str(fi.get('writ_checked_at') or ''), str(fi.get('last_checked_at') or ''))
        if str(old.get('writ_checked_at') or '') > observed_at:
            for field in ('writs', 'writ_checked_at', 'result', 'writ_observed_bank_role'):
                if field in old:
                    fi[field] = deepcopy(old[field])
            if fi.get('writ_observed_bank_role'):
                case['bank_role'] = fi['writ_observed_bank_role']
            status, reason = state(case, today)
        task.update(status=status, reason=reason, block=deepcopy(fi),
                    parent={k: deepcopy(case.get(k)) for k in ('id', 'plaintiff', 'defendant', 'bank_role', 'track', 'track_origin', 'current_stage')})
        # Расчёт FI не подтверждает силу после перехода в вышестоящую инстанцию.
        if case.get('current_stage') != 'first_instance' or status in ('closed', 'needs_review'):
            fi.pop('legal_force_est', None)
            fi.pop('writ_awaited_since', None)
        fi['writ_watch_status'] = status
        fi['writ_watch_reason'] = reason
        try:
            last = date.fromisoformat(str(fi.get('writ_checked_at') or fi.get('last_checked_at') or '')[:10])
        except ValueError:
            last = None
        anchor = parse_date(fi.get('decision_date') or '')
        long_wait = bool(anchor and (today - anchor.date()).days > 180)
        task['long_wait'] = long_wait
        # Длительное ожидание остаётся в отчёте, ограниченный месячный контроль.
        interval = 30 if long_wait else config.BANK_WRIT_CHECK_DAYS
        due = last + timedelta(days=interval) if last else today
        task['next_check_at'] = due.isoformat() if status in ('waiting', 'monitoring', 'needs_review') else ''
    return tasks


def observe(case, card, today, *, baseline=False):
    """Один прочитанный ответ обслуживает и обычный обход, и контроль ИЛ."""
    fi = case['first_instance']
    fi['writ_checked_at'] = today.isoformat()
    new = card.get('_writs') or []
    old = fi.get('writs') or []
    def key(w):
        return tuple(w.get(k) or '' for k in ('issue_date', 'blank_number', 'electronic_id'))
    index = {key(w): w for w in old}
    issued = [{**w, 'kind': lifecycle.classify_writ_kind(w, fi)} for w in new if key(w) not in index]
    changed = [{**w, 'old_status': index[key(w)].get('status') or '',
                'kind': lifecycle.classify_writ_kind(w, fi)} for w in new
               if key(w) in index and w.get('status') != index[key(w)].get('status')]
    # Пустая вкладка не стирает уже известный лист.
    if new:
        fi['writs'] = deepcopy(new)
    if baseline or not (issued or changed):
        return None
    types, details = [], {'link': fi.get('link') or '', 'court_domain': fi.get('court_domain') or ''}
    if issued:
        types.append('fi_writ_issued')
        details['writs'] = issued
    if changed:
        types.append('fi_writ_status_changed')
        details['writ_status_changes'] = changed
    return {'case': fi.get('case_number'), 'court': fi.get('court'),
            'plaintiff': case.get('plaintiff') or '', 'defendant': case.get('defendant') or '',
            'bank_role': case.get('bank_role') or '', 'track': 'plaintiff_light',
            'type': types, 'details': details}


def checkpoint(data):
    disk = load_json(config.JSON_PATH)
    for field in (FIELD, PENDING):
        if field in data:
            disk[field] = deepcopy(data[field])
    save_json(disk, config.JSON_PATH)
