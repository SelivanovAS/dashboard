"""Общие ограничения двух очередей: календарь, бюджет и повторы."""
from __future__ import annotations

import hashlib
import time
from datetime import date, datetime, timedelta

from court_monitor.textutil import parse_date

VERSION = 2
MAX_AGE = 180
BUDGET_SECONDS = 600
BACKFILL_SECONDS = 180
BACKFILL_CARDS = 10
MAX_DAILY_ATTEMPTS = 2
RETRY_SECONDS = 1800


def age(block, today, anchor):
    value = parse_date(anchor(block))
    return (today - value.date()).days if value else None


def next_check(block, today, anchor, key=''):
    days_old = age(block, today, anchor)
    if days_old is None or days_old > MAX_AGE:
        return None
    try:
        last = date.fromisoformat(str(block.get('last_checked_at') or '')[:10])
    except ValueError:
        return today
    if days_old <= 30:
        due = last + timedelta(days=1)
        while due.weekday() >= 5:
            due += timedelta(days=1)
        return due
    seed = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)
    if days_old <= 90:
        due = last + timedelta(days=1)
        while due.weekday() != seed % 5:
            due += timedelta(days=1)
        return due
    # Постоянный день месяца 1..28; календарные месяцы, а не накопление
    # всех дел на «через 30 суток после массового первого прохода».
    month = last.month + 1
    due = date(last.year + (month == 13), 1 if month == 13 else month, seed % 28 + 1)
    while due.weekday() >= 5:
        due += timedelta(days=1)
    return due


def prepare(task, today, anchor, key):
    block = task['block']
    task.setdefault('policy_version', VERSION)
    if task['status'] == 'complete':
        return
    if task['status'] == 'needs_review':
        return
    days_old = age(block, today, anchor)
    if days_old is None:
        task.update(status='needs_review', reason='missing_decision_date')
    elif days_old > MAX_AGE:
        task.update(status='expired', reason='age_limit', next_check_at='')
    else:
        task['status'] = 'waiting'
        due = next_check(block, today, anchor, key)
        task['next_check_at'] = due.isoformat() if due else ''


def phase(task):
    return 'waiting' if task['block'].get('act_absent_checked_at') else 'backfill'


def retry_reason(task, now):
    day = now.date().isoformat()
    if task.get('attempt_day') != day:
        return ''
    if task.get('attempt_count', 0) >= MAX_DAILY_ATTEMPTS:
        return 'daily_retry_limit'
    previous = task.get('last_attempt_at')
    if previous:
        try:
            if (now - datetime.fromisoformat(previous)).total_seconds() < RETRY_SECONDS:
                return 'retry_cooldown'
        except (ValueError, TypeError):
            pass
    return ''


def attempt(task, now):
    if task.get('attempt_day') != now.date().isoformat():
        task['attempt_count'] = 0
    task['attempt_day'] = now.date().isoformat()
    task['attempt_count'] = task.get('attempt_count', 0) + 1
    task['last_attempt_at'] = now.isoformat(timespec='seconds')


class Budget:
    """Один бюджет на обе инстанции; лимит догрузки переживает новые слоты."""
    def __init__(self, data, today, *, allow_backfill=True, clock=time.monotonic):
        self.clock = clock
        self.started = clock()
        self.allow_backfill = allow_backfill
        previous = data.get('act_watch_budget') or {}
        if previous.get('date') != today.isoformat():
            previous = {'date': today.isoformat(), 'backfill_count': 0, 'backfill_seconds': 0}
        previous.setdefault('backfill_count', 0)
        previous.setdefault('backfill_seconds', 0)
        interrupted = previous.pop('backfill_inflight_started', None)
        if interrupted is not None:
            elapsed = clock() - interrupted
            previous['backfill_seconds'] += min(BACKFILL_SECONDS, elapsed) if elapsed >= 0 else BACKFILL_SECONDS
        self.state = data['act_watch_budget'] = previous

    def remaining(self, kind):
        remaining = BUDGET_SECONDS - (self.clock() - self.started)
        if kind == 'backfill':
            if not self.allow_backfill or self.state['backfill_count'] >= BACKFILL_CARDS:
                return 0
            remaining = min(remaining, BACKFILL_SECONDS - self.state['backfill_seconds'])
        return max(0, remaining)

    def begin(self, kind):
        if kind == 'backfill':
            self.state['backfill_count'] += 1
            self.state['backfill_inflight_started'] = self.clock()
        return self.clock()

    def finish(self, kind, started):
        if kind == 'backfill':
            self.state['backfill_seconds'] += self.clock() - started
            self.state.pop('backfill_inflight_started', None)


def merge_reports(first, second):
    result = dict(second)
    for field in ('due', 'planned', 'read', 'published', 'backfilled', 'unplanned', 'unread'):
        result[field] = first.get(field, 0) + second.get(field, 0)
    result['items'] = first.get('items', []) + second.get('items', [])
    result['long_wait'] = first.get('long_wait', 0) + second.get('long_wait', 0)
    return result
