"""Аудит пересказов одного выпуска: без исходников, ответов и секретов.

Один акт одного суда/инстанции учитывается один раз, даже если шаблон
дайджеста обращается к нему повторно. В начале выпуска вызывается reset().
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json

from court_monitor import config
from court_monitor.act_preparation import prepare_act, source_hash

_records = {}
_ATTEMPT_FIELDS = ('provider', 'model', 'status', 'context_limit',
                   'input_token_upper_bound', 'output_reserve', 'token_estimate')


def reset():
    _records.clear()


def identity(text, meta):
    """Хэш отделяет одинаковые номера разных судов и инстанций."""
    values = [config.REGION, source_hash(text), meta.get('court_domain', ''),
              meta.get('stage', ''), meta.get('case_number', ''),
              meta.get('judicial_uid', '')]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()


def record(identity, outcome, cached=False):
    """Сохраняем только служебные поля; успешный свежий ответ сильнее кэша."""
    previous = _records.get(identity)
    if previous and previous['status'] == 'ready' and (
            cached or (not previous['cached'] and outcome.get('status') != 'ready')):
        return
    attempts = [{key: value for key, value in attempt.items()
                 if key in _ATTEMPT_FIELDS and isinstance(value, (str, int, float, bool))}
                for attempt in (outcome.get('attempts') or []) if isinstance(attempt, dict)]
    fallback = outcome.get('fallback')
    if fallback is None:
        # Старая очередь уже хранила attempts, но ещё не флаг fallback.
        # История запроса доказывает резерв; текущая конфигурация — нет.
        ready_provider = next((attempt.get('provider') for attempt in reversed(attempts)
                               if attempt.get('status') == 'ready'), None)
        if ready_provider and any(attempt.get('provider') not in (None, ready_provider)
                                  for attempt in attempts):
            fallback = True
    _records[identity] = {
        'id': identity,
        'status': outcome.get('status') or 'technical_error',
        'model': outcome.get('model') or 'unknown',
        'cached': bool(cached),
        'fallback': fallback,
        'attempts': attempts,
    }


def snapshot():
    """Независимый JSON-совместимый снимок для технического отчёта."""
    return deepcopy(list(_records.values()))


BACKLOG_CATEGORIES = ('ready', 'superseded', 'waiting_source', 'needs_review', 'retry_pending')
_SOURCE_STATUSES = frozenset({'source_incomplete', 'empty_source', 'text_extraction_required'})
_REVIEW_STATUSES = frozenset({'needs_review', 'refused', 'refusal', 'provider_refusal',
                              'source_conflict'})
_RETRY_STATUSES = frozenset({'pending', 'retry_pending', 'technical_error', 'timeout', 'network',
                            'rate_limit', 'daily_quota', 'missing_key', 'missing_keys',
                            'context_exceeded', 'context_unknown', 'invalid_response',
                            'empty_response', 'output_limit', 'invalid_answer', 'answer_too_long',
                            'outcome_conflict', 'recheck_context_exceeded'})
_META_TEXT_FIELDS = ('case_number', 'cassation_number', 'judicial_uid', 'document_uid',
                     'source_url', 'court_domain', 'stage', 'act_received_at')


def classify_job(job):
    """Классифицировать сохранённую работу без сети, записи и вызова модели.

    Старый pending не доказывает, что источник пригоден: проверяем его тем же
    stdlib-подготовщиком, который защищает вход модели. Неизвестная структура
    требует проверки и не может исчезнуть из отчёта как нулевой остаток.
    """
    if not isinstance(job, dict) or not isinstance(job.get('status'), str):
        return 'needs_review'
    if any(key in job and not isinstance(job[key], str)
           for key in ('source_hash', 'retry_after', 'updated_at')):
        return 'needs_review'
    if 'runs' in job and (isinstance(job['runs'], bool) or
                          not isinstance(job['runs'], int) or job['runs'] < 0):
        return 'needs_review'
    status = job['status']
    if status == 'superseded':
        return 'superseded'
    if job.get('summary') is not None and not isinstance(job['summary'], str):
        return 'needs_review'
    if status in _REVIEW_STATUSES or status.endswith('_mismatch'):
        return 'needs_review'
    retryable = status in _RETRY_STATUSES or (
        status.startswith('http_') and status[5:].isdigit())
    if status not in _SOURCE_STATUSES and status != 'ready' and not retryable:
        return 'needs_review'
    text, meta = job.get('text'), job.get('meta')
    if not isinstance(text, str) or not isinstance(meta, dict):
        return 'needs_review'
    if any(meta.get(key) is not None and not isinstance(meta[key], str)
           for key in _META_TEXT_FIELDS):
        return 'needs_review'
    try:
        prepared = prepare_act(text, meta)
    except (TypeError, ValueError):
        return 'needs_review'
    if prepared.status in _SOURCE_STATUSES:
        return 'waiting_source'
    if prepared.status != 'ready':
        return 'needs_review'
    if status == 'ready':
        summary = job.get('summary')
        return 'ready' if isinstance(summary, str) and summary.strip() else 'needs_review'
    if status in _SOURCE_STATUSES:
        return 'waiting_source'
    return 'retry_pending'


def backlog_snapshot(jobs):
    """Только счётчики: без исходников, реквизитов и изменения записей."""
    result = dict.fromkeys(BACKLOG_CATEGORIES, 0)
    if not isinstance(jobs, dict):
        result['needs_review'] = 1
        return result
    for job in jobs.values():
        result[classify_job(job)] += 1
    return result
