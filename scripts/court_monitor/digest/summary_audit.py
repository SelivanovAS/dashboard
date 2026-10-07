"""Аудит пересказов одного выпуска: без исходников, ответов и секретов.

Один акт одного суда/инстанции учитывается один раз, даже если шаблон
дайджеста обращается к нему повторно. В начале выпуска вызывается reset().
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json

from court_monitor import config
from court_monitor.act_preparation import source_hash

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
