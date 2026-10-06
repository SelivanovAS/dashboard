"""Сохраняемая очередь пересказов, независимая от объявления актов.

Повтор обновляет только карточку и кэш, не создаёт события/рассылки.
Один владелец очереди обеспечивается общим lock прогона/replay.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
import hashlib
import json
import os

from court_monitor import config
from court_monitor.act_preparation import source_hash, prepare_act
from court_monitor.textutil import _bare_case_number
from court_monitor.config import log
from court_monitor.digest import llm

_WAIT_FOR_SOURCE = {'source_incomplete', 'empty_source', 'text_extraction_required',
                    'case_mismatch', 'court_mismatch', 'uid_mismatch', 'stage_mismatch',
                    'refused', 'source_conflict'}


def _load():
    try:
        with open(config.ACT_SUMMARY_PENDING_PATH, encoding='utf-8') as f:
            value = json.load(f)
            if not isinstance(value, dict):
                raise ValueError('очередь не является объектом')
            return value
    except FileNotFoundError:
        return {}
    # Повреждённый файл не заменяем пустым: ошибка должна сохранить данные.


def _save(jobs):
    path = config.ACT_SUMMARY_PENDING_PATH
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path + '.tmp', 'w', encoding='utf-8') as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
        f.write('\n')
    os.replace(path + '.tmp', path)


def _key(text, meta):
    identity = [config.LLM_PROVIDER, llm._summary_model(config.LLM_PROVIDER),
                source_hash(text), meta.get('stage', ''), meta.get('court_domain', ''), meta.get('case_number', '')]
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()


def _ready_status(job):
    prepared = prepare_act(job['text'], job['meta'])
    if prepared.status != 'ready':
        return prepared.status
    return llm._response_status(job.get('summary'), prepared.text,
                                job['meta'].get('verdict_label', ''))[1]


def summarize_tracked(text, *, case_meta):
    """Write-ahead: сохраняем исходник до API и до фиксации дедупа доставки."""
    jobs = _load()
    key = _key(text, case_meta)
    now = datetime.now(timezone.utc)
    job = jobs.get(key) or {
        'text': text, 'meta': dict(case_meta), 'source_hash': source_hash(text),
        'created_at': now.isoformat(timespec='seconds'), 'runs': 0,
    }
    job['meta'] = dict(case_meta)
    # Успешный результат всегда проходит актуальные проверки источника/ответа.
    if job.get('summary') and prepare_act(text, case_meta).status == 'ready':
        summary, _ = llm._response_status(job['summary'], text, case_meta.get('verdict_label', ''))
        if summary:
            config.METRICS['llm_summary_cache_hits'] += 1
            if job.get('model'):
                config.SUMMARY_MODELS_USED.add(job['model'])
            return summary
    if job.get('status') in _WAIT_FOR_SOURCE or job.get('status') == 'needs_review':
        return None
    due = job.get('retry_after', '')
    if due and due > now.isoformat():
        return None
    if llm.summaries_configured():
        job['runs'] += 1
    job.update(status='pending', retry_after=(now + timedelta(days=1)).isoformat(timespec='seconds'))
    jobs[key] = job
    _save(jobs)
    meta = dict(case_meta, _refusal_rechecked=job.get('refusal_rechecked', False))
    def checkpoint_recheck():
        job['refusal_rechecked'] = True
        _save(jobs)
    meta['_on_recheck'] = checkpoint_recheck
    try:
        summary = llm.summarize_act_motivation(text, case_meta=meta)
    except Exception:
        log.exception('Пересказ остался в очереди после ошибки')
        config.METRICS['llm_summary_failed'] += 1
        summary = None
        meta['_summary_result'] = {'status': 'technical_error'}
    result = meta.get('_summary_result') or {'status': 'ready' if summary else 'technical_error'}
    job.update(result)
    job['updated_at'] = now.isoformat(timespec='seconds')
    if summary:
        job['summary'] = summary
    elif job['runs'] >= config.SUMMARY_RETRY_DAYS and job['status'] not in _WAIT_FOR_SOURCE:
        job['last_reason'], job['status'] = job['status'], 'needs_review'
    if result['status'] == 'missing_keys':
        # Следующий replay с ключами может обработать сразу.
        job.pop('retry_after', None)
    _save(jobs)
    return summary


def _blocks(cases):
    for case in cases:
        for episode in [case] + [h for h in case.get('history', []) if isinstance(h, dict)]:
            for stage in ('first_instance', 'appeal', 'cassation'):
                block = episode.get(stage)
                if isinstance(block, dict):
                    yield case, stage, block


def _matches(job, cases):
    meta = job['meta']
    matches = []
    for case, stage, block in _blocks(cases):
        if stage != meta.get('stage'):
            continue
        if meta.get('court_domain') and block.get('court_domain') != meta['court_domain']:
            continue
        number = block.get('case_number') or (case.get('id') if stage == 'first_instance' else '')
        if meta.get('case_number') and _bare_case_number(number or '') != _bare_case_number(meta['case_number']):
            continue
        if meta.get('judicial_uid') and block.get('judicial_uid') and meta['judicial_uid'] != block['judicial_uid']:
            continue
        # При отсутствии реквизитов нужен именно полный источник, не совпадение номера.
        if not (meta.get('court_domain') and meta.get('case_number')):
            if not block.get('act_text') or source_hash(block['act_text']) != job['source_hash']:
                continue
        matches.append(block)
    return matches


def seed_legacy(cases):
    """Только старые raw_act вместо AI-анализа; хорошие пересказы не трогаем."""
    jobs = _load()
    for case, stage, block in _blocks(cases):
        if (block.get('act_analysis') or {}).get('source') != 'raw_act' or not block.get('act_text'):
            continue
        meta = {'stage': stage, 'case_number': block.get('case_number') or case.get('id', ''),
                'court_domain': block.get('court_domain') or '',
                'judicial_uid': block.get('judicial_uid') or '',
                'cassation_number': block.get('cassation_number') or '',
                'source_url': block.get('act_source_url') or '',
                'act_received_at': block.get('act_received_at') or ''}
        text = block['act_text']
        key = _key(text, meta)
        if key not in jobs:
            jobs[key] = {'text': text, 'meta': meta, 'source_hash': source_hash(text),
                         'created_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                         'runs': 0, 'status': 'pending'}
    if jobs:
        _save(jobs)


def retry_pending(cases):
    """Ограниченная пачка; неполный источник заменяем лишь из той же карточки."""
    if not llm.summaries_configured():
        return
    jobs = _load()
    count = 0
    for key, job in list(jobs.items()):
        if count >= config.SUMMARY_RETRY_BATCH:
            break
        if job.get('status') == 'ready':
            status = _ready_status(job)
            if status == 'ready':
                continue
            job['status'] = status
            job.pop('summary', None)
            current = _load()
            current[key] = job
            _save(current)
        matches = _matches(job, cases)
        text = job['text']
        refreshed = False
        if len(matches) == 1 and matches[0].get('act_text'):
            candidate = matches[0]['act_text']
            if (source_hash(candidate) != job['source_hash'] or
                    (job.get('status') == 'source_incomplete' and matches[0].get('act_received_at') and
                     matches[0]['act_received_at'] != job['meta'].get('act_received_at'))):
                new_meta = dict(job['meta'], act_received_at=matches[0].get('act_received_at', ''),
                                source_url=matches[0].get('act_source_url') or job['meta'].get('source_url', ''))
                if prepare_act(candidate, new_meta).status != 'ready':
                    continue
                text = candidate
                refreshed = True
                job['meta'] = new_meta
                # Старую запись помечаем заменённой, не удаляем до сохранения новой.
                current = _load()
                current[key]['status'] = 'superseded'
                current[key].pop('retry_after', None)
                _save(current)
        if not refreshed and text == job['text'] and (job.get('status') in _WAIT_FOR_SOURCE or job.get('status') in ('needs_review', 'superseded')):
            continue
        if not refreshed and text == job['text'] and job.get('retry_after', '') > datetime.now(timezone.utc).isoformat():
            continue
        count += 1
        summarize_tracked(text, case_meta=job['meta'])


def attach_ready(cases):
    """Записать результат без событий и рассылок, только при уникальной связке."""
    changed = 0
    for job in _load().values():
        matches = _matches(job, cases)
        if job.get('status') in ('source_incomplete', 'empty_source') and len(matches) == 1:
            if not matches[0].get('act_summary_needs_source'):
                matches[0]['act_summary_needs_source'] = True
                changed += 1
        if job.get('status') != 'ready' or not job.get('summary'):
            continue
        if _ready_status(job) != 'ready':
            continue
        if len(matches) != 1:
            continue
        block = matches[0]
        if not block.get('act_text') or source_hash(block['act_text']) != job['source_hash']:
            continue
        desired = {
            'html': '<p><b>Почему:</b> ' + escape(job['summary']) + '</p>',
            'source': 'llm_summary', 'act_date': block.get('act_decision_date') or block.get('act_date') or '',
            'generated_at': job['updated_at'], 'model': job.get('model', ''),
            'source_hash': job['source_hash'], 'preparation': job.get('preparation', {}),
        }
        if block.get('act_analysis') != desired:
            block['act_analysis'] = desired
            changed += 1
    return changed
