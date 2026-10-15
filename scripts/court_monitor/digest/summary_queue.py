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
from court_monitor.digest import llm, summary_audit

_WAIT_FOR_SOURCE = {'source_incomplete', 'empty_source', 'text_extraction_required',
                    'case_mismatch', 'court_mismatch', 'uid_mismatch', 'stage_mismatch',
                    'refused', 'provider_refusal', 'source_conflict'}


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


def _readable_job(job):
    """Одна повреждённая запись не должна останавливать остальные пересказы."""
    if (not isinstance(job, dict) or not isinstance(job.get('text'), str)
            or not isinstance(job.get('meta'), dict)
            or ('status' in job and not isinstance(job['status'], str))
            or ('source_hash' in job and not isinstance(job['source_hash'], str))
            or ('runs' in job and (type(job['runs']) is not int or job['runs'] < 0))
            or (job.get('summary') is not None and not isinstance(job['summary'], str))
            or (job.get('retry_after') is not None and not isinstance(job['retry_after'], str))):
        return False
    try:
        prepare_act(job['text'], job['meta'])
    except (AttributeError, TypeError, ValueError):
        return False
    return True


def _normalize_source(job):
    """Классификация сохранённого источника не является попыткой пересказа."""
    if (not _readable_job(job) or job.get('status') in ('ready', 'superseded')
            or summary_audit.classify_job(job) != 'waiting_source'):
        return False
    prepared = prepare_act(job['text'], job['meta'])
    if prepared.status == 'ready':
        return False
    changed = (job.get('status') != prepared.status
               or job.get('preparation') != prepared.audit or 'retry_after' in job)
    job.update(status=prepared.status, preparation=prepared.audit)
    job.pop('retry_after', None)
    return changed


def reconcile_completed(jobs):
    """Закрыть старые повторы лишь при проверенном успехе того же источника.

    Модель входит в ключ кэша, но её смена не делает акт новой работой.
    Сравниваем весь текст и метаданные, включая УИД, происхождение и исход.
    Исходную запись и аудит попыток сохраняем; сети и записи здесь нет.
    Возвращаем ссылки старый ключ -> успешный ключ для журнала ремонта.
    """
    completed = {}
    for key, job in sorted(jobs.items()):
        if (not _readable_job(job) or summary_audit.classify_job(job) != 'ready'
                or job.get('source_hash', source_hash(job['text'])) != source_hash(job['text'])
                or _ready_status(job) != 'ready'):
            continue
        identity = (source_hash(job['text']), json.dumps(job['meta'], sort_keys=True, ensure_ascii=False))
        completed.setdefault(identity, []).append((key, job))
    replacements = {}
    for key, job in jobs.items():
        if not _readable_job(job) or summary_audit.classify_job(job) != 'retry_pending':
            continue
        text_hash = source_hash(job['text'])
        if job.get('source_hash', text_hash) != text_hash:
            continue
        identity = (text_hash, json.dumps(job['meta'], sort_keys=True, ensure_ascii=False))
        for target, ready in completed.get(identity, []):
            if target == key or ready['text'] != job['text'] or ready['meta'] != job['meta']:
                continue
            job.update(superseded_status=job['status'], status='superseded',
                       superseded_by=target, superseded_reason='same_source_completed')
            job.pop('retry_after', None)
            replacements[key] = target
            break
    return replacements


def reconcile_pending():
    """Локальный ремонт очереди под внешним run_lock, без модели/карточек/доставки."""
    jobs = _load()
    replacements = reconcile_completed(jobs)
    if replacements:
        _save(jobs)
    return replacements


def summarize_tracked(text, *, case_meta):
    """Write-ahead: сохраняем исходник до API и до фиксации дедупа доставки."""
    jobs = _load()
    key = _key(text, case_meta)
    now = datetime.now(timezone.utc)
    if key in jobs and not _readable_job(jobs[key]):
        summary_audit.record(summary_audit.identity(text, case_meta), {'status': 'needs_review'})
        return None
    job = jobs.get(key) or {
        'text': text, 'meta': dict(case_meta), 'source_hash': source_hash(text),
        'created_at': now.isoformat(timespec='seconds'), 'runs': 0, 'status': 'pending',
    }
    old_meta = job['meta']
    prepared = prepare_act(text, case_meta)
    # Повторно полученный полный источник может иметь те же 8000 символов:
    # подтверждение происхождения снимает старую блокировку без смены ключа.
    if (job.get('status') == 'source_incomplete' and prepared.status == 'ready'
            and case_meta.get('act_received_at')
            and case_meta['act_received_at'] != old_meta.get('act_received_at')):
        job['status'] = 'pending'
        job.pop('retry_after', None)
    job['meta'] = dict(case_meta)
    if prepared.status != 'ready':
        job.update(status=prepared.status, preparation=prepared.audit)
        job.pop('retry_after', None)
        jobs[key] = job
        _save(jobs)
        summary_audit.record(summary_audit.identity(text, case_meta), job)
        return None
    # Успешный результат всегда проходит актуальные проверки источника/ответа.
    if job.get('summary'):
        summary, _ = llm._response_status(job['summary'], text, case_meta.get('verdict_label', ''))
        if summary:
            if (job.get('status') != 'ready' or old_meta != case_meta
                    or job.get('preparation') != prepared.audit):
                job.update(status='ready', preparation=prepared.audit)
                jobs[key] = job
                _save(jobs)
            if reconcile_completed(jobs):
                _save(jobs)
            config.METRICS['llm_summary_cache_hits'] += 1
            if job.get('model'):
                config.SUMMARY_MODELS_USED.add(job['model'])
            summary_audit.record(summary_audit.identity(text, case_meta),
                                 dict(job, status='ready'), cached=True)
            return summary
    if (job.get('status') in _WAIT_FOR_SOURCE
            or summary_audit.classify_job(job) == 'needs_review'):
        summary_audit.record(summary_audit.identity(text, case_meta), job)
        return None
    due = job.get('retry_after', '')
    if due and due > now.isoformat():
        summary_audit.record(summary_audit.identity(text, case_meta), job)
        return None
    if llm.summaries_configured():
        job['runs'] = job.get('runs', 0) + 1
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
    summary_audit.record(summary_audit.identity(text, case_meta), result,
                         cached=result.get('cached', False))
    job.update(result)
    job['updated_at'] = now.isoformat(timespec='seconds')
    if summary:
        job['summary'] = summary
    elif job.get('runs', 0) >= config.SUMMARY_RETRY_DAYS and job['status'] not in _WAIT_FOR_SOURCE:
        job['last_reason'], job['status'] = job['status'], 'needs_review'
    if result['status'] == 'missing_keys':
        # Следующий replay с ключами может обработать сразу.
        job.pop('retry_after', None)
    reconcile_completed(jobs)
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
            if not block.get('act_text') or source_hash(block['act_text']) != job.get('source_hash', source_hash(job['text'])):
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
    jobs = _load()
    # Старые pending/technical_error могут содержать ещё обрезанные тексты.
    # Их диагноз сохраняем сразу, даже без ключей; batch, runs и аудит модели
    # относятся только к фактической обработке, а не к этой классификации.
    normalized = bool(reconcile_completed(jobs))
    for job in jobs.values():
        normalized = _normalize_source(job) or normalized
    if normalized:
        _save(jobs)
    if not llm.summaries_configured():
        return
    count = 0
    processed = set()
    for key in list(jobs):
        if count >= config.SUMMARY_RETRY_BATCH:
            break
        if key in processed:
            continue
        job = jobs[key]
        if not _readable_job(job):
            continue
        if (summary_audit.classify_job(job) == 'needs_review'
                and job.get('status') not in _WAIT_FOR_SOURCE):
            continue
        if job.get('status') == 'superseded':
            continue
        if job.get('status') == 'ready':
            status = _ready_status(job)
            if status == 'ready':
                continue
            job['status'] = status
            _save(jobs)
        matches = _matches(job, cases)
        text = job['text']
        if len(matches) == 1 and matches[0].get('act_text'):
            candidate = matches[0]['act_text']
            if (source_hash(candidate) != job.get('source_hash', source_hash(job['text'])) or
                    (job.get('status') == 'source_incomplete' and matches[0].get('act_received_at') and
                     matches[0]['act_received_at'] != job['meta'].get('act_received_at'))):
                new_meta = dict(job['meta'], act_received_at=matches[0].get('act_received_at', ''),
                                source_url=matches[0].get('act_source_url') or job['meta'].get('source_url', ''))
                if prepare_act(candidate, new_meta).status != 'ready':
                    continue
                text = candidate
                new_key = _key(text, new_meta)
                if new_key == key:
                    # Подтверждено происхождение прежнего текста. Не теряем
                    # созданную запись, счётчики и фактический аудит попыток.
                    job.update(meta=new_meta, status='pending')
                    job.pop('retry_after', None)
                else:
                    if new_key in jobs and not _readable_job(jobs[new_key]):
                        # Нельзя закрывать исходную задачу в пользу повреждённой
                        # записи: сохраняем обе для разбора без потери дочитки.
                        continue
                    # Один atomic-write сохраняет замену ДО API. Нельзя сначала
                    # закрыть старую запись: между двумя записями процесс может
                    # прерваться. Уже полученный результат нового ключа сохраняем.
                    jobs.setdefault(new_key, {
                        'text': text, 'meta': new_meta, 'source_hash': source_hash(text),
                        'created_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                        'runs': 0, 'status': 'pending',
                    })
                    job['status'] = 'superseded'
                    job.pop('retry_after', None)
                    key, job = new_key, jobs[new_key]
                _save(jobs)
        if key in processed:
            continue
        if job.get('status') in _WAIT_FOR_SOURCE or job.get('status') in ('needs_review', 'superseded'):
            continue
        if job.get('status') == 'ready' and _ready_status(job) == 'ready':
            continue
        if summary_audit.classify_job(job) != 'retry_pending':
            continue
        if job.get('retry_after', '') > datetime.now(timezone.utc).isoformat():
            continue
        processed.add(key)
        count += 1
        summarize_tracked(job['text'], case_meta=job['meta'])
        jobs = _load()


def attach_ready(cases):
    """Записать результат без событий и рассылок, только при уникальной связке."""
    changed = 0
    for job in _load().values():
        if not _readable_job(job):
            continue
        matches = _matches(job, cases)
        if job.get('status') in ('source_incomplete', 'empty_source') and len(matches) == 1:
            block = matches[0]
            meta = dict(job['meta'], act_received_at=block.get('act_received_at', ''),
                        source_url=block.get('act_source_url') or job['meta'].get('source_url', ''))
            # Старая запись относится к старому источнику. Она не должна снова
            # объявлять неполным новый текст, уже проверенный после дочитки.
            current_needs_source = prepare_act(block.get('act_text') or '', meta).status in (
                'source_incomplete', 'empty_source')
            if current_needs_source and not block.get('act_summary_needs_source'):
                block['act_summary_needs_source'] = True
                changed += 1
        if (summary_audit.classify_job(job) != 'ready'
                or not isinstance(job.get('updated_at'), str)):
            continue
        if _ready_status(job) != 'ready':
            continue
        if len(matches) != 1:
            continue
        block = matches[0]
        if not block.get('act_text') or source_hash(block['act_text']) != job.get('source_hash', source_hash(job['text'])):
            continue
        desired = {
            'html': '<p><b>Почему:</b> ' + escape(job['summary']) + '</p>',
            'source': 'llm_summary', 'act_date': block.get('act_decision_date') or block.get('act_date') or '',
            'generated_at': job['updated_at'], 'model': job.get('model', ''),
            'source_hash': job.get('source_hash', source_hash(job['text'])), 'preparation': job.get('preparation', {}),
        }
        if block.get('act_analysis') != desired:
            block['act_analysis'] = desired
            changed += 1
    return changed
