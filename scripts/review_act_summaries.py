#!/usr/bin/env python3
"""Read-only inventory of saved summaries and their source-watch tasks.

Use a snapshot of one region. This command never fetches courts, calls a model,
changes JSON or creates notifications. stdout contains metadata, never act text.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

from court_monitor.act_preparation import source_hash
from court_monitor.digest.summary_audit import backlog_snapshot, classify_job
from court_monitor.textutil import _bare_case_number
from court_monitor import act_watch_policy

REGIONS = ('hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen')
WATCH_FIELDS = {'first_instance': 'fi_act_watch', 'appeal': 'appeal_act_watch',
                'cassation': 'cassation_act_watch'}
CASE_FILES = ('cases.json', 'cases_archive.json', 'cases_bank.json',
              'cases_bank_archive.json')


def _read(path, hashes):
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f'{path.name}: ожидается JSON-объект')
    hashes[path.name] = hashlib.sha256(raw).hexdigest()
    return value


def _blocks(stores):
    for filename, data in stores:
        for case in data['cases']:
            if not isinstance(case, dict):
                raise ValueError(f'{filename}: некорректная карточка')
            for episode in [case] + [x for x in case.get('history', []) if isinstance(x, dict)]:
                for stage in WATCH_FIELDS:
                    block = episode.get(stage)
                    if isinstance(block, dict):
                        yield filename, case, stage, block


def _matches(meta, stage, block, case_id='', text_hash=''):
    if stage != meta.get('stage'):
        return False
    domain, number = meta.get('court_domain'), meta.get('case_number')
    if domain and domain != block.get('court_domain'):
        return False
    actual = block.get('case_number') or (case_id if stage == 'first_instance' else '')
    if number and _bare_case_number(number) != _bare_case_number(actual):
        return False
    uid = meta.get('judicial_uid')
    if uid and block.get('judicial_uid') and uid != block['judicial_uid']:
        return False
    if not (domain and number):
        return bool(text_hash and block.get('act_text') and source_hash(block['act_text']) == text_hash)
    return True


def _watch_state(meta, target, stores):
    stage = meta.get('stage')
    field = WATCH_FIELDS.get(stage)
    if not field or target is None:
        return 'unmatched'
    matches = []
    for _, data in stores:
        tasks = data.get(field) or {}
        if not isinstance(tasks, dict):
            raise ValueError(f'{field}: ожидается объект задач')
        for task in tasks.values():
            if not isinstance(task, dict) or not isinstance(task.get('block'), dict):
                raise ValueError(f'{field}: некорректная задача')
            block = task.get('block') or {}
            if not _matches(meta, stage, block):
                continue
            # A later round with the same case number is a different obligation.
            if any(target.get(k) and block.get(k) and str(target[k]) != str(block[k])
                   for k in ('judicial_uid', 'link', 'srv_num', 'delo_id')):
                continue
            wanted = target.get('decision_date') or target.get('act_decision_date')
            actual = block.get('decision_date') or block.get('act_decision_date')
            if wanted and actual and wanted != actual:
                continue
            status = task.get('status') or 'unknown'
            if status == 'waiting':
                # A monthly slot beyond MAX_AGE will be expired before it runs.
                from court_monitor.textutil import parse_date
                decision = parse_date(block.get('decision_date') or block.get('act_decision_date') or '')
                try:
                    due = date.fromisoformat(task.get('next_check_at') or '')
                except (ValueError, TypeError):
                    status = 'unscheduled'
                else:
                    if decision and due > decision.date() + timedelta(days=act_watch_policy.MAX_AGE):
                        status = 'scheduled_after_expiry'
            matches.append(status)
    return matches[0] if len(matches) == 1 else ('ambiguous' if matches else 'missing')


def inventory(data_dir, region):
    root = Path(data_dir).resolve()
    marker = root.parent / 'REGION'
    if marker.exists() and marker.read_text(encoding='utf-8').strip() != region:
        raise ValueError('REGION снимка не совпадает с выбранной территорией')
    hashes = {}
    jobs = _read(root / '.act_summaries.pending.json', hashes)
    filenames = set(CASE_FILES)
    filenames.update(p.name for p in root.glob('cases*archive_*.json')
                     if re.fullmatch(r'cases_(?:bank_)?archive_\d{4}\.json', p.name))
    stores = []
    for name in sorted(filenames):
        if not (root / name).exists():
            continue
        data = _read(root / name, hashes)
        if not isinstance(data.get('cases'), list):
            raise ValueError(f'{name}: отсутствует список cases')
        declared_region = data.get('region')
        if isinstance(declared_region, dict):
            declared_region = declared_region.get('code') or 'unknown'
        if declared_region and declared_region != region:
            raise ValueError(f'{name}: чужая территория')
        stores.append((name, data))
    if not stores:
        raise ValueError('В снимке нет картотеки для проверки связей')
    blocks = list(_blocks(stores))
    items = []
    for key, job in jobs.items():
        category = classify_job(job)
        if category in ('ready', 'superseded'):
            continue
        job = job if isinstance(job, dict) else {}
        meta = job.get('meta') if isinstance(job.get('meta'), dict) else {}
        valid_meta = all(isinstance(meta.get(k, ''), str) for k in
                         ('stage', 'court_domain', 'case_number', 'judicial_uid'))
        matches = [(filename, block) for filename, case, stage, block in blocks
                   if valid_meta and _matches(meta, stage, block, case.get('id', ''),
                                              job.get('source_hash', ''))]
        meta = meta if valid_meta else {}
        target = matches[0][1] if len(matches) == 1 else None
        status = job.get('status') if isinstance(job.get('status'), str) else 'unknown'
        items.append({'job_id': key, 'category': category, 'status': status,
                      'stage': meta.get('stage', ''), 'court_domain': meta.get('court_domain', ''),
                      'case_number': meta.get('case_number', ''), 'matches': len(matches),
                      'store': matches[0][0] if len(matches) == 1 else '',
                      'source_watch': _watch_state(meta, target, stores)})
    # Refuse to present a mixed snapshot as a consistent inventory.
    for filename, digest in hashes.items():
        if hashlib.sha256((root / filename).read_bytes()).hexdigest() != digest:
            raise ValueError('Снимок изменился во время чтения; повторите на копии данных')
    return {'schema_version': 1, 'region': region, 'read_only': True,
            'snapshot_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'files': hashes, 'counts': backlog_snapshot(jobs),
            'source_watch_counts': dict(Counter(x['source_watch'] for x in items
                                              if x['category'] == 'waiting_source')),
            'items': items}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', required=True, type=Path)
    parser.add_argument('--region', required=True, choices=REGIONS)
    args = parser.parse_args()
    try:
        result = inventory(args.data_dir, args.region)
    except (OSError, ValueError, TypeError) as exc:
        parser.exit(2, f'Не удалось проверить снимок: {exc}\n')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
