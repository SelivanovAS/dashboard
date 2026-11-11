#!/usr/bin/env python3
"""Исправить подтверждённые ошибки и добавить ИЛ в новой копии данных.

Сеть и рассылки запрещены. Рабочую установку выполняет отдельная процедура
под блокировкой после проверки результата и исходных контрольных сумм.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import csv
from datetime import date
import json
import os
from pathlib import Path
import shutil
import socket
from unittest.mock import patch

from reconcile_watch_queues import load_documents, counts, sha, QUESTIONS


def run(source, output, evidence_dir, region, day):
    def forbidden(*args, **kwargs):
        raise RuntimeError('Проверка копии не использует сеть')
    with patch.object(socket, 'create_connection', forbidden), patch.object(socket.socket, 'connect', forbidden):
        return _run(source, output, evidence_dir, region, day)


def _run(source, output, evidence_dir, region, day):
    from court_monitor import config, card_refresh
    from court_monitor.storage import save_json, save_bank_json
    from court_monitor.writ_repair import load_evidence, reconcile, canonical
    source, output = Path(source).resolve(), Path(output).resolve()
    if config.REGION != region or output.exists() or source in output.parents or output in source.parents:
        raise ValueError('Нужны верная территория и отдельный новый каталог результата')
    passport = json.loads((source / '.program-release.json').read_text())
    if passport['region'] != region:
        raise ValueError('Паспорт другой территории')
    originals = {str(p.relative_to(source)): sha(p) for p in source.rglob('*') if p.is_file()}
    for path in (source / 'data').glob('*.json'):
        json.loads(path.read_text())
    evidence, observations = load_evidence(evidence_dir)
    shutil.copytree(source, output)
    docs = load_documents(output)
    data = next(d for p, _, d in docs if p.name == 'cases.json')
    before_docs = {str(p): deepcopy(d) for p, _, d in docs}
    baseline = counts(data)
    all_cases = [c for _, _, d in docs for c in d.get('cases', [])]
    actions = reconcile(data, all_cases, evidence, observations, day)
    after_first = canonical([d for _, _, d in docs])
    all_cases = [c for _, _, d in docs for c in d.get('cases', [])]
    assert not reconcile(data, all_cases, evidence, observations, day), 'Повтор создаёт действия'
    assert canonical([d for _, _, d in docs]) == after_first, 'Повтор меняет данные'
    # Обход после исправления проверяется без записи результатов планирования.
    planned_data, planned_cases = deepcopy([data, all_cases])
    pending_before = {k: deepcopy(v) for k, v in planned_data.items() if k.startswith('pending_') and v}
    # Все пути чтения кэшей/очередей также направлены в проверяемую копию.
    path_fields = {k: str(output / 'data' / Path(v).name)
                   for k, v in vars(config).items()
                   if k.endswith('_PATH') and isinstance(v, str) and v.startswith('data/')}
    with patch.multiple(config, **path_fields):
        cards, _ = card_refresh.plan(planned_data, planned_cases, day)
    assert {k: v for k, v in planned_data.items() if k.startswith('pending_') and v} == pending_before, 'План создал уведомление'
    new_review = []
    for key, task in (planned_data.get('writ_watch') or {}).items():
        actual = (data.get('writ_watch') or {}).get(key)
        if task.get('status') == 'needs_review' and actual and actual.get('status') != 'needs_review':
            new_review.append(key)
    assert not new_review, 'План снова заморозил исправленную задачу: ' + str(new_review)
    changed_files = []
    allowed_top = {'cases', 'writ_watch', 'fi_act_watch', 'appeal_act_watch', 'writ_repair_log'}
    for path, events, doc in docs:
        old = before_docs[str(path)]
        for field in set(old) | set(doc):
            if field == 'cases' or (path.name == 'cases.json' and field in allowed_top):
                continue
            assert old.get(field) == doc.get(field), f'Постороннее поле: {path.name}/{field}'
        if doc == old:
            continue
        if events:
            # Сохранение полного документа штатным split-API. Если менялась
            # только служебная дата файла событий, исходные байты сохраняем.
            raw_events = events.read_bytes()
            original_events = json.loads(raw_events)
            save_bank_json(doc, str(path), str(events))
            written_events = json.loads(events.read_text())
            assert original_events.get('events') == written_events.get('events'), 'Изменена хроника bank-events'
            events.write_bytes(raw_events)
        else:
            save_json(doc, str(path))
        changed_files.append(str(path.relative_to(output)))
    reopened = load_documents(output)
    for (_, _, expected), (_, _, actual) in zip(docs, reopened):
        assert {k: v for k, v in expected.items() if k != 'updated_at'} == {
            k: v for k, v in actual.items() if k != 'updated_at'}, 'Данные изменились после сохранения'
    assert originals == {str(p.relative_to(source)): sha(p) for p in source.rglob('*') if p.is_file()}
    actual_changed = sorted(n for n, old_sha in originals.items() if sha(output / n) != old_sha)
    assert actual_changed == sorted(changed_files), 'Изменён файл вне списка картотеки'
    remaining = []
    for field in baseline:
        for key, task in (data.get(field) or {}).items():
            if task.get('status') != 'needs_review':
                continue
            block = task.get('block') or {}
            reason = task.get('reason') or ''
            remaining.append({'region': region, 'queue': field, 'key': key,
                'case_number': block.get('case_number') or '', 'court': block.get('court') or '',
                'court_domain': block.get('court_domain') or '', 'judicial_uid': block.get('judicial_uid') or '',
                'reason': reason, 'question': QUESTIONS.get(reason, reason),
                'url': card_refresh._url(card_refresh.STAGES[field], block),
                'decision_date': block.get('decision_date') or '', 'result': block.get('result') or '',
                'writs': block.get('writs') or []})
    report = {'author': 'Александр Селиванов', 'region': region, 'date': day.isoformat(),
              'before': baseline, 'after': counts(data), 'actions': actions, 'remaining': remaining,
              'changed_files': changed_files, 'input_hashes': originals,
              'normal_plan_cards': len(cards),
              'checks': {'input_unchanged': True, 'idempotent': True, 'network_disabled': True,
                         'bank_events_preserved': True, 'unrelated_files_preserved': True,
                         'reopened': True, 'normal_plan_no_regression': True,
                         'notification_contexts_unchanged': True}}
    (output / 'writ_repair_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    fields = ['region', 'queue', 'case_number', 'court', 'judicial_uid', 'reason', 'question',
              'url', 'decision_date', 'result', 'key']
    with (output / 'manual_review.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        writer.writerows(remaining)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('input', 'output', 'evidence'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--region', choices=('hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen'), required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    os.environ['REGION'] = args.region
    report = run(args.input, args.output, args.evidence, args.region, args.date)
    print(json.dumps({k: report[k] for k in ('region', 'before', 'after', 'changed_files', 'checks')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
