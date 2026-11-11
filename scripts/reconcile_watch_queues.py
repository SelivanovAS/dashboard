#!/usr/bin/env python3
"""Восстановить очереди только в НОВОЙ копии данных, без сети и рассылок.

Вход неизменяемый. Выход содержит копию картотеки, журнал автоматических
действий и построчный остаток. Установка результата на сервер не выполняется.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import csv
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
from unittest.mock import patch

AUTHOR = 'Александр Селиванов'
QUESTIONS = {
    'missing_decision_date': 'Уточнить дату и вид акта по движению дела или документу; сохранённых сведений недостаточно.',
    'missing_card_link': 'Найти карточку того же суда, площадки и производства; сверить номер и УИД.',
    'missing_case_number': 'Уточнить номер первой инстанции по акту вышестоящего суда и найти её карточку.',
    'missing_court_domain': 'Уточнить суд и доступный источник карточки; для мирового судьи нужен отдельный источник.',
    'act_identity_mismatch': 'Сверить дату и реквизиты текста с контролируемым актом; исключить текст другого круга.',
    'identity_mismatch': 'Сверить номер, суд, площадку и УИД полученной карточки с ожидаемым производством.',
    'identity_conflict': 'Разрешить противоречие идентификаторов; автоматическое объединение запрещено.',
    'invalid_card_request': 'Проверить адрес карточки на сайте суда: ранее сайт отклонил запрос. Новый адрес не подтверждён.',
    'basis_changed': 'По акту вышестоящего суда определить действующие требования банка и основание контроля ИЛ.',
    'decision_cancelled': 'Уточнить новое рассмотрение после отмены и актуальное основание контроля ИЛ.',
    'decision_date_changed': 'Установить, исправлена дата или принято новое решение; подтвердить актуальное основание ИЛ.',
    'writ_recalled_or_returned': 'Уточнить причину возврата/отзыва листа и наличие действующего или нового листа.',
    'fi_production_reopened': 'Сопоставить возобновлённое производство с прежним решением и основанием ИЛ.',
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_documents(root):
    from court_monitor.storage import load_bank_json
    docs = []
    for path in sorted((root / 'data').glob('cases*.json')):
        if path.stem.endswith('_events'):
            continue
        events = (path.with_name(path.stem + '_events.json')
                  if path.name in ('cases_bank.json', 'cases_bank_archive.json') else None)
        if events and not events.exists():
            raise ValueError(f'Нет отдельного файла событий: {events.name}')
        data = load_bank_json(str(path), str(events)) if events else json.loads(path.read_text())
        docs.append((path, events, data))
    return docs


def counts(data):
    fields = ('fi_act_watch', 'appeal_act_watch', 'cassation_act_watch', 'writ_watch')
    return {field: dict(Counter(t.get('reason') or '' for t in (data.get(field) or {}).values()
                                if t.get('status') == 'needs_review')) for field in fields}


def validate_case_changes(before, after, location='cases'):
    """Исправление ссылок может только заполнить пустые идентификаторы."""
    from court_monitor.watch_repair import IDENTIFIERS
    if isinstance(before, dict) and isinstance(after, dict):
        for key in set(before) | set(after):
            if (key in IDENTIFIERS and location.endswith('.first_instance')
                    and not before.get(key) and after.get(key)):
                continue
            if key not in before or key not in after:
                raise AssertionError(f'Постороннее поле: {location}.{key}')
            validate_case_changes(before[key], after[key], location + '.' + key)
    elif isinstance(before, list) and isinstance(after, list):
        if len(before) != len(after):
            raise AssertionError(f'Изменён состав: {location}')
        for index, (left, right) in enumerate(zip(before, after)):
            validate_case_changes(left, right, f'{location}[{index}]')
    elif before != after:
        raise AssertionError(f'Изменено защищённое значение: {location}')


def run(source, output, region, day):
    def no_network(*args, **kwargs):
        raise RuntimeError('Сеть запрещена при проверке копий')
    with patch.object(socket, 'create_connection', no_network), patch.object(socket.socket, 'connect', no_network):
        return _run(source, output, region, day)


def _run(source, output, region, day):
    from court_monitor import config, card_refresh
    from court_monitor.storage import save_json, save_bank_json
    from court_monitor.watch_repair import reconcile
    source, output = source.resolve(), output.resolve()
    if config.REGION != region:
        raise ValueError('Территория процесса не совпадает с аргументом')
    if output == source or source in output.parents or output in source.parents:
        raise ValueError('Вход и выход не могут совпадать или находиться друг в друге')
    if output.exists():
        raise ValueError('Выход должен быть новым каталогом')
    passport = source / '.program-release.json'
    if passport.exists() and json.loads(passport.read_text()).get('region') != region:
        raise ValueError('Территория не совпадает с паспортом снимка')
    originals = {str(p.relative_to(source)): sha(p) for p in source.rglob('*') if p.is_file()}
    for path in (source / 'data').glob('*.json'):
        json.loads(path.read_text())  # Ошибку исходника не заменяем пустой картотекой.
    shutil.copytree(source, output)
    docs = load_documents(output)
    data = next(d for p, _, d in docs if p.name == 'cases.json')
    if (data.get('region') or {}).get('code', region) != region:
        raise ValueError('Территория картотеки не совпадает с аргументом')
    before = {str(p): deepcopy(d) for p, _, d in docs}
    baseline = counts(data)
    all_cases = [c for _, _, d in docs for c in d.get('cases', [])]
    actions = reconcile(data, all_cases, day)
    after_first = canonical([d for _, _, d in docs])
    assert not reconcile(data, all_cases, day), 'Повтор создал новые действия'
    assert canonical([d for _, _, d in docs]) == after_first, 'Повтор изменил результат'
    # План обычного прогона проверяется на ещё одной копии: его служебные
    # изменения не входят в результат исправления исходной картотеки.
    planned_data, planned_cases = deepcopy([data, all_cases])
    pending_before = deepcopy({k: v for k, v in planned_data.items() if k.startswith('pending_')})
    cards, _ = card_refresh.plan(planned_data, planned_cases, day)
    assert {k: v for k, v in planned_data.items() if k.startswith('pending_') and v} == {
        k: v for k, v in pending_before.items() if v}, 'План создал уведомления'
    for action in actions:
        key = action.get('previous_key') if action['action'] == 'duplicate_act_task_closed' else action['key']
        assert planned_data[action['queue']][key]['status'] != 'needs_review', 'Исправленная задача снова остановлена'
        previous = action.get('previous_key')
        if previous and previous != key:
            assert previous not in planned_data[action['queue']], 'Старый дубль возник повторно'
    allowed_queues = {'fi_act_watch', 'writ_watch'}
    for path, events, doc in docs:
        old = before[str(path)]
        validate_case_changes(old.get('cases', []), doc.get('cases', []))
        for field in set(old) | set(doc):
            if field == 'cases' or (path.name == 'cases.json' and field in allowed_queues):
                continue
            assert old.get(field) == doc.get(field), f'Изменено постороннее поле {path.name}:{field}'
        if doc != old:
            if events:
                save_bank_json(doc, str(path), str(events))
            else:
                save_json(doc, str(path))
    # Повторное открытие проверяет именно сохранённый результат и bank-events.
    reopened = load_documents(output)
    for (_, _, expected), (_, _, actual) in zip(docs, reopened):
        # save_json обновляет только служебную дату корневого документа.
        assert {k: v for k, v in expected.items() if k != 'updated_at'} == {
            k: v for k, v in actual.items() if k != 'updated_at'}
    assert originals == {str(p.relative_to(source)): sha(p) for p in source.rglob('*') if p.is_file()}
    remaining = []
    for field in baseline:
        for key, task in (data.get(field) or {}).items():
            if task.get('status') != 'needs_review':
                continue
            block = task.get('block') or {}
            stage = card_refresh.STAGES[field]
            reason = task.get('reason') or ''
            remaining.append({'region': region, 'queue': field, 'key': key,
                'case_number': block.get('case_number') or '',
                'court': block.get('court') or block.get('court_domain') or '',
                'court_domain': block.get('court_domain') or '',
                'judicial_uid': block.get('judicial_uid') or '',
                'url': card_refresh._url(stage, block), 'reason': reason,
                'question': QUESTIONS.get(reason, 'Проверить основание остановки задачи.'),
                'decision_date': block.get('decision_date') or '',
                'result': block.get('result') or '',
                'writs': block.get('writs') or []})
    report = {'author': AUTHOR, 'region': region, 'date': day.isoformat(),
              'mode': 'offline_copy', 'before': baseline, 'after': counts(data),
              'actions': actions, 'remaining': remaining,
              'source_hashes': originals,
              'checks': {'input_unchanged': True, 'idempotent': True,
                         'case_content_preserved': True, 'reopened': True,
                         'notifications_unchanged': True, 'network_disabled': True,
                         'normal_plan_no_regression': True},
              'normal_plan_cards': len(cards)}
    for action in actions:
        task = data[action['queue']][action.get('previous_key') if
                    action['action'] == 'duplicate_act_task_closed' else action['key']]
        action['case_number'] = task['block'].get('case_number') or ''
        action['court_domain'] = task['block'].get('court_domain') or ''
        action['status_after'] = task['status']
        action['evidence'] = task.get('repair_history') or []
    (output / 'repair_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    columns = ['region', 'queue', 'case_number', 'court', 'court_domain', 'judicial_uid',
               'reason', 'question', 'url', 'decision_date', 'result', 'key']
    with (output / 'manual_review.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction='ignore')
        writer.writeheader(); writer.writerows(remaining)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--region', choices=('hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen'), required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    os.environ['REGION'] = args.region
    report = run(args.input, args.output, args.region, args.date)
    print(json.dumps({k: report[k] for k in ('region', 'before', 'after', 'checks')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
