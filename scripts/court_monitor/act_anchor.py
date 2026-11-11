"""Дата контролируемого акта; процессуальное определение не меняет дату решения.

Только сохранённые реквизиты и датированные события того же производства.
Дата размещения на сайте и дата передачи дела в экспедицию не подходят.
"""
from __future__ import annotations

import re

from court_monitor.textutil import parse_date


def _text(value):
    return re.sub(r'\s+', ' ', str(value or '').lower().replace('ё', 'е')).strip()


def kind(value):
    text = _text(value)
    if re.search(r'отказ\w* в принят', text):
        return 'refusal_to_accept'
    if re.search(r'(?:возвращ\w*|возврат) (?:иска|заявлен|заявител|материал)', text):
        return 'return'
    if re.search(r'переда\w*.*(?:по подсудност|на рассмотрение другого суда)', text):
        return 'transfer'
    if re.search(r'производств\w* по делу прекращ', text):
        return 'termination'
    if re.search(r'оставл\w*.*без рассмотрен', text):
        return 'without_consideration'
    # Общая формулировка портала используется и при разрешении ходатайств.
    # Она сама по себе не подтверждает новое решение по существу.
    if re.search(r'вынесено (?:заочное )?решение\b(?!\s*\(определение\))', text):
        return 'judgment'
    if re.search(r'(?:иск|заявлен|жалоб).*удовлетворен|отказано в удовлетворении', text):
        return 'judgment'
    return ''


def evidence(block):
    """Вернуть проверяемое основание или пустой dict при недостатке данных."""
    result_kind = kind(block.get('result'))
    for field in ('decision_date', 'act_decision_date'):
        day = parse_date(block.get(field) or '')
        if day:
            return {'date': day.strftime('%d.%m.%Y'), 'kind': result_kind or 'judgment',
                    'source': field}
    candidates = []
    for index, event in enumerate(block.get('events') or []):
        if not isinstance(event, dict):
            continue
        day = parse_date(event.get('date') or '')
        event_kind = kind(event.get('result_event') or event.get('text'))
        name = _text(event.get('name') or event.get('text'))
        # Результат заседания или вынесение постановления; упоминание решения
        # в жалобе, выдаче копии, отправке или отмене не является новым актом.
        judicial = (name.startswith(('решение вопроса о принятии',
                                    'рассмотрение исправленных материалов',
                                    'принятие судебного постановления',
                                    'судебное заседание', 'вынесено решение',
                                    'вынесено заочное решение')))
        if day and event_kind and judicial:
            candidates.append((day, event_kind, index, event))
    if not candidates:
        return {}
    latest_day = max(x[0] for x in candidates)
    latest = [x for x in candidates if x[0] == latest_day]
    kinds = {x[1] for x in latest}
    if len(kinds) != 1 or (result_kind and result_kind not in kinds):
        return {}
    # Пустой текущий результат допустим только у завершённой карточки.
    if not result_kind and (block.get('result') or block.get('status') != 'Решено'):
        return {}
    # Более позднее принятие/возобновление не позволяет воскресить старый акт.
    for event in block.get('events') or []:
        if not isinstance(event, dict):
            continue
        day = parse_date(event.get('date') or '')
        text = _text(' '.join(str(event.get(f) or '') for f in ('name', 'result_event', 'text')))
        if day and day > latest_day and re.search(
                r'принят\w* к производств|возобновлен|рассмотрение.*с начала|отмен\w*.*решени', text):
            return {}
    day, act_kind, index, event = latest[-1]
    return {'date': day.strftime('%d.%m.%Y'), 'kind': act_kind,
            'source': 'events', 'event_index': index,
            'event_name': event.get('name') or '',
            'event_result': event.get('result_event') or ''}


def date(block):
    return evidence(block).get('date', '')
