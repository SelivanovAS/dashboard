"""Подтверждённое появление текста, даты и безопасный вход для пересказа.

Отсутствие текста в старой базе не доказывает его отсутствие на сайте суда.
Метка отрицательного наблюдения ставится только после успешного чтения.
"""
from __future__ import annotations

import re
from datetime import date

from court_monitor.textutil import parse_date

FIELDS = ('act_absent_checked_at', 'act_detected_at', 'act_decision_date',
          'act_notification_kind')
_MONTHS = {name: i + 1 for i, name in enumerate((
    'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'))}


def document_date(text: str, confirmed_date: str = '') -> str:
    """Дата в шапке акта; дата изготовления и даты прежних решений не подходят."""
    if parse_date(confirmed_date):
        return confirmed_date
    head = (text or '')[:2500]
    kind = re.search(r'(?:АПЕЛЛЯЦИОННОЕ\s+ОПРЕДЕЛЕНИЕ|РЕШЕНИЕ|'
                     r'О\s*П\s*Р\s*Е\s*Д\s*Е\s*Л\s*Е\s*Н\s*И\s*Е|ПОСТАНОВЛЕНИЕ)', head)
    if not kind:
        return ''
    header = head[kind.end():].split('установил')[0][:400]
    match = re.search(r'\b(\d{2}\.\d{2}\.\d{4})\b', header)
    if match and parse_date(match[1]):
        return match[1]
    match = re.search(r'[«"]?(\d{1,2})[»"]?\s+(' + '|'.join(_MONTHS) +
                      r')\s+(\d{4})\b', header, re.I)
    if match:
        try:
            return date(int(match[3]), _MONTHS[match[2].lower()], int(match[1])).strftime('%d.%m.%Y')
        except ValueError:
            pass
    return ''


def observe(block: dict, text: str, today: date, *, present: bool,
            confirmed_date: str = '') -> bool:
    """Сохранить успешное наблюдение; вернуть право объявить новый текст.

present=True при ссылке/признаке документа без доступного текста: это не
отрицательное наблюдение и не успешная загрузка документа.
"""
    text = (text or '').strip()
    if not text:
        if not present and not block.get('act_text'):
            block['act_absent_checked_at'] = today.isoformat()
        return False
    first = not (block.get('act_text') or '').strip()
    if first:
        block.setdefault('act_detected_at', today.isoformat())
        block['act_notification_kind'] = (
            'new_publication' if block.get('act_absent_checked_at') else 'backfill')
    if first or (not summary_source(block.get('act_text') or '') and summary_source(text)):
        block['act_text'] = text
    block['act_published'] = True
    decision = document_date(text, confirmed_date)
    if decision:
        block['act_decision_date'] = decision
    return first and block.get('act_notification_kind') == 'new_publication'


def dates(block: dict) -> dict:
    return {k: block.get(k) or '' for k in ('act_decision_date', 'act_detected_at')}


def summary_source(text: str, stage: str = '') -> str:
    """Пересказ допустим для документа с заключительной частью.

Обрезанное после «установил» изложение иска не является мотивировкой.
Для длинного документа оставляем шапку и заключительные 26 тысяч знаков.
"""
    text = (text or '').strip()
    ending = re.search(r'(?:о\s*п\s*р\s*е\s*д\s*е\s*л\s*и\s*л[аи]?|'
                       r'р\s*е\s*ш\s*и\s*л[аи]?|постановил[аи]?)\s*:', text, re.I)
    if not ending or ending.start() < 100 or len(text[ending.end():].strip()) < 30:
        return ''
    if len(text) <= 32000:
        return text
    return text[:5000] + '\n[Промежуточная часть документа опущена]\n' + text[-26000:]


def summary_agrees(summary: str, text: str, verdict: str) -> bool:
    """Отсечь явное противоречие резолюции; неоднозначность не исправлять догадкой."""
    lower = summary.lower()
    parts = re.split(r'(?:определил[аи]?|решил[аи]?|постановил[аи]?)\s*:', text, flags=re.I)
    tail = parts[-1].lower()
    denied = bool(re.search(r'в удовлетворении .{0,200}отказа(?:ть|но)', tail))
    granted = bool(re.search(r'(?:иск(?:овые требования)?|требования)[^.]{0,80}удовлетворить', tail))
    if denied and not granted and re.search(r'(?:иск|требования)[^.]{0,80}удовлетворен|договор признан недействительным', lower):
        return False
    if granted and not denied and re.search(r'в удовлетворении .{0,150}отказано', lower):
        return False
    return bool(summary)
