"""Один полный источник для всех провайдеров. Никакого отбора по длине."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from court_monitor import act_publication

VERSION = 'full-act-v1'
NUMBER_RE = re.compile(r'(?:дел[оа]|производство)\s*(?:№|N)?\s*([\d]+[а-яА-ЯA-Za-z-]*[-–]\d+/\d{4})', re.I)


def normalize_text(text: str) -> str:
    # HTML-сущности уже раскрыты извлечением страницы. Второй unescape
    # превратил бы буквальное &lt;...&gt; в разметку. Смысл/OCR не исправляем.
    text = (text or '').replace('\r\n', '\n').replace('\r', '\n').replace('\xa0', ' ')
    return '\n'.join(re.sub(r'[\t \f\v]+', ' ', line).strip()
                     for line in text.split('\n')).strip()


def source_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class PreparedAct:
    text: str
    status: str
    audit: dict


def prepare_act(raw: str, meta: dict) -> PreparedAct:
    text = normalize_text(raw)
    status = 'ready'
    if not text:
        status = 'empty_source'
    elif text.startswith('%PDF-'):
        status = 'text_extraction_required'
    elif not meta.get('act_received_at') and 7990 <= len((raw or '').strip()) <= 8000:
        # Старый экстрактор резал на 8000, иногда после готовой резолюции.
        # Без происхождения нельзя объявлять такой источник полным.
        status = 'source_incomplete'
    elif 'document.getElementById' in text and ('case_type' in text or 'window.location' in text):
        status = 'source_incomplete'
    elif '[Промежуточная часть документа опущена]' in text:
        status = 'source_incomplete'
    elif not act_publication.summary_source(text, meta.get('stage', '')):
        status = 'source_incomplete'

    # Реквизиты проверяем только там, где они действительно извлечены.
    # Номер в истории дела внутри акта не является номером этого акта.
    def number(value):
        match = re.search(r'\d+[а-яА-ЯA-Za-z-]*[-–]\d+/\d{4}', value or '')
        return match[0].replace('–', '-') if match else ''
    expected = number(meta.get('case_number'))
    aliases = [n for n in (expected, number(meta.get('cassation_number'))) if n]
    # У кассации внутренний 8Г-номер и номер акта 88-... различаются.
    # Номер первой инстанции в описании не заменяет реквизиты текущего акта.
    header_numbers = re.findall(r'\d+[а-яА-ЯA-Za-z-]*[-–]\d+/\d{4}', text[:700])
    identity_check = 'header_unavailable'
    for alias in aliases:
        family = alias.split('-')[0]
        comparable = [n.replace('–', '-') for n in header_numbers if n.replace('–', '-').split('-')[0] == family]
        if comparable:
            if alias != comparable[0]:
                status = 'case_mismatch'
            else:
                identity_check = 'matched_header'
    header_uid = re.search(r'\d{2}[RР][SС]\d{4}-\d{2}-\d{4}-\d{6}-\d{2}', text[:700], re.I)
    if header_uid and meta.get('judicial_uid'):
        if header_uid[0] != meta['judicial_uid']:
            status = 'uid_mismatch'
        else:
            identity_check = 'matched_uid'
    url = meta.get('source_url') or ''
    domain = meta.get('court_domain') or ''
    if url and domain and urlparse(url).hostname != domain:
        status = 'court_mismatch'
    if meta.get('document_uid') and meta.get('judicial_uid') and meta['document_uid'] != meta['judicial_uid']:
        status = 'uid_mismatch'
    head = text[:700].lower()
    if meta.get('stage') == 'first_instance' and re.search(r'(?:апелляционное|кассационное)\s+определение', head):
        status = 'stage_mismatch'
    if meta.get('stage') == 'appeal' and re.search(r'кассационное\s+определение', head):
        status = 'stage_mismatch'
    audit = {
        'version': VERSION, 'source_hash': source_hash(raw),
        'raw_chars': len(raw or ''), 'prepared_chars': len(text),
        'source_url': url, 'court_domain': domain,
        'received_at': meta.get('act_received_at') or '',
        'case_number': meta.get('case_number') or '', 'stage': meta.get('stage') or '',
        'judicial_uid': meta.get('judicial_uid') or '',
        'identity_check': identity_check,
        'status': status,
    }
    return PreparedAct(text, status, audit)
