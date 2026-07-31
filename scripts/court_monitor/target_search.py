# -*- coding: utf-8 -*-
"""Целевой поиск дела 1-й инстанции по номеру и сборка JSON-записи.

Функции выросли в scripts/add_cases_manually.py (ручное добавление дел) и
вынесены сюда для переиспользования импортёром реестра исков банка
(scripts/import_bank_registry.py): оба канала заводят дела, которых нет на
первой странице выдачи — автопоиск и авто-подхват трека читают только её.

URL целевого поиска строит CourtConfig.search_by_number_url (regions/base.py,
поле G1_CASE__CASE_NUMBERSS — проверено вживую 06.07.2026).
"""

from __future__ import annotations

from court_monitor.config import SBER_PATTERNS
from court_monitor.parsing import (
    _find_results_table,
    _parse_combined_cell,
    cell_href,
    cell_text,
    extract_tables,
)
from court_monitor.textutil import _CASE_ID_RE, _CASE_UID_RE, _FI_CASE_NUM_RE


def parse_search_row(html: str, court, target_case_number: str) -> dict | None:
    """Найти в поисковой выдаче строку с нужным case_number.

    В отличие от parse_first_instance_search — без фильтра по bank_role, чтобы
    не отбрасывать дела, где банк — истец или третье лицо.
    """
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        return None

    for row in results_table:
        if len(row) < 3:
            continue
        case_number_raw = cell_text(row[0]).strip()
        # Регулярка та же, что у поиска 1-й инст. (_FI_CASE_NUM_RE): узкий
        # «\d+-\d+/\d{4}» отбрасывал М-номера и трёхчастные номера постоянных
        # присутствий («2-2-279/2026», Покачи) — целевой поиск по такому номеру
        # никогда не находил дело. Границу номера всё равно стережёт сверка
        # case_bare с target ниже.
        if not _FI_CASE_NUM_RE.match(case_number_raw):
            continue
        # Номер может приходить в трёх форматах:
        #   "2-583/2026"                              — обычный
        #   "2-583/2026 ~ М-7442/2025"                — с материалом
        #   "2-583/2026 (2-9702/2025;) ~ М-7442/2025" — после переномерования
        # Сохраняем полный (без материала) как id, но матчим target по «голому».
        case_number = case_number_raw.split("~")[0].strip()
        case_bare = case_number.split("(")[0].strip()
        if case_bare != target_case_number:
            continue

        href = cell_href(row[0])
        cid = cuid = ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)

        date_received = cell_text(row[1]).strip() if len(row) > 1 else ""
        combined = cell_text(row[2]) if len(row) > 2 else ""
        parsed = _parse_combined_cell(combined)
        judge = cell_text(row[3]).strip() if len(row) > 3 else ""
        result = cell_text(row[5]).strip() if len(row) > 5 else ""

        return {
            "case_number": case_number,
            "filing_date": date_received,
            "plaintiff": parsed["plaintiff"],
            "defendant": parsed["defendant"],
            "category": parsed["category"],
            "judge": judge,
            "result": result,
            "status": "Решено" if result else "В производстве",
            "link": f"{cid}|{cuid}" if cid and cuid else "",
            "court": court.name,
            "court_domain": court.domain,
        }
    return None


def determine_bank_role(plaintiff: str, defendant: str) -> str | None:
    """Вернуть 'Истец'/'Ответчик' или None, если Сбербанк не упомянут в сторонах."""
    p_low = plaintiff.lower()
    d_low = defendant.lower()
    if any(p in p_low for p in SBER_PATTERNS):
        return "Истец"
    if any(p in d_low for p in SBER_PATTERNS):
        return "Ответчик"
    return None


def build_json_entry(fi_row: dict, card_info: dict) -> dict:
    """Собрать JSON-запись для cases.json из поисковой строки + карточки."""
    case_number = fi_row["case_number"]
    return {
        "id": case_number,
        "current_stage": "first_instance",
        "plaintiff": fi_row["plaintiff"],
        "defendant": fi_row["defendant"],
        "category": fi_row["category"],
        "bank_role": fi_row["bank_role"],
        "notes": "",
        "first_instance": {
            "case_number": case_number,
            "court": fi_row["court"],
            "court_domain": fi_row["court_domain"],
            "judge": fi_row["judge"],
            "filing_date": fi_row["filing_date"],
            "status": card_info.get("Статус") or fi_row["status"],
            "result": card_info.get("Результат") or fi_row["result"],
            "last_event": card_info.get("Последнее событие", ""),
            "event_date": card_info.get("Дата события", ""),
            "hearing_date": card_info.get("Дата заседания", ""),
            "hearing_time": card_info.get("Время заседания", ""),
            "link": fi_row["link"],
            "act_published": card_info.get("Акт опубликован") == "Да",
            "act_date": card_info.get("Дата публикации акта", ""),
            "events": card_info.get("_events", []),
        },
        "appeal": None,
    }
