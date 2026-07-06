# -*- coding: utf-8 -*-
"""Парсинг поисковой выдачи: апелляция (Суд ХМАО) и 20 судов 1-й инстанции.
Фильтр «настоящий Сбербанк» против дочек/страховых, определение роли банка.
"""

from __future__ import annotations

import re

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import CourtConfig, JUDICIAL_UID_RE
from court_monitor.parsing.tables import extract_tables, cell_text, cell_href
from court_monitor.textutil import (
    _strip_html, _CASE_NUM_RE, _FI_CASE_NUM_RE, _TIME_RE,
    _CASE_ID_RE, _CASE_UID_RE, parse_date,
)

# ── Парсинг страницы поиска ──────────────────────────────────────────────────

def _parse_combined_cell(text: str) -> dict:
    """
    Разбирает объединённую ячейку с категорией, сторонами и судом.
    Формат: 'КАТЕГОРИЯ: ...ИСТЕЦ(ЗАЯВИТЕЛЬ): ...ОТВЕТЧИК: ...Суд ... первой инстанции: ...'
    """
    result = {"category": "", "plaintiff": "", "defendant": "", "court": ""}

    m = re.search(r"КАТЕГОРИЯ:\s*(.+?)(?=ИСТЕЦ|ЗАЯВИТЕЛЬ|ОТВЕТЧИК|Суд\s|$)", text)
    if m:
        result["category"] = m.group(1).strip().rstrip("→ \xa0")

    m = re.search(r"(?:ИСТЕЦ|ЗАЯВИТЕЛЬ)\(?[^)]*\)?:\s*(.+?)(?=ОТВЕТЧИК|Суд\s|Номер дела|$)", text)
    if m:
        result["plaintiff"] = m.group(1).strip()

    m = re.search(r"ОТВЕТЧИК:\s*(.+?)(?=Суд\s|Номер дела|$)", text)
    if m:
        result["defendant"] = m.group(1).strip()

    m = re.search(r"Суд\s*\([^)]*\)\s*первой инстанции:\s*(.+?)(?=Номер дела|$)", text)
    if m:
        result["court"] = m.group(1).strip()

    return result


# Паттерны дочерних структур Сбербанка, которые НЕ являются ПАО Сбербанк
# (страхование, НПФ, УК и т.п.). Порядок не важен — все применяются последовательно.
_SBER_SUBSIDIARY_PATTERNS = [
    # Сбербанк страхование [жизни] — СК ООО/АО «Сбербанк страхование жизни» и варианты
    re.compile(r'сбербанк\s+страхован\w*(?:\s+жизн\w*)?', re.IGNORECASE),
    # НПФ Сбербанк — АО «НПФ Сбербанк», «Негосударственный пенсионный фонд Сбербанк»
    re.compile(r'нпф\s+сбербанк', re.IGNORECASE),
    re.compile(r'негосударственн\w*\s+пенсионн\w*\s+фонд\w*\s+сбербанк', re.IGNORECASE),
    # Сбербанк Управление Активами — УК
    re.compile(r'сбербанк\s+управлен\w*\s+актив\w*', re.IGNORECASE),
    # Сбербанк Лизинг
    re.compile(r'сбербанк\s+лизинг\w*', re.IGNORECASE),
    # Сбербанк Факторинг
    re.compile(r'сбербанк\s+факторинг\w*', re.IGNORECASE),
]


def is_subsidiary_only_case(plaintiff: str, defendant: str) -> bool:
    """Вернуть True, если «сбербанк» упоминается только в названии дочерней структуры
    (страхование, НПФ, лизинг и т.п.), а не самого ПАО Сбербанк.

    Если «сбербанк» вообще не встречается в сторонах — возвращаем False
    (дело найдено по поиску, значит банк упомянут где-то ещё, например как третье лицо).
    """
    combined = (plaintiff + " " + defendant).lower()
    if "сбербанк" not in combined:
        return False
    cleaned = combined
    for pat in _SBER_SUBSIDIARY_PATTERNS:
        cleaned = pat.sub("", cleaned)
    return "сбербанк" not in cleaned


# Backward-compat alias
is_insurance_only_case = is_subsidiary_only_case


def _is_real_sberbank(name: str) -> bool:
    """True, если имя содержит ПАО Сбербанк (не дочку: страхование/НПФ/лизинг/УК).
    Возвращает False для пустых имён и для строк без подстроки «сбербанк»."""
    nm = (name or "").lower()
    if "сбербанк" not in nm:
        return False
    cleaned = nm
    for pat in _SBER_SUBSIDIARY_PATTERNS:
        cleaned = pat.sub("", cleaned)
    return "сбербанк" in cleaned


def determine_bank_role_from_participants(participants: list[dict]) -> str:
    """Вернуть фактическую роль ПАО Сбербанк по списку участников карточки.

    participants: список dict с ключами 'role' (вид участника, напр. ИСТЕЦ /
    ОТВЕТЧИК / ТРЕТЬЕ ЛИЦО) и 'name' (наименование стороны).

    Возвращает:
    - «Истец» / «Ответчик» / «Третье лицо» — если ПАО Сбербанк найден среди
      участников хотя бы один раз. При нескольких вхождениях приоритет:
      Ответчик > Истец > Третье лицо (банк может быть упомянут в разных ролях,
      но «Ответчик» — самая значимая для исхода).
    - "" (пустая строка) — если ПАО Сбербанка нет среди участников вообще
      (только дочки или вовсе нет). Внешний код решает, что с этим делать:
      для 1-й инстанции = «Третье лицо» (нейтрально), для кассации = drop.
    """
    found_roles: set[str] = set()
    for p in participants or []:
        if not _is_real_sberbank(p.get("name") or ""):
            continue
        role_up = (p.get("role") or "").upper()
        if "ОТВЕТЧИК" in role_up:
            found_roles.add("Ответчик")
        elif "ИСТЕЦ" in role_up or "ЗАЯВИТЕЛЬ" in role_up:
            found_roles.add("Истец")
        else:
            found_roles.add("Третье лицо")
    if "Ответчик" in found_roles:
        return "Ответчик"
    if "Истец" in found_roles:
        return "Истец"
    if "Третье лицо" in found_roles:
        return "Третье лицо"
    return ""


def parse_search_page(html: str) -> list[dict]:
    """
    Парсит страницу результатов поиска.
    Таблица результатов — 6-я на странице (индекс 5).
    Столбцы: Номер дела (ссылка) | Дата поступления |
             Категория/Стороны/Суд (объединённая) | Судья | ...
    """
    tables = extract_tables(html)
    if len(tables) < 6:
        log.warning(f"Ожидалось ≥6 таблиц, найдено {len(tables)}")
        return []

    results_table = tables[5]
    cases = []

    for row in results_table:
        if len(row) < 3:
            continue

        # Первый столбец — номер дела со ссылкой
        case_number_cell = row[0]
        case_number = cell_text(case_number_cell)

        # Пропускаем заголовок и строки без номера дела
        if not _CASE_NUM_RE.match(case_number):
            continue

        href = cell_href(case_number_cell)

        # Извлекаем case_id и case_uid из href
        cid, cuid = "", ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)

        date_received = cell_text(row[1]) if len(row) > 1 else ""

        # Третий столбец — объединённая ячейка с категорией, сторонами и судом
        combined = cell_text(row[2]) if len(row) > 2 else ""
        parsed = _parse_combined_cell(combined)
        category = parsed["category"]
        plaintiff = parsed["plaintiff"]
        defendant = parsed["defendant"]
        court = parsed["court"]

        # Пропускаем дела, где «Сбербанк» — только дочерняя структура (страхование, НПФ и т.п.)
        if is_subsidiary_only_case(plaintiff, defendant):
            log.info(f"Пропуск дела {case_number}: только Сбербанк Страхование")
            continue

        # Определяем роль банка
        role = "Третье лицо"
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in config.SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in config.SBER_PATTERNS):
            role = "Ответчик"

        link = f"{cid}|{cuid}" if cid and cuid else ""

        cases.append({
            "Номер дела": case_number,
            "Дата поступления": date_received,
            "Истец": plaintiff,
            "Ответчик": defendant,
            "Категория": category,
            "Суд 1 инстанции": court,
            "Судья 1 инстанции": "",
            "Роль банка": role,
            "Статус": "В производстве",
            "Последнее событие": "",
            "Дата события": "",
            "Время заседания": "",
            "Акт опубликован": "Нет",
            "Результат": "",
            "Ссылка": link,
            "Заметки": "",
            "Апеллянт": "",
            "Дата публикации акта": "",
            "Судья-докладчик": "",
        })

    return cases


def _find_results_table(tables: list) -> list | None:
    """Найти таблицу результатов поиска по заголовку (\"№ дела\").

    Для апелляции это обычно индекс 5, для 1 инстанции — индекс 8+.
    Надёжнее искать по содержимому заголовка.
    """
    for tbl in tables:
        if len(tbl) < 2:
            continue
        header_text = " ".join(cell_text(c) for c in tbl[0]).lower()
        if "дела" in header_text and ("дата" in header_text or "поступлен" in header_text):
            return tbl
    return None


def find_fi_case_link(html: str, case_number: str) -> str:
    """Найти в выдаче поиска 1-й инст. строку ровно этого дела → "cid|cuid".

    Для бэкфилла ссылок на карточку (см. linking.backfill_fi_links): целевой
    поиск по номеру (G1_CASE__CASE_NUMBERSS) сервер ведёт подстрокой, поэтому
    границу номера проверяем сами — текст ячейки должен быть равен номеру или
    продолжаться скобкой/тильдой (комбо-номер вида
    «2-716/2025 (2-9422/2024;) ~ М-7693/2024»), чтобы запрос «2-71/2025» не
    сматчил строку «2-716/2025». Возвращает "case_id|case_uid" или "".
    """
    if not case_number:
        return ""
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        return ""
    boundary = re.compile(rf'^{re.escape(case_number)}\s*(?:$|[(~])')
    for row in results_table:
        if not row:
            continue
        num_cell = row[0]
        if not boundary.match(cell_text(num_cell).strip()):
            continue
        href = cell_href(num_cell)
        if not href:
            continue
        m_id = _CASE_ID_RE.search(href)
        m_uid = _CASE_UID_RE.search(href)
        if m_id and m_uid:
            return f"{m_id.group(1)}|{m_uid.group(1)}"
    return ""


def parse_first_instance_search(html: str, court: CourtConfig) -> list[dict]:
    """Парсит страницу поиска суда первой инстанции.

    Отличия от parse_search_page (апелляция):
    - Таблица результатов ищется по заголовку, а не по индексу
    - 8 столбцов: № дела | Дата | Категория/Стороны | Судья | Дата решения | Решение | ...
    - Фильтр: только дела, где Сбербанк — ответчик
    - Номер дела может содержать '~' (материал) — берём первую часть
    """
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        log.warning(f"{court.name}: таблица результатов не найдена")
        return []

    cases = []
    for row in results_table:
        if len(row) < 3:
            continue

        case_number_cell = row[0]
        case_number_raw = cell_text(case_number_cell).strip()

        # Пропускаем заголовок и строки без номера дела
        if not _FI_CASE_NUM_RE.match(case_number_raw):
            continue

        # Номер может быть «2-5628/2026 ~ М-3298/2026» — берём первый.
        # Материалы (М-XXXX, 9-XXXX) тоже отслеживаем — юристу нужна
        # видимость по всем поступлениям против Сбербанка, не только по
        # основным гражданским делам.
        parts = [p.strip() for p in case_number_raw.split("~")]
        case_number = parts[0]
        # Хвостовой М-номер сохраняем отдельно — нужен для «промоушена»
        # ранее сохранённой М-записи в гражданское 2-XXX (когда материал
        # регистрируется и в выдаче появляется комбо-номер).
        material_number = next(
            (p for p in parts[1:] if p.startswith("М-")), ""
        )

        href = cell_href(case_number_cell)
        cid, cuid = "", ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)

        date_received = cell_text(row[1]).strip() if len(row) > 1 else ""

        # Третий столбец — объединённая ячейка с категорией и сторонами
        combined = cell_text(row[2]) if len(row) > 2 else ""
        parsed = _parse_combined_cell(combined)
        plaintiff = parsed["plaintiff"]
        defendant = parsed["defendant"]
        category = parsed["category"]

        # Судья — 4й столбец
        judge = cell_text(row[3]).strip() if len(row) > 3 else ""

        # Дата решения и результат (столбцы 4-5, могут быть пустые)
        result_date = cell_text(row[4]).strip() if len(row) > 4 else ""
        result = cell_text(row[5]).strip() if len(row) > 5 else ""

        # Пропускаем дела, где «Сбербанк» — только дочерняя структура (страхование, НПФ и т.п.)
        if is_subsidiary_only_case(plaintiff, defendant):
            continue

        # Определяем роль банка
        role = "Третье лицо"
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in config.SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in config.SBER_PATTERNS):
            role = "Ответчик"

        # Фильтр: только банк-ответчик
        if role != "Ответчик":
            continue

        link = f"{cid}|{cuid}" if cid and cuid else ""

        # Статус: если есть результат — решено
        status = "Решено" if result else "В производстве"

        cases.append({
            "case_number": case_number,
            "material_number": material_number,
            "filing_date": date_received,
            "plaintiff": plaintiff,
            "defendant": defendant,
            "category": category,
            "court": court.name,
            "court_domain": court.domain,
            "court_delo_id": court.delo_id,
            "court_srv_num": court.srv_num,
            "judge": judge,
            "bank_role": role,
            "status": status,
            "result": result,
            "result_date": result_date,
            "link": link,
        })

    return cases
