#!/usr/bin/env python3
"""Одноразовый скрипт: добавить дела первой инстанции в data/cases.json по списку
(court_domain, case_number).

Зачем: авто-скрапер берёт только первую страницу поиска суда и фильтрует по
«Сбербанк — ответчик». Дела, не попадающие в эту выборку (старые / банк — истец),
приходится ставить на отслеживание вручную.

Поток:
1. Для каждой пары (court_domain, case_number) ищем дело на сайте суда
   через параметр G1_CASE__CASE_NUMBERSS (поиск по номеру, без фильтра по стороне).
2. Парсим поисковую строку → извлекаем link=case_id|case_uid.
3. Загружаем карточку дела, парсим события/статус/судью через parse_case_card.
4. Собираем JSON-entry и добавляем в cases.json (с дедупом по id).

Пропуски логируются и в cases.json не пишутся:
- [ALREADY TRACKED]  — уже в cases.json
- [NOT FOUND]        — поиск вернул пусто / нужное дело не в результатах
- [NO SBERBANK]      — Сбербанк не в plaintiff/defendant (не сторона дела)
- [SUBSIDIARY ONLY]  — упомянута только дочка Сбера (страхование, НПФ и т.п.)
- [FETCH FAIL]       — сбой загрузки страницы суда

Запуск: python3 scripts/add_cases_manually.py (из корня репо).
"""
from __future__ import annotations

import os
import re
import sys
from urllib.parse import quote

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from update_cases import (  # noqa: E402
    FIRST_INSTANCE_COURTS,
    JSON_PATH,
    SBER_PATTERNS,
    _CASE_ID_RE,
    _CASE_NUM_RE,
    _CASE_UID_RE,
    _find_results_table,
    _parse_combined_cell,
    cell_href,
    cell_text,
    extract_tables,
    fetch_page,
    is_subsidiary_only_case,
    load_json,
    log,
    parse_case_card,
    polite_delay,
    save_json,
)


CASES_TO_ADD: list[tuple[str, str]] = [
    ("vartovgor--hmao.sudrf.ru", "2-1394/2026"),
    ("surggor--hmao.sudrf.ru",   "2-2408/2026"),
]


# Принудительная роль банка для дел, где Сбербанк не прописан в plaintiff/defendant
# на странице поиска суда (например, суд забыл его указать). Ключ — bare case_number,
# значение — "Истец" | "Ответчик" | "Третье лицо". Такие дела пропускают проверку
# [NO SBERBANK] и добавляются с указанной ролью.
FORCE_BANK_ROLE: dict[str, str] = {
    "2-216/2026": "Ответчик",  # Советский районный — Сбер ответчик по факту,
                                # на сайте суда указан только Альфа-Банк.
}


def build_case_number_search_url(court, case_number: str) -> str:
    """URL поиска по номеру дела (параметр G1_CASE__CASE_NUMBERSS).

    Подтверждено эмпирически: этот параметр работает на first-instance карточках
    судов ХМАО-Югры. Submit закодирован в win-1251 (Найти).
    """
    case_enc = quote(case_number.encode("windows-1251"))
    return (
        f"{court.base_url}/modules.php?name=sud_delo&srv_num={court.srv_num}"
        f"&name_op=r&delo_id={court.delo_id}&case_type=0&new=0"
        f"&G1_CASE__CASE_NUMBERSS={case_enc}"
        f"&delo_table=g1_case&Submit=%CD%E0%E9%F2%E8"
    )


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
        if not _CASE_NUM_RE.match(case_number_raw):
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


def main() -> None:
    courts_by_domain = {c.domain: c for c in FIRST_INSTANCE_COURTS}

    data = load_json(JSON_PATH)
    cases = data.get("cases", [])
    # Индекс содержит и полный id вида "2-583/2026 (2-9702/2025;)", и «голую»
    # часть "2-583/2026" — так же, как делает main_json() для дедупа архивов.
    existing_ids: set[str] = set()
    for c in cases:
        cid = (c.get("id") or "").strip()
        if cid:
            existing_ids.add(cid)
            bare = cid.split("(")[0].strip()
            if bare and bare != cid:
                existing_ids.add(bare)

    stats = {
        "added": 0,
        "already": 0,
        "not_found": 0,
        "no_sber": 0,
        "subsidiary": 0,
        "fetch_fail": 0,
        "unknown_court": 0,
    }
    new_entries: list[dict] = []

    total = len(CASES_TO_ADD)
    for i, (domain, case_num) in enumerate(CASES_TO_ADD, 1):
        log.info(f"[{i}/{total}] {domain} / {case_num}")

        if case_num in existing_ids:
            log.info("  [ALREADY TRACKED]")
            stats["already"] += 1
            continue

        court = courts_by_domain.get(domain)
        if not court:
            log.warning(f"  [UNKNOWN COURT] домен не найден в FIRST_INSTANCE_COURTS")
            stats["unknown_court"] += 1
            continue

        polite_delay()
        search_url = build_case_number_search_url(court, case_num)
        html = fetch_page(search_url)
        if not html:
            log.warning("  [FETCH FAIL] поисковая страница")
            stats["fetch_fail"] += 1
            continue

        fi_row = parse_search_row(html, court, case_num)
        if not fi_row:
            log.warning("  [NOT FOUND] дело не найдено в результатах поиска")
            stats["not_found"] += 1
            continue

        if is_subsidiary_only_case(fi_row["plaintiff"], fi_row["defendant"]):
            log.info("  [SUBSIDIARY ONLY] упомянута только дочка Сбера — пропуск")
            stats["subsidiary"] += 1
            continue

        role = determine_bank_role(fi_row["plaintiff"], fi_row["defendant"])
        if role is None:
            forced = FORCE_BANK_ROLE.get(case_num)
            if forced:
                log.info(
                    "  [FORCED ROLE] Сбер не в сторонах по данным суда, "
                    "ставим role=%s вручную",
                    forced,
                )
                role = forced
            else:
                log.info(
                    "  [NO SBERBANK] plaintiff=%r defendant=%r",
                    fi_row["plaintiff"][:80],
                    fi_row["defendant"][:80],
                )
                stats["no_sber"] += 1
                continue
        fi_row["bank_role"] = role

        link = fi_row["link"]
        if not link or "|" not in link:
            log.warning("  [FETCH FAIL] в поиске нет case_id/case_uid — не сможем авто-обновлять")
            stats["fetch_fail"] += 1
            continue
        cid, _, cuid = link.partition("|")

        polite_delay()
        card_url = court.card_url(cid, cuid)
        card_html = fetch_page(card_url)
        if not card_html:
            log.warning("  [FETCH FAIL] карточка дела")
            stats["fetch_fail"] += 1
            continue
        card_info = parse_case_card(card_html, court.base_url)

        entry = build_json_entry(fi_row, card_info)
        new_entries.append(entry)
        existing_ids.add(case_num)
        stats["added"] += 1
        fi = entry["first_instance"]
        log.info(
            "  [OK] role=%s judge=%r hearing=%s last=%r",
            role,
            (fi["judge"] or "")[:40],
            fi["hearing_date"] or "—",
            (fi["last_event"] or "")[:60],
        )

    if new_entries:
        data["cases"] = new_entries + cases
        save_json(data, JSON_PATH)
    else:
        log.info("Нечего добавлять — cases.json не изменён")

    log.info("=" * 60)
    log.info(
        "Итого: +%d новых | %d уже в базе | %d не найдено | %d без Сбербанка | "
        "%d subsidiary-only | %d сбоев загрузки | %d неизв. суд",
        stats["added"],
        stats["already"],
        stats["not_found"],
        stats["no_sber"],
        stats["subsidiary"],
        stats["fetch_fail"],
        stats["unknown_court"],
    )


if __name__ == "__main__":
    main()
