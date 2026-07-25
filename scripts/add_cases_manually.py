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
import sys
from urllib.parse import quote

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# Импорт напрямую из модулей пакета court_monitor (бывший монолит
# update_cases.py распилен — см. docs/Распил_монолита_контекст.md).
from court_monitor.config import (  # noqa: E402
    JSON_PATH,
    log,
)
from court_monitor.courts import FIRST_INSTANCE_COURTS  # noqa: E402
from court_monitor.netutil import fetch_card_checked, fetch_page, polite_delay  # noqa: E402
from court_monitor.parsing import (  # noqa: E402
    is_subsidiary_only_case,
    parse_case_card,
)
from court_monitor.storage import load_json, save_json  # noqa: E402

# Разбор поисковой строки и сборка записи вынесены в пакет (переиспользуются
# импортёром реестра исков банка — scripts/import_bank_registry.py).
from court_monitor.target_search import (  # noqa: E402,F401
    build_json_entry,
    determine_bank_role,
    parse_search_row,
)


CASES_TO_ADD: list[tuple[str, str]] = [
    ("sovetsk--hmao.sudrf.ru", "2-193/2026"),
]


# Принудительная роль банка для дел, где Сбербанк не прописан в plaintiff/defendant
# на странице поиска суда (например, суд забыл его указать). Ключ — bare case_number,
# значение — "Истец" | "Ответчик" | "Третье лицо". Такие дела пропускают проверку
# [NO SBERBANK] и добавляются с указанной ролью.
FORCE_BANK_ROLE: dict[str, str] = {
    "2-193/2026": "Ответчик",  # Советский районный — Сбер ответчик по факту,
                                # на сайте суда может быть не указан в сторонах.
    "2-1012/2026": "Третье лицо",  # Нефтеюганский — иск прокурора, особое производство.
    "2-1071/2026": "Третье лицо",  # Нефтеюганский — иск прокурора, особое производство.
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
        html = fetch_page(search_url, context=case_num)
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
        card_html = fetch_card_checked(card_url, context=case_num)
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
