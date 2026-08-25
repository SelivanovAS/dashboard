# -*- coding: utf-8 -*-
"""Приём дел апелляции: общие правила для всех каналов ввода.

Зеркало bank_intake.py. Каналов ввода апелляции с 25.08.2026 два:

1. поиск апелляции в прогоне (`main_json`, фаза 3) + целевой дослинк
   `relink_awaiting_appeal` — оба живут в runs.py;
2. **дамп выдачи капчёвого апел-суда** (`scripts/import_search_dump.py`):
   Свердловский областной суд закрыл поиск проверочным кодом, карточки при
   этом открыты — та же модель, что у 54 судов 1-й инстанции области.

Скрипты `scripts/*.py` не имеют права тянуть runs.py целиком (он подтягивает
дайджест и доставку), поэтому конвертеры строки выдачи живут здесь, а runs.py
их ре-экспортирует под прежними приватными именами — существующие вызовы и
патчи тестов работают без правок.
"""

from __future__ import annotations

from court_monitor.courts import appeal_court_by_domain
from court_monitor.parsing.cards import _warn_if_card_degraded


def enrich_appeal_row_from_card(nc: dict, card_info: dict) -> str:
    """Обогатить CSV-строку апел. дела данными его карточки (parse_case_card).

    Общий код поиска апелляции, целевого дослинка (relink_awaiting_appeal) и
    импортёра дампов. Возвращает «Номер дела 1 инстанции» с карточки
    ("" — суд ещё не проставил).
    """
    _warn_if_card_degraded(card_info, nc["Номер дела"])
    nc["Последнее событие"] = card_info.get("Последнее событие", "")
    nc["Дата события"] = card_info.get("Дата события", "")
    nc["Время заседания"] = card_info.get("Время заседания", "")
    nc["Статус"] = card_info.get("Статус", "В производстве")
    nc["Результат"] = card_info.get("Результат", "")
    nc["Акт опубликован"] = card_info.get("Акт опубликован", "Нет")
    if card_info.get("Судья 1 инстанции"):
        nc["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
    if card_info.get("Судья-докладчик"):
        nc["Судья-докладчик"] = card_info["Судья-докладчик"]
    # Номер 1-й инст. — и В СТРОКУ (13.08.2026): секция «Новые дела»
    # дайджеста печатает его в шапке дела, юрист сразу видит, какое дело
    # поехало наверх. В CSV колонка не уедет (CSV_COLUMNS фиксирован,
    # extrasaction="ignore"), а digest-контекст и replay ключ сохранят.
    nc["Номер дела 1 инстанции"] = card_info.get("Номер дела 1 инстанции", "")
    return card_info.get("Номер дела 1 инстанции", "")


def appeal_row_to_json_case(
    row: dict,
    fi_number_lookup: dict[tuple[str, str], str] | None = None,
    court=None,
) -> dict:
    """Конвертировать CSV-строку апел. дела (после обогащения parse_case_card)
    в JSON-структуру для cases.json. Без этой конверсии новое апел. дело
    оседает только в CSV: link_cases ищет апел. в существующем JSON-индексе
    и молча пропускает то, чего там ещё нет.

    fi_number_lookup — словарь {(домен_апел_суда, номер_апелляции) →
    номер_1_инст}, который main_json собирает по результатам парсинга апел.
    карточек (ключ составной: номера 33-… между двумя апел-судами региона не
    уникальны). Если запись есть, кладём её в first_instance.case_number сразу,
    чтобы новое дело с самого начала имело корректный якорь для
    link_cassation_cases (иначе кассация на 7kas не находит существующее дело
    по `fi_case_number` и создаёт двойник через discovery — см. кейс
    33-1643/2026 ↔ 8Г-7248/2026). Без словаря — поведение прежнее (`""`).

    Суд апелляции — из сервисного ключа строки `_appeal_domain` (проставляет
    поиск апелляции); без него — первый апел-суд региона (legacy). Явный
    `court` перебивает оба: импортёр дампа уже держит CourtConfig выбранного
    суда, и брать его из фасада активного региона (он собран НА ИМПОРТЕ
    модуля) там незачем."""
    case_num = (row.get("Номер дела") or "").strip()
    ap_court = court or appeal_court_by_domain(row.get("_appeal_domain"))
    fi_case_number = ""
    if fi_number_lookup and case_num:
        fi_case_number = (
            fi_number_lookup.get((ap_court.domain, case_num)) or ""
        ).strip()
    return {
        "id": case_num,
        "current_stage": "appeal",
        "plaintiff": row.get("Истец", ""),
        "defendant": row.get("Ответчик", ""),
        "category": row.get("Категория", ""),
        "bank_role": row.get("Роль банка", ""),
        "notes": row.get("Заметки", ""),
        "first_instance": {
            "case_number": fi_case_number,
            "court": row.get("Суд 1 инстанции", ""),
            "court_domain": "",
            "judge": row.get("Судья 1 инстанции", ""),
            "filing_date": "",
            "status": "",
            "result": "",
            "last_event": "",
            "event_date": "",
            "hearing_date": "",
            "hearing_time": "",
            "link": "",
            "act_published": False,
            "act_date": "",
            "events": [],
        },
        "appeal": {
            "case_number": case_num,
            "court": ap_court.name,
            "court_domain": ap_court.domain,
            "delo_id": ap_court.delo_id,
            "judge_reporter": row.get("Судья-докладчик", ""),
            "filing_date": row.get("Дата поступления", ""),
            "status": row.get("Статус", "В производстве"),
            "result": row.get("Результат", ""),
            "last_event": row.get("Последнее событие", ""),
            "event_date": row.get("Дата события", ""),
            "hearing_date": row.get("Дата заседания", ""),
            "hearing_time": row.get("Время заседания", ""),
            "link": row.get("Ссылка", ""),
            "act_published": row.get("Акт опубликован", "Нет") == "Да",
            "act_date": row.get("Дата публикации акта", ""),
            "appellant": row.get("Апеллянт", ""),
            "events": [],
        },
    }


def appeal_case_to_row(case: dict) -> dict:
    """Обратная конверсия: JSON-дело стадии `appeal` → строка выдачи.

    Нужна ровно одному потребителю — анонсу дел, заведённых дампом апелляции
    между прогонами (`announce_imported_appeal_cases` в runs.py): секция
    «📥 Новые дела» дайджеста рендерится по СТРОКАМ выдачи, а не по JSON-делу.

    ⚠️ Строка предназначена ТОЛЬКО для контекста дайджеста и в CSV не уходит:
    строку CSV импортёр записал сам в момент импорта, и повторная запись дала
    бы делу второй ряд (карточки апелляции обходятся по CSV).
    """
    ap = case.get("appeal") or {}
    fi = case.get("first_instance") or {}
    return {
        "Номер дела": (ap.get("case_number") or case.get("id") or "").strip(),
        "Дата поступления": ap.get("filing_date", ""),
        "Истец": case.get("plaintiff", ""),
        "Ответчик": case.get("defendant", ""),
        "Категория": case.get("category", ""),
        "Суд 1 инстанции": fi.get("court", ""),
        "Судья 1 инстанции": fi.get("judge", ""),
        "Роль банка": case.get("bank_role", ""),
        "Статус": ap.get("status", "В производстве"),
        "Последнее событие": ap.get("last_event", ""),
        "Дата события": ap.get("event_date", ""),
        "Дата заседания": ap.get("hearing_date", ""),
        "Время заседания": ap.get("hearing_time", ""),
        "Акт опубликован": "Да" if ap.get("act_published") else "Нет",
        "Результат": ap.get("result", ""),
        "Ссылка": ap.get("link", ""),
        "Заметки": case.get("notes", ""),
        "Апеллянт": ap.get("appellant", ""),
        "Дата публикации акта": ap.get("act_date", ""),
        "Судья-докладчик": ap.get("judge_reporter", ""),
        # Хвост строки «(дело 1-й инст. № …)» в секции «Новые дела» — тот же
        # ключ, что кладёт enrich_appeal_row_from_card.
        "Номер дела 1 инстанции": fi.get("case_number", ""),
        "_appeal_domain": ap.get("court_domain", ""),
    }
