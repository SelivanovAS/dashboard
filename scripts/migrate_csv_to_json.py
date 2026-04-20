#!/usr/bin/env python3
"""
Одноразовая миграция: CSV → JSON.

Читает sberbank_cases.csv и sberbank_cases_archive.csv,
конвертирует в новый JSON-формат с поддержкой стадий (первая инстанция / апелляция).

Существующие дела — апелляционные, поэтому:
- first_instance содержит только суд и судью (то, что уже есть в CSV)
- appeal содержит все данные о ходе дела в апелляции
- id = номер апелляционного дела (первичный ключ 1 инстанции пока неизвестен)
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime


def csv_to_cases(csv_path: str) -> list[dict]:
    """Прочитать CSV и конвертировать каждую строку в JSON-структуру."""
    if not os.path.exists(csv_path):
        print(f"  Файл не найден: {csv_path}")
        return []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    cases = []
    for row in rows:
        case_num = row.get("Номер дела", "").strip()
        if not case_num:
            continue

        # Данные первой инстанции (только суд и судья, если известны)
        fi_court = row.get("Суд 1 инстанции", "").strip()
        fi_judge = row.get("Судья 1 инстанции", "").strip()
        first_instance = None
        if fi_court:
            first_instance = {
                "case_number": "",  # номер дела 1 инстанции пока неизвестен
                "court": fi_court,
                "court_domain": "",  # будет заполнен позже
                "judge": fi_judge,
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
            }

        # Данные апелляции
        appeal = {
            "case_number": case_num,
            "court": "Суд ХМАО-Югры",
            "judge_reporter": row.get("Судья-докладчик", "").strip(),
            "filing_date": row.get("Дата поступления", "").strip(),
            "status": row.get("Статус", "").strip(),
            "result": row.get("Результат", "").strip(),
            "last_event": row.get("Последнее событие", "").strip(),
            "event_date": row.get("Дата события", "").strip(),
            "hearing_date": row.get("Дата заседания", "").strip(),
            "hearing_time": row.get("Время заседания", "").strip(),
            "link": row.get("Ссылка", "").strip(),
            "act_published": row.get("Акт опубликован", "").strip() == "Да",
            "act_date": row.get("Дата публикации акта", "").strip(),
            "appellant": row.get("Апеллянт", "").strip(),
            "events": [],
        }

        case = {
            "id": case_num,  # пока ключ = номер апелляционного дела
            "current_stage": "appeal",
            "plaintiff": row.get("Истец", "").strip(),
            "defendant": row.get("Ответчик", "").strip(),
            "category": row.get("Категория", "").strip(),
            "bank_role": row.get("Роль банка", "").strip(),
            "notes": row.get("Заметки", "").strip(),
            "first_instance": first_instance,
            "appeal": appeal,
        }
        cases.append(case)

    return cases


def main():
    data_dir = os.environ.get("DATA_DIR", "data")
    csv_active = os.path.join(data_dir, "sberbank_cases.csv")
    csv_archive = os.path.join(data_dir, "sberbank_cases_archive.csv")
    json_active = os.path.join(data_dir, "cases.json")
    json_archive = os.path.join(data_dir, "cases_archive.json")

    print(f"Миграция CSV → JSON")
    print(f"  Активные: {csv_active}")
    active_cases = csv_to_cases(csv_active)
    print(f"  → {len(active_cases)} дел")

    print(f"  Архив: {csv_archive}")
    archive_cases = csv_to_cases(csv_archive)
    print(f"  → {len(archive_cases)} дел")

    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Активные
    active_data = {
        "version": 1,
        "updated_at": now,
        "cases": active_cases,
    }
    with open(json_active, "w", encoding="utf-8") as f:
        json.dump(active_data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"  Записано: {json_active}")

    # Архив
    archive_data = {
        "version": 1,
        "updated_at": now,
        "cases": archive_cases,
    }
    with open(json_archive, "w", encoding="utf-8") as f:
        json.dump(archive_data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"  Записано: {json_archive}")

    print("Готово!")


if __name__ == "__main__":
    main()
