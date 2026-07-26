# -*- coding: utf-8 -*-
"""Персистентность: cases.json / CSV, дедуп-файлы .digested_acts и
.cassation_acts, кэш LLM-пересказов мотивировок.

Пути берутся из config (env-переопределяемые), чтение — только config.X.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime

from court_monitor import config
from court_monitor.config import log

def load_digested_acts() -> set:
    """Загрузить множество номеров дел, чьи акты уже попали в дайджест."""
    if not os.path.exists(config.DIGESTED_ACTS_PATH):
        return set()
    with open(config.DIGESTED_ACTS_PATH, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_digested_acts(acts: set):
    """Сохранить множество номеров дел, чьи акты уже попали в дайджест."""
    os.makedirs(os.path.dirname(config.DIGESTED_ACTS_PATH) or ".", exist_ok=True)
    with open(config.DIGESTED_ACTS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(acts)) + "\n")


def load_cassation_acts() -> set:
    """Загрузить ключи кассационных определений, уже ушедших в дайджест
    (формат ключа — см. _cassation_act_key)."""
    if not os.path.exists(config.CASSATION_ACTS_PATH):
        return set()
    with open(config.CASSATION_ACTS_PATH, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_cassation_acts(acts: set):
    os.makedirs(os.path.dirname(config.CASSATION_ACTS_PATH) or ".", exist_ok=True)
    with open(config.CASSATION_ACTS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(acts)) + "\n")


def _cassation_act_key(cass_block: dict) -> str:
    """Ключ дедупа определения: «8Г-номер|дата». Дата — act_date (= дата
    вынесения при опубликованном тексте), фолбэк decision_date: если по
    одной жалобе когда-нибудь появится второе определение с другой датой,
    оно пройдёт в дайджест как новое."""
    num = (cass_block.get("case_number") or "").strip()
    dt = (cass_block.get("act_date") or cass_block.get("decision_date") or "").strip()
    if not num:
        return ""
    return f"{num}|{dt}"


def _load_act_summaries() -> dict:
    """Загрузить кэш LLM-пересказов мотивировок: {hash: {summary, ...}}."""
    if not os.path.exists(config.ACT_SUMMARIES_PATH):
        return {}
    try:
        with open(config.ACT_SUMMARIES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        log.warning(f"Не удалось прочитать {config.ACT_SUMMARIES_PATH}: {e}")
        return {}


def _save_act_summaries(cache: dict) -> None:
    """Сохранить кэш пересказов атомарно (tmp + replace)."""
    os.makedirs(os.path.dirname(config.ACT_SUMMARIES_PATH) or ".", exist_ok=True)
    tmp = config.ACT_SUMMARIES_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, config.ACT_SUMMARIES_PATH)


def load_csv(path: str) -> list[dict]:
    """Загрузить CSV в список словарей."""
    if not os.path.exists(path):
        log.warning(f"CSV не найден: {path}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def save_csv(cases: list[dict], path: str):
    """Сохранить список словарей в CSV (атомарно: temp + os.replace)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cases)
    os.replace(tmp, path)
    log.info(f"CSV сохранён: {path} ({len(cases)} дел)")


def load_json(path: str) -> dict:
    """Загрузить JSON-базу дел. Возвращает корневой объект {version, updated_at, cases}."""
    if not os.path.exists(path):
        log.warning(f"JSON не найден: {path}")
        return {"version": 1, "updated_at": "", "cases": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        # Поддержка старого формата (голый список)
        return {"version": 1, "updated_at": "", "cases": data}
    return data


def save_json(data: dict, path: str):
    """Сохранить JSON-базу дел атомарно (temp + os.replace)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    # Публичный блок региона — только в основной cases.json (фронт строит из
    # него подписи судов и ссылки; архивы фронт грузит без этого блока).
    if path == config.JSON_PATH:
        from court_monitor.regions import get_region  # ленивый: без цикла импортов
        data["region"] = get_region().public_info()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    count = len(data.get("cases", []))
    log.info(f"JSON сохранён: {path} ({count} дел)")


def bank_events_key(case: dict) -> str:
    """Композитный ключ мапы событий bank-дела: «домен|номер». Номера дел не
    уникальны между судами (тот же принцип, что case_court_key в linking.py),
    поэтому голый id ключом быть не может."""
    fi = case.get("first_instance") or {}
    domain = (fi.get("court_domain") or "").strip()
    num = (case.get("id") or fi.get("case_number") or "").strip()
    return f"{domain}|{num}"


def load_bank_json(list_path: str, events_path: str) -> dict:
    """Загрузить bank-базу «список + events» и вернуть СКЛЕЕННЫЕ записи —
    как будто events всегда лежали в first_instance.events. Весь пайплайн
    (детект изменений, state machine, дайджест) работает с полными записями
    и о split-хранении не знает.

    Обратная совместимость: старый монолитный файл (events inline, events-файла
    нет) читается как есть; запись с непустыми inline events не перетирается
    мапой (источник истины — то, что реально лежит в записи)."""
    data = load_json(list_path)
    events_map: dict = {}
    if os.path.exists(events_path):
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            events_map = raw.get("events") or {}
        except (OSError, json.JSONDecodeError) as e:
            log.warning(f"Не удалось прочитать events-файл {events_path}: {e}")
    for case in data.get("cases", []):
        fi = case.get("first_instance")
        if not isinstance(fi, dict):
            continue
        if fi.get("events"):
            continue  # inline events (старый монолит) — оставляем как есть
        fi["events"] = events_map.get(bank_events_key(case), [])
    return data


def save_bank_json(data: dict, list_path: str, events_path: str):
    """Сохранить bank-базу split-форматом: список БЕЗ events + отдельная мапа
    «домен|номер» → events. Split недеструктивный — записи в data остаются
    склеенными (пайплайн после сохранения продолжает видеть events).
    Содержимое events не меняется ни на байт: по паре (date, text) идёт дедуп
    событий (_events_newly_match), любая мутация = дайджест-паводок.

    ⚠️ events-файл перезаписывается ЦЕЛИКОМ из переданных записей — перед
    сохранением базу обязательно грузить через load_bank_json (склеенной),
    иначе события дел, не тронутых текущим кодом, будут потеряны."""
    events_map: dict = {}
    slim_cases = []
    for case in data.get("cases", []):
        fi = case.get("first_instance")
        if isinstance(fi, dict) and fi.get("events") is not None:
            events_map[bank_events_key(case)] = fi.get("events") or []
            slim_fi = {k: v for k, v in fi.items() if k != "events"}
            case = {**case, "first_instance": slim_fi}
        slim_cases.append(case)
    slim_data = {k: v for k, v in data.items() if k != "cases"}
    slim_data["cases"] = slim_cases
    save_json(slim_data, list_path)
    data["updated_at"] = slim_data["updated_at"]
    save_json({"version": 1, "track": "plaintiff_light",
               "events": events_map}, events_path)
