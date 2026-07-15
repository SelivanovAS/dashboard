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
