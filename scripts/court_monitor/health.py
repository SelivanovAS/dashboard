# -*- coding: utf-8 -*-
"""Журнал здоровья парсеров и детектор «молчаливой поломки»:
суд со стабильной историей результатов вдруг отдаёт 0 / HTTP-фейлы подряд /
все источники разом по нулям. update_parse_health возвращает (state, alerts) —
отправка алертов остаётся на вызывающем (main_json).
"""

from __future__ import annotations

import json
import os
import statistics
import tempfile
from datetime import datetime

from court_monitor import config
from court_monitor.config import log

# ── Здоровье парсеров (детектор молчаливой поломки) ─────────────────────────

def load_parse_health() -> dict:
    """Загрузить журнал здоровья парсеров ({version, updated_at, sources})."""
    if not os.path.exists(config.PARSE_HEALTH_PATH):
        return {"version": 1, "updated_at": "", "sources": {}}
    try:
        with open(config.PARSE_HEALTH_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        log.warning(f"parse-health: не удалось прочитать {config.PARSE_HEALTH_PATH}")
        return {"version": 1, "updated_at": "", "sources": {}}
    if not isinstance(data, dict):
        return {"version": 1, "updated_at": "", "sources": {}}
    data.setdefault("sources", {})
    return data


def save_parse_health(state: dict) -> None:
    """Атомарно сохранить итоговый публичный журнал здоровья.

    Непрерывная телеметрия живёт отдельно, но финальный ``parse_health.json``
    тоже нельзя оставлять обрезанным при сне Mac/заполненном диске: его читают
    гейт доставки и админка. Временный файл создаём в том же каталоге, затем
    fsync + replace. Ошибку не глотаем — вызывающий уже умеет превратить её в
    WARNING, а прежний валидный журнал при этом остаётся на месте.
    """
    path = config.PARSE_HEALTH_PATH
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = ""
    try:
        fd, tmp = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}.{os.getpid()}.",
            suffix=".tmp",
            dir=directory,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = ""
        # После replace данные уже консистентны. fsync каталога —
        # best-effort усиление на случай внезапного отключения питания.
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            dir_fd = os.open(directory, flags)
        except OSError:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            except OSError:
                pass
            finally:
                os.close(dir_fd)
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def update_parse_health(
    observations: dict,
    labels: dict | None = None,
    state: dict | None = None,
) -> tuple[dict, list[str]]:
    """Обновить журнал здоровья парсеров и вернуть (state, список алертов).

    observations: {ключ источника: int — сколько строк дал поиск в этом
    прогоне; None — страница поиска не загрузилась после всех ретраев}.
    labels: {ключ: человекочитаемое имя для алертов}.
    state: журнал (по умолчанию читается из config.PARSE_HEALTH_PATH; параметр —
    для тестов).

    Правила алертов:
    - «стал нулём»: медиана последних успешных прогонов ≥1, а сегодня 0 —
      алерт на 1-м и 3-м нулевом прогоне подряд, дальше тишина до
      восстановления (тогда придёт «снова отдаёт результаты»). Суды, у
      которых 0 — норма (нет дел банка на первой странице), не алертят:
      медиана их истории < 1.
    - HTTP-фейл config.PARSE_HEALTH_FAIL_ALERT прогонов подряд — алерт на каждом
      кратном пороге (3, 6, 9…): одиночные сетевые сбои не шумят.
    - Все источники разом 0/фейл при живой истории — отдельный алерт
      (лежит sudrf целиком или глобально сменилась вёрстка).
    """
    labels = labels or {}
    state = state if state is not None else load_parse_health()
    sources = state.setdefault("sources", {})
    alerts: list[str] = []
    now_iso = datetime.now().isoformat(timespec="seconds")

    for key, count in observations.items():
        src = sources.setdefault(key, {
            "counts": [], "zero_streak": 0, "fail_streak": 0,
            "alerted_zero": False,
        })
        name = labels.get(key, key)
        # Человекочитаемое имя источника пишем в журнал: админка читает его
        # отсюда (s.label) вместо ручной карты COURT_NAMES — реестры регионов
        # больше не нужно синхронизировать с admin_page.js вручную.
        if key in labels:
            src["label"] = labels[key]
        if count is None:
            src["fail_streak"] = int(src.get("fail_streak", 0)) + 1
            src["last_run_at"] = now_iso
            if src["fail_streak"] % config.PARSE_HEALTH_FAIL_ALERT == 0:
                alerts.append(
                    f"{name}: страница поиска не загружается "
                    f"{src['fail_streak']} прогонов подряд"
                )
            continue
        src["fail_streak"] = 0
        history = [c for c in src.get("counts", []) if isinstance(c, int)]
        median = statistics.median(history) if history else 0
        if count == 0 and median >= 1:
            src["zero_streak"] = int(src.get("zero_streak", 0)) + 1
            if src["zero_streak"] in (1, 3):
                src["alerted_zero"] = True
                alerts.append(
                    f"{name}: поиск вернул 0 результатов, хотя обычно "
                    f"~{int(median)} ({src['zero_streak']}-й нулевой "
                    f"прогон подряд)"
                )
        elif count > 0:
            if src.get("alerted_zero"):
                alerts.append(f"{name}: снова отдаёт результаты ({count})")
            src["zero_streak"] = 0
            src["alerted_zero"] = False
        src["counts"] = (history + [count])[-config.PARSE_HEALTH_HISTORY_LEN:]
        src["last_count"] = count
        src["last_run_at"] = now_iso

    # Глобальный ноль: ни один источник не дал результатов, при том что
    # раньше жизнь была (иначе первый прогон на пустой истории алертил бы).
    # Требуем ≥2 источников: для одиночного это дубль пер-судового алерта.
    if len(observations) >= 2:
        all_dead = all((c is None or c == 0) for c in observations.values())
        had_life = any(
            any(isinstance(x, int) and x > 0
                for x in (sources.get(k, {}).get("counts") or []))
            for k in observations
        )
        if all_dead and had_life:
            alerts.append(
                "ВСЕ источники разом вернули 0 или не загрузились — похоже, "
                "лежит sudrf целиком либо глобально сменилась вёрстка"
            )

    state["updated_at"] = now_iso
    return state, alerts
