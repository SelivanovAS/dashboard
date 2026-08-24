# -*- coding: utf-8 -*-
"""Локальный атомарный checkpoint текущего сетевого прогона.

Это НЕ хранилище судебных данных и НЕ публичный ``parse_health.json``.
Checkpoint нужен для посмертного разбора оборванного процесса: какая фаза
шла, сколько карточек успели проверить и на каком HTTP-запросе процесс
остановился. Вызывающий явно включает запись через :func:`begin_run`; все
остальные функции до этого момента — безопасный no-op, поэтому общий
``fetch_page`` могут по-прежнему использовать импортёры и ручные утилиты.

Два уровня результата запроса намеренно разведены:

* transport — одна запись на реальную HTTP-попытку (200, ReadTimeout, reset);
* semantic — уточнение уже полученного HTTP 200 (blocked, captcha и т.п.).

Иначе ``fetch_page`` записал бы ``ok``, а ``fetch_card_checked`` следом
``blocked`` и один запрос превратился бы в два. ``request_id`` объединяет
ретраи одного логического запроса, ``attempt`` различает реальные попытки.

Все записи best-effort: сбой телеметрии один раз попадает в WARNING, но не
может остановить основной парсер. Файл пишется во временный файл в том же
каталоге, fsync'ается и публикуется через atomic ``os.replace``.
"""

from __future__ import annotations

import atexit
import copy
import json
import logging
import math
import os
import tempfile
import threading
from datetime import datetime
from typing import Any


VERSION = 1
HISTORY_LIMIT = 3
RECENT_FAILURES_LIMIT = 20

_LOG = logging.getLogger("court-monitor")
_LOCK = threading.RLock()

_path = ""
_state: dict[str, Any] = {}
_active = False
_write_warning_emitted = False
_run_id = ""
_request_seq = 0
_open_requests: set[str] = set()
_finished_attempts: set[tuple[str, int]] = set()
_semantic_by_request: dict[str, str] = {}
_durations: dict[str, list[float]] = {}


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _new_run_id() -> str:
    stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%f%z")
    return f"{stamp}-{os.getpid()}"


def _clean_text(value: Any, limit: int) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _safe_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return _clean_text(value, 500)


def _load_existing(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {"version": VERSION, "current": None, "history": []}
    except (OSError, ValueError) as exc:
        _warn_once(f"telemetry: не удалось прочитать checkpoint {path}: {exc}")
        return {"version": VERSION, "current": None, "history": []}
    if not isinstance(data, dict):
        return {"version": VERSION, "current": None, "history": []}
    history = data.get("history")
    if not isinstance(history, list):
        history = []
    current = data.get("current")
    if not isinstance(current, dict):
        current = None
    return {
        "version": VERSION,
        "current": current,
        "history": [x for x in history if isinstance(x, dict)][:HISTORY_LIMIT],
    }


def _warn_once(message: str) -> None:
    global _write_warning_emitted
    if _write_warning_emitted:
        return
    _write_warning_emitted = True
    _LOG.warning(message)


def _fsync_directory(directory: str) -> None:
    """Best-effort fsync каталога после replace (на Windows может не уметь)."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _persist_locked() -> bool:
    if not _active or not _path:
        return False
    directory = os.path.dirname(_path) or "."
    tmp = ""
    try:
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{os.path.basename(_path)}.{os.getpid()}.",
            suffix=".tmp",
            dir=directory,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(_state, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _path)
        tmp = ""
        _fsync_directory(directory)
        return True
    except Exception as exc:  # телеметрия не имеет права ронять парсер
        _warn_once(f"telemetry: checkpoint не записан ({type(exc).__name__}: {exc})")
        return False
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _current_locked() -> dict[str, Any] | None:
    if not _active:
        return None
    cur = _state.get("current")
    return cur if isinstance(cur, dict) else None


def _touch_locked(cur: dict[str, Any]) -> None:
    cur["updated_at"] = _now()


def _latency_summary(values: list[float]) -> dict[str, Any]:
    xs = sorted(values)
    if not xs:
        return {}

    def pct(p: float) -> float:
        idx = max(0, math.ceil(p * len(xs)) - 1)
        return xs[min(len(xs) - 1, idx)]

    return {
        "n": len(xs),
        "p50": round(pct(0.50), 3),
        "p90": round(pct(0.90), 3),
        "max": round(xs[-1], 3),
    }


def _inc(mapping: dict[str, int], key: str, delta: int = 1) -> None:
    mapping[key] = int(mapping.get(key, 0)) + delta
    if mapping[key] <= 0:
        mapping.pop(key, None)


def _append_recent_failure(cur: dict[str, Any], item: dict[str, Any]) -> None:
    failures = cur.setdefault("recent_failures", [])
    failures.append(item)
    del failures[:-RECENT_FAILURES_LIMIT]


def begin_run(path: str, region: str, *, run_id: str | None = None) -> str:
    """Начать локальный checkpoint и вернуть идентификатор прогона.

    Если прежний ``current`` остался в статусе ``running``, новый запуск сам
    фиксирует его как ``interrupted`` и переносит в bounded history. Это
    работает и после SIGKILL/``os._exit``, когда atexit не исполняется.
    """
    global _path, _state, _active, _write_warning_emitted, _run_id
    global _request_seq, _open_requests, _finished_attempts
    global _semantic_by_request, _durations

    with _LOCK:
        _path = os.path.abspath(os.fspath(path))
        _write_warning_emitted = False
        previous = _load_existing(_path)
        history = list(previous.get("history") or [])
        old = previous.get("current")
        now = _now()
        if isinstance(old, dict):
            old = copy.deepcopy(old)
            if old.get("status") == "running":
                old["last_checkpoint_at"] = old.get("updated_at")
                old["status"] = "interrupted"
                old["interruption_detected_at"] = now
                old["ended_at"] = now
            history.insert(0, old)

        _run_id = run_id or _new_run_id()
        current = {
            "run_id": _run_id,
            "region": _clean_text(region, 100),
            "pid": os.getpid(),
            "started_at": now,
            "updated_at": now,
            "status": "running",
            "phase": None,
            "coverage": {},
            "network": {
                "logical_requests_started": 0,
                "logical_requests_completed": 0,
                "attempts_started": 0,
                "attempts_completed": 0,
                "transport_outcomes": {},
                "semantic_outcomes": {},
                "latency_by_transport": {},
            },
            "in_flight": None,
            "recent_failures": [],
        }
        _state = {
            "version": VERSION,
            "current": current,
            "history": history[:HISTORY_LIMIT],
        }
        _request_seq = 0
        _open_requests = set()
        _finished_attempts = set()
        _semantic_by_request = {}
        _durations = {}
        _active = True
        _persist_locked()
        return _run_id


def set_phase(number: int, total: int, name: str) -> None:
    """Сохранить текущую фазу прогона."""
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        cur["phase"] = {
            "number": int(number),
            "total": int(total),
            "name": _clean_text(name, 300),
        }
        _touch_locked(cur)
        _persist_locked()


def set_coverage(
    stage: str,
    read: int,
    planned: int,
    *,
    processed: int | None = None,
    **extra: Any,
) -> None:
    """Сохранить прогресс одной инстанции без содержимого карточек."""
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        row: dict[str, Any] = {
            "read": max(0, int(read)),
            "planned": max(0, int(planned)),
        }
        if processed is not None:
            row["processed"] = max(0, int(processed))
        for key, value in extra.items():
            row[_clean_text(key, 100)] = _safe_scalar(value)
        cur.setdefault("coverage", {})[_clean_text(stage, 100)] = row
        _touch_locked(cur)
        _persist_locked()


def begin_fetch(
    host: str,
    context: str = "",
    *,
    attempt: int = 1,
    max_attempts: int = 1,
    request_id: str | None = None,
) -> str:
    """Начать реальную HTTP-попытку; вернуть/продолжить logical request id.

    Для ретрая вызывающий передаёт ``request_id``, возвращённый первой
    попыткой. Тогда logical request остаётся один, а ``attempts_started``
    честно растёт.
    """
    global _request_seq
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return request_id or ""
        network = cur["network"]
        if not request_id:
            _request_seq += 1
            request_id = f"{_run_id}:{_request_seq}"
        if request_id not in _open_requests:
            _open_requests.add(request_id)
            network["logical_requests_started"] += 1
        network["attempts_started"] += 1
        cur["in_flight"] = {
            "request_id": request_id,
            "attempt": max(1, int(attempt)),
            "max_attempts": max(1, int(max_attempts)),
            "host": _clean_text(host, 300),
            "context": _clean_text(context, 500),
            "started_at": _now(),
        }
        _touch_locked(cur)
        _persist_locked()
        return request_id


def finish_fetch_transport(
    request_id: str,
    kind: str,
    elapsed: float,
    *,
    attempt: int | None = None,
    will_retry: bool = False,
    status: int | None = None,
    error: str | None = None,
) -> None:
    """Закончить одну HTTP-попытку, не смешивая её с semantic verdict."""
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        in_flight = cur.get("in_flight") or {}
        attempt_no = max(1, int(
            attempt if attempt is not None else in_flight.get("attempt", 1)
        ))
        attempt_key = (request_id, attempt_no)
        if attempt_key in _finished_attempts:
            return
        _finished_attempts.add(attempt_key)

        outcome = _clean_text(kind, 100) or "unknown"
        duration = max(0.0, float(elapsed))
        network = cur["network"]
        network["attempts_completed"] += 1
        _inc(network["transport_outcomes"], outcome)
        samples = _durations.setdefault(outcome, [])
        samples.append(duration)
        network["latency_by_transport"][outcome] = _latency_summary(samples)

        if not will_retry and request_id in _open_requests:
            _open_requests.remove(request_id)
            network["logical_requests_completed"] += 1

        if (cur.get("in_flight") or {}).get("request_id") == request_id:
            cur["in_flight"] = None

        if not (outcome == "ok" or outcome.startswith("http_2")):
            item: dict[str, Any] = {
                "at": _now(),
                "layer": "transport",
                "kind": outcome,
                "request_id": request_id,
                "attempt": attempt_no,
                "elapsed": round(duration, 3),
                "host": _clean_text(in_flight.get("host", ""), 300),
                "context": _clean_text(in_flight.get("context", ""), 500),
            }
            if status is not None:
                item["status"] = int(status)
            if error:
                item["error"] = _clean_text(error, 200)
            _append_recent_failure(cur, item)

        _touch_locked(cur)
        _persist_locked()


def classify_semantic(
    request_id: str | None,
    kind: str,
    *,
    host: str = "",
    context: str = "",
    **details: Any,
) -> None:
    """Уточнить смысл уже завершённого HTTP-ответа, не добавляя попытку.

    Повторная классификация того же request заменяет прежний semantic-kind:
    счётчики остаются балансными и одна страница не становится двумя.
    ``request_id=None`` допустим для synthetic-события без HTTP (например,
    breaker skip); ему выдаётся отдельный локальный ключ.
    """
    global _request_seq
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        if not request_id:
            _request_seq += 1
            request_id = f"{_run_id}:synthetic:{_request_seq}"
        semantic = _clean_text(kind, 100) or "unknown"
        outcomes = cur["network"]["semantic_outcomes"]
        previous = _semantic_by_request.get(request_id)
        if previous == semantic:
            return
        if previous:
            _inc(outcomes, previous, -1)
        _inc(outcomes, semantic)
        _semantic_by_request[request_id] = semantic

        if semantic not in (
            "ok", "valid", "valid_card", "valid_search", "valid_act",
        ):
            item: dict[str, Any] = {
                "at": _now(),
                "layer": "semantic",
                "kind": semantic,
                "request_id": request_id,
                "host": _clean_text(host, 300),
                "context": _clean_text(context, 500),
            }
            for key, value in details.items():
                item[_clean_text(key, 100)] = _safe_scalar(value)
            _append_recent_failure(cur, item)
        _touch_locked(cur)
        _persist_locked()


def complete_run(*, status: str = "completed", **summary: Any) -> None:
    """Закрыть current checkpoint; сам файл сохраняется для диагностики."""
    global _active
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        now = _now()
        cur["status"] = _clean_text(status, 100) or "completed"
        cur["ended_at"] = now
        cur["updated_at"] = now
        cur["in_flight"] = None
        if summary:
            cur["summary"] = {
                _clean_text(k, 100): _safe_scalar(v)
                for k, v in summary.items()
            }
        # При временном сбое replace не выключаем процессную копию: atexit
        # получит ещё одну возможность дописать именно `completed`. Прежний
        # код всегда ставил _active=False, на диске оставался `running`, и
        # следующий запуск ложно архивировал штатный прогон как interrupted.
        _active = not _persist_locked()


def _mark_interrupted_at_exit() -> None:
    global _active
    with _LOCK:
        cur = _current_locked()
        if cur is None:
            return
        if cur.get("status") != "running":
            _persist_locked()
            _active = False
            return
        now = _now()
        cur["status"] = "interrupted"
        cur["ended_at"] = now
        cur["updated_at"] = now
        cur["interruption_reason"] = "process_exit_without_complete"
        _persist_locked()
        _active = False


def _reset_for_tests() -> None:
    """Сброс process-local состояния; файл не трогает."""
    global _path, _state, _active, _write_warning_emitted, _run_id
    global _request_seq, _open_requests, _finished_attempts
    global _semantic_by_request, _durations
    with _LOCK:
        _path = ""
        _state = {}
        _active = False
        _write_warning_emitted = False
        _run_id = ""
        _request_seq = 0
        _open_requests = set()
        _finished_attempts = set()
        _semantic_by_request = {}
        _durations = {}


atexit.register(_mark_interrupted_at_exit)
