"""Отдельная доставка готового выпуска без повторного обхода судов."""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[2]
RESERVE = REPO / "ops" / "mac-local-run"
sys.path.insert(0, str(RESERVE))

import cloud_run_ok  # noqa: E402


@pytest.fixture
def current_issue(monkeypatch):
    from court_monitor import config

    monkeypatch.setattr(cloud_run_ok, "_today_dates", lambda: {"2026-09-14"})
    monkeypatch.setattr(config, "REGION", "hmao")
    ctx = {
        "saved_at": "2026-09-14T06:20:00",
        "issue_key": "2026-09-14T06:20:00",
        **{key: [] for key in cloud_run_ok.CTX_DELTA_KEYS},
    }
    state = {"last_run": {
        "at": "2026-09-14T06:19:00",
        "cards_read_today": 261,
        "cards_planned_today": 261,
    }}
    return state, ctx


def test_completed_day_without_events_is_deliverable(current_issue):
    state, ctx = current_issue
    before = copy.deepcopy((state, ctx))
    assert cloud_run_ok.can_deliver_today(state, ctx)
    assert not cloud_run_ok.context_pending(ctx)
    assert (state, ctx) == before, "Проверка готовности не должна ставить штамп"


def test_incomplete_run_with_events_can_be_delivered(current_issue):
    state, ctx = current_issue
    state["last_run"]["cards_read_today"] = 10
    state["sources"] = {"fi:example": {
        "last_run_at": "2026-09-14T06:15:00", "last_count": 0, "fail_streak": 3,
    }}
    ctx["fi_changes"] = [{"number": "2-1/2026"}]
    assert not cloud_run_ok.run_complete_today(state)[0]
    assert cloud_run_ok.context_pending(ctx)
    assert cloud_run_ok.can_deliver_today(state, ctx)


@pytest.mark.parametrize("changes", [
    {"saved_at": "2026-09-13T08:00:00"},
    {"saved_at": ""},
    {"issue_key": ""},
    {"issue_key": "  "},
    {"delivered_at": "2026-09-14T08:45:00"},
    {"delivery_id": "bashkortostan:2026-09-14T06:20:00"},
])
def test_invalid_or_delivered_context_is_not_ready(current_issue, changes):
    state, ctx = current_issue
    ctx.update(changes)
    assert not cloud_run_ok.can_deliver_today(state, ctx)


@pytest.mark.parametrize("state", [
    {}, {"last_run": {}}, {"last_run": []},
    {"last_run": {"at": "2026-09-13T06:20:00"}},
])
def test_context_without_todays_run_is_not_ready(current_issue, state):
    _, ctx = current_issue
    assert not cloud_run_ok.can_deliver_today(state, ctx)


def test_missing_context_is_not_ready(current_issue):
    state, _ = current_issue
    assert not cloud_run_ok.can_deliver_today(state, {})


def test_existing_matching_delivery_id_is_ready(current_issue):
    state, ctx = current_issue
    ctx["delivery_id"] = "hmao:" + ctx["issue_key"]
    assert cloud_run_ok.can_deliver_today(state, ctx)


def test_can_deliver_cli_and_has_pending_keep_distinct_meanings(
    current_issue, monkeypatch,
):
    state, ctx = current_issue
    monkeypatch.setattr(cloud_run_ok, "_context", lambda: ctx)
    monkeypatch.setattr(cloud_run_ok, "_health_state", lambda: state)
    assert cloud_run_ok.main(["--can-deliver"]) == 0
    assert cloud_run_ok.main(["--has-pending"]) == 1
    ctx["delivered_at"] = "2026-09-14T08:45:00"
    assert cloud_run_ok.main(["--can-deliver"]) == 1


@pytest.mark.parametrize("local_date, expected", [
    (dt.date(2026, 9, 14), 0),
    (dt.date(2026, 9, 13), 1),
    (dt.date(2026, 11, 4), 1),
])
def test_working_day_cli_uses_local_date_and_shared_calendar(
    monkeypatch, local_date, expected,
):
    class LocalDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.combine(local_date, dt.time(8, 45))

    monkeypatch.setattr(cloud_run_ok.dt, "datetime", LocalDateTime)
    monkeypatch.setattr(cloud_run_ok, "_health_state", lambda: pytest.fail("Не нужен журнал"))
    monkeypatch.setattr(cloud_run_ok, "_context", lambda: pytest.fail("Не нужен контекст"))
    assert cloud_run_ok.main(["--is-working-day"]) == expected


def test_delivery_skips_a_clone_locked_by_another_live_process(tmp_path):
    """Настоящая shell-ветка выходит до recovery, git и сетевых действий."""
    clone = tmp_path / "clone"
    log_dir = clone / "ops" / "mac-local-run"
    log_dir.mkdir(parents=True)
    lock = log_dir / ".run.lock"
    shutil.copy2(RESERVE / "run_lock.py", log_dir / "run_lock.py")
    # В sandbox macOS системный ps недоступен. Подменяем только OS start-time;
    # настоящий run_lock.py и shell-ветка захвата блокировки остаются в тесте.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ps = bin_dir / "ps"
    ps.write_text("#!/bin/sh\nprintf '%s\\n' 'Mon Sep 14 08:45:00 2026'\n", encoding="utf-8")
    ps.chmod(0o755)
    env = {
        **os.environ,
        "PATH": str(bin_dir) + os.pathsep + os.environ.get("PATH", ""),
        "CM_DELIVERY_WINDOW_MIN": "0",
    }
    lock_command = [sys.executable, str(RESERVE / "run_lock.py")]
    subprocess.run(lock_command + ["acquire", str(lock), str(os.getpid())], env=env, check=True)
    owner_before = json.loads((lock / "owner.json").read_text(encoding="utf-8"))
    try:
        result = subprocess.run(
            ["bash", str(RESERVE / "parse_and_push.sh"), str(clone), "--deliver-pending"],
            env=env,
            capture_output=True, text=True, timeout=10, check=False,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        log = (log_dir / "parse_and_push.log").read_text(encoding="utf-8")
        assert "Другой живой прогон уже идёт" in log
        assert "Старт parse_and_push" not in log
        assert json.loads((lock / "owner.json").read_text(encoding="utf-8")) == owner_before
        assert not (log_dir / ".runtime").exists()
    finally:
        subprocess.run(lock_command + ["release", str(lock), str(os.getpid())], env=env, check=True)
