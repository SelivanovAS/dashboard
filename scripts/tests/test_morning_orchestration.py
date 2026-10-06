"""Доставка и импорты не удерживают общий утренний обход трёх регионов."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
MAC = ROOT / "ops" / "mac-local-run"
VPS = ROOT / "ops" / "vps-run"


def executable(path, text):
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture
def setup(tmp_path):
    repos = []
    for name in ("hmao", "sverdlovsk_yanao", "bashkortostan"):
        repo = tmp_path / name
        (repo / ".git").mkdir(parents=True)
        (repo / "REGION").write_text(name)
        helpers = repo / "ops" / "mac-local-run"
        helpers.mkdir(parents=True)
        (helpers / "cloud_run_ok.py").write_text(
            "import os, sys\n"
            "key = 'CM_TEST_CALENDAR' if '--is-working-day' in sys.argv else 'CM_TEST_READY'\n"
            "if '--report' in sys.argv: sys.exit(int(os.environ.get('CM_TEST_DELIVERED', '1')))\n"
            "sys.exit(int(os.environ.get(key, '0')))\n"
        )
        repos.append(repo)
    territories = tmp_path / "territories"
    territories.write_text("\n".join(map(str, repos)) + "\n")
    trace = tmp_path / "trace"
    worker = executable(tmp_path / "worker.sh",
        '#!/bin/bash\nprintf "%s:%s\\n" "$(cat "$1/REGION")" "$2" >> "$CM_TEST_TRACE"\n')
    env = {**os.environ, "CM_TERRITORIES_FILE": str(territories),
           "CM_WORKER": str(worker), "CM_PYTHON": sys.executable,
           "CM_TEST_TRACE": str(trace), "CM_DELIVERY_WINDOW_MIN": "0"}
    return tmp_path, repos, trace, worker, env


def run_delivery(env, *args):
    return subprocess.run(["bash", str(MAC / "delivery_all.sh"), *args],
                          env=env, text=True, capture_output=True, timeout=15)


def test_delivery_visits_all_regions_in_delivery_only_mode(setup):
    _, _, trace, _, env = setup
    result = run_delivery(env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert set(trace.read_text().splitlines()) == {
        f"{region}:--deliver-pending"
        for region in ("hmao", "sverdlovsk_yanao", "bashkortostan")}


def test_slow_or_failed_region_does_not_hold_ready_neighbor(setup):
    tmp, _, trace, worker, env = setup
    # Первый ребёнок ждёт результата третьего. При последовательном обходе
    # тест завершится по ошибке: готовая Башкирия должна запуститься сама.
    executable(worker, '#!/bin/bash\n'
        'region=$(cat "$1/REGION")\n'
        'if [ "$region" = hmao ]; then\n'
        '  for n in {1..50}; do\n'
        '    if [ -f "$CM_TEST_READY_FILE" ]; then exit 7; fi\n'
        '    sleep 0.02\n'
        '  done\n'
        '  echo blocked >> "$CM_TEST_TRACE"; exit 8\n'
        'fi\n'
        'echo "$region" >> "$CM_TEST_TRACE"\n'
        'if [ "$region" = bashkortostan ]; then touch "$CM_TEST_READY_FILE"; fi\n')
    env["CM_TEST_READY_FILE"] = str(tmp / "ready")
    result = run_delivery(env)
    assert result.returncode == 1
    assert set(trace.read_text().splitlines()) == {"sverdlovsk_yanao", "bashkortostan"}


@pytest.mark.parametrize("args,overrides", [
    (("--check",), {}),
    ((), {"CM_TEST_CALENDAR": "1"}),
    ((), {"CM_DELIVERY_WINDOW_MIN": "1440"}),
])
def test_check_closed_window_and_nonworking_day_never_send(setup, args, overrides):
    _, _, trace, _, env = setup
    result = run_delivery({**env, **overrides}, *args)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not trace.exists()


def test_calendar_failure_is_not_misreported_as_a_day_off(setup):
    _, _, trace, _, env = setup
    result = run_delivery({**env, "CM_TEST_CALENDAR": "2"})
    assert result.returncode == 1
    assert not trace.exists()


def test_automatic_driver_rejects_force(setup):
    _, _, trace, _, env = setup
    result = run_delivery(env, "--force")
    assert result.returncode == 2
    assert not trace.exists()


def test_confirmed_delivered_day_does_not_start_git_or_delivery(setup):
    _, _, trace, _, env = setup
    result = run_delivery({**env, "CM_TEST_DELIVERED": "0"})
    assert result.returncode == 0
    assert not trace.exists()


@pytest.mark.parametrize("journal", ["delivery_txn.json", "parse_txn.json"])
def test_local_stamp_never_skips_unfinished_transaction_recovery(setup, journal):
    _, repos, trace, _, env = setup
    runtime = repos[0] / "ops" / "mac-local-run" / ".runtime"
    runtime.mkdir()
    (runtime / journal).write_text('{}')
    result = run_delivery({**env, "CM_TEST_DELIVERED": "0"})
    assert result.returncode == 0
    assert trace.read_text().splitlines() == ["hmao:--deliver-pending"]


def test_vps_parser_does_not_start_or_wait_for_inline_imports(setup):
    tmp, _, trace, worker, env = setup
    executable(worker, '#!/bin/bash\n'
        'echo "$(cat "$1/REGION")" >> "$CM_TEST_TRACE"\n'
        'if [ "$(cat "$1/REGION")" = hmao ]; then exit 1; fi\n')
    importer = executable(tmp / "importer.sh", '#!/bin/bash\n'
        'echo import >> "$CM_TEST_TRACE"\nexit 1\n')
    env.update({"CM_IMPORTER": str(importer), "CM_IMPORTS_AFTER_PARSE": "0",
                "CM_PARALLEL_TERRITORIES": "0", "CM_DELIVERY_WINDOW_MIN": "1440"})
    result = subprocess.run(["bash", str(MAC / "parse_all.sh")], env=env,
                            text=True, capture_output=True, timeout=15)
    assert result.returncode == 1  # Отказ ХМАО сохраняется, остальные работают.
    assert set(trace.read_text().splitlines()) == {"hmao", "sverdlovsk_yanao", "bashkortostan"}


def test_vps_hands_imports_to_its_own_service_on_success_and_failure():
    service = (VPS / "systemd" / "court-parse.service").read_text()
    assert "OnSuccess=court-import.service" in service
    assert "OnFailure=court-import.service" in service
    assert "ExecStartPost=" not in service  # Не ждём импорт внутри oneshot.
    assert "export CM_IMPORTS_AFTER_PARSE=0" in (VPS / "parse_all.sh").read_text()


def test_delivery_timer_is_separate_from_parser_and_cannot_catch_up_on_weekends():
    timer = (VPS / "systemd" / "court-delivery.timer").read_text()
    assert [line for line in timer.splitlines() if line.startswith("OnCalendar=")] == [
        "OnCalendar=Mon..Fri 08:45/5", "OnCalendar=Mon..Fri 09:00/5", "OnCalendar=Mon..Fri 10..23:00/15"]
    assert "Persistent=false" in timer
    service = (VPS / "systemd" / "court-delivery.service").read_text()
    assert "ops/vps-run/delivery_all.sh" in service
    assert "court-parse.service" not in service
    assert "TimeoutStartSec=4min" in service
    assert "TimeoutStopSec=30s" in service


def test_long_imports_wake_delivery_even_after_the_last_timer_tick():
    service = (VPS / "systemd" / "court-import.service").read_text()
    assert "OnSuccess=court-delivery.service" in service
    assert "OnFailure=court-delivery.service" in service
    poll_service = (VPS / "systemd" / "court-import-poll.service").read_text()
    assert "OnFailure=court-delivery.service" in poll_service
    poll = (VPS / "import_poll.sh").read_text()
    trigger = poll.index("systemctl --no-block start court-delivery.service")
    assert poll.index('if [ "$ran_import" = "1" ]; then') < trigger
    assert poll.index('bash "$IMPORTER"') < trigger
    assert "OnSuccess=" not in poll_service  # Пустой тик ничего не запускает.


@pytest.mark.parametrize("path", [MAC / "delivery_all.sh", VPS / "delivery_all.sh"])
def test_new_drivers_have_valid_shell_syntax(path):
    subprocess.run(["bash", "-n", str(path)], check=True, capture_output=True)
    assert path.stat().st_mode & 0o111
