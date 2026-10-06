"""Реальный Bash-драйвер с безопасными детьми и виртуальными задержками."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


DRIVER = Path(__file__).resolve().parents[2] / "ops/mac-local-run/parse_all.sh"
REGIONS = ("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen")


def _executable(path: Path, body: str) -> Path:
    path.write_text("#!/bin/bash\nset -eu\n" + body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _run_driver(tmp_path: Path, overrides: str | None, stagger: str | None = None):
    repos = []
    for region in REGIONS:
        repo = tmp_path / region
        (repo / ".git").mkdir(parents=True)
        (repo / "REGION").write_text(region + "\n", encoding="utf-8")
        repos.append(repo)
    territories = tmp_path / "territories"
    territories.write_text("".join(f"{repo}\n" for repo in repos), encoding="utf-8")
    trace = tmp_path / "trace"
    trace.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()

    # sleep — отдельный процесс ребёнка драйвера; после exec Bash-воркер
    # сохраняет PID этого ребёнка. Так проверяем именно переданную задержку,
    # а не только описание плана в stdout. Реального ожидания здесь нет.
    _executable(
        fake_bin / "sleep",
        'printf "%s\\n" "$1" >> "$CM_TEST_TRACE/$PPID.delay"\n',
    )
    worker = _executable(
        tmp_path / "worker.sh",
        'region=$(cat "$1/REGION")\n'
        'delay=0\n'
        'if [ -f "$CM_TEST_TRACE/$$.delay" ]; then\n'
        '  delay=$(cat "$CM_TEST_TRACE/$$.delay")\n'
        'fi\n'
        'printf "%s\\n" "$delay" >> "$CM_TEST_TRACE/$region.started"\n',
    )
    python = _executable(tmp_path / "python.sh", 'cat REGION\n')
    importer = _executable(tmp_path / "importer.sh", "exit 97\n")

    # Не наследуем локальные настройки запуска: тест использует только
    # временные клоны, не готовит маршруты и не запускает импорт/доставку.
    env = {key: value for key, value in os.environ.items() if not key.startswith("CM_")}
    env.update({
        "PATH": f"{fake_bin}:{env.get('PATH', '')}",
        "CM_TERRITORIES_FILE": str(territories),
        "CM_WORKER": str(worker),
        "CM_IMPORTER": str(importer),
        "CM_PYTHON": str(python),
        "CM_TEST_TRACE": str(trace),
        "CM_PARALLEL_TERRITORIES": "1",
        "CM_PARALLEL_FIRST_REGION": "sverdlovsk_yanao",
        "CM_COURT_ROUTES_READY": "1",
        "CM_DELIVERY_WINDOW_MIN": "1440",
        "CM_IMPORTS_AFTER_PARSE": "0",
    })
    if overrides is not None:
        env["CM_PARALLEL_START_DELAYS"] = overrides
    if stagger is not None:
        env["CM_PARALLEL_STAGGER_SECONDS"] = stagger
    result = subprocess.run(
        ["bash", str(DRIVER)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    starts = {
        path.stem: path.read_text(encoding="utf-8").splitlines()
        for path in trace.glob("*.started")
    }
    return result, starts, list(trace.glob("*.delay"))


@pytest.mark.parametrize(
    ("overrides", "stagger", "expected"),
    [
        pytest.param(
            "sverdlovsk_yanao=0 hmao=300 bashkortostan=600 tyumen=600",
            "300",
            {"sverdlovsk_yanao": 0, "hmao": 300, "bashkortostan": 600, "tyumen": 600},
            id="bashkortostan-and-tyumen-start-together",
        ),
        pytest.param(
            None,
            None,
            {"sverdlovsk_yanao": 0, "hmao": 600, "bashkortostan": 1200, "tyumen": 1800},
            id="unchanged-default-stagger",
        ),
        pytest.param(
            "tyumen=75",
            "300",
            {"sverdlovsk_yanao": 0, "hmao": 300, "bashkortostan": 600, "tyumen": 75},
            id="partial-override-preserves-other-regions",
        ),
    ],
)
def test_regional_delays_reach_all_workers(tmp_path, overrides, stagger, expected):
    result, starts, sleeps = _run_driver(tmp_path, overrides, stagger)
    assert result.returncode == 0, result.stdout + result.stderr
    # Ровно один фактический запуск каждого региона, независимо от порядка
    # выполнения быстрых подменённых детей.
    assert starts == {region: [str(delay)] for region, delay in expected.items()}
    assert len(sleeps) == sum(delay != 0 for delay in expected.values())


@pytest.mark.parametrize(
    "overrides",
    [
        "tyumen",
        "=600",
        "tyumen=",
        "tyumen=-1",
        "tyumen=1.5",
        "Tyumen=600",
        "tyumen-west=600",
        "9tyumen=600",
        "tyumen=600=1",
        "hmao=0 tyumen=invalid",
        "tyumen=600 tyumen=600",
        "tyumen=600 hmao=300 tyumen=900",
    ],
)
def test_invalid_delays_fail_before_starting_any_worker(tmp_path, overrides):
    result, starts, sleeps = _run_driver(tmp_path, overrides, "300")
    assert result.returncode == 2, result.stdout + result.stderr
    assert starts == {}, "Некорректная настройка не должна запускать ни один регион"
    assert sleeps == [], "Проверка настройки должна предшествовать запуску детей"
