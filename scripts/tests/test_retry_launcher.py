"""Дневная дочитка публикует данные, но не начинает поиск или доставку.

Shell запускается целиком в временном клоне. HTTP/Git/Python-парсер заменены
локальными транспортами; snapshot/ACK и lock работают настоящими helpers,
а сведения о процессе для lock возвращает локальная заглушка ps.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
MAC = ROOT / "ops/mac-local-run"
VPS = ROOT / "ops/vps-run"


def executable(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture
def worker_env(tmp_path):
    repo = tmp_path / "repo"
    helpers = repo / "ops/mac-local-run"
    helpers.mkdir(parents=True)
    (repo / "data").mkdir()
    (repo / "scripts").mkdir()
    (repo / "scripts/technical_report.py").write_text('# test stub intercepted by fake_python\n')
    data = repo / "data/cases.json"
    data.write_text('{"cases": [], "pending_retry_context": []}\n')
    context = repo / "data/last_digest_context.json"
    context.write_text('{"issue_key":"morning","delivered_at":"2026-10-07T08:45:00+05:00"}\n')
    for name in ("parse_txn.py", "run_lock.py"):
        shutil.copyfile(MAC / name, helpers / name)
    # Меняем только внешние исполняемые файлы, сохраняя shell-ветвления.
    worker = helpers / "parse_and_push.sh"
    worker.write_text((MAC / worker.name).read_text().replace(
        'PYTHON="/usr/bin/python3"', 'PYTHON="$CM_TEST_PYTHON"').replace(
        '/usr/bin/osascript', '"$CM_TEST_OSASCRIPT"'))
    (helpers / "lib_sber_net.sh").write_text(r'''
CM_SBER_GATEWAY=test
cm_in_sber_network() { return 1; }
cm_clear_court_routes() { :; }
cm_git_ssh_url() { echo test-remote; }
cm_git_ssh_command() { echo test-ssh; }
cm_delivery_window_open() { return 0; }
cm_any_court_reachable() { echo probe >> "$CM_TEST_ACTIONS"; return 0; }
cm_region_code() { echo hmao; }
cm_load_territory_env() { export RUN_DEADLINE_SECONDS=9999; }
cm_alert_telegram() { echo telegram >> "$CM_TEST_ACTIONS"; }
''')
    (repo / "ops/stage_data_files.sh").write_text(r'''
if [ "$1" = --list ]; then
  printf '%s\n' data/cases.json data/last_digest_context.json
fi
''')
    fake_python = executable(tmp_path / "python", f"#!{sys.executable}\n" + r'''
import json, os, pathlib, sys
name = pathlib.Path(sys.argv[1]).name
with open(os.environ["CM_TEST_TRACE"], "a") as f:
    f.write(json.dumps({"tool": name, "args": sys.argv[2:]}) + "\n")
if name in {"parse_txn.py", "run_lock.py"}:
    os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
if name == "progress_pusher.py":
    sys.exit(1)
if name == "delivery_txn.py":
    sys.exit(1)
if name == "technical_report.py":
    sys.exit(int(os.environ.get("CM_TEST_REPORT_FAIL", "0")))
if name == "cloud_run_ok.py":
    print("дайджест отправлен" if "--report" in sys.argv else "прочитано")
    sys.exit(0)
if name == "run_parse.py":
    fields = ("CM_RETRY_ONLY", "SKIP_CHECKED_TODAY", "SKIP_NON_WORKING_DAYS",
              "RUN_DEADLINE_SECONDS", "DIGEST_CONTEXT_REQUIRED")
    pathlib.Path(os.environ["CM_TEST_PARSE_ENV"]).write_text(json.dumps(
        {key: os.environ.get(key) for key in fields}))
    pathlib.Path("data/cases.json").write_text('{"pending_retry_context":["new event"]}')
    if os.environ.get("CM_TEST_PARSE_FAIL") == "1":
        sys.exit(7)
    pathlib.Path(os.environ["PARSE_TXN_ACK_FILE"]).write_text(json.dumps(
        {"txn_id": os.environ["PARSE_TXN_ID"]}))
    sys.exit(0)
sys.exit("unexpected Python tool: " + name)
''')
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable(bin_dir / "ps", "#!/bin/sh\necho test-process-start\n")
    executable(bin_dir / "osascript", "#!/bin/sh\nexit 0\n")
    executable(bin_dir / "git", f"#!{sys.executable}\n" + r'''
import json, os, sys
with open(os.environ["CM_TEST_TRACE"], "a") as f:
    f.write(json.dumps({"tool": "git", "args": sys.argv[1:]}) + "\n")
if sys.argv[1:2] == ["diff"]:
    sys.exit(int(os.environ.get("CM_TEST_DATA_DIFF", "1")))
if sys.argv[1:2] == ["merge-base"]:
    sys.exit(int(os.environ.get("CM_TEST_REMOTE_CONTAINS_HEAD", "0")))
sys.exit(0)
''')
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "CM_TEST_PYTHON": str(fake_python), "CM_COURT_ROUTES_READY": "1",
           "CM_TEST_OSASCRIPT": str(bin_dir / "osascript"),
           "CM_TEST_TRACE": str(tmp_path / "trace"),
           "CM_TEST_ACTIONS": str(tmp_path / "actions"),
           "CM_TEST_PARSE_ENV": str(tmp_path / "parse_env")}
    return repo, worker, env


def run_worker(setup, *args):
    repo, worker, env = setup
    result = subprocess.run(["bash", str(worker), str(repo), "--anywhere", *args],
                            env=env, capture_output=True, text=True, timeout=15)
    trace = Path(env["CM_TEST_TRACE"])
    calls = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
    return result, calls


def test_retry_publishes_with_daily_skip_and_hard_budget_without_delivery(worker_env):
    repo, _, env = worker_env
    before = (repo / "data/last_digest_context.json").read_bytes()
    result, calls = run_worker(worker_env, "--retry-only")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(Path(env["CM_TEST_PARSE_ENV"]).read_text()) == {
        "CM_RETRY_ONLY": "1", "SKIP_CHECKED_TODAY": "1",
        "SKIP_NON_WORKING_DAYS": "1", "RUN_DEADLINE_SECONDS": "600",
        "DIGEST_CONTEXT_REQUIRED": "1",
    }
    assert (repo / "data/last_digest_context.json").read_bytes() == before
    assert json.loads((repo / "data/cases.json").read_text())["pending_retry_context"] == ["new event"]
    assert not Path(env["CM_TEST_ACTIONS"]).exists(), "дочитка сделала пробу/отправку"
    assert not any(call["tool"] == "delivery_txn.py" for call in calls)
    assert not any("--report" in call["args"] or "--mark-delivered" in call["args"]
                   or "--health-alerts" in call["args"] for call in calls)
    git_calls = [call["args"] for call in calls if call["tool"] == "git"]
    commits = [args for args in git_calls if "commit" in args]
    assert len(commits) == 1 and "--only" in commits[0]
    assert "Mac-парсинг" not in commits[0][commits[0].index("-m") + 1]
    assert sum(args[:1] == ["push"] for args in git_calls) == 1
    assert not (repo / "ops/mac-local-run/.runtime/parse_txn.json").exists()
    report_calls = [call for call in calls if call['tool'] == 'technical_report.py']
    assert len(report_calls) == 1
    assert report_calls[0]['args'][:3] == ['ops', '--event', 'retry-result']
    assert calls.index(report_calls[0]) > max(i for i, call in enumerate(calls)
        if call['tool'] == 'git' and call['args'][:1] == ['push'])


def test_retry_waits_for_delivery_recovery_without_touching_markers(worker_env):
    repo, _, env = worker_env
    runtime = repo / "ops/mac-local-run/.runtime"
    runtime.mkdir()
    journal = runtime / "delivery_txn.json"
    journal.write_text('{"status":"committed","delivery_id":"pending"}')
    context = repo / "data/last_digest_context.json"
    before = context.read_bytes(), journal.read_bytes()
    result, calls = run_worker(worker_env, "--retry-only")
    assert result.returncode == 0
    assert (context.read_bytes(), journal.read_bytes()) == before
    assert all(call["tool"] == "run_lock.py" for call in calls)
    assert not Path(env["CM_TEST_PARSE_ENV"]).exists()


def test_failed_retry_rolls_back_data_without_publishing_and_reports_real_failure(worker_env):
    repo, _, env = worker_env
    data = repo / "data/cases.json"
    before = data.read_bytes()
    env["CM_TEST_PARSE_FAIL"] = "1"
    result, calls = run_worker(worker_env, "--retry-only")
    assert result.returncode == 1
    assert data.read_bytes() == before
    assert not Path(env["CM_TEST_ACTIONS"]).exists()
    assert not any(call["tool"] == "git" and "push" in call["args"] for call in calls)
    reports = [call for call in calls if call['tool'] == 'technical_report.py']
    assert len(reports) == 1
    assert reports[0]['args'][:3] == ['ops', '--event', 'failure']
    assert 'парсинг завершился с кодом 7' in reports[0]['args'][-1]


def test_retry_reporting_failure_does_not_change_published_result(worker_env):
    repo, _, env = worker_env
    env['CM_TEST_REPORT_FAIL'] = '2'
    result, calls = run_worker(worker_env, '--retry-only')
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (repo / 'ops/mac-local-run/.runtime/parse_txn.json').exists()
    assert any(call['tool'] == 'git' and call['args'][:1] == ['push'] for call in calls)


@pytest.mark.parametrize('remote_rc,report_count', [('0', 1), ('1', 0)])
def test_empty_diff_only_reports_publication_if_remote_contains_current_commit(worker_env, remote_rc, report_count):
    _, _, env = worker_env
    env.update(CM_TEST_DATA_DIFF='0', CM_TEST_REMOTE_CONTAINS_HEAD=remote_rc)
    result, calls = run_worker(worker_env, '--retry-only')
    assert result.returncode == 0, result.stdout + result.stderr
    assert not any(call['tool'] == 'git' and call['args'][:1] == ['push'] for call in calls)
    assert len([call for call in calls if call['tool'] == 'technical_report.py']) == report_count


def test_ordinary_slot_keeps_delivered_day_gate(worker_env):
    _, _, env = worker_env
    result, calls = run_worker(worker_env)
    assert result.returncode == 0
    assert any("--report" in call["args"] for call in calls)
    assert not Path(env["CM_TEST_PARSE_ENV"]).exists()


@pytest.mark.parametrize("conflict", ["--force", "--deliver-pending", "--check", "--ignore-calendar"])
@pytest.mark.parametrize("script", ["parse_all.sh", "parse_and_push.sh"])
def test_retry_rejects_conflicting_modes_before_work(conflict, script):
    result = subprocess.run(["bash", str(MAC / script), "--retry-only", conflict],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 2
    assert "--retry-only" in result.stderr


@pytest.mark.parametrize("parallel", ["0", "1"])
def test_driver_retry_never_runs_delivery_sweep_or_imports(tmp_path, parallel):
    repos = []
    for region in ("hmao", "sverdlovsk_yanao"):
        repo = tmp_path / region
        (repo / ".git").mkdir(parents=True)
        module = repo / "scripts/court_monitor"
        module.mkdir(parents=True)
        (module / "__init__.py").write_text("")
        (module / "config.py").write_text(f"REGION = {region!r}\n")
        repos.append(repo)
    territories = tmp_path / "territories"
    territories.write_text("\n".join(map(str, repos)) + "\n")
    trace = tmp_path / "trace"
    worker = executable(tmp_path / "worker.sh", '#!/bin/bash\necho "$*" >> "$CM_TEST_TRACE"\n')
    importer = executable(tmp_path / "importer.sh", '#!/bin/bash\necho import >> "$CM_TEST_TRACE"\n')
    env = {**os.environ, "CM_TERRITORIES_FILE": str(territories), "CM_WORKER": str(worker),
           "CM_IMPORTER": str(importer), "CM_PYTHON": sys.executable,
           "CM_TEST_TRACE": str(trace), "CM_DELIVERY_WINDOW_MIN": "0",
           "CM_IMPORTS_AFTER_PARSE": "1", "CM_PARALLEL_TERRITORIES": parallel,
           "CM_PARALLEL_STAGGER_SECONDS": "0", "CM_PARALLEL_START_DELAYS": "",
           "CM_COURT_ROUTES_READY": "1"}
    result = subprocess.run(["bash", str(MAC / "parse_all.sh"), "--retry-only"],
                            env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert set(trace.read_text().splitlines()) == {f"{repo} --retry-only" for repo in repos}


def test_retry_slots_are_separate_explicitly_local_and_do_not_catch_up():
    timer = (VPS / "systemd/court-retry.timer").read_text()
    assert [line for line in timer.splitlines() if line.startswith("OnCalendar=")] == [
        "OnCalendar=Mon..Fri 12:00 Asia/Yekaterinburg",
        "OnCalendar=Mon..Fri 16:00 Asia/Yekaterinburg",
    ]
    assert "Persistent=false" in timer
    service = (VPS / "systemd/court-retry.service").read_text()
    assert "ops/vps-run/parse_all.sh --retry-only" in service
    assert "OnSuccess=" not in service and "OnFailure=" not in service
    assert "--force" not in service
