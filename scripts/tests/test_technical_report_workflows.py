"""Проверка итоговых статусов Actions без Git, сети и Telegram."""

from __future__ import annotations

import os
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ("replay_on_push.yml", "update_cases.yml", "test_digest.yml")


def _workflow(name):
    return (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")


def _step(workflow, name):
    match = re.search(
        rf"^      - name: {re.escape(name)}\n(?P<body>.*?)(?=^      - name: |\Z)",
        _workflow(workflow), re.MULTILINE | re.DOTALL,
    )
    assert match, (workflow, name)
    return match.group("body")


def _script(step):
    match = re.search(r"^        run: (\||>-)\n((?:          [^\n]*\n|\n)*)", step, re.MULTILINE)
    assert match, step
    script = textwrap.dedent(match.group(2))
    return " ".join(script.splitlines()) if match.group(1) == ">-" else script


def _stub(tmp_path, name, body):
    directory = tmp_path / "bin"
    directory.mkdir(exist_ok=True)
    path = directory / name
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(0o755)


def _execute(tmp_path, script, **settings):
    output = tmp_path / "github_output"
    env = {
        **os.environ,
        "PATH": str(tmp_path / "bin") + os.pathsep + os.environ["PATH"],
        "GITHUB_OUTPUT": str(output),
        **settings,
    }
    result = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", script], cwd=tmp_path,
        env=env, capture_output=True, text=True, timeout=15,
    )
    outputs = {}
    if output.exists():
        outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    return result, outputs


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_report_always_targets_personal_chat_after_publication(workflow):
    source = _workflow(workflow)
    step = _step(workflow, "Технический отчёт в личный Telegram")
    assert "if: always()" in step
    assert "TELEGRAM_CHAT_ID_PERSONAL: ${{ secrets.TELEGRAM_CHAT_ID_TEST }}" in step
    assert "inputs.to_group" not in step
    assert 'DEFER_TECHNICAL_REPORT: "1"' in source
    assert "--digest-step" in step
    assert "--data-publication" in step
    assert "--pages" in step
    assert "--push-step" in step
    assert source.index("id: commit") < source.index("Технический отчёт в личный Telegram")
    assert source.count("api.telegram.org") == 1
    assert 'if [ -f scripts/technical_report.py ]; then' in step
    assert step.index('else\n') < step.index('api.telegram.org'), "Запасной алерт разрешён только без CLI"


@pytest.mark.parametrize("workflow", ("update_cases.yml", "test_digest.yml"))
def test_group_retains_digest_and_personal_receives_only_technical(workflow):
    source = _workflow(workflow)
    assert "TELEGRAM_DIGEST_MODE: ${{ inputs.to_group && 'digest' || 'technical' }}" in source
    assert "TELEGRAM_CHAT_ID: ${{ inputs.to_group && secrets.TELEGRAM_CHAT_ID || secrets.TELEGRAM_CHAT_ID_TEST }}" in source


def test_replay_reports_after_push_and_journal_without_changing_defer_pair():
    source = _workflow("replay_on_push.yml")
    assert 'DEFER_WEB_PUSH: "1"' in source
    assert source.index("--replay-last --push-all") < source.index("Commit rendered digest")
    assert source.index("Дождаться публикации дайджеста") < source.index("--push-web-only --push-all")
    assert source.index("--push-web-only --push-all") < source.index("Commit журнала push-рассылки")
    assert source.index("Commit журнала push-рассылки") < source.index("Технический отчёт в личный Telegram")
    assert "PUSH_STATUS: ${{ steps.push.outcome || 'skipped' }}" in source


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_report_initialization_clears_only_this_run_files(tmp_path, workflow):
    (tmp_path / "court-technical-report.json").write_text("stale")
    (tmp_path / "court-technical-report.html").write_text("stale")
    unrelated = tmp_path / "other.json"
    unrelated.write_text("keep")
    github_env = tmp_path / "github_env"
    result, _ = _execute(
        tmp_path, _script(_step(workflow, "Начать технический отчёт")),
        RUNNER_TEMP=str(tmp_path), GITHUB_ENV=str(github_env),
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "court-technical-report.json").exists()
    assert not (tmp_path / "court-technical-report.html").exists()
    assert unrelated.read_text() == "keep"
    assert github_env.read_text().strip() == f"TECHNICAL_REPORT_PATH={tmp_path}/court-technical-report.json"


@pytest.mark.parametrize(
    "rebase_status,push_status,expected,returncode",
    [("0", "0", "confirmed", 0), ("1", "0", "not_confirmed", 0), ("0", "1", "failed", 1)],
)
@pytest.mark.parametrize(
    "workflow,name",
    [("replay_on_push.yml", "Commit rendered digest"), ("test_digest.yml", "Публикация результатов на дашборд")],
)
def test_commit_status_distinguishes_rebase_conflict_and_failed_push(
    tmp_path, workflow, name, rebase_status, push_status, expected, returncode,
):
    _stub(tmp_path, "git", """
case "$1" in
  diff) exit 1 ;;
  pull) exit "$STUB_REBASE_STATUS" ;;
  push) exit "$STUB_PUSH_STATUS" ;;
  *) exit 0 ;;
esac
""")
    result, outputs = _execute(
        tmp_path, _script(_step(workflow, name)),
        STUB_REBASE_STATUS=rebase_status, STUB_PUSH_STATUS=push_status,
    )
    assert result.returncode == returncode, result.stderr
    assert outputs["status"] == expected
    if rebase_status == "1" and workflow == "replay_on_push.yml":
        assert outputs["pushed"] == "0"


@pytest.mark.parametrize("served_fresh,expected", [(True, "confirmed"), (False, "unconfirmed")])
@pytest.mark.parametrize(
    "workflow,name",
    [("replay_on_push.yml", "Дождаться публикации дайджеста на Pages"),
     ("test_digest.yml", "Проверка публикации дайджеста на Pages")],
)
def test_pages_timeout_is_unconfirmed_and_keeps_existing_warning_policy(
    tmp_path, workflow, name, served_fresh, expected,
):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "last_digest.json").write_text('{"version":"fresh"}')
    _stub(tmp_path, "curl", 'if [ "$STUB_FRESH" = "1" ]; then cat data/last_digest.json; else echo old; fi\n')
    _stub(tmp_path, "sleep", "exit 0\n")
    result, outputs = _execute(
        tmp_path, _script(_step(workflow, name)),
        STUB_FRESH="1" if served_fresh else "0", PAGES_URL="https://example.invalid/digest",
    )
    assert result.returncode == 0, result.stderr
    assert outputs["status"] == expected
    if not served_fresh:
        assert "::warning::" in result.stdout


@pytest.mark.parametrize("deployment_state,expected", [("failure", "failed"), ("success", "unconfirmed")])
def test_update_pages_does_not_treat_deployment_success_as_fresh_bytes(tmp_path, deployment_state, expected):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "last_digest.json").write_text('{"version":"fresh"}')
    _stub(tmp_path, "curl", "echo old\n")
    _stub(tmp_path, "sleep", "exit 0\n")
    _stub(tmp_path, "gh", """
case "$2" in
  */statuses*) echo "$STUB_DEPLOYMENT_STATE" ;;
  *) echo 123 ;;
esac
""")
    script = _script(_step("update_cases.yml", "Проверка публикации GitHub Pages"))
    script = script.replace("${{ steps.commit.outputs.sha }}", "test-sha")
    result, outputs = _execute(
        tmp_path, script, STUB_DEPLOYMENT_STATE=deployment_state,
        PAGES_URL="https://example.invalid/digest", GITHUB_REPOSITORY="test/repo",
    )
    assert result.returncode == 0, result.stderr
    assert outputs["status"] == expected


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize("accepted", [True, False])
def test_checkout_failure_notifies_without_project_or_dependencies(tmp_path, workflow, accepted):
    """Исполняем настоящий inline Python с заглушенным единственным HTTP-запросом."""
    driver = tmp_path / "python_stub.py"
    driver.write_text(textwrap.dedent('''
        import io, json, os, sys
        from pathlib import Path
        import urllib.request
        assert sys.argv[1:] == ['-'], sys.argv
        def fake_open(request, timeout):
            assert timeout == 30
            Path(os.environ['REQUEST_CAPTURE']).write_text(request.data.decode())
            return io.StringIO(json.dumps({'ok': os.environ['STUB_ACCEPTED'] == '1'}))
        urllib.request.urlopen = fake_open
        exec(compile(sys.stdin.read(), '<workflow fallback>', 'exec'))
    '''))
    _stub(tmp_path, "python", f'exec {shlex.quote(sys.executable)} {shlex.quote(str(driver))} "$@"\n')
    capture = tmp_path / "request.json"
    result, _ = _execute(
        tmp_path, _script(_step(workflow, "Технический отчёт в личный Telegram")),
        REQUEST_CAPTURE=str(capture), STUB_ACCEPTED="1" if accepted else "0",
        TELEGRAM_BOT_TOKEN="test-token", TELEGRAM_CHAT_ID_PERSONAL="personal-test-chat",
        TELEGRAM_CHAT_ID="other-chat", GITHUB_REPOSITORY="test/repo", GITHUB_RUN_ID="123",
    )
    assert result.returncode == (0 if accepted else 1), result.stderr
    payload = json.loads(capture.read_text())
    assert payload['chat_id'] == 'personal-test-chat'
    assert 'не подтверждён' in payload['text']
    assert 'https://github.com/test/repo/actions/runs/123' in payload['text']
    assert 'test-token' not in result.stdout + result.stderr


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_existing_cli_failure_does_not_trigger_second_notification(tmp_path, workflow):
    (tmp_path / 'scripts').mkdir()
    (tmp_path / 'scripts' / 'technical_report.py').write_text('# CLI present\n')
    _stub(tmp_path, 'python', 'echo "$*" >> "$CLI_CAPTURE"\nexit 1\n')
    capture = tmp_path / 'cli_calls'
    result, _ = _execute(
        tmp_path, _script(_step(workflow, "Технический отчёт в личный Telegram")),
        CLI_CAPTURE=str(capture), WORKFLOW_STATUS='failure', DIGEST_STATUS='failure',
        PUBLICATION_STATUS='skipped', PAGES_STATUS='skipped', PUSH_STATUS='skipped',
    )
    assert result.returncode == 1
    calls = capture.read_text().splitlines()
    assert len(calls) == 1
    assert calls[0].startswith('scripts/technical_report.py --send')
    assert '--workflow-status failure' in calls[0]
