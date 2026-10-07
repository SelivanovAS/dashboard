"""Диспетчер запускает установленную программу каждого клона, без сети/данных."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def launchers(tmp_path):
    supervisor = tmp_path / "supervisor"
    mac = supervisor / "ops/mac-local-run"
    vps = supervisor / "ops/vps-run"
    mac.mkdir(parents=True)
    vps.mkdir(parents=True)
    for relative in (
        "ops/mac-local-run/parse_all.sh", "ops/mac-local-run/import_all.sh",
        "ops/mac-local-run/delivery_all.sh", "ops/vps-run/import_poll.sh",
    ):
        shutil.copyfile(ROOT / relative, supervisor / relative)
    # Only dispatch is exercised: infrastructure, court/calendar checks, HTTP
    # and systemctl are replaced at the process boundary.
    (vps / "vps_env.sh").write_text("# isolated dispatch test\n")
    (mac / "lib_sber_net.sh").write_text('''
cm_territories() { cat "$CM_TEST_REPOS"; }
cm_region_code() { cat REGION; }
cm_delivery_window_open() { [ "${CM_TEST_WINDOW:-1}" = "1" ]; }
cm_worker_conf() { printf 'https://worker.invalid\\nowner-test\\npush-test\\n'; }
''')
    trace = tmp_path / "calls.jsonl"
    logger = tmp_path / "record.py"
    logger.write_text('''import json, os, sys
with open(os.environ["CM_TEST_TRACE"], "a") as stream:
    stream.write(json.dumps({"version": sys.argv[1], "kind": sys.argv[2],
                            "argv": sys.argv[3:],
                            "routes": os.environ.get("CM_COURT_ROUTES_READY")}) + "\\n")
''')

    def program(path, version, kind):
        path.write_text(
            '#!/bin/bash\nexec "$CM_TEST_PYTHON" "$CM_TEST_LOGGER" '
            f'{version} {kind} "$@"\n'
        )
        path.chmod(0o755)
        return path

    # An accidental fallback to the supervisor is observable and harmless.
    program(mac / "parse_and_push.sh", "supervisor", "parser")
    program(mac / "import_dumps.sh", "supervisor", "importer")
    clones = []
    for region, version in (("hmao", "v1"), ("sverdlovsk_yanao", "v2")):
        clone = tmp_path / f"clone {region}"
        (clone / ".git").mkdir(parents=True)
        local = clone / "ops/mac-local-run"
        local.mkdir(parents=True)
        (clone / "REGION").write_text(region)
        program(local / "parse_and_push.sh", version, "parser")
        program(local / "import_dumps.sh", version, "importer")
        (local / "cloud_run_ok.py").write_text(
            'import sys\nsys.exit(1 if "--report" in sys.argv else 0)\n'
        )
        clones.append(clone)
    repos = tmp_path / "territories"
    repos.write_text("".join(f"{clone}\n" for clone in clones))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "curl").write_text('''#!/bin/bash
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then shift; body="$1"; fi
  shift
done
printf '{"at":"test-pending-1"}\\n' > "$body"
printf 200
''')
    (bin_dir / "jq").write_text("#!/bin/bash\nprintf 'test-pending-1\\n'\n")
    program(bin_dir / "systemctl", "fake", "systemctl")
    for path in bin_dir.iterdir():
        path.chmod(0o755)
    env = {key: value for key, value in os.environ.items() if not key.startswith("CM_")}
    env.update({
        "PATH": f"{bin_dir}:{env.get('PATH', '')}",
        "CM_TEST_REPOS": str(repos), "CM_TEST_TRACE": str(trace),
        "CM_TEST_LOGGER": str(logger), "CM_TEST_PYTHON": sys.executable,
        "CM_PYTHON": sys.executable, "CM_COURT_ROUTES_READY": "1",
        "CM_PARALLEL_STAGGER_SECONDS": "0",
    })

    def calls():
        return [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []

    def run(name, *args, override=False, **settings):
        run_env = dict(env, **settings)
        if override:
            run_env["CM_WORKER"] = str(program(tmp_path / "override parser.sh", "override", "parser"))
            run_env["CM_IMPORTER"] = str(program(tmp_path / "override importer.sh", "override", "importer"))
        driver = vps / name if name == "import_poll.sh" else mac / name
        return subprocess.run(["bash", str(driver), *args], env=run_env, cwd=tmp_path,
                              capture_output=True, text=True, timeout=15)

    return SimpleNamespace(run=run, calls=calls, clones=clones)


def assert_regional_calls(launchers, calls, kind, args, override):
    selected = [call for call in calls if call["kind"] == kind and call["argv"][1:] == args]
    assert len(selected) == 2, calls
    by_repo = {call["argv"][0]: call for call in selected}
    for clone, version in zip(launchers.clones, ("v1", "v2")):
        assert by_repo[str(clone)]["version"] == ("override" if override else version)
        assert by_repo[str(clone)]["routes"] == "1"


@pytest.mark.parametrize("parallel", ["0", "1"])
@pytest.mark.parametrize("override", [False, True])
def test_parse_import_and_delivery_sweep_use_each_clones_program(launchers, parallel, override):
    args = ["--anywhere", "--ignore-calendar"]
    result = launchers.run("parse_all.sh", *args, override=override,
                           CM_PARALLEL_TERRITORIES=parallel)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = launchers.calls()
    assert len(calls) == 6
    assert_regional_calls(launchers, calls, "parser", args, override)
    assert_regional_calls(launchers, calls, "parser", ["--deliver-pending"], override)
    assert_regional_calls(launchers, calls, "importer", args, override)


def test_retry_selects_regional_parser_without_import_or_delivery(launchers):
    args = ["--anywhere", "--retry-only"]
    result = launchers.run("parse_all.sh", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = launchers.calls()
    assert len(calls) == 2
    assert_regional_calls(launchers, calls, "parser", args, False)


@pytest.mark.parametrize("override", [False, True])
def test_import_driver_selects_regional_importers_and_forwards_args(launchers, override):
    args = ["--anywhere", "--dry-run"]
    result = launchers.run("import_all.sh", *args, override=override)
    assert result.returncode == 0, result.stdout + result.stderr
    assert_regional_calls(launchers, launchers.calls(), "importer", args, override)


@pytest.mark.parametrize("override", [False, True])
def test_delivery_driver_selects_regional_parsers(launchers, override):
    result = launchers.run("delivery_all.sh", "--anywhere", override=override)
    assert result.returncode == 0, result.stdout + result.stderr
    assert_regional_calls(launchers, launchers.calls(), "parser", ["--deliver-pending"], override)


def test_delivery_check_does_not_execute_regional_programs(launchers):
    result = launchers.run("delivery_all.sh", "--check")
    assert result.returncode == 0, result.stdout + result.stderr
    assert launchers.calls() == []


@pytest.mark.parametrize("override", [False, True])
def test_poller_selects_regional_importers_then_acknowledges_and_requests_delivery(launchers, override):
    result = launchers.run("import_poll.sh", override=override)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = launchers.calls()
    assert_regional_calls(launchers, calls, "importer", ["--anywhere"], override)
    assert calls[-1]["kind"] == "systemctl"
    assert calls[-1]["argv"] == ["--no-block", "start", "court-delivery.service"]
    for clone in launchers.clones:
        assert (clone / "ops/mac-local-run/.runtime/import_pending_seen").read_text() == "test-pending-1\n"
    result = launchers.run("import_poll.sh", override=override)
    assert result.returncode == 0, result.stdout + result.stderr
    assert launchers.calls() == calls  # An unchanged tick is still silent.


@pytest.mark.parametrize("blocked", ["lock", "missing_program"])
def test_poller_does_not_acknowledge_a_clone_it_cannot_launch(launchers, blocked):
    first, second = launchers.clones
    if blocked == "lock":
        (first / "ops/mac-local-run/.run.lock").mkdir()
    else:
        (first / "ops/mac-local-run/import_dumps.sh").unlink()
    result = launchers.run("import_poll.sh")
    assert result.returncode == (1 if blocked == "missing_program" else 0), result.stdout + result.stderr
    imports = [call for call in launchers.calls() if call["kind"] == "importer"]
    assert [(call["version"], call["argv"]) for call in imports] == [("v2", [str(second), "--anywhere"])]
    assert not (first / "ops/mac-local-run/.runtime/import_pending_seen").exists()
    assert (second / "ops/mac-local-run/.runtime/import_pending_seen").exists()
