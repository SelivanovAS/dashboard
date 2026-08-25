#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Локальный telemetry-checkpoint: атомарность, interrupted и честный HTTP."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

import pytest
import requests


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, netutil, runs, telemetry  # noqa: E402
from court_monitor.parsing import cards  # noqa: E402


def _read(path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def _clean_process_state():
    telemetry._reset_for_tests()
    config._metrics_reset()
    yield
    telemetry._reset_for_tests()
    config._metrics_reset()


def test_phase_and_coverage_are_atomic_plain_telemetry(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    run_id = telemetry.begin_run(str(path), "hmao", run_id="run-1")
    telemetry.set_phase(4, 9, "Обновление карточек апелляции")
    telemetry.set_coverage("appeal", 17, 34, processed=20, breaker_skipped=3)

    data = _read(path)
    cur = data["current"]
    assert run_id == "run-1"
    assert cur["status"] == "running"
    assert cur["phase"] == {
        "number": 4, "total": 9, "name": "Обновление карточек апелляции"
    }
    assert cur["coverage"]["appeal"] == {
        "read": 17, "planned": 34, "processed": 20, "breaker_skipped": 3
    }
    # Содержимого карточек/дел в контракте нет.
    assert "cases" not in cur and "events" not in cur


@pytest.mark.parametrize("region", ["hmao", "sverdlovsk_yanao"])
def test_checkpoint_keeps_territory_identity(tmp_path, region):
    path = tmp_path / region / "parse_telemetry.json"
    telemetry.begin_run(str(path), region, run_id=f"run-{region}")
    cur = _read(path)["current"]
    assert cur["region"] == region
    assert cur["run_id"] == f"run-{region}"


def test_retry_is_two_attempts_but_one_logical_request(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="run-1")

    rid = telemetry.begin_fetch(
        "court.test", "2-1/2026", attempt=1, max_attempts=2
    )
    telemetry.finish_fetch_transport(
        rid, "connection_reset", 0.12, attempt=1, will_retry=True,
        error="ConnectionError",
    )
    same = telemetry.begin_fetch(
        "court.test", "2-1/2026", attempt=2, max_attempts=2, request_id=rid
    )
    assert same == rid
    telemetry.finish_fetch_transport(rid, "http_200", 1.5, attempt=2)

    net = _read(path)["current"]["network"]
    assert net["logical_requests_started"] == 1
    assert net["logical_requests_completed"] == 1
    assert net["attempts_started"] == 2
    assert net["attempts_completed"] == 2
    assert net["transport_outcomes"] == {
        "connection_reset": 1, "http_200": 1
    }
    assert net["latency_by_transport"]["connection_reset"]["max"] == 0.12
    assert net["latency_by_transport"]["http_200"]["max"] == 1.5


def test_http_200_then_blocked_is_one_attempt(tmp_path):
    """Semantic reclassification must not turn one response into two HTTPs."""
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="run-1")
    rid = telemetry.begin_fetch("court.test", "2-1/2026")
    telemetry.finish_fetch_transport(rid, "http_200", 0.8, status=200)
    telemetry.classify_semantic(
        rid, "blocked", host="court.test", context="2-1/2026", rule="G"
    )
    # Идемпотентный повтор того же verdict счётчик тоже не двигает.
    telemetry.classify_semantic(rid, "blocked", host="court.test")

    cur = _read(path)["current"]
    net = cur["network"]
    assert net["logical_requests_started"] == 1
    assert net["attempts_started"] == 1
    assert net["attempts_completed"] == 1
    assert net["transport_outcomes"] == {"http_200": 1}
    assert net["semantic_outcomes"] == {"blocked": 1}
    assert cur["recent_failures"][-1]["layer"] == "semantic"


def test_semantic_reclassification_replaces_previous_kind(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="run-1")
    rid = telemetry.begin_fetch("court.test")
    telemetry.finish_fetch_transport(rid, "http_200", 0.1)
    telemetry.classify_semantic(rid, "empty_shell")
    telemetry.classify_semantic(rid, "blocked")
    assert _read(path)["current"]["network"]["semantic_outcomes"] == {
        "blocked": 1
    }


def test_breaker_snapshot_is_persisted_without_case_payload(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="breaker-run")
    telemetry.set_breaker_snapshot({
        "mode": "time",
        "open_hosts": 1,
        "by_kind": {"portal_placeholder": 1},
        "hosts": [{
            "host": "court.test",
            "kind": "portal_placeholder",
            "next_probe_in_seconds": 180.0,
            "deferred_remaining": 4,
        }],
    })

    breaker = _read(path)["current"]["breaker"]
    assert breaker["mode"] == "time"
    assert breaker["by_kind"] == {"portal_placeholder": 1}
    assert breaker["hosts"][0]["deferred_remaining"] == 4


def test_replace_failure_preserves_old_json_and_never_raises(
    tmp_path, monkeypatch, caplog
):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="run-1")
    before = path.read_bytes()

    def boom(*_args, **_kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(telemetry.os, "replace", boom)
    telemetry.set_phase(2, 9, "Кассация")
    telemetry.set_phase(3, 9, "Апелляция")

    assert path.read_bytes() == before
    warnings = [r for r in caplog.records if "checkpoint не записан" in r.message]
    assert len(warnings) == 1


def test_complete_write_failure_retries_completed_state_at_exit(
    tmp_path, monkeypatch,
):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="run-complete")
    real_replace = telemetry.os.replace
    failed = {"once": False}

    def fail_once(src, dst):
        if not failed["once"]:
            failed["once"] = True
            raise OSError("transient replace failure")
        return real_replace(src, dst)

    monkeypatch.setattr(telemetry.os, "replace", fail_once)
    telemetry.complete_run(cards_read=7)
    assert _read(path)["current"]["status"] == "running"

    telemetry._mark_interrupted_at_exit()
    cur = _read(path)["current"]
    assert cur["status"] == "completed"
    assert cur["summary"]["cards_read"] == 7


def test_os_exit_leaves_recoverable_in_flight_checkpoint(tmp_path):
    """os._exit bypasses atexit; the last atomic pre-request write survives."""
    path = tmp_path / "parse_telemetry.json"
    code = r'''
import os, sys
sys.path.insert(0, sys.argv[1])
from court_monitor import telemetry
telemetry.begin_run(sys.argv[2], "hmao", run_id="killed-run")
telemetry.set_phase(5, 9, "Первая инстанция")
telemetry.begin_fetch("slow.court.test", "2-99/2026", attempt=1, max_attempts=1)
os._exit(17)
'''
    proc = subprocess.run(
        [sys.executable, "-c", code, SCRIPTS_DIR, str(path)], check=False
    )
    assert proc.returncode == 17
    killed = _read(path)["current"]
    assert killed["status"] == "running"
    assert killed["in_flight"]["host"] == "slow.court.test"
    assert killed["in_flight"]["context"] == "2-99/2026"

    telemetry.begin_run(str(path), "hmao", run_id="next-run")
    data = _read(path)
    assert data["current"]["run_id"] == "next-run"
    old = data["history"][0]
    assert old["run_id"] == "killed-run"
    assert old["status"] == "interrupted"
    assert old["in_flight"]["host"] == "slow.court.test"
    assert old["last_checkpoint_at"]
    assert old["interruption_detected_at"]


def test_history_keeps_every_attempt_of_current_day(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    for i in range(6):
        telemetry.begin_run(str(path), "hmao", run_id=f"run-{i}")
        telemetry.complete_run(cards_read=i)
    telemetry.begin_run(str(path), "hmao", run_id="run-final")

    data = _read(path)
    assert len(data["history"]) == 6
    assert data["history"][0]["run_id"] == "run-5"
    assert data["history"][0]["status"] == "completed"
    assert data["history"][0]["summary"]["cards_read"] == 5
    assert data["daily"]["attempt_count"] == 7


def test_daily_case_sets_are_union_not_sum_of_changing_plans(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="attempt-1")
    telemetry.register_planned_case_ids(
        "first_instance", ["a.test|2-1/2026", "b.test|2-2/2026"]
    )
    telemetry.mark_case_read("first_instance", "a.test|2-1/2026")
    telemetry.set_coverage("first_instance", 1, 2)
    telemetry.complete_run()

    telemetry.begin_run(str(path), "hmao", run_id="attempt-2")
    # Дочитка пересчитала план: уже прочитанное дело из знаменателя выпало,
    # новое появилось. День должен дать 2/3, а не сумму 2/3 случайно по counts.
    telemetry.register_planned_case_ids(
        "first_instance", ["b.test|2-2/2026", "c.test|2-3/2026"]
    )
    telemetry.mark_case_read("first_instance", "b.test|2-2/2026")
    telemetry.set_coverage("first_instance", 1, 2)

    daily = _read(path)["daily"]
    assert daily["planned_case_ids_today"]["first_instance"] == [
        "a.test|2-1/2026", "b.test|2-2/2026", "c.test|2-3/2026",
    ]
    assert daily["read_case_ids_today"]["first_instance"] == [
        "a.test|2-1/2026", "b.test|2-2/2026",
    ]
    assert daily["coverage"]["first_instance"] == {"read": 2, "planned": 3}


def test_daily_network_aggregates_hosts_errors_and_recovery(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="attempt-1")
    rid = telemetry.begin_fetch("a.test", "2-1/2026")
    telemetry.finish_fetch_transport(rid, "read_timeout", 65.0)
    telemetry.set_breaker_snapshot({
        "probes": 1, "probe_failures": 1, "probe_successes": 0,
        "deferred_total": 3, "deferred_recovered": 0,
        "deferred_remaining": 3, "by_kind": {"read_timeout": 1},
        "hosts": [{"host": "a.test", "state": "open",
                   "kind": "read_timeout", "probes": 1,
                   "deferred_total": 3, "deferred_recovered": 0,
                   "deferred_remaining": 3}],
    })
    telemetry.complete_run()

    telemetry.begin_run(str(path), "hmao", run_id="attempt-2")
    rid = telemetry.begin_fetch("a.test", "2-1/2026")
    telemetry.finish_fetch_transport(rid, "http_200", 1.0, status=200)
    telemetry.classify_semantic(rid, "valid_card", host="a.test")
    telemetry.set_breaker_snapshot({
        "probes": 1, "probe_failures": 0, "probe_successes": 1,
        "deferred_total": 3, "deferred_recovered": 3,
        "deferred_remaining": 0, "by_kind": {},
        "hosts": [{"host": "a.test", "state": "closed", "kind": "",
                   "probes": 1, "deferred_total": 3,
                   "deferred_recovered": 3, "deferred_remaining": 0}],
    })

    daily = _read(path)["daily"]
    assert daily["network"]["transport_outcomes"] == {
        "http_200": 1, "read_timeout": 1,
    }
    assert daily["network"]["by_host"]["a.test"]["attempts_completed"] == 2
    assert daily["recovery"]["opened_hosts"] == ["a.test"]
    assert daily["recovery"]["recovered_hosts"] == ["a.test"]
    assert daily["recovery"]["probe_failures"] == 1
    assert daily["recovery"]["probe_successes"] == 1


def test_calls_before_begin_are_noop(tmp_path):
    path = tmp_path / "never-created.json"
    telemetry.set_phase(1, 9, "x")
    telemetry.set_coverage("fi", 0, 1)
    rid = telemetry.begin_fetch("court.test")
    telemetry.finish_fetch_transport(rid, "read_timeout", 65.0)
    telemetry.classify_semantic(rid, "blocked")
    telemetry.complete_run()
    assert not path.exists()


def test_runtime_directory_is_ignored_and_not_auto_staged():
    runtime = "ops/mac-local-run/.runtime/parse_telemetry.json"
    ignored = subprocess.run(
        ["git", "check-ignore", "-q", runtime], cwd=REPO, check=False
    )
    assert ignored.returncode == 0

    listed = subprocess.run(
        ["bash", "ops/stage_data_files.sh", "--list"],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    assert ".runtime" not in listed
    assert "parse_telemetry" not in listed


class _Response:
    def __init__(self, status: int, body: str):
        self.status_code = status
        self.text = body
        self.content = body.encode("windows-1251", errors="replace")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {self.status_code}", response=self
            )


@pytest.mark.parametrize(
    "body, expected_text, semantic, is_failure",
    [
        ("<html><body>Текст судебного акта</body></html>",
         "Текст судебного акта", "valid_act", False),
        ("<html><body><script>onlyCode()</script></body></html>",
         "", "empty_act", True),
    ],
)
def test_act_page_replaces_generic_valid_card_verdict(
    tmp_path, monkeypatch, body, expected_text, semantic, is_failure,
):
    """Act helper must describe the same HTTP as an act, not a case card."""
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="act-run")
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    monkeypatch.setattr(cards, "polite_delay", lambda: None)
    monkeypatch.setattr(
        netutil.session, "get", lambda _url, timeout=None: _Response(200, body)
    )
    url = (
        "https://court.test/modules.php?name=sud_delo&name_op=doc"
        "&number=123&delo_id=1540005"
    )

    assert cards.fetch_act_text(url, context="2-3/2026") == expected_text

    cur = _read(path)["current"]
    assert cur["network"]["logical_requests_started"] == 1
    assert cur["network"]["transport_outcomes"] == {"http_200": 1}
    assert cur["network"]["semantic_outcomes"] == {semantic: 1}
    semantic_failures = [
        item for item in cur["recent_failures"]
        if item.get("layer") == "semantic"
    ]
    assert bool(semantic_failures) is is_failure


def test_fetch_page_retry_wiring_keeps_one_logical_request(
    tmp_path, monkeypatch
):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="net-run")
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 2)
    monkeypatch.setattr(netutil.time, "sleep", lambda *_: None)
    calls = 0

    def get(_url, timeout=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise requests.ConnectionError(
                "wrapped", ConnectionResetError(54, "reset")
            )
        return _Response(200, "<html><table></table></html>")

    monkeypatch.setattr(netutil.session, "get", get)
    assert netutil.fetch_page(
        "https://court.test/modules.php", context="поиск"
    )

    cur = _read(path)["current"]
    net = cur["network"]
    assert net["logical_requests_started"] == 1
    assert net["logical_requests_completed"] == 1
    assert net["attempts_started"] == 2
    assert net["attempts_completed"] == 2
    assert net["transport_outcomes"] == {
        "connection_reset": 1, "http_200": 1
    }
    assert cur["in_flight"] is None
    assert config.FETCH_DIAG["request_id"].startswith("net-run:")


def test_fetch_card_http_200_blocked_is_not_double_counted(
    tmp_path, monkeypatch
):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="card-run")
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    block = (
        "<html><body>Этот запрос заблокирован по соображениям безопасности "
        "(G) ip: 43.245.226.66 Host: court.test</body></html>"
    )
    monkeypatch.setattr(
        netutil.session, "get", lambda _url, timeout=None: _Response(200, block)
    )

    assert netutil.fetch_card_checked(
        "https://court.test/modules.php?name=sud_delo&name_op=case&case_id=1",
        context="2-1/2026",
    ) == ""

    cur = _read(path)["current"]
    net = cur["network"]
    assert net["logical_requests_started"] == 1
    assert net["logical_requests_completed"] == 1
    assert net["attempts_started"] == 1
    assert net["attempts_completed"] == 1
    assert net["transport_outcomes"] == {"http_200": 1}
    assert net["semantic_outcomes"] == {"waf_block": 1}
    assert config.FETCH_DIAG["kind"] == "waf_block"
    assert config.FETCH_DIAG["request_id"].startswith("card-run:")
    # В checkpoint нет ни тела страницы, ни полного URL карточки.
    raw = path.read_text(encoding="utf-8")
    assert "Этот запрос заблокирован" not in raw
    assert "modules.php" not in raw


def test_search_http_200_outage_reclassifies_same_request(
    tmp_path, monkeypatch
):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="search-run")
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    outage = (
        "<html><body>Информация временно недоступна. "
        "Приносим свои извинения.</body></html>"
    )
    monkeypatch.setattr(
        netutil.session,
        "get",
        lambda _url, timeout=None: _Response(200, outage),
    )
    url = "https://court.test/modules.php?name=sud_delo&name_op=r_judge"
    assert netutil.fetch_page(url, context="поиск суда")
    netutil.mark_last_fetch_semantic(
        "outage_search", url, context="поиск суда", rows=0
    )

    cur = _read(path)["current"]
    net = cur["network"]
    assert net["logical_requests_started"] == 1
    assert net["attempts_started"] == 1
    assert net["transport_outcomes"] == {"http_200": 1}
    assert net["semantic_outcomes"] == {"outage_search": 1}
    assert cur["recent_failures"][-1]["context"] == "поиск суда"


def test_empty_shell_overrides_valid_card_without_second_attempt(
    tmp_path, monkeypatch
):
    """Второй рубеж парсера уточняет тот же HTTP 200, а не рисует запрос."""
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="empty-shell-run")
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    # Якорь УИД пропускает страницу через строгий transport/card-гейт; уже
    # parse_case_card вправе обнаружить, что полезных таблиц всё же нет.
    html = "<html><body>Уникальный идентификатор</body></html>"
    monkeypatch.setattr(
        netutil.session,
        "get",
        lambda _url, timeout=None: _Response(200, html),
    )
    url = "https://court.test/modules.php?name=sud_delo&name_op=case&case_id=1"
    assert netutil.fetch_card_checked(url, context="2-2/2026") == html
    netutil.mark_last_fetch_semantic(
        "empty_shell", url, context="2-2/2026"
    )

    cur = _read(path)["current"]
    net = cur["network"]
    assert net["logical_requests_started"] == 1
    assert net["attempts_started"] == 1
    assert net["transport_outcomes"] == {"http_200": 1}
    assert net["semantic_outcomes"] == {"empty_shell": 1}
    assert cur["recent_failures"][-1]["context"] == "2-2/2026"


def test_log_phase_writes_live_checkpoint(tmp_path):
    path = tmp_path / "parse_telemetry.json"
    telemetry.begin_run(str(path), "hmao", run_id="phase-run")
    runs.log_phase(6, 9, "Обновление карточек 1-й инстанции")
    assert _read(path)["current"]["phase"] == {
        "number": 6,
        "total": 9,
        "name": "Обновление карточек 1-й инстанции",
    }


@pytest.mark.parametrize("rows,captcha,outage,expected", [
    (3, False, False, "valid_search"),
    (0, True, False, "captcha_search"),
    (0, False, True, "outage_search"),
    (0, False, False, "empty_search"),
])
def test_unknown_zero_search_is_not_semantic_success(
    rows, captcha, outage, expected,
):
    assert runs._search_semantic_kind(
        rows, captcha=captcha, outage=outage
    ) == expected


def test_production_wiring_uses_local_env_and_all_stage_checkpoints():
    with open(
        os.path.join(REPO, "ops/mac-local-run/parse_and_push.sh"),
        encoding="utf-8",
    ) as f:
        launcher = f.read()
    assert (
        'PARSE_TELEMETRY_FILE="$REPO/ops/mac-local-run/.runtime/'
        'parse_telemetry.json"' in launcher
    )

    with open(
        os.path.join(REPO, "scripts/court_monitor/runs.py"), encoding="utf-8"
    ) as f:
        source = f.read()
    assert "telemetry.begin_run(" in source
    assert "config.PARSE_TELEMETRY_FILE" in source
    assert "config.REGION" in source
    assert "telemetry.complete_run(" in source
    # Три search-пути + второй рубеж пустой карточки в FI/appeal hot loops.
    assert source.count("mark_last_fetch_semantic(") >= 5
    for semantic in (
        "captcha_search", "outage_search", "valid_search", "empty_search",
        "degraded_card", "unparsed_card",
    ):
        assert semantic in source
    assert source.count('"empty_shell", url') >= 2
    for stage in (
        '"cassation_search"',
        '"cassation_refresh"',
        '"appeal"',
        '"first_instance"',
    ):
        assert re.search(
            rf"telemetry\.set_coverage\(\s*{re.escape(stage)}", source
        )


def test_cassation_coverage_plan_is_fixed_before_http_and_business_filter():
    """Обрыв цикла не вправе ужимать denominator; валидная карточка без
    Сбера тоже прочитана на техническом уровне."""
    source = open(
        os.path.join(REPO, "scripts/court_monitor/runs.py"),
        encoding="utf-8",
    ).read()
    start = source.index("cass_plan: list[dict] = []")
    end = source.index("# Передаём горячий архив", start)
    block = source[start:end]
    assert block.index("cass_planned = len(cass_plan)") < block.index(
        "cass_queue = DeferredCardQueue("
    )
    assert "for _work in cass_queue:" in block
    assert block.index("cass_parsed += 1") < block.index(
        'if not info.get("sber_present")'
    )

    refresh = source.index("cass_refresh_total = max(")
    refresh_loop = source.index("for case in cases:", refresh)
    assert refresh < refresh_loop


def test_generic_fi_degraded_card_is_not_counted_or_date_stamped():
    source = open(
        os.path.join(REPO, "scripts/court_monitor/runs.py"),
        encoding="utf-8",
    ).read()
    start = source.index('if _fi_degraded == "degraded":')
    end = source.index("# Промоушен материала по карточке", start)
    block = source[start:end]
    assert 'mark_last_fetch_semantic(\n                "degraded_card"' in block
    assert "continue" in block
    assert 'fi["last_checked_at"]' not in block
    assert "fi_parsed +=" not in block
