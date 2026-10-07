"""Дневная дочитка: сохранность событий без повторного открытия доставки.

Только временные JSON и программный рендер. Ограниченный участок main_json
исполняется из его AST: обход судов, сеть и отправка не запускаются.
"""

from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from court_monitor import config, delivery, fi_act_watch  # noqa: E402
from court_monitor.digest import core  # noqa: E402
from court_monitor.digest.template import generate_template_digest  # noqa: E402
from court_monitor.storage import load_json, save_json  # noqa: E402


class Clock(datetime):
    value = datetime(2026, 10, 7, 12)

    @classmethod
    def now(cls, tz=None):
        return cls.value

    @classmethod
    def utcnow(cls):
        return cls.value


@pytest.fixture
def files(tmp_path, monkeypatch):
    for key, name in (
        ("JSON_PATH", "cases.json"),
        ("LAST_DIGEST_CONTEXT_PATH", "last_digest_context.json"),
        ("LAST_DIGEST_PATH", "last_digest.json"),
        ("PARSE_TXN_ACK_FILE", "parse.ack.json"),
    ):
        monkeypatch.setattr(config, key, str(tmp_path / name))
    monkeypatch.setattr(config, "PARSE_TXN_ID", "retry-txn")
    monkeypatch.setattr(config, "DIGEST_CONTEXT_REQUIRED", False)
    monkeypatch.setattr(config, "TELEGRAM_BOT_TOKEN", "isolated-test-token")
    monkeypatch.setattr(core, "datetime", Clock)
    monkeypatch.setattr(Clock, "value", datetime(2026, 10, 7, 12))
    data = {"cases": [{"id": "kept"}], "archive": [{"id": "archived"}],
            "other_queue": {"durable": True}}
    save_json(data, config.JSON_PATH)
    save_json({"saved_at": "2026-10-07T08:45:00", "issue_key": "morning",
               "delivered_at": "2026-10-07T08:46:00",
               "fi_changes": [{"case": "already-delivered"}]},
              config.LAST_DIGEST_CONTEXT_PATH)
    Path(config.LAST_DIGEST_PATH).write_text('{"html": "утренний выпуск"}\n', encoding="utf-8")
    return SimpleNamespace(data=data, ack=Path(config.PARSE_TXN_ACK_FILE),
                           context=Path(config.LAST_DIGEST_CONTEXT_PATH),
                           digest=Path(config.LAST_DIGEST_PATH))


def payload(slot):
    return {
        **{key: [{"case": f"{key}-{slot}", "type": ["test_event"],
                  "details": {"slot": slot}}] for key in core._CTX_DELTA_KEYS},
        "cases": [{"id": f"snapshot-{slot}"}],
        "total_active_fi": slot,
        "total_active_appeal": slot + 10,
    }


def _main_context_boundary(data, values, *, retry_only, calls, consumer_probes=None):
    """Реальные save/merge/ack/return из main_json, без его парсерной части."""
    path = ROOT / "scripts/court_monitor/runs.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main_json")

    def assignment(node, name):
        return isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets)

    start = next(i for i, node in enumerate(main.body) if assignment(node, "digest_will_deliver"))
    end = next(i for i, node in enumerate(main.body[start:], start) if assignment(node, "digest"))
    function = ast.parse("def run_boundary():\n    pass\n").body[0]
    function.body = deepcopy(main.body[start:end + 1])
    if consumer_probes is not None:
        # Исполняем именно фактические вызовы downstream-потребителей, без
        # send_web_push/Telegram/LLM. Фабрика персонального push настоящая.
        names = {"_lint_digest_and_alert", "_make_per_sub_callback",
                 "attach_act_analyses", "_attach_bank_act_analyses"}
        probes = [n for n in ast.walk(main) if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name) and n.func.id in names]
        assert {n.func.id for n in probes} == names
        function.body += [ast.Expr(value=deepcopy(n)) for n in sorted(probes, key=lambda n: n.lineno)]
    function.body.append(ast.Return(value=ast.Name(id="context_args", ctx=ast.Load())))

    def record(name):
        return lambda *args, **kwargs: calls.append((name, deepcopy(kwargs)))

    fields = {"appeal_new_cases_csv": values.get("new_cases", []),
              "csv_cases": values.get("cases", []),
              **{key: values.get(key, []) for key in core._CTX_DELTA_KEYS if key != "new_cases"},
              **{key: values.get(key, 0) for key in (
                  "total_active_fi", "total_active_appeal", "total_active_cassation", "total_active_bank")}}
    # В настоящем main_json это локальные переменные. Инициализируем их
    # аргументами, чтобы последующее присваивание не читало пустые locals.
    function.args.args = [ast.arg(arg=name) for name in fields]
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    namespace = dict(fields, data=data, retry_only=retry_only, config=config,
                     _merge_day_context=core._merge_day_context,
                     save_digest_context=core.save_digest_context,
                     load_json=load_json, save_json=save_json, json=json,
                     telemetry=SimpleNamespace(complete_run=record("retry_complete")),
                     log=SimpleNamespace(info=lambda *args: None),
                     acknowledge_imported_cassation_changes=record("cassation_ack"),
                     appeal_act_watch=SimpleNamespace(acknowledge=record("appeal_ack")),
                     fi_act_watch=SimpleNamespace(acknowledge=record("fi_ack"),
                                                  merge_changes=fi_act_watch.merge_changes),
                     writ_watch=SimpleNamespace(PENDING="pending_writ_changes"),
                     generate_digest=record("render"),
                     fi_parsed=0, fi_total=0, cass_parsed=0, cass_refresh_parsed=0,
                     cass_planned=0, cass_refresh_total=0,
                     ap_skip_stats={"parsed": 0, "total": 0})
    if consumer_probes is not None:
        def consumer(name):
            def capture(*args, **kwargs):
                consumer_probes[name] = (deepcopy(args), deepcopy(kwargs))
                return 0
            return capture

        def personal_callback(**kwargs):
            consumer_probes["push_args"] = deepcopy(kwargs)
            consumer_probes["push_callback"] = delivery._make_per_sub_callback(**kwargs)

        namespace.update(
            cases=data.get("cases") or [], archived_cases=[],
            bank_active=[], bank_archived_all=[], push_summary="Сводка", digest_is_empty=False,
            _lint_digest_and_alert=consumer("lint"),
            _make_per_sub_callback=personal_callback,
            attach_act_analyses=consumer("analysis"),
            _attach_bank_act_analyses=consumer("bank_analysis"),
        )
    exec(compile(module, str(path), "exec"), namespace)
    return namespace["run_boundary"](**fields)


def test_retry_preserves_delivery_bytes_and_wal_follows_durable_write(files, monkeypatch):
    before = files.context.read_bytes(), files.digest.read_bytes()
    actual_save = core.save_json
    writes = []

    def checked_save(value, path):
        if str(path) == str(files.ack):
            pending = load_json(config.JSON_PATH)["pending_retry_context"]
            assert pending["issue_key"] == value["issue_key"]
            assert pending["fi_changes"] == payload(1)["fi_changes"]
        actual_save(value, path)
        writes.append(str(path))

    monkeypatch.setattr(core, "save_json", checked_save)
    issue = core.save_digest_context(**payload(1), defer_data=files.data, will_deliver=True)

    assert (files.context.read_bytes(), files.digest.read_bytes()) == before
    assert load_json(str(files.context))["delivered_at"] == "2026-10-07T08:46:00"
    assert issue.startswith("retry:")
    assert writes == [config.JSON_PATH, str(files.ack)]
    assert load_json(str(files.ack))["txn_id"] == "retry-txn"
    saved = load_json(config.JSON_PATH)
    assert saved["other_queue"] == {"durable": True}
    assert saved["cases"] == [{"id": "kept"}]
    assert saved["archive"] == [{"id": "archived"}]


def test_two_retry_slots_survive_restart_and_enter_next_normal_render(files, monkeypatch):
    before = files.context.read_bytes(), files.digest.read_bytes()
    calls = []
    for slot in (1, 2):
        monkeypatch.setattr(Clock, "value", datetime(2026, 10, 7, 10 + slot))
        data = load_json(config.JSON_PATH)  # новый процесс следующего слота
        assert _main_context_boundary(data, payload(slot), retry_only=True, calls=calls) is None
        assert (files.context.read_bytes(), files.digest.read_bytes()) == before
    assert [kind for kind, _ in calls] == ["retry_complete", "retry_complete"]
    pending = load_json(config.JSON_PATH)["pending_retry_context"]
    for key in core._CTX_DELTA_KEYS:
        assert pending[key] == payload(1)[key] + payload(2)[key]
    assert pending["cases"] == payload(2)["cases"]
    assert pending["total_active_fi"] == 2

    monkeypatch.setattr(Clock, "value", datetime(2026, 10, 8, 8, 45))
    data = load_json(config.JSON_PATH)
    rendered = _main_context_boundary(data, payload(3), retry_only=False, calls=calls)
    context = load_json(config.LAST_DIGEST_CONTEXT_PATH)
    assert "pending_retry_context" not in data
    assert "pending_retry_context" not in load_json(config.JSON_PATH)
    for key in core._CTX_DELTA_KEYS:
        expected = payload(1)[key] + payload(2)[key] + payload(3)[key]
        assert rendered[key] == context[key] == expected
    assert rendered["cases"] == payload(3)["cases"]
    assert context["delivered_at"] == "2026-10-08T08:45:00"
    assert calls[-1] == ("render", rendered)


def test_deferred_delta_reaches_every_delivery_and_analysis_consumer(files, monkeypatch):
    core.save_digest_context(**payload(1), defer_data=files.data)
    monkeypatch.setattr(Clock, "value", datetime(2026, 10, 8, 8, 45))
    probes = {}
    rendered = _main_context_boundary(files.data, payload(2), retry_only=False,
                                      calls=[], consumer_probes=probes)
    lint = probes["lint"][1]
    push = probes["push_args"]
    for key in core._CTX_DELTA_KEYS:
        expected = payload(1)[key] + payload(2)[key]
        assert rendered[key] == expected
        assert push["appeal_new_cases_csv" if key == "new_cases" else key] == expected
        if key != "stage_transitions":
            assert lint[key] == expected
    assert probes["analysis"][1]["all_changes"] == rendered["changes"] + rendered["fi_changes"]
    assert probes["analysis"][1]["cass_changes"] == rendered["cass_changes"]
    assert probes["bank_analysis"][0][2] == rendered["fi_changes"]


def test_deferred_only_act_notifies_watcher_in_next_direct_run(files, monkeypatch):
    change = _act_change()
    files.data["cases"] = [{"id": change["case"], "current_stage": "first_instance",
                             "first_instance": {"case_number": change["case"],
                                                "court_domain": "court.invalid"}}]
    core.save_digest_context([], [], fi_changes=[change], defer_data=files.data)
    monkeypatch.setattr(Clock, "value", datetime(2026, 10, 8, 8, 45))
    probes = {}
    _main_context_boundary(files.data, {}, retry_only=False, calls=[], consumer_probes=probes)
    # На утреннем обходе новых изменений нет. Единственный повод для
    # персонального push — акт, найденный вчера отдельной дочиткой.
    notification = probes["push_callback"]({"watchlist": [change["case"]]})
    assert notification is not None
    assert "твои дела" in notification[0]
    assert probes["push_callback"]({"watchlist": ["2-999/2026"]}) is None
    assert probes["analysis"][1]["all_changes"] == [change]


def test_exact_duplicates_merge_but_two_writs_for_same_case_survive(files):
    one = {"case": "2-1/2026", "type": ["fi_writ_issued"],
           "details": {"writ_number": "111", "writ_date": "01.10.2026"}}
    two = deepcopy(one)
    two["details"]["writ_number"] = "222"
    for changes in ([one], [one, two]):
        core.save_digest_context([], [], fi_changes=changes, defer_data=files.data)
    assert files.data["pending_retry_context"]["fi_changes"] == [one, two]


def test_deferred_write_failure_preserves_pending_and_does_not_ack(files, monkeypatch):
    core.save_digest_context(**payload(1), defer_data=files.data)
    previous = deepcopy(files.data)
    before = Path(config.JSON_PATH).read_bytes()
    ack_before = files.ack.read_bytes()

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(core, "save_json", fail)
    with pytest.raises(OSError, match="disk full"):
        core.save_digest_context(**payload(2), defer_data=files.data)
    assert files.data == previous
    assert Path(config.JSON_PATH).read_bytes() == before
    assert files.ack.read_bytes() == ack_before


def test_wal_ack_failure_leaves_durable_delta_retryable_without_duplicate(files, monkeypatch):
    actual_save = core.save_json

    def fail_ack(value, path):
        if str(path) == str(files.ack):
            raise OSError("ack unavailable")
        actual_save(value, path)

    monkeypatch.setattr(core, "save_json", fail_ack)
    with pytest.raises(RuntimeError, match="WAL"):
        core.save_digest_context(**payload(1), defer_data=files.data)
    assert not files.ack.exists()
    assert load_json(config.JSON_PATH)["pending_retry_context"]["fi_changes"] == payload(1)["fi_changes"]
    monkeypatch.setattr(core, "save_json", actual_save)
    data = load_json(config.JSON_PATH)
    core.save_digest_context(**payload(1), defer_data=data)
    assert data["pending_retry_context"]["fi_changes"] == payload(1)["fi_changes"]
    assert files.ack.exists()


def test_normal_context_write_failure_never_consumes_retry_events(files, monkeypatch):
    core.save_digest_context(**payload(1), defer_data=files.data)
    actual_save = core.save_json
    before = Path(config.JSON_PATH).read_bytes()

    def fail_context(value, path):
        if str(path) == str(files.context):
            raise OSError("context unavailable")
        actual_save(value, path)

    monkeypatch.setattr(core, "save_json", fail_context)
    calls = []
    with pytest.raises(RuntimeError, match="дочитка не подтверждена"):
        _main_context_boundary(files.data, payload(2), retry_only=False, calls=calls)
    assert calls == []  # ни ACK остальных очередей, ни рендер/доставка
    assert files.data["pending_retry_context"]["fi_changes"] == payload(1)["fi_changes"]
    assert Path(config.JSON_PATH).read_bytes() == before


@pytest.mark.parametrize("damage", ["wrong_issue", *core._CTX_DELTA_KEYS])
def test_retry_ack_requires_same_issue_and_all_delta_lists(files, damage):
    core.save_digest_context(**payload(1), defer_data=files.data)
    expected = core._merge_day_context(files.data["pending_retry_context"], payload(2))
    issue = core.save_digest_context(**expected)
    saved = load_json(str(files.context))
    if damage == "wrong_issue":
        saved["issue_key"] = "different"
    else:
        saved[damage] = saved[damage][1:]
    save_json(saved, str(files.context))
    with pytest.raises(RuntimeError, match="дочитка не подтверждена"):
        core.acknowledge_retry_context(files.data, issue, expected)
    assert "pending_retry_context" in files.data
    assert "pending_retry_context" in load_json(config.JSON_PATH)


def test_ack_clear_failure_restores_memory_and_disk_queue(files, monkeypatch):
    core.save_digest_context(**payload(1), defer_data=files.data)
    expected = core._merge_day_context(files.data["pending_retry_context"], payload(2))
    issue = core.save_digest_context(**expected)
    previous = deepcopy(files.data["pending_retry_context"])
    actual_save = core.save_json

    def fail_clear(value, path):
        if str(path) == config.JSON_PATH:
            raise OSError("cannot clear queue")
        actual_save(value, path)

    monkeypatch.setattr(core, "save_json", fail_clear)
    with pytest.raises(OSError, match="cannot clear"):
        core.acknowledge_retry_context(files.data, issue, expected)
    assert files.data["pending_retry_context"] == previous
    assert load_json(config.JSON_PATH)["pending_retry_context"] == previous
    monkeypatch.setattr(core, "save_json", actual_save)
    core.acknowledge_retry_context(files.data, issue, expected)
    assert "pending_retry_context" not in load_json(config.JSON_PATH)


def _act_change():
    return {"case": "2-100/2026", "court": "Тестовый районный суд",
            "plaintiff": "Истец", "defendant": "Банк", "bank_role": "Ответчик",
            "type": ["fi_act_text_published"], "details": {
                "link": "https://court.invalid/card", "court_domain": "court.invalid",
                "decision_date": "01.10.2026", "act_text": "Тестовый текст решения",
                "verdict_label": "В иске отказано",
                "fi_act_watch_key": "court.invalid|1540005|1|2-100/2026|2026-10-01"}}


def test_same_fi_act_from_retry_and_watch_is_rendered_once(files):
    one = _act_change()
    two = deepcopy(one)
    two["details"]["act_date"] = "07.10.2026"
    core.save_digest_context([], [], fi_changes=[one], defer_data=files.data)
    rendered = _main_context_boundary(files.data, {"fi_changes": [two]}, retry_only=False, calls=[])
    html = generate_template_digest(**rendered)
    assert "Опубликованные тексты решений (1)" in html
    assert len(rendered["fi_changes"]) == 1
    assert rendered["fi_changes"][0]["details"]["act_date"] == "07.10.2026"


def test_act_consolidation_keeps_distinct_rounds_courts_and_writs():
    one = _act_change()
    another_round = deepcopy(one)
    another_round["details"].update(decision_date="02.10.2026",
                                    fi_act_watch_key="court.invalid|1540005|1|2-100/2026|2026-10-02")
    another_court = deepcopy(one)
    another_court["details"].update(court_domain="other.invalid",
                                    fi_act_watch_key="other.invalid|1540005|1|2-100/2026|2026-10-01")
    writs = [{"case": one["case"], "type": ["fi_writ_issued"],
              "details": {"writ_number": number}} for number in ("111", "222")]
    values = [one, another_round, another_court, *writs]
    assert fi_act_watch.merge_changes({}, values) == values
