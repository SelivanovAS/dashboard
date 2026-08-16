# -*- coding: utf-8 -*-
"""Пер-кейсовый отчёт парсинга трека «Иски банка» (bank_report.py):
аккумулятор исходов, атрибуция fetch-фейлов по дельте METRICS, totals,
запись файла и проводка (workflow коммитит файл, админка его читает)."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config  # noqa: E402
from court_monitor.bank_report import (  # noqa: E402
    BankParseReport, classify_fetch_failure, metrics_snapshot,
    save_bank_parse_report,
)
from court_monitor.lifecycle import skip_reason_ru  # noqa: E402

ROOT = os.path.dirname(SCRIPTS_DIR)


def _bank_case(number="2-100/2026", domain="surggor--hmao.sudrf.ru", **extra):
    case = {
        "id": number,
        "current_stage": "first_instance",
        "bank_role": "Истец",
        "track": "plaintiff_light",
        "first_instance": {
            "case_number": number,
            "court": "Сургутский городской суд",
            "court_domain": domain,
            "status": "Решено",
            "last_checked_at": "2026-07-27",
        },
    }
    case.update(extra)
    return case


# ── Аккумулятор ──────────────────────────────────────────────────────────────

class TestAccumulator:
    def test_non_track_case_ignored(self):
        rep = BankParseReport()
        case = _bank_case()
        case.pop("track")
        rep.seed(case, in_queue=True)
        rep.record(case, "parsed")
        assert rep.rows() == []

    def test_seed_not_in_queue_gets_stage_reason(self):
        rep = BankParseReport()
        case = _bank_case(current_stage="awaiting_appeal")
        rep.seed(case, in_queue=False)
        (row,) = rep.rows()
        assert row["outcome"] == "not_in_queue"
        assert row["detail"] == "awaiting_appeal"
        assert "уходит из лёгкого трека" in row["reason_ru"]

    def test_seed_in_queue_without_record_is_unknown(self):
        """Страховка от будущих правок цикла: незакрытая строка видна как
        unknown, а не молча теряется."""
        rep = BankParseReport()
        rep.seed(_bank_case(), in_queue=True)
        (row,) = rep.rows()
        assert row["outcome"] == "unknown"

    def test_skip_keeps_machine_reason_and_ru(self):
        rep = BankParseReport()
        case = _bank_case()
        reason = "writ_weekly(3d/7d)"
        rep.seed(case, in_queue=True)
        rep.record(case, "skip", reason=reason, reason_ru=skip_reason_ru(reason))
        (row,) = rep.rows()
        assert row["outcome"] == "skip"
        assert row["reason"] == reason
        assert "иск банка решён" in row["reason_ru"]

    def test_outcome_ru_fallback(self):
        rep = BankParseReport()
        case = _bank_case()
        rep.record(case, "fetch_captcha")
        (row,) = rep.rows()
        assert "проверочный код" in row["reason_ru"]

    def test_degraded_survives_parsed_record(self):
        """mark_degraded идёт ДО record('parsed') — флаг не должен теряться."""
        rep = BankParseReport()
        case = _bank_case()
        rep.mark_degraded(case)
        rep.record(case, "parsed")
        (row,) = rep.rows()
        assert row["outcome"] == "parsed" and row["degraded"] is True

    def test_events_and_flags(self):
        rep = BankParseReport()
        case = _bank_case()
        rep.record(case, "parsed")
        rep.mark_events(case, ["fi_writ_issued"], True)
        rep.mark_force_parsed(case)
        (row,) = rep.rows()
        assert row["events"] == ["fi_writ_issued"]
        assert row["changed"] is True and row["force_parsed"] is True

    def test_number_resolved_after_m_promotion(self):
        """Промоушен М→2 меняет id посреди цикла — отчёт держит дело по
        идентичности dict и отдаёт финальный номер."""
        rep = BankParseReport()
        case = _bank_case(number="М-6585/2026")
        rep.seed(case, in_queue=True)
        rep.record(case, "parsed")
        case["id"] = "2-3668/2026"
        case["first_instance"]["case_number"] = "2-3668/2026"
        (row,) = rep.rows()
        assert row["number"] == "2-3668/2026"
        assert row["key"] == "surggor--hmao.sudrf.ru|2-3668/2026"

    def test_mark_track_moves_after_split(self):
        """split_bank_track снимает маркер track у переехавших — пометка
        left_track должна находить строку и без маркера."""
        rep = BankParseReport()
        case = _bank_case()
        rep.record(case, "parsed")
        case.pop("track")
        case["track_origin"] = "plaintiff_light"
        rep.mark_track_moves()
        (row,) = rep.rows()
        assert row["left_track"] is True

    def test_mark_archived(self):
        rep = BankParseReport()
        case = _bank_case()
        rep.record(case, "skip", reason="writ_weekly(9d/7d)")
        rep.mark_archived(case)
        (row,) = rep.rows()
        assert row["archived"] is True


# ── Атрибуция fetch-фейлов по дельте METRICS ─────────────────────────────────

class TestClassifyFetchFailure:
    def _snapshot_and_bump(self, monkeypatch, key=None):
        before = metrics_snapshot()
        if key:
            monkeypatch.setitem(config.METRICS, key, config.METRICS.get(key, 0) + 1)
        return before

    def test_captcha(self, monkeypatch):
        before = self._snapshot_and_bump(monkeypatch, "cards_captcha")
        assert classify_fetch_failure(before) == "fetch_captcha"

    def test_blocked(self, monkeypatch):
        before = self._snapshot_and_bump(monkeypatch, "cards_blocked")
        assert classify_fetch_failure(before) == "fetch_blocked"

    def test_http(self, monkeypatch):
        before = self._snapshot_and_bump(monkeypatch, "requests_failed")
        assert classify_fetch_failure(before) == "fetch_http"

    def test_empty_body_fallback(self, monkeypatch):
        before = self._snapshot_and_bump(monkeypatch)
        assert classify_fetch_failure(before) == "fetch_empty"


# ── totals + запись файла ────────────────────────────────────────────────────

class TestTotalsAndSave:
    def _filled_report(self):
        rep = BankParseReport()
        cases = [
            (_bank_case("2-1/2026"), "parsed", ""),
            (_bank_case("2-2/2026"), "skip", "writ_weekly(3d/7d)"),
            (_bank_case("2-3/2026"), "fetch_captcha", ""),
            (_bank_case("2-4/2026"), "empty_shell", ""),
            (_bank_case("2-5/2026"), "no_link", ""),
        ]
        for case, outcome, reason in cases:
            rep.seed(case, in_queue=True)
            rep.record(case, outcome, reason=reason)
        out_of_queue = _bank_case("2-6/2026", current_stage="appeal")
        rep.seed(out_of_queue, in_queue=False)
        # Заведено авто-подхватом в этом же прогоне: в очередь обхода такое
        # дело не попадало (записи ещё не существовало), карточку прочитал сам
        # подхват — отдельный исход, не «спарсено».
        rep.record(_bank_case("2-7/2026"), "intake_new")
        return rep

    def test_totals_sum(self):
        t = self._filled_report().totals()
        assert t == {"total": 7, "parsed": 1, "skip": 1, "failed": 2,
                     "no_card": 1, "not_in_queue": 1, "intake_new": 1}
        assert (t["parsed"] + t["skip"] + t["failed"] + t["no_card"]
                + t["not_in_queue"] + t["intake_new"]) == t["total"]

    def test_intake_row_has_russian_reason(self):
        """Причины считает Python — JS админки логику не дублирует."""
        rep = BankParseReport()
        case = _bank_case("2-7/2026")
        rep.record(case, "intake_new", detail="Сургутский городской суд")
        (row,) = [r for r in rep.rows() if r["number"] == "2-7/2026"]
        assert row["outcome"] == "intake_new"
        assert "авто-подхват" in row["reason_ru"]

    def test_save_writes_schema(self, tmp_path):
        path = str(tmp_path / "bank_parse_report.json")
        self._filled_report().save(path, date(2026, 7, 29), True)
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        assert d["version"] == 1
        assert d["run_date"] == "2026-07-29"
        assert d["smart_skip"] is True
        assert d["updated_at"]
        assert d["totals"]["total"] == len(d["cases"]) == 7
        row = next(r for r in d["cases"] if r["number"] == "2-2/2026")
        assert row["outcome"] == "skip"
        assert row["reason"] == "writ_weekly(3d/7d)"
        assert row["key"] == "surggor--hmao.sudrf.ru|2-2/2026"
        assert row["last_checked_at"] == "2026-07-27"

    def test_save_wrapper_never_raises(self, tmp_path, monkeypatch, caplog):
        """Отчёт — сервисный канал: сбой записи гасится в WARNING, прогон
        не падает (config.BANK_PARSE_REPORT_PATH указывает на каталог —
        os.replace упадёт)."""
        monkeypatch.setattr(config, "BANK_PARSE_REPORT_PATH", str(tmp_path))
        save_bank_parse_report(self._filled_report(), date(2026, 7, 29), True)
        assert "Отчёт парсинга исков банка не записан" in caplog.text


# ── Проводка: workflow, Worker, админка ──────────────────────────────────────

class TestBankReportWiring:
    """Файл отчёта должен доезжать до GitHub Pages и читаться админкой —
    иначе аккумулятор пишет в никуда (по образцу TestBankTrackWiring)."""

    @staticmethod
    def _read(rel: str) -> str:
        with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
            return f.read()

    def test_workflow_commits_report(self):
        # С 16.08.2026 список коммитимых файлов один на облако и Mac-резерв:
        # ops/stage_data_files.sh спрашивает пути у court_monitor.config.
        import subprocess
        root = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        out = subprocess.run(["bash", "ops/stage_data_files.sh", "--list"],
                             cwd=root, capture_output=True, text=True,
                             check=True).stdout
        assert "data/bank_parse_report.json" in out, (
            "отчёт парсинга не коммитится — он не попадёт на GitHub Pages "
            "и карточка админки будет пустой."
        )
        wf = self._read(os.path.join(".github", "workflows", "update_cases.yml"))
        assert "stage_data_files.sh" in wf, "workflow не зовёт хелпер"

    def test_worker_config_has_bank_parse_url(self):
        worker = self._read(os.path.join("cloudflare-worker", "worker.js"))
        assert "bankParseUrl" in worker
        assert "bank_parse_report.json" in worker

    def test_admin_page_renders_card(self):
        admin = self._read(os.path.join("cloudflare-worker", "admin_page.js"))
        assert "bankParseUrl" in admin
        assert "bank-parse-card" in admin
        assert "loadBankParse" in admin

    def test_runs_wires_accumulator(self):
        """Точки врезки в FI-цикле: сид, запись исходов и сохранение в 7c."""
        runs_src = self._read(os.path.join("scripts", "court_monitor", "runs.py"))
        assert "bank_report = BankParseReport()" in runs_src
        assert "bank_report.seed(" in runs_src
        assert "save_bank_parse_report(bank_report" in runs_src
