#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проба доступа к карточкам с раннера (scripts/probe_court_access.py).

Прежняя проба (probe_courts.yml) спрашивала только ПОИСК и только хосты ХМАО:
env REGION в неё не передавался, и на форке территории она проверяла чужой
регион. У капчёвых судов Свердловской области поиск закрыт по проекту, весь
канал мониторинга — карточки, и молчали 16.08.2026 именно они.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

import probe_court_access as pca  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402


def _read_repo(rel: str) -> str:
    with open(os.path.join(os.path.dirname(SCRIPTS_DIR), rel),
              encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def sverdlovsk(monkeypatch):
    """Активный регион — Свердловск/ЯНАО: там и живут капчёвые суды."""
    monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
    return get_region()


def _cases_file(tmp_path, cases: list[dict]) -> str:
    path = tmp_path / "cases.json"
    path.write_text(json.dumps({"version": 1, "cases": cases},
                               ensure_ascii=False), encoding="utf-8")
    return str(path)


def _case(num: str, domain: str, link: str = "111|abc-uid") -> dict:
    return {
        "id": num, "current_stage": "first_instance",
        "first_instance": {"case_number": num, "court_domain": domain,
                           "srv_num": 1, "link": link},
    }


class TestCollectTargets:
    def test_gated_courts_go_first(self, sverdlovsk, tmp_path):
        """Капчёвые суды спрашиваем в первую очередь: у них карточки —
        единственный канал, поиск закрыт по проекту."""
        gated = [c for c in sverdlovsk.first_instance_courts if c.search_gated]
        plain = [c for c in sverdlovsk.first_instance_courts
                 if not c.search_gated]
        assert gated and plain, "фикстура региона потеряла смысл теста"
        path = _cases_file(tmp_path, [
            _case("2-1/2026", plain[0].domain),
            _case("2-2/2026", gated[0].domain),
        ])
        targets = pca.collect_targets(path)
        assert targets[0]["label"] == gated[0].name
        assert targets[0]["gated"] is True

    def test_one_target_per_court(self, sverdlovsk, tmp_path):
        gated = [c for c in sverdlovsk.first_instance_courts if c.search_gated]
        path = _cases_file(tmp_path, [
            _case("2-1/2026", gated[0].domain),
            _case("2-2/2026", gated[0].domain),
            _case("2-3/2026", gated[1].domain),
        ])
        assert len(pca.collect_targets(path)) == 2

    def test_limit_respected(self, sverdlovsk, tmp_path):
        courts = [c for c in sverdlovsk.first_instance_courts
                  if c.search_gated][:5]
        path = _cases_file(tmp_path, [
            _case(f"2-{i}/2026", c.domain) for i, c in enumerate(courts)])
        assert len(pca.collect_targets(path, limit=2)) == 2

    def test_foreign_and_broken_rows_skipped(self, sverdlovsk, tmp_path):
        """Чужой регион (данные эталона в форке) и запись без пары
        case_id|case_uid карточку не дают — URL из них не собрать."""
        gated = [c for c in sverdlovsk.first_instance_courts if c.search_gated]
        path = _cases_file(tmp_path, [
            _case("2-9/2026", "surggor--hmao.sudrf.ru"),
            _case("2-8/2026", gated[0].domain, link="без-разделителя"),
            _case("2-7/2026", gated[1].domain, link="|только-uid"),
            _case("2-6/2026", gated[2].domain),
        ])
        targets = pca.collect_targets(path)
        assert [t["case"] for t in targets] == ["2-6/2026"]

    def test_url_is_a_card_url(self, sverdlovsk, tmp_path):
        gated = [c for c in sverdlovsk.first_instance_courts if c.search_gated]
        path = _cases_file(tmp_path, [_case("2-1/2026", gated[0].domain)])
        url = pca.collect_targets(path)[0]["url"]
        assert "name_op=case" in url and "case_id=111" in url
        assert "case_uid=abc-uid" in url

    def test_missing_file_is_not_a_crash(self, sverdlovsk, tmp_path):
        assert pca.collect_targets(str(tmp_path / "нет.json")) == []


class TestReport:
    @staticmethod
    def _res(verdict, **kw):
        return {"label": "Суд", "url": "https://x--svd.sudrf.ru/modules.php",
                "gated": True, "case": "2-1/2026", "status": 200,
                "bytes": 100, "verdict": verdict, **kw}

    def test_blocked_verdict_names_the_runner(self):
        text = pca.render_report([self._res(pca.BLOCKED, ip="1.2.3.4")], "x")
        assert "BLOCKED" in text and "1.2.3.4" in text
        assert "прочитает НИЧЕГО" in text

    def test_all_ok_says_run_is_safe(self):
        text = pca.render_report([self._res(pca.OK)], "x")
        assert "OK —" in text and "Карточки читаются: 1/1" in text

    def test_mixed_is_not_reported_as_clean(self):
        results = [self._res(pca.OK), self._res(pca.BLOCKED)]
        assert pca.overall_verdict(results).startswith("MIXED")

    def test_no_targets_is_explicit(self):
        assert pca.overall_verdict([]) == "НЕТ ЦЕЛЕЙ"


class TestWorkflowWiring:
    """Проба бесполезна, если отчёт не доедет: логи ранов требуют admin-прав,
    а без REGION форк проверяет чужой регион (прежняя проба так и делала)."""

    def test_workflow_runs_probe_with_region_and_commits(self):
        yml = _read_repo(".github/workflows/probe_courts.yml")
        assert "probe_court_access.py" in yml
        assert "REGION: ${{ vars.REGION }}" in yml
        assert "ops/court_probe/report.txt" in yml
        assert "contents: write" in yml, "без прав push отчёта не будет"
        assert "git add ops/court_probe/report.txt" in yml

    def test_probe_step_runs_after_requests_installed(self):
        """Скрипт тянет requests через netutil; ставит его предыдущий шаг."""
        yml = _read_repo(".github/workflows/probe_courts.yml")
        assert yml.index("pip install --quiet requests") < \
            yml.index("probe_court_access.py")
