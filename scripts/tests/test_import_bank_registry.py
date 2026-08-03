# -*- coding: utf-8 -*-
"""Импортёр реестра исков банка (scripts/import_bank_registry.py).

Сеть замокана (fetch_page/fetch_card_checked → фикстуры), хранилище — tmp
через monkeypatch config.* (config.X-инвариант: код читает значения на вызов).
Регион — Свердловск/ЯНАО: фикстура search_fi_all_roles.html содержит дела
всех ролей (2-1001 ответчик, 2-1002 истец, 2-1003 дочка, 2-1004 третье лицо).
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

import import_bank_registry as ibr  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from fixture_dates import days_ago, recent_fi_card_html  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _domain(idx: int = 0) -> str:
    return get_region("sverdlovsk_yanao").first_instance_courts[idx].domain


@pytest.fixture
def env(tmp_path, monkeypatch):
    """tmp-хранилище + регион Свердловск/ЯНАО + замоканная сеть."""
    monkeypatch.setattr(cm_config, "JSON_PATH", str(tmp_path / "cases.json"))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(tmp_path / "cases_archive.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_PATH", str(tmp_path / "cases_bank.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_PATH",
                        str(tmp_path / "cases_bank_archive.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_EVENTS_PATH",
                        str(tmp_path / "cases_bank_events.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_EVENTS_PATH",
                        str(tmp_path / "cases_bank_archive_events.json"))
    monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
    monkeypatch.setattr(ibr, "polite_delay", lambda: None)
    monkeypatch.setattr(ibr, "fetch_page",
                        lambda url, context=None: _fixture("search_fi_all_roles.html"))
    # Карточка с датами «на этой неделе»: у фикстуры решение датировано
    # февралём-2026, и гейт приёма (entry_is_spent) справедливо счёл бы такое
    # дело отработавшим — импортёру нечего было бы заводить.
    monkeypatch.setattr(ibr, "fetch_card_checked",
                        lambda url, context=None: recent_fi_card_html())
    return tmp_path


def _bank_cases(tmp_path) -> list[dict]:
    path = tmp_path / "cases_bank.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("cases", [])


class TestImport:
    def test_plaintiff_added_with_track_and_no_announce(self, env):
        counters = ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        assert counters["added"] == 1
        (case,) = _bank_cases(env)
        assert case["bank_role"] == "Истец"
        assert case["track"] == "plaintiff_light"
        # announced:true сразу — иски банка в дайджесте не анонсируются.
        assert case["import"]["announced"] is True
        assert case["import"]["source"] == "bank_registry"
        assert case["first_instance"]["court_domain"] == _domain()

    def test_defendant_not_taken(self, env):
        """Банк-ответчик — вне трека: такие дела заводит автопоиск."""
        counters = ibr.import_registry([(_domain(), "2-1001/2026")], 0, "тест")
        assert counters["not_plaintiff"] == 1
        assert counters["added"] == 0
        assert _bank_cases(env) == []

    def test_third_party_not_taken(self, env):
        counters = ibr.import_registry([(_domain(), "2-1004/2026")], 0, "тест")
        assert counters["not_plaintiff"] == 1

    def test_subsidiary_skipped(self, env):
        counters = ibr.import_registry([(_domain(), "2-1003/2026")], 0, "тест")
        assert counters["subsidiary"] == 1

    def test_already_tracked_in_main_cases(self, env):
        """Дело уже в основной базе (пришло «с апелляции») → [ALREADY]."""
        main = {"version": 1, "cases": [{
            "id": "2-1002/2026", "bank_role": "Истец",
            "first_instance": {"case_number": "2-1002/2026",
                               "court_domain": _domain()},
        }]}
        (env / "cases.json").write_text(
            json.dumps(main, ensure_ascii=False), encoding="utf-8")
        counters = ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        assert counters["already"] == 1
        assert counters["added"] == 0

    def test_same_number_other_court_not_blocked(self, env):
        """Дедуп судо-зависимый: номер занят в суде А — суд Б не блокируется."""
        main = {"version": 1, "cases": [{
            "id": "2-1002/2026", "bank_role": "Ответчик",
            "first_instance": {"case_number": "2-1002/2026",
                               "court_domain": _domain(1)},
        }]}
        (env / "cases.json").write_text(
            json.dumps(main, ensure_ascii=False), encoding="utf-8")
        counters = ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        assert counters["added"] == 1

    def test_idempotent_rerun(self, env):
        pairs = [(_domain(), "2-1002/2026")]
        assert ibr.import_registry(pairs, 0, "тест")["added"] == 1
        second = ibr.import_registry(pairs, 0, "тест")
        assert second["added"] == 0
        assert second["already"] == 1
        assert len(_bank_cases(env)) == 1

    def test_resolved_case_gets_resolved_emitted(self, env, monkeypatch):
        """Уже решённое дело не должно объявить решение задним числом."""
        monkeypatch.setattr(ibr, "parse_case_card",
                            lambda html, base_url: {"Статус": "Решено"})
        ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        (case,) = _bank_cases(env)
        assert case["first_instance"]["status"] == "Решено"
        assert case["first_instance"]["resolved_emitted"] is True

    def test_existing_writs_stored_silently(self, env, monkeypatch):
        """Уже выданные ИЛ переносятся в запись при импорте — первый прогон
        не объявит их «новыми» (принцип resolved_emitted)."""
        writ = {"issue_date": "01.06.2026", "blank_number": "",
                "electronic_id": "86RS0004#2-1002/2026#1", "status": "Выдан",
                "recipient": "ОСП"}
        monkeypatch.setattr(ibr, "parse_case_card",
                            lambda html, base_url: {"Статус": "Решено",
                                                    "_writs": [writ]})
        ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        (case,) = _bank_cases(env)
        assert case["first_instance"]["writs"] == [writ]

    def test_active_case_no_resolved_flag(self, env, monkeypatch):
        monkeypatch.setattr(ibr, "parse_case_card",
                            lambda html, base_url: {"Статус": "В производстве"})
        ibr.import_registry([(_domain(), "2-1002/2026")], 0, "тест")
        (case,) = _bank_cases(env)
        assert "resolved_emitted" not in case["first_instance"]

    def test_limit_counts_only_added(self, env):
        """Лимит тратится добавлениями, пропуски бесплатны; остаток — след. запуском."""
        pairs = [
            (_domain(), "2-1001/2026"),   # не истец — лимит не тратит
            (_domain(), "2-1002/2026"),   # добавится
            (_domain(1), "2-1002/2026"),  # тот же номер, другой суд — не обработан
        ]
        counters = ibr.import_registry(pairs, 1, "тест")
        assert counters["added"] == 1
        assert counters["not_plaintiff"] == 1
        # Второй запуск доберёт хвост.
        counters2 = ibr.import_registry([(_domain(1), "2-1002/2026")], 1, "тест")
        assert counters2["added"] == 1
        assert len(_bank_cases(env)) == 2

    def test_unknown_court(self, env):
        counters = ibr.import_registry(
            [("neizvestny--sud.sudrf.ru", "2-1002/2026")], 0, "тест")
        assert counters["unknown_court"] == 1


class TestReadRegistry:
    def test_reads_pairs_skips_header_comments_blank(self, tmp_path):
        reg = tmp_path / "registry.csv"
        reg.write_text(
            "court_domain;case_number\n"
            "# комментарий\n"
            "\n"
            f"{_domain()};2-1002/2026\n"
            f"{_domain(1).upper()} ; 2-500/2026 \n",
            encoding="utf-8",
        )
        assert ibr.read_registry(str(reg)) == [
            (_domain(), "2-1002/2026"),
            (_domain(1), "2-500/2026"),
        ]

    def test_bom_tolerated(self, tmp_path):
        reg = tmp_path / "registry.csv"
        reg.write_bytes(f"﻿{_domain()};2-1/2026\n".encode("utf-8"))
        assert ibr.read_registry(str(reg)) == [(_domain(), "2-1/2026")]
