#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Список коммитимых файлов данных — ОДИН на облако и Mac-резерв.

Список вёлся руками в двух местах — .github/workflows/update_cases.yml и
ops/mac-local-run/parse_and_push.sh — и молча разъехался: резерв усыпили
05.07.2026, трек «Иски банка» появился 25.07, и Mac-путь не коммитил семь его
файлов. Переключение на резерв означало бы «трек спарсен и выброшен»: 500 дел
ХМАО и 153 Урала на дашборде замерли бы, а негативный кэш отказников качался бы
заново каждый прогон. Теперь список не существует вовсе — пути спрашиваются у
court_monitor.config, где они и объявлены.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config  # noqa: E402

HELPER = os.path.join("ops", "stage_data_files.sh")


def _read_repo(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def staged() -> list[str]:
    out = subprocess.run(["bash", HELPER, "--list"], cwd=REPO_DIR,
                         capture_output=True, text=True, check=True)
    return [l.strip() for l in out.stdout.splitlines() if l.strip()]


def _declared_paths() -> list[str]:
    """Все объявленные пути данных: константы *_PATH модуля конфига."""
    return sorted({
        getattr(config, name) for name in dir(config)
        if name.endswith("_PATH") and isinstance(getattr(config, name), str)
        and getattr(config, name)
    })


class TestHelperCoversConfig:
    def test_every_declared_path_is_staged(self, staged):
        """Главный инвариант: новый файл данных не может потеряться. Появилась
        константа — путь попадает в коммит сам."""
        missing = [p for p in _declared_paths() if os.path.normpath(p) not in staged]
        assert not missing, f"не коммитятся объявленные пути: {missing}"

    def test_cold_archive_globs_are_staged(self, staged):
        """Холодные годовые архивы обеих картотек живут глобами, а не
        константами — их легче всего забыть."""
        for pattern in (config.cold_archive_glob(),
                        config.bank_cold_archive_glob()):
            assert os.path.normpath(pattern) in staged

    def test_bank_track_files_present(self, staged):
        """Именно эти семь путей и потерял Mac-резерв 25.07–16.08.2026."""
        for name in ("cases_bank.json", "cases_bank_events.json",
                     "cases_bank_archive.json",
                     "cases_bank_archive_events.json",
                     "bank_parse_report.json", ".bank_intake_seen.json"):
            assert any(p.endswith(name) for p in staged), name

    def test_no_stray_paths(self, staged):
        """Хелпер отдаёт только data/ — случайный конфиг в индекс не уедет."""
        assert all(p.startswith("data" + os.sep) for p in staged), staged


class TestBothConsumersUseHelper:
    """Оба пути обязаны звать хелпер и НЕ вести свой список — иначе они
    разъедутся снова, и точно так же молча."""

    @pytest.mark.parametrize("rel", [
        ".github/workflows/update_cases.yml",
        "ops/mac-local-run/parse_and_push.sh",
    ])
    def test_calls_helper(self, rel):
        text = _read_repo(rel)
        assert "stage_data_files.sh" in text, f"{rel} не зовёт хелпер"

    @pytest.mark.parametrize("rel", [
        ".github/workflows/update_cases.yml",
        "ops/mac-local-run/parse_and_push.sh",
    ])
    def test_no_own_data_add(self, rel):
        text = _read_repo(rel)
        own = [l.strip() for l in text.splitlines()
               if "git add data/" in l and not l.strip().startswith("#")]
        assert not own, f"{rel} снова ведёт свой список: {own}"
