# -*- coding: utf-8 -*-
"""Два раздела Тюменского облсуда проходят настоящий импортёр раздельно.

Адаптированные обезличенные фикстуры существующих судов проверяют цепочку
реестр → распознавание выдачи → разбор карточки → сохранение. Сеть отключена;
эти тесты не подтверждают доступность живого сайта Тюменского облсуда.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit

import pytest

import import_search_dump as isd
from court_monitor import config
from court_monitor.regions import get_region


DOMAIN = "oblsud--tum.sudrf.ru"
FIXTURES = Path(__file__).parent / "fixtures"


def _one_result(filename: str, case_id: str) -> str:
    html = (FIXTURES / filename).read_text(encoding="utf-8")
    rows = re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.S)
    header = next(row for row in rows if "<th" in row)
    row, = [row for row in rows if f"case_id={case_id}&" in row]
    return (f"<html><body><table>{header}{row}</table></body></html>"
            .replace("oblsud--svd.sudrf.ru", DOMAIN)
            .replace("Асбестовский городской суд", "Ленинский районный суд г. Тюмени")
            .replace("Ханты-Мансийский р-н", "Тюменский р-н"))


@pytest.fixture
def dump_env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REGION", "tyumen")
    for field, filename in (
        ("JSON_PATH", "cases.json"),
        ("JSON_ARCHIVE_PATH", "cases_archive.json"),
        ("CSV_PATH", "cases.csv"),
        ("CSV_ARCHIVE_PATH", "cases_archive.csv"),
        ("CASSATION_ACTS_PATH", ".cassation_acts"),
    ):
        monkeypatch.setattr(config, field, str(tmp_path / filename))
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output.txt"))
    monkeypatch.setattr(isd, "polite_delay", lambda: None)
    (tmp_path / "cases.json").write_text('{"version": 1, "cases": []}', encoding="utf-8")

    presidium_card = ((FIXTURES / "case_card_presidium.html").read_text(encoding="utf-8")
                      .replace("86MS0072", "72MS0072")
                      .replace("86OS0000", "72OS0000")
                      .replace("Ханты-Мансийский р-н", "Тюменский р-н"))
    appeal_card = """<html><body><table id="tablcont">
      <tr><td>Уникальный идентификатор дела</td><td>72RS0001-01-2026-000001-01</td></tr>
      <tr><td>Номер дела в первой инстанции</td><td>2-5001/2026</td></tr>
      <tr><td>Судья-докладчик</td><td>Тестов Т.Т.</td></tr>
      </table></body></html>"""
    calls = []

    def fetch_card(url, context=None):
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        assert parsed.hostname == DOMAIN
        calls.append(query)
        if query["delo_id"] == ["2800001"]:
            assert query["case_id"] == ["26942242"]
            return presidium_card
        assert query["delo_id"] == ["5"]
        assert query["case_id"] == ["9001"]
        return appeal_card

    monkeypatch.setattr(isd, "fetch_card_checked", fetch_card)
    return tmp_path, calls


def test_resolve_tyumen_oblast_sections_by_delo_id(monkeypatch):
    monkeypatch.setattr(config, "REGION", "tyumen")
    region = get_region("tyumen")
    assert isd.resolve_court(DOMAIN, delo_id=5) is region.appeal_courts[0]
    assert isd.resolve_court(DOMAIN, delo_id="2800001") is region.presidium_courts[0]
    assert isd.resolve_court("oblsud.tum.sudrf.ru", delo_id=2800001) is region.presidium_courts[0]


@pytest.mark.parametrize("order", [("appeal", "cassation"), ("cassation", "appeal")])
def test_imported_sections_do_not_create_each_other(dump_env, order):
    tmp_path, calls = dump_env
    dumps = {
        "appeal": _one_result("search_appeal_dump_svd.html", "9001"),
        "cassation": _one_result("search_presidium_dump_hmao.html", "26942242"),
    }
    for index, section in enumerate(order, start=1):
        path = tmp_path / f"{section}.html"
        path.write_text(dumps[section], encoding="utf-8")
        assert isd.main([str(path), "--court-domain", DOMAIN, "--operator", "Тестовый оператор"]) == isd.EXIT_OK
        summary = json.loads((tmp_path / "output.txt").read_text(encoding="utf-8").split("summary=")[-1])
        assert summary["section"] == section
        assert summary["added"] == 1
        cases = json.loads((tmp_path / "cases.json").read_text(encoding="utf-8"))["cases"]
        assert len(cases) == index
        for case in cases:
            actual = case["current_stage"]
            other = "cassation" if actual == "appeal" else "appeal"
            assert case[actual]["court_domain"] == DOMAIN
            assert not case.get(other)
            assert case["import"]["source"] == ("dump_appeal" if actual == "appeal" else "dump_presidium")
    assert [call["delo_id"][0] for call in calls] == ["5" if section == "appeal" else "2800001" for section in order]
