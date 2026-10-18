# -*- coding: utf-8 -*-
"""Два раздела одного областного суда проходят настоящий импортёр раздельно.

Адаптированные обезличенные фикстуры существующих судов проверяют цепочку
реестр → распознавание выдачи → разбор карточки → сохранение. Сеть отключена;
эти тесты не подтверждают доступность живого сайта одного областного суда.
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


FIXTURES = Path(__file__).parent / "fixtures"


def _one_result(filename: str, case_id: str, domain: str, district: str) -> str:
    html = (FIXTURES / filename).read_text(encoding="utf-8")
    rows = re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.S)
    header = next(row for row in rows if "<th" in row)
    row, = [row for row in rows if f"case_id={case_id}&" in row]
    return (f"<html><body><table>{header}{row}</table></body></html>"
            .replace("oblsud--svd.sudrf.ru", domain)
            .replace("Асбестовский городской суд", district)
            .replace("Ханты-Мансийский р-н", "Тюменский р-н" if "--tum." in domain else "Ханты-Мансийский р-н"))


@pytest.fixture(params=[
    pytest.param(("hmao", "oblsud--hmao.sudrf.ru", "86", "Сургутский городской суд"), id="hmao"),
    pytest.param(("tyumen", "oblsud--tum.sudrf.ru", "72", "Ленинский районный суд г. Тюмени"), id="tyumen"),
])
def dump_env(tmp_path, monkeypatch, request):
    code, domain, uid_prefix, district = request.param
    monkeypatch.setattr(config, "REGION", code)
    if code == "hmao":
        monkeypatch.setattr(config, "BANK_TRACK", False)
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
                      .replace("86MS0072", uid_prefix + "MS0072")
                      .replace("86OS0000", uid_prefix + "OS0000")
                      .replace("Ханты-Мансийский р-н", "Тюменский р-н" if code == "tyumen" else "Ханты-Мансийский р-н"))
    appeal_card = f"""<html><body><table id="tablcont">
      <tr><td>Уникальный идентификатор дела</td><td>{uid_prefix}RS0001-01-2026-000001-01</td></tr>
      <tr><td>Номер дела в первой инстанции</td><td>2-5001/2026</td></tr>
      <tr><td>Судья-докладчик</td><td>Тестов Т.Т.</td></tr>
      </table></body></html>"""
    calls = []

    def fetch_card(url, context=None):
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        assert parsed.hostname == domain
        calls.append(query)
        if query["delo_id"] == ["2800001"]:
            assert query["case_id"] == ["26942242"]
            return presidium_card
        assert query["delo_id"] == ["5"]
        assert query["case_id"] == ["9001"]
        return appeal_card

    monkeypatch.setattr(isd, "fetch_card_checked", fetch_card)
    return tmp_path, calls, code, domain, district


def test_resolve_oblast_sections_by_delo_id(dump_env):
    _, _, code, domain, _ = dump_env
    region = get_region(code)
    assert isd.resolve_court(domain, delo_id=5) is region.appeal_courts[0]
    assert isd.resolve_court(domain, delo_id="2800001") is region.presidium_courts[0]
    assert isd.resolve_court(domain.replace("--", "."), delo_id=2800001) is region.presidium_courts[0]


@pytest.mark.parametrize("order", [("appeal", "cassation"), ("cassation", "appeal")])
def test_imported_sections_do_not_create_each_other(dump_env, order):
    tmp_path, calls, _, domain, district = dump_env
    dumps = {
        "appeal": _one_result("search_appeal_dump_svd.html", "9001", domain, district),
        "cassation": _one_result("search_presidium_dump_hmao.html", "26942242", domain, district),
    }
    for index, section in enumerate(order, start=1):
        path = tmp_path / f"{section}.html"
        path.write_text(dumps[section], encoding="utf-8")
        assert isd.main([str(path), "--court-domain", domain, "--operator", "Тестовый оператор"]) == isd.EXIT_OK
        summary = json.loads((tmp_path / "output.txt").read_text(encoding="utf-8").split("summary=")[-1])
        assert summary["section"] == section
        assert summary["added"] == 1
        cases = json.loads((tmp_path / "cases.json").read_text(encoding="utf-8"))["cases"]
        assert len(cases) == index
        for case in cases:
            actual = case["current_stage"]
            other = "cassation" if actual == "appeal" else "appeal"
            assert case[actual]["court_domain"] == domain
            assert not case.get(other)
            assert case["import"]["source"] == ("dump_appeal" if actual == "appeal" else "dump_presidium")
    assert [call["delo_id"][0] for call in calls] == ["5" if section == "appeal" else "2800001" for section in order]
