# -*- coding: utf-8 -*-
"""Разовый сборщик исков банка с выдачи суда (scripts/collect_bank_claims.py):
обнаружение пейджера, фильтр строк (истец + исключаемые итоги юриста),
e2e с замоканной сетью, стоп-защита от повторившейся страницы, dry-run.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import collect_bank_claims as cbc  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _court():
    return next(c for c in get_region("hmao").first_instance_courts
                if c.domain == "surggor--hmao.sudrf.ru")


# ── discover_page_urls ───────────────────────────────────────────────────────

class TestDiscoverPager:
    def test_pager_links_found_and_absolutized(self):
        pages = cbc.discover_page_urls(
            _fixture("search_fi_bank_p1.html"), "https://surggor--hmao.sudrf.ru")
        assert sorted(pages) == [2, 3]
        assert pages[2].startswith("https://surggor--hmao.sudrf.ru/modules.php")
        # &amp; из разметки разэкранирован, параметр страницы на месте.
        assert "&page=2" in pages[2] and "&amp;" not in pages[2]

    def test_no_pager_empty(self):
        assert cbc.discover_page_urls(
            _fixture("search_fi_bank_p2.html"), "https://x.sudrf.ru") == {}

    def test_non_sud_delo_links_ignored(self):
        html = '<a href="index.php?page=2">2</a><a href="news.html">n</a>'
        assert cbc.discover_page_urls(html, "https://x.sudrf.ru") == {}


# ── row_passes ───────────────────────────────────────────────────────────────

class TestRowPasses:
    @staticmethod
    def _row(role="Истец", result="", link="1|a-1"):
        return {"bank_role": role, "result": result, "link": link}

    def test_plaintiff_passes(self):
        assert cbc.row_passes(self._row()) == (True, "")

    def test_plaintiff_with_refusal_passes(self):
        """«Отказано» вносим — по нему возможна апелляция банка."""
        assert cbc.row_passes(
            self._row(result="ОТКАЗАНО в удовлетворении иска"))[0] is True

    @pytest.mark.parametrize("role", ["Ответчик", "Третье лицо", ""])
    def test_non_plaintiff_rejected(self, role):
        assert cbc.row_passes(self._row(role=role)) == (False, "role")

    @pytest.mark.parametrize("result", [
        "Иск ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ",
        "Дело передано ПО ПОДСУДНОСТИ",
        "Заявление ВОЗВРАЩЕНО заявителю",
        "Производство по делу ПРЕКРАЩЕНО",
    ])
    def test_excluded_results_rejected(self, result):
        assert cbc.row_passes(self._row(result=result)) == (False, "excluded_result")

    def test_no_link_rejected(self):
        assert cbc.row_passes(self._row(link="")) == (False, "no_link")


# ── e2e с замоканной сетью ───────────────────────────────────────────────────

@pytest.fixture
def env(tmp_path, monkeypatch):
    """tmp-хранилище + сеть: стр.1 → p1, page=2 → p2, page=3 → снова p2
    (проверка стоп-защиты «страница повторилась»), карточки — фикстура."""
    monkeypatch.setattr(cm_config, "JSON_PATH", str(tmp_path / "cases.json"))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(tmp_path / "arch.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_PATH", str(tmp_path / "cases_bank.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_PATH",
                        str(tmp_path / "cases_bank_archive.json"))
    monkeypatch.setattr(cbc, "polite_delay", lambda: None)

    def fake_fetch_page(url, context=None):
        if "page=2" in url:
            return _fixture("search_fi_bank_p2.html")
        if "page=" in url:
            return _fixture("search_fi_bank_p2.html")  # повтор → стоп-защита
        return _fixture("search_fi_bank_p1.html")

    monkeypatch.setattr(cbc, "fetch_page", fake_fetch_page)
    monkeypatch.setattr(cbc, "fetch_card_checked",
                        lambda url, context=None: _fixture("case_card_first_instance.html"))
    return tmp_path


def _bank_cases(tmp_path) -> list[dict]:
    path = tmp_path / "cases_bank.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("cases", [])


class TestCollectE2E:
    def test_full_sweep(self, env):
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        # Стр. 3 повторила стр. 2 → обход остановился на двух страницах.
        assert counters["pages"] == 2
        assert counters["rows"] == 9
        assert counters["added"] == 3          # 2001 (активный), 2002 (решён), 2008 (отказ)
        assert counters["role"] == 1           # 2005 — банк ответчик
        assert counters["excluded_result"] == 4  # 2003, 2004, 2006, 2007
        assert counters["no_link"] == 1        # 2009
        added = {c["id"] for c in _bank_cases(env)}
        assert added == {"2-2001/2026", "2-2002/2026", "2-2008/2026"}
        for c in _bank_cases(env):
            assert c["track"] == "plaintiff_light"
            assert c["bank_role"] == "Истец"
            assert c["import"]["source"] == "search_sweep"
            assert c["import"]["announced"] is True

    def test_rerun_dedups(self, env):
        cbc.collect(_court(), 10, 0, False, "тест")
        second = cbc.collect(_court(), 10, 0, False, "тест")
        assert second["added"] == 0
        assert second["already"] == 3
        assert len(_bank_cases(env)) == 3

    def test_dry_run_writes_nothing(self, env):
        counters = cbc.collect(_court(), 10, 0, True, "тест")
        assert counters["added"] == 3
        assert not (env / "cases_bank.json").exists()

    def test_limit_stops(self, env):
        counters = cbc.collect(_court(), 10, 1, False, "тест")
        assert counters["added"] == 1
        assert len(_bank_cases(env)) == 1

    def test_pages_cap_respected(self, env):
        """--pages 1: пейджер найден, но дальше первой страницы не идём."""
        counters = cbc.collect(_court(), 1, 0, False, "тест")
        assert counters["pages"] == 1
        assert counters["rows"] == 5

    def test_card_result_second_filter(self, env, monkeypatch):
        """Выдача отстаёт от карточки: исключаемый итог виден только в
        карточке → дело не берём (кейс 2-8442/2026, dry-run 26.07.2026)."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {"Результат": "Дело передано ПО ПОДСУДНОСТИ"})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["added"] == 0
        # 4 исключены по выдаче + 3 кандидата добиты итогом из карточки.
        assert counters["excluded_result"] == 7
        assert _bank_cases(env) == []

    def test_already_in_main_base_skipped(self, env):
        """Дело уже в основной базе (тот же суд) → [ALREADY]."""
        main = {"version": 1, "cases": [{
            "id": "2-2001/2026", "bank_role": "Истец",
            "first_instance": {"case_number": "2-2001/2026",
                               "court_domain": "surggor--hmao.sudrf.ru"},
        }]}
        (env / "cases.json").write_text(
            json.dumps(main, ensure_ascii=False), encoding="utf-8")
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["already"] == 1
        assert counters["added"] == 2
