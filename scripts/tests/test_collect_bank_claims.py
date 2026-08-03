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
sys.path.insert(0, TESTS_DIR)

import collect_bank_claims as cbc  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from fixture_dates import days_ago as _ago, recent_fi_card_html  # noqa: E402

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


# ── resolve_court: пара (домен, srv_num) ─────────────────────────────────────

class TestResolveCourt:
    """На vartovray--hmao.sudrf.ru два суда: районный (srv 1) и постоянное
    присутствие в Покачи (srv 2). Резолв по одному домену всегда отдавал
    первый — Покачи собрать было нельзя."""

    def test_default_server_gives_district_court(self):
        court = cbc.resolve_court("vartovray--hmao.sudrf.ru", 1)
        assert court is not None and court.name == "Нижневартовский районный суд"

    def test_second_server_gives_pokachi(self):
        court = cbc.resolve_court("vartovray--hmao.sudrf.ru", 2)
        assert court is not None and court.srv_num == 2
        assert "Покачи" in court.name

    def test_domain_normalized(self):
        court = cbc.resolve_court("  VARTOVRAY--HMAO.SUDRF.RU ", 2)
        assert court is not None and "Покачи" in court.name

    def test_missing_server_is_none(self):
        """У обычного суда второго сервера нет — молча брать первый нельзя."""
        assert cbc.resolve_court("megion--hmao.sudrf.ru", 2) is None

    def test_unknown_domain_is_none(self):
        assert cbc.resolve_court("nosuch--hmao.sudrf.ru", 1) is None


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
    monkeypatch.setattr(cm_config, "JSON_BANK_EVENTS_PATH",
                        str(tmp_path / "cases_bank_events.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_EVENTS_PATH",
                        str(tmp_path / "cases_bank_archive_events.json"))
    monkeypatch.setattr(cbc, "polite_delay", lambda: None)

    def fake_fetch_page(url, context=None):
        if "page=2" in url:
            return _fixture("search_fi_bank_p2.html")
        if "page=" in url:
            return _fixture("search_fi_bank_p2.html")  # повтор → стоп-защита
        return _fixture("search_fi_bank_p1.html")

    monkeypatch.setattr(cbc, "fetch_page", fake_fetch_page)
    # Карточка с датами «на этой неделе»: иначе гейт приёма (entry_is_spent)
    # справедливо считает февральское дело отработавшим и e2e-набор пустеет.
    monkeypatch.setattr(cbc, "fetch_card_checked",
                        lambda url, context=None: recent_fi_card_html())
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


class TestCardLevelFilters:
    """Карточные фильтры: апелляция/кассация и ИЛ на исполнение решения
    (решение юриста 30.07.2026, сбор по Нижневартовскому городскому).
    До карточки в e2e-наборе доходят 3 кандидата — счётчики исключений = 3."""

    @pytest.mark.parametrize("flag", [
        "_fi_appeal_filed", "_fi_sent_to_appeal",
        "_fi_cassation_filed", "_fi_sent_to_cassation",
    ])
    def test_appeal_flag_skips(self, env, monkeypatch, flag):
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {"Результат": "Иск УДОВЛЕТВОРЕН", flag: True})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["added"] == 0
        assert counters["excluded_appeal"] == 3
        assert _bank_cases(env) == []

    @pytest.mark.parametrize("status", ["Выдан", "Отозван", "Возвращен"])
    def test_enforcement_writ_skips_any_status(self, env, monkeypatch, status):
        """Лист ПОСЛЕ решения — на исполнение; «Отозван»/«Возвращен» тоже
        пропускаются: лист всё равно был выдан."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "Статус": "Решено",
                "Дата заседания": _ago(60),
                "_events": [{"date": _ago(60),
                             "text": "Вынесено решение по делу. Иск удовлетворён"}],
                "_writs": [{"issue_date": _ago(20), "status": status}]})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["added"] == 0
        assert counters["excluded_writ"] == 3
        assert _bank_cases(env) == []

    def test_interim_writ_not_skipped(self, env, monkeypatch):
        """Обеспечительный лист (выдан ДО решения) — дело ещё ждёт ИЛ.

        Решение свежее (20 дней): у отказного дела набора окно на жалобу
        (BANK_DENIED_ARCHIVE_DAYS=30) ещё не истекло, иначе его срезал бы
        гейт приёма и счёт заведённых был бы про другое."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "Статус": "Решено",
                "Дата заседания": _ago(20),
                "_events": [{"date": _ago(20),
                             "text": "Вынесено решение по делу. Иск удовлетворён"}],
                "_writs": [{"issue_date": _ago(40), "status": "Выдан"}]})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["added"] == 3
        assert counters["excluded_writ"] == 0
        # Лист перенесён в запись (make_bank_entry), а не потерян.
        assert all(c["first_instance"].get("writs") for c in _bank_cases(env))

    def test_writ_without_anchor_not_skipped(self, env, monkeypatch):
        """Нет ни решения, ни терминального статуса → interim, не пропуск."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]})
        assert cbc.collect(_court(), 10, 0, False, "тест")["added"] == 3

    def test_pending_case_interim_writ_after_last_hearing_kept(self, env, monkeypatch):
        """Дело «В производстве»: обеспечительный лист выдан ПОЗЖЕ последнего
        зарегистрированного заседания (меры приняты в ходе процесса, следующее
        заседание ещё не опубликовано). Решения нет → лист может быть только
        обеспечительным, дело обязано попасть в трек.

        Регресс ревизии 30.07.2026: якорь «Дата заседания» давал здесь
        enforcement, и живое дело молча терялось — строка отчёта была
        неотличима от честного исключения."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "Статус": "В производстве",
                "Дата заседания": "12.05.2026",
                "_events": [{"date": "12.05.2026",
                             "text": "Судебное заседание. Объявлен перерыв"},
                            {"date": "15.05.2026",
                             "text": "Заявление об обеспечении иска удовлетворено"}],
                "_writs": [{"issue_date": "15.05.2026", "status": "Выдан"}]})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["excluded_writ"] == 0
        assert counters["added"] == 3

    def test_enforcement_writ_skipped_despite_post_decision_hearing(self, env, monkeypatch):
        """Решённое дело с ПОСТ-решенческим заседанием (заявление об отмене
        заочного, судебные расходы): «Дата заседания» уехала за дату выдачи
        листа. Якорь — решение, поэтому лист остаётся enforcement и дело
        исключается (регресс ревизии 30.07.2026)."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "Статус": "Решено",
                "Дата заседания": "20.06.2026",
                "_events": [{"date": "10.03.2026",
                             "text": "Вынесено заочное решение по делу"},
                            {"date": "20.06.2026",
                             "text": "Судебное заседание. Заявление о судебных расходах"}],
                "_writs": [{"issue_date": "05.05.2026", "status": "Выдан"}]})
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["excluded_writ"] == 3
        assert counters["added"] == 0

    def test_decided_card_without_decision_event_falls_back_to_hearing(self, env, monkeypatch):
        """Решённая карточка без события решения в истории движения → фолбэк
        на «Дату заседания» (прежнее поведение)."""
        monkeypatch.setattr(
            cbc, "parse_case_card",
            lambda html, base_url: {
                "Статус": "Решено",
                "Дата заседания": "12.02.2026",
                "_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]})
        assert cbc.collect(_court(), 10, 0, False, "тест")["excluded_writ"] == 3

    def test_real_card_with_enforcement_writs_skipped(self, env, monkeypatch):
        """Сквозной путь через настоящий parse_case_card: заседание 19.01.2026,
        листы 24.06/26.06.2026 (enforcement) + 21.11.2023 (interim) → пропуск."""
        monkeypatch.setattr(
            cbc, "fetch_card_checked",
            lambda url, context=None: _fixture("case_card_fi_writs.html"))
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["excluded_writ"] == 3
        assert counters["added"] == 0

    def test_real_short_card_with_appeal_skipped(self, env, monkeypatch):
        """Короткая карточка вкладки обжалования (_fi_appeal_filed=True) →
        пропуск через настоящий parse_case_card."""
        monkeypatch.setattr(
            cbc, "fetch_card_checked",
            lambda url, context=None: _fixture("case_card_fi_with_appeal.html"))
        counters = cbc.collect(_court(), 10, 0, False, "тест")
        assert counters["excluded_appeal"] == 3
        assert counters["added"] == 0
