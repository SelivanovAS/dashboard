# -*- coding: utf-8 -*-
"""Точечное добавление дел (court_monitor/targeted_add.py + CLI + проводка).

Сеть замокана (fetch_page/fetch_card_checked на уровне модуля targeted_add),
хранилище — tmp через monkeypatch config.* (config.X-инвариант). Регион —
Свердловск/ЯНАО: там есть и капчёвые суды (link-режим — единственный путь),
и открытые (номер-режим), и двухсерверные домены (Камышловский 1/2,
Железнодорожный только srv=2).

Фикстура выдачи search_fi_all_roles.html: 2-1001 ответчик · 2-1002 истец ·
2-1003 дочка · 2-1004 третье-лицо-без-Сбера · 2-1005 ответчик без ссылки ·
2-1006 ~ М-500 комбо-номер (href srv_num=2).
"""

from __future__ import annotations

import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

import add_cases_targeted as cli  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor import targeted_add as ta  # noqa: E402
from court_monitor.courts import fi_court_by_domain  # noqa: E402
from court_monitor.parsing import parse_case_card  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from fixture_dates import recent_fi_card_html  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")
REGION = "sverdlovsk_yanao"


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _region():
    return get_region(REGION)


def _open_courts():
    return [c for c in _region().first_instance_courts
            if c.enabled and not c.search_gated]


def _gated_court():
    return next(c for c in _region().first_instance_courts if c.search_gated)


def _court_url(domain: str, *, case_id="777", case_uid="abcd-7777",
               delo_id=1540005, srv=1, name_op="case") -> str:
    return (f"https://{domain}/modules.php?name=sud_delo&srv_num={srv}"
            f"&name_op={name_op}&case_id={case_id}&case_uid={case_uid}"
            f"&delo_id={delo_id}")


def _card_html(*, title="ДЕЛО № 2-9001/2026", participants=(),
               result="", filing="05.07.2026",
               category="Иски о взыскании сумм по кредитному договору",
               judge="Судьина Светлана Сергеевна") -> str:
    """Синтетическая карточка 1-й инст.: заголовок «ДЕЛО № …», таблица «ДЕЛО»
    и таблица «Лица, участвующие в деле» (роль → parse_case_card)."""
    result_row = (f"<tr><td><b>Результат рассмотрения</b></td>"
                  f"<td>{result}</td></tr>" if result else "")
    part_rows = "".join(
        f"<tr><td>{role}</td><td>{name}</td></tr>"
        for role, name in participants)
    return f"""<html><body>
<div>{title}</div>
<table>
  <tr><td><b>Дата поступления</b></td><td>{filing}</td></tr>
  <tr><td><b>Категория дела</b></td><td>{category}</td></tr>
  <tr><td><b>Судья</b></td><td>{judge}</td></tr>
  {result_row}
</table>
<table>
  <tr><td>Лица, участвующие в деле</td></tr>
  <tr><td>Вид лица</td><td>ФИО</td></tr>
  {part_rows}
</table>
</body></html>"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    """tmp-хранилище + регион Свердловск/ЯНАО + отключённый polite_delay."""
    monkeypatch.setattr(cm_config, "JSON_PATH", str(tmp_path / "cases.json"))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH",
                        str(tmp_path / "cases_archive.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_PATH",
                        str(tmp_path / "cases_bank.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_PATH",
                        str(tmp_path / "cases_bank_archive.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_EVENTS_PATH",
                        str(tmp_path / "cases_bank_events.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_EVENTS_PATH",
                        str(tmp_path / "cases_bank_archive_events.json"))
    monkeypatch.setattr(cm_config, "REGION", REGION)
    monkeypatch.setattr(ta, "polite_delay", lambda: None)
    return tmp_path


def _mock_search(monkeypatch, result_domains=(), fail_domains=()):
    """fetch_page: фикстура выдачи для перечисленных доменов, '' — сбой сети,
    остальным — пустая страница. Возвращает список запрошенных URL."""
    fixture = _fixture("search_fi_all_roles.html")
    urls: list[str] = []

    def fake_fetch(url, context=None):
        urls.append(url)
        if any(d in url for d in fail_domains):
            return ""
        if any(d in url for d in result_domains):
            return fixture
        return "<html><body>Данных по запросу не обнаружено</body></html>"

    monkeypatch.setattr(ta, "fetch_page", fake_fetch)
    return urls


def _mock_card(monkeypatch, html):
    monkeypatch.setattr(ta, "fetch_card_checked",
                        lambda url, context=None: html)


def _forbid_card(monkeypatch):
    def boom(url, context=None):
        raise AssertionError(f"карточка не должна была качаться: {url}")
    monkeypatch.setattr(ta, "fetch_card_checked", boom)


def _run_item(env, monkeypatch, raw, *, court_override=None):
    state = ta.load_tracked_state()
    res = ta.process_item(state, raw, "Тест", "2026-08-10T10:00:00",
                          court_override)
    saved = ta.save_state(state)
    return res, state, saved


def _main_cases(tmp_path):
    p = tmp_path / "cases.json"
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8")).get("cases", [])


def _bank_pair(tmp_path):
    lst = tmp_path / "cases_bank.json"
    ev = tmp_path / "cases_bank_events.json"
    cases = (json.loads(lst.read_text(encoding="utf-8")).get("cases", [])
             if lst.exists() else [])
    events = (json.loads(ev.read_text(encoding="utf-8")).get("events", {})
              if ev.exists() else {})
    return cases, events


# ── Классификация ввода и разбор ссылки ──────────────────────────────────────

class TestClassifyInput:
    @pytest.mark.parametrize("raw,expected", [
        ("2-1234/2026", "2-1234/2026"),
        ("№ 2-1234/2026", "2-1234/2026"),
        ("№ 2-1234/2026", "2-1234/2026"),
        ("  2-1234/2026 ~ М-99/2026 ", "2-1234/2026"),
        ("2-122/2026 (2-535/2025;)", "2-122/2026"),
        ("2-2-279/2026", "2-2-279/2026"),   # Покачи, трёхчастный
        ("М-500/2026", "М-500/2026"),
    ])
    def test_numbers(self, raw, expected):
        assert ta.classify_input(raw) == ("number", expected)

    @pytest.mark.parametrize("raw", [
        "https://kamyshlovsky--svd.sudrf.ru/modules.php?name=sud_delo",
        "kamyshlovsky--svd.sudrf.ru/modules.php?name=sud_delo&case_id=1",
    ])
    def test_links(self, raw):
        kind, _ = ta.classify_input(raw)
        assert kind == "link"

    @pytest.mark.parametrize("raw", ["", "привет", "дело такое-то", "2-1234"])
    def test_garbage(self, raw):
        assert ta.classify_input(raw)[0] == ""


class TestParseCardLink:
    def test_full_url_with_amp_escapes(self):
        url = ("https://kamyshlovsky--svd.sudrf.ru/modules.php?name=sud_delo"
               "&amp;srv_num=2&amp;name_op=case&amp;case_id=123"
               "&amp;case_uid=ab12-cd34&amp;delo_id=1540005")
        link = ta.parse_card_link(url)
        assert link == {
            "domain": "kamyshlovsky--svd.sudrf.ru", "srv_num": 2,
            "delo_id": 1540005, "case_id": "123", "case_uid": "ab12-cd34",
            "name_op": "case",
        }

    def test_no_case_id(self):
        link = ta.parse_card_link(
            "https://kamyshlovsky--svd.sudrf.ru/modules.php?name=sud_delo"
            "&srv_num=1&name_op=sf&delo_id=1540005")
        assert link is not None and link["case_id"] == ""

    def test_not_sudrf(self):
        assert ta.parse_card_link("https://example.com/x?case_id=1") is None


class TestResolveLinkTarget:
    def _link(self, domain, **kw):
        base = {"domain": domain, "srv_num": 1, "delo_id": 1540005,
                "case_id": "1", "case_uid": "aa-11", "name_op": "case"}
        base.update(kw)
        return base

    def test_appeal_card_refused(self, env):
        appeal = _region().appeal_courts[0]
        court, reason = ta.resolve_link_target(
            self._link(appeal.domain, delo_id=appeal.delo_id))
        assert court is None
        assert "апелляци" in reason

    def test_cassation_card_refused(self, env):
        cass = _region().cassation_court
        court, reason = ta.resolve_link_target(
            self._link(cass.domain, delo_id=cass.delo_id))
        assert court is None
        assert "кассаци" in reason

    def test_foreign_region_refused(self, env):
        court, reason = ta.resolve_link_target(
            self._link("surggor--hmao.sudrf.ru"))
        assert court is None
        assert "не из нашего региона" in reason

    def test_wrong_section_refused(self, env):
        fi = _region().first_instance_courts[0]
        court, reason = ta.resolve_link_target(
            self._link(fi.domain, delo_id=5))
        assert court is None
        assert "другой раздел" in reason

    def test_two_server_domain_srv2(self, env):
        court, reason = ta.resolve_link_target(
            self._link("kamyshlovsky--svd.sudrf.ru", srv_num=2))
        assert reason == ""
        assert court.srv_num == 2

    def test_two_server_domain_default_first(self, env):
        court, _ = ta.resolve_link_target(
            self._link("kamyshlovsky--svd.sudrf.ru", srv_num=None))
        assert court.srv_num == 1

    def test_srv2_only_court(self, env):
        """Железнодорожный ЕКБ заведён ТОЛЬКО как srv_num=2."""
        court, _ = ta.resolve_link_target(
            self._link("zheleznodorozhny--svd.sudrf.ru", srv_num=None))
        assert court is not None and court.srv_num == 2

    def test_missing_case_id_refused(self, env):
        fi = _region().first_instance_courts[0]
        court, reason = ta.resolve_link_target(
            self._link(fi.domain, delo_id=fi.delo_id, case_id=""))
        assert court is None
        assert "case_id" in reason


class TestFiCourtByDomain:
    def test_unknown_domain(self, env):
        assert fi_court_by_domain("neizvestny--xxx.sudrf.ru") is None

    def test_srv_fallback_to_first(self, env):
        court = fi_court_by_domain("kamyshlovsky--svd.sudrf.ru", 9)
        assert court is not None and court.srv_num == 1


# ── Карточка: аддитивные ключи ───────────────────────────────────────────────

class TestCardKeys:
    def test_filing_category_and_material(self):
        html = _card_html(title="ДЕЛО № 2-9001/2026 ~ М-321/2026")
        card = parse_case_card(html)
        assert card["Дата поступления (карточка)"] == "05.07.2026"
        assert card["Категория (карточка)"].startswith("Иски о взыскании")
        assert card["Номер дела (карточка)"] == "2-9001/2026"
        assert card["Номер материала (карточка)"] == "М-321/2026"

    def test_material_only_title(self):
        card = parse_case_card(_card_html(title="ДЕЛО № М-2-309/2026"))
        assert card["Номер дела (карточка)"] == ""
        assert card["Номер материала (карточка)"] == "М-2-309/2026"

    def test_canonical_fixture_untouched(self):
        """Старые ключи канонической фикстуры не изменились."""
        card = parse_case_card(_fixture("case_card_first_instance.html"))
        assert card["Результат"].startswith("ОТКАЗАНО")
        assert card["Дата поступления (карточка)"] == "08.10.2025"
        assert card["Категория (карточка)"].startswith("Споры, связанные")


# ── Целевой поиск по номеру ──────────────────────────────────────────────────

class TestNumberSearch:
    def test_single_match(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        matches, failed, sub = ta.search_number_in_courts(
            "2-1001/2026", _open_courts())
        assert len(matches) == 1
        assert matches[0]["court_domain"] == court0.domain
        assert failed == [] and sub is False

    def test_material_alias_match(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        matches, _, _ = ta.search_number_in_courts(
            "М-500/2026", _open_courts())
        assert len(matches) == 1
        assert matches[0]["case_number"] == "2-1006/2026"

    def test_all_failed(self, env, monkeypatch):
        courts = _open_courts()
        _mock_search(monkeypatch,
                     fail_domains=[c.domain for c in courts])
        matches, failed, _ = ta.search_number_in_courts("2-1001/2026", courts)
        assert matches == []
        assert len(failed) == len(courts)

    def test_subsidiary_flag(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        matches, _, sub = ta.search_number_in_courts(
            "2-1003/2026", _open_courts())
        assert matches == [] and sub is True


# ── Пер-строчная оркестрация: номер-режим ────────────────────────────────────

class TestProcessNumber:
    def test_defendant_added_to_main(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        res, state, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ADDED_MAIN, res["line"]
        (case,) = _main_cases(env)
        assert case["id"] == "2-1001/2026"
        assert case["bank_role"] == "Ответчик"
        assert case["import"]["source"] == "targeted"
        assert "announced" not in case["import"]  # объявит ближайший прогон
        assert case["first_instance"]["events"] == []  # первый парс — штатный
        assert str(cm_config.JSON_PATH) in saved

    def test_plaintiff_added_to_bank(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        res, state, saved = _run_item(env, monkeypatch, "2-1002/2026")
        assert res["status"] == ta.ST_ADDED_BANK, res["line"]
        cases, events = _bank_pair(env)
        (case,) = cases
        assert case["track"] == "plaintiff_light"
        assert case["import"]["announced"] is True  # тихо, как реестр
        assert case["import"]["source"] == "targeted"
        # события карточки уехали в events-файл (split-хранение)
        key = f"{court0.domain}|2-1002/2026"
        assert key in events
        assert _main_cases(env) == []

    def test_ambiguous_needs_court(self, env, monkeypatch):
        courts = _open_courts()
        _mock_search(monkeypatch,
                     result_domains=[courts[0].domain, courts[1].domain])
        _forbid_card(monkeypatch)
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REFUSED
        assert "нескольких судах" in res["line"]

    def test_court_override_limits_search(self, env, monkeypatch):
        courts = _open_courts()
        urls = _mock_search(monkeypatch, result_domains=[courts[1].domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026",
                              court_override=courts[1])
        assert res["status"] == ta.ST_ADDED_MAIN
        assert all(courts[1].domain in u for u in urls)
        assert len(urls) == 1

    def test_gated_override_refused(self, env, monkeypatch):
        _forbid_card(monkeypatch)
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026",
                              court_override=_gated_court())
        assert res["status"] == ta.ST_REFUSED
        assert "проверочным кодом" in res["line"]

    def test_not_found_hints_gated_link(self, env, monkeypatch):
        _mock_search(monkeypatch)
        res, _, _ = _run_item(env, monkeypatch, "2-7777/2026")
        assert res["status"] == ta.ST_NOT_FOUND
        assert "ссылку на карточку" in res["line"]

    def test_gated_courts_not_searched(self, env, monkeypatch):
        urls = _mock_search(monkeypatch)
        _run_item(env, monkeypatch, "2-7777/2026")
        gated = {c.domain for c in _region().first_instance_courts
                 if c.search_gated}
        assert not any(any(g in u for g in gated) for u in urls)

    def test_subsidiary_refused(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _forbid_card(monkeypatch)
        res, _, _ = _run_item(env, monkeypatch, "2-1003/2026")
        assert res["status"] == ta.ST_REFUSED
        assert "дочерняя" in res["line"]

    def test_no_sber_refused_with_parties(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        res, _, _ = _run_item(env, monkeypatch, "2-1004/2026")
        assert res["status"] == ta.ST_REFUSED
        assert "Сбербанк не найден" in res["line"]
        assert "Прокурор" in res["line"]  # стороны показаны оператору

    def test_third_party_by_card_added(self, env, monkeypatch):
        """Сбер — третье лицо ПО КАРТОЧКЕ (в И:/О: строки его нет)."""
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, _card_html(
            title="ДЕЛО № 2-1004/2026",
            participants=(("ИСТЕЦ", "Прокурор г. Екатеринбурга"),
                          ("ОТВЕТЧИК", "Орлова Ольга Олеговна"),
                          ("ТРЕТЬЕ ЛИЦО", "ПАО Сбербанк"))))
        res, _, _ = _run_item(env, monkeypatch, "2-1004/2026")
        assert res["status"] == ta.ST_ADDED_MAIN, res["line"]
        (case,) = _main_cases(env)
        assert case["bank_role"] == "Третье лицо"

    def test_no_link_refused(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _forbid_card(monkeypatch)
        res, _, _ = _run_item(env, monkeypatch, "2-1005/2026")
        assert res["status"] == ta.ST_REFUSED
        assert "нет ссылки на карточку" in res["line"]

    def test_defendant_card_down_added_from_row(self, env, monkeypatch):
        """Карточка недоступна → ответчик-дело заводится по строке выдачи
        (как дамповый импортёр), заметка в line."""
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, "")
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ADDED_MAIN
        assert "карточка недоступна" in res["line"]

    def test_plaintiff_card_down_fetch_error(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, "")
        res, _, _ = _run_item(env, monkeypatch, "2-1002/2026")
        assert res["status"] == ta.ST_FETCH_ERROR
        assert _bank_pair(env)[0] == []

    def test_all_courts_down_fetch_error(self, env, monkeypatch):
        courts = _open_courts()
        _mock_search(monkeypatch, fail_domains=[c.domain for c in courts])
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_FETCH_ERROR

    def test_bank_track_disabled_refuses_plaintiff(self, env, monkeypatch):
        """BANK_TRACK=0: прогон bank-файлы не грузит — истцовая запись
        зависла бы замороженной, отказываем с пояснением."""
        monkeypatch.setattr(cm_config, "BANK_TRACK", False)
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        res, _, saved = _run_item(env, monkeypatch, "2-1002/2026")
        assert res["status"] == ta.ST_REFUSED, res["line"]
        assert "выключен" in res["line"]
        assert saved == []
        assert _bank_pair(env)[0] == []


# ── Пер-строчная оркестрация: link-режим ─────────────────────────────────────

class TestProcessLink:
    def test_defendant_link_gated_court_added(self, env, monkeypatch):
        """Главный сценарий Урала: капчёвый суд, ссылка на карточку."""
        gated = _gated_court()
        _mock_card(monkeypatch, _card_html(
            title="ДЕЛО № 2-9001/2026",
            participants=(("ИСТЕЦ", "Иванов Иван Иванович"),
                          ("ОТВЕТЧИК", "ПАО Сбербанк"))))
        url = _court_url(gated.domain, srv=gated.srv_num)
        res, _, _ = _run_item(env, monkeypatch, url)
        assert res["status"] == ta.ST_ADDED_MAIN, res["line"]
        (case,) = _main_cases(env)
        assert case["id"] == "2-9001/2026"
        assert case["bank_role"] == "Ответчик"
        fi = case["first_instance"]
        assert fi["link"] == "777|abcd-7777"
        assert fi["court_domain"] == gated.domain
        assert fi["filing_date"] == "05.07.2026"
        assert fi["judge"].startswith("Судьина")

    def test_srv_num_from_url_wins(self, env, monkeypatch):
        _mock_card(monkeypatch, _card_html(
            participants=(("ОТВЕТЧИК", "ПАО Сбербанк"),)))
        url = _court_url("kamyshlovsky--svd.sudrf.ru", srv=2)
        res, _, _ = _run_item(env, monkeypatch, url)
        assert res["status"] == ta.ST_ADDED_MAIN
        (case,) = _main_cases(env)
        assert case["first_instance"]["srv_num"] == 2

    def test_plaintiff_link_added_to_bank(self, env, monkeypatch):
        gated = _gated_court()
        _mock_card(monkeypatch, _card_html(
            participants=(("ИСТЕЦ", "ПАО Сбербанк"),
                          ("ОТВЕТЧИК", "Иванов Иван Иванович"))))
        res, _, _ = _run_item(
            env, monkeypatch, _court_url(gated.domain, srv=gated.srv_num))
        assert res["status"] == ta.ST_ADDED_BANK, res["line"]
        cases, _ = _bank_pair(env)
        assert cases[0]["bank_role"] == "Истец"

    def test_material_only_added_as_material(self, env, monkeypatch):
        gated = _gated_court()
        _mock_card(monkeypatch, _card_html(
            title="ДЕЛО № М-777/2026",
            participants=(("ОТВЕТЧИК", "ПАО Сбербанк"),)))
        res, _, _ = _run_item(
            env, monkeypatch, _court_url(gated.domain, srv=gated.srv_num))
        assert res["status"] == ta.ST_ADDED_MAIN
        (case,) = _main_cases(env)
        assert case["id"] == "М-777/2026"

    def test_appeal_link_refused(self, env, monkeypatch):
        _forbid_card(monkeypatch)
        appeal = _region().appeal_courts[0]
        res, _, _ = _run_item(
            env, monkeypatch,
            _court_url(appeal.domain, delo_id=appeal.delo_id))
        assert res["status"] == ta.ST_REFUSED
        assert "апелляци" in res["line"]

    def test_card_down_fetch_error(self, env, monkeypatch):
        gated = _gated_court()
        _mock_card(monkeypatch, "")
        res, _, _ = _run_item(
            env, monkeypatch, _court_url(gated.domain, srv=gated.srv_num))
        assert res["status"] == ta.ST_FETCH_ERROR


# ── Дедуп и реактивация ──────────────────────────────────────────────────────

def _record(num, domain, *, stage="first_instance", archived_at="",
            track=None, events=None):
    rec = {
        "id": num, "current_stage": stage, "bank_role": "Ответчик",
        "plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
        "first_instance": {
            "case_number": num, "court_domain": domain,
            "court": "", "status": "Решено",
        },
    }
    if events is not None:
        rec["first_instance"]["events"] = events
    if archived_at:
        rec["archived_at"] = archived_at
    if track:
        rec["track"] = track
        rec["bank_role"] = "Истец"
    return rec


class TestDedupReactivation:
    def _search0(self, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _forbid_card(monkeypatch)  # дубли/архив не должны стоить HTTP карточки
        return court0

    def test_active_main_already(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        (env / "cases.json").write_text(json.dumps(
            {"version": 1,
             "cases": [_record("2-1001/2026", court0.domain)]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ALREADY
        assert "«основная»" in res["line"]
        assert saved == []

    def test_active_bank_already(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        (env / "cases_bank.json").write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, track="plaintiff_light")]},
            ensure_ascii=False), encoding="utf-8")
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ALREADY
        assert "«иски банка»" in res["line"]

    def test_same_number_other_court_not_blocked(self, env, monkeypatch):
        """Дедуп судо-зависимый: номер занят в другом суде — не дубль."""
        court0 = _open_courts()[0]
        other = _open_courts()[1]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        (env / "cases.json").write_text(json.dumps(
            {"version": 1, "cases": [_record("2-1001/2026", other.domain)]},
            ensure_ascii=False), encoding="utf-8")
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ADDED_MAIN, res["line"]

    def test_hot_archive_reactivated(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        (env / "cases_archive.json").write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, archived_at="2026-05-01")]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REACTIVATED, res["line"]
        (case,) = _main_cases(env)
        assert case["id"] == "2-1001/2026"
        assert "archived_at" not in case
        assert case["import"]["announced"] is True  # не «новый иск»
        # архивный файл ОБЯЗАН быть пересохранён (урок клонов 04–07.08.2026)
        archived = json.loads(
            (env / "cases_archive.json").read_text(encoding="utf-8"))
        assert archived["cases"] == []
        assert str(cm_config.JSON_ARCHIVE_PATH) in saved

    def test_cold_archive_reactivated(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        cold = env / "cases_archive_2025.json"
        cold.write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, archived_at="2025-03-01")]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REACTIVATED
        assert json.loads(cold.read_text(encoding="utf-8"))["cases"] == []
        assert str(cold) in saved
        assert _main_cases(env)[0]["id"] == "2-1001/2026"

    def test_bank_archive_reactivated_events_kept(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        events = [{"date": "01.07.2026", "text": "Решение вынесено"}]
        # Архив трека монолитом (inline events) — load_bank_json так умеет.
        (env / "cases_bank_archive.json").write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, track="plaintiff_light",
                archived_at="2026-06-01", events=events)]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REACTIVATED, res["line"]
        assert "«иски банка»" in res["line"]
        cases, ev_map = _bank_pair(env)
        assert cases[0]["id"] == "2-1001/2026"
        # события пережили переезд и попали в events-файл split-хранения
        assert ev_map[f"{court0.domain}|2-1001/2026"] == events
        # архив трека пересохранён пустым, счётчик для фронта обновлён
        arch = json.loads(
            (env / "cases_bank_archive.json").read_text(encoding="utf-8"))
        assert arch["cases"] == []
        bank_root = json.loads(
            (env / "cases_bank.json").read_text(encoding="utf-8"))
        assert bank_root["archived_count"] == 0

    def test_bank_cold_reactivated(self, env, monkeypatch):
        court0 = self._search0(monkeypatch)
        cold = env / "cases_bank_archive_2025.json"
        cold.write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, track="plaintiff_light",
                archived_at="2025-06-01", events=[])]},
            ensure_ascii=False), encoding="utf-8")
        res, _, _ = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REACTIVATED
        assert json.loads(cold.read_text(encoding="utf-8"))["cases"] == []
        cases, _ = _bank_pair(env)
        assert cases[0]["id"] == "2-1001/2026"

    def test_domainless_archive_match_refused(self, env, monkeypatch):
        """Архивная запись БЕЗ определённого суда (дело «с апелляции») матчится
        по номеру с любым судом — реактивировать её нельзя: номера не уникальны
        между судами, изъялась бы чужая запись. Отказ, архив не тронут."""
        self._search0(monkeypatch)
        rec = _record("2-1001/2026", "")
        rec["archived_at"] = "2026-05-01"
        (env / "cases_archive.json").write_text(json.dumps(
            {"version": 1, "cases": [rec]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_REFUSED, res["line"]
        assert "неоднозначно" in res["line"]
        assert saved == []
        archived = json.loads(
            (env / "cases_archive.json").read_text(encoding="utf-8"))
        assert len(archived["cases"]) == 1

    def test_active_and_archive_gives_already(self, env, monkeypatch):
        """Дело и в активных, и в архиве → «уже отслеживается», а не клон."""
        court0 = self._search0(monkeypatch)
        (env / "cases.json").write_text(json.dumps(
            {"version": 1, "cases": [_record("2-1001/2026", court0.domain)]},
            ensure_ascii=False), encoding="utf-8")
        (env / "cases_archive.json").write_text(json.dumps(
            {"version": 1, "cases": [_record(
                "2-1001/2026", court0.domain, archived_at="2026-05-01")]},
            ensure_ascii=False), encoding="utf-8")
        res, _, saved = _run_item(env, monkeypatch, "2-1001/2026")
        assert res["status"] == ta.ST_ALREADY
        assert saved == []
        archived = json.loads(
            (env / "cases_archive.json").read_text(encoding="utf-8"))
        assert len(archived["cases"]) == 1  # архив не тронут


class TestPromotion:
    def test_material_promoted(self, env, monkeypatch):
        """Ввод «2-1006/2026» при живой М-записи → промоушен, не дубль.
        Карточка не качается: промоушен решается до сети."""
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _forbid_card(monkeypatch)
        (env / "cases.json").write_text(json.dumps(
            {"version": 1, "cases": [_record("М-500/2026", court0.domain)]},
            ensure_ascii=False), encoding="utf-8")
        res, _, _ = _run_item(env, monkeypatch, "2-1006/2026")
        assert res["status"] == ta.ST_PROMOTED, res["line"]
        (case,) = _main_cases(env)
        assert case["id"] == "2-1006/2026"
        fi = case["first_instance"]
        assert fi["material_number"] == "М-500/2026"
        assert fi["srv_num"] == 2  # href_srv_num фикстуры
        assert fi["accepted_pending_emit"] is True

    def test_other_court_material_not_promoted(self, env, monkeypatch):
        court0 = _open_courts()[0]
        other = _open_courts()[1]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        (env / "cases.json").write_text(json.dumps(
            {"version": 1, "cases": [_record("М-500/2026", other.domain)]},
            ensure_ascii=False), encoding="utf-8")
        res, _, _ = _run_item(env, monkeypatch, "2-1006/2026")
        assert res["status"] == ta.ST_ADDED_MAIN
        cases = _main_cases(env)
        assert {c["id"] for c in cases} == {"2-1006/2026", "М-500/2026"}


# ── CLI: пачка, счётчики, коды выхода ────────────────────────────────────────

class TestBatchCLI:
    def _job(self, tmp_path, monkeypatch, payload):
        job = tmp_path / "job.json"
        job.write_text(json.dumps(payload, ensure_ascii=False),
                       encoding="utf-8")
        summary_path = tmp_path / "summary.json"
        monkeypatch.setenv("IMPORT_SUMMARY_PATH", str(summary_path))
        monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
        return job, summary_path

    def test_mixed_batch(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        job, summary_path = self._job(env, monkeypatch, {
            "items": ["2-1001/2026", "2-1001/2026",  # дубль ВНУТРИ пачки
                      "2-1002/2026", "мусорная строка"],
            "operator": "Тест",
        })
        assert cli.main(["--job", str(job)]) == cli.EXIT_OK
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["kind"] == "case"
        assert summary["items"] == 4
        assert summary["added_main"] == 1
        assert summary["already"] == 1   # второе вхождение того же дела
        assert summary["added_bank"] == 1
        assert summary["refused"] == 1
        assert len(summary["lines"]) == 4
        # счётчики сходятся с построчным отчётом
        assert sum(1 for ln in summary["lines"]
                   if ln.startswith("[ADDED]")) == 2
        assert sum(1 for ln in summary["lines"]
                   if ln.startswith("[ALREADY]")) == 1

    def test_refused_batch_is_done_not_failed(self, env, monkeypatch):
        _mock_search(monkeypatch)
        job, summary_path = self._job(env, monkeypatch, {
            "items": ["2-7777/2026"], "operator": "Тест"})
        assert cli.main(["--job", str(job)]) == cli.EXIT_OK
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["not_found"] == 1
        assert "error" not in summary

    def test_total_network_failure_exit4(self, env, monkeypatch):
        _mock_search(monkeypatch,
                     fail_domains=[c.domain for c in _open_courts()])
        job, summary_path = self._job(env, monkeypatch, {
            "items": ["2-1001/2026", "2-1002/2026"], "operator": "Тест"})
        assert cli.main(["--job", str(job)]) == cli.EXIT_NETWORK
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert "недоступны" in summary["error"]

    def test_bad_job_exit5(self, env, monkeypatch, tmp_path):
        job, _ = self._job(env, monkeypatch, {"items": []})
        assert cli.main(["--job", str(job)]) == cli.EXIT_BAD_JOB
        assert cli.main(["--job", str(tmp_path / "нет.json")]) == cli.EXIT_BAD_JOB

    def test_unknown_override_court_exit5(self, env, monkeypatch):
        job, _ = self._job(env, monkeypatch, {
            "items": ["2-1001/2026"],
            "court_domain": "neizvestny--xxx.sudrf.ru"})
        assert cli.main(["--job", str(job)]) == cli.EXIT_BAD_JOB

    def test_dry_run_saves_nothing(self, env, monkeypatch):
        court0 = _open_courts()[0]
        _mock_search(monkeypatch, result_domains=[court0.domain])
        _mock_card(monkeypatch, recent_fi_card_html())
        job, summary_path = self._job(env, monkeypatch, {
            "items": ["2-1001/2026"], "operator": "Тест"})
        assert cli.main(["--job", str(job), "--dry-run"]) == cli.EXIT_OK
        assert not (env / "cases.json").exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["added_main"] == 1 and summary["dry_run"] is True


# ── Проводка: workflow / Worker / админка ────────────────────────────────────

def _read(rel: str) -> str:
    with open(os.path.join(ROOT_DIR, rel), encoding="utf-8") as f:
        return f.read()


class TestWiring:
    def test_workflow_contract(self):
        yml = _read(".github/workflows/add_cases.yml")
        # один писатель data/ — общая concurrency-группа с прогоном/импортами
        assert "group: cases-data-write" in yml
        assert "cancel-in-progress: false" in yml
        # 45 мин: до 400 поисковых запросов худшего случая; смерть по таймауту
        # посреди Python теряла бы всю пачку
        assert "timeout-minutes: 45" in yml
        # без ретраев: один лежащий суд с ретраями взрывал бы худший случай
        assert 'FETCH_MAX_RETRIES: "1"' in yml
        assert "REGION: ${{ vars.REGION }}" in yml
        # мастер-выключатель трека исков банка доезжает до скрипта
        assert "BANK_TRACK: ${{ vars.BANK_TRACK || '1' }}" in yml
        # done — только при успешном коммите (упавший push ≠ «+N добавлено»)
        assert "COMMIT_OUTCOME: ${{ steps.commit.outcome }}" in yml
        # свой User-Agent (дефолтные UA режутся на workers.dev, ошибка 1010)
        assert "court-monitor-addcase" in yml
        assert "add_cases_targeted.py" in yml
        # inputs только через env, не через ${{ }} в run:
        assert "JOB_KEY: ${{ inputs.job_key }}" in yml
        assert "OPERATOR: ${{ inputs.operator }}" in yml
        run_blocks = yml.split("run: |")
        for block in run_blocks[1:]:
            assert "${{ inputs." not in block.split("- name:")[0], (
                "inputs нельзя интерполировать в run: — только через env")

    def test_worker_contract(self):
        js = _read("cloudflare-worker/worker.js")
        assert '"add_cases.yml"' in js
        assert "/admin/add-case" in js
        assert "/add-case-job" in js
        assert "import:case:" in js
        # обе роли могут диспатчить
        wf_block = js.split('"add_cases.yml"')[1][:300]
        assert '"owner"' in wf_block and '"operator"' in wf_block
        assert '"job_key"' in wf_block
        # светофор свежести дампов не бумпается точечным добавлением
        assert 'kind !== "case"' in js.replace("record.kind", "kind")
        # кап пачки
        assert "ADD_CASE_MAX_ITEMS" in js

    def test_admin_page_contract(self):
        js = _read("cloudflare-worker/admin_page.js")
        assert 'id="ac-card"' in js
        assert "ac-input" in js and "ac-send" in js and "ac-court" in js
        assert "/admin/add-case" in js
        assert "Добавить дела" in js
