# -*- coding: utf-8 -*-
"""Целевой дослинк awaiting_appeal ↔ апелляция (relink_awaiting_appeal).

Контекст: поиск апелляции по «Сбербанк» видит только первую страницу выдачи —
дела, зарегистрированные в апел-суде до появления в нашей базе (заведены
импортёром дампов), на стр. 1 не попадают никогда (три дела Урала,
дослинкованные вручную 17.07.2026). Дослинк делает точечный запрос по полю
«Номер дела в первой инстанции» (G2_CASE__CASE_NUMBER_ISS).

Покрывает:
- CourtConfig.search_by_fi_number_url — построение URL + запрет не-апелляции
- appeal_court_for_fi_domain — выбор апел-суда по домену суда 1-й инст.
- is_no_data_page — детект «Данных по запросу не обнаружено»
- relink_awaiting_appeal — отбор кандидатов, сверка номера по карточке
  (_bare_case_number), мягкая сверка суда, дедуп, штатный выход в
  appeal_new_cases_csv/appeal_fi_numbers

Сетевые функции мокаются monkeypatch'ем В МОДУЛЬ-ДОМ (runs) — см. правило
config.X в CLAUDE.md.
"""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import courts as cm_courts  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.parsing import is_no_data_page  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402

AP_SVD = CourtConfig("Свердловский областной суд", "oblsud--svd.sudrf.ru", 5, "appeal")
AP_YNAO = CourtConfig("Суд ЯНАО", "oblsud--ynao.sudrf.ru", 5, "appeal")
AP1 = cm_courts.APPEAL_COURTS[0]  # Суд ХМАО-Югры (активный регион тестов)


def _awaiting_case(num: str, court: str = "Сургутский городской суд",
                   domain: str = "surggor--hmao.sudrf.ru", **over) -> dict:
    case = {
        "id": num,
        "current_stage": "awaiting_appeal",
        "plaintiff": "Иванов И.И.",
        "defendant": "ПАО Сбербанк",
        "first_instance": {
            "case_number": num,
            "court": court,
            "court_domain": domain,
            "sent_to_appeal": True,
            "sent_to_appeal_date": "01.06.2026",
            "events": [],
        },
        "appeal": None,
    }
    case.update(over)
    return case


def _search_html(rows) -> str:
    """Минимальная страница выдачи апелляции: (номер, cid, cuid, суд 1 инст.)."""
    trs = "".join(
        f'<tr><td><a href="modules.php?name=sud_delo&srv_num=1&name_op=case'
        f'&case_id={cid}&case_uid={cuid}&delo_id=5&new=5">{num}</a></td>'
        f"<td>01.06.2026</td>"
        f"<td>КАТЕГОРИЯ: Иски о взыскании сумм по договору займа "
        f"ИСТЕЦ(ЗАЯВИТЕЛЬ): Иванов Иван Иванович "
        f"ОТВЕТЧИК: ПАО Сбербанк "
        f"Суд (наименование) первой инстанции: {court}</td>"
        f"<td>Судьина С.С.</td></tr>"
        for num, cid, cuid, court in rows
    )
    return (
        "<html><body><table>"
        "<tr><td>№ дела</td><td>Дата поступления</td>"
        "<td>Стороны</td><td>Судья</td></tr>"
        f"{trs}</table></body></html>"
    )


NO_DATA_HTML = "<html><body>Данных по запросу не обнаружено</body></html>"


class TestSearchByFiNumberUrl:
    def test_appeal_url_contains_fi_number_field(self):
        url = AP_SVD.search_by_fi_number_url("2-716/2025")
        assert url.startswith("https://oblsud--svd.sudrf.ru/modules.php")
        assert "G2_CASE__CASE_NUMBER_ISS=2-716%2F2025" in url
        assert "delo_id=5" in url and "new=5" in url
        assert "delo_table=g2_case" in url
        # Поле стороны в целевом запросе не участвует
        assert "G2_PARTS__NAMESS" not in url

    def test_non_appeal_court_raises(self):
        fi = CourtConfig("Сургутский городской суд", "surggor--hmao.sudrf.ru",
                         1540005, "first_instance")
        with pytest.raises(ValueError):
            fi.search_by_fi_number_url("2-716/2025")


class TestAppealCourtForFiDomain:
    def test_subject_suffix_matches(self, monkeypatch):
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP_SVD, AP_YNAO))
        assert cm_courts.appeal_court_for_fi_domain(
            "nadymsky--ynao.sudrf.ru") is AP_YNAO
        assert cm_courts.appeal_court_for_fi_domain(
            "verhisetsky--svd.sudrf.ru") is AP_SVD

    def test_unknown_suffix_falls_back_to_first(self, monkeypatch):
        """Кировградский «--cvd» (опечатка ГАС) → первый суд региона (облсуд)."""
        monkeypatch.setattr(cm_courts, "APPEAL_COURTS", (AP_SVD, AP_YNAO))
        assert cm_courts.appeal_court_for_fi_domain(
            "kirovgradsky--cvd.sudrf.ru") is AP_SVD
        assert cm_courts.appeal_court_for_fi_domain("") is AP_SVD
        assert cm_courts.appeal_court_for_fi_domain("нечто") is AP_SVD


class TestIsNoDataPage:
    def test_positive(self):
        assert is_no_data_page(NO_DATA_HTML)

    def test_negative(self):
        assert not is_no_data_page("")
        assert not is_no_data_page(_search_html([]))


class _Net:
    """Мок-сеть: фиксированные ответы + журнал запрошенных URL."""

    def __init__(self, search_html: str, card_fi: str = ""):
        self.search_html = search_html
        self.card_fi = card_fi
        self.search_urls: list[str] = []
        self.card_urls: list[str] = []

    def fetch_page(self, url, context=""):
        self.search_urls.append(url)
        return self.search_html

    def fetch_card_checked(self, url, context=""):
        self.card_urls.append(url)
        return "<html>карточка</html>"

    def parse_case_card(self, html, base_url):
        return {"Номер дела 1 инстанции": self.card_fi, "_table_count": 6}


@pytest.fixture
def net(monkeypatch):
    def _install(search_html, card_fi=""):
        n = _Net(search_html, card_fi)
        monkeypatch.setattr(cm_runs, "fetch_page", n.fetch_page)
        monkeypatch.setattr(cm_runs, "fetch_card_checked", n.fetch_card_checked)
        monkeypatch.setattr(cm_runs, "parse_case_card", n.parse_case_card)
        monkeypatch.setattr(cm_runs, "polite_delay", lambda *a, **k: None)
        return n
    return _install


ROW = ("33-9001/2026", "777888", "aaaa1111-bbbb-cccc-dddd-eeee22223333",
       "Сургутский городской суд")


class TestRelinkAwaitingAppeal:
    def test_happy_path_links_via_standard_route(self, net):
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        cases = [_awaiting_case("2-500/2026")]
        csv_existing: set = set()
        new_csv: list = []
        fi_nums: dict = {}
        found = cm_runs.relink_awaiting_appeal(cases, csv_existing, new_csv, fi_nums)
        assert found == 1
        assert len(new_csv) == 1
        assert new_csv[0]["Номер дела"] == "33-9001/2026"
        assert new_csv[0]["_appeal_domain"] == AP1.domain
        assert fi_nums == {(AP1.domain, "33-9001/2026"): "2-500/2026"}
        assert "33-9001/2026" in csv_existing
        # Запрос шёл по полю номера 1-й инст., а не по стороне
        assert "G2_CASE__CASE_NUMBER_ISS=2-500%2F2026" in n.search_urls[0]

    def test_hybrid_fi_number_on_card_matches_bare(self, net):
        """Карточка отдаёт гибрид «2-500/2026 (2-100/2025;)» — bare-сверка."""
        net(_search_html([ROW]), card_fi="2-500/2026 (2-100/2025;)")
        fi_nums: dict = {}
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-500/2026")], set(), [], fi_nums)
        assert found == 1
        # В lookup идёт номер С КАРТОЧКИ — как в обычном поиске апелляции
        assert fi_nums[(AP1.domain, "33-9001/2026")] == "2-500/2026 (2-100/2025;)"

    def test_substring_match_rejected_by_card(self, net):
        """Сервер ищет подстрокой: запрос «2-50/2026» вернул чужое дело
        (карточка говорит «2-500/2026») — не линкуем."""
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-50/2026")], set(), [], {})
        assert found == 0
        assert len(n.card_urls) == 1  # карточку проверили, но отвергли

    def test_other_fi_court_in_row_skipped_without_card_fetch(self, net):
        """Тот же номер 1-й инст. у другого суда субъекта — строка отсекается
        сверкой имени суда ещё до запроса карточки."""
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-500/2026", court="Урайский городской суд",
                            domain="uray--hmao.sudrf.ru")],
            set(), [], {})
        assert found == 0
        assert n.card_urls == []

    def test_no_data_page_means_not_registered_yet(self, net):
        n = net(NO_DATA_HTML)
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-500/2026")], set(), [], {})
        assert found == 0
        assert n.card_urls == []

    def test_non_candidates_do_not_hit_network(self, net):
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        not_sent = _awaiting_case("2-1/2026")
        not_sent["first_instance"]["sent_to_appeal"] = False
        wrong_stage = _awaiting_case("2-2/2026", current_stage="first_instance")
        has_appeal = _awaiting_case("2-3/2026")
        has_appeal["appeal"] = {"case_number": "33-1/2026", "events": []}
        found = cm_runs.relink_awaiting_appeal(
            [not_sent, wrong_stage, has_appeal], set(), [], {})
        assert found == 0
        assert n.search_urls == []

    def test_already_tracked_appeal_number_skipped(self, net):
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-500/2026")], {"33-9001/2026"}, [], {})
        assert found == 0
        assert n.card_urls == []

    def test_found_by_page_one_search_this_run_not_duplicated(self, net):
        """Апелляция только что найдена обычным поиском (стр. 1) — дослинк
        её не задваивает."""
        n = net(_search_html([ROW]), card_fi="2-500/2026")
        new_csv = [{"Номер дела": "33-9001/2026", "_appeal_domain": AP1.domain}]
        found = cm_runs.relink_awaiting_appeal(
            [_awaiting_case("2-500/2026")], set(), new_csv, {})
        assert found == 0
        assert len(new_csv) == 1
        assert n.card_urls == []
