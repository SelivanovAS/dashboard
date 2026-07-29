# -*- coding: utf-8 -*-
"""Тесты пер-суд предохранителя карточек (circuit breaker).

Аутейдж Сургутского городского 29.07.2026: суд отдавал заглушку на каждой
карточке, прогон впустую молотил polite_delay + HTTP по всем его делам.
Механизм (netutil.card_breaker_*): счётчик «N подряд не прочитанных
карточек» по хосту (заглушка/код/сеть) → суд отключается до конца прогона,
канарейка пре-открывает по заглушке на странице ПОИСКА, half-open пробы
возвращают ожившего в обход. Состояние — config.CARD_BREAKER (очистка
между тестами — autouse-фикстура conftest.py).
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
FIXTURES_DIR = os.path.join(TESTS_DIR, "fixtures")
sys.path.insert(0, SCRIPTS_DIR)

import update_cases as uc  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor import netutil  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.bank_report import (  # noqa: E402
    _OUTCOME_RU, BankParseReport, classify_fetch_failure, metrics_snapshot,
)


def _read_fixture(name: str) -> str:
    with open(os.path.join(FIXTURES_DIR, name), encoding="utf-8") as f:
        return f.read()


HOST = "surgut--hmao.sudrf.ru"
CARD_URL = (
    f"https://{HOST}/modules.php?name=sud_delo&srv_num=1&name_op=case"
    "&case_id=1&case_uid=a&delo_id=1540005&new=0"
)
# Заглушка с ≥2 фразами недоступности — строгая (URL-независимая) ветка.
OUTAGE_HTML = (
    "<html><body><p>Информация временно недоступна.</p>"
    "<p>Приносим свои извинения.</p></body></html>"
)


class _Net:
    """Замоканная сеть: канённый ответ + счётчик реальных «HTTP»-вызовов."""

    def __init__(self, html: str):
        self.html = html
        self.calls = 0

    def fetch(self, url, context=None):
        self.calls += 1
        return self.html


@pytest.fixture
def net(monkeypatch):
    n = _Net(OUTAGE_HTML)
    monkeypatch.setattr(netutil, "fetch_page", n.fetch)
    monkeypatch.setattr(cm_config, "CARD_BREAKER_THRESHOLD", 5)
    monkeypatch.setattr(cm_config, "CARD_BREAKER_PROBE_EVERY", 5)
    monkeypatch.setitem(cm_config.METRICS, "cards_breaker_skipped", 0)
    monkeypatch.setitem(cm_config.METRICS, "cards_blocked", 0)
    monkeypatch.setitem(cm_config.METRICS, "cards_captcha", 0)
    return n


class TestCardBreakerCore:
    """Ядро: счётчик «N подряд», отключение, сброс успехом."""

    def test_threshold_opens_and_skips_without_http(self, net, caplog):
        with caplog.at_level(logging.WARNING):
            for _ in range(5):
                assert netutil.fetch_card_checked(CARD_URL) == ""
        assert net.calls == 5
        assert netutil.card_breaker_open(HOST) is True
        assert any("обход приостановлен" in r.message for r in caplog.records)
        # 6-я карточка — без HTTP: гейт вернул "" сразу.
        assert netutil.fetch_card_checked(CARD_URL) == ""
        assert net.calls == 5
        assert cm_config.METRICS["cards_breaker_skipped"] == 1

    def test_success_resets_counter(self, net):
        """4 фейла → успех → 4 фейла: порог «подряд» не достигнут."""
        card_html = _read_fixture("case_card_with_act.html")
        for _ in range(4):
            netutil.fetch_card_checked(CARD_URL)
        net.html = card_html
        assert netutil.fetch_card_checked(CARD_URL) == card_html
        net.html = OUTAGE_HTML
        for _ in range(4):
            netutil.fetch_card_checked(CARD_URL)
        assert netutil.card_breaker_open(HOST) is False
        assert net.calls == 9  # все вызовы дошли до сети

    def test_network_and_captcha_count_too(self, net):
        """Сетевой фейл (пустой ответ) и проверочный код копят тот же счётчик."""
        net.html = ""  # сеть
        for _ in range(3):
            netutil.fetch_card_checked(CARD_URL)
        net.html = _read_fixture("search_captcha_challenge.html")  # код
        for _ in range(2):
            netutil.fetch_card_checked(CARD_URL)
        assert netutil.card_breaker_open(HOST) is True
        assert cm_config.CARD_BREAKER[HOST]["reason"] == "проверочный код"

    def test_hosts_independent(self, net):
        other_url = CARD_URL.replace(HOST, "hmankord--hmao.sudrf.ru")
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)
        assert netutil.card_breaker_open(HOST) is True
        # Соседний суд не отключён: его карточка идёт в сеть.
        netutil.fetch_card_checked(other_url)
        assert net.calls == 6
        assert netutil.card_breaker_open("hmankord--hmao.sudrf.ru") is False

    def test_threshold_zero_disables(self, net, monkeypatch):
        monkeypatch.setattr(cm_config, "CARD_BREAKER_THRESHOLD", 0)
        for _ in range(10):
            netutil.fetch_card_checked(CARD_URL)
        assert net.calls == 10
        assert netutil.card_breaker_open(HOST) is False
        assert cm_config.METRICS["cards_breaker_skipped"] == 0

    def test_metrics_reset_clears_state(self, net):
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)
        assert cm_config.CARD_BREAKER
        cm_config._metrics_reset()
        assert cm_config.CARD_BREAKER == {}


class TestCardBreakerHalfOpen:
    """Half-open пробы: каждая K-я пропущенная карточка идёт в сеть."""

    def test_probe_cadence_and_recovery(self, net):
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)  # 5 HTTP → открыт
        # Пропуски 1-4 — без HTTP; 5-й (K=5) — проба, заглушка держит открытым.
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)
        assert net.calls == 6
        assert netutil.card_breaker_open(HOST) is True
        # Суд ожил: следующая проба (ещё 5 вызовов) закрывает предохранитель…
        net.html = _read_fixture("case_card_with_act.html")
        results = [netutil.fetch_card_checked(CARD_URL) for _ in range(5)]
        assert results[:4] == ["", "", "", ""]
        assert results[4] == net.html  # проба вернула живую карточку
        assert netutil.card_breaker_open(HOST) is False
        # …и хвост суда дочитывается обычным порядком.
        assert netutil.fetch_card_checked(CARD_URL) == net.html
        assert net.calls == 8  # 5 фейлов + 2 пробы + 1 обычный

    def test_probe_every_zero_no_probes(self, net, monkeypatch):
        monkeypatch.setattr(cm_config, "CARD_BREAKER_PROBE_EVERY", 0)
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)
        for _ in range(40):
            assert netutil.fetch_card_checked(CARD_URL) == ""
        assert net.calls == 5
        assert cm_config.METRICS["cards_breaker_skipped"] == 40

    def test_breaker_gate_false_bypasses_gate(self, net):
        """Вызыватель с собственным пре-чеком (FI-цикл) не гейтится второй
        раз — иначе задвоение skipped ломало бы каденс проб."""
        for _ in range(5):
            netutil.fetch_card_checked(CARD_URL)
        skipped_before = cm_config.CARD_BREAKER[HOST]["skipped"]
        netutil.fetch_card_checked(CARD_URL, breaker_gate=False)
        assert net.calls == 6  # HTTP состоялся вопреки открытому предохранителю
        assert cm_config.CARD_BREAKER[HOST]["skipped"] == skipped_before


class TestCardBreakerCanary:
    """Канарейка: заглушка на странице ПОИСКА пре-открывает предохранитель."""

    def test_outage_detector_on_search_pages(self):
        """Строгая ветка детекта: заглушка — да; капча, «нет данных» и живая
        выдача — нет (капча на поиске — штатный режим капчёвых судов,
        их карточки живут и мониторятся)."""
        assert uc.looks_like_outage_page(OUTAGE_HTML) is True
        assert uc.looks_like_outage_page(
            _read_fixture("case_card_outage.html")) is True
        assert uc.looks_like_outage_page(
            _read_fixture("search_captcha_challenge.html")) is False
        assert uc.looks_like_outage_page(
            "<html><body>Данных по запросу не обнаружено</body></html>"
        ) is False
        assert uc.looks_like_outage_page(
            _read_fixture("search_page_normal.html")) is False
        assert uc.looks_like_outage_page("") is False

    def test_preopen_blocks_cards_and_probe_recovers(self, net):
        netutil.card_breaker_preopen(HOST, "заглушка на странице поиска")
        assert netutil.card_breaker_open(HOST) is True
        assert cm_config.CARD_BREAKER[HOST]["preopened"] is True
        # Ни одной потраченной карточки до первой пробы…
        net.html = _read_fixture("case_card_with_act.html")
        for _ in range(4):
            assert netutil.fetch_card_checked(CARD_URL) == ""
        assert net.calls == 0
        # …а проба (карточки живы вопреки канарейке) возвращает суд в обход.
        assert netutil.fetch_card_checked(CARD_URL) == net.html
        assert netutil.card_breaker_open(HOST) is False

    def test_preopen_disabled_by_threshold_zero(self, net, monkeypatch):
        monkeypatch.setattr(cm_config, "CARD_BREAKER_THRESHOLD", 0)
        netutil.card_breaker_preopen(HOST, "заглушка на странице поиска")
        assert netutil.card_breaker_open(HOST) is False
        netutil.fetch_card_checked(CARD_URL)
        assert net.calls == 1


class TestCardBreakerReporting:
    """Отчётность: bank_report, classify_fetch_failure, 🩺-алерты 4e."""

    def test_classify_fetch_failure_court_breaker(self, monkeypatch):
        monkeypatch.setitem(cm_config.METRICS, "cards_breaker_skipped", 0)
        before = metrics_snapshot()
        cm_config.METRICS["cards_breaker_skipped"] += 1
        assert classify_fetch_failure(before) == "court_breaker"

    def test_outcome_ru_and_totals(self):
        assert "предохранител" in _OUTCOME_RU["court_breaker"]
        rep = BankParseReport()
        totals = rep.totals(rows=[{"outcome": "court_breaker"}])
        assert totals["failed"] == 1

    def test_alert_lines(self):
        cm_config.CARD_BREAKER.clear()
        cm_config.CARD_BREAKER["a--hmao.sudrf.ru"] = {
            "fails": 5, "open": True, "reason": "заглушка/блок портала",
            "skipped": 40, "probes": 1, "preopened": False,
        }
        cm_config.CARD_BREAKER["b--hmao.sudrf.ru"] = {
            "fails": 0, "open": False, "reason": "сеть/пустой ответ",
            "skipped": 10, "probes": 2, "preopened": False,
        }
        # Копил фейлы, но порога не достиг — в алерты не попадает.
        cm_config.CARD_BREAKER["c--hmao.sudrf.ru"] = {
            "fails": 2, "open": False, "reason": "сеть/пустой ответ",
            "skipped": 0, "probes": 0, "preopened": False,
        }
        lines = cm_runs._card_breaker_alert_lines(
            {"a--hmao.sudrf.ru": "Сургутский городской суд"}
        )
        assert len(lines) == 2
        assert "Сургутский городской суд" in lines[0]
        assert "снят с обхода" in lines[0]
        assert "пропущено 40" in lines[0]
        # Незнакомый хост печатается как есть; закрывшийся — «возобновлён».
        assert "b--hmao.sudrf.ru" in lines[1]
        assert "возобновлён" in lines[1]


class TestCardBreakerWiring:
    """Проводка предохранителя в runs.py — unit-тест на исходник вместо
    тяжёлого e2e (по образцу TestBankTrackWiring/TestFiTerminationWiring).

    Load-bearing: пре-чек FI-цикла стоит ПОСЛЕ smart-skip и ДО polite_delay
    (иначе сотни итераций отключённого суда жгут по 2-3 с задержки), а fetch
    после пре-чека идёт с breaker_gate=False (иначе гейт зовётся дважды и
    каденс half-open проб ломается).
    """

    @staticmethod
    def _runs_src():
        path = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_fi_cycle_gate_before_delay_then_ungated_fetch(self):
        src = self._runs_src()
        i_skip = src.index('bank_report.record(case_j, "skip"')
        i_gate = src.index("if not card_breaker_allows(court_cfg.domain):")
        i_delay = src.index("polite_delay()", i_gate)
        i_fetch = src.index("breaker_gate=False", i_gate)
        assert i_skip < i_gate < i_delay < i_fetch
        # Пропуск учитывается в отчёте bank-трека.
        gate_block = src[i_gate:i_delay]
        assert 'bank_report.record(case_j, "court_breaker")' in gate_block

    def test_appeal_cycle_gate_wired(self):
        src = self._runs_src()
        i_gate = src.index("if not card_breaker_allows(_ap_court.domain):")
        i_delay = src.index("polite_delay()", i_gate)
        i_fetch = src.index("breaker_gate=False", i_gate)
        assert i_gate < i_delay < i_fetch

    def test_search_canaries_wired_for_all_sources(self):
        """Пре-открытие по заглушке на поиске — во всех трёх фазах:
        суды 1-й инст., апелляция, кассация 7kas."""
        src = self._runs_src()
        assert "card_breaker_preopen(court.domain" in src
        assert "card_breaker_preopen(_ap_court.domain" in src
        assert "CASSATION_COURT.domain, \"заглушка на странице поиска\"" in src

    def test_alert_lines_wired_into_4e(self):
        src = self._runs_src()
        i_blocked = src.index("карточек не прочитано: ")
        i_breaker = src.index("_card_breaker_alert_lines(_breaker_names)")
        i_send = src.index("🩺 <b>Мониторинг парсеров</b>")
        assert i_blocked < i_breaker < i_send
