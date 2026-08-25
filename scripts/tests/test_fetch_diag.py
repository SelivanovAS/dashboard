#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Класс сетевого отказа: config.FETCH_DIAG + человеческая формулировка.

До 16.08.2026 отказ карточки был безымянным: 403, страница защиты ГАС
«Правосудие» с HTTP 200, проверочный код, заглушка портала и таймаут давали
оператору одну строку «карточка не прочиталась», а HTTP-код вдобавок съедал
`raise_for_status` внутри fetch_page. В тот день три импорта Свердловской
области не прочитали ни одной карточки, и понять «нас блокируют по адресу» или
«портал лёг» было нечем — а от этого зависит, сможет ли вообще работать
основной прогон.
"""
from __future__ import annotations

import os
import socket
import sys

import pytest
import requests
from urllib3 import exceptions as urllib3_exc

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

from court_monitor import config, netutil  # noqa: E402
from court_monitor.netutil import (  # noqa: E402
    block_page_marks, fetch_card_checked, fetch_fail_reason_ru, fetch_page,
)
from court_monitor.parsing import (  # noqa: E402
    classify_non_card_page, classify_outage_page,
)
from fixture_dates import recent_fi_card_html  # noqa: E402
from probe_court_access import (  # noqa: E402
    BLOCKED, CAPTCHA, EMPTY, FAIL, OK, OUTAGE, classify_response,
)

CARD_URL = ("https://leninskyeka--svd.sudrf.ru/modules.php?name=sud_delo"
            "&srv_num=1&name_op=case&case_id=1&case_uid=u&delo_id=1540005&new=0")

# Настоящее тело страницы защиты ГАС (снято 16.08.2026 с боевого суда): HTTP
# 200 или 403, ноль таблиц, в тексте — наш адрес, страна и буква правила.
BLOCK_PAGE = (
    "<HTML><HEAD><TITLE>ГАС «Правосудие»</TITLE></HEAD><BODY>"
    "<h3>Этот запрос заблокирован по соображениям безопасности (G)</h3>"
    "ip: 43.245.226.66 Host: leninskyeka--svd.sudrf.ru , /modules.php Australia"
    "</BODY></HTML>"
)
OUTAGE_PAGE = ("<html><body>Информация временно недоступна. "
               "Приносим свои извинения.</body></html>")
CAPTCHA_PAGE = "<html><body>Введите проверочный код с картинки</body></html>"


class _Resp:
    def __init__(self, status: int, text: str):
        self.status_code = status
        self.text = text
        self.content = text.encode("windows-1251", errors="replace")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error",
                                     response=self)


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    config.FETCH_DIAG.clear()
    config.CARD_BREAKER.clear()
    config.FETCH_TIMINGS.clear()
    config.FETCH_FAIL_KINDS.clear()
    config.FETCH_FAIL_TIMINGS.clear()
    for k in config.METRICS:
        config.METRICS[k] = 0
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    monkeypatch.setattr(config, "CARD_BREAKER_MODE", "count")
    monkeypatch.setattr(netutil.time, "sleep", lambda *_: None)
    netutil.start_run_deadline(0)
    yield
    config.FETCH_DIAG.clear()
    config.CARD_BREAKER.clear()


def _serve(monkeypatch, status: int, body: str):
    monkeypatch.setattr(netutil.session, "get",
                        lambda url, timeout=30: _Resp(status, body))


class TestBlockPageMarks:
    def test_ip_and_rule_extracted(self):
        marks = block_page_marks(BLOCK_PAGE)
        assert marks["ip"] == "43.245.226.66"
        assert marks["rule"] == "G"

    def test_ordinary_page_has_no_marks(self):
        assert block_page_marks("<html><table></table></html>") == {}
        assert block_page_marks("") == {}


class TestFetchPageDiag:
    def test_http_403_keeps_code_and_marks(self, monkeypatch):
        """403 — единственное место, где код вообще виден: наружу
        raise_for_status отдаёт только исключение."""
        _serve(monkeypatch, 403, BLOCK_PAGE)
        assert fetch_page(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "http_403"
        assert config.FETCH_DIAG["status"] == 403
        assert config.FETCH_DIAG["ip"] == "43.245.226.66"

    def test_network_error_without_response(self, monkeypatch):
        def boom(url, timeout=30):
            raise requests.ConnectionError("нет соединения")
        monkeypatch.setattr(netutil.session, "get", boom)
        assert fetch_page(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "connection_error"
        assert config.FETCH_DIAG["status"] is None

    def test_success_marks_ok(self, monkeypatch):
        _serve(monkeypatch, 200, "<html><table></table></html>")
        assert fetch_page(CARD_URL)
        assert config.FETCH_DIAG["kind"] == "ok"


class TestFetchCardCheckedDiag:
    def test_block_page_with_http_200(self, monkeypatch):
        """Внешне успех: код 200, отличает только тело."""
        _serve(monkeypatch, 200, BLOCK_PAGE)
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "waf_block"
        assert config.FETCH_DIAG["ip"] == "43.245.226.66"
        assert config.METRICS["cards_blocked"] == 1

    def test_captcha(self, monkeypatch):
        _serve(monkeypatch, 200, CAPTCHA_PAGE)
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "captcha_card"

    def test_breaker_skip_does_not_inherit_previous_diag(self, monkeypatch):
        """Открытый предохранитель пропускает карточку БЕЗ запроса. Без своего
        диагноза отчёт назвал бы причиной ответ предыдущей карточки."""
        _serve(monkeypatch, 200, BLOCK_PAGE)
        for _ in range(config.CARD_BREAKER_THRESHOLD):
            fetch_card_checked(CARD_URL)
        assert config.FETCH_DIAG["kind"] == "waf_block"
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "breaker"


class TestReasonRu:
    @pytest.mark.parametrize("diag, expected", [
        ({"kind": "http_403"}, "суд отвечает HTTP 403 — адрес заблокирован"),
        ({"kind": "http_502"}, "суд отвечает HTTP 502"),
        ({"kind": "captcha"}, "карточка закрыта проверочным кодом"),
        ({"kind": "captcha_card"}, "карточка закрыта проверочным кодом"),
        ({"kind": "waf_block"},
         "суд заблокировал запрос (страница защиты ГАС)"),
        ({"kind": "portal_placeholder"},
         "вместо карточки пришла заглушка портала"),
        ({"kind": "empty_search"},
         "поиск суда вернул страницу без распознанных дел"),
        ({"kind": "unparsed_card"},
         "карточка суда не распознана парсером"),
        ({"kind": "breaker"},
         "суд снят с обхода после нескольких неудач подряд"),
        ({"kind": "network"}, "сеть недоступна или таймаут"),
    ])
    def test_wording(self, diag, expected):
        assert fetch_fail_reason_ru(diag) == expected

    def test_ip_appended(self):
        reason = fetch_fail_reason_ru({"kind": "blocked", "ip": "1.2.3.4"})
        assert reason.endswith("(наш адрес 1.2.3.4)")

    def test_unknown_kind_is_silent(self):
        """Неизвестный класс не выдумываем — вызыватель оставит своё."""
        assert fetch_fail_reason_ru({"kind": "ok"}) == ""
        assert fetch_fail_reason_ru({}) == ""

    def test_reads_live_diag_by_default(self, monkeypatch):
        _serve(monkeypatch, 403, BLOCK_PAGE)
        fetch_page(CARD_URL)
        assert "403" in fetch_fail_reason_ru()


class TestProbeClassification:
    """Проба и боевой код обязаны видеть одно и то же — правила общие."""

    @pytest.mark.parametrize("status, body, expected", [
        (403, BLOCK_PAGE, BLOCKED),
        (200, BLOCK_PAGE, BLOCKED),
        (200, OUTAGE_PAGE, OUTAGE),
        (200, CAPTCHA_PAGE, CAPTCHA),
        (None, "", FAIL),
        (500, "", FAIL),
    ])
    def test_verdicts(self, status, body, expected):
        assert classify_response(status, body, CARD_URL)[0] == expected

    def test_real_card_is_ok(self):
        assert classify_response(200, recent_fi_card_html(), CARD_URL)[0] == OK

    def test_semantic_classifier_keeps_waf_and_portal_outage_separate(self):
        assert classify_outage_page(BLOCK_PAGE) == "waf_block"
        assert classify_outage_page(OUTAGE_PAGE) == "portal_placeholder"
        assert classify_non_card_page(BLOCK_PAGE, CARD_URL) == "waf_block"
        assert classify_non_card_page(OUTAGE_PAGE, CARD_URL) == (
            "portal_placeholder"
        )

    def test_unknown_service_page_is_not_misnamed_as_portal_outage(self):
        page = "<html><body>Служебная страница</body></html>"
        assert classify_non_card_page(page, CARD_URL) == "non_card_page"

    def test_page_without_tables_is_empty_shell(self):
        """Ни маркеров блока, ни капчи, ни таблиц — отдельный класс: так
        выглядит протухший сид карточки, а не отказ доступа."""
        page = ("<html><body><p>Данных по запросу не обнаружено</p>"
                "</body></html>")
        assert classify_response(200, page, CARD_URL)[0] == EMPTY

    def test_copyright_in_outage_footer_is_not_a_block(self):
        """БЛОК от ЗАГЛУШКИ отличает НАШ АДРЕС, а не любой mark:
        _BLOCK_RULE_RE матчит первую латинскую букву в скобках, и «(C) 2006»
        из футера обычной заглушки перекрашивал бы её в БЛОК — главный
        вердикт отчёта (ревью Fable 16.08.2026)."""
        page = OUTAGE_PAGE.replace(
            "</body>", "<footer>(C) 2006 ГАС Правосудие</footer></body>")
        assert classify_response(200, page, CARD_URL)[0] == OUTAGE


class TestFetchObservability:
    """Сколько шло и почему не вышло — иначе режимы отказа неразличимы.

    24.08.2026 за одно утро портал успел побывать в двух состояниях: отвечал
    за 26–58 с при таймауте 30 с, а с обеда стал отдавать заглушку с HTTP 200.
    Оба раза система знала только «запрос не удался», и разбирать пришлось
    вручную curl'ом — при том что лечатся они по-разному.
    """

    def test_success_records_timing_and_no_failure(self, monkeypatch):
        _serve(monkeypatch, 200, "<html><table></table></html>")
        assert fetch_page(CARD_URL)
        assert len(config.FETCH_TIMINGS) == 1
        assert config.FETCH_TIMINGS[0] >= 0
        assert config.FETCH_FAIL_KINDS == {}

    def test_failure_counted_under_the_diag_kind(self, monkeypatch):
        """Класс берётся из _set_diag — второй копии правил быть не должно."""
        _serve(monkeypatch, 403, BLOCK_PAGE)
        assert fetch_page(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "http_403"
        assert config.FETCH_FAIL_KINDS == {"http_403": 1}
        assert config.FETCH_TIMINGS == [], "у отказа времени ответа нет"

    def test_kinds_accumulate_across_requests(self, monkeypatch):
        """FETCH_DIAG живёт до следующего запроса — сводка обязана пережить
        весь обход, иначе считать нечего."""
        _serve(monkeypatch, 403, BLOCK_PAGE)
        fetch_page(CARD_URL)
        fetch_page(CARD_URL)

        def boom(url, timeout=30):
            raise requests.ConnectionError("нет соединения")
        monkeypatch.setattr(netutil.session, "get", boom)
        fetch_page(CARD_URL)
        assert config.FETCH_FAIL_KINDS == {
            "http_403": 2, "connection_error": 1,
        }

    def test_card_level_classes_counted_too(self, monkeypatch):
        """Заглушка портала приходит с HTTP 200 — на уровне fetch_page это
        успех, и без учёта карточных классов авария была бы невидима."""
        _serve(monkeypatch, 200, "<html>Информация временно недоступна</html>")
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_FAIL_KINDS.get("portal_placeholder") == 1

    def test_latency_summary_percentiles(self, monkeypatch):
        config.FETCH_TIMINGS.extend([1.0, 2.0, 3.0, 40.0, 50.0])
        s = netutil.fetch_latency_summary()
        assert s["n"] == 5
        assert s["p50"] == 3.0
        assert s["max"] == 50.0

    def test_nearest_rank_is_not_shifted_for_even_sample(self):
        config.FETCH_TIMINGS.extend([1.0, 2.0])
        s = netutil.fetch_latency_summary()
        assert s["p50"] == 1.0

    def test_failure_latency_is_recorded_by_exact_kind(self, monkeypatch):
        ticks = iter([10.0, 75.0])
        monkeypatch.setattr(netutil.time, "monotonic", lambda: next(ticks))

        def boom(url, timeout=30):
            raise requests.ReadTimeout("молчание суда")

        monkeypatch.setattr(netutil.session, "get", boom)
        assert fetch_page(CARD_URL) == ""
        assert config.FETCH_FAIL_KINDS == {"read_timeout": 1}
        assert netutil.fetch_failure_latency_summary() == {
            "read_timeout": {"n": 1, "p50": 65.0, "p90": 65.0, "max": 65.0}
        }

    @pytest.mark.parametrize("kind, elapsed, expected", [
        ("connection_reset", 0.2, True),
        ("connection_reset", 12.0, False),
        ("http_503", 1.0, True),
        ("read_timeout", 1.0, False),
        ("connect_timeout", 1.0, False),
        ("http_403", 0.1, False),
    ])
    def test_retry_policy_uses_kind_and_elapsed(
        self, monkeypatch, kind, elapsed, expected,
    ):
        monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 3)
        monkeypatch.setattr(config, "FETCH_RETRY_FAST_MAX_SECONDS", 5.0)
        assert netutil.should_retry_fetch(kind, elapsed, 1) is expected

    def test_fast_reset_retries_but_is_one_logical_result(
        self, monkeypatch,
    ):
        monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 3)
        monkeypatch.setattr(config, "FETCH_RETRY_FAST_MAX_SECONDS", 5.0)
        calls = {"n": 0}

        def flicker(url, timeout=30):
            calls["n"] += 1
            if calls["n"] == 1:
                raise requests.ConnectionError(ConnectionResetError(54, "reset"))
            return _Resp(200, "<html><table></table></html>")

        monkeypatch.setattr(netutil.session, "get", flicker)
        assert fetch_page(CARD_URL)
        assert calls["n"] == 2
        assert config.FETCH_FAIL_KINDS == {"connection_reset": 1}
        assert config.METRICS["requests_failed"] == 0
        assert config.METRICS["requests_retried"] == 1

    def test_read_timeout_never_retries_even_when_max_is_three(
        self, monkeypatch,
    ):
        monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 3)
        calls = {"n": 0}

        def slow_fail(url, timeout=30):
            calls["n"] += 1
            raise requests.ReadTimeout("slow court")

        monkeypatch.setattr(netutil.session, "get", slow_fail)
        assert fetch_page(CARD_URL) == ""
        assert calls["n"] == 1
        assert config.FETCH_FAIL_KINDS == {"read_timeout": 1}
        assert config.METRICS["requests_failed"] == 1

    def test_search_semantic_failure_reaches_public_breakdown(
        self, monkeypatch,
    ):
        _serve(monkeypatch, 200, OUTAGE_PAGE)
        assert fetch_page(CARD_URL)
        netutil.mark_last_fetch_semantic(
            "outage_search", CARD_URL, context="поиск суда"
        )
        assert config.FETCH_FAIL_KINDS == {"outage_search": 1}
        assert config.FETCH_DIAG["kind"] == "outage_search"
        assert netutil.fetch_failure_latency_summary()["outage_search"]["n"] == 1

    @pytest.mark.parametrize("exc, expected", [
        (requests.ReadTimeout(), "read_timeout"),
        (requests.ConnectTimeout(), "connect_timeout"),
        (requests.exceptions.SSLError(), "tls_error"),
        (requests.exceptions.ProxyError(), "proxy_error"),
        (requests.exceptions.TooManyRedirects(), "redirect_error"),
        (requests.exceptions.ChunkedEncodingError(), "response_error"),
        (requests.exceptions.ContentDecodingError(), "response_error"),
        (requests.ConnectionError(socket.gaierror(-2, "dns")), "dns_error"),
        (requests.ConnectionError(ConnectionResetError(54, "reset")),
         "connection_reset"),
        (requests.ConnectionError("other"), "connection_error"),
        (requests.Timeout(), "timeout"),
        (requests.RequestException(), "request_error"),
    ])
    def test_transport_failure_classes_are_exact(self, exc, expected):
        assert netutil.transport_fail_kind(exc) == expected

    def test_real_urllib3_dns_reason_is_not_lost(self):
        resolution = urllib3_exc.NameResolutionError(
            "court.test", None, socket.gaierror(-2, "Name or service not known")
        )
        wrapped = requests.ConnectionError(
            urllib3_exc.MaxRetryError(None, "/modules.php", resolution)
        )
        assert netutil.transport_fail_kind(wrapped) == "dns_error"

    def test_real_urllib3_reset_reason_gets_fast_retry_class(self):
        protocol = urllib3_exc.ProtocolError(
            "Connection aborted", ConnectionResetError(54, "reset")
        )
        wrapped = requests.ConnectionError(
            urllib3_exc.MaxRetryError(None, "/modules.php", protocol)
        )
        assert netutil.transport_fail_kind(wrapped) == "connection_reset"

    def test_chunked_response_wrapping_reset_keeps_reset_class(self):
        """Реальный requests часто заворачивает reset во время чтения body
        в ChunkedEncodingError; внешний класс не должен скрыть первопричину."""
        protocol = urllib3_exc.ProtocolError(
            "Response ended prematurely", ConnectionResetError(54, "reset")
        )
        wrapped = requests.exceptions.ChunkedEncodingError(protocol)
        assert netutil.transport_fail_kind(wrapped) == "connection_reset"

    def test_empty_http_body_is_a_failure_class(self, monkeypatch):
        _serve(monkeypatch, 200, "")
        assert fetch_page(CARD_URL) == ""
        assert config.FETCH_FAIL_KINDS == {"empty": 1}

    def test_latency_summary_empty_without_samples(self):
        assert netutil.fetch_latency_summary() == {}


class TestObservabilityWiring:
    """Числа обязаны доезжать до last_run — читателя (пульт, cloud_run_ok)
    у них два, и оба ходят туда, а не в METRICS."""

    def test_last_run_carries_latency_and_kinds(self):
        runs = _read_runs()
        for key in ('"latency": fetch_latency_summary()',
                    '"fail_kinds": dict(config.FETCH_FAIL_KINDS)',
                    '"breaker": _breaker',
                    '"courts_unavailable"', '"courts_with_unrequested"',
                    '"courts_outage"',
                    '"cards_breaker_recovered"',
                    '"cards_breaker_unrequested"',
                    '"cards_unreachable"', '"cards_unread_other"'):
            assert key in runs, key

    def test_timeout_has_env_lever(self):
        """24.08.2026 таймаут был литералом, и покрутить его в аварийное утро
        можно было только правкой кода с коммитом."""
        cfg = _read_config()
        assert 'FETCH_TIMEOUT_CONNECT = float(os.environ.get(' in cfg
        assert 'FETCH_TIMEOUT_READ = float(os.environ.get(' in cfg
        net = _read_netutil()
        assert "config.FETCH_TIMEOUT_CONNECT" in net
        assert "config.FETCH_TIMEOUT_READ" in net
        assert "timeout=(10, 65)" not in net, "литерал вернулся вместо рычага"

    def test_run_deadline_stops_before_http_without_blame(self, monkeypatch):
        calls = {"n": 0}

        def should_not_run(*_args, **_kwargs):
            calls["n"] += 1
            raise AssertionError("HTTP начался после общего дедлайна")

        monkeypatch.setattr(netutil.session, "get", should_not_run)
        monkeypatch.setattr(netutil.time, "monotonic", lambda: 100.0)
        netutil._RUN_DEADLINE_AT = 99.0
        assert fetch_card_checked(CARD_URL, breaker_gate=False) == ""
        assert calls["n"] == 0
        assert config.FETCH_DIAG["kind"] == "run_deadline"
        assert config.FETCH_FAIL_KINDS == {}
        assert config.CARD_BREAKER == {}

    def test_run_deadline_is_wired_to_full_run(self):
        cfg = _read_config()
        net = _read_netutil()
        runs = _read_runs()
        assert 'RUN_DEADLINE_SECONDS = float(os.environ.get(' in cfg
        main_json = runs[runs.index("def main_json():"):]
        main_json = main_json[:main_json.index("\ndef main_replay_last(")]
        assert "start_run_deadline()" in main_json
        assert "run_deadline_reached()" in net
        assert 'status=("deadline_reached"' in runs


def _read_repo(rel: str) -> str:
    with open(os.path.join(os.path.dirname(SCRIPTS_DIR), rel),
              encoding="utf-8") as f:
        return f.read()


def _read_runs() -> str:
    return _read_repo("scripts/court_monitor/runs.py")


def _read_config() -> str:
    return _read_repo("scripts/court_monitor/config.py")


def _read_netutil() -> str:
    return _read_repo("scripts/court_monitor/netutil.py")
