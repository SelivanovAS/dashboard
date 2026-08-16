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
import sys

import pytest
import requests

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

from court_monitor import config, netutil  # noqa: E402
from court_monitor.netutil import (  # noqa: E402
    block_page_marks, fetch_card_checked, fetch_fail_reason_ru, fetch_page,
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
    for k in config.METRICS:
        config.METRICS[k] = 0
    monkeypatch.setattr(config, "FETCH_MAX_RETRIES", 1)
    monkeypatch.setattr(netutil.time, "sleep", lambda *_: None)
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
        assert config.FETCH_DIAG["kind"] == "network"
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
        assert config.FETCH_DIAG["kind"] == "blocked"
        assert config.FETCH_DIAG["ip"] == "43.245.226.66"
        assert config.METRICS["cards_blocked"] == 1

    def test_captcha(self, monkeypatch):
        _serve(monkeypatch, 200, CAPTCHA_PAGE)
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "captcha"

    def test_breaker_skip_does_not_inherit_previous_diag(self, monkeypatch):
        """Открытый предохранитель пропускает карточку БЕЗ запроса. Без своего
        диагноза отчёт назвал бы причиной ответ предыдущей карточки."""
        _serve(monkeypatch, 200, BLOCK_PAGE)
        for _ in range(config.CARD_BREAKER_THRESHOLD):
            fetch_card_checked(CARD_URL)
        assert config.FETCH_DIAG["kind"] == "blocked"
        assert fetch_card_checked(CARD_URL) == ""
        assert config.FETCH_DIAG["kind"] == "breaker"


class TestReasonRu:
    @pytest.mark.parametrize("diag, expected", [
        ({"kind": "http_403"}, "суд отвечает HTTP 403 — адрес заблокирован"),
        ({"kind": "http_502"}, "суд отвечает HTTP 502"),
        ({"kind": "captcha"}, "карточка закрыта проверочным кодом"),
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
