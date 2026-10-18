# -*- coding: utf-8 -*-
"""Тесты наблюдаемости прогона (улучшения логирования 13.07.2026)."""

from __future__ import annotations

import logging
import os
import re
import sys
import types
from types import SimpleNamespace

import pytest
import requests

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config as cm_config  # noqa: E402
from court_monitor import delivery as cm_delivery  # noqa: E402
from court_monitor import ghlog  # noqa: E402
from court_monitor import netutil as cm_netutil  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.digest import llm as cm_llm  # noqa: E402
from court_monitor.textutil import shorten_court_name  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_state():
    """Групповой флаг ghlog и METRICS — глобальное состояние процесса."""
    ghlog._group_open = False
    cm_config._metrics_reset()
    yield
    ghlog._group_open = False
    cm_config._metrics_reset()


# ── fetch_page: контекст в сетевых ошибках ───────────────────────────────────

class TestFetchPageContext:
    @pytest.fixture(autouse=True)
    def _network_stub(self, monkeypatch):
        monkeypatch.setattr(cm_config, "FETCH_MAX_RETRIES", 2)
        monkeypatch.setattr(cm_netutil.time, "sleep", lambda s: None)
        monkeypatch.setattr(
            cm_netutil.session, "get",
            lambda *a, **k: (_ for _ in ()).throw(
                requests.ConnectionError(
                    "соединение сброшено",
                    ConnectionResetError(54, "Connection reset by peer"),
                )
            ),
        )

    def test_context_in_retry_warning_and_final_error(self, caplog):
        url = "https://surggor--hmao.sudrf.ru/case?id=1"
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            html = cm_netutil.fetch_page(
                url, context="2-716/2025, Сургутский горсуд"
            )
        assert html == ""
        warn = next(r for r in caplog.records if r.levelno == logging.WARNING)
        assert "surggor--hmao.sudrf.ru (2-716/2025, Сургутский горсуд)" in warn.getMessage()
        err = next(r for r in caplog.records if r.levelno == logging.ERROR)
        assert url in err.getMessage()
        assert "(2-716/2025, Сургутский горсуд)" in err.getMessage()
        assert cm_config.METRICS["requests_failed"] == 1

    def test_without_context_no_parens(self, caplog):
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            cm_netutil.fetch_page("https://oblsud.hmao.sudrf.ru/search")
        warn = next(r for r in caplog.records if r.levelno == logging.WARNING)
        assert "oblsud.hmao.sudrf.ru — connection_reset" in warn.getMessage()


# ── строки-балансы очередей парсинга ─────────────────────────────────────────

class TestQueueBalanceLines:
    """Баланс одной строкой: «<субъект> N → парсим X (a; b)», X + слагаемые = N.

    Формат введён 15.07.2026 после вопросов юриста «входят ли 18 в 39?» —
    все группы очереди теперь видны в одной строке, нулевые не печатаются.
    """

    # Мини-якорь KEY_RE из ops/mac-local-run/progress_pusher.py: строка-баланс
    # должна содержать триггер, иначе выпадет из вех «🛰 Парсинг» админки.
    PUSHER_KEY_ANCHOR = re.compile(r"Апелляция: |1 инст: |Кассац|7kas")

    def test_no_parts_no_parens(self):
        out = cm_runs._format_queue_balance("Апелляция: активных дел", 5, 5, [])
        assert out == "Апелляция: активных дел 5 → парсим 5"

    def test_parts_joined_semicolon(self):
        out = cm_runs._format_queue_balance(
            "1 инст: дел со стадией 1-й инстанции", 109, 70,
            ["20 отложено — заседание в будущем",
             "19 «третье лицо» не парсим — ждём кассацию на 7kas"],
        )
        assert out == (
            "1 инст: дел со стадией 1-й инстанции 109 → парсим 70 "
            "(20 отложено — заседание в будущем; "
            "19 «третье лицо» не парсим — ждём кассацию на 7kas)"
        )
        assert self.PUSHER_KEY_ANCHOR.search(out)

    def test_appeal_balance_arithmetic(self, monkeypatch, caplog):
        # 3 активных дела: 1 парсим + 1 отложено (будущее заседание)
        # + 1 уже прошло апелляцию (skip_apel_nums) = 3.
        monkeypatch.setattr(cm_runs, "polite_delay", lambda: None)
        monkeypatch.setattr(cm_runs, "fetch_page", lambda *a, **k: "")
        monkeypatch.setattr(cm_runs, "load_digested_acts", lambda: set())
        # Без этого тест перезапишет боевой data/.digested_acts —
        # update_active_cases сохраняет дедуп-набор в конце прогона.
        monkeypatch.setattr(cm_runs, "save_digested_acts", lambda acts: None)
        monkeypatch.setattr(
            cm_runs, "should_skip_case",
            lambda shim, today, **kw: (
                (True, "future_hearing_31.12.2099")
                if (shim.get("appeal") or {}).get("_skip") else (False, "")
            ),
        )
        cases = [
            {"Номер дела": "33-1/2026", "Ссылка": ""},
            {"Номер дела": "33-2/2026", "Ссылка": ""},
            {"Номер дела": "33-3/2026", "Ссылка": ""},
        ]
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            _, _, stats = cm_runs.update_active_cases(
                cases,
                json_appeal_by_num={"33-2/2026": {"_skip": True}},
                skip_apel_nums={"33-3/2026"},
            )
        assert (
            "Апелляция: активных дел 3 → парсим 1 "
            "(1 отложено — заседание в будущем; "
            "1 не парсим — апелляция уже пройдена)"
        ) in caplog.text
        # Числа согласованы: план (после smart-skip) и очередь итерации.
        assert stats["planned"] == 1
        assert stats["total"] == 2

    def test_progress_line_uses_plan_denominator(self, monkeypatch, caplog):
        """Прогресс «проверено X из Y» — в единицах ПЛАНА: скипнутые
        smart-skip'ом дела не считаются ни в числителе, ни в знаменателе.
        Раньше лог писал «парсим 40», а потом «проверено 20 из 45»
        (знаменатель включал отложенных; разбор 12.08.2026)."""
        monkeypatch.setattr(cm_runs, "polite_delay", lambda: None)
        monkeypatch.setattr(cm_runs, "load_digested_acts", lambda: set())
        monkeypatch.setattr(cm_runs, "save_digested_acts", lambda acts: None)
        monkeypatch.setattr(
            cm_runs, "should_skip_case",
            lambda shim, today, **kw: (
                (True, "future_hearing_31.12.2099")
                if (shim.get("appeal") or {}).get("_skip") else (False, "")
            ),
        )
        # 25 дел: 4 отложено, 21 в плане; пустая «Ссылка» — дальше цикл не
        # ходит (HTTP нет), но «проверено» уже посчитано.
        cases = [{"Номер дела": f"33-{i}/2026", "Ссылка": ""}
                 for i in range(25)]
        json_map = {f"33-{i}/2026": {"_skip": i < 4} for i in range(25)}
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_runs.update_active_cases(cases, json_appeal_by_num=json_map)
        assert "Апелляция: проверено 20 из 21 (изменений 0)" in caplog.text


# ── METRICS: инкременты Web Push ─────────────────────────────────────────────

class _FakeWebPushException(Exception):
    """Мини-двойник pywebpush.WebPushException (response опционален)."""

    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class TestWebPushMetrics:
    def test_sent_and_failed_counted(self, monkeypatch):
        calls = {"n": 0}

        def fake_webpush(**kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise _FakeWebPushException("push-сервис вернул 500")

        fake_pywebpush = types.ModuleType("pywebpush")
        fake_pywebpush.webpush = fake_webpush
        fake_pywebpush.WebPushException = _FakeWebPushException
        fake_py_vapid = types.ModuleType("py_vapid")
        fake_py_vapid.Vapid = SimpleNamespace(from_pem=lambda pem: object())
        monkeypatch.setitem(sys.modules, "pywebpush", fake_pywebpush)
        monkeypatch.setitem(sys.modules, "py_vapid", fake_py_vapid)

        monkeypatch.setattr(cm_config, "PUSH_WORKER_URL", "https://worker.test")
        monkeypatch.setattr(cm_config, "PUSH_SECRET", "secret")
        monkeypatch.setattr(cm_config, "VAPID_PRIVATE_KEY", "PEM")
        subs = [
            {"endpoint": "https://push.example/1"},
            {"endpoint": "https://push.example/2"},
            {"endpoint": "https://push.example/3"},
        ]
        monkeypatch.setattr(
            cm_delivery.requests, "get",
            lambda *a, **k: SimpleNamespace(ok=True, json=lambda: subs),
        )
        # Не пишем data/last_personal_pushes.json из теста.
        monkeypatch.setattr(cm_delivery, "save_json", lambda *a, **k: None)

        cm_delivery.send_web_push("Заголовок", "Текст")

        assert calls["n"] == 3
        assert cm_config.METRICS["push_sent"] == 2
        assert cm_config.METRICS["push_failed"] == 1
