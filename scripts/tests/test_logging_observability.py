# -*- coding: utf-8 -*-
"""Тесты наблюдаемости прогона (улучшения логирования 13.07.2026).

Покрывают:
- ghlog          — экранирование, группы ::group::/::endgroup::, аннотации
                   ::warning::/::error::, гейт LOG_GH_ANNOTATIONS
- log_phase      — контракт с progress_pusher (строка «— [N/9] …» без изменений)
- fetch_page     — context «суд/дело» в WARNING ретрая и финальном ERROR
- _format_slow_courts — топ медленных судов фазы 5
- log_run_summary — опциональные строки (огрызки/Web Push/LLM-пересказы)
- METRICS        — инкременты push_sent/push_failed и llm_summary_*

Запуск: python3 -m pytest scripts/tests/test_logging_observability.py -v
"""

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


# ── ghlog: экранирование ─────────────────────────────────────────────────────

class TestGhEscape:
    def test_percent_escaped_first(self):
        # Порядок замен: сначала %, иначе %0A перевода строки двоится.
        assert ghlog.gh_escape("50%\r\nконец") == "50%25%0D%0Aконец"

    def test_plain_text_untouched(self):
        assert ghlog.gh_escape("обычная строка") == "обычная строка"

    def test_long_message_truncated(self):
        out = ghlog.gh_escape("x" * 2000)
        assert out.endswith("…")
        assert len(out) == ghlog._ANNOTATION_MAX_LEN + 1


# ── ghlog: группы ────────────────────────────────────────────────────────────

class TestGroups:
    def test_start_closes_previous_group(self, monkeypatch, capsys):
        monkeypatch.setenv("LOG_GH_ANNOTATIONS", "1")
        ghlog.start_group("Фаза 1")
        ghlog.start_group("Фаза 2")
        out = capsys.readouterr().out
        assert out == "::group::Фаза 1\n::endgroup::\n::group::Фаза 2\n"

    def test_end_group_idempotent(self, monkeypatch, capsys):
        monkeypatch.setenv("LOG_GH_ANNOTATIONS", "1")
        ghlog.start_group("Фаза")
        ghlog.end_group()
        ghlog.end_group()  # повторное закрытие — no-op
        out = capsys.readouterr().out
        assert out.count("::endgroup::") == 1

    def test_disabled_without_env(self, monkeypatch, capsys):
        monkeypatch.delenv("LOG_GH_ANNOTATIONS", raising=False)
        ghlog.start_group("Фаза")
        ghlog.end_group()
        assert capsys.readouterr().out == ""
        assert ghlog._group_open is False


# ── ghlog: аннотации ─────────────────────────────────────────────────────────

class TestAnnotationHandler:
    def _fresh_logger(self, name: str) -> logging.Logger:
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = False  # не шумим в root во время теста
        lg.setLevel(logging.DEBUG)
        return lg

    def test_warning_and_error_become_annotations(self, monkeypatch, capsys):
        monkeypatch.setenv("LOG_GH_ANNOTATIONS", "1")
        lg = self._fresh_logger("ghlog-test-annotations")
        ghlog.install(lg)
        lg.warning("суд не отвечает: 50%")
        lg.error("карточка не загрузилась")
        lg.info("info-строка аннотацией не становится")
        out = capsys.readouterr().out
        assert "::warning::суд не отвечает: 50%25" in out
        assert "::error::карточка не загрузилась" in out
        assert "info-строка" not in out

    def test_install_noop_without_env(self, monkeypatch):
        monkeypatch.delenv("LOG_GH_ANNOTATIONS", raising=False)
        lg = self._fresh_logger("ghlog-test-disabled")
        ghlog.install(lg)
        assert lg.handlers == []


# ── log_phase: контракт с progress_pusher ────────────────────────────────────

class TestLogPhaseContract:
    # Литералы из ops/mac-local-run/progress_pusher.py (KEY_RE): пушер вне
    # пакета, поэтому дублируем якорь здесь — тест сломается, если формат
    # фазовой строки уедет.
    PUSHER_PHASE_ANCHOR = re.compile(r"— \[")

    def test_phase_line_unchanged_and_no_group_without_env(
        self, monkeypatch, capsys, caplog
    ):
        monkeypatch.delenv("LOG_GH_ANNOTATIONS", raising=False)
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_runs.log_phase(3, 9, "Здоровье парсеров")
        assert "— [3/9] Здоровье парсеров —" in caplog.text
        assert self.PUSHER_PHASE_ANCHOR.search(caplog.text)
        assert "::group::" not in capsys.readouterr().out

    def test_phase_opens_group_with_env(self, monkeypatch, capsys, caplog):
        monkeypatch.setenv("LOG_GH_ANNOTATIONS", "1")
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_runs.log_phase(4, 9, "Обновление карточек")
        out = capsys.readouterr().out
        assert "::group::— [4/9] Обновление карточек —" in out
        # Строка-веха в логе не изменилась (её парсит progress_pusher).
        assert "— [4/9] Обновление карточек —" in caplog.text


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


# ── фаза 5: топ медленных судов ──────────────────────────────────────────────

class TestFormatSlowCourts:
    def test_empty_dict_gives_empty_string(self):
        assert cm_runs._format_slow_courts({}, {}) == ""

    def test_sorted_desc_and_top3(self):
        seconds = {
            "Сургутский городской суд": 5.0,
            "Няганский городской суд": 1.0,
            "Белоярский городской суд": 3.0,
            "Урайский городской суд": 0.5,
        }
        counts = {
            "Сургутский городской суд": 2,
            "Няганский городской суд": 1,
            "Белоярский городской суд": 4,
            "Урайский городской суд": 1,
        }
        out = cm_runs._format_slow_courts(seconds, counts)
        parts = out.split("; ")
        assert len(parts) == 3  # топ-3, четвёртый суд отброшен
        assert parts[0] == (
            f"{shorten_court_name('Сургутский городской суд')} 5.0s (2 карт.)"
        )
        assert parts[1].endswith("3.0s (4 карт.)")
        assert "Урайск" not in out


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


# ── log_run_summary: опциональные строки ─────────────────────────────────────

class TestRunSummaryOptionalLines:
    def test_zero_metrics_no_optional_lines(self, caplog):
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_delivery.log_run_summary("test", {})
        assert "Транспорт: 0 успешных HTTP-ответов / 0 сбоев" in caplog.text
        assert "Карточек-огрызков" not in caplog.text
        assert "Web Push:" not in caplog.text
        assert "LLM-пересказы" not in caplog.text

    def test_nonzero_metrics_appear_in_log_and_step_summary(
        self, caplog, monkeypatch, tmp_path
    ):
        cm_config.METRICS["cards_degraded"] = 3
        cm_config.METRICS["push_sent"] = 5
        cm_config.METRICS["push_failed"] = 1
        cm_config.METRICS["llm_summary_calls"] = 2
        cm_config.METRICS["llm_summary_cache_hits"] = 7
        summary_path = tmp_path / "step_summary.md"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_delivery.log_run_summary("test", {"total": 1.0})
        assert "Карточек-огрызков: 3" in caplog.text
        assert "Web Push: отправлено 5, сбоев 1" in caplog.text
        assert "LLM-пересказы актов: вызовов 2, из кэша 7" in caplog.text
        md = summary_path.read_text(encoding="utf-8")
        assert "- Карточек-огрызков: 3" in md
        assert "- Web Push: отправлено 5, сбоев 1" in md
        assert "- LLM-пересказы актов: вызовов 2, из кэша 7" in md

    def test_push_line_without_failures(self, caplog):
        cm_config.METRICS["push_sent"] = 4
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_delivery.log_run_summary("test", {})
        assert "Web Push: отправлено 4" in caplog.text
        assert "Web Push: отправлено 4, сбоев" not in caplog.text

    def test_llm_line_failed_and_fallback_suffixes(self, caplog):
        # Сбои пересказов и спасения фолбэк-моделью видны в сводке прогона
        # (единственный агрегированный след инцидента «сырая мотивировка
        # вместо „Почему:“» — отдельного алерта нет).
        cm_config.METRICS["llm_summary_calls"] = 5
        cm_config.METRICS["llm_summary_fallback_saved"] = 1
        cm_config.METRICS["llm_summary_failed"] = 2
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_delivery.log_run_summary("test", {})
        assert ("LLM-пересказы актов: вызовов 5, из кэша 0, "
                "спасено фолбэком 1, сбоев 2 (откат на excerpt)") in caplog.text

    def test_llm_line_provider_fallback_suffix(self, caplog):
        # Спасение фолбэк-провайдером Claude (бесплатный пул лёг целиком,
        # инцидент 28.08.2026) — своё слагаемое в сводке: сбоев при этом
        # ноль, и без суффикса выручка была бы невидима.
        cm_config.METRICS["llm_summary_calls"] = 6
        cm_config.METRICS["llm_summary_provider_fallback_saved"] = 2
        with caplog.at_level(logging.INFO, logger="court-monitor"):
            cm_delivery.log_run_summary("test", {})
        assert ("LLM-пересказы актов: вызовов 6, из кэша 0, "
                "спасено Claude 2") in caplog.text


# ── METRICS: инкременты LLM-пересказов ───────────────────────────────────────

class TestSummarizeMetrics:
    ACT = "Мотивировочная часть судебного акта, достаточно длинная. " * 5

    def test_cache_hit_counted(self, monkeypatch):
        key = cm_llm._act_cache_key(self.ACT.strip())
        monkeypatch.setattr(
            cm_llm, "_load_act_summaries",
            lambda: {key: {"summary": "Готовый пересказ."}},
        )
        out = cm_llm.summarize_act_motivation(
            self.ACT, case_meta={"stage": "appeal"}, use_cache=True
        )
        assert out == "Готовый пересказ."
        assert cm_config.METRICS["llm_summary_cache_hits"] == 1
        assert cm_config.METRICS["llm_summary_calls"] == 0

    def test_real_call_counted(self, monkeypatch):
        monkeypatch.setattr(cm_config, "LLM_PROVIDER", "claude")
        # Ключ обязателен: без него пересказ пропускается ДО вызова
        # (llm_summary_skipped_no_key), и патч транспорта не сработал бы.
        monkeypatch.setattr(cm_config, "ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            cm_llm, "_call_claude_simple", lambda prompt: "Суд отказал банку."
        )
        out = cm_llm.summarize_act_motivation(
            self.ACT, case_meta={"stage": "appeal"}, use_cache=False
        )
        assert out
        assert cm_config.METRICS["llm_summary_calls"] == 1
        assert cm_config.METRICS["llm_summary_cache_hits"] == 0


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
