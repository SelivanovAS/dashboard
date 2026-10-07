# -*- coding: utf-8 -*-
"""Дочитка поисков: пропуск судов, чей поиск сегодня уже удался.

Контекст (26.08.2026). Дочитка слотов Mac-резерва (SKIP_CHECKED_TODAY, 21.08)
закрывала только карточки — все три поисковые фазы (7kas, апелляция, 20 судов
1-й инст.) каждый слот прогонялись заново. В день, когда поиск удался в 06:10,
оставшиеся 4–5 слотов жгли ~22 запроса на уже сделанную работу; в WAF-день,
наоборот, повтор слепых поисков и есть путь к зрячему прогону — поэтому
критерий пропуска строгий: `last_count > 0` (WAF-заглушка парсится как
«0 строк» и успехом не считается).

Здесь три группы стражей:
  1. хелпер `searched_ok_today` (health.py) — критерий «сегодня + строки>0 +
     без fail_streak»;
  2. wiring по исходнику runs.py — гейт стоит во всех ТРЁХ фазах, пропуск не
     пишет health_obs (иначе union дня в cloud_run_ok сбился бы);
  3. комментарий parse_and_push.sh документирует расширение флага.

Запуск: `python3 -m pytest scripts/tests/test_search_skip_today.py`.
"""

import ast
from copy import deepcopy
import os
import re
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, SCRIPTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)

from court_monitor.health import (  # noqa: E402
    searched_ok_today, update_parse_health,
)


def _read(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


def _src(key: str, *, days_ago: int = 0, count=5, fail_streak: int = 0) -> dict:
    at = (datetime.now() - timedelta(days=days_ago)).isoformat(timespec="seconds")
    return {key: {"last_run_at": at, "last_count": count,
                  "fail_streak": fail_streak}}


class TestSearchedOkToday:
    """Критерий «поиск сегодня удался»: строгие границы каждого условия."""

    def test_today_with_rows_is_done(self):
        state = {"sources": _src("fi:surgut", count=9)}
        assert searched_ok_today(state) == {"fi:surgut"}

    def test_yesterday_not_done(self):
        state = {"sources": _src("fi:surgut", days_ago=1, count=9)}
        assert searched_ok_today(state) == set()

    def test_zero_rows_not_done(self):
        # WAF-заглушка парсится как «0 строк» — слепой поиск обязан
        # ретраиться каждым слотом, «страница загрузилась» успехом не считается.
        state = {"sources": _src("fi:surgut", count=0)}
        assert searched_ok_today(state) == set()

    def test_failed_fetch_not_done(self):
        # count=None (страница не загрузилась) → last_count не пишется вовсе,
        # растёт fail_streak. Оба признака держат источник в очереди ретраев.
        state = {"sources": _src("fi:surgut", count=None)}
        assert searched_ok_today(state) == set()
        state = {"sources": _src("fi:surgut", count=9, fail_streak=2)}
        assert searched_ok_today(state) == set()

    def test_gated_appeal_never_done(self):
        # Капчёвая апелляция (search_gated) пишет в журнал None — её поиск
        # прогон делает всегда: «снимут код — вернётся сам».
        state = {"sources": {"appeal:oblsud--svd": {
            "last_run_at": datetime.now().isoformat(timespec="seconds"),
            "fail_streak": 0,
        }}}
        assert searched_ok_today(state) == set()

    def test_empty_and_broken_state(self):
        assert searched_ok_today({}) == set()
        assert searched_ok_today({"sources": {}}) == set()
        # Мусор вместо dict источника не роняет хелпер.
        assert searched_ok_today({"sources": {"fi:x": "мусор"}}) == set()

    def test_mixed_sources_filtered(self):
        state = {"sources": {}}
        state["sources"].update(_src("fi:ok", count=9))
        state["sources"].update(_src("fi:blind", count=0))
        state["sources"].update(_src("fi:old", days_ago=1, count=9))
        state["sources"].update(_src("cassation:7kas:total", count=25))
        assert searched_ok_today(state) == {"fi:ok", "cassation:7kas:total"}


class TestRunsWiring:
    """Гейт проведён во все три поисковые фазы main_json и не портит журнал."""

    def _runs(self) -> str:
        return _read("scripts/court_monitor/runs.py")

    def _run_search_prefix(self, phase, *, skipped=False, retry_only=False,
                           queue_only=False):
        """Исполнить настоящий цикл до первого HTTP, не вызывая main_json.

        AST сохраняет вложенные условия и continue: перестановка гейта
        после HTTP, потеря continue или запись в health не пройдут тест.
        Всё после начала запроса отсечено: парсер/доставка здесь не нужны.
        """
        tree = ast.parse(self._runs())
        target = "court_idx" if phase == "fi" else "_ap_i"
        loops = [node for node in ast.walk(tree) if isinstance(node, ast.For)
                 and target in {n.id for n in ast.walk(node.target)
                                if isinstance(n, ast.Name)}]
        assert len(loops) == 1, f"не найден единственный цикл {phase}"
        loop = deepcopy(loops[0])
        http_index = next(i for i, statement in enumerate(loop.body)
            if any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                   and n.func.id == "fetch_page" for n in ast.walk(statement)))
        loop.body = loop.body[:http_index + 1]
        loop.orelse = []
        module = ast.fix_missing_locations(ast.Module(body=[loop], type_ignores=[]))
        court = SimpleNamespace(name="Тестовый суд", domain="test.sudrf.ru",
                                search_disabled=False, search_url=lambda: "search://test")
        old_health = {"source:test": 7, "other": 3}
        context = dict(
            intake_courts=[court], enabled_courts=[] if queue_only else [court],
            queue_only_courts=[court] if queue_only else [], APPEAL_COURTS=[court],
            retry_only=retry_only, search_skip_keys={"source:test"} if skipped else set(),
            fi_search_skipped_today=0, appeal_search_gated_now=set(),
            fi_health_key=lambda c: "source:test", _appeal_health_key=lambda c: "source:test",
            shorten_court_name=lambda name: name, log=Mock(),
            time=SimpleNamespace(perf_counter=lambda: 0), health_obs=old_health.copy(),
            health_labels={}, health_captcha={}, polite_delay=Mock(), fetch_page=Mock(),
            card_breaker_note_failure=Mock(), consume_bank_candidates=Mock(),
        )
        exec(compile(module, "runs.py search prefix", "exec"), context)
        return context, court, old_health

    def test_gate_is_behind_skip_checked_today(self):
        runs = self._runs()
        m = re.search(
            r"search_skip_keys: set\[str\] = \(\s*"
            r"searched_ok_today\(load_parse_health\(\)\)\s*"
            r"if config\.SKIP_CHECKED_TODAY else set\(\)",
            runs,
        )
        assert m, ("гейт дочитки поисков обязан жить под тем же флагом "
                   "SKIP_CHECKED_TODAY, что и дочитка карточек")

    def test_all_three_phases_consult_the_gate(self):
        calls = [node for node in ast.walk(ast.parse(self._runs()))
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                 and node.func.id == "fetch_cassation_search"]
        assert any(len(call.args) >= 3 and isinstance(call.args[2], ast.Name)
                   and call.args[2].id == "search_skip_keys" for call in calls), \
            "фаза кассации не передаёт дневной гейт в общий поиск"
        for phase in ("fi", "appeal"):
            regular, _, _ = self._run_search_prefix(phase)
            regular["fetch_page"].assert_called_once()
            skipped, _, _ = self._run_search_prefix(phase, skipped=True)
            skipped["fetch_page"].assert_not_called()

    def test_cassation_daily_gate_skips_http_and_delay(self, monkeypatch):
        from court_monitor import runs
        from court_monitor.regions.base import CourtConfig
        court = CourtConfig("Кассация", "7kas.sudrf.ru", 2800001, "cassation")
        def unexpected(*args, **kwargs):
            raise AssertionError("уже успешный поиск не должен делать HTTP или ждать")
        monkeypatch.setattr(runs, "fetch_page", unexpected)
        monkeypatch.setattr(runs, "polite_delay", unexpected)
        assert runs.fetch_cassation_search(court, "cass:ok", {"cass:ok"}) == ("", "", True)

    @pytest.mark.parametrize("gate", ["skipped", "retry_only", "queue_only"])
    def test_fi_gate_precedes_polite_delay(self, gate):
        state, court, _ = self._run_search_prefix("fi", **{gate: True})
        state["polite_delay"].assert_not_called()
        state["fetch_page"].assert_not_called()
        # Поиск пропущен, но ранее обнаруженная карточка не теряется.
        state["consume_bank_candidates"].assert_called_once_with(court, [], "[1/1]")
        assert state["fi_search_skipped_today"] == (0 if gate == "queue_only" else 1)

    def test_cassation_skip_does_not_feed_failure_branch(self):
        # Пропуск ≠ отказ: ветка «поиск не загрузился» пишет health_obs=None и
        # кормит предохранитель — при дочитке она обязана молчать, иначе union
        # дня в cloud_run_ok посчитал бы пропуск аварией.
        runs = self._runs()
        assert "if not cass_search_html and not cass_search_skipped:" in runs
        assert "elif not cass_search_skipped:" in runs, (
            "«7kas: пустой ответ от поиска» не должен печататься при дочитке"
        )

    @pytest.mark.parametrize("phase,gate", [
        ("fi", "skipped"), ("fi", "retry_only"), ("fi", "queue_only"),
        ("appeal", "skipped"), ("appeal", "retry_only"),
    ])
    def test_skip_branches_do_not_write_health_obs(self, phase, gate):
        state, _, old_health = self._run_search_prefix(phase, **{gate: True})
        assert state["health_obs"] == old_health
        assert state["health_labels"] == {}
        assert state["health_captcha"] == {}
        state["card_breaker_note_failure"].assert_not_called()


class TestKnownAliveGuard:
    """Глобальный алерт «все источники разом по нулям» глушится дочиткой.

    При пропуске 18 живых судов наблюдаемыми остаются лишь неудачники и
    честные нули — без поправки update_parse_health объявил бы «лежит sudrf
    целиком», хотя живые поиски просто не попали в observations."""

    def _state_with_life(self) -> dict:
        return {"sources": {
            "fi:a": {"counts": [5, 6], "zero_streak": 0, "fail_streak": 0,
                     "alerted_zero": False},
            "fi:b": {"counts": [3, 4], "zero_streak": 0, "fail_streak": 0,
                     "alerted_zero": False},
        }}

    def test_all_dead_alert_without_skips(self):
        _, alerts = update_parse_health(
            {"fi:a": 0, "fi:b": None}, state=self._state_with_life()
        )
        assert any("ВСЕ источники" in a for a in alerts)

    def test_all_dead_alert_muted_by_known_alive(self):
        _, alerts = update_parse_health(
            {"fi:a": 0, "fi:b": None}, state=self._state_with_life(),
            known_alive_today=18,
        )
        assert not any("ВСЕ источники" in a for a in alerts)

    def test_run_wiring_passes_skip_count(self):
        runs = _read("scripts/court_monitor/runs.py")
        assert "known_alive_today=len(search_skip_keys)" in runs, (
            "блок 4e обязан передавать число пропущенных дочиткой поисков"
        )


class TestShellDoc:
    """Комментарий у SKIP_CHECKED_TODAY в parse_and_push.sh документирует,
    что флаг гасит и повторные поиски (иначе расширение семантики молчаливое)."""

    def test_comment_mentions_searches(self):
        sh = _read("ops/mac-local-run/parse_and_push.sh")
        assert "searched_ok_today" in sh
        assert "ПОИСКИ" in sh or "поиски" in sh
