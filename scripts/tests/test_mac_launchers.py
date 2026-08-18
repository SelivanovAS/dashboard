#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Подстраховка на Mac: гейт «облако уже отработало», агенты, пульт.

18.08.2026 суды резали часть адресного пула облачных раннеров: юрист вручную
перезапускал прогон Урала 8 раз за два часа и дважды руками подбирал дампы
(~94 иска). Решение: облако не трогаем, подстраховка живёт на Mac — агент
парсит ТОЛЬКО когда облачный прогон был слепым или не состоялся, дампы
подбираются по расписанию, пульт даёт ручной запуск и живой лог.

Главная опасность здесь — двойной дайджест: Mac, не разобравшийся, что облако
уже отработало, пушит с маркером «(Mac-парсинг)» и рассылает всем повторно.
Поэтому гейт стережём поведенческими тестами, а не только grep'ом.
"""
from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)
RESERVE = os.path.join(REPO_DIR, "ops", "mac-local-run")
sys.path.insert(0, RESERVE)

import cloud_run_ok  # noqa: E402

PARSE_PLIST = os.path.join(RESERVE, "com.court-monitor.parse.plist")
IMPORT_PLIST = os.path.join(RESERVE, "com.court-monitor.import.plist")
PULT = os.path.join(RESERVE, "СберСуд-пульт.command")
SHIM = os.path.join(RESERVE, "Парсинг судов.command")


def _read(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


def _slots(path: str) -> set:
    with open(path, "rb") as f:
        p = plistlib.load(f)
    return {(d["Weekday"], d["Hour"], d["Minute"])
            for d in p["StartCalendarInterval"]}


# ── Гейт: зрячий/слепой/не было ──────────────────────────────────────────────

def _src(run_at: str, count: int, fail_streak: int = 0) -> dict:
    return {"last_run_at": run_at, "last_count": count,
            "fail_streak": fail_streak, "counts": [count]}


class TestSightedRunToday:
    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    OLD = "2026-01-01T08:00:00"

    def test_sighted_today(self):
        ok, text = cloud_run_ok.sighted_run_today(
            {"sources": {"a": _src(self.TODAY, 24), "b": _src(self.TODAY, 0)}})
        assert ok and "зрячий" in text

    def test_blind_today_means_parse(self):
        """Слепой прогон (адрес раннера заблокирован) пишет нули по ВСЕМ
        источникам — Mac обязан парсить сам."""
        ok, text = cloud_run_ok.sighted_run_today(
            {"sources": {"a": _src(self.TODAY, 0), "b": _src(self.TODAY, 0)}})
        assert not ok and "СЛЕПОЙ" in text

    def test_no_run_today_means_parse(self):
        ok, text = cloud_run_ok.sighted_run_today(
            {"sources": {"a": _src(self.OLD, 24)}})
        assert not ok and "не было" in text

    def test_fetch_fail_is_not_sighted(self):
        """⚠️ Ловушка: при сетевом фейле update_parse_health бампает
        last_run_at, но НЕ трогает last_count — остаётся вчерашний ненулевой.
        Без проверки fail_streak провальный прогон сошёл бы за зрячий, гейт
        промолчал бы, и слепое утро осталось бы без данных вовсе."""
        ok, _ = cloud_run_ok.sighted_run_today(
            {"sources": {"a": _src(self.TODAY, 24, fail_streak=1)}})
        assert not ok

    def test_empty_or_missing_state(self):
        assert not cloud_run_ok.sighted_run_today({})[0]
        assert not cloud_run_ok.sighted_run_today({"sources": {}})[0]


# ── Проводка гейта в parse_and_push ──────────────────────────────────────────

class TestGateWiring:
    def test_gate_sits_after_pull_and_before_probe(self):
        """Гейт читает журнал ПОСЛЕ git pull (иначе решает по вчерашнему
        файлу) и ДО пробы судов (иначе при живом облаке агент дёргал бы
        🚨-алерт «суд недоступен» из-за собственных сетевых проблем)."""
        text = _read("ops/mac-local-run/parse_and_push.sh")
        pull = text.index("git pull --rebase")
        gate = text.index("cloud_run_ok.py --report")
        probe = text.index("cm_probe_court_host")
        assert pull < gate < probe

    def test_force_bypasses_gate_and_check_does_not_hit_it(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert '"$FORCE" != "1"' in text, "гейт не отключается --force"
        assert "--force)" in text, "parse_and_push не принимает --force"
        # Сам if гейта: --check не должен утыкаться в него (диагностика, а не
        # прогон). Ищем строку кода, а не окно вокруг комментария.
        assert '[ "$CHECK_ONLY" != "1" ] && [ "$FORCE" != "1" ]' in text

    def test_skip_message_names_the_reason(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "Облако сегодня уже отработало" in text
        assert "пропуск" in text


# ── Расписания агентов (plistlib: в CI нет plutil) ───────────────────────────

class TestAgentSchedules:
    def test_parse_slots(self):
        """09:00 — после облачных кронов (08:00/08:30 + ~15-30 мин прогона);
        11:00 — страховка «Mac спал в 09:00» (18.08.2026: runs=0 за день).
        Гейт делает лишний слот тихим пропуском."""
        assert _slots(PARSE_PLIST) == {
            (w, h, 0) for w in range(1, 6) for h in (9, 11)}

    def test_import_slots(self):
        """Расписание юриста (18.08.2026): будни 10:30–18:30 каждые 2 часа —
        оператор кормит дампы до вечера, облако часть теряет."""
        assert _slots(IMPORT_PLIST) == {
            (w, h, 30) for w in range(1, 6) for h in (10, 12, 14, 16, 18)}

    def test_agents_call_the_right_drivers(self):
        with open(PARSE_PLIST, "rb") as f:
            assert "parse_all.sh" in plistlib.load(f)["ProgramArguments"][1]
        with open(IMPORT_PLIST, "rb") as f:
            assert "import_all.sh" in plistlib.load(f)["ProgramArguments"][1]

    def test_import_agent_has_own_logs(self):
        with open(IMPORT_PLIST, "rb") as f:
            p = plistlib.load(f)
        assert "launchd-import" in p["StandardOutPath"], \
            "импорт-агент пишет в логи парсинг-агента — историю не разобрать"


# ── Драйверы и территории ────────────────────────────────────────────────────

class TestDrivers:
    @pytest.mark.parametrize("rel", ["ops/mac-local-run/parse_all.sh",
                                     "ops/mac-local-run/import_all.sh"])
    def test_territories_come_from_shared_helper(self, rel):
        """Список клонов — ТОЛЬКО cm_territories: копия списка уже дважды
        была причиной молчаливых поломок резерва (файлы данных, домены)."""
        text = _read(rel)
        assert "cm_territories" in text, f"{rel} не берёт территории из lib"
        # Признак СВОЕГО чтения — присвоение пути списка (упоминание файла в
        # комментариях-доках законно).
        assert 'LIST="$HOME/.config' not in text, \
            f"{rel} читает файл территорий сам, мимо cm_territories"

    def test_import_all_survives_one_territory(self):
        text = _read("ops/mac-local-run/import_all.sh")
        assert "|| rc=1" in text and "continue" in text

    @pytest.mark.parametrize("name", ["import_all.sh", "СберСуд-пульт.command",
                                      "Парсинг судов.command"])
    def test_shell_syntax(self, name):
        subprocess.run(["bash", "-n", os.path.join(RESERVE, name)],
                       check=True, capture_output=True)


# ── Пульт ────────────────────────────────────────────────────────────────────

class TestPult:
    def test_menu_has_all_four_actions(self):
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        for label in ("Запустить парсинг сейчас", "Подобрать дампы сейчас",
                      "Смотреть живой лог", "Проверка"):
            assert label in text, f"из меню пропал пункт «{label}»"

    def test_manual_parse_bypasses_gate(self):
        """Пункт [1] — воля юриста «прямо сейчас»: без --force гейт увидел бы
        зрячее облако и молча ничего не сделал — кнопка выглядела бы битой."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "--force --anywhere" in text

    def test_header_shows_cloud_status(self):
        assert "cloud_run_ok.py --report" in _read(
            "ops/mac-local-run/СберСуд-пульт.command")

    def test_old_shortcut_still_works(self):
        """Старые копии «Парсинг судов.command» на рабочем столе обязаны
        открывать пульт, а не умирать."""
        text = _read("ops/mac-local-run/Парсинг судов.command")
        assert "exec" in text and "СберСуд-пульт.command" in text
