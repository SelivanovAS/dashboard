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
import random
import re
import subprocess
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)
RESERVE = os.path.join(REPO_DIR, "ops", "mac-local-run")
sys.path.insert(0, RESERVE)

import cloud_run_ok  # noqa: E402
import probe_sample  # noqa: E402

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


class TestRunVerdicts:
    """Вердикты cloud_run_ok («один дайджест в день», 20.08.2026): удачная
    попытка = поиски зрячие И карточки ≥85% плана (CARDS_READ_OK_RATIO,
    решение юриста — сначала 75%, поправил на 85%)."""

    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    OLD = "2026-01-01T08:00:00"

    def _lr(self, read: int, planned: int, at: str | None = None) -> dict:
        return {"at": at or self.TODAY, "cards_read": read,
                "cards_planned": planned}

    def _state(self, count: int, fail: int = 0, lr: dict | None = None) -> dict:
        st = {"sources": {"a": _src(self.TODAY, count, fail_streak=fail)}}
        if lr is not None:
            st["last_run"] = lr
        return st

    def test_complete_when_sighted_and_cards_over_threshold(self):
        ok, why = cloud_run_ok.run_complete_today(
            self._state(24, lr=self._lr(160, 172)))
        assert ok and "прочитано 160 из 172" in why

    def test_threshold_is_inclusive_85(self):
        ok, _ = cloud_run_ok.run_complete_today(
            self._state(24, lr=self._lr(85, 100)))
        assert ok, "85% — это уже удачная попытка (порог включительный)"
        ok, why = cloud_run_ok.run_complete_today(
            self._state(24, lr=self._lr(84, 100)))
        assert not ok and "84%" in why

    def test_half_read_run_is_incomplete(self):
        """ХМАО 20.08: поиски ожили, но сеть срезала 52 карточки из 172 —
        попытка неполная, дайджест не шлём, копим и дочитываем."""
        ok, why = cloud_run_ok.run_complete_today(
            self._state(24, lr=self._lr(120, 172)))
        assert not ok and "из 172" in why

    def test_blind_searches_incomplete(self):
        """Формулировка называет, что не удалось: юрист 20.08 прочитал «не
        спарсилось» при доехавших карточках как полный провал."""
        ok, why = cloud_run_ok.run_complete_today(
            self._state(0, lr=self._lr(170, 172)))
        assert not ok and "СЛЕПЫЕ" in why and "170" in why

    def test_fetch_fail_is_not_sighted(self):
        """⚠️ Ловушка: при сетевом фейле update_parse_health бампает
        last_run_at, но НЕ трогает last_count — без fail_streak провальный
        прогон сошёл бы за зрячий."""
        ok, _ = cloud_run_ok.run_complete_today(self._state(24, fail=1))
        assert not ok

    def test_no_run_today(self):
        ok, why = cloud_run_ok.run_complete_today(
            {"sources": {"a": _src(self.OLD, 24)}})
        assert not ok and "не было" in why

    def test_old_journal_without_cards_is_complete(self):
        """Журнал без блока last_run (старый формат) — судим только по
        поискам, как раньше."""
        ok, _ = cloud_run_ok.run_complete_today(self._state(24))
        assert ok

    def test_stale_last_run_is_ignored(self):
        """Вчерашний last_run не судит сегодняшний день."""
        ok, _ = cloud_run_ok.run_complete_today(
            self._state(24, lr=self._lr(1, 100, at=self.OLD)))
        assert ok


class TestMorningCumulative:
    """Накопительная сводка утра (21.08.2026): слоты пересчитывают план на
    каждой попытке, и числа последней попытки юрист прочитал как итог дня —
    «прочитано 119 из 362 (32%)» на дедлайне при реальных ~70% покрытия.
    Ключ cards_read_today пишет блок 4e main_json (штампы last_checked_at за
    сегодня). Хвост «за утро…» появляется ТОЛЬКО при повторной попытке
    (total > read текущей): в первой total == read, и прежние формулировки
    остаются побайтово — их стерегут тесты «…unchanged»."""

    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    def _state(self, read: int, planned: int, total: int | None = None,
               sighted: bool = True) -> dict:
        lr = {"at": self.TODAY, "cards_read": read, "cards_planned": planned}
        if total is not None:
            lr["cards_read_today"] = total
        return {"sources": {"a": _src(self.TODAY, 24 if sighted else 0)},
                "last_run": lr}

    def test_progress_line_repeat_attempt_cumulative(self):
        """Боевой кейс дедлайна 21.08: попытка 119/362 при 346 прочитанных
        за утро — алерт обязан вести накопительный счёт."""
        line = cloud_run_ok.progress_line(
            self._state(119, 362, total=346, sighted=False))
        assert "за утро прочитано 346 карточек" in line
        assert "недочитано 243" in line
        assert "эта попытка: 119 из 362" in line
        assert line.endswith("поиски молчали")

    def test_progress_line_first_attempt_unchanged(self):
        line = cloud_run_ok.progress_line(self._state(340, 484, total=340))
        assert line == "прочитано 340 из 484 карточек (70%), поиски отвечали"

    def test_progress_line_old_journal_unchanged(self):
        """Журнал без cards_read_today (до 21.08.2026) — прежняя строка."""
        line = cloud_run_ok.progress_line(self._state(340, 484))
        assert line == "прочитано 340 из 484 карточек (70%), поиски отвечали"

    def test_incomplete_verdict_carries_cumulative(self):
        ok, why = cloud_run_ok.run_complete_today(
            self._state(119, 362, total=346))
        assert not ok and "за утро всего 346" in why

    def test_blind_verdict_carries_cumulative(self):
        ok, why = cloud_run_ok.run_complete_today(
            self._state(119, 362, total=346, sighted=False))
        assert not ok and "СЛЕПЫЕ" in why and "за утро всего 346" in why

    def test_success_single_attempt_unchanged(self):
        ok, why = cloud_run_ok.run_complete_today(
            self._state(160, 172, total=160))
        assert ok
        assert why == "прочитано 160 из 172 карточек, поиски отвечали"

    def test_success_after_dochitka_carries_cumulative(self):
        """Дочитка добила остаток (130 из 144 ≥ 85%): вердикт удачный,
        дайджест уходит ДО дедлайна, сводка — накопительная."""
        ok, why = cloud_run_ok.run_complete_today(
            self._state(130, 144, total=470))
        assert ok and "за утро всего 470" in why


class TestDeliveredGate:
    """Гейт слота: пропуск ТОЛЬКО когда дайджест дня уже отправлен
    (delivered_at) — иначе повторный маркер-коммит разослал бы его дважды."""

    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    def _ctx(self, delivered: bool, changes: int = 1, at: str | None = None) -> dict:
        ctx = {"saved_at": at or self.TODAY,
               "fi_changes": [{"case": f"2-{i}/2026"} for i in range(changes)]}
        if delivered:
            ctx["delivered_at"] = at or self.TODAY
        return ctx

    def test_delivered_today_skips(self):
        skip, text = cloud_run_ok.gate({}, self._ctx(True))
        assert skip and "отправлен" in text

    def test_undelivered_complete_run_still_works(self):
        """Удачная попытка без доставки (сорвался маркер-коммит) — слот
        обязан работать и закрыть день доставкой."""
        state = {"sources": {"a": _src(self.TODAY, 24)}}
        skip, text = cloud_run_ok.gate(state, self._ctx(False))
        assert not skip and "не отправлен" in text

    def test_undelivered_incomplete_keeps_working(self):
        skip, text = cloud_run_ok.gate(
            {"sources": {"a": _src(self.TODAY, 0)}}, self._ctx(False))
        assert not skip and "копим" in text

    def test_yesterdays_delivery_does_not_skip(self):
        old = "2026-01-01T09:00:00"
        skip, _ = cloud_run_ok.gate({}, self._ctx(True, at=old))
        assert not skip

    def test_context_pending(self):
        assert cloud_run_ok.context_pending(self._ctx(False))
        assert not cloud_run_ok.context_pending(self._ctx(True)), \
            "доставленное — не pending"
        assert not cloud_run_ok.context_pending(self._ctx(False, changes=0)), \
            "пустое накопление — нечего доставлять"
        assert not cloud_run_ok.context_pending({})

    def test_delta_keys_mirror_core(self):
        """CTX_DELTA_KEYS — зеркало _CTX_DELTA_KEYS из digest/core.py: при
        расхождении --has-pending молча ослепнет на новый вид дельты."""
        import importlib
        sys.path.insert(0, SCRIPTS_DIR)
        core = importlib.import_module("court_monitor.digest.core")
        assert tuple(cloud_run_ok.CTX_DELTA_KEYS) == tuple(core._CTX_DELTA_KEYS)


class TestProgressLine:
    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    def test_numbers_and_searches(self):
        state = {"sources": {"a": _src(self.TODAY, 24)},
                 "last_run": {"at": self.TODAY, "cards_read": 120,
                              "cards_planned": 172}}
        line = cloud_run_ok.progress_line(state)
        assert "120 из 172" in line and "69%" in line and "отвечали" in line
        state["sources"]["a"] = _src(self.TODAY, 0)
        assert "молчали" in cloud_run_ok.progress_line(state)

    def test_appeal_cannot_mask_blind_first_instance_searches(self):
        state = {
            "sources": {
                "fi:one.test": _src(self.TODAY, 0),
                "fi:two.test": _src(self.TODAY, 0),
                "appeal:ok.test": _src(self.TODAY, 22),
            },
            "last_run": {
                "at": self.TODAY,
                "cards_read": 20,
                "cards_planned": 20,
            },
        }
        ran, sighted = cloud_run_ok.searches_state_today(state)
        assert ran and not sighted
        assert "молчали" in cloud_run_ok.progress_line(state)
        ok, why = cloud_run_ok.run_complete_today(state)
        assert not ok and "СЛЕПЫЕ" in why

    def test_non_fi_sources_remain_valid_when_no_fi_search_is_configured(self):
        state = {
            "sources": {"appeal:ok.test": _src(self.TODAY, 22)},
            "last_run": {
                "at": self.TODAY,
                "cards_read": 20,
                "cards_planned": 20,
            },
        }
        assert cloud_run_ok.searches_state_today(state) == (True, True)


# ── Проводка гейта в parse_and_push ──────────────────────────────────────────

class TestGateWiring:
    def test_gate_sits_after_pull_and_before_probe(self):
        """Гейт читает журнал ПОСЛЕ git pull (иначе решает по вчерашнему
        файлу) и ДО пробы судов (иначе при живом облаке агент дёргал бы
        🚨-алерт «суд недоступен» из-за собственных сетевых проблем)."""
        text = _read("ops/mac-local-run/parse_and_push.sh")
        pull = text.index("git pull --rebase")
        gate = text.index("cloud_run_ok.py --report")
        probe = text.index("cm_any_court_reachable")
        assert pull < gate < probe

    def test_stale_lock_and_parse_snapshot_recover_before_pull(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        lock = text.index('"$RUN_LOCK_TOOL" acquire')
        recovery = text.index("  reconcile_parse_transaction", lock)
        sync_call = text.index("  sync_git_and_delivery_state", recovery)
        assert lock < recovery < sync_call
        sync_fn = text[text.index("sync_git_and_delivery_state()"):
                       text.index("\n}", text.index("sync_git_and_delivery_state()"))]
        assert "git pull --rebase" in sync_fn
        assert 'trap \'release_run_lock\' EXIT' in text

    def test_parser_is_bracketed_by_snapshot_and_wal_ack(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        prepare = text.index('"$PARSE_TXN_TOOL" prepare')
        parser = text.index('"$PYTHON" ops/mac-local-run/run_parse.py')
        finish = text.index('"$PARSE_TXN_TOOL" finish', parser)
        assert prepare < parser < finish
        env = text[prepare:parser]
        assert 'PARSE_TXN_ID="$PARSE_TXN_ID"' in env
        assert 'PARSE_TXN_ACK_FILE="$PARSE_TXN_ACK_FILE"' in env

    def test_force_bypasses_gate_and_check_does_not_hit_it(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert '"$FORCE" != "1"' in text, "гейт не отключается --force"
        assert "--force)" in text, "parse_and_push не принимает --force"
        # Сам if гейта: --check не должен утыкаться в него (диагностика, а не
        # прогон). Ищем строку кода, а не окно вокруг комментария.
        assert '[ "$CHECK_ONLY" != "1" ] && [ "$FORCE" != "1" ]' in text

    def test_skip_message_names_the_reason(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "Дайджест дня уже отправлен" in text
        assert "пропуск" in text


# ── Канарейка судов: тихие ретраи, один алерт в день ─────────────────────────

class TestCanaryQuietRetries:
    """Слоты агента идут каждые полчаса и сами добивают сорвавшуюся пробу
    (20.08.2026 Урал: отказ в 08:19 → спарсился в 08:30), а алерт на КАЖДУЮ
    неудачу дал 5 одинаковых сообщений за утро. Агентская ветка обязана
    молчать до конца окна и кричать один раз в день."""

    def test_quiet_until_deadline_then_deliver_or_single_alert(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        lib = _read("ops/mac-local-run/lib_sber_net.sh")
        assert 'CM_DELIVERY_WINDOW_MIN="${CM_DELIVERY_WINDOW_MIN:-525}"' in lib, \
            "окно доставки 08:45 пропало — дайджест уйдёт не вовремя"
        assert ".alerted-parse-" in text, "дневной дедуп алерта «утро потеряно» пропал"
        body = text[text.index("probe_failed()"):]
        body = body[:body.index('if PROBE_HOST=')]
        assert '"$FORCE" = "1"' in body, "ручной запуск (--force) обязан кричать сразу"
        assert "exit 0" in body, "тихая ветка обязана выходить без ошибки"
        assert "finish_pusher" in body, "pusher уже запущен к моменту пробы — его надо дождаться"
        # Окно доставки при мёртвых судах: накопленное утро уезжает
        # доставочным коммитом БЕЗ парсинга, а не пропадает до завтра.
        assert "--has-pending" in body and "deliver_and_push" in body, \
            "probe_failed потерял доставку накопленного в окне"

    def test_import_agent_gets_anywhere_only_with_quiet_canary(self):
        """--anywhere у агентов появился вместе с тихой канарейкой: вернуть
        алерт на каждую неудачу при 12 запусках в день — снова шторм."""
        text = _read("ops/mac-local-run/import_dumps.sh")
        assert ".alerted-dumps-" in text, "дневной дедуп алерта дампов пропал"


# ── «Один дайджест в день»: проводка решения «слать или копить» ─────────────

class TestOneDigestPerDay:
    """Отправку решает СООБЩЕНИЕ КОММИТА: replay_on_push стреляет по
    contains(message, 'Mac-парсинг'). Черновик обязан быть БЕЗ этой подстроки,
    доставка — с ней, а штамп delivered_at обязан входить В доставочный коммит
    (иначе следующий слот не узнает о доставке и продублирует дайджест).

    ⚠️ 24.08.2026 порядок изменён: штамп ставится ПОСЛЕ того, как данные уже
    успешно запушены. Прежнее «штамп → коммит → push» при упавшем пуше
    оставляло delivered_at в локальном контексте — день закрыт, дайджест не
    отправлен, следующие слоты выходят по гейту. Внутри доставочной фазы
    порядок прежний: штамп → коммит с маркером → push."""

    def test_draft_message_has_no_marker_substring(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        # Сообщение с 24.08.2026 инлайнится прямо в git commit -m, отдельной
        # переменной COMMIT_MSG больше нет — ищем по самой формулировке.
        draft = [l for l in text.splitlines()
                 if "копим дайджест" in l and "commit --only -m" in l]
        assert draft, "черновое сообщение коммита пропало"
        assert all("Mac-парсинг" not in l for l in draft), (
            "в черновом сообщении подстрока «Mac-парсинг» — contains() гарда "
            "replay_on_push разошлёт недособранное утро"
        )

    def test_delivery_message_keeps_marker(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert text.count("(Mac-парсинг)") >= 2, (
            "маркер доставки должен стоять и в deliver_and_push, и в "
            "замыкающем коммите после парсинга"
        )

    def test_draft_commit_cannot_consume_unrelated_staged_work(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        phase = text[text.index("# ── Фаза 1: данные"):
                     text.index("# ── Фаза 2: доставка")]
        assert 'git diff --cached --quiet -- "${DATA_FILES[@]}"' in phase
        assert 'commit --only -m "📊 Данные обновлены' in phase
        assert '-- "${DATA_FILES[@]}"' in phase
        assert "if git diff --cached --quiet; then" not in phase

    def test_stamp_after_data_push_but_inside_delivery_commit(self):
        """Два инварианта разом, и они не противоречат друг другу.

        (1) Штамп ставится ПОСЛЕ успешного пуша ДАННЫХ: упавший пуш не должен
        закрывать день (24.08.2026 — юрист уходил из сети в минуту окна, оба
        прогона пришлось гасить руками). (2) Но внутри доставочной фазы штамп
        по-прежнему ДО коммита с маркером — replay читает контекст из того же
        коммита, и без штампа дайджест ушёл бы мимо отметки.
        """
        text = _read("ops/mac-local-run/parse_and_push.sh")
        decision = text.index('RUN_WHY=$(')
        push_data = text.index('die "git push данных не удался', decision)
        delivery_call = text.index('deliver_and_push "обычный финиш', push_data)
        assert push_data < delivery_call, "delivery helper вызван до push данных"

        fn = text[text.index("deliver_and_push() {"):]
        fn = fn[:fn.index("\n}")]
        journal = fn.index('"$DELIVERY_TXN_TOOL" prepare')
        stamp = fn.index("--mark-delivered")
        commit = fn.index("(Mac-парсинг)", stamp)
        assert journal < stamp < commit, (
            "journal обязан появиться до штампа, а штамп — "
            "внутри marker-коммита"
        )

    def test_delivery_recovery_precedes_pull_and_daily_gate(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        recovery = text.index("  reconcile_delivery_transaction")
        pull = text.index("git pull --rebase --autostash")
        gate = text.index("cloud_run_ok.py --report")
        assert recovery < pull < gate

    def test_prepared_crash_before_mark_clears_without_false_unmark(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        recovery = text[text.index("reconcile_delivery_transaction() {"):]
        recovery = recovery[:recovery.index("\n}")]
        prepared = recovery[recovery.index("prepared)"):]
        prepared = prepared[:prepared.index("committed)")]
        assert 'head_sha=$(git rev-parse HEAD' in prepared
        assert 'git diff --quiet -- "$DELIVERY_CONTEXT_PATH"' in prepared
        assert 'git diff --cached --quiet -- "$DELIVERY_CONTEXT_PATH"' in prepared
        clean_clear = prepared.index('"$DELIVERY_TXN_TOOL" clear')
        unmark = prepared.index("rollback_delivery_transaction", clean_clear)
        assert clean_clear < unmark, (
            "crash между journal и mark нельзя пытаться unmark: "
            "delivery_id в контексте ещё нет"
        )

    def test_all_delivery_paths_use_one_helper(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert text.count("deliver_and_push() {") == 1
        assert text.count('deliver_and_push "') == 3, (
            "обычная, canary-fallback и final-sweep ветки должны идти "
            "через одну транзакцию"
        )
        assert text.count("--mark-delivered") == 1, (
            "кроме единого helper копий mark быть не должно"
        )
        # Helper теперь возвращается: canary-fallback обязан
        # закончить ветку, а обычный path — дойти до финального log.
        canary = text[text.index('deliver_and_push "накопленное утро'):]
        assert "exit 0" in "\n".join(canary.splitlines()[:6])
        sweep = text[text.index('deliver_and_push "финальный sweep'):]
        assert "exit 0" in "\n".join(sweep.splitlines()[:6])
        normal = text[text.index('deliver_and_push "обычный финиш'):]
        assert 'notify "Готово: данные обновлены' in normal
        assert 'log "Готово"' in normal and "finish_pusher" in normal

    def test_marker_sha_is_journaled_before_push(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        fn = text[text.index("deliver_and_push() {"):]
        fn = fn[:fn.index("\n}")]
        commit = fn.index('commit --only -m "📊')
        marker_sha = fn.index("marker_sha=$(git rev-parse HEAD", commit)
        journal = fn.index('"$DELIVERY_TXN_TOOL" committed', marker_sha)
        push = fn.index('git push "$GIT_URL" HEAD:main', journal)
        assert commit < marker_sha < journal < push

    def test_rollback_commit_is_scoped_to_digest_context(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        rb = text[text.index("rollback_delivery_transaction() {"):]
        rb = rb[:rb.index("\n}")]
        assert 'git add -- "$DELIVERY_CONTEXT_PATH"' in rb
        assert 'commit --only -m "↩️' in rb
        assert '-- "$DELIVERY_CONTEXT_PATH"' in rb

    def test_only_window_or_force_deliver(self):
        """⚠️ Решение юриста 21.08.2026 «дайджест не раньше 08:45»: доставку
        решает ОКНО, а удачный вердикт — больше нет. Со слотами от 06:00
        прежняя ветка «RUN_OK → DELIVER=1» разослала бы дайджест в 06:30."""
        text = _read("ops/mac-local-run/parse_and_push.sh")
        block = text[text.index('RUN_WHY=$('):]
        block = block[:block.index("if git diff --cached --quiet")]
        assert '[ "$FORCE" = "1" ] && DELIVER=1' in block
        assert "cm_delivery_window_open && DELIVER=1" in block
        assert 'RUN_OK" = "1" ] && DELIVER=1' not in block, (
            "удачная попытка до 08:45 обязана оставаться черновиком — "
            "иначе слот 06:00 отправит дайджест в 06:30"
        )

    def test_incomplete_attempt_sends_progress_alert(self):
        """Решение юриста 20.08.2026: после КАЖДОЙ неполной попытки — алерт
        «прочитано X из Y» (--progress), без дневного дедупа. Удачная попытка
        до окна молчит: копить больше нечего, а шесть «всё прочитано» за утро
        — тот же спам, от которого уходили 20.08."""
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "попытка неполная" in text and "--progress" in text
        block = text[text.index("Черновик запушен"):]
        block = block[:block.index("\n  fi")]
        assert '[ "$RUN_OK" != "1" ]' in block, \
            "алерт-прогресс потерял гейт «попытка неполная»"

    def test_window_delivery_names_incompleteness(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "дайджест отправлен с тем, что дочиталось" in text


class TestFinalDeliverySweep:
    """Финальный barrier закрывает гонку одного LaunchAgent с двумя клонами.

    25.08.2026 Урал закончил до окна и оставил pending, ХМАО держал процесс
    занятым после 08:45, а launchd не поставил календарный слот в очередь.
    Родитель обязан проверить ОБА контекста после wait, не запуская суды снова.
    """

    def test_parent_sweeps_both_contexts_after_wait_and_before_imports(self):
        driver = _read("ops/mac-local-run/parse_all.sh")
        main = driver[driver.index('if [ "${#valid_repos[@]}"'):]
        wait = main.index("run_parallel_parsers")
        sweep = main.index("run_delivery_sweep", wait)
        imports = main.index('run_imports "$@"', sweep)
        assert wait < sweep < imports
        fn = driver[driver.index("run_delivery_sweep()"):
                    driver.index("\n}", driver.index("run_delivery_sweep()"))]
        assert 'for repo in "${valid_repos[@]}"' in fn
        assert 'run_worker "$repo" --deliver-pending' in fn

    def test_sweep_mode_bypasses_court_preflight_and_parser(self):
        worker = _read("ops/mac-local-run/parse_and_push.sh")
        branch = worker.index('if [ "$DELIVER_PENDING_ONLY" = "1" ]; then')
        preflight = worker.index("# ── Preflight:", branch)
        run_parse = worker.index('run_parse.py >>"$LOG"', preflight)
        body = worker[branch:preflight]
        assert branch < preflight < run_parse
        assert "cm_any_court_reachable" not in body
        assert "run_parse.py" not in body
        assert "--has-pending" in body
        assert 'deliver_and_push "финальный sweep' in body

    def test_sweep_reuses_exact_once_funnel_and_requires_committed_context(self):
        worker = _read("ops/mac-local-run/parse_and_push.sh")
        branch = worker[worker.index('if [ "$DELIVER_PENDING_ONLY" = "1" ]; then'):]
        branch = branch[:branch.index("# ── Preflight:")]
        assert 'git status --porcelain -- "$DELIVERY_CONTEXT_PATH"' in branch
        assert "--mark-delivered" not in branch, \
            "sweep создал вторую реализацию штампа мимо deliver_and_push"
        assert "--mark-delivered" not in _read("ops/mac-local-run/parse_all.sh")

    def test_sweep_never_runs_in_check_mode(self):
        driver = _read("ops/mac-local-run/parse_all.sh")
        fn = driver[driver.index("run_delivery_sweep()"):
                    driver.index("\n}", driver.index("run_delivery_sweep()"))]
        assert fn.index('[ "$CHECK_ONLY" = "1" ] && return 0') \
            < fn.index("cm_delivery_window_open")

    def test_sweep_flag_rejects_force_and_check(self):
        worker = _read("ops/mac-local-run/parse_and_push.sh")
        assert "--deliver-pending) DELIVER_PENDING_ONLY=1" in worker
        assert "--deliver-pending нельзя совмещать с --check или --force" in worker


# ── Расписания агентов (plistlib: в CI нет plutil) ───────────────────────────

class TestAgentSchedules:
    def test_parse_slots(self):
        """Расписание 21.08.2026 (решение юриста «парсинг с 06:00, дайджест
        не раньше 08:45»): каждые 30 минут с 06:00 до 08:30 + доставочный
        08:45. Он же ПОСЛЕДНИЙ — слот 09:00 юрист велел убрать, страховки
        после окна нет; проспанный слот launchd доигрывает при пробуждении,
        и окно внутри скрипта отправит накопленное сразу."""
        assert _slots(PARSE_PLIST) == {
            (w, h, m) for w in range(1, 6)
            for h, m in ((6, 0), (6, 30), (7, 0), (7, 30),
                         (8, 0), (8, 30), (8, 45))}

    def test_delivery_slot_matches_window(self):
        """Доставочный слот и окно в скрипте держать ПАРОЙ: слот раньше окна
        превратил бы дайджест в черновик и отложил его до завтра."""
        text = _read("ops/mac-local-run/lib_sber_net.sh")
        match = re.search(r"CM_DELIVERY_WINDOW_MIN=.*:-([0-9]+)\}", text)
        assert match, "общая граница окна доставки не найдена"
        window = int(match.group(1))
        last = max(h * 60 + m for _w, h, m in _slots(PARSE_PLIST))
        assert last >= window, (
            f"последний слот {last // 60}:{last % 60:02d} раньше окна "
            f"{window // 60}:{window % 60:02d} — дайджест не уйдёт"
        )

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

    def test_agents_run_anywhere(self):
        """--anywhere у ОБОИХ агентов (19.08.2026): без флага агент требовал
        сети Сбера — 19.08 оба слота пропущены («не в сети Сбера», Mac был
        дома), парсинг и очередь дампов запускались руками. В офисе флаг
        поведения не меняет (преflight «в сети Сбера» первым и строит
        маршруты), вне офиса честная проба судов решает, есть ли доступ."""
        for path in (PARSE_PLIST, IMPORT_PLIST):
            with open(path, "rb") as f:
                args = plistlib.load(f)["ProgramArguments"]
            assert "--anywhere" in args, f"{os.path.basename(path)} без --anywhere"

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
    def test_menu_has_all_actions(self):
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        for label in ("Запустить парсинг сейчас", "Подобрать дампы сейчас",
                      "Смотреть живой лог", "Проверка",
                      "Включить/починить автоматику",
                      "Открыть дашборд и админку"):
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

    def test_interrupt_returns_to_menu(self):
        """⌘. должен прерывать ТЕКУЩЕЕ действие (tail, парсинг) и возвращать в
        меню — а не убивать пульт: из «живого лога» иначе не выйти вовсе."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "trap ':' INT" in text

    def test_no_region_codes_in_header(self):
        """Юрист читает «ХМАО-Югра», а не внутренний код hmao: имя даёт
        get_region().name через cloud_run_ok --report; пульт сам коды не
        печатает и не хардкодит."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "hmao:" not in text and "sverdlovsk_yanao" not in text
        assert "get_region().name" in text

    def test_holiday_asks_instead_of_silent_exit(self):
        """[1] в выходной: штатный запуск тихо выходит «нерабочий день» — для
        юриста это поломка. Пульт обязан спросить (зеркало ignore_calendar
        облачной админки) и передать --ignore-calendar."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "is_russian_working_day" in text
        assert "Всё равно прогнать" in text
        assert "--ignore-calendar" in text

    def test_header_is_cached(self):
        """Шапка (питон + сеть, 3-5 с) считается в файл и пересобирается
        только действиями, меняющими состояние, — иначе каждое нажатие в
        меню ждало бы пересчёта."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "build_header" in text
        assert text.count("build_header") >= 4, \
            "после действий [1]/[2]/[5] шапка не пересобирается"

    def test_live_log_follows_rotated_files(self):
        """Логи ротируются через mv (tail>tmp && mv): tail -f держит старый
        файл и молча замолкает — обязателен -F (следить по имени)."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "tail -n 8 -F" in text and "tail -n 0 -F" in text

    def test_ignore_calendar_reaches_python(self):
        """Флаг пульта обязан доехать до run_parse.py: календарь решает Python
        (SKIP_NON_WORKING_DAYS), в shell своей копии календаря нет."""
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "--ignore-calendar) IGNORE_CALENDAR=1" in text
        assert 'SKIP_NON_WORKING_DAYS=$([ "$IGNORE_CALENDAR" = "1" ]' in text


class TestProbeSample:
    """Состав выборочной пробы — решение юриста 18.08.2026: кассация + ВСЕ
    апелляции + случайные 3 суда Свердловской обл. (обязательно с одним судом
    Екатеринбурга) + 3 ЯНАО + 3 ХМАО."""

    def test_composition(self):
        """Цикл по сидам, не один запуск: постоянные присутствия делят домен
        с родительским судом (Покачи, Пышма, Ачит), и жребий без дедупа по
        домену давал зоне 2 строки вместо 3 на ~2% сидов — одиночный запуск
        такое ловил раз в месяц (плавающее падение 20.08.2026)."""
        state = random.getstate()
        try:
            for seed in range(300):
                random.seed(seed)
                targets = probe_sample.build_targets()
                assert len(targets) == 13, \
                    f"seed={seed}: целей {len(targets)}, а не 13"
                labels = [l for l, _ in targets]
                domains = [d for _, d in targets]
                assert len(set(domains)) == 13, f"seed={seed}: домен задвоился"
                assert "7kas.sudrf.ru" in domains, "кассация выпала из пробы"
                for ap in ("oblsud--hmao.sudrf.ru", "oblsud--svd.sudrf.ru",
                           "oblsud--ynao.sudrf.ru"):
                    assert ap in domains, f"апелляция {ap} выпала из пробы"
                for zone in ("Свердловская обл. ·", "ЯНАО ·", "ХМАО ·"):
                    assert sum(1 for l in labels if l.startswith(zone)) == 3, \
                        f"seed={seed}: в зоне «{zone}» не три суда"
        finally:
            random.setstate(state)

    def test_ekb_court_is_guaranteed(self):
        """В свердловской тройке обязателен суд Екатеринбурга — там основной
        объём дел банка, и проба без ЕКБ ничего не говорит о главном."""
        for _ in range(5):
            labels = [l for l, _ in probe_sample.build_targets()]
            svd = [l for l in labels if l.startswith("Свердловская обл.")]
            assert any("Екатеринбург" in l for l in svd)

    def test_sample_is_random(self):
        """Тройки новые на каждый запуск — за неделю проверок покрывается
        заметная часть реестра (одинаковая пятёрка выборок из 8·C(54,2)·C(12,3)
        вариантов означала бы сломанный random)."""
        draws = {tuple(d for _, d in probe_sample.build_targets())
                 for _ in range(5)}
        assert len(draws) > 1

    def test_wired_into_pult_check(self):
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "probe_sample.py" in text
        assert "Выборочная проба судов" in text


class TestTerritoryChoice:
    def test_menu_offers_single_territory(self):
        """Отдельный запуск ХМАО и Урала (просьба юриста 18.08.2026): Enter —
        обе, цифра — одна; одна территория идёт напрямую через parse_and_push,
        не через parse_all."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "Какую территорию?" in text
        assert 'parse_and_push.sh" "$ONE_REPO" --force --anywhere' in text

    def test_colors_are_tty_gated(self):
        """ANSI-цвет только в настоящем терминале: escape-мусор в пайпе теста
        или логе launchd хуже отсутствия цвета."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "[ -t 1 ]" in text
        gate = text.index('if [ -t 1 ] && [ -n "${TERM:-}" ]; then')
        assert text.index("C_OK=") > gate

    def test_check_scripts_not_piped_through_paint(self):
        """log() скриптов печатает на экран только при живом терминале —
        пайп через paint выключил бы их вывод целиком."""
        text = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert 'parse_all.sh" --check --anywhere | paint' not in text
        assert 'import_all.sh" --check --anywhere | paint' not in text


class TestWorkerConfShared:
    def test_import_uses_cm_worker_conf(self):
        """Парсер конфига worker.<регион> — ОДИН (cm_worker_conf в lib):
        копия awk-строк в каждом потребителе разъехалась бы, как разъезжались
        списки файлов и доменов."""
        imp = _read("ops/mac-local-run/import_dumps.sh")
        assert "cm_worker_conf" in imp
        assert "awk -F= '/^owner_secret=/" not in imp, \
            "import_dumps парсит конфиг сам, мимо cm_worker_conf"
        pult = _read("ops/mac-local-run/СберСуд-пульт.command")
        assert "cm_worker_conf" in pult
        assert "awk -F= '/^owner_secret=/" not in pult

    def test_lib_reads_without_source(self):
        lib = _read("ops/mac-local-run/lib_sber_net.sh")
        block = lib[lib.index("cm_worker_conf()"):]
        block = block[:block.index("\n}")]
        assert "awk -F=" in block and "source" not in block


class TestUnavailabilityTail:
    """«11%» не должно читаться как поломка системы, когда лёг портал.

    24.08.2026 по ХМАО вердикт был «прочитано 36 из 323 (11% — порог 85%)»,
    хотя апелляция дала 34 из 34, кассация 2 из 2, а все 20 судов 1-й
    инстанции отдавали заглушку: прочитано было 100% доступного.
    """

    def test_tail_names_courts_and_cards(self):
        state = {"last_run": {"cards_unreachable": 283,
                              "cards_unread_other": 4,
                              "courts_unavailable": 0,
                              "courts_with_unrequested": 20,
                              "courts_outage": 20}}
        tail = cloud_run_ok.unavailability_tail(state)
        assert "283 карточки не запрошены" in tail and "20 судов" in tail
        assert "заглушка портала замечена" in tail
        assert "ещё 4 карточки не прочитаны" in tail
        assert "по другим причинам" in tail

    def test_tail_silent_without_numbers(self):
        """Строки обычного дня обязаны остаться побайтово прежними."""
        assert cloud_run_ok.unavailability_tail({"last_run": {}}) == ""
        assert cloud_run_ok.unavailability_tail({}) == ""

    def test_outage_part_optional(self):
        """Суд могли снять и по отказам карточек, а не по заглушке поиска."""
        state = {"last_run": {"cards_unreachable": 5, "courts_unavailable": 1}}
        tail = cloud_run_ok.unavailability_tail(state)
        assert tail and "заглушка портала" not in tail

    def test_other_unread_is_visible_without_open_breaker(self):
        state = {"last_run": {"cards_unread_other": 3}}
        tail = cloud_run_ok.unavailability_tail(state)
        assert "3 карточки не прочитаны" in tail
        assert "по другим причинам" in tail

    def test_denominator_stays_full(self):
        """⚠️ Знаменатель НЕ уменьшается — решение 24.08.2026: вердикт правит
        только текст алерта, и «удачный прогон» в день полного аутейджа
        заглушил бы предупреждение ровно тогда, когда оно нужнее всего."""
        import datetime as _dt
        # cards_progress читает только СЕГОДНЯШНИЙ блок — без штампа `at`
        # сводка считается отсутствующей и знаменатель проверить нечем.
        state = {"last_run": {"at": _dt.date.today().isoformat(),
                              "cards_read": 36, "cards_planned": 323,
                              "cards_unreachable": 283,
                              "cards_unread_other": 4,
                              "courts_unavailable": 0,
                              "courts_with_unrequested": 20,
                              "courts_outage": 20}}
        line = cloud_run_ok.progress_line(state)
        assert "36 из 323" in line, "знаменатель ужали — предупреждение замолчит"
        assert "283 карточки не запрошены" in line
        assert "ещё 4 карточки не прочитаны" in line



class TestDeliveryIdentity:
    """Rollback обязан снимать штамп именно своего выпуска.

    Без delivery_id запоздавшая ошибка push могла бы снять
    delivered_at уже нового контекста и разрешить дубль.
    """

    ISSUE_KEY = cloud_run_ok.dt.datetime.now().strftime("%Y-%m-%dT08:25:42")

    @staticmethod
    def _setup_ctx(tmp_path, monkeypatch, *, issue_key=None):
        issue_key = issue_key or TestDeliveryIdentity.ISSUE_KEY
        ctx = tmp_path / "ctx.json"
        ctx.write_text(json.dumps({
            "saved_at": issue_key,
            "issue_key": issue_key,
            "fi_changes": [{"case": "2-1/2026"}],
        }), encoding="utf-8")
        from court_monitor import config as cm_config
        monkeypatch.setattr(cm_config, "LAST_DIGEST_CONTEXT_PATH", str(ctx))
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        return ctx

    def test_mark_prints_stable_id_and_is_idempotent(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        expected = f"hmao:{self.ISSUE_KEY}"

        assert cloud_run_ok._mark_delivered() == 0
        assert capsys.readouterr().out.strip() == expected
        first = json.loads(ctx.read_text(encoding="utf-8"))
        first_bytes = ctx.read_bytes()
        assert first["delivery_id"] == expected
        assert first.get("delivered_at")

        assert cloud_run_ok._mark_delivered() == 0
        assert capsys.readouterr().out.strip() == expected
        assert ctx.read_bytes() == first_bytes, (
            "повторный mark сдвинул штамп или переписал контекст"
        )

    def test_mark_fails_closed_without_issue_key(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        data = json.loads(ctx.read_text(encoding="utf-8"))
        data.pop("issue_key")
        ctx.write_text(json.dumps(data), encoding="utf-8")
        before = ctx.read_bytes()

        assert cloud_run_ok._mark_delivered() == 1
        assert "issue_key" in capsys.readouterr().out
        assert ctx.read_bytes() == before

    def test_unmark_requires_exact_delivery_id(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        expected = f"hmao:{self.ISSUE_KEY}"
        assert cloud_run_ok._mark_delivered() == 0
        capsys.readouterr()
        before = ctx.read_bytes()

        assert cloud_run_ok._unmark_delivered("hmao:чужой-выпуск") == 1
        assert "не совпал" in capsys.readouterr().out
        assert ctx.read_bytes() == before
        assert cloud_run_ok._unmark_delivered() == 1
        assert "не указан" in capsys.readouterr().out
        assert ctx.read_bytes() == before

        assert cloud_run_ok._unmark_delivered(expected) == 0
        assert "штамп доставки снят" in capsys.readouterr().out
        rolled_back = json.loads(ctx.read_text(encoding="utf-8"))
        assert "delivered_at" not in rolled_back
        assert rolled_back["delivery_id"] == expected

        # Повтор того же rollback не должен ложно стать аварией.
        assert cloud_run_ok._unmark_delivered(expected) == 0

    def test_cli_unmark_requires_delivery_id(self, capsys):
        assert cloud_run_ok.main(["--unmark-delivered"]) == 2
        assert "--delivery-id ID" in capsys.readouterr().out

    def test_cli_delivery_id_is_read_only(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        before = ctx.read_bytes()
        assert cloud_run_ok.main(["--delivery-id"]) == 0
        assert capsys.readouterr().out.strip() == f"hmao:{self.ISSUE_KEY}"
        assert ctx.read_bytes() == before

    def test_same_issue_key_is_isolated_by_region(
        self, tmp_path, monkeypatch,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        from court_monitor import config as cm_config

        hmao_id = cloud_run_ok._context_delivery_id(
            json.loads(ctx.read_text(encoding="utf-8"))
        )
        monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
        ural_id = cloud_run_ok._context_delivery_id(
            json.loads(ctx.read_text(encoding="utf-8"))
        )

        assert hmao_id == f"hmao:{self.ISSUE_KEY}"
        assert ural_id == f"sverdlovsk_yanao:{self.ISSUE_KEY}"
        assert ural_id != hmao_id

    def test_cli_delivery_id_fails_without_issue_key(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        ctx.write_text(json.dumps({"saved_at": self.ISSUE_KEY}),
                       encoding="utf-8")
        assert cloud_run_ok.main(["--delivery-id"]) == 1
        assert "issue_key" in capsys.readouterr().out

    def test_stale_context_cannot_be_marked_as_today(
        self, tmp_path, monkeypatch, capsys,
    ):
        yesterday = (
            cloud_run_ok.dt.datetime.now() - cloud_run_ok.dt.timedelta(days=1)
        ).isoformat(timespec="seconds")
        ctx = self._setup_ctx(tmp_path, monkeypatch, issue_key=yesterday)
        before = ctx.read_bytes()

        assert cloud_run_ok.main(["--delivery-id"]) == 1
        assert "не свежий" in capsys.readouterr().out
        assert cloud_run_ok._mark_delivered() == 1
        assert "delivery_id не построен" in capsys.readouterr().out
        assert ctx.read_bytes() == before, (
            "вчерашний контекст получил сегодняшний stamp и уйдёт повторно"
        )

    def test_unmark_save_failure_is_nonzero(
        self, tmp_path, monkeypatch, capsys,
    ):
        self._setup_ctx(tmp_path, monkeypatch)
        expected = f"hmao:{self.ISSUE_KEY}"
        assert cloud_run_ok._mark_delivered() == 0
        capsys.readouterr()

        def _cannot_save(_path, _ctx):
            raise OSError("диск недоступен")

        monkeypatch.setattr(cloud_run_ok, "_save_context", _cannot_save)
        assert cloud_run_ok._unmark_delivered(expected) == 1
        assert "не удалось сохранить откат" in capsys.readouterr().out

    def test_partial_mark_write_preserves_previous_context(
        self, tmp_path, monkeypatch, capsys,
    ):
        ctx = self._setup_ctx(tmp_path, monkeypatch)
        before = ctx.read_bytes()

        def _partial_then_fail(_value, stream, **_kwargs):
            stream.write('{"delivery_id":')
            raise OSError("disk full")

        monkeypatch.setattr(cloud_run_ok.json, "dump", _partial_then_fail)
        assert cloud_run_ok._mark_delivered() == 1
        assert "не удалось сохранить штамп" in capsys.readouterr().out
        assert ctx.read_bytes() == before
        assert not list(tmp_path.glob(".ctx.json.*.tmp"))
