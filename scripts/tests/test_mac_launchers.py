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


class TestSightedRunToday:
    TODAY = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    OLD = "2026-01-01T08:00:00"

    def test_sighted_today(self):
        ok, text = cloud_run_ok.sighted_run_today(
            {"sources": {"a": _src(self.TODAY, 24), "b": _src(self.TODAY, 0)}})
        assert ok and "отработало" in text

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
        probe = text.index("cm_any_court_reachable")
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


# ── Канарейка судов: тихие ретраи, один алерт в день ─────────────────────────

class TestCanaryQuietRetries:
    """Слоты агента идут каждые полчаса и сами добивают сорвавшуюся пробу
    (20.08.2026 Урал: отказ в 08:19 → спарсился в 08:30), а алерт на КАЖДУЮ
    неудачу дал 5 одинаковых сообщений за утро. Агентская ветка обязана
    молчать до конца окна и кричать один раз в день."""

    def test_quiet_until_window_end_then_single_alert(self):
        text = _read("ops/mac-local-run/parse_and_push.sh")
        assert "PROBE_ALERT_AFTER_MIN=655" in text, \
            "порог 10:55 пропал — либо алерты вернутся на каждый слот, либо их не будет вовсе"
        assert ".alerted-parse-" in text, "дневной дедуп алерта пропал"
        body = text[text.index("probe_failed()"):]
        body = body[:body.index('if PROBE_HOST=')]
        assert '"$FORCE" = "1"' in body, "ручной запуск (--force) обязан кричать сразу"
        assert "exit 0" in body, "тихая ветка обязана выходить без ошибки"
        assert "finish_pusher" in body, "pusher уже запущен к моменту пробы — его надо дождаться"

    def test_import_agent_gets_anywhere_only_with_quiet_canary(self):
        """--anywhere у агентов появился вместе с тихой канарейкой: вернуть
        алерт на каждую неудачу при 12 запусках в день — снова шторм."""
        text = _read("ops/mac-local-run/import_dumps.sh")
        assert ".alerted-dumps-" in text, "дневной дедуп алерта дампов пропал"


# ── Расписания агентов (plistlib: в CI нет plutil) ───────────────────────────

class TestAgentSchedules:
    def test_parse_slots(self):
        """С 19.08.2026 облачный крон ВЫКЛЮЧЕН (crons = [] в wrangler.toml
        обеих территорий) и агент — основной путь парсинга: слоты каждые
        30 минут с 08:00 до 11:00 (решение юриста). Лишние запуски бесплатны
        — гейт cloud_run_ok видит уже состоявшийся зрячий прогон, лок на клон
        не пускает параллель; плотная сетка добивает сорвавшуюся пробу судов
        (19.08.2026: проба ХМАО прошла с третьего раза) и поздний старт Mac."""
        assert _slots(PARSE_PLIST) == {
            (w, h, m) for w in range(1, 6)
            for h, m in ((8, 0), (8, 30), (9, 0), (9, 30),
                         (10, 0), (10, 30), (11, 0))}

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
        targets = probe_sample.build_targets()
        assert len(targets) == 13
        labels = [l for l, _ in targets]
        domains = [d for _, d in targets]
        assert "7kas.sudrf.ru" in domains, "кассация выпала из пробы"
        for ap in ("oblsud--hmao.sudrf.ru", "oblsud--svd.sudrf.ru",
                   "oblsud--ynao.sudrf.ru"):
            assert ap in domains, f"апелляция {ap} выпала из пробы"
        for zone in ("Свердловская обл. ·", "ЯНАО ·", "ХМАО ·"):
            assert sum(1 for l in labels if l.startswith(zone)) == 3, \
                f"в зоне «{zone}» не три суда"

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
