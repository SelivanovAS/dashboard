# -*- coding: utf-8 -*-
"""Общие правила приёма в трек «Иски банка» (court_monitor/bank_intake.py).

Модуль — единственный источник правды для трёх каналов ввода (реестр, разовый
сборщик выдачи, авто-подхват прогона). Тут проверяем сами правила и сборку
записи; e2e каналов — в их собственных файлах.
"""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import bank_intake  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor import runs  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402


def _court(domain: str = "surggor--hmao.sudrf.ru", srv: int = 1):
    return next(c for c in get_region("hmao").first_instance_courts
                if c.domain == domain and c.srv_num == srv)


# ── Ре-экспорты: каналы обязаны звать один и тот же код ──────────────────────

class TestReExports:
    def test_collector_reuses_shared_rules(self):
        import collect_bank_claims as cbc

        assert cbc.row_passes is bank_intake.row_passes
        assert cbc.card_rejects is bank_intake.card_rejects
        assert cbc._EXCLUDED_RESULT_RX is bank_intake._EXCLUDED_RESULT_RX

    def test_registry_reuses_shared_entry_builder(self):
        import import_bank_registry as ibr

        assert ibr.make_bank_entry is bank_intake.make_bank_entry


# ── card_rejects ─────────────────────────────────────────────────────────────

class TestCardRejects:
    @staticmethod
    def _decided(**extra):
        card = {
            "Статус": "Решено",
            "Дата заседания": "12.02.2026",
            "_events": [{"date": "12.02.2026",
                         "text": "Вынесено решение по делу. Иск удовлетворён"}],
        }
        card.update(extra)
        return card

    def test_clean_card_passes(self):
        assert bank_intake.card_rejects(self._decided()) == ""

    def test_excluded_result_from_card(self):
        """Выдача отстаёт от карточки — итог виден только в ней."""
        card = self._decided(**{"Результат": "Дело передано ПО ПОДСУДНОСТИ"})
        assert bank_intake.card_rejects(card) == "excluded_result"

    def test_refusal_is_not_excluded(self):
        """«Отказано» берём — по нему возможна апелляция банка."""
        card = self._decided(**{"Результат": "ОТКАЗАНО в удовлетворении иска"})
        assert bank_intake.card_rejects(card) == ""

    @pytest.mark.parametrize("flag", [
        "_fi_appeal_filed", "_fi_sent_to_appeal",
        "_fi_cassation_filed", "_fi_sent_to_cassation",
    ])
    def test_appeal_flags_reject_when_asked(self, flag):
        assert bank_intake.card_rejects(
            self._decided(**{flag: True}), skip_appeal=True) == "excluded_appeal"

    @pytest.mark.parametrize("flag", [
        "_fi_appeal_filed", "_fi_sent_to_appeal",
        "_fi_cassation_filed", "_fi_sent_to_cassation",
    ])
    def test_appeal_flags_pass_for_auto_intake(self, flag):
        """Решение юриста 31.07.2026: авто-подхват такие дела БЕРЁТ — они
        переезжают в основную картотеку и встают на мониторинг апелляции."""
        assert bank_intake.card_rejects(
            self._decided(**{flag: True}), skip_appeal=False) == ""

    @pytest.mark.parametrize("status", ["Выдан", "Отозван", "Возвращен"])
    def test_enforcement_writ_rejects_any_status(self, status):
        card = self._decided(_writs=[{"issue_date": "20.04.2026", "status": status}])
        assert bank_intake.card_rejects(card) == "excluded_writ"

    def test_interim_writ_passes(self):
        """Обеспечительный лист (выдан ДО решения) — дело ещё ждёт ИЛ."""
        card = self._decided(_writs=[{"issue_date": "01.11.2025", "status": "Выдан"}])
        assert bank_intake.card_rejects(card) == ""

    def test_writ_without_anchor_passes(self):
        """Ни решения, ни терминального статуса → interim, не пропуск."""
        card = {"_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]}
        assert bank_intake.card_rejects(card) == ""

    def test_decided_card_without_decision_event_uses_hearing_anchor(self):
        card = {"Статус": "Решено", "Дата заседания": "12.02.2026",
                "_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]}
        assert bank_intake.card_rejects(card) == "excluded_writ"

    def test_result_checked_before_appeal_and_writ(self):
        """Порядок причин стабилен: итог важнее прочего (отчёты каналов
        считают по одной причине на дело)."""
        card = self._decided(
            **{"Результат": "Производство по делу ПРЕКРАЩЕНО",
               "_fi_appeal_filed": True})
        assert bank_intake.card_rejects(card) == "excluded_result"


# ── delo_id / srv_num в записи ───────────────────────────────────────────────

class TestCourtIds:
    """Ссылку «в суд» фронт собирает из delo_id/srv_num (фолбэк 1540005/1);
    у записей ручных каналов их не было вовсе."""

    @staticmethod
    def _row(**extra):
        row = {
            "case_number": "2-100/2026", "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.", "category": "Кредит", "court": "Суд",
            "court_domain": "surggor--hmao.sudrf.ru", "judge": "Судья",
            "filing_date": "01.02.2026", "status": "В производстве",
            "result": "", "link": "1|a-1", "bank_role": "Истец",
        }
        row.update(extra)
        return row

    def test_ids_from_search_row(self):
        entry = bank_intake.make_bank_entry(
            self._row(court_delo_id=1540005, court_srv_num=1), {}, "тест", "now")
        assert entry["first_instance"]["delo_id"] == 1540005
        assert entry["first_instance"]["srv_num"] == 1

    def test_href_srv_wins_over_config(self):
        """Двухсерверные домены: href строки авторитетнее конфига."""
        entry = bank_intake.make_bank_entry(
            self._row(court_delo_id=1540005, court_srv_num=1, href_srv_num=2),
            {}, "тест", "now")
        assert entry["first_instance"]["srv_num"] == 2

    def test_ids_from_court_when_row_has_none(self):
        """Целевой поиск по номеру (parse_search_row) этих ключей не отдаёт."""
        court = _court()
        entry = bank_intake.make_bank_entry(
            self._row(), {}, "тест", "now", court=court)
        assert entry["first_instance"]["delo_id"] == court.delo_id
        assert entry["first_instance"]["srv_num"] == court.srv_num

    def test_no_ids_no_court_leaves_keys_absent(self):
        fi = bank_intake.make_bank_entry(self._row(), {}, "тест", "now")["first_instance"]
        assert "srv_num" not in fi

    def test_track_markers_intact(self):
        entry = bank_intake.make_bank_entry(self._row(), {}, "оператор", "now")
        assert entry["track"] == "plaintiff_light"
        assert entry["import"]["announced"] is True
        assert entry["initial_bank_role"] == "Истец"


# ── Дата проверки при заведении (карточку читал импорт) ─────────────────────

class TestIntakeCheckedStamp:
    """Без штампа ветка force-parse в should_skip_case перебивает ВСЁ —
    и будущее заседание, и недельный ритм ИЛ (разгон Урала 14.08.2026)."""

    @staticmethod
    def _card(**extra):
        # _table_count — настоящая карточка (у заглушки таблиц 0).
        card = {"Статус": "В производстве", "_table_count": 5, "_events": [
            {"date": "01.08.2026", "text": "Регистрация иска"},
        ]}
        card.update(extra)
        return card

    def test_stamp_is_date_only(self):
        """⚠️ Полный таймстамп should_skip_case не разберёт (date.fromisoformat
        бросит ValueError) и уйдёт в тот же force-parse — правка холостая."""
        from datetime import date

        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(), self._card(), "оператор",
            "2026-08-14T05:53:09")["first_instance"]
        assert fi["last_checked_at"] == "2026-08-14"
        assert date.fromisoformat(fi["last_checked_at"]) == date(2026, 8, 14)

    def test_intake_marker_set(self):
        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(), self._card(), "оператор",
            "2026-08-14T05:53:09")["first_instance"]
        assert fi["intake_card_parse"] is True

    def test_empty_now_iso_leaves_key_absent(self):
        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(), self._card(), "оператор", "")["first_instance"]
        assert "last_checked_at" not in fi

    def test_garbage_now_iso_not_stamped(self):
        """Лучше лишний парс, чем «проверено» наугад: битую строку в поле
        should_skip_case всё равно не разберёт, а в данных она осядет."""
        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(), self._card(), "оператор", "вчера")["first_instance"]
        assert "last_checked_at" not in fi
        assert "intake_card_parse" not in fi

    def test_shell_card_not_stamped(self):
        """Заглушка sudrf (HTTP 200, ноль таблиц) — не проверка. У приёма нет
        второго рубежа FI-цикла, а «Решено» запись берёт и из строки выдачи:
        штамп дал бы неделю тишины по writ_weekly без единого чтения."""
        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(status="Решено"),
            {"_table_count": 0}, "оператор",
            "2026-08-14T05:53:09")["first_instance"]
        assert "last_checked_at" not in fi
        assert "intake_card_parse" not in fi

    def test_future_hearing_skipped_right_after_intake(self):
        """Ради этого всё и делается: заведённое дело с заседанием впереди
        прогон читать не должен."""
        from datetime import date

        from court_monitor import lifecycle

        entry = bank_intake.make_bank_entry(
            TestCourtIds._row(), self._card(_events=[
                {"date": "20.09.2026", "text": "Судебное заседание. 10:00"},
            ]), "оператор", "2026-08-14T05:53:09")
        skip, reason = lifecycle.should_skip_case(entry, date(2026, 8, 17))
        assert skip is True
        assert reason.startswith("future_hearing")

    def test_resolved_case_goes_to_weekly_writ_rhythm(self):
        """Решённое дело без штампа парсилось бы каждым прогоном, хотя для
        него и придуман недельный ритм ожидания ИЛ."""
        from datetime import date

        from court_monitor import lifecycle

        entry = bank_intake.make_bank_entry(
            TestCourtIds._row(status="Решено"),
            self._card(**{"Статус": "Решено", "_events": [
                {"date": "10.08.2026", "text": "Решение. Иск удовлетворен"},
            ]}),
            "оператор", "2026-08-14T05:53:09")
        skip, reason = lifecycle.should_skip_case(entry, date(2026, 8, 17))
        assert skip is True
        assert reason.startswith("writ_weekly")


# ── Перенос признаков жалобы (решение №3: такие дела подхват берёт) ──────────

class TestAppealFlagsCarried:
    def test_flags_stamped_from_card(self):
        """Без переноса bank_case_left_track увидел бы дело «чистым», и оно
        неделю висело бы в лёгком треке (следующий парс — writ_weekly)."""
        from court_monitor import lifecycle
        entry = bank_intake.make_bank_entry(
            TestCourtIds._row(), {"_fi_appeal_filed": True,
                                  "_fi_appeal_filed_date": "20.07.2026"},
            "auto", "now")
        assert entry["first_instance"]["appeal_filed"] is True
        assert entry["first_instance"]["appeal_filed_date"] == "20.07.2026"
        # Тем же прогоном дело уезжает в основной cases.json.
        assert lifecycle.bank_case_left_track(entry) is True

    def test_clean_card_leaves_no_flags(self):
        fi = bank_intake.make_bank_entry(
            TestCourtIds._row(), {}, "auto", "now")["first_instance"]
        assert "appeal_filed" not in fi and "sent_to_cassation" not in fi


# ── Негативный кэш ───────────────────────────────────────────────────────────

class TestIntakeSeenCache:
    def test_permanent_reasons_remembered(self):
        seen: dict = {}
        assert bank_intake.remember_rejection(
            seen, "x.sudrf.ru", "2-1/2026", "excluded_writ") is True
        assert seen["x.sudrf.ru|2-1/2026"]["reason"] == "excluded_writ"

    @pytest.mark.parametrize("reason", ["fetch_fail", "breaker", "role"])
    def test_transient_reasons_not_remembered(self, reason):
        """Сетевой сбой должен ретраиться следующим прогоном."""
        seen: dict = {}
        assert bank_intake.remember_rejection(
            seen, "x.sudrf.ru", "2-1/2026", reason) is False
        assert seen == {}

    def test_prune_drops_stale(self):
        from datetime import date, timedelta
        today = date(2026, 8, 1)
        old = (today - timedelta(days=cm_config.BANK_INTAKE_SEEN_TTL_DAYS + 5))
        seen = {
            "x|свежее": {"reason": "excluded_writ", "last_seen": today.isoformat()},
            "x|старое": {"reason": "excluded_writ", "last_seen": old.isoformat()},
        }
        assert list(bank_intake.prune_intake_seen(seen, today)) == ["x|свежее"]

    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / ".bank_intake_seen.json")
        seen: dict = {}
        bank_intake.remember_rejection(seen, "x.sudrf.ru", "2-1/2026", "no_link")
        bank_intake.save_intake_seen(seen, path)
        assert bank_intake.load_intake_seen(path) == seen

    def test_missing_and_broken_file_read_as_empty(self, tmp_path):
        """Сервисные данные не имеют права ронять прогон."""
        missing = str(tmp_path / "нет.json")
        assert bank_intake.load_intake_seen(missing) == {}
        broken = tmp_path / "битый.json"
        broken.write_text("{не json", encoding="utf-8")
        assert bank_intake.load_intake_seen(str(broken)) == {}


# ── intake_bank_rows: приём кандидатов в прогоне (блок 3b фазы 3) ───────────

@pytest.fixture
def net(monkeypatch):
    """Сеть подхвата: карточка — «живое дело в производстве»."""
    monkeypatch.setattr(runs, "polite_delay", lambda: None)
    monkeypatch.setattr(runs, "fetch_card_checked",
                        lambda url, context=None, breaker_gate=True: "<html/>")
    monkeypatch.setattr(runs, "parse_case_card",
                        lambda html, base_url=None: {"Статус": "В производстве"})
    monkeypatch.setattr(cm_config, "BANK_INTAKE_DRY_RUN", False)
    monkeypatch.setattr(cm_config, "CARD_BREAKER_THRESHOLD", 5)
    cm_config.CARD_BREAKER.clear()
    for k in ("bank_intake_candidates", "bank_intake_cards", "bank_intake_added"):
        cm_config.METRICS[k] = 0


def _row(num: str, role: str = "Истец", result: str = "", link: str = "1|a-1"):
    return {"case_number": num, "bank_role": role, "result": result, "link": link,
            "plaintiff": "ПАО Сбербанк", "defendant": "Иванов И.И.",
            "category": "Кредит", "court": "Суд", "judge": "Судья",
            "court_domain": "surggor--hmao.sudrf.ru", "filing_date": "01.07.2026",
            "status": "В производстве", "court_delo_id": 1540005,
            "court_srv_num": 1}


def _intake(rows, *, seen=None, tracked=None, budget=30):
    return runs.intake_bank_rows(
        _court(), rows, dedup_exact=set(tracked or []), dedup_wildcard=set(),
        seen=seen if seen is not None else {}, budget=budget)


class TestIntakeBankRows:
    def test_plaintiff_row_becomes_track_entry(self, net):
        entries, c = _intake([_row("2-100/2026")])
        assert c["added"] == 1 and c["cards"] == 1
        assert entries[0]["track"] == "plaintiff_light"
        assert entries[0]["import"]["source"] == "auto_search"
        assert cm_config.METRICS["bank_intake_added"] == 1

    @pytest.mark.parametrize("role", ["Ответчик", "Третье лицо"])
    def test_other_roles_never_touched(self, net, role):
        """Ответчик-дела ведёт основной трек, третьи лица не отслеживаются."""
        entries, c = _intake([_row("2-100/2026", role=role)])
        assert (entries, c["cards"], c["role"]) == ([], 0, 1)

    def test_known_case_costs_no_http(self, net):
        entries, c = _intake(
            [_row("2-100/2026")],
            tracked=[("surggor--hmao.sudrf.ru", "2-100/2026")])
        assert (entries, c["already"], c["cards"]) == ([], 1, 0)

    def test_card_rejection_cached_and_second_run_free(self, net, monkeypatch):
        monkeypatch.setattr(
            runs, "parse_case_card",
            lambda html, base_url=None: {
                "Статус": "Решено", "Дата заседания": "12.02.2026",
                "_events": [{"date": "12.02.2026",
                             "text": "Вынесено решение по делу"}],
                "_writs": [{"issue_date": "20.04.2026", "status": "Выдан"}]})
        seen: dict = {}
        entries, c = _intake([_row("2-100/2026")], seen=seen)
        assert (entries, c["excluded_writ"], c["cards"]) == ([], 1, 1)
        # Второй прогон видит ту же строку — карточку уже не качает.
        entries2, c2 = _intake([_row("2-100/2026")], seen=seen)
        assert (entries2, c2["seen_cached"], c2["cards"]) == ([], 1, 0)

    def test_excluded_result_needs_no_card(self, net):
        entries, c = _intake([_row("2-100/2026", result="Дело передано ПО ПОДСУДНОСТИ")])
        assert (entries, c["cards"], c["excluded_result"]) == ([], 0, 1)

    def test_appeal_case_taken(self, net, monkeypatch):
        """Решение юриста 31.07.2026 — обратное правилу ручного сборщика."""
        monkeypatch.setattr(
            runs, "parse_case_card",
            lambda html, base_url=None: {"Статус": "Решено",
                                         "_fi_appeal_filed": True})
        entries, c = _intake([_row("2-100/2026")])
        assert c["added"] == 1
        assert entries[0]["first_instance"]["appeal_filed"] is True

    def test_budget_caps_run(self, net):
        rows = [_row(f"2-10{i}/2026") for i in range(5)]
        entries, c = _intake(rows, budget=2)
        assert (len(entries), c["capped"]) == (2, 3)

    def test_per_court_card_cap(self, net, monkeypatch):
        """Фаза 3 идёт раньше FI-цикла: пачка карточек одного суда не должна
        в одиночку открывать предохранитель."""
        monkeypatch.setattr(cm_config, "BANK_INTAKE_MAX_CARDS_PER_COURT", 2)
        rows = [_row(f"2-10{i}/2026") for i in range(5)]
        entries, c = _intake(rows)
        assert (len(entries), c["cards"], c["capped"]) == (2, 2, 3)

    def test_dry_run_makes_no_requests(self, net, monkeypatch):
        monkeypatch.setattr(cm_config, "BANK_INTAKE_DRY_RUN", True)
        entries, c = _intake([_row("2-100/2026")])
        assert (entries, c["cards"], c["candidates"]) == ([], 0, 1)

    def test_open_breaker_skips_without_http(self, net, monkeypatch):
        def boom(*a, **kw):
            raise AssertionError("карточка не должна качаться при открытом предохранителе")
        monkeypatch.setattr(runs, "fetch_card_checked", boom)
        cm_config.CARD_BREAKER["surggor--hmao.sudrf.ru"] = {
            "fails": 5, "open": True, "reason": "заглушка", "skipped": 0,
            "probes": 0}
        monkeypatch.setattr(cm_config, "CARD_BREAKER_PROBE_EVERY", 0)
        entries, c = _intake([_row("2-100/2026")])
        assert (entries, c["breaker"]) == ([], 1)

    def test_fetch_failure_not_cached(self, net, monkeypatch):
        """Сбой сети — не приговор: следующий прогон попробует снова."""
        monkeypatch.setattr(runs, "fetch_card_checked",
                            lambda url, context=None, breaker_gate=True: "")
        seen: dict = {}
        entries, c = _intake([_row("2-100/2026")], seen=seen)
        assert (entries, c["fetch_fail"], seen) == ([], 1, {})


class TestIntakeWiring:
    """Блок 3b должен быть подключён к фазе 3 и к раскладке 7c, а рубильник —
    доезжать из Actions Variables (по образцу TestBankTrackWiring)."""

    @staticmethod
    def _src(rel: str) -> str:
        root = os.path.dirname(SCRIPTS_DIR)
        with open(os.path.join(root, rel), encoding="utf-8") as f:
            return f.read()

    def test_intake_called_in_run(self):
        src = self._src("scripts/court_monitor/runs.py")
        assert "intake_bank_rows(" in src
        assert "cases = bank_new_cases + cases" in src, (
            "подхваченные иски не вливаются в общий список — фаза 7c их не "
            "разложит по файлам трека"
        )

    def test_auto_intake_forwarded_from_variables(self):
        import re
        wf = self._src(".github/workflows/update_cases.yml")
        m = re.search(r"^\s*BANK_AUTO_INTAKE:\s*(.+)$", wf, re.M)
        assert m and "vars.BANK_AUTO_INTAKE" in m.group(1), (
            "BANK_AUTO_INTAKE не прокинут — переменная территории не сработает"
        )
        assert re.search(r"\|\|\s*'1'", m.group(1)), "нет фолбэка '1'"

    def test_seen_cache_committed(self):
        """С 16.08.2026 список коммитимых файлов один на облако и Mac-резерв
        (ops/stage_data_files.sh спрашивает пути у config) — проверяем там же,
        где он теперь и живёт."""
        import subprocess
        root = os.path.dirname(SCRIPTS_DIR)
        out = subprocess.run(["bash", "ops/stage_data_files.sh", "--list"],
                             cwd=root, capture_output=True, text=True,
                             check=True).stdout
        assert "data/.bank_intake_seen.json" in out, (
            "негативный кэш не коммитится — отказники будут перекачиваться "
            "каждым прогоном"
        )
        assert "stage_data_files.sh" in self._src(
            ".github/workflows/update_cases.yml"), "workflow не зовёт хелпер"
