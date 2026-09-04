"""
Стражи окна ожидания апел. жалобы по решённому делу 1-й инст. (04.09.2026).

Повод — 2-857/2026 (Володин → ПАО Сбербанк, Нижневартовский гор. суд): в иске
отказано 25.06.2026, мотивировка изготовлена 10.08.2026 (срок на жалобу по
ст. 321 ГПК — до 10.09), а плоские FI_ARCHIVE_DAYS=60 от даты ЗАСЕДАНИЯ увели
дело в архив 25.08 — мотивировка в правиле основной картотеки не участвовала
вовсе. Теперь окно считает `fi_appeal_window_end` по ГПК (мотивировка + месяц
+ запас FI_APPEAL_GRACE_DAYS=14; без мотивировки — резолютивка + 10 раб. дн
+ месяц + запас ≈ прежние 60), правило ОДНО на обе картотеки (решение юриста
04.09.2026), а фронт зеркалит его в `appealWindowEnd` (app.js).

Запуск: python3 -m pytest scripts/tests/test_fi_appeal_window.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, lifecycle  # noqa: E402

NODE = shutil.which("node")


def _dmy(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")


def _ev(d: str, text: str) -> dict:
    return {"date": d, "time": "10:00", "text": text}


MOTIV = "Изготовлено мотивированное решение в окончательной форме. 12:05. 20.08.2026"
DECIDED = "Судебное заседание. 09:45. 307. Вынесено решение по делу. ОТКАЗАНО в удовлетворении иска (заявлении, жалобы). 25.06.2026"


def _volodin(**over) -> dict:
    """Фикстура 2-857/2026 как в data/cases_archive.json на 04.09.2026."""
    fi = {
        "case_number": "2-857/2026 (2-7073/2025;)",
        "status": "Решено",
        "result": "ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)",
        "hearing_date": "25.06.2026",
        "decision_date": "25.06.2026",
        "event_date": "10.08.2026",
        "act_date": "",
        "events": [_ev("25.06.2026", DECIDED), _ev("10.08.2026", MOTIV)],
    }
    fi.update(over)
    return {"id": "2-857/2026 (2-7073/2025;)", "current_stage": "first_instance",
            "bank_role": "Ответчик", "first_instance": fi}


# ── Хелпер ───────────────────────────────────────────────────────────────────

class TestAppealWindowEnd:
    def test_motivirovka_from_events(self):
        assert lifecycle.fi_motivirovka_date_from_events(
            _volodin()["first_instance"]["events"]) == "10.08.2026"
        assert lifecycle.fi_motivirovka_date_from_events([]) == ""
        assert lifecycle.fi_motivirovka_date_from_events(None) == ""

    def test_last_motivirovka_wins(self):
        events = [_ev("01.03.2026", MOTIV), _ev("10.08.2026", MOTIV)]
        assert lifecycle.fi_motivirovka_date_from_events(events) == "10.08.2026"

    def test_volodin_window_ends_24_09(self, monkeypatch):
        """Мотивировка 10.08 → месяц до 10.09 (ст. 321/108) + 14 дн запаса."""
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 14)
        assert lifecycle.fi_appeal_window_end(_volodin()["first_instance"]) == date(2026, 9, 24)

    def test_volodin_active_on_04_09_archived_on_25_09(self, monkeypatch):
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 14)
        fi = _volodin()["first_instance"]
        assert lifecycle.fi_appeal_window_passed(fi, datetime(2026, 9, 4)) is False
        assert lifecycle.fi_appeal_window_passed(fi, datetime(2026, 9, 24)) is False
        assert lifecycle.fi_appeal_window_passed(fi, datetime(2026, 9, 25)) is True

    def test_without_motivirovka_window_matches_old_60_days(self, monkeypatch):
        """Суд не публикует событие мотивировки (18 из 24 архивных ХМАО):
        резолютивка 25.06 + 10 раб. дн (09.07, ст. 199) + месяц (09.08) + 14
        = 23.08 — окно НЕ короче прежних 60 дней от заседания (24.08)."""
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 14)
        fi = {"status": "Решено", "hearing_date": "25.06.2026",
              "decision_date": "25.06.2026", "events": []}
        end = lifecycle.fi_appeal_window_end(fi)
        assert end == date(2026, 8, 24)
        assert end >= date(2026, 6, 25) + timedelta(days=config.FI_ARCHIVE_DAYS - 2)

    def test_anchor_fallbacks(self, monkeypatch):
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 0)
        # Штамп трека motivirovka_date — второй источник.
        assert lifecycle.fi_appeal_window_end(
            {"motivirovka_date": "10.08.2026"}) == date(2026, 9, 10)
        # act_date (публикация текста) — третий.
        assert lifecycle.fi_appeal_window_end(
            {"act_date": "12.08.2026"}) == date(2026, 9, 14)
        # Событие мотивировки сильнее act_date: срок течёт от изготовления.
        assert lifecycle.fi_appeal_window_end(
            {"act_date": "20.08.2026",
             "events": [_ev("10.08.2026", MOTIV)]}) == date(2026, 9, 10)
        # Без решения и заседания — event_date (возвраты на стадии принятия).
        assert lifecycle.fi_appeal_window_end({"event_date": "01.06.2026"})
        # Ни одной даты → None: пустые данные не архивируем.
        assert lifecycle.fi_appeal_window_end({}) is None
        assert lifecycle.fi_appeal_window_passed({}, datetime.now()) is False

    def test_month_end_rule(self, monkeypatch):
        """Ст. 108: 31.01 → нет 31.02 → последний день месяца (28.02)."""
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 0)
        assert lifecycle.fi_appeal_window_end(
            {"motivirovka_date": "31.01.2026"}) >= date(2026, 2, 28)


# ── Основная картотека: is_case_archived ─────────────────────────────────────

class TestMainCatalogArchive:
    def test_volodin_not_archived_while_window_open(self):
        """Реальный сценарий: заседание давно (>60 дн), мотивировка недавно."""
        case = _volodin(hearing_date=_dmy(75), decision_date=_dmy(75),
                        events=[_ev(_dmy(75), DECIDED), _ev(_dmy(20), MOTIV)])
        assert lifecycle.is_case_archived(case) is False

    def test_archived_after_window(self):
        case = _volodin(hearing_date=_dmy(120), decision_date=_dmy(120),
                        events=[_ev(_dmy(120), DECIDED), _ev(_dmy(60), MOTIV)])
        assert lifecycle.is_case_archived(case) is True

    def test_appeal_flag_beats_window(self):
        case = _volodin(hearing_date=_dmy(200), decision_date=_dmy(200),
                        events=[], appeal_filed=True)
        assert lifecycle.is_case_archived(case) is False

    def test_returned_keeps_flat_window(self):
        """«Возвращено» — прежние FI_ARCHIVE_DAYS от event_date/hearing_date:
        возврат обжалуется частной жалобой (15 дн), запас с лихвой."""
        case = _volodin(status="Возвращено", hearing_date="", decision_date="",
                        events=[], event_date=_dmy(config.FI_ARCHIVE_DAYS + 5))
        assert lifecycle.is_case_archived(case) is True
        case["first_instance"]["event_date"] = _dmy(config.FI_ARCHIVE_DAYS - 5)
        assert lifecycle.is_case_archived(case) is False

    def test_real_archive_returns_only_volodin(self):
        """На снимке данных 04.09.2026 новое правило возвращает из архива
        ровно одно решённое дело — 2-857/2026 (проверка на живом файле, если
        он есть; дата зафиксирована, чтобы страж не протух)."""
        path = os.path.join(ROOT, "data", "cases_archive.json")
        if not os.path.exists(path):
            pytest.skip("нет data/cases_archive.json")
        with open(path, encoding="utf-8") as f:
            cases = json.load(f)["cases"]
        now = datetime(2026, 9, 4)
        back = [c["id"] for c in cases
                if c.get("current_stage") == "first_instance"
                and (c.get("first_instance") or {}).get("status") == "Решено"
                and not (c["first_instance"].get("appeal_filed")
                         or c["first_instance"].get("appeal_filed_date"))
                and not lifecycle.fi_appeal_window_passed(c["first_instance"], now)]
        if any(i.startswith("2-857/2026") for i in [c["id"] for c in cases]):
            assert back == ["2-857/2026 (2-7073/2025;)"]


# ── Трек «Иски банка»: единое окно для отказа, 30 дн для завершений ──────────

def _track(**fi_over) -> dict:
    fi = {"status": "Решено", "hearing_date": _dmy(60), "decision_date": _dmy(60),
          "events": []}
    fi.update(fi_over)
    return {"id": "2-1/2026", "current_stage": "first_instance", "track": "plaintiff_light",
            "bank_role": "Истец", "first_instance": fi}


class TestBankTrackWindow:
    DENIED = "ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)"
    REFUSAL = ("ОТКАЗАНО в принятии заявленияЗАЯВЛЕНИЕ НЕ ПОДЛЕЖИТ "
               "РАССМОТРЕНИЮ и разрешению в порядке гражданского судопроизводства")

    def test_denied_uses_unified_window(self, monkeypatch):
        """Прежние 30 дн от мотивировки без запаса: на 35-й день дело уходило
        в архив, теперь живо до месяца + 14."""
        monkeypatch.setattr(config, "FI_APPEAL_GRACE_DAYS", 14)
        case = _track(result=self.DENIED, motivirovka_date=_dmy(35))
        assert lifecycle.is_case_archived(case) is False
        case = _track(result=self.DENIED, motivirovka_date=_dmy(50))
        assert lifecycle.is_case_archived(case) is True

    def test_denied_reads_motivirovka_event(self):
        case = _track(result=self.DENIED, hearing_date=_dmy(75), decision_date=_dmy(75),
                      events=[_ev(_dmy(20), MOTIV)])
        assert lifecycle.is_case_archived(case) is False

    def test_procedural_termination_keeps_30_days(self):
        """Отказ в принятии обжалуется частной жалобой (15 дн, ст. 332) —
        окно BANK_RETURNED_ARCHIVE_DAYS, а не месяц + запас."""
        case = _track(result=self.REFUSAL, hearing_date="", decision_date="",
                      event_date=_dmy(config.BANK_RETURNED_ARCHIVE_DAYS + 5))
        assert lifecycle.is_case_archived(case) is True
        case["first_instance"]["event_date"] = _dmy(config.BANK_RETURNED_ARCHIVE_DAYS - 5)
        assert lifecycle.is_case_archived(case) is False

    def test_left_unconsidered_keeps_30_days(self):
        case = _track(status="В производстве",
                      result="Иск (заявление, жалоба) ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ",
                      hearing_date=_dmy(40), decision_date=_dmy(40))
        assert lifecycle.is_case_archived(case) is True

    def test_old_constant_gone(self):
        assert not hasattr(config, "BANK_DENIED_ARCHIVE_DAYS")
        assert config.FI_APPEAL_GRACE_DAYS == 14


# ── Фронт: зеркало в app.js ──────────────────────────────────────────────────

def _app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    if m:
        return m.group(0)
    m = re.search(r"^function\s+" + re.escape(name) + r"\s*\(.*\}$", src, re.M)
    assert m, f"В app.js нет функции {name}."
    return m.group(0)


class TestFrontendMirror:
    def test_grace_constant_in_sync(self):
        m = re.search(r"const APPEAL_GRACE_DAYS=(\d+);", _app_js())
        assert m and int(m.group(1)) == config.FI_APPEAL_GRACE_DAYS, (
            "APPEAL_GRACE_DAYS в app.js разошёлся с FI_APPEAL_GRACE_DAYS в config.py")

    def test_wiring(self):
        src = _app_js()
        derived = _fn_src(src, "computeDerived")
        assert "fiAppealWindowPassed(c._fi)" in derived, (
            "computeDerived не зовёт fiAppealWindowPassed — решённое дело "
            "прячется по плоским ARCHIVE_DAYS раньше срока на жалобу")
        assert "fiAppealWindowPassed(c._fi)" in _fn_src(src, "isArchived")

    @pytest.mark.skipif(not NODE, reason="node не установлен")
    def test_volodin_window_in_node(self):
        src = _app_js()
        consts = "\n".join(
            re.search(r"^const " + n + r"=.*;$", src, re.M).group(0)
            for n in ("APPEAL_GRACE_DAYS", "MOTIVATION_TERM_CAL_DAYS", "MOTIVATED_DECISION_RE"))
        deps = "\n".join(_fn_src(src, n) for n in ("parseDate", "appealWindowEnd", "fiAppealWindowPassed"))
        fixtures = {
            "volodin": _volodin()["first_instance"],
            "no_motiv": {"status": "Решено", "hearing_date": "25.06.2026",
                         "decision_date": "25.06.2026", "events": []},
            "empty": {},
        }
        script = (consts + "\n" + deps + "\nconst F=" + json.dumps(fixtures, ensure_ascii=False)
                  + ";const fmt=d=>d?[d.getFullYear(),d.getMonth()+1,d.getDate()].join('-'):null;"
                  "process.stdout.write(JSON.stringify(Object.fromEntries("
                  "Object.entries(F).map(([k,v])=>[k,fmt(appealWindowEnd(v))]))));")
        out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
        got = json.loads(out.stdout)
        assert got["volodin"] == "2026-9-24"
        assert got["empty"] is None
        # Без мотивировки: 25.06 + 14 + месяц + 14 = 23.08 — не раньше
        # прежних 60 дней (24.08) больше чем на пару дней.
        assert got["no_motiv"] == "2026-8-23"
