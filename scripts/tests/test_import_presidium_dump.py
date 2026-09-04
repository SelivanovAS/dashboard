# -*- coding: utf-8 -*-
"""Ветка ПРЕЗИДИУМА импортёра дампов (scripts/import_search_dump.py, 04.09.2026).

Кассация по делам мировых судей — президиум облсуда; раздел `delo_id=2800001`
на домене апел-суда, поиск за проверочным кодом. Раздел выбирает САМ ДАМП
(delo_id в ссылках карточек), оператор шлёт голый домен. Карточка обязательна
(УИД, участники, статус жалобы — только в ней); дело заводит боевой
link_cassation_cases как discovery.

Фикстура выдачи — реальная страница Суда ХМАО-Югры (4 строки, обезличена).
"""

from __future__ import annotations

import json
import os
import re
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

import import_search_dump as isd  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")
DOMAIN = "oblsud--hmao.sudrf.ru"


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _card_for(case_id: str) -> str | None:
    """Карточка по case_id из фикстуры выдачи; 26578025 (4Г-16) «не открывается»."""
    if case_id == "26578025":
        return None
    card = _fixture("case_card_presidium.html")
    if case_id == "26578029":  # 4Г-17/2026 [44Г-2/2026] — другой УИД и должник
        card = (card.replace("4Г-66/2026", "4Г-17/2026")
                    .replace("86MS0072-01-2019-003202-72", "86MS0071-01-2026-000864-11")
                    .replace("2-1543-2803/2019", "2-864-2802/2026"))
    return card


@pytest.fixture
def env(tmp_path, monkeypatch):
    paths = {
        "json": tmp_path / "cases.json",
        "archive": tmp_path / "cases_archive.json",
        "csv": tmp_path / "sberbank_cases.csv",
        "csv_archive": tmp_path / "sberbank_cases_archive.csv",
        "acts": tmp_path / ".cassation_acts",
        "gh_out": tmp_path / "gh_output.txt",
        "dump": tmp_path / "dump.html",
    }
    monkeypatch.setattr(cm_config, "JSON_PATH", str(paths["json"]))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(paths["archive"]))
    monkeypatch.setattr(cm_config, "CSV_PATH", str(paths["csv"]))
    monkeypatch.setattr(cm_config, "CSV_ARCHIVE_PATH", str(paths["csv_archive"]))
    monkeypatch.setattr(cm_config, "CASSATION_ACTS_PATH", str(paths["acts"]))
    monkeypatch.setattr(cm_config, "REGION", "hmao")
    monkeypatch.setenv("GITHUB_OUTPUT", str(paths["gh_out"]))
    monkeypatch.setattr(isd, "polite_delay", lambda: None)
    calls: list[str] = []

    def fake_card(url, context=None):
        calls.append(url)
        m = re.search(r"case_id=(\d+)", url)
        html = _card_for(m.group(1)) if m else None
        if html is None:
            cm_config.FETCH_DIAG.clear()
            cm_config.FETCH_DIAG["kind"] = "http_403"
        return html

    monkeypatch.setattr(isd, "fetch_card_checked", fake_card)
    paths["dump"].write_text(_fixture("search_presidium_dump_hmao.html"), encoding="utf-8")
    paths["calls"] = calls
    diag_before = dict(cm_config.FETCH_DIAG)
    yield paths
    cm_config.FETCH_DIAG.clear()
    cm_config.FETCH_DIAG.update(diag_before)


def _seed(env, cases: list[dict]) -> None:
    env["json"].write_text(json.dumps({"version": 1, "cases": cases}, ensure_ascii=False),
                           encoding="utf-8")


def _summary(env) -> dict:
    text = env["gh_out"].read_text(encoding="utf-8")
    line = [l for l in text.splitlines() if l.startswith("summary=")][-1]
    return json.loads(line[len("summary="):])


def _cases(env) -> list[dict]:
    return json.loads(env["json"].read_text(encoding="utf-8"))["cases"]


def _run(env, *extra) -> int:
    return isd.main([str(env["dump"]), "--court-domain", DOMAIN,
                     "--operator", "Селиванов А.С.", *extra])


class TestResolveCourt:
    def test_by_delo_id_picks_presidium(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        c = isd.resolve_court(DOMAIN, delo_id="2800001")
        assert c.court_type == "cassation" and c.name == "Президиум Суда ХМАО-Югры"
        assert isd.resolve_court(DOMAIN, delo_id=5).court_type == "appeal"

    def test_without_delo_id_keeps_appeal(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        assert isd.resolve_court(DOMAIN).court_type == "appeal"
        assert isd.resolve_court("oblsud.hmao.sudrf.ru", delo_id="2800001").court_type == "cassation"


class TestPresidiumDumpImport:
    def test_end_to_end(self, env):
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        s = _summary(env)
        assert s["section"] == "cassation"
        assert s["court"] == "Президиум Суда ХМАО-Югры"
        assert s["rows"] == 4
        # 4Г-2072/2019 — до реформы: без карточки; 4Г-16 — карточка не открылась.
        assert (s["added"], s["linked"], s["already"], s["skipped_old"],
                s["fetch_fail"], s["no_link"]) == (2, 0, 0, 1, 1, 0)
        assert s["card_fail_reason"]
        marks = [l.split("]")[0] + "]" for l in s["lines"]]
        assert marks.count("[ADDED PRESIDIUM]") == 2
        assert "[SKIPPED OLD]" in marks and "[FETCH FAIL]" in marks
        assert not any("26578038" in u for u in env["calls"])  # 2019 — без HTTP
        cases = _cases(env)
        assert {c["id"] for c in cases} == {"4Г-66/2026", "4Г-17/2026"}
        c = next(c for c in cases if c["id"] == "4Г-66/2026")
        assert c["current_stage"] == "cassation"
        assert c["import"]["source"] == "dump_presidium" and "announced" not in c["import"]
        assert c["import"]["operator"] == "Селиванов А.С."
        assert "президиума" in c["notes"]
        assert c["cassation"]["court_domain"] == DOMAIN
        assert c["cassation"]["link"] == "26942242|ae405a64-4d0b-4d1c-a637-ac45ee64df8c"
        assert c["first_instance"]["magistrate"] is True
        # Номер президиума из скобок выдачи.
        c17 = next(c for c in cases if c["id"] == "4Г-17/2026")
        assert c17["cassation"]["cassation_number"] == "44Г-2/2026"
        assert c17["cassation"]["outcome"] == "cassation_reversed"
        # CSV не пишется — кассаций в CSV нет.
        assert not env["csv"].exists()

    def test_already_tracked_and_reimport(self, env):
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        n_calls = len(env["calls"])
        assert _run(env) == isd.EXIT_OK
        s = _summary(env)
        assert s["already"] == 2 and s["added"] == 0
        # Повтор дампа читает только карточку не открывшегося ранее дела.
        assert len(env["calls"]) == n_calls + 1
        assert len(_cases(env)) == 2

    def test_known_by_uid_is_linked_not_added(self, env):
        """Дело с тем же УИД уже в базе (например, заведено раньше по другому
        каналу) → кассация вливается, счётчик linked."""
        _seed(env, [{
            "id": "4Г-66/2026", "current_stage": "cassation",
            "first_instance": {"case_number": "2-1543-2803/2019", "magistrate": True,
                               "court": "Мировой судья (…)", "court_domain": "", "events": []},
            "appeal": None,
            "cassation": {"case_number": "4Г-66/2026", "court_domain": DOMAIN,
                          "judicial_uid": "86MS0072-01-2019-003202-72", "events": []},
        }])
        assert _run(env) == isd.EXIT_OK
        s = _summary(env)
        assert s["already"] == 1  # (домен, номер) уже известны
        assert s["added"] == 1     # 4Г-17
        assert len(_cases(env)) == 2

    def test_dry_run_touches_nothing(self, env):
        _seed(env, [])
        assert _run(env, "--dry-run") == isd.EXIT_OK
        s = _summary(env)
        assert s["skipped_old"] == 1 and s["added"] == 0
        assert sum(1 for l in s["lines"] if l.startswith("[DRY RUN]")) == 3
        assert env["calls"] == []
        assert _cases(env) == []

    def test_no_sber_in_card_is_subsidiary(self, env, monkeypatch):
        card = _fixture("case_card_presidium.html").replace("ПАО Сбербанк", "ООО Сбербанк страхование жизни")
        monkeypatch.setattr(isd, "fetch_card_checked", lambda url, context=None: card)
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        s = _summary(env)
        assert s["subsidiary"] == 3 and s["added"] == 0
        assert any(l.startswith("[NO SBER]") for l in s["lines"])

    def test_appeal_dump_on_same_domain_goes_to_appeal_branch(self, env):
        """Тот же домен, delo_id=5 в ссылках → ветка апелляции, section=appeal."""
        env["dump"].write_text(
            _fixture("search_appeal_dump_svd.html").replace("oblsud--svd.sudrf.ru", DOMAIN),
            encoding="utf-8",
        )
        _seed(env, [])
        rc = _run(env, "--dry-run")
        s = _summary(env)
        assert s["section"] == "appeal" and s["court"] == "Суд ХМАО-Югры"
        assert rc == isd.EXIT_OK

    def test_wrong_section_names_presidium(self, env):
        """Ссылки раздела 2800001, но у домена НЕТ президиума → гейт раздела
        называет кассационный раздел."""
        env["dump"].write_text(
            _fixture("search_presidium_dump_hmao.html").replace("delo_id=2800001", "delo_id=777"),
            encoding="utf-8",
        )
        _seed(env, [])
        assert _run(env) == isd.EXIT_WRONG_COURT
        assert "раздела" in _summary(env)["error"] and "delo_id=777" in _summary(env)["error"]

    def test_text_paste_of_presidium_is_explained(self, env):
        """Вставка «как текст»: ссылок нет, раздел не распознан → ветка апелляции
        объясняет, что это выдача президиума без ссылок."""
        html = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", _fixture("search_presidium_dump_hmao.html"))
        env["dump"].write_text(html, encoding="utf-8")
        _seed(env, [])
        assert _run(env) == isd.EXIT_WRONG_COURT
        assert "президиум" in _summary(env)["error"]


class TestAnnounce:
    def test_presidium_import_announced_once(self, env):
        from court_monitor import runs as cm_runs
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        cases = _cases(env)
        first = cm_runs.announce_imported_presidium_cases(cases)
        assert {c["id"] for c in first} == {"4Г-66/2026", "4Г-17/2026"}
        assert all(c["import"]["announced"] for c in first)
        assert cm_runs.announce_imported_presidium_cases(cases) == []
        # Канал «Новые иски» такие дела не берёт.
        for c in cases:
            c["import"]["announced"] = False
        assert cm_runs.announce_imported_cases(cases) == []
