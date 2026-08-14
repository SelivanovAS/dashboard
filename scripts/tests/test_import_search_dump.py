# -*- coding: utf-8 -*-
"""Импортёр дампов поисковой выдачи капчёвых судов (scripts/import_search_dump.py)
+ режим keep_all_roles в parse_first_instance_search.

Скрипт полностью офлайн (сайты судов не трогает), поэтому тесты гоняют его
целиком через main() на tmp-файлах: monkeypatch config.JSON_PATH /
JSON_ARCHIVE_PATH / REGION — config.X-инвариант, код читает значения на вызов.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, TESTS_DIR)

import import_search_dump as isd  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.parsing.search import parse_first_instance_search  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from fixture_dates import recent_fi_card_html  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _svd_court():
    return get_region("sverdlovsk_yanao").first_instance_courts[0]  # Академический


# ── parse_first_instance_search: keep_all_roles ──────────────────────────────

class TestKeepAllRoles:
    def test_default_only_defendant(self):
        """Боевой режим не изменился: только «банк-ответчик»."""
        rows = parse_first_instance_search(_fixture("search_fi_all_roles.html"), _svd_court())
        assert [r["case_number"] for r in rows] == [
            "2-1001/2026", "2-1005/2026", "2-1006/2026",
        ]

    def test_keep_all_roles_returns_every_sber_role(self):
        rows = parse_first_instance_search(
            _fixture("search_fi_all_roles.html"), _svd_court(), keep_all_roles=True,
        )
        assert {(r["case_number"], r["bank_role"]) for r in rows} == {
            ("2-1001/2026", "Ответчик"),
            ("2-1002/2026", "Истец"),
            ("2-1004/2026", "Третье лицо"),
            ("2-1005/2026", "Ответчик"),
            ("2-1006/2026", "Ответчик"),
        }

    def test_subsidiary_dropped_in_both_modes_with_stats(self):
        """Дочка (Сбербанк страхование) отсеивается и считается в stats."""
        stats: dict = {}
        rows = parse_first_instance_search(
            _fixture("search_fi_all_roles.html"), _svd_court(),
            stats=stats, keep_all_roles=True,
        )
        assert "2-1003/2026" not in [r["case_number"] for r in rows]
        assert stats["subsidiary_rows"] == 1
        assert stats["subsidiary_cases"] == ["2-1003/2026"]

    def test_href_srv_num_extracted(self):
        """srv_num из href суда — отдельным ключом (боевой court_srv_num прежний)."""
        rows = parse_first_instance_search(
            _fixture("search_fi_all_roles.html"), _svd_court(), keep_all_roles=True,
        )
        by_num = {r["case_number"]: r for r in rows}
        assert by_num["2-1001/2026"]["href_srv_num"] == 1
        assert by_num["2-1002/2026"]["href_srv_num"] == 2
        assert by_num["2-1005/2026"]["href_srv_num"] is None  # строка без ссылки
        assert all(r["court_srv_num"] == _svd_court().srv_num for r in rows)


# ── normalize_dump: pretty-print дампы ───────────────────────────────────────

class TestNormalizeDump:
    def test_pretty_print_breaks_raw_parse(self):
        """Документируем саму проблему: без нормализации стороны теряются
        (регексы _parse_combined_cell без DOTALL)."""
        rows = parse_first_instance_search(
            _fixture("search_fi_all_roles_pretty.html"), _svd_court(),
            keep_all_roles=True,
        )
        by_num = {r["case_number"]: r for r in rows}
        assert by_num["2-1002/2026"]["bank_role"] != "Истец"

    def test_normalize_restores_parties(self):
        html = isd.normalize_dump(_fixture("search_fi_all_roles_pretty.html"))
        rows = parse_first_instance_search(html, _svd_court(), keep_all_roles=True)
        by_num = {r["case_number"]: r for r in rows}
        assert by_num["2-1001/2026"]["plaintiff"] == "Петров Пётр Петрович"
        assert by_num["2-1001/2026"]["bank_role"] == "Ответчик"
        assert by_num["2-1002/2026"]["bank_role"] == "Истец"


# ── Импортёр e2e ─────────────────────────────────────────────────────────────

@pytest.fixture
def import_env(tmp_path, monkeypatch):
    """tmp-хранилище + активный регион Свердловск/ЯНАО + GITHUB_OUTPUT.

    С веткой истцовых строк импортёр перестал быть офлайновым: по искам банка
    качается карточка — сеть мокается (свежая карточка, счётчик вызовов в
    card_calls), как в test_collect_bank_claims.
    """
    json_path = tmp_path / "cases.json"
    archive_path = tmp_path / "cases_archive.json"
    bank_path = tmp_path / "cases_bank.json"
    bank_events_path = tmp_path / "cases_bank_events.json"
    seen_path = tmp_path / ".bank_intake_seen.json"
    gh_out = tmp_path / "gh_output.txt"
    monkeypatch.setattr(cm_config, "JSON_PATH", str(json_path))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(archive_path))
    monkeypatch.setattr(cm_config, "JSON_BANK_PATH", str(bank_path))
    monkeypatch.setattr(cm_config, "JSON_BANK_EVENTS_PATH", str(bank_events_path))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_PATH",
                        str(tmp_path / "cases_bank_archive.json"))
    monkeypatch.setattr(cm_config, "JSON_BANK_ARCHIVE_EVENTS_PATH",
                        str(tmp_path / "cases_bank_archive_events.json"))
    monkeypatch.setattr(cm_config, "BANK_INTAKE_SEEN_PATH", str(seen_path))
    monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
    monkeypatch.setenv("GITHUB_OUTPUT", str(gh_out))
    monkeypatch.setattr(isd, "polite_delay", lambda: None)
    card_calls = {"n": 0}

    def fake_card(url, context=None):
        card_calls["n"] += 1
        return recent_fi_card_html()

    monkeypatch.setattr(isd, "fetch_card_checked", fake_card)
    dump = tmp_path / "dump.html"
    dump.write_text(_fixture("search_fi_all_roles.html"), encoding="utf-8")
    return {
        "tmp": tmp_path, "json": json_path, "archive": archive_path,
        "bank": bank_path, "bank_events": bank_events_path, "seen": seen_path,
        "gh_out": gh_out, "dump": dump, "card_calls": card_calls,
    }


def _read_summary(gh_out) -> dict:
    text = gh_out.read_text(encoding="utf-8")
    line = [l for l in text.splitlines() if l.startswith("summary=")][-1]
    return json.loads(line[len("summary="):])


def _run(env, *extra) -> int:
    return isd.main([
        str(env["dump"]),
        "--court-domain", "akademicheskiy--svd.sudrf.ru",
        "--operator", "Творонович Ю.А.",
        *extra,
    ])


class TestImporterE2E:
    def test_roles_split_between_tracks(self, import_env):
        """Ответчики → cases.json, истец → трек «Иски банка» (с 13.08.2026),
        третье лицо — [SKIPPED ROLE] (решение юриста 16.07.2026)."""
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        ids = [c["id"] for c in data["cases"]]
        # 2 добавлено в основную: ответчики со ссылкой. 2-1002 (истец) — в
        # трек исков банка; 2-1004 (третье лицо) — [SKIPPED ROLE];
        # 2-1005 — ответчик без ссылки ([NO LINK]).
        assert sorted(ids) == ["2-1001/2026", "2-1006/2026"]
        by_id = {c["id"]: c for c in data["cases"]}
        c1 = by_id["2-1001/2026"]
        assert c1["current_stage"] == "first_instance"
        assert c1["bank_role"] == "Ответчик"
        assert c1["initial_bank_role"] == "Ответчик"
        assert c1["first_instance"]["court"] == "Академический районный суд г. Екатеринбурга"
        assert c1["first_instance"]["link"] == "111|aaaa-1111"
        assert c1["import"]["operator"] == "Творонович Ю.А."
        assert c1["import"]["source"] == "dump"
        assert c1["import"]["at"]

    def test_srv_num_from_href_overrides_config(self, import_env):
        """Дело со srv_num=2 в href получает сервер 2, хоть конфиг суда — 1."""
        _run(import_env)
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        by_id = {c["id"]: c for c in data["cases"]}
        assert by_id["2-1006/2026"]["first_instance"]["srv_num"] == 2
        assert by_id["2-1001/2026"]["first_instance"]["srv_num"] == 1

    def test_summary_in_github_output(self, import_env):
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2
        assert s["added_bank"] == 1
        assert s["already"] == 0
        assert s["skipped_role"] == 1
        assert s["no_link"] == 1
        assert s["subsidiary"] == 1
        assert s["court"] == "Академический районный суд г. Екатеринбурга"
        assert s["operator"] == "Творонович Ю.А."
        assert any("[ADDED BANK] 2-1002/2026" in l for l in s["lines"])
        assert any("[SKIPPED ROLE] 2-1004/2026" in l for l in s["lines"])
        assert any("[NO LINK] 2-1005/2026" in l for l in s["lines"])
        assert any("[SUBSIDIARY] 2-1003/2026" in l for l in s["lines"])

    def test_rerun_dedupes_everything(self, import_env):
        _run(import_env)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 0
        assert s["added_bank"] == 0
        assert s["already"] == 3  # 2 основной картотеки + 1 из трека банка
        assert s["skipped_role"] == 1  # пропуски ролей стабильны на повторе
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        assert len(data["cases"]) == 2  # дублей нет
        bank = json.loads(import_env["bank"].read_text(encoding="utf-8"))
        assert len(bank["cases"]) == 1

    def test_dedup_against_archive(self, import_env):
        """Дело из горячего архива не всплывает как новое."""
        import_env["archive"].write_text(json.dumps({
            "version": 1, "cases": [{"id": "2-1001/2026"}],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 1
        assert s["already"] == 1

    def test_dry_run_leaves_json_untouched(self, import_env):
        rc = _run(import_env, "--dry-run")
        assert rc == isd.EXIT_OK
        assert not import_env["json"].exists()
        assert not import_env["bank"].exists()
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2 and s["dry_run"] is True
        assert s["bank_dry_run"] == 1 and s["added_bank"] == 0
        # dry-run не ходит в сеть даже за карточками исков банка
        assert import_env["card_calls"]["n"] == 0

    def test_captcha_dump_rejected(self, import_env):
        import_env["dump"].write_text(
            _fixture("search_captcha_challenge.html"), encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_CAPTCHA
        s = _read_summary(import_env["gh_out"])
        assert "проверочного кода" in s["error"]
        assert not import_env["json"].exists()

    def test_unknown_court_rejected(self, import_env):
        rc = isd.main([
            str(import_env["dump"]),
            "--court-domain", "nosuchcourt--svd.sudrf.ru",
        ])
        assert rc == isd.EXIT_UNKNOWN_COURT

    def test_broken_dump_no_table(self, import_env):
        import_env["dump"].write_text(
            "<html><body><p>что-то не то</p></body></html>", encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_NO_TABLE
        s = _read_summary(import_env["gh_out"])
        assert "Таблица результатов не найдена" in s["error"]

    def test_same_number_in_other_court_is_not_duplicate(self, import_env):
        """Номера дел не уникальны между судами: «2-1001/2026» другого суда
        не должен блокировать добавление дела Академического (вопрос юриста
        16.07.2026 — дедуп с учётом суда)."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "2-1001/2026",
                "current_stage": "first_instance",
                "bank_role": "Ответчик",
                "first_instance": {"case_number": "2-1001/2026",
                                   "court": "Алапаевский городской суд",
                                   "court_domain": "alapaevsky--svd.sudrf.ru"},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2      # 2-1001 Академического добавлен, не ALREADY
        assert s["already"] == 0
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        same_num = [c for c in data["cases"] if c["id"] == "2-1001/2026"]
        assert len(same_num) == 2   # по одному на каждый суд
        assert {(c["first_instance"]["court_domain"]) for c in same_num} == {
            "alapaevsky--svd.sudrf.ru", "akademicheskiy--svd.sudrf.ru",
        }

    def test_same_number_same_court_is_duplicate(self, import_env):
        """А в ТОМ ЖЕ суде совпадение номера — честный дубль → ALREADY."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "2-1001/2026",
                "current_stage": "first_instance",
                "bank_role": "Ответчик",
                "first_instance": {"case_number": "2-1001/2026",
                                   "court_domain": "akademicheskiy--svd.sudrf.ru"},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 1 and s["already"] == 1

    def test_record_without_court_blocks_everywhere(self, import_env):
        """Запись без домена И без имени суда — wildcard: консервативно
        блокирует номер во всех судах (лучше пропуск, чем дубль).
        Так же работает архивный дедуп (архив в test_dedup_against_archive)."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{"id": "2-1001/2026", "current_stage": "appeal"}],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 1 and s["already"] == 1

    def test_appeal_record_other_court_by_name_not_blocking(self, import_env):
        """Сценарий Ивделя (16.07.2026): дело «с апелляции» без court_domain,
        но с именем суда — домен резолвится по имени, и номер НЕ блокирует
        одноимённое дело другого суда (11 ложных [ALREADY] на первых живых
        импортах: 2-114/2026 Ивдельского отброшено из-за Пуровского и т.п.)."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "33-999/2026",
                "current_stage": "appeal",
                "first_instance": {"case_number": "2-1001/2026",
                                   "court": "Алапаевский городской суд"},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2 and s["already"] == 0
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        same_num = [c for c in data["cases"]
                    if (c.get("first_instance") or {}).get("case_number") == "2-1001/2026"]
        assert len(same_num) == 2   # алапаевское «с апелляции» + академическое из дампа

    def test_appeal_record_same_court_by_name_is_duplicate(self, import_env):
        """А если имя суда записи «с апелляции» — суд самого дампа,
        совпадение номера остаётся честным дублем → ALREADY."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "33-999/2026",
                "current_stage": "appeal",
                "first_instance": {
                    "case_number": "2-1001/2026",
                    "court": "Академический районный суд г. Екатеринбурга",
                },
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 1 and s["already"] == 1

    def test_material_in_other_court_not_promoted(self, import_env):
        """М-500/2026 ДРУГОГО суда не переименовывается комбо-строкой
        Академического — дело добавляется как новое."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "М-500/2026",
                "current_stage": "first_instance",
                "bank_role": "Ответчик",
                "first_instance": {"case_number": "М-500/2026",
                                   "court_domain": "alapaevsky--svd.sudrf.ru"},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["promoted"] == 0
        assert s["added"] == 2  # 2-1006 добавлен новым делом
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        ids = [c["id"] for c in data["cases"]]
        assert "М-500/2026" in ids  # чужая запись не тронута

    def test_material_promoted_to_case(self, import_env):
        """Комбо-номер «2-1006 ~ М-500» при уже отслеживаемом материале
        М-500/2026: запись переименовывается (зеркало промоушена main_json),
        дубль не создаётся, ссылка/сервер обновляются из дампа."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{
                "id": "М-500/2026",
                "current_stage": "first_instance",
                "bank_role": "Ответчик",
                "plaintiff": "New Wave Group AB",
                "defendant": "ПАО Сбербанк",
                "import": {"operator": "Прошлый", "at": "2026-07-10T10:00:00",
                           "source": "dump", "announced": True},
                "first_instance": {"case_number": "М-500/2026",
                                   "court_domain": "akademicheskiy--svd.sudrf.ru",
                                   "link": "1|aaaa-0000", "srv_num": 1},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        ids = [c["id"] for c in data["cases"]]
        assert "М-500/2026" not in ids
        assert ids.count("2-1006/2026") == 1  # переименована, не задвоена
        by_id = {c["id"]: c for c in data["cases"]}
        fi = by_id["2-1006/2026"]["first_instance"]
        assert fi["case_number"] == "2-1006/2026"
        assert fi["material_number"] == "М-500/2026"  # ★ на материале живёт
        assert fi["link"] == "666|ffff-6666"
        assert fi["srv_num"] == 2
        assert fi["accepted_pending_emit"] is True  # событие эмитит прогон
        s = _read_summary(import_env["gh_out"])
        assert s["promoted"] == 1
        assert s["added"] == 1  # только 2-1001 (2-1006 не добавлялось заново)
        assert any("[PROMOTED] М-500/2026 → 2-1006/2026" in l for l in s["lines"])

    def test_imported_case_announced_once(self, import_env):
        """Импортированное дело объявляется новым в ближайшем дайджесте РОВНО
        один раз (runs.announce_imported_cases + import.announced)."""
        import update_cases as uc
        _run(import_env)
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        cases = data["cases"]
        first = uc.announce_imported_cases(cases)
        assert sorted(c["id"] for c in first) == ["2-1001/2026", "2-1006/2026"]
        assert all(c["import"]["announced"] is True for c in first)
        # Повторный прогон (флаг уже в данных) — анонса нет.
        assert uc.announce_imported_cases(cases) == []
        # Дело без блока import (автопоиск) анонс не трогает.
        cases.append({"id": "2-9999/2026"})
        assert uc.announce_imported_cases(cases) == []

    def test_legit_empty_result_is_ok(self, import_env):
        import_env["dump"].write_text(
            "<html><body>Данных по запросу не обнаружено</body></html>",
            encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 0 and "error" not in s

    def test_win1251_dump_decoded(self, import_env):
        """Файл «только HTML» с sudrf приходит в win-1251."""
        raw = _fixture("search_fi_all_roles.html").encode("windows-1251")
        import_env["dump"].write_bytes(raw)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        assert _read_summary(import_env["gh_out"])["added"] == 2

    def test_pretty_dump_end_to_end(self, import_env):
        """Pretty-print дамп: нормализация внутри импортёра, стороны на месте
        (без неё 2-1001 потерял бы истца, а роль 2-1002 распозналась бы
        неверно и иск банка НЕ ушёл бы в свой трек)."""
        import_env["dump"].write_text(
            _fixture("search_fi_all_roles_pretty.html"), encoding="utf-8")
        _run(import_env)
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        by_id = {c["id"]: c for c in data["cases"]}
        assert list(by_id) == ["2-1001/2026"]  # истец 2-1002 не в основной
        assert by_id["2-1001/2026"]["plaintiff"] == "Петров Пётр Петрович"
        s = _read_summary(import_env["gh_out"])
        assert s["added_bank"] == 1  # 2-1002 распознан истцом → в трек


# ── Истцовые строки → трек «Иски банка» (с 13.08.2026, разгон Урала) ─────────

def _bank_cases(env) -> list[dict]:
    if not env["bank"].exists():
        return []
    return json.loads(env["bank"].read_text(encoding="utf-8")).get("cases", [])


def _seen_map(env) -> dict:
    if not env["seen"].exists():
        return {}
    return json.loads(env["seen"].read_text(encoding="utf-8")).get("seen", {})


class TestBankDumpImport:
    def test_plaintiff_added_to_bank_track(self, import_env):
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        cases = _bank_cases(import_env)
        assert [c["id"] for c in cases] == ["2-1002/2026"]
        c = cases[0]
        assert c["track"] == "plaintiff_light"
        assert c["bank_role"] == "Истец"
        assert c["current_stage"] == "first_instance"
        assert c["import"]["source"] == "dump"
        assert c["import"]["announced"] is True   # не анонсируется «новым иском»
        assert c["import"]["operator"] == "Творонович Ю.А."
        # srv_num из href (2) авторитетнее конфига суда (1)
        assert c["first_instance"]["srv_num"] == 2
        # split-хранение: events уехали в отдельный файл под композитным ключом
        assert import_env["bank_events"].exists()
        assert "2-1002/2026" in import_env["bank_events"].read_text(encoding="utf-8")
        # 3 карточки: истец (2-1002) + два ответчика со ссылкой (2-1001,
        # 2-1006). Ответчиков читаем с 14.08.2026 — до этого дело основной
        # картотеки заводилось пустышкой до ближайшего прогона.
        assert import_env["card_calls"]["n"] == 3

    def test_track_off_keeps_old_behaviour(self, import_env, monkeypatch):
        """BANK_TRACK=0: истцовые строки идут прежним [SKIPPED ROLE], сеть
        не трогается — территория без трека ведёт себя байт-в-байт как
        раньше."""
        monkeypatch.setattr(cm_config, "BANK_TRACK", False)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["skipped_role"] == 2 and s["added_bank"] == 0
        assert any("[SKIPPED ROLE] 2-1002/2026" in l for l in s["lines"])
        assert not import_env["bank"].exists()
        # Истцовые строки сети не касаются; два ответчика со ссылкой — да
        # (их карточки от выключателя трека не зависят).
        assert import_env["card_calls"]["n"] == 2

    def test_dedup_against_bank_file(self, import_env):
        """Иск банка, уже живущий в треке, — [ALREADY] без единого HTTP."""
        import_env["bank"].write_text(json.dumps({
            "version": 1, "track": "plaintiff_light",
            "cases": [{
                "id": "2-1002/2026",
                "current_stage": "first_instance",
                "track": "plaintiff_light",
                "first_instance": {"case_number": "2-1002/2026",
                                   "court_domain": "akademicheskiy--svd.sudrf.ru"},
            }],
        }, ensure_ascii=False), encoding="utf-8")
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["added_bank"] == 0 and s["already"] == 1
        # Карточку иска банка не качаем (дедуп раньше), но два ответчика — да.
        assert import_env["card_calls"]["n"] == 2
        assert len(_bank_cases(import_env)) == 1  # дублей нет

    def test_spent_case_rejected_and_cached(self, import_env, monkeypatch):
        """Дело с давним решением (сразу ушло бы в архив трека) — [SPENT],
        отказ вечный → негативный кэш, повтор без второго HTTP."""
        monkeypatch.setattr(isd, "fetch_card_checked", lambda url, context=None: (
            import_env["card_calls"].__setitem__("n", import_env["card_calls"]["n"] + 1)
            or _fixture("case_card_first_instance.html")))
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["already_spent"] == 1 and s["added_bank"] == 0
        assert not import_env["bank"].exists()
        key = "akademicheskiy--svd.sudrf.ru|2-1002/2026"
        assert _seen_map(import_env)[key]["reason"] == "already_spent"
        assert import_env["card_calls"]["n"] == 3   # истец + два ответчика
        _run(import_env)
        s2 = _read_summary(import_env["gh_out"])
        assert s2["seen_cached"] == 1
        # Второго HTTP по истцу не было (негативный кэш), ответчики на
        # повторе отсекаются дедупом ещё раньше — счётчик не вырос.
        assert import_env["card_calls"]["n"] == 3

    def test_fetch_fail_not_cached(self, import_env, monkeypatch):
        """Сетевой сбой карточки — отказ НЕ вечный: кэш пуст, повтор импорта
        с ожившей карточкой добавляет дело."""
        monkeypatch.setattr(isd, "fetch_card_checked",
                            lambda url, context=None: None)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK  # сбой строки не валит импорт
        s = _read_summary(import_env["gh_out"])
        assert s["fetch_fail"] == 1 and s["added_bank"] == 0
        assert not import_env["seen"].exists()
        monkeypatch.setattr(isd, "fetch_card_checked",
                            lambda url, context=None: recent_fi_card_html())
        _run(import_env)
        assert _read_summary(import_env["gh_out"])["added_bank"] == 1

    def test_appeal_case_taken_with_flags(self, import_env, monkeypatch):
        """Дело с поданной жалобой берётся (skip_appeal=False, как в
        авто-подхвате) — флаги переносятся, ближайший прогон уведёт его в
        основной cases.json."""
        monkeypatch.setattr(isd, "fetch_card_checked", lambda url, context=None:
                            _fixture("case_card_fi_with_appeal.html"))
        _run(import_env)
        cases = _bank_cases(import_env)
        assert len(cases) == 1
        assert cases[0]["first_instance"].get("appeal_filed")

    def test_row_excluded_result_cached(self, import_env):
        """Терминальный итог уже в строке выдачи — отказ до HTTP, в кэш."""
        state = {"seen": None, "seen_dirty": False, "cards": 0}
        outcome, line, entry = isd._import_bank_row(
            {"case_number": "2-500/2026",
             "court_domain": "akademicheskiy--svd.sudrf.ru",
             "bank_role": "Истец", "link": "1|a",
             "result": "Производство по делу ПРЕКРАЩЕНО",
             "plaintiff": "ПАО Сбербанк", "defendant": "Иванов И.И."},
            "Тест", "2026-08-13T12:00:00", False, state)
        assert outcome == "excluded_result" and entry is None
        assert "[EXCLUDED RESULT]" in line
        assert state["seen_dirty"] is True
        assert state["cards"] == 0

    def test_no_link_not_cached(self, import_env):
        """no_link в кэш НЕ пишется: ссылку обычно теряет вставка «как
        текст», а не выдача суда — иначе правильный повторный дамп молча
        скипал бы дело как [SEEN]."""
        state = {"seen": None, "seen_dirty": False, "cards": 0}
        outcome, _, _ = isd._import_bank_row(
            {"case_number": "2-501/2026",
             "court_domain": "akademicheskiy--svd.sudrf.ru",
             "bank_role": "Истец", "link": "", "result": ""},
            "Тест", "2026-08-13T12:00:00", False, state)
        assert outcome == "no_link"
        assert state["seen_dirty"] is False

    def test_cap_limits_cards(self, import_env, monkeypatch):
        """Кэп-страховка таймаута: при исчерпании — [BANK CAPPED], без HTTP."""
        monkeypatch.setattr(isd, "MAX_BANK_CARDS_PER_IMPORT", 0)
        _run(import_env)
        s = _read_summary(import_env["gh_out"])
        assert s["bank_capped"] == 1 and s["added_bank"] == 0
        assert import_env["card_calls"]["n"] == 0
        assert any("[BANK CAPPED]" in l for l in s["lines"])


# ── Защита «выбран суд А, вставлен дамп суда Б» ──────────────────────────────

def _absolutize(html: str, host: str) -> str:
    """Абсолютизировать href карточек, как это делает rich-paste браузера."""
    return html.replace('href="modules.php', 'href="https://' + host + '/modules.php')


class TestWrongCourtGuard:
    def test_foreign_host_rejected(self, import_env):
        """Абсолютные ссылки чужого суда → EXIT_WRONG_COURT, база не тронута."""
        import_env["dump"].write_text(
            _absolutize(_fixture("search_fi_all_roles.html"), "alapaevsky--svd.sudrf.ru"),
            encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_WRONG_COURT
        s = _read_summary(import_env["gh_out"])
        assert "alapaevsky--svd.sudrf.ru" in s["error"]
        assert "akademicheskiy--svd.sudrf.ru" in s["error"]
        assert s["dump_hosts"] == ["alapaevsky--svd.sudrf.ru"]
        assert not import_env["json"].exists()

    def test_matching_host_accepted(self, import_env):
        """Абсолютные ссылки ВЫБРАННОГО суда — штатный импорт."""
        import_env["dump"].write_text(
            _absolutize(_fixture("search_fi_all_roles.html"), "akademicheskiy--svd.sudrf.ru"),
            encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        assert _read_summary(import_env["gh_out"])["added"] == 2

    def test_relative_hrefs_pass(self, import_env):
        """Относительные href (файл «только HTML» без хостов) — проверка
        молчит, обратная совместимость со всеми прежними дампами."""
        rc = _run(import_env)  # фикстура как есть
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2 and "error" not in s

    def test_saved_from_marker_rejected(self, import_env):
        """Файл Chrome «только HTML»: href относительные, но маркер
        «saved from url=…» выдаёт настоящий суд."""
        html = ("<!-- saved from url=(0074)https://alapaevsky--svd.sudrf.ru"
                "/modules.php?name=sud_delo&name_op=r -->\n"
                + _fixture("search_fi_all_roles.html"))
        import_env["dump"].write_text(html, encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_WRONG_COURT
        assert "alapaevsky--svd.sudrf.ru" in _read_summary(import_env["gh_out"])["error"]

    def test_saved_from_marker_matching_passes(self, import_env):
        """Маркер выбранного суда — не мешает импорту."""
        html = ("<!-- saved from url=(0078)https://akademicheskiy--svd.sudrf.ru"
                "/modules.php?name=sud_delo&name_op=r -->\n"
                + _fixture("search_fi_all_roles.html"))
        import_env["dump"].write_text(html, encoding="utf-8")
        assert _run(import_env) == isd.EXIT_OK

    def test_mixed_hosts_rejected(self, import_env):
        """Ссылки двух разных судов в одном дампе — блок, даже если выбранный
        среди них (склейка двух выдач — не то, что ждёт импортёр)."""
        html = (_absolutize(_fixture("search_fi_all_roles.html"),
                            "akademicheskiy--svd.sudrf.ru")
                + '<a href="https://alapaevsky--svd.sudrf.ru/modules.php?'
                  'name=sud_delo&name_op=case&case_id=9">2-9/2026</a>')
        import_env["dump"].write_text(html, encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_WRONG_COURT
        s = _read_summary(import_env["gh_out"])
        assert sorted(s["dump_hosts"]) == [
            "akademicheskiy--svd.sudrf.ru", "alapaevsky--svd.sudrf.ru",
        ]

    def test_wrong_section_delo_id_rejected(self, import_env):
        """Выдача другого раздела (delo_id≠1540005 в href карточек) ловится
        и при относительных href — хостов в них нет."""
        html = _fixture("search_fi_all_roles.html").replace(
            "delo_id=1540005", "delo_id=5")
        import_env["dump"].write_text(html, encoding="utf-8")
        rc = _run(import_env)
        assert rc == isd.EXIT_WRONG_COURT
        s = _read_summary(import_env["gh_out"])
        assert "раздела" in s["error"] and "delo_id=5" in s["error"]
        assert not import_env["json"].exists()

    def test_detect_dump_hosts_amp_entities(self):
        """innerHTML вставки сериализует &amp; в href — хост и delo_id
        карточки всё равно извлекаются."""
        html = ('<a href="https://revdinsky--svd.sudrf.ru/modules.php?'
                'name=sud_delo&amp;srv_num=1&amp;name_op=case&amp;case_id=7'
                '&amp;case_uid=aaaa-7777&amp;delo_id=1540005">2-7/2026</a>')
        assert isd.detect_dump_hosts(html) == {"revdinsky--svd.sudrf.ru"}
        assert isd.detect_card_delo_ids(html) == {"1540005"}


# ── Проводка workflow (по образцу TestWiring из test_add_cases_targeted) ─────

ROOT_DIR = os.path.dirname(SCRIPTS_DIR)


def _read_repo(rel: str) -> str:
    with open(os.path.join(ROOT_DIR, rel), encoding="utf-8") as f:
        return f.read()


class TestWorkflowWiring:
    def test_import_result_checks_commit_outcome(self):
        """status:"done" в журнал импортов — только при успешном push: упавший
        шаг «Commit cases.json» (конфликт rebase после трёх ретраев) без сверки
        COMMIT_OUTCOME слал бы оператору «+N добавлено» при потерянных данных."""
        yml = _read_repo(".github/workflows/import_cases.yml")
        assert "id: commit" in yml
        assert "IMPORT_OUTCOME: ${{ steps.import.outcome }}" in yml
        assert "COMMIT_OUTCOME: ${{ steps.commit.outcome }}" in yml
        # оба outcome входят в расчёт STATUS
        assert ('[ "$IMPORT_OUTCOME" = "success" ] && '
                '[ "$COMMIT_OUTCOME" = "success" ]') in yml
        # при упавшем коммите summary получает подсказку повторить импорт
        assert "коммит не запушился" in yml

    def test_bank_track_wiring(self):
        """Проводка ветки истцовых строк: BANK_TRACK из Variables, таймаут
        с запасом на карточки, bank-файлы в коммите, added_bank в журнале.
        Любой из четырёх пропусков ломает канал молча: флаг не доезжает /
        джоб отстреливается на 100 карточках / данные трека не коммитятся /
        оператор не видит «+N в трек» в админке."""
        yml = _read_repo(".github/workflows/import_cases.yml")
        assert "BANK_TRACK: ${{ vars.BANK_TRACK || '1' }}" in yml
        assert "timeout-minutes: 45" in yml
        for f in ("data/cases_bank.json", "data/cases_bank_events.json",
                  "data/.bank_intake_seen.json"):
            assert f in yml, f"{f} не попадает в commit-шаг import_cases.yml"
        assert "added_bank:(.added_bank // 0)" in yml

    def test_bank_counters_reach_operator(self):
        """Сквозная проводка счётчиков трека: Python считает все 14, а до глаз
        оператора доходили 6 — сводка писала «+1 добавлено» там, где в трек
        ушло ещё 4 дела и 5 отсеялось (разбор 14.08.2026). Рвётся в любом из
        трёх звеньев независимо, и каждое молчит."""
        yml = _read_repo(".github/workflows/import_cases.yml")
        worker = _read_repo("cloudflare-worker/worker.js")
        admin = _read_repo("cloudflare-worker/admin_page.js")
        for key in ("excluded_result", "excluded_writ", "already_spent",
                    "seen_cached", "bank_capped", "fetch_fail"):
            assert f"{key}:(.{key} // 0)" in yml, f"{key} не уезжает из workflow"
            assert f'"{key}"' in worker, f"{key} режет whitelist Worker'а"
        # Сводка админки считает ОБА трека (образец — acResultText рядом).
        assert 'parts = ["+" + (item.added || 0) + " в картотеку"]' in admin
        assert 'item.added_bank' in admin and '" в иски банка"' in admin
        for word in ("отсеяно по итогу", "ИЛ уже выдан", "уже в треке"):
            assert word in admin, f"в сводке нет корзины «{word}»"
        # Светофор свежести «+N из M» — тоже по обоим трекам.
        assert "(e.added || 0) + (e.added_bank || 0)" in admin
        assert "added_bank: record.added_bank || 0" in worker


# ── Карточка для исков ПРОТИВ банка (основная картотека, с 14.08.2026) ───────

class TestMainTrackCardRead:
    """До 14.08.2026 дело «банк-ответчик» заводилось из строки выдачи: без
    даты заседания и хронологии — до ближайшего прогона. Истцовые строки того
    же дампа карточку качали всегда."""

    @staticmethod
    def _defendant(env) -> dict:
        data = json.loads(env["json"].read_text(encoding="utf-8"))
        return {c["id"]: c for c in data["cases"]}["2-1001/2026"]

    def test_card_data_lands_in_record(self, import_env):
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        fi = self._defendant(import_env)["first_instance"]
        assert fi["events"], "хронология не приехала — карточку не прочитали"
        assert fi["hearing_date"]
        assert fi["last_event"]

    def test_intake_stamp_and_marker_set(self, import_env):
        """Пара неразделима: один только штамп навсегда выключит
        first_card_parse, и стародатный фильтр дайджеста умрёт молча."""
        from datetime import date as _date

        _run(import_env)
        fi = self._defendant(import_env)["first_instance"]
        assert _date.fromisoformat(fi["last_checked_at"])  # строго дата
        assert fi["intake_card_parse"] is True

    def test_srv_num_from_href_survives_card_merge(self, import_env):
        """build_json_entry не кладёт srv_num вовсе — переопределение из href
        обязано уцелеть под наложением карточки (двухсерверные суды)."""
        _run(import_env)
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        by_id = {c["id"]: c for c in data["cases"]}
        assert by_id["2-1001/2026"]["first_instance"]["srv_num"] == 1
        assert by_id["2-1001/2026"]["first_instance"]["delo_id"]
        assert by_id["2-1001/2026"]["initial_bank_role"] == "Ответчик"

    def test_fetch_failure_still_adds_case(self, import_env, monkeypatch):
        """Иск ПРОТИВ банка терять нельзя: карточка не открылась — заводим по
        строке выдачи, без штампа, прогон дочитает."""
        monkeypatch.setattr(isd, "fetch_card_checked",
                            lambda url, context=None: None)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2
        fi = self._defendant(import_env)["first_instance"]
        assert fi["events"] == []
        assert "last_checked_at" not in fi and "intake_card_parse" not in fi
        assert any("карточка недоступна" in l for l in s["lines"])

    def test_empty_shell_not_stamped(self, import_env, monkeypatch):
        """Заглушка sudrf (HTTP 200, ноль таблиц) карточкой не считается."""
        monkeypatch.setattr(isd, "fetch_card_checked",
                            lambda url, context=None: "<html><body>Информация "
                            "временно недоступна</body></html>")
        _run(import_env)
        fi = self._defendant(import_env)["first_instance"]
        assert "last_checked_at" not in fi

    def test_dry_run_stays_offline(self, import_env):
        """Кэп и dry-run считают запросы, а не роли."""
        _run(import_env, "--dry-run")
        assert import_env["card_calls"]["n"] == 0

    def test_cap_shared_with_bank_branch(self, import_env, monkeypatch):
        monkeypatch.setattr(isd, "MAX_BANK_CARDS_PER_IMPORT", 0)
        _run(import_env)
        assert import_env["card_calls"]["n"] == 0
        fi = self._defendant(import_env)["first_instance"]
        assert fi["events"] == []      # заведено, но card-blind
