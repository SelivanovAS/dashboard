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

import import_search_dump as isd  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.parsing.search import parse_first_instance_search  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

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
    """tmp-хранилище + активный регион Свердловск/ЯНАО + GITHUB_OUTPUT."""
    json_path = tmp_path / "cases.json"
    archive_path = tmp_path / "cases_archive.json"
    gh_out = tmp_path / "gh_output.txt"
    monkeypatch.setattr(cm_config, "JSON_PATH", str(json_path))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(archive_path))
    monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
    monkeypatch.setenv("GITHUB_OUTPUT", str(gh_out))
    dump = tmp_path / "dump.html"
    dump.write_text(_fixture("search_fi_all_roles.html"), encoding="utf-8")
    return {
        "tmp": tmp_path, "json": json_path, "archive": archive_path,
        "gh_out": gh_out, "dump": dump,
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
    def test_import_keeps_only_bank_defendant(self, import_env):
        """Только «банк-ответчик» — решение юриста 16.07.2026 (в 1-й инст.
        дела истца/третьего лица не отслеживаем, как и в автопоиске)."""
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        ids = [c["id"] for c in data["cases"]]
        # 2 добавлено: ответчики со ссылкой. 2-1002 (истец) и 2-1004 (третье
        # лицо) — [SKIPPED ROLE]; 2-1005 — ответчик без ссылки ([NO LINK]).
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
        assert s["already"] == 0
        assert s["skipped_role"] == 2
        assert s["no_link"] == 1
        assert s["subsidiary"] == 1
        assert s["court"] == "Академический районный суд г. Екатеринбурга"
        assert s["operator"] == "Творонович Ю.А."
        assert any("[SKIPPED ROLE] 2-1002/2026" in l for l in s["lines"])
        assert any("[SKIPPED ROLE] 2-1004/2026" in l for l in s["lines"])
        assert any("[NO LINK] 2-1005/2026" in l for l in s["lines"])
        assert any("[SUBSIDIARY] 2-1003/2026" in l for l in s["lines"])

    def test_rerun_dedupes_everything(self, import_env):
        _run(import_env)
        rc = _run(import_env)
        assert rc == isd.EXIT_OK
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 0
        assert s["already"] == 2
        assert s["skipped_role"] == 2  # пропуски ролей стабильны на повторе
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        assert len(data["cases"]) == 2  # дублей нет

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
        s = _read_summary(import_env["gh_out"])
        assert s["added"] == 2 and s["dry_run"] is True

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

    def test_record_without_domain_blocks_everywhere(self, import_env):
        """Запись без court_domain (легаси/«с апелляции») — wildcard:
        консервативно блокирует номер во всех судах (лучше пропуск, чем дубль).
        Так же работает архивный дедуп (архив в test_dedup_against_archive)."""
        import_env["json"].write_text(json.dumps({
            "version": 1,
            "cases": [{"id": "2-1001/2026", "current_stage": "appeal"}],
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
        неверно и дело НЕ отсеялось бы фильтром ролей)."""
        import_env["dump"].write_text(
            _fixture("search_fi_all_roles_pretty.html"), encoding="utf-8")
        _run(import_env)
        data = json.loads(import_env["json"].read_text(encoding="utf-8"))
        by_id = {c["id"]: c for c in data["cases"]}
        assert list(by_id) == ["2-1001/2026"]  # истец 2-1002 отсеян
        assert by_id["2-1001/2026"]["plaintiff"] == "Петров Пётр Петрович"
        s = _read_summary(import_env["gh_out"])
        assert s["skipped_role"] == 1
