# -*- coding: utf-8 -*-
"""Ветка АПЕЛЛЯЦИИ импортёра дампов (scripts/import_search_dump.py).

25.08.2026 Свердловский областной суд закрыл поиск проверочным кодом (карточки
при этом открыты — тем же прогоном прочитано 69 карточек из 118 дел стадии
appeal). Дела апелляции стали заводиться дампом выдачи тем же каналом, что и
дела капчёвых судов 1-й инстанции: админка → Worker → KV → import_cases.yml.

Тесты гоняют импортёр целиком через main() на tmp-файлах; сеть мокается —
карточка апелляции обязательна (в ней и только в ней живёт номер дела
1-й инстанции), поэтому её отказ проверяется отдельно.
"""

from __future__ import annotations

import json
import os
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
APPEAL_DOMAIN = "oblsud--svd.sudrf.ru"


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def appeal_card_html(fi_number: str = "", *, result: str = "") -> str:
    """Минимальная карточка апел. дела: якорь «Номер дела в первой инстанции».

    Больше карточке для приёма не нужно — статус/результат/события дочитает
    ближайший прогон по сохранённой ссылке.
    """
    fi_row = (
        f"<tr><td><b>Номер дела в первой инстанции</b></td><td>{fi_number}</td></tr>"
        if fi_number else ""
    )
    return (
        "<html><body>"
        "<table><tr><td>шапка</td></tr></table>"
        "<table><tr><td>меню</td></tr></table>"
        "<table id='tablcont'>"
        "<tr><td><b>Уникальный идентификатор дела</b></td>"
        "<td>66RS0001-01-2026-000001-01</td></tr>"
        "<tr><td><b>Судья-докладчик</b></td><td>Иванов И.И.</td></tr>"
        f"<tr><td><b>Результат рассмотрения</b></td><td>{result}</td></tr>"
        f"{fi_row}</table>"
        "<table><tr><td>Дата</td><td>Наименование события</td><td>Результат</td></tr>"
        "<tr><td>05.08.2026</td><td>Судебное заседание</td><td></td></tr></table>"
        "</body></html>"
    )


# Какая карточка отвечает по какому case_id дампа.
CARDS_BY_ID = {
    "9001": appeal_card_html("2-5001/2026"),   # дело 1-й инст. уже в базе
    "9002": appeal_card_html("2-5002/2026"),   # 1-й инстанции в базе нет
    "9006": None,                              # карточка не открывается
}


@pytest.fixture
def env(tmp_path, monkeypatch):
    """tmp-хранилище (JSON + CSV!) + регион Свердловск/ЯНАО + GITHUB_OUTPUT.

    ⚠️ CSV_PATH патчить обязательно: ветка апелляции пишет СТРОКУ CSV — без неё
    прогон не обойдёт карточку дела (update_active_cases идёт по строкам CSV).
    """
    paths = {
        "json": tmp_path / "cases.json",
        "archive": tmp_path / "cases_archive.json",
        "csv": tmp_path / "sberbank_cases.csv",
        "csv_archive": tmp_path / "sberbank_cases_archive.csv",
        "gh_out": tmp_path / "gh_output.txt",
        "dump": tmp_path / "dump.html",
    }
    monkeypatch.setattr(cm_config, "JSON_PATH", str(paths["json"]))
    monkeypatch.setattr(cm_config, "JSON_ARCHIVE_PATH", str(paths["archive"]))
    monkeypatch.setattr(cm_config, "CSV_PATH", str(paths["csv"]))
    monkeypatch.setattr(cm_config, "CSV_ARCHIVE_PATH", str(paths["csv_archive"]))
    monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
    monkeypatch.setenv("GITHUB_OUTPUT", str(paths["gh_out"]))
    monkeypatch.setattr(isd, "polite_delay", lambda: None)
    calls: list[str] = []

    def fake_card(url, context=None):
        calls.append(url)
        for cid, html in CARDS_BY_ID.items():
            if f"case_id={cid}" in url:
                if html is None:
                    # Класс отказа читается СРАЗУ после провала — сводке нужна
                    # причина, а не голый факт.
                    cm_config.FETCH_DIAG.clear()
                    cm_config.FETCH_DIAG["kind"] = "http_403"
                return html
        return appeal_card_html("")

    monkeypatch.setattr(isd, "fetch_card_checked", fake_card)
    paths["dump"].write_text(_fixture("search_appeal_dump_svd.html"),
                             encoding="utf-8")
    paths["calls"] = calls
    # ⚠️ FETCH_DIAG — глобальный dict: monkeypatch чинит атрибуты, а не их
    # содержимое, и подставленный тут «HTTP 403» утекал в соседние файлы
    # тестов (падал test_fetch_failure_still_adds_case импортёра 1-й инст.).
    diag_before = dict(cm_config.FETCH_DIAG)
    yield paths
    cm_config.FETCH_DIAG.clear()
    cm_config.FETCH_DIAG.update(diag_before)


def _seed(env, cases: list[dict]) -> None:
    env["json"].write_text(
        json.dumps({"version": 1, "cases": cases}, ensure_ascii=False),
        encoding="utf-8",
    )


def _summary(env) -> dict:
    text = env["gh_out"].read_text(encoding="utf-8")
    line = [l for l in text.splitlines() if l.startswith("summary=")][-1]
    return json.loads(line[len("summary="):])


def _run(env, *extra) -> int:
    return isd.main([
        str(env["dump"]),
        "--court-domain", APPEAL_DOMAIN,
        "--operator", "Творонович Ю.А.",
        *extra,
    ])


def _cases(env) -> list[dict]:
    return json.loads(env["json"].read_text(encoding="utf-8"))["cases"]


def _fi_case(num: str, *, stage: str = "awaiting_appeal") -> dict:
    """Дело 1-й инстанции с реальными данными карточки (не стаб)."""
    return {
        "id": num,
        "current_stage": stage,
        "plaintiff": "ПАО Сбербанк",
        "defendant": "Петров Пётр Петрович",
        "first_instance": {
            "case_number": num,
            "court": "Асбестовский городской суд",
            "court_domain": "asbestovsky--svd.sudrf.ru",
            "link": "555|dddd-5555",
            "sent_to_appeal": True,
            "events": [{"date": "01.07.2026", "text": "Решение по делу принято"}],
        },
    }


def _appeal_case(num: str, domain: str = APPEAL_DOMAIN) -> dict:
    return {
        "id": "2-7777/2026",
        "current_stage": "appeal",
        "first_instance": {"case_number": "2-7777/2026", "events": []},
        "appeal": {"case_number": num, "court_domain": domain,
                   "link": "1|2", "events": []},
    }


class TestAppealDumpImport:
    def test_new_case_added_with_csv_row(self, env):
        """Новое дело апелляции: запись JSON + СТРОКА CSV (иначе прогон
        никогда не перечитает карточку — обход идёт по строкам CSV)."""
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        by_ap = {(c.get("appeal") or {}).get("case_number"): c for c in _cases(env)}
        case = by_ap["33-2002/2026"]
        assert case["current_stage"] == "appeal"
        assert case["appeal"]["court_domain"] == APPEAL_DOMAIN
        assert case["appeal"]["link"] == "9002|aaaa-9002"
        assert case["first_instance"]["case_number"] == "2-5002/2026"
        # id остаётся апелляционным — ровно как у дел «с апелляции» боевого
        # прогона (на Урале таких 122 из 142): link_cases видит номер 1-й инст.
        # уже в самой записи и переименовывать её не нужно.
        assert case["id"] == "33-2002/2026"
        assert case["import"]["source"] == "dump_appeal"
        assert case["import"]["operator"] == "Творонович Ю.А."
        # announced НЕ ставим: дело объявит ближайший прогон ровно один раз.
        assert "announced" not in case["import"]
        csv_text = env["csv"].read_text(encoding="utf-8")
        assert "33-2002/2026" in csv_text

    def test_links_into_existing_first_instance_case(self, env):
        """Дело, ушедшее наверх, приклеивается к известной 1-й инстанции:
        одна запись, стадия appeal, «новым» оно не объявляется."""
        _seed(env, [_fi_case("2-5001/2026")])
        assert _run(env) == isd.EXIT_OK
        cases = _cases(env)
        owner = [c for c in cases if c["id"] == "2-5001/2026"]
        assert len(owner) == 1  # двойника нет
        owner = owner[0]
        assert owner["current_stage"] == "appeal"
        assert owner["appeal"]["case_number"] == "33-2001/2026"
        # История 1-й инстанции цела — слили, а не подменили.
        assert owner["first_instance"]["events"]
        # Блок import уехал вместе с записью-сиротой: дело не новое.
        assert "import" not in owner
        summary = _summary(env)
        assert summary["linked"] == 1

    def test_already_tracked_row_skipped(self, env):
        _seed(env, [_appeal_case("33-2003/2026")])
        assert _run(env) == isd.EXIT_OK
        summary = _summary(env)
        assert summary["already"] == 1
        assert any("[ALREADY] 33-2003/2026" in l for l in summary["lines"])
        # Карточку уже отслеживаемого дела не качаем.
        assert not any("case_id=9003" in u for u in env["calls"])

    def test_row_without_link_reported(self, env):
        """Вставка «как текст»: без case_id|case_uid карточка недостижима."""
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        summary = _summary(env)
        assert summary["no_link"] == 1
        assert any("[NO LINK] 33-2004/2026" in l for l in summary["lines"])

    def test_unreadable_card_loses_row_with_reason(self, env):
        """Карточка не открылась → дело НЕ заводим: номер 1-й инстанции живёт
        только в ней, и card-blind запись стала бы вечным двойником."""
        _seed(env, [])
        assert _run(env) == isd.EXIT_OK
        summary = _summary(env)
        assert summary["fetch_fail"] == 1
        assert "403" in summary["card_fail_reason"]
        assert any("[FETCH FAIL] 33-2006/2026" in l for l in summary["lines"])
        nums = {(c.get("appeal") or {}).get("case_number") for c in _cases(env)}
        assert "33-2006/2026" not in nums

    def test_same_number_in_other_appeal_court_is_not_already(self, env):
        """Номера 33-…/YYYY между апел-судами региона НЕ уникальны: дело Суда
        ЯНАО с тем же номером не должно давать ложное «уже отслеживается»."""
        _seed(env, [_appeal_case("33-2003/2026", "oblsud--ynao.sudrf.ru")])
        assert _run(env) == isd.EXIT_OK
        summary = _summary(env)
        assert summary["already"] == 0
        assert summary["added"] >= 1

    def test_dry_run_touches_nothing(self, env):
        _seed(env, [])
        assert _run(env, "--dry-run") == isd.EXIT_OK
        assert _cases(env) == []
        assert not env["csv"].exists()
        assert env["calls"] == []  # офлайн: карточки не качаем

    def test_counters_in_summary(self, env):
        _seed(env, [_fi_case("2-5001/2026"), _appeal_case("33-2003/2026")])
        assert _run(env) == isd.EXIT_OK
        s = _summary(env)
        assert (s["added"], s["linked"], s["already"],
                s["no_link"], s["fetch_fail"]) == (1, 1, 1, 1, 1)
        assert s["rows"] == 5  # дочка отсеяна парсером
        assert s["court"] == "Свердловский областной суд"


class TestAppealDumpGuards:
    def test_dump_of_another_court_refused(self, env, tmp_path):
        """Хосты ссылок карточек обязаны совпасть с выбранным судом."""
        _seed(env, [])
        rc = isd.main([
            str(env["dump"]),
            "--court-domain", "oblsud--ynao.sudrf.ru",
            "--operator", "оператор",
        ])
        assert rc == isd.EXIT_WRONG_COURT

    def test_first_instance_dump_named_by_section(self, env):
        """Выдача 1-й инстанции, отправленная как дамп апелляции: ловится по
        delo_id карточек, и отказ называет НУЖНЫЙ раздел."""
        _seed(env, [])
        dump = env["dump"]
        dump.write_text(
            _fixture("search_fi_all_roles.html").replace(
                'href="modules.php',
                'href="https://oblsud--svd.sudrf.ru/modules.php'),
            encoding="utf-8",
        )
        assert _run(env) == isd.EXIT_WRONG_COURT
        assert "апелляционных" in _summary(env)["error"]

    def test_captcha_dump_refused(self, env):
        _seed(env, [])
        env["dump"].write_text(_fixture("search_captcha_challenge.html"),
                               encoding="utf-8")
        assert _run(env) == isd.EXIT_CAPTCHA


class TestCountersReachOperator:
    """Сквозная проводка счётчика `linked`: jq-пейлоад → whitelist Worker'а →
    сводка админки. Пропуск любого звена = счётчик молча исчезает у оператора
    (класс поломки, которым проект болел трижды)."""

    def test_linked_in_jq_payload(self):
        with open(os.path.join(REPO_ROOT, "ops", "import_result_body.jq"),
                  encoding="utf-8") as f:
            assert "linked: (.linked // 0)" in f.read()

    def test_linked_in_worker_whitelist(self):
        with open(os.path.join(REPO_ROOT, "cloudflare-worker", "worker.js"),
                  encoding="utf-8") as f:
            text = f.read()
        start = text.index("function handleImportResult")
        assert '"linked"' in text[start:start + 4000]

    def test_linked_rendered_in_admin_summary(self):
        with open(os.path.join(REPO_ROOT, "cloudflare-worker", "admin_page.js"),
                  encoding="utf-8") as f:
            text = f.read()
        assert "item.linked" in text


class TestIntraDumpDuplicate:
    def test_row_repeated_in_one_dump_added_once(self, env, tmp_path):
        """Оператор скопировал страницу дважды: дедуп-снимок берётся ДО цикла,
        и без пополнения индекса на лету строка завела бы вторую запись."""
        _seed(env, [])
        html = _fixture("search_appeal_dump_svd.html")
        # Дублируем строку 33-2002/2026 внутри одного дампа.
        row = [l for l in html.splitlines() if "33-2002/2026" in l][0]
        env["dump"].write_text(html.replace(row, row + "\n" + row),
                               encoding="utf-8")
        assert _run(env) == isd.EXIT_OK
        nums = [(c.get("appeal") or {}).get("case_number") for c in _cases(env)]
        assert nums.count("33-2002/2026") == 1
        assert _summary(env)["already"] == 1  # второй проход — «уже в базе»
