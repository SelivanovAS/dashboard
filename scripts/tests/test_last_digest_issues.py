# -*- coding: utf-8 -*-
"""Дневной накопитель last_digest.json (save_last_digest, issues).

С 20.08.2026 (решение юриста) выпуски одного дня складываются, а не затирают
друг друга: гейт-дочитка даёт утру до двух прогонов, и второй выпуск стирал
первый с дашборда — утренние новости оставались только в Telegram.

Что охраняем:
1. Новый контекст того же дня — append (склейка html через «➕ Дополнение»).
2. Пере-рендер ТОГО ЖЕ контекста (issue_key) — replace НА МЕСТЕ: Mac-черновик
   → полированный replay через минуту не плодит второй выпуск, а повторный
   replay старого контекста не ломает порядок выпусков.
3. Новый день — файл начинается заново одним выпуском.
4. Легаси-файл без issues конвертируется в выпуск legacy:* и не теряется.
5. Верхнеуровневые поля — контракт фронта/админки: html = склейка,
   summary/generated_at = последний выпуск, is_empty = все ли пустые.

Запуск: python3 -m pytest scripts/tests/test_last_digest_issues.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config  # noqa: E402
from court_monitor.digest.core import save_last_digest  # noqa: E402


def _setup(tmp_path, monkeypatch):
    path = tmp_path / "last_digest.json"
    monkeypatch.setattr(config, "LAST_DIGEST_PATH", str(path))
    return path


def _read(path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_second_context_same_day_appends(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_last_digest("<b>утро</b>", summary="Новых: 1", issue_key="k1")
    save_last_digest("<b>дочитка</b>", summary="Изменений: 2", issue_key="k2")
    d = _read(path)
    assert [i["key"] for i in d["issues"]] == ["k1", "k2"]
    assert "утро" in d["html"] and "дочитка" in d["html"]
    assert "➕ <b>Дополнение (" in d["html"], "второй выпуск без заголовка-разделителя"
    assert d["html"].index("утро") < d["html"].index("дочитка"), "выпуски не хронологичны"
    assert d["summary"] == "Изменений: 2"
    assert d["generated_at"] == d["issues"][-1]["at"]


def test_same_key_replaces_in_place(tmp_path, monkeypatch):
    """Mac-черновик → полированный replay: тот же контекст, один выпуск."""
    path = _setup(tmp_path, monkeypatch)
    save_last_digest("<b>черновик</b>", summary="draft", issue_key="k1")
    save_last_digest("<b>полированный</b>", summary="polished", issue_key="k1")
    d = _read(path)
    assert len(d["issues"]) == 1
    assert "черновик" not in d["html"] and "полированный" in d["html"]
    assert "Дополнение" not in d["html"]


def test_replay_of_first_issue_keeps_order(tmp_path, monkeypatch):
    """Повторный replay СТАРОГО контекста при уже существующей дочитке не
    уводит первый выпуск в конец: замена на месте, хронология цела."""
    path = _setup(tmp_path, monkeypatch)
    save_last_digest("<b>утро-1</b>", issue_key="k1")
    save_last_digest("<b>дочитка</b>", issue_key="k2")
    save_last_digest("<b>утро-2</b>", issue_key="k1")
    d = _read(path)
    assert [i["key"] for i in d["issues"]] == ["k1", "k2"]
    assert d["html"].index("утро-2") < d["html"].index("дочитка")


def test_new_day_resets_file(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    stale = {
        "version": 1, "generated_at": "2026-01-01T08:00:00",
        "summary": "старое", "html": "<b>вчера</b>", "is_empty": False,
        "issues": [{"key": "old", "at": "2026-01-01T08:00:00",
                    "summary": "старое", "is_empty": False,
                    "html": "<b>вчера</b>"}],
    }
    path.write_text(json.dumps(stale), encoding="utf-8")
    save_last_digest("<b>сегодня</b>", issue_key="k1")
    d = _read(path)
    assert [i["key"] for i in d["issues"]] == ["k1"]
    assert "вчера" not in d["html"]


def test_legacy_file_becomes_first_issue(tmp_path, monkeypatch):
    """Файл прежнего формата (без issues) того же дня — первый выпуск дня,
    его html не теряется."""
    path = _setup(tmp_path, monkeypatch)
    today = datetime.now().isoformat(timespec="seconds")
    legacy = {"version": 1, "generated_at": today, "summary": "утро",
              "html": "<b>легаси</b>", "is_empty": False}
    path.write_text(json.dumps(legacy), encoding="utf-8")
    save_last_digest("<b>дочитка</b>", issue_key="k2")
    d = _read(path)
    assert len(d["issues"]) == 2
    assert d["issues"][0]["key"].startswith("legacy:")
    assert "легаси" in d["html"] and "дочитка" in d["html"]


def test_is_empty_is_all_issues_empty(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_last_digest("<b>тихо</b>", is_empty=True, issue_key="k1")
    d = _read(path)
    assert d["is_empty"] is True
    save_last_digest("<b>новости</b>", is_empty=False, issue_key="k2")
    d = _read(path)
    assert d["is_empty"] is False, "непустая дочитка обязана снимать is_empty дня"


def test_empty_html_still_not_written(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_last_digest("", summary="x", issue_key="k1")
    assert not path.exists()


def test_broken_previous_file_does_not_break_save(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    path.write_text("{битый json", encoding="utf-8")
    save_last_digest("<b>новый</b>", issue_key="k1")
    d = _read(path)
    assert d["html"] == "<b>новый</b>" and len(d["issues"]) == 1
