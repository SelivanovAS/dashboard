# -*- coding: utf-8 -*-
"""Дневной накопитель контекста дайджеста (save_digest_context, merge).

«Один дайджест в день» (решение юриста 20.08.2026): неполные утренние попытки
сохраняют данные и КОПЯТ новости в last_digest_context.json, ничего не
отправляя; замыкающая попытка (удачная или дедлайн 10:00) шлёт один дайджест
со всем накопленным. Дельты попыток дизъюнктны по построению (события уже
влиты в данные, флаги «объявлено» поставлены) — merge = конкатенация с
дедупом-поясом по json-идентичности.

Что охраняем:
1. Дельта того же дня ВЛИВАЕТСЯ в неотправленное накопление (не затирает).
2. delivered_at останавливает накопление: следующий прогон начинает свежий
   контекст (ручной дневной прогон не переотправит утро).
3. will_deliver=True (облачный прогон — доставляет сам) сразу закрывает день.
4. issue_key стабилен на всё накопление (выпуск на дашборде один).
5. Пустая дельта не трогает файл байт-в-байт (иначе холостой коммит каждые
   полчаса) и возвращает прежний issue_key.
6. Точный дубль записи не задваивается (пояс).
7. Новый день начинает файл заново.

Запуск: python3 -m pytest scripts/tests/test_digest_context_accumulator.py
"""

from __future__ import annotations

import json
import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config  # noqa: E402
from court_monitor.digest.core import save_digest_context  # noqa: E402


def _setup(tmp_path, monkeypatch):
    path = tmp_path / "last_digest_context.json"
    monkeypatch.setattr(config, "LAST_DIGEST_CONTEXT_PATH", str(path))
    return path


def _read(path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _ch(num: str) -> dict:
    return {"case": num, "type": ["fi_hearing_next"], "details": {}}


def test_same_day_delta_merges(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    k1 = save_digest_context([], [], fi_changes=[_ch("2-1/2026")],
                             total_active_fi=10)
    k2 = save_digest_context([], [], fi_changes=[_ch("2-2/2026")],
                             total_active_fi=11)
    d = _read(path)
    assert [c["case"] for c in d["fi_changes"]] == ["2-1/2026", "2-2/2026"]
    assert k1 == k2 == d["issue_key"], "issue_key обязан быть стабильным"
    assert d["total_active_fi"] == 11, "totals — свежие, не первые"
    assert "delivered_at" not in d


def test_delivered_stops_accumulation(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_digest_context([], [], fi_changes=[_ch("2-1/2026")])
    ctx = _read(path)
    ctx["delivered_at"] = ctx["saved_at"]
    path.write_text(json.dumps(ctx), encoding="utf-8")
    k2 = save_digest_context([], [], fi_changes=[_ch("2-2/2026")])
    d = _read(path)
    assert [c["case"] for c in d["fi_changes"]] == ["2-2/2026"], (
        "после доставки день закрыт — новый прогон начинает свежий контекст, "
        "иначе ручной дневной прогон переотправил бы утро"
    )
    # Свежий контекст несёт СВОЙ ключ (= свой saved_at). Неравенство со старым
    # ключом не проверяем: в тесте оба сохранения попадают в одну секунду, а
    # боевые слоты идут раз в полчаса.
    assert d["issue_key"] == k2 == d["saved_at"]


def test_will_deliver_marks_delivered(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_digest_context([], [], fi_changes=[_ch("2-1/2026")], will_deliver=True)
    d = _read(path)
    assert d.get("delivered_at") == d["saved_at"]


def test_cloud_delivery_absorbs_pending_morning(tmp_path, monkeypatch):
    """Ручной облачный прогон посреди Mac-накопления вливает утро и закрывает
    день (контекст не теряется; в сам облачный дайджест утро не попадает —
    осознанный редкий гибрид, задокументирован в runs.py)."""
    path = _setup(tmp_path, monkeypatch)
    save_digest_context([], [], fi_changes=[_ch("2-1/2026")])
    save_digest_context([], [], fi_changes=[_ch("2-2/2026")], will_deliver=True)
    d = _read(path)
    assert [c["case"] for c in d["fi_changes"]] == ["2-1/2026", "2-2/2026"]
    assert d.get("delivered_at")


def test_empty_delta_keeps_file_untouched(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    k1 = save_digest_context([], [], fi_changes=[_ch("2-1/2026")])
    before = path.read_bytes()
    k2 = save_digest_context([], [])
    assert path.read_bytes() == before, (
        "пустая дельта перезаписала накопление — saved_at бампнется и "
        "каждый холостой слот будет коммитить"
    )
    assert k1 == k2


def test_exact_duplicate_entry_not_doubled(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    save_digest_context([], [], fi_changes=[_ch("2-1/2026")])
    save_digest_context([], [], fi_changes=[_ch("2-1/2026"), _ch("2-3/2026")])
    d = _read(path)
    assert [c["case"] for c in d["fi_changes"]] == ["2-1/2026", "2-3/2026"]


def test_stale_day_starts_fresh(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch)
    stale = {"saved_at": "2026-01-01T08:00:00", "issue_key": "2026-01-01T08:00:00",
             "fi_changes": [_ch("2-9/2025")], "new_cases": [], "changes": [],
             "cases": [], "fi_new_cases": [], "stage_transitions": [],
             "cass_changes": [], "cass_discovered": []}
    path.write_text(json.dumps(stale), encoding="utf-8")
    save_digest_context([], [], fi_changes=[_ch("2-1/2026")])
    d = _read(path)
    assert [c["case"] for c in d["fi_changes"]] == ["2-1/2026"]


def test_cases_snapshot_is_latest_not_merged(tmp_path, monkeypatch):
    """«cases» — снимок картотеки для рендера, а не дельта: при merge берётся
    свежий, иначе файл рос бы на полный список дел каждой попыткой."""
    path = _setup(tmp_path, monkeypatch)
    save_digest_context([], [], cases=[{"id": "a"}], fi_changes=[_ch("2-1/2026")])
    save_digest_context([], [], cases=[{"id": "b"}], fi_changes=[_ch("2-2/2026")])
    d = _read(path)
    assert d["cases"] == [{"id": "b"}]
