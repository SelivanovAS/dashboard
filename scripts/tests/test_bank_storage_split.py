# -*- coding: utf-8 -*-
"""Split-хранение трека «Иски банка»: список + events отдельным файлом
(load_bank_json/save_bank_json), ротация bank-архива в холодные годовые,
дедуп холодных bank-файлов и watchlist-матчинг push-доставки по composite
«домен|номер».
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, delivery, linking, storage  # noqa: E402


def _bank_case(num: str, domain: str, events: list | None) -> dict:
    fi = {"case_number": num, "court_domain": domain, "status": "В производстве"}
    if events is not None:
        fi["events"] = events
    return {
        "id": num,
        "current_stage": "first_instance",
        "bank_role": "Истец",
        "track": "plaintiff_light",
        "first_instance": fi,
    }


EV_A = [{"date": "01.07.2026", "text": "Иск принят"}]
EV_B = [{"date": "02.07.2026", "text": "Назначено заседание"},
        {"date": "10.07.2026", "text": "Отложено"}]


class TestSaveLoadRoundTrip:
    def test_round_trip(self, tmp_path):
        lst = str(tmp_path / "cases_bank.json")
        ev = str(tmp_path / "cases_bank_events.json")
        data = {"version": 1, "track": "plaintiff_light", "cases": [
            _bank_case("2-100/2026", "a.sudrf.ru", EV_A),
            _bank_case("2-200/2026", "b.sudrf.ru", EV_B),
        ]}
        storage.save_bank_json(data, lst, ev)

        # На диске: список без events, events — мапой по composite-ключу.
        raw_list = json.loads(open(lst, encoding="utf-8").read())
        for c in raw_list["cases"]:
            assert "events" not in c["first_instance"]
        raw_ev = json.loads(open(ev, encoding="utf-8").read())
        assert raw_ev["events"]["a.sudrf.ru|2-100/2026"] == EV_A
        assert raw_ev["events"]["b.sudrf.ru|2-200/2026"] == EV_B

        # Обратная загрузка склеивает записи байт-в-байт по содержимому.
        loaded = storage.load_bank_json(lst, ev)
        by_id = {c["id"]: c for c in loaded["cases"]}
        assert by_id["2-100/2026"]["first_instance"]["events"] == EV_A
        assert by_id["2-200/2026"]["first_instance"]["events"] == EV_B

    def test_save_is_non_destructive(self, tmp_path):
        """После save_bank_json записи в data сохраняют events — пайплайн
        продолжает работать со склеенными записями."""
        data = {"version": 1, "cases": [_bank_case("2-1/2026", "a.sudrf.ru", EV_A)]}
        storage.save_bank_json(
            data, str(tmp_path / "l.json"), str(tmp_path / "e.json"))
        assert data["cases"][0]["first_instance"]["events"] == EV_A

    def test_same_number_different_courts(self, tmp_path):
        """Номера не уникальны между судами — composite-ключ не даёт
        событиям двух одноимённых дел перепутаться."""
        lst = str(tmp_path / "l.json")
        ev = str(tmp_path / "e.json")
        data = {"version": 1, "cases": [
            _bank_case("2-500/2026", "a.sudrf.ru", EV_A),
            _bank_case("2-500/2026", "b.sudrf.ru", EV_B),
        ]}
        storage.save_bank_json(data, lst, ev)
        loaded = storage.load_bank_json(lst, ev)
        by_dom = {c["first_instance"]["court_domain"]: c for c in loaded["cases"]}
        assert by_dom["a.sudrf.ru"]["first_instance"]["events"] == EV_A
        assert by_dom["b.sudrf.ru"]["first_instance"]["events"] == EV_B

    def test_legacy_monolith_reads_as_is(self, tmp_path):
        """Старый монолитный файл (events inline, events-файла нет) читается
        без потерь; inline events не перетираются пустой мапой."""
        lst = str(tmp_path / "cases_bank.json")
        storage.save_json(
            {"version": 1, "cases": [_bank_case("2-9/2026", "a.sudrf.ru", EV_B)]},
            lst,
        )
        loaded = storage.load_bank_json(lst, str(tmp_path / "нет_такого.json"))
        assert loaded["cases"][0]["first_instance"]["events"] == EV_B

    def test_missing_files_give_empty_skeleton(self, tmp_path):
        loaded = storage.load_bank_json(
            str(tmp_path / "нет.json"), str(tmp_path / "тоже_нет.json"))
        assert loaded["cases"] == []


class TestBankColdRotation:
    def test_rotate_with_bank_path_builder(self, tmp_path, monkeypatch):
        """Ротация горячего bank-архива: дело старше COLD_ARCHIVE_DAYS уезжает
        в cases_bank_archive_YYYY.json ПОЛНОЙ записью (events inline)."""
        monkeypatch.setattr(config, "JSON_BANK_ARCHIVE_PATH",
                            str(tmp_path / "cases_bank_archive.json"))
        old_stamp = (datetime.now() - timedelta(days=400)).date()
        old = _bank_case("2-7/2025", "a.sudrf.ru", EV_A)
        old["archived_at"] = old_stamp.isoformat()
        fresh = _bank_case("2-8/2026", "a.sudrf.ru", EV_B)
        fresh["archived_at"] = datetime.now().date().isoformat()

        kept = linking.rotate_cold_archive(
            [old, fresh], path_builder=config.bank_cold_archive_path)

        assert [c["id"] for c in kept] == ["2-8/2026"]
        cold_path = config.bank_cold_archive_path(old_stamp.year)
        cold = json.loads(open(cold_path, encoding="utf-8").read())
        assert cold["cases"][0]["id"] == "2-7/2025"
        assert cold["cases"][0]["first_instance"]["events"] == EV_A

    def test_rotate_same_number_other_court_not_swallowed(self, tmp_path, monkeypatch):
        """Дело суда Б с тем же номером, что уже лежит в холодном файле от
        суда А, НЕ считается дублем (composite-сверка) — иначе запись
        потерялась бы насовсем."""
        monkeypatch.setattr(config, "JSON_BANK_ARCHIVE_PATH",
                            str(tmp_path / "cases_bank_archive.json"))
        stamp = (datetime.now() - timedelta(days=400)).date()
        first = _bank_case("2-1/2025", "a.sudrf.ru", None)
        first["archived_at"] = stamp.isoformat()
        linking.rotate_cold_archive(
            [first], path_builder=config.bank_cold_archive_path)

        second = _bank_case("2-1/2025", "b.sudrf.ru", None)
        second["archived_at"] = stamp.isoformat()
        linking.rotate_cold_archive(
            [second], path_builder=config.bank_cold_archive_path)

        cold = json.loads(
            open(config.bank_cold_archive_path(stamp.year), encoding="utf-8").read())
        domains = {c["first_instance"]["court_domain"] for c in cold["cases"]}
        assert domains == {"a.sudrf.ru", "b.sudrf.ru"}

    def test_glob_filter_excludes_events_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "JSON_BANK_ARCHIVE_PATH",
                            str(tmp_path / "cases_bank_archive.json"))
        assert config.is_bank_cold_archive_file(
            str(tmp_path / "cases_bank_archive_2025.json"))
        assert not config.is_bank_cold_archive_file(
            str(tmp_path / "cases_bank_archive_events.json"))
        assert not config.is_bank_cold_archive_file(
            str(tmp_path / "cases_bank_archive.json"))


class TestImporterSeesColdBank:
    def test_load_all_tracked_includes_bank_cold(self, tmp_path, monkeypatch):
        """Дедуп импортёров видит холодные bank-файлы: старое дело из
        cases_bank_archive_YYYY.json не заведётся заново."""
        for name in ("JSON_PATH", "JSON_ARCHIVE_PATH", "JSON_BANK_PATH",
                     "JSON_BANK_ARCHIVE_PATH"):
            monkeypatch.setattr(config, name, str(tmp_path / f"{name}.json"))
        monkeypatch.setattr(
            config, "JSON_BANK_ARCHIVE_PATH",
            str(tmp_path / "cases_bank_archive.json"))
        cold = tmp_path / "cases_bank_archive_2025.json"
        cold.write_text(json.dumps(
            {"version": 1, "cases": [_bank_case("2-77/2025", "a.sudrf.ru", None)]},
            ensure_ascii=False), encoding="utf-8")
        # events-файл архива glob тоже цепляет — он должен быть отфильтрован,
        # а не прочитан как список дел.
        (tmp_path / "cases_bank_archive_events.json").write_text(
            '{"version": 1, "events": {}}', encoding="utf-8")

        import import_bank_registry as ibr
        tracked = ibr.load_all_tracked()
        assert any(c.get("id") == "2-77/2025" for c in tracked)


class TestPushWatchlistBankMatching:
    def _bank_change(self):
        return {
            "case": "2-4440/2026",
            "court": "Сургутский городской суд",
            "track": "plaintiff_light",
            "type": ["fi_writ_issued"],
            "details": {"court_domain": "surggor--hmao.sudrf.ru",
                        "writs": [{"issue_date": "20.07.2026"}]},
        }

    def test_composite_entry_matches(self):
        f = delivery._filter_events_by_watchlist(
            {"surggor--hmao.sudrf.ru|2-4440/2026"},
            fi_new_cases=[], fi_changes=[self._bank_change()],
            stage_transitions=[], appeal_new_cases_csv=[], changes=[],
        )
        assert len(f["fi_changes"]) == 1

    def test_bare_entry_matches_as_fallback(self):
        f = delivery._filter_events_by_watchlist(
            {"2-4440/2026"},
            fi_new_cases=[], fi_changes=[self._bank_change()],
            stage_transitions=[], appeal_new_cases_csv=[], changes=[],
        )
        assert len(f["fi_changes"]) == 1

    def test_foreign_composite_does_not_match(self):
        f = delivery._filter_events_by_watchlist(
            {"other--court.sudrf.ru|2-4440/2026"},
            fi_new_cases=[], fi_changes=[self._bank_change()],
            stage_transitions=[], appeal_new_cases_csv=[], changes=[],
        )
        assert f["fi_changes"] == []

    def test_global_push_ignores_bank_events(self):
        """Подписчик без watchlist: одни bank-события → push не уходит,
        обычное FI-событие → уходит."""
        cb = delivery._make_per_sub_callback(
            cases=[], fi_new_cases=[], fi_changes=[self._bank_change()],
            changes=[], stage_transitions=[], appeal_new_cases_csv=[],
            push_summary="сводка",
        )
        assert cb({"watchlist": []}) is None

        ordinary = {"case": "2-1/2026", "type": ["fi_resolved"], "details": {}}
        cb2 = delivery._make_per_sub_callback(
            cases=[], fi_new_cases=[], fi_changes=[self._bank_change(), ordinary],
            changes=[], stage_transitions=[], appeal_new_cases_csv=[],
            push_summary="сводка",
        )
        assert cb2({"watchlist": []}) is not None

    def test_watchlist_subscriber_gets_bank_push(self):
        cb = delivery._make_per_sub_callback(
            cases=[], fi_new_cases=[], fi_changes=[self._bank_change()],
            changes=[], stage_transitions=[], appeal_new_cases_csv=[],
            push_summary="сводка",
        )
        res = cb({"watchlist": ["surggor--hmao.sudrf.ru|2-4440/2026"]})
        assert res is not None
        title, body, url = res
        assert "твои дела" in title
        assert "mine=1" in url

    def test_moved_case_composite_resolves_via_alias(self):
        """Bank-дело переехало в основной cases.json — composite-звезда
        резолвится в bare-канон через alias-карту (звезда «оживает»)."""
        moved = {
            "id": "2-4440/2026",
            "first_instance": {"case_number": "2-4440/2026",
                               "court_domain": "surggor--hmao.sudrf.ru"},
            "appeal": {"case_number": "33-100/2026"},
        }
        a2c, c2a = delivery._build_watchlist_alias_indexes([moved])
        wl = delivery._expand_watchlist_via_aliases(
            ["surggor--hmao.sudrf.ru|2-4440/2026"], a2c, c2a)
        assert "2-4440/2026" in wl
        assert "33-100/2026" in wl
