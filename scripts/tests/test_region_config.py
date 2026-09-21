# -*- coding: utf-8 -*-
"""Регион как конфиг (этапы 0.4–0.6 тиражирования): лоадер get_region,
производные ключи RegionConfig и регион-зависимые куски дайджеста."""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import update_cases as uc  # noqa: E402
from court_monitor import config as cm_config  # noqa: E402
from court_monitor.digest import template as cm_template  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig, RegionConfig  # noqa: E402


class TestGetRegion:
    def test_default_is_hmao(self):
        assert get_region().code == "hmao"

    def test_explicit_code(self):
        assert get_region("hmao").name == "ХМАО-Югра"

    def test_unknown_code_raises(self):
        with pytest.raises(ValueError, match="Неизвестный регион"):
            get_region("atlantida")

    def test_reads_config_region(self, monkeypatch):
        """config.X-инвариант: get_region() читает config.REGION на каждый
        вызов — подмена через monkeypatch видна без переимпорта."""
        monkeypatch.setattr(cm_config, "REGION", "nowhere")
        with pytest.raises(ValueError):
            get_region()


class TestRegionConfigDerived:
    def test_hmao_health_cassation_keys_historic(self):
        """Ключи здоровья ХМАО обязаны совпасть с историческими — иначе
        parse_health.json потеряет медианы."""
        assert get_region("hmao").health_cassation_keys() == (
            "cassation:7kas:total", "cassation:7kas:hmao",
        )

    def test_fi_default_delo_id(self):
        assert get_region("hmao").fi_default_delo_id == 1540005

    def test_facade_matches_region(self):
        r = get_region("hmao")
        assert uc.APPEAL_COURT.domain == r.appeal_courts[0].domain
        assert len(uc.FIRST_INSTANCE_COURTS) == len(r.first_instance_courts)
        assert uc.CASSATION_COURT.domain == r.cassation_court.domain

    def test_public_info_shape(self):
        """Блок region в cases.json — контракт фронта (app.js: подписи судов
        и ссылки апелляции/кассации)."""
        info = get_region("hmao").public_info()
        assert info["code"] == "hmao"
        # search_gated/srv_num — с 25.08.2026: капча бывает и на апелляции,
        # и оттуда же строится dropdown дампов админки. С 04.09.2026 под кодом
        # и Суд ХМАО-Югры — режим как у СВД (оба флага), суд встаёт первой
        # закреплённой строкой секции «Импорт».
        assert info["appeal_courts"] == [
            {"name": "Суд ХМАО-Югры", "domain": "oblsud--hmao.sudrf.ru",
             "delo_id": 5, "search_gated": True, "search_disabled": True,
             "srv_num": 1, "timezone": "Asia/Yekaterinburg", "new": 5},
        ]
        assert info["cassation"]["domain"] == "7kas.sudrf.ru"
        assert info["cassation"]["new"] == 2800001
        # Внутренние поля (маркеры, health-ключи) наружу не отдаём.
        assert "fi_region_markers" not in info

    def test_save_json_stamps_region_only_for_main(self, monkeypatch, tmp_path):
        """save_json пишет блок region в основной cases.json и НЕ пишет в
        архивы (фронт грузит архив без блока)."""
        import json
        from court_monitor import storage
        main_p = str(tmp_path / "cases.json")
        arch_p = str(tmp_path / "cases_archive.json")
        monkeypatch.setattr(cm_config, "JSON_PATH", main_p)
        storage.save_json({"version": 1, "cases": []}, main_p)
        storage.save_json({"version": 1, "cases": []}, arch_p)
        with open(main_p, encoding="utf-8") as f:
            assert json.load(f)["region"]["code"] == "hmao"
        with open(arch_p, encoding="utf-8") as f:
            assert "region" not in json.load(f)


class TestMatchRegionFirstInstance:
    """Смоук «регион переключается»: матчер работает по ЯВНО переданному
    региону (не по активному) — включая регион с ДВУМЯ апелляциями."""

    FAKE = RegionConfig(
        code="fake2",
        name="Тестовия",
        digest_title="Мониторинг дел Сбербанка Тестовия",
        appeal_courts=(
            CourtConfig("Тестовский областной суд", "oblsud--fk1.sudrf.ru", 5, "appeal"),
            CourtConfig("Суд Фейского АО", "oblsud--fk2.sudrf.ru", 5, "appeal"),
        ),
        first_instance_courts=(
            CourtConfig("Октябрьский районный суд", "oktb--fk.sudrf.ru", 777, "first_instance"),
        ),
        cassation_court=CourtConfig("Седьмой КСОЮ", "7kas.sudrf.ru", 2800001, "cassation"),
        fi_region_markers=("тестовской области", "фейского автономного округа"),
        appeal_long_markers=(
            ("тестовский областной суд", "oblsud--fk1.sudrf.ru"),
            ("суд фейского автономного округа", "oblsud--fk2.sudrf.ru"),
        ),
    )

    def test_fi_court_matched_by_region_marker(self):
        got = uc.match_region_first_instance(
            "Октябрьский районный суд г. Тестова Тестовской области", self.FAKE
        )
        assert got is self.FAKE.first_instance_courts[0]

    def test_each_appeal_court_matched_by_its_marker(self):
        got1 = uc.match_region_first_instance("Тестовский областной суд", self.FAKE)
        got2 = uc.match_region_first_instance(
            "Суд Фейского автономного округа", self.FAKE
        )
        assert got1 is self.FAKE.appeal_courts[0]
        assert got2 is self.FAKE.appeal_courts[1]

    def test_foreign_region_rejected(self):
        """Одноимённый суд чужого региона (ХМАО) не матчится в фейк-регион."""
        assert uc.match_region_first_instance(
            "Октябрьский районный суд Ханты-Мансийского автономного округа-Югры",
            self.FAKE,
        ) is None

    def test_hmao_registry_via_explicit_region(self):
        """Тот же вызов с явным ХМАО — прежняя семантика match_hmao_*."""
        hmao = get_region("hmao")
        got = uc.match_region_first_instance(
            "Урайский городской суд Ханты-Мансийского автономного округа-Югры",
            hmao,
        )
        assert got is not None and got.name == "Урайский городской суд"


class TestSverdlovskYanaoRegion:
    """Регион этапа 1: 54 записи судов Свердловской области (52 суда, две
    вторые площадки; все search_gated — поиск за капчей) + 12 судов ЯНАО
    (автопоиск) + ДВА апел-суда (Свердловский облсуд + Суд ЯНАО), кассация —
    тот же 7-й КСОЮ."""

    def test_loads_with_two_appeal_courts(self):
        r = get_region("sverdlovsk_yanao")
        assert [c.domain for c in r.appeal_courts] == [
            "oblsud--svd.sudrf.ru", "oblsud--ynao.sudrf.ru",
        ]
        assert len(r.first_instance_courts) == 54 + 12
        assert r.cassation_court.domain == "7kas.sudrf.ru"
        assert r.health_cassation_keys() == (
            "cassation:7kas:total", "cassation:7kas:sverdlovsk_yanao",
        )

    def test_sverdlovsk_registry_shape(self):
        """Свердловские суды: все за капчей (search_gated), delo_id стандартный,
        ЯНАО — полный автопоиск; Академический — первый проверочный."""
        r = get_region("sverdlovsk_yanao")
        # Свердловские — всё, что не ЯНАО: у Кировградского домен --cvd
        # (не --svd; подтверждено пробой 16.07.2026).
        svd = [c for c in r.first_instance_courts if "--ynao." not in c.domain]
        ynao = [c for c in r.first_instance_courts if "--ynao." in c.domain]
        assert len(svd) == 54 and len(ynao) == 12
        assert any(c.domain == "kirovgradsky--cvd.sudrf.ru" for c in svd)
        assert all(c.search_gated for c in svd)
        assert not any(c.search_gated for c in ynao)
        assert all(c.delo_id == 1540005 for c in svd)
        assert svd[0].domain == "akademicheskiy--svd.sudrf.ru"
        # Две вторые площадки на общих доменах: Камышловский и Красноуфимский.
        two_srv = sorted(c.domain for c in svd if c.srv_num == 2)
        assert two_srv == [
            "kamyshlovsky--svd.sudrf.ru",
            "krasnoufimsky--svd.sudrf.ru",
            "zheleznodorozhny--svd.sudrf.ru",  # единственная площадка на srv 2
        ]

    def test_courts_for_search_excludes_gated(self):
        """Автопоиск региона — только 12 ЯНАО-судов; капчёвые исключены,
        порядок сохраняется."""
        r = get_region("sverdlovsk_yanao")
        searchable = uc.courts_for_search(list(r.first_instance_courts))
        assert [c.domain for c in searchable] == [
            c.domain for c in r.first_instance_courts if "--ynao." in c.domain
        ]

    def test_courts_for_search_excludes_disabled(self):
        c_on = CourtConfig("А", "a.sudrf.ru", 1540005, "first_instance")
        c_off = CourtConfig("Б", "b.sudrf.ru", 1540005, "first_instance", enabled=False)
        c_gated = CourtConfig("В", "c.sudrf.ru", 1540005, "first_instance", search_gated=True)
        assert uc.courts_for_search([c_off, c_on, c_gated]) == [c_on]

    def test_fi_courts_in_public_info(self):
        """Блок fi_courts (dropdown импорта в админке): имя, домен, срв,
        флаг капчи; у ХМАО gated-судов нет — секция импорта прячется."""
        info = get_region("sverdlovsk_yanao").public_info()
        fi = info["fi_courts"]
        assert len(fi) == 66
        akadem = fi[0]
        assert akadem == {
            "name": "Академический районный суд г. Екатеринбурга",
            "domain": "akademicheskiy--svd.sudrf.ru",
            "search_gated": True,
            "search_disabled": False,
            "srv_num": 1,
            "delo_id": 1540005,
            "timezone": "Asia/Yekaterinburg",
        }
        assert any(not c["search_gated"] for c in fi)  # ЯНАО ищется
        hmao_fi = get_region("hmao").public_info()["fi_courts"]
        assert len(hmao_fi) == 20
        assert not any(c["search_gated"] for c in hmao_fi)

    def test_ynao_fi_court_matches(self):
        r = get_region("sverdlovsk_yanao")
        got = uc.match_region_first_instance(
            "Пуровский районный суд Ямало-Ненецкого автономного округа", r
        )
        assert got is not None and got.domain == "purovsky--ynao.sudrf.ru"

    def test_appeal_courts_as_first_instance(self):
        """Облсуд/окружной суд как 1-я инстанция → соответствующий апел-суд."""
        r = get_region("sverdlovsk_yanao")
        got_svd = uc.match_region_first_instance("Свердловский областной суд", r)
        got_ynao = uc.match_region_first_instance(
            "Суд Ямало-Ненецкого автономного округа", r
        )
        assert got_svd is not None and got_svd.domain == "oblsud--svd.sudrf.ru"
        assert got_ynao is not None and got_ynao.domain == "oblsud--ynao.sudrf.ru"

    def test_sverdlovsk_fi_courts_match(self):
        """Свердловские суды в реестре: длинная форма 7kas матчится в свой
        CourtConfig; одноимённые суды двух городов не путаются."""
        r = get_region("sverdlovsk_yanao")
        cases = {
            "Октябрьский районный суд г. Екатеринбурга Свердловской области":
                "oktiabrsky--svd.sudrf.ru",
            "Ленинский районный суд г. Екатеринбурга Свердловской области":
                "leninskyeka--svd.sudrf.ru",
            "Ленинский районный суд г. Нижний Тагил Свердловской области":
                "leninskytag--svd.sudrf.ru",
            "Алапаевский городской суд Свердловской области":
                "alapaevsky--svd.sudrf.ru",
        }
        for long_name, domain in cases.items():
            got = uc.match_region_first_instance(long_name, r)
            assert got is not None and got.domain == domain, long_name

    def test_second_server_entries_not_matched_by_long_name(self):
        """Вторые площадки («(сервер 2)») в длинной форме 7kas отдельно не
        пишутся — матчер отдаёт запись первого сервера."""
        r = get_region("sverdlovsk_yanao")
        got = uc.match_region_first_instance(
            "Камышловский районный суд Свердловской области", r
        )
        assert got is not None
        assert got.domain == "kamyshlovsky--svd.sudrf.ru" and got.srv_num == 1

    def test_perm_sverdlovsky_district_court_not_matched(self):
        """«Свердловский районный суд г. Перми» — район ГОРОДА ПЕРМИ, а не
        Свердловская область (Пермский край в юрисдикции 7-го КСОЮ — норма
        выдачи). Голая подстрока «свердловск» в fi_region_markers пропускала
        его через guard и заодно поднимала WARNING «похож на регион»
        (лог Урала 12.08.2026)."""
        import re
        r = get_region("sverdlovsk_yanao")
        perm = "Свердловский районный суд г. Перми"
        assert uc.match_region_first_instance(perm, r) is None
        assert re.search(r.fi_suspect_regex, perm, re.IGNORECASE) is None

    def test_suspect_regex_still_covers_region_forms(self):
        """Детектор рассинхрона обязан по-прежнему ловить настоящие суды
        региона (страховка «ё/е» и склонений живёт на suspect_regex)."""
        import re
        r = get_region("sverdlovsk_yanao")
        for long_name in (
            "Алапаевский городской суд Свердловской области",
            "Ленинский районный суд г. Нижний Тагил Свердловской области",
            "Октябрьский районный суд г. Екатеринбурга",
            "Пуровский районный суд Ямало-Ненецкого автономного округа",
        ):
            assert re.search(r.fi_suspect_regex, long_name, re.IGNORECASE), \
                long_name

    def test_cross_region_matrix(self):
        """Матрица «длинное имя → ровно один регион»: реестры ХМАО и
        Свердловск+ЯНАО не перехватывают чужие суды (одноимённые районные
        есть в обоих субъектах)."""
        hmao = get_region("hmao")
        svd = get_region("sverdlovsk_yanao")
        cases = {
            "Октябрьский районный суд Ханты-Мансийского автономного округа-Югры":
                ("hmao", "oktb--hmao.sudrf.ru"),
            "Салехардский городской суд Ямало-Ненецкого автономного округа":
                ("sverdlovsk_yanao", "salehardsky--ynao.sudrf.ru"),
            "Суд Ханты-Мансийского автономного округа - Югры":
                ("hmao", "oblsud--hmao.sudrf.ru"),
            "Суд Ямало-Ненецкого автономного округа":
                ("sverdlovsk_yanao", "oblsud--ynao.sudrf.ru"),
        }
        for long_name, (owner, domain) in cases.items():
            got_h = uc.match_region_first_instance(long_name, hmao)
            got_s = uc.match_region_first_instance(long_name, svd)
            if owner == "hmao":
                assert got_h is not None and got_h.domain == domain, long_name
                assert got_s is None, f"svd_yanao перехватил: {long_name}"
            else:
                assert got_s is not None and got_s.domain == domain, long_name
                assert got_h is None, f"hmao перехватил: {long_name}"


class TestDigestHeaderFromRegion:
    # Пустой контекст уводит рендер в путь «изменений не было» (другой
    # заголовок + подклейка прошлого дайджеста из data/last_digest.json) —
    # поэтому заголовок проверяем на НЕпустом контексте с одним изменением.
    _ONE_CHANGE = [{
        "case": "33-42/2026", "type": ["status_change"],
        "details": {"plaintiff": "Иванов И.И.", "defendant": "ПАО Сбербанк",
                    "category": "", "case_url": "",
                    "old_status": "В производстве",
                    "new_status": "Приостановлено"},
    }]

    def _render(self) -> str:
        return uc.generate_template_digest(
            new_cases=[], changes=list(self._ONE_CHANGE), fi_new_cases=[],
            fi_changes=[], cass_changes=[], cass_discovered=[],
            total_active_appeal=1, total_active_fi=0, total_active_cassation=0,
        )

    def test_header_uses_region_digest_title(self, monkeypatch):
        fake = RegionConfig(
            code="test",
            name="Тестовая область",
            digest_title="Мониторинг дел Сбербанка Тест-область",
            appeal_courts=(CourtConfig("Тестовый облсуд", "oblsud--test.sudrf.ru", 5, "appeal"),),
            first_instance_courts=(),
            cassation_court=CourtConfig("Седьмой КСОЮ", "7kas.sudrf.ru", 2800001, "cassation"),
            fi_region_markers=("тестов",),
        )
        # Патчим модуль-дом рендера: template.py зовёт get_region() в момент
        # сборки заголовка.
        monkeypatch.setattr(cm_template, "get_region", lambda code=None: fake)
        html = self._render()
        assert "📊 <b>Мониторинг дел Сбербанка Тест-область — " in html
        assert "Мониторинг дел Сбербанка ХМАО-Югра — " not in html

    def test_hmao_header_unchanged(self):
        html = self._render()
        assert "📊 <b>Мониторинг дел Сбербанка ХМАО-Югра — " in html


class TestBashkortostanRegion:
    """Новый реестр: исходные строки не теряются, СП не становятся выдуманными базами."""

    @staticmethod
    def _registry():
        import json
        from pathlib import Path
        return json.loads(
            (Path(__file__).resolve().parents[2] / "docs/regions/bashkortostan_courts.json")
            .read_text(encoding="utf-8")
        )

    def test_source_rows_and_runtime_courts_agree(self):
        r = get_region("bashkortostan")
        registry = self._registry()
        source = [row for row in registry["records"] if row["source_row"] is not None]
        assert sorted(row["source_row"] for row in source) == list(range(5, 74))
        assert len({row["source_bpm_id"] for row in source}) == 69
        assert len(r.first_instance_courts) == 45
        expected = {
            (row["runtime_name"], row["runtime_domain"])
            for row in source if row["record_type"] == "first_instance"
        }
        assert {(c.name, c.domain) for c in r.first_instance_courts} == expected
        assert [c.domain for c in r.appeal_courts] == ["vs--bkr.sudrf.ru"]
        assert [c.domain for c in r.presidium_courts] == ["vs--bkr.sudrf.ru"]
        assert not r.presidium_courts[0].search_gated
        assert not r.presidium_courts[0].search_disabled
        assert r.manual_import_all_courts
        assert r.public_info()["manual_import_all_courts"] is True

    def test_presences_share_verified_base_without_duplicate_runtime_courts(self):
        r = get_region("bashkortostan")
        presences = [
            row for row in self._registry()["records"]
            if row["record_type"] == "judicial_presence"
        ]
        assert len(presences) == 23
        assert all(row["configured_srv_num"] is None for row in presences)
        assert all(not row["runtime_entry"] for row in presences)
        assert len({c.domain for c in r.first_instance_courts}) == 45
        assert r.extra["presence_server_mapping_complete"]
        assert all(row["mapped_srv_num"] == 1 for row in presences)
        assert all(row["building_id"] for row in presences)
        mezhgorye = next(row for row in presences if row["source_row"] == 12)
        assert mezhgorye["confirmed_srv_nums"] == [1]
        assert "completeness_unverified" in mezhgorye["mapping_status"]
        assert mezhgorye["building_id"] == "913410001"
        fedorovka = next(row for row in presences if row["source_row"] == 69)
        assert fedorovka["building_id"] == "913410001"
        assert "МС" not in fedorovka["building_name"]

    def test_original_conflicting_akyar_id_not_silently_rewritten(self):
        by_row = {row["source_row"]: row for row in self._registry()["records"]}
        assert by_row[8]["source_gas_id"] == "03RS0040_Akiar"
        assert by_row[8]["source_id_conflict"]
        assert by_row[8]["runtime_domain"] == "zilairsky--bkr.sudrf.ru"
        assert by_row[8]["parent_source_row"] == 48
        assert by_row[45]["source_gas_id"] == "03RS0040"
        assert by_row[48]["source_gas_id"] == "03RS0043"

    def test_sixth_cassation_search_gated_but_cards_enabled(self):
        from urllib.parse import parse_qs, urlsplit
        c = get_region("bashkortostan").cassation_court
        assert c.enabled and c.search_gated and c.search_disabled
        assert uc.courts_for_search([c]) == []
        search = urlsplit(c.search_url())
        params = parse_qs(search.query)
        assert search.netloc == "6kas.sudrf.ru"
        assert params["delo_id"] == params["new"] == ["2800001"]
        assert params["delo_table"] == ["g33_case"]
        assert "G33_PARTS__NAMESS" in params
        card = urlsplit(c.card_url("24413318", "268d4cf1-f089-48be-b3c8-44b86ad5d164"))
        assert card.netloc == "6kas.sudrf.ru"
        assert parse_qs(card.query)["name_op"] == ["case"]

    def test_local_schedule_and_samara_hearings_have_distinct_zones(self):
        r = get_region("bashkortostan")
        assert r.timezone == "Asia/Yekaterinburg"
        assert r.tz_offset_hours == 5
        assert r.cassation_court.timezone == "Europe/Samara"
        assert r.health_cassation_keys() == (
            "cassation:6kas:total", "cassation:6kas:bashkortostan",
        )

    def test_network_uncertainty_does_not_disable_first_instance_search(self):
        r = get_region("bashkortostan")
        assert all(c.enabled and not c.search_gated and not c.search_disabled
                   for c in (*r.first_instance_courts, *r.appeal_courts))
        assert not r.extra["registry_live_verification_complete"]

    def test_canonical_first_instance_names_match_only_own_region(self):
        r = get_region("bashkortostan")
        for c in r.first_instance_courts:
            assert uc.match_region_first_instance(
                f"{c.name} Республики Башкортостан", r,
            ) is c
        assert uc.match_region_first_instance(
            "Кировский районный суд г. Казани Республики Татарстан", r,
        ) is None
        assert uc.match_region_first_instance("Верховный суд Республики Башкортостан", r) is None

    @pytest.mark.parametrize("name,domain", [
        ("Бижбулякский районный суд", "bizhbuliaksky--bkr.sudrf.ru"),
        ("Зилаирский районный суд", "zilairsky--bkr.sudrf.ru"),
        ("Кумертауский городской суд", "kumertauskiy--bkr.sudrf.ru"),
        ("Белорецкий городской суд", "belorecky--bkr.sudrf.ru"),
        ("Октябрьский районный суд г.Уфы", "oktiabrsky--bkr.sudrf.ru"),
        ("Октябрьский городской суд", "oktabrsky--bkr.sudrf.ru"),
        ("Салаватский городской суд", "salavatsky--bkr.sudrf.ru"),
        ("Салаватский межрайонный суд", "salavatskiy--bkr.sudrf.ru"),
    ])
    def test_verified_name_variants_and_similar_courts_stay_distinct(self, name, domain):
        r = get_region("bashkortostan")
        got = uc.match_region_first_instance(f"{name} Республики Башкортостан", r)
        assert got is not None and got.domain == domain
