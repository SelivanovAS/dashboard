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
        assert info["appeal_courts"] == [
            {"name": "Суд ХМАО-Югры", "domain": "oblsud--hmao.sudrf.ru",
             "delo_id": 5},
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
