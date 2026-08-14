# -*- coding: utf-8 -*-
"""Разведка судебных присутствий (--scan-servers в build_region_registry.py).

Судебное присутствие живёт на домене районного суда отдельным сервером
(srv_num=2+: Покачи в ХМАО, Камышловский/Красноуфимский на Урале), и суд без
записи в конфиге невидим целиком. Обычная проба по CSV ходит только на сервер
1 — площадок не находит; этот режим разбирает селектор площадок страницы
sud_delo и сверяет с конфигом региона. Сеть в тестах не трогается: разбор и
сверка — чистые функции.
"""

from __future__ import annotations

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT_DIR = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import build_region_registry as brr  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

URAL = get_region("sverdlovsk_yanao")


# ── parse_server_options ─────────────────────────────────────────────────────

class TestParseServerOptions:
    def test_selector_with_labeled_links(self):
        html = """
        <div>Выберите подразделение:</div>
        <a href="modules.php?name=sud_delo&srv_num=1">Камышловский районный суд</a>
        <a href="modules.php?name=sud_delo&srv_num=2">Постоянное судебное
           присутствие в г. Новая Ляля</a>
        """
        found = brr.parse_server_options(html)
        assert found == {
            1: "Камышловский районный суд",
            2: "Постоянное судебное присутствие в г. Новая Ляля",
        }

    def test_single_server_sections_collapse_to_one(self):
        """На односерверном сайте srv_num=1 сидит в ссылках разделов —
        словарь честно схлопывается до одной площадки."""
        html = """
        <a href="modules.php?name=sud_delo&srv_num=1&H_date=...">Дела 1 инст.</a>
        <a href="modules.php?name=sud_delo&srv_num=1&delo_id=1540005">Гражданские</a>
        """
        assert set(brr.parse_server_options(html)) == {1}

    def test_fallback_union_when_not_links(self):
        """Селектор, свёрстанный не ссылками, ловится фолбэком (подписи
        пустые, но площадки видны)."""
        html = "<option value='?srv_num=1'>А</option><option value='?srv_num=2'>Б</option>"
        assert set(brr.parse_server_options(html)) == {1, 2}

    def test_empty_page_means_single_default_server(self):
        assert brr.parse_server_options("") == {1: ""}
        assert brr.parse_server_options("<html>нет параметров</html>") == {1: ""}

    def test_first_nonempty_label_wins(self):
        html = (
            '<a href="?srv_num=2"></a>'
            '<a href="?srv_num=2">Присутствие</a>'
            '<a href="?srv_num=2">Дубль-вкладка</a>'
        )
        assert brr.parse_server_options(html)[2] == "Присутствие"


# ── compare_servers: сверка с конфигом региона ───────────────────────────────

def _cfg(lines: list[str]) -> list[str]:
    """Только строки CourtConfig — комментарии «проверить глазами» отдельно."""
    return [ln for ln in lines if "CourtConfig(" in ln]


class TestCompareServers:
    def test_known_two_server_domain_is_complete(self):
        """Камышловский: обе площадки в конфиге — новых нет."""
        verdict, lines = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Суд", 2: "Присутствие"}, URAL)
        assert "НОВАЯ ПЛОЩАДКА" not in verdict
        assert _cfg(lines) == []

    def test_new_server_on_gated_domain_inherits_captcha(self):
        """Новая площадка свердловского суда наследует search_gated=True."""
        verdict, lines = brr.compare_servers(
            "alapaevsky--svd.sudrf.ru", "Алапаевский городской суд",
            {1: "Суд", 2: "ПСП в п. Махнёво"}, URAL)
        assert "⚠ НОВАЯ ПЛОЩАДКА" in verdict and "ПСП в п. Махнёво" in verdict
        cfg = _cfg(lines)
        assert len(cfg) == 1
        assert "search_gated=True" in cfg[0]
        assert "srv_num=2" in cfg[0]
        assert "alapaevsky--svd.sudrf.ru" in cfg[0]
        # Подпись «ПСП…» не опознана как гражданская — строку сопровождает
        # предупреждение (в конфиг вслепую такие не добавляем).
        assert any("подпись не опознана" in ln for ln in lines)

    def test_new_server_on_open_domain_stays_open(self):
        """Площадка суда ЯНАО (поиск открыт) — без search_gated."""
        _, lines = brr.compare_servers(
            "purovsky--ynao.sudrf.ru", "Пуровский районный суд",
            {1: "", 2: "ПСП в с. Красноселькуп"}, URAL)
        cfg = _cfg(lines)
        assert len(cfg) == 1
        assert "search_gated" not in cfg[0]
        assert "srv_num=2" in cfg[0]

    def test_unknown_domain_reports_all_servers_new(self):
        """Домен-кандидат не из конфига: новыми считаются ВСЕ площадки."""
        verdict, lines = brr.compare_servers(
            "newcourt--svd.sudrf.ru", "Новый суд", {1: "Суд"}, URAL)
        assert "в конфиге: —" in verdict
        cfg = _cfg(lines)
        assert len(cfg) == 1 and "srv_num=1" in cfg[0]

    def test_configured_but_missing_from_page_flagged(self):
        """Конфиг знает площадку, страница — нет: чаще это неразобранный
        селектор, чем закрытое присутствие; помечаем, но не удаляем."""
        verdict, lines = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Суд"}, URAL)
        assert "в конфиге, но не на странице: [2]" in verdict
        assert _cfg(lines) == []


# ── Классификация площадок: гражданская / уголовная (14.08.2026) ─────────────
# Первый прогон по Уралу нашёл 4 вторые площадки, и все четыре оказались
# картотеками уголовного судопроизводства. Юрист их отверг — в конфиг
# предлагаем только гражданские.

class TestClassifyServerLabel:
    def test_civil_label(self):
        assert brr.classify_server_label("Гражданское судопроизводство") == brr.SRV_CIVIL
        assert brr.classify_server_label("Гражданская коллегия") == brr.SRV_CIVIL

    def test_criminal_labels(self):
        for label in ("Уголовная коллегия", "Уголовное судопроизводство",
                      "УГОЛОВНОЕ СУДОПРОИЗВОДСТВО"):
            assert brr.classify_server_label(label) == brr.SRV_OTHER, label

    def test_criminal_with_administrative_tail(self):
        """Боевая подпись Железнодорожного р/с ЕКБ (14.08.2026) — самая
        скользкая: содержит «административных», но площадка уголовная."""
        assert brr.classify_server_label(
            "Уголовные дела и дела об административных правонарушениях"
        ) == brr.SRV_OTHER

    def test_administrative_only(self):
        assert brr.classify_server_label(
            "Административное судопроизводство") == brr.SRV_OTHER

    def test_empty_label_is_unknown(self):
        """Односерверный домен отдаёт {1: ""} — это норма, не повод шуметь."""
        assert brr.classify_server_label("") == brr.SRV_UNKNOWN
        assert brr.classify_server_label("   ") == brr.SRV_UNKNOWN

    def test_service_labels_unknown(self):
        for label in ("Судебное делопроизводство", "Президиум", "Суд"):
            assert brr.classify_server_label(label) == brr.SRV_UNKNOWN, label

    def test_mixed_label_is_unknown(self):
        """«Гражданские и административные дела» — ни кандидат, ни повод
        удалять: автоматика ошиблась бы в обе стороны."""
        assert brr.classify_server_label(
            "Гражданские и административные дела") == brr.SRV_UNKNOWN


class TestClassifyServerBySections:
    """Второй слой: разделы страницы самой площадки, когда подпись молчит."""

    def test_civil_section_decides(self):
        assert brr.classify_server(
            "", {1540005: "Гражданские дела", 1540006: "Иное"},
            civil_delo_id=1540005) == brr.SRV_CIVIL

    def test_criminal_sections_only(self):
        assert brr.classify_server(
            "", {1540001: "Уголовные дела первой инстанции"},
            civil_delo_id=1540005) == brr.SRV_OTHER

    def test_empty_sections_stay_unknown(self):
        assert brr.classify_server("", {}, civil_delo_id=1540005) == brr.SRV_UNKNOWN

    def test_label_wins_over_sections(self):
        """Опознанная подпись приоритетнее: лишний запрос её не переигрывает."""
        assert brr.classify_server(
            "Уголовная коллегия", {1540005: "Гражданские дела"},
            civil_delo_id=1540005) == brr.SRV_OTHER


class TestCompareServersLabels:
    def test_labels_printed_for_all_platforms(self):
        """Суть правки: подписи и сконфигурированных площадок тоже видны —
        иначе уголовная картотека в конфиге невидима."""
        verdict, _ = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Гражданское судопроизводство", 2: "Уголовная коллегия"}, URAL)
        assert "подписи:" in verdict
        assert "1: Гражданское судопроизводство [гражданская]" in verdict
        assert "2: Уголовная коллегия [НЕ гражданская]" in verdict

    def test_no_label_block_on_single_server(self):
        """Антишум: 60 односерверных доменов не должны давать строку подписей."""
        verdict, _ = brr.compare_servers(
            "alapaevsky--svd.sudrf.ru", "Алапаевский городской суд",
            {1: ""}, URAL)
        assert "подписи:" not in verdict

    def test_criminal_new_platform_not_offered(self):
        """Кейс Верх-Исетского: новая площадка «Уголовная коллегия» — в конфиг
        не предлагаем, готовой строки нет."""
        verdict, lines = brr.compare_servers(
            "verhisetsky--svd.sudrf.ru", "Верх-Исетский районный суд",
            {1: "Гражданское судопроизводство", 2: "Уголовная коллегия"}, URAL)
        assert "НЕ гражданская, в конфиг не предлагаем" in verdict
        assert _cfg(lines) == []

    def test_civil_new_platform_offered(self):
        verdict, lines = brr.compare_servers(
            "alapaevsky--svd.sudrf.ru", "Алапаевский городской суд",
            {1: "", 2: "Гражданское судопроизводство (ПСП)"}, URAL)
        assert "⚠ НОВАЯ ПЛОЩАДКА 2" in verdict
        cfg = _cfg(lines)
        assert len(cfg) == 1 and "srv_num=2" in cfg[0]
        assert not any("подпись не опознана" in ln for ln in lines)

    def test_configured_criminal_flagged(self):
        """Главная цель правки: сконфигурированная уголовная площадка."""
        verdict, _ = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Гражданское судопроизводство", 2: "Уголовное судопроизводство"},
            URAL)
        assert "⚠ В КОНФИГЕ НЕ ГРАЖДАНСКАЯ ПЛОЩАДКА 2" in verdict
        assert "убрать из региона" in verdict

    def test_configured_civil_not_flagged(self):
        verdict, _ = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Гражданское судопроизводство", 2: "Гражданская коллегия"},
            URAL)
        assert "В КОНФИГЕ НЕ ГРАЖДАНСКАЯ" not in verdict

    def test_railway_court_case(self):
        """Железнодорожный ЕКБ: гражданская картотека на srv 2 (в конфиге),
        уголовная на srv 1 — ни алярма, ни кандидата. Страж от «починки»
        конфига по номеру площадки."""
        verdict, lines = brr.compare_servers(
            "zheleznodorozhny--svd.sudrf.ru",
            "Железнодорожный районный суд г. Екатеринбурга",
            {1: "Уголовные дела и дела об административных правонарушениях",
             2: "Гражданское судопроизводство"}, URAL)
        assert "В КОНФИГЕ НЕ ГРАЖДАНСКАЯ" not in verdict
        assert _cfg(lines) == []
        assert "НЕ гражданская, в конфиг не предлагаем" in verdict

    def test_delo_id_comes_from_region(self):
        """delo_id в готовой строке — из региона, не зашитый в код."""
        _, lines = brr.compare_servers(
            "alapaevsky--svd.sudrf.ru", "Алапаевский городской суд",
            {1: "", 2: "Гражданская коллегия"}, URAL)
        assert str(URAL.fi_default_delo_id) in _cfg(lines)[0]

    def test_sections_used_when_label_silent(self):
        """Подпись молчит — решают разделы площадки (второй слой)."""
        verdict, lines = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Суд", 2: "Присутствие"}, URAL,
            {2: {1540001: "Уголовные дела"}})
        assert "⚠ В КОНФИГЕ НЕ ГРАЖДАНСКАЯ ПЛОЩАДКА 2" in verdict
        assert _cfg(lines) == []


# ── Проводка workflow ────────────────────────────────────────────────────────

def _read_repo(rel: str) -> str:
    with open(os.path.join(ROOT_DIR, rel), encoding="utf-8") as f:
        return f.read()


class TestProbeWorkflowWiring:
    def test_scan_servers_mode_wired(self):
        """Галка scan_servers, REGION из vars и свой файл отчёта: без любого
        из трёх скан на территории не запустить / он пойдёт по ХМАО / отчёт
        затрёт основной."""
        yml = _read_repo(".github/workflows/probe_region_registry.yml")
        assert "scan_servers:" in yml
        assert "REGION: ${{ vars.REGION }}" in yml
        assert "--scan-servers" in yml
        assert "ops/region_probe/servers_report.txt" in yml
        # оба отчёта попадают в коммит
        assert "git add ops/region_probe/report.txt" in yml

    def test_workflow_path_not_in_push_trigger(self):
        """Путь самого workflow не в push.paths: иначе merge правки в форк —
        это push, и проба запускается на территории без спроса, перезаписывая
        её отчёт (класс инцидента 26.07.2026 со сборщиком исков банка).
        Триггер по courts_probe.csv остаётся — он и нужен."""
        yml = _read_repo(".github/workflows/probe_region_registry.yml")
        push_block = yml.split("permissions:")[0].split("push:")[1]
        assert "courts_probe.csv" in push_block
        assert "probe_region_registry.yml" not in push_block
