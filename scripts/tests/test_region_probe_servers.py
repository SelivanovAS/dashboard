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

class TestCompareServers:
    def test_known_two_server_domain_is_complete(self):
        """Камышловский: обе площадки в конфиге — новых нет."""
        verdict, lines = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Суд", 2: "Присутствие"}, URAL)
        assert "НОВАЯ ПЛОЩАДКА" not in verdict
        assert lines == []

    def test_new_server_on_gated_domain_inherits_captcha(self):
        """Новая площадка свердловского суда наследует search_gated=True."""
        verdict, lines = brr.compare_servers(
            "alapaevsky--svd.sudrf.ru", "Алапаевский городской суд",
            {1: "Суд", 2: "ПСП в п. Махнёво"}, URAL)
        assert "⚠ НОВАЯ ПЛОЩАДКА" in verdict and "ПСП в п. Махнёво" in verdict
        assert len(lines) == 1
        assert "search_gated=True" in lines[0]
        assert "srv_num=2" in lines[0]
        assert "alapaevsky--svd.sudrf.ru" in lines[0]

    def test_new_server_on_open_domain_stays_open(self):
        """Площадка суда ЯНАО (поиск открыт) — без search_gated."""
        _, lines = brr.compare_servers(
            "purovsky--ynao.sudrf.ru", "Пуровский районный суд",
            {1: "", 2: "ПСП в с. Красноселькуп"}, URAL)
        assert len(lines) == 1
        assert "search_gated" not in lines[0]
        assert "srv_num=2" in lines[0]

    def test_unknown_domain_reports_all_servers_new(self):
        """Домен-кандидат не из конфига: новыми считаются ВСЕ площадки."""
        verdict, lines = brr.compare_servers(
            "newcourt--svd.sudrf.ru", "Новый суд", {1: "Суд"}, URAL)
        assert "в конфиге: —" in verdict
        assert len(lines) == 1 and "srv_num=1" in lines[0]

    def test_configured_but_missing_from_page_flagged(self):
        """Конфиг знает площадку, страница — нет: чаще это неразобранный
        селектор, чем закрытое присутствие; помечаем, но не удаляем."""
        verdict, lines = brr.compare_servers(
            "kamyshlovsky--svd.sudrf.ru", "Камышловский районный суд",
            {1: "Суд"}, URAL)
        assert "в конфиге, но не на странице: [2]" in verdict
        assert lines == []


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
