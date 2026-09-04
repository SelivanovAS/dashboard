# -*- coding: utf-8 -*-
"""Кассация в президиуме областного суда — дела МИРОВЫХ судей (04.09.2026).

С мая 2026 (ГПК) кассационные жалобы на акты мировых судей рассматривают
президиумы облсудов, а не КСОЮ. Раздел `delo_id=2800001` живёт на домене
апел-суда, поиск за проверочным кодом → новые дела заводит дамп выдачи;
карточки открыты и перечитываются фазой 4d по `cassation.court_domain`.

Фикстуры — реальные страницы Суда ХМАО-Югры (обезличены):
case_card_presidium.html (карточка 4Г-66/2026) и search_presidium_dump_hmao.html
(4 строки выдачи раздела президиума).
"""

from __future__ import annotations

import datetime as dt
import os
import re
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config as cm_config  # noqa: E402
from court_monitor import courts, lifecycle, linking  # noqa: E402
from court_monitor.parsing import cassation as pc  # noqa: E402
from court_monitor.parsing.search import determine_bank_role_from_participants  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

FIXTURES = os.path.join(TESTS_DIR, "fixtures")
PRES_DOMAIN = "oblsud--hmao.sudrf.ru"
PRES_BASE = "https://oblsud--hmao.sudrf.ru"


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _read(rel: str) -> str:
    with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
        return f.read()


@pytest.fixture(autouse=True)
def _acts_tmp(tmp_path, monkeypatch):
    # link_cassation_cases пишет .cassation_acts — не в боевой файл.
    monkeypatch.setattr(cm_config, "CASSATION_ACTS_PATH", str(tmp_path / ".cassation_acts"))


def _card_info(**over) -> dict:
    info = pc.parse_cassation_card(_fixture("case_card_presidium.html"), PRES_BASE)
    assert info is not None
    info["link"] = "26942242|ae405a64-4d0b-4d1c-a637-ac45ee64df8c"
    info["cassation_internal_number"] = info["page_case_number"]
    info.update(over)
    return info


# ── Парсер ───────────────────────────────────────────────────────────────────

class TestPresidiumParser:
    def test_card_header_number_and_magistrate_fallback(self):
        info = _card_info()
        assert info["page_case_number"] == "4Г-66/2026"
        assert info["court_domain"] == PRES_DOMAIN
        assert info["judicial_uid"] == "86MS0072-01-2019-003202-72"
        assert info["fi_case_number"] == "2-1543-2803/2019"
        # Строки «Суд первой инстанции» в карточке нет — суд из скобок судьи.
        assert info["fi_judge"] == "Миненко Ю.В."
        assert info["fi_court_long"] == "Мировой судья (Судебный уч. №3, Ханты-Мансийский р-н)"
        assert info["fi_magistrate"] is True
        assert info["fi_court_config"] is None
        assert info["cassator"] == "ПАО Сбербанк" and info["cassator_status"] == "ВЗЫСКАТЕЛЬ"
        # Приказное производство: банк-взыскатель = истцовая сторона.
        assert info["bank_role"] == "Истец"
        assert info["sber_present"] is True

    def test_search_rows_4g_with_bracket_number_and_magistrate(self):
        rows = pc.parse_cassation_search_page(_fixture("search_presidium_dump_hmao.html"))
        by = {r["cassation_internal_number"]: r for r in rows}
        assert set(by) == {"4Г-66/2026", "4Г-17/2026", "4Г-16/2026", "4Г-2072/2019"}
        r = by["4Г-66/2026"]
        assert r["case_id"] == "26942242" and r["case_uid"].startswith("ae405a64")
        assert r["cassator"] == "ПАО Сбербанк"
        assert r["fi_magistrate"] is True
        assert r["fi_judge"] == "Миненко Ю.В."
        assert r["fi_court_long"].startswith("Мировой судья (")
        assert r["fi_case_number"] == "2-1543-2803/2019"
        # Второй номер в квадратных скобках — номер президиума после передачи.
        assert by["4Г-17/2026"]["cassation_number"] == "44Г-2/2026"
        assert by["4Г-17/2026"]["result_text"] == "СУДЕБНЫЙ ПРИКАЗ ОТМЕНЕН"
        # Строка 2019 года — районный суд 1-й инст., не мировой судья.
        assert by["4Г-2072/2019"]["fi_magistrate"] is False
        assert by["4Г-2072/2019"]["fi_court_long"] == "Нижневартовский городской суд"

    def test_8g_rows_still_parsed(self):
        """Регексп номера обобщён, 7kas не сломан."""
        html = (
            "<table><tr><td>Номер дела</td><td>Дата поступления</td><td>x</td></tr>"
            "<tr><td><a href='/modules.php?name=sud_delo&srv_num=1&name_op=case"
            "&case_id=1&case_uid=aa-bb&delo_id=2800001&new=2800001'>8Г-15253/2026</a></td>"
            "<td>31.08.2026</td><td>КАТЕГОРИЯ: Прочие Жалобу подал(а): Иванов И.И. "
            "Суд (судебный участок) первой инстанции: Сургутский городской суд "
            "Номер дела в первой инстанции: 2-1/2026</td></tr></table>"
        )
        rows = pc.parse_cassation_search_page(html)
        assert [r["cassation_internal_number"] for r in rows] == ["8Г-15253/2026"]
        assert rows[0]["fi_magistrate"] is False

    def test_bank_role_synonyms_off_by_default(self):
        parts = [{"role": "ВЗЫСКАТЕЛЬ", "name": "ПАО Сбербанк", "inn": ""},
                 {"role": "ДОЛЖНИК", "name": "Иванов И.И.", "inn": ""}]
        assert determine_bank_role_from_participants(parts) == "Третье лицо"
        assert determine_bank_role_from_participants(parts, synonyms=True) == "Истец"
        parts2 = [{"role": "ДОЛЖНИК", "name": "ПАО Сбербанк", "inn": ""}]
        assert determine_bank_role_from_participants(parts2, synonyms=True) == "Ответчик"

    def test_presidium_outcomes(self):
        assert pc.classify_cassation_outcome("СУДЕБНЫЙ ПРИКАЗ ОТМЕНЕН") == "cassation_reversed"
        assert pc.classify_cassation_outcome(
            "АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ ОТМЕНЕНО - с направлением на новое рассмотрение"
        ) == "cassation_remanded"
        assert pc.cassation_remanded_to(
            "АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ ОТМЕНЕНО - с направлением на новое рассмотрение"
        ) == "appeal"
        # Прежние правила 7kas не задеты.
        assert pc.classify_cassation_outcome(
            "ОСТАВЛЕНО БЕЗ УДОВЛЕТВОРЕНИЯ", "БЕЗ ИЗМЕНЕНИЯ"
        ) == "cassation_upheld"
        assert pc.classify_cassation_outcome("", "") == ""

    def test_act_number_regex_accepts_presidium(self):
        assert pc._CASS_ACT_DELO_NUM_RE.search("Дело № 44Г-2/2026").group(1) == "44Г-2/2026"
        assert pc._CASS_ACT_DELO_NUM_RE.search("Дело №88-1234/2026").group(1) == "88-1234/2026"


# ── Реестр ───────────────────────────────────────────────────────────────────

class TestPresidiumRegistry:
    def test_presidium_courts_in_public_info(self):
        info = get_region("hmao").public_info()
        assert [c["domain"] for c in info["presidium_courts"]] == [PRES_DOMAIN]
        p = info["presidium_courts"][0]
        assert p["delo_id"] == 2800001 and p["new"] == 2800001
        assert p["search_gated"] is True and p["search_disabled"] is True
        ural = get_region("sverdlovsk_yanao").public_info()
        assert [c["domain"] for c in ural["presidium_courts"]] == [
            "oblsud--svd.sudrf.ru", "oblsud--ynao.sudrf.ru",
        ]
        # КСОЮ остаётся единственной «cassation» региона.
        assert info["cassation"]["domain"] == "7kas.sudrf.ru"

    def test_presidium_not_in_appeal_courts(self):
        """_appeal_health_key и appeal_court_by_domain считают апелляции по
        кортежу appeal_courts — президиум там сломал бы ключ здоровья ХМАО."""
        for code in ("hmao", "sverdlovsk_yanao"):
            r = get_region(code)
            assert all(c.court_type == "appeal" for c in r.appeal_courts)
            assert all(c.court_type == "cassation" for c in r.presidium_courts)

    def test_cassation_court_by_domain(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        assert courts.cassation_court_by_domain(PRES_DOMAIN).name == "Президиум Суда ХМАО-Югры"
        assert courts.cassation_court_by_domain("oblsud.hmao.sudrf.ru").delo_id == 2800001
        assert courts.cassation_court_by_domain("").domain == "7kas.sudrf.ru"
        assert courts.cassation_court_by_domain(None).domain == "7kas.sudrf.ru"
        assert courts.cassation_court_by_domain("7kas.sudrf.ru").domain == "7kas.sudrf.ru"
        assert courts.presidium_court_by_domain("7kas.sudrf.ru") is None

    def test_cassation_card_url_per_court(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        url = courts.cassation_card_url({"link": "1|a-b", "court_domain": PRES_DOMAIN})
        assert url.startswith("https://oblsud--hmao.sudrf.ru/") and "delo_id=2800001" in url
        assert "new=2800001" in url
        assert courts.cassation_card_url({"link": "1|a-b"}).startswith("https://7kas.sudrf.ru/")
        assert courts.cassation_card_url({"link": "", "court_domain": PRES_DOMAIN}) == ""


# ── Связка ───────────────────────────────────────────────────────────────────

class TestPresidiumLinking:
    def test_discovery_builds_presidium_case(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        cases, changes, discovered = linking.link_cassation_cases([], [_card_info()], [])
        assert len(discovered) == 1
        c = discovered[0]
        assert c["id"] == "4Г-66/2026"  # главный номер — президиума
        assert c["current_stage"] == "cassation"
        assert c["discovered_via_cassation"] is True
        assert "президиума" in c["notes"]
        cs = c["cassation"]
        assert cs["case_number"] == "4Г-66/2026"
        assert cs["court"] == "Президиум Суда ХМАО-Югры"
        assert cs["court_domain"] == PRES_DOMAIN
        assert cs["delo_id"] == 2800001
        assert cs["appellant"] == "ПАО Сбербанк" and cs["appellant_is_bank"] is True
        fi = c["first_instance"]
        assert fi["magistrate"] is True
        assert fi["case_number"] == "2-1543-2803/2019"
        assert fi["court"].startswith("Мировой судья (") and fi["court_domain"] == ""
        assert fi["judge"] == "Миненко Ю.В."
        assert c["plaintiff"] == "ПАО Сбербанк" and c["defendant"]
        assert c["bank_role"] == "Истец"
        ch = changes[0]
        assert ch["type"] == ["discovered_in_cassation"]
        assert ch["case"] == "4Г-66/2026"
        assert ch["details"]["court_domain"] == PRES_DOMAIN

    def test_presidium_find_ignores_district_case_with_same_fi_number(self, monkeypatch):
        """FI-номер мирового судьи ≠ номер районного дела: матча нет."""
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        district = {
            "id": "2-1543-2803/2019", "current_stage": "cassation_watch",
            "first_instance": {"case_number": "2-1543-2803/2019",
                               "court": "Сургутский городской суд",
                               "court_domain": "surggor--hmao.sudrf.ru",
                               "judicial_uid": "86RS0004-01-2019-000001-11"},
            "appeal": None, "cassation": None,
        }
        cases, _, discovered = linking.link_cassation_cases([district], [_card_info()], [])
        assert len(cases) == 2 and len(discovered) == 1
        assert cases[0]["cassation"] is None  # районное дело не тронуто

    def test_reimport_same_card_merges_not_duplicates(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        cases, _, disc1 = linking.link_cassation_cases([], [_card_info()], [])
        cases, changes, disc2 = linking.link_cassation_cases(cases, [_card_info()], [])
        assert len(cases) == 1 and len(disc1) == 1 and disc2 == []
        assert cases[0]["cassation"]["court_domain"] == PRES_DOMAIN

    def test_duplicate_rows_in_one_call_collapse(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        cases, _, disc = linking.link_cassation_cases([], [_card_info(), _card_info()], [])
        assert len(cases) == 1 and len(disc) == 1

    def test_same_4g_number_in_two_presidiums_not_merged(self, monkeypatch):
        """На Урале два президиума: ключ связки и дедупа — пара (домен, номер)."""
        monkeypatch.setattr(cm_config, "REGION", "sverdlovsk_yanao")
        a = _card_info(court_domain="oblsud--svd.sudrf.ru", judicial_uid="66MS0001-01-2026-000001-01")
        b = _card_info(court_domain="oblsud--ynao.sudrf.ru", judicial_uid="89MS0001-01-2026-000002-02")
        cases, _, disc = linking.link_cassation_cases([], [a, b], [])
        assert len(disc) == 2
        assert {c["cassation"]["court"] for c in cases} == {
            "Президиум Свердловского областного суда",
            "Президиум Суда Ямало-Ненецкого автономного округа",
        }
        assert lifecycle.dedupe_cassation_by_internal_number(cases) == 0
        assert len(cases) == 2

    def test_dedup_indexes_skip_magistrate_numbers(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        cases, _, _ = linking.link_cassation_cases([], [_card_info()], [])
        ids = linking.collect_existing_ids(cases)
        assert "4Г-66/2026" in ids and "2-1543-2803/2019" not in ids
        exact, wildcard = linking.collect_fi_dedup_index(cases)
        assert "2-1543-2803/2019" not in wildcard
        assert not any(n == "2-1543-2803/2019" for _d, n in exact)

    def test_refresh_find_keeps_presidium_court(self, monkeypatch):
        """Перечитка 4d: info с доменом президиума не «откатывает» блок на 7kas."""
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        cases, _, _ = linking.link_cassation_cases([], [_card_info()], [])
        refreshed = _card_info(result_text="СУДЕБНЫЙ ПРИКАЗ ОТМЕНЕН", decision_date="25.09.2026")
        cases, changes, disc = linking.link_cassation_cases(cases, [refreshed])
        assert disc == [] and len(cases) == 1
        cs = cases[0]["cassation"]
        assert cs["court_domain"] == PRES_DOMAIN and cs["court"].startswith("Президиум")
        assert cs["outcome"] == "cassation_reversed"
        assert any("outcome_change" in ch["type"] for ch in changes)

    def test_block_without_domain_stays_7kas(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        info = _card_info(court_domain="", fi_magistrate=False)
        block = linking._cassation_card_to_block(info)
        assert block["court_domain"] == "7kas.sudrf.ru"
        assert block["delo_id"] == 2800001


# ── Стадии ───────────────────────────────────────────────────────────────────

def _cass_case(domain: str, outcome: str, decision_days_ago: int = 60) -> dict:
    dec = (dt.date.today() - dt.timedelta(days=decision_days_ago)).strftime("%d.%m.%Y")
    return {
        "id": "4Г-1/2026", "current_stage": "cassation",
        "first_instance": {"case_number": "2-1/2026", "magistrate": True, "events": []},
        "appeal": None,
        "cassation": {"case_number": "4Г-1/2026", "court_domain": domain,
                      "outcome": outcome, "decision_date": dec,
                      "act_published": False, "events": []},
    }


class TestPresidiumStages:
    def test_presidium_remanded_stays_in_cassation_and_archives(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        c = _cass_case(PRES_DOMAIN, "cassation_remanded")
        assert lifecycle.advance_case_stage(c) is None
        assert c["current_stage"] == "cassation"
        # 60 дней без акта > CASSATION_NO_ACT_PUBLISH_DAYS → архив по общим окнам.
        assert lifecycle.is_case_archived(c) is True
        fresh = _cass_case(PRES_DOMAIN, "cassation_remanded", decision_days_ago=3)
        assert lifecycle.is_case_archived(fresh) is False

    def test_7kas_remanded_still_awaiting_relink(self, monkeypatch):
        monkeypatch.setattr(cm_config, "REGION", "hmao")
        c = _cass_case("7kas.sudrf.ru", "cassation_remanded")
        assert lifecycle.is_case_archived(c) is False
        assert lifecycle.advance_case_stage(c) == "cassation"
        assert c["current_stage"] == "awaiting_relink"


# ── Проводка ─────────────────────────────────────────────────────────────────

class TestPresidiumWiring:
    def test_refresh_phase_resolves_court_per_case(self):
        src = _read("scripts/court_monitor/runs.py")
        a = src.index("# ── 4d. Refresh кассации по cassation.link ──")
        b = src.index("# Резервный щит после обоих link_cassation_cases")
        seg = src[a:b]
        assert "cassation_court_by_domain(cass.get(\"court_domain\"))" in seg
        code_lines = [l for l in seg.splitlines() if "CASSATION_COURT" in l
                      and not l.strip().startswith("#")]
        assert not code_lines, code_lines
        assert "court.card_url(cid, cuid)" in seg
        assert "parse_cassation_card(card_html, court.base_url)" in seg

    def test_digest_cassation_urls_per_court(self):
        for rel in ("scripts/court_monitor/digest/template.py",
                    "scripts/court_monitor/digest/core.py"):
            src = _read(rel)
            assert "CASSATION_COURT.card_url(" not in src, rel
            assert "cassation_card_url(" in src, rel

    def test_announce_wiring(self):
        src = _read("scripts/court_monitor/runs.py")
        assert "def announce_imported_presidium_cases(" in src
        i = src.index("def announce_imported_cases(")
        body = src[i:src.index("def announce_imported_presidium_cases(")]
        assert 'imp.get("source") == "dump_presidium"' in body, (
            "стаб мирового судьи уехал бы в «📥 Новые иски»")
        i_call = src.index("presidium_imported_new = announce_imported_presidium_cases(cases)")
        assert "cass_discovered = list(cass_discovered) + presidium_imported_new" in src[i_call:i_call + 800]

    def test_frontend_cassation_link_honours_block_domain(self):
        src = _read("app.js")
        i = src.index("if(isCass){")
        block = src[i:i + 700]
        assert "cs.court_domain" in block and "cs.delo_id" in block
        assert "buildCourtLink(cs.link," in block

    def test_frontend_favor_uses_cassator(self):
        src = _read("app.js")
        i = src.index("function getResultFavor(c){")
        body = src[i:i + 2500]
        assert "cassAppellantIsBank" in body and "resultSource==='cassation'" in body

    def test_admin_presidium_pinned_and_keyed(self):
        src = _read("cloudflare-worker/admin_page.js")
        body = src.split("async function loadImportCourts", 1)[1][:5000]
        assert "acRegion.presidium_courts" in body
        assert 'section: "cassation"' in body and "pinned: true" in body
        assert "gatedAppeal.concat(gatedPresidium).concat(gated)" in body
        m = re.search(r"function impCourtKey\(c\)[^\n]*", src)
        assert m and 'domain + "|"' in m.group(0) and '"|cassation"' in m.group(0)
        assert "function impDetectDeloIds(html)" in src
        assert "impDetectedDeloIds.length === 1" in src
        assert "— президиум (кассация)" in src
        # Президиум в проверке ссылок точечного добавления — ДО апелляции
        # (хост общий, различает delo_id).
        i = src.index("function acCheckLink(url)")
        b = src[i:i + 2500]
        assert b.index("presidium_courts") < b.index("appeal_courts")
        assert "delo_id=2800001" in b

    def test_section_reaches_operator(self):
        """Правило трёх звеньев: jq → whitelist Worker'а → админка."""
        jq = _read("ops/import_result_body.jq")
        worker = _read("cloudflare-worker/worker.js")
        admin = _read("cloudflare-worker/admin_page.js")
        assert re.search(r"\bsection\s*:\s*\(\s*\.section\s*//\s*\"\"\s*\)", jq)
        assert re.search(r"\bskipped_old\s*:\s*\(\s*\.skipped_old\s*//\s*0\s*\)", jq)
        i = worker.index("async function handleImportResult")
        w = worker[i:i + 6000]
        assert '"skipped_old"' in w
        assert 'record.section = body.section' in w
        assert "item.skipped_old" in admin and "item.section" in admin
        assert "function impIsPresidium(item)" in admin
