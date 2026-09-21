"""Открытый президиум: автопоиск, дамп, дедуп и честные отказы."""
import ast
from dataclasses import replace
from pathlib import Path

import pytest

from court_monitor import config, linking, presidium_search as ps
from court_monitor.regions import get_region
import import_search_dump as importer

FIXTURES = Path(__file__).parent / "fixtures"
DOMAIN = "vs--bkr.sudrf.ru"


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "REGION", "bashkortostan")
    monkeypatch.setattr(config, "CASSATION_ACTS_PATH", str(tmp_path / "acts"))
    monkeypatch.setattr(ps, "polite_delay", lambda: None)
    search = (FIXTURES / "search_presidium_dump_hmao.html").read_text()
    card = (FIXTURES / "case_card_presidium.html").read_text()
    monkeypatch.setattr(ps, "fetch_page", lambda *a, **kw: search)
    calls = []
    def fetch(url, **kw):
        calls.append(url)
        return card
    monkeypatch.setattr(ps, "fetch_card_checked", fetch)
    coverage = []
    monkeypatch.setattr(ps.telemetry, "set_coverage", lambda *a, **kw: coverage.append((a, kw)))
    return get_region().presidium_courts[0], calls, coverage


def collect(env, cases=(), archives=(), skipped=()):
    health, labels, captcha = {}, {}, {}
    finds = ps.collect_presidium_finds(env[0], cases, archives, set(skipped), health, labels, captcha)
    return finds, health, captcha


def test_new_presidium_discovery_and_idempotent_link(env):
    finds, health, captcha = collect(env)
    assert len(finds) == 3 and len(env[1]) == 3  # строка 2019 года не запрошена
    assert all(url.startswith('https://' + DOMAIN) for url in env[1])
    assert set(health.values()) == {4} and not captcha
    cases, changes, discovered = linking.link_cassation_cases([], finds, [])
    assert discovered and cases[0]['cassation']['court_domain'] == DOMAIN
    assert cases[0]['id'].startswith('4Г-') and cases[0]['first_instance']['magistrate']
    before = len(cases)
    cases, _, discovered = linking.link_cassation_cases(cases, finds, [])
    assert len(cases) == before and not discovered


@pytest.mark.parametrize('flag', ['search_gated', 'search_disabled', 'enabled'])
def test_disabled_search_makes_no_requests(env, monkeypatch, flag):
    court = replace(env[0], **{flag: flag != 'enabled'})
    monkeypatch.setattr(ps, 'fetch_page', lambda *a, **kw: pytest.fail('unexpected request'))
    assert collect((court, *env[1:])) == ([], {}, {})


def test_successful_search_today_skipped_without_health_change(env, monkeypatch):
    monkeypatch.setattr(ps, 'fetch_page', lambda *a, **kw: pytest.fail('unexpected request'))
    assert collect(env, skipped=[f'cassation:presidium:{DOMAIN}:total']) == ([], {}, {})


def test_failed_card_stays_in_denominator(env, monkeypatch):
    monkeypatch.setattr(ps, 'fetch_card_checked', lambda *a, **kw: None)
    stats = {}
    finds = ps.collect_presidium_finds(env[0], [], [], set(), {}, {}, {}, stats)
    assert not finds
    assert stats == {'planned': 3, 'parsed': 0}
    assert env[2][-1][0][1:] == (0, 3)


def test_captcha_is_not_empty_success(env, monkeypatch):
    monkeypatch.setattr(ps, 'fetch_page', lambda *a, **kw: (FIXTURES / 'search_captcha_challenge.html').read_text())
    finds, health, captcha = collect(env)
    assert not finds and not env[1]
    assert set(captcha.values()) == {DOMAIN}


def test_search_network_failure_keeps_retry_eligible(env, monkeypatch):
    monkeypatch.setattr(ps, 'fetch_page', lambda *a, **kw: None)
    monkeypatch.setattr(config, 'FETCH_DIAG', {'kind': 'http_503'})
    assert list(collect(env)[1].values()) == [None]


def test_no_bank_is_read_but_not_imported(env, monkeypatch):
    monkeypatch.setattr(ps, 'parse_cassation_card', lambda *a: {'sber_present': False})
    assert collect(env)[0] == []
    assert env[2][-1][0][1:] == (3, 3)


def test_archived_identity_skips_only_same_court(env):
    archive = {'current_stage': 'archived', 'cassation': {
        'court_domain': DOMAIN, 'case_number': '4Г-66/2026'}}
    assert len(collect(env, archives=[archive])[0]) == 2
    archive['cassation']['court_domain'] = 'oblsud--hmao.sudrf.ru'
    assert len(collect(env, archives=[archive])[0]) == 3


def test_dump_resolves_presidium_and_preserves_appeal(env, monkeypatch):
    assert importer.resolve_court(DOMAIN, 2800001) == env[0]
    assert importer.resolve_court(DOMAIN, 5).court_type == 'appeal'
    monkeypatch.setattr(importer, 'fetch_card_checked', lambda *a, **kw: pytest.fail('dry run'))
    assert importer._before_reform('30.09.2019')
    assert not importer._before_reform('10.09.2026')
    public = get_region().public_info()['presidium_courts'][0]
    assert public['domain'] == DOMAIN and public['search_gated'] is False


def test_search_runs_before_refresh_and_appeal():
    tree = ast.parse((Path(__file__).parents[1] / 'court_monitor/runs.py').read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main_json')
    calls = [n for n in ast.walk(main) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
    discovery = next(n.lineno for n in calls if n.func.id == 'collect_presidium_finds')
    refresh = next(n.lineno for n in calls if n.func.id == 'cassation_court_by_domain')
    assert discovery < refresh
