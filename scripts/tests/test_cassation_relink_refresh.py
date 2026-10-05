"""Поздняя публикация кассационного акта после перехода в awaiting_relink."""
from datetime import date
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest
from court_monitor import config, lifecycle, runs


@pytest.mark.parametrize('stage,expected', [('cassation', 1), ('awaiting_relink', 1), ('first_instance', 0), ('appeal', 0)])
def test_refresh_plan_includes_waiting_remand(monkeypatch, stage, expected):
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    case = {'id': '33-30/2026', 'current_stage': stage, 'cassation': {
        'case_number': '8Г-12188/2026', 'court_domain': '7kas.sudrf.ru',
        'link': '12120551|228a9fc7-0e96-4596-9ae4-99bc86b65684',
        'last_checked_at': '2026-09-09', 'outcome': 'cassation_remanded',
    }}
    # Выполняем реальный фрагмент планирования до HTTP и побочных эффектов.
    source = Path(runs.__file__).read_text()
    start = source.index('        _plan_total = 0', source.index('today_for_refresh ='))
    end = source.index('        cass_refresh_queue = DeferredCardQueue(', start)
    scope = dict(vars(runs))
    scope.update(cases=[case], today_for_refresh=date(2026, 10, 5), today_iso='2026-10-05',
                 cass_refresh_fresh=0, cass_refresh_force_parsed=0,
                 cass_refresh_skipped_future=0, cass_refresh_skipped_suspended=0,
                 telemetry=SimpleNamespace(set_coverage=lambda *a, **k: None))
    exec(dedent(source[start:end]), scope)
    assert scope['cass_refresh_total'] == expected
    assert len(scope['cass_refresh_plan']) == expected
    assert case['current_stage'] == stage


def test_remand_daily_skip_uses_cassation_stamp(monkeypatch):
    monkeypatch.setattr(config, 'SMART_SKIP_CASES', True)
    monkeypatch.setattr(config, 'SKIP_CHECKED_TODAY', True)
    case = {'current_stage': 'awaiting_relink', 'cassation': {'last_checked_at': '2026-10-05'}}
    assert lifecycle.should_skip_case(case, date(2026, 10, 5)) == (True, 'checked_today')
    assert lifecycle.should_skip_case(case, date(2026, 10, 6))[0] is False


def test_mobile_card_with_separate_case_header_and_act_number():
    from court_monitor.parsing.cassation import parse_cassation_card
    html = '''<body><div>ДЕЛО № 8Г-12188/2026 [88-13066/2026]</div>
    <table><tr><th>ДЕЛО</th></tr><tr></tr>
    <tr><td>Уникальный идентификатор дела</td><td>86RS0004-01-2024-013180-35</td></tr>
    <tr><td>Дата рассмотрения</td><td>08.09.2026</td></tr></table>
    <table><tr><th>РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ</th></tr>
    <tr><td>Номер дела в первой инстанции</td><td>2-716/2025</td></tr></table>
    <div id="cont_doc1">№ 88-13066/2026 Мотивированное определение ''' + 'Мотивы суда. ' * 30 + '</div></body>'
    info = parse_cassation_card(html, 'https://7kas.sudrf.ru')
    assert info['act_published'] is True
    assert info['decision_date'] == '08.09.2026'
    assert info['cassation_number'] == '88-13066/2026'
    assert info['judicial_uid'] == '86RS0004-01-2024-013180-35'
