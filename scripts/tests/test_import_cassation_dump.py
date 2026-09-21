"""КСОЮ с закрытым поиском: приём, территория, два трека и безопасная связка.

Карточная фикстура — структура реального небанковского дела 6kas (11.09.2026).
Банковские стороны и поисковые строки ниже синтетические, сеть не вызывается.
"""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import import_search_dump as isd
from court_monitor import config, linking, runs
from court_monitor.parsing import parse_cassation_card
from court_monitor.regions import get_region
from court_monitor.storage import load_bank_json, save_bank_json
from court_monitor.targeted_add import resolve_link_target

HTML = (Path(__file__).parent / 'fixtures/case_card_6kas.html').read_text(encoding='utf-8')
LONG = 'Ленинский районный суд г. Уфы Республики Башкортостан'
DOMAIN = 'leninsky--bkr.sudrf.ru'
OTHER = 'demsky--bkr.sudrf.ru'
NUMBER = '13-2317/2025'


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'REGION', 'bashkortostan')
    monkeypatch.setattr(config, 'BANK_TRACK', True)
    for name in ['JSON_PATH', 'JSON_ARCHIVE_PATH', 'JSON_BANK_PATH', 'JSON_BANK_EVENTS_PATH',
                 'JSON_BANK_ARCHIVE_PATH', 'JSON_BANK_ARCHIVE_EVENTS_PATH', 'CASSATION_ACTS_PATH',
                 'CSV_PATH', 'CSV_ARCHIVE_PATH', 'LAST_DIGEST_CONTEXT_PATH']:
        monkeypatch.setattr(config, name, str(tmp_path / name))
    for name in ['GITHUB_OUTPUT', 'IMPORT_SUMMARY_PATH']:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(isd, 'polite_delay', lambda: None)
    monkeypatch.setattr(config, 'PARSE_TXN_ID', '')
    monkeypatch.setattr(config, 'PARSE_TXN_ACK_FILE', '')
    monkeypatch.setattr(isd, 'fetch_card_checked', lambda *a, **k: pytest.fail('unexpected network'))
    Path(config.JSON_PATH).write_text(json.dumps({'version': 1, 'cases': []}))
    Path(config.JSON_ARCHIVE_PATH).write_text(json.dumps({'version': 1, 'cases': []}))
    return tmp_path


def info(**changes):
    out = parse_cassation_card(HTML, 'https://6kas.sudrf.ru')
    out.update(sber_present=True, bank_role='Истец', judicial_uid='',
               participants=[{'role': 'ИСТЕЦ', 'name': 'ПАО Сбербанк'},
                             {'role': 'ОТВЕТЧИК', 'name': 'Тестовый Ответчик'}],
               cassation_internal_number='8Г-7652/2026', link='24413318|abc-def')
    out.update(changes)
    return out


def row(**changes):
    out = {'cassation_internal_number': '8Г-7652/2026', 'case_id': '24413318',
           'case_uid': 'abc-def', 'filing_date': '13.04.2026',
           'fi_case_number': NUMBER, 'fi_court_long': LONG}
    out.update(changes)
    return out


def case(domain=DOMAIN, **changes):
    out = {'id': NUMBER, 'current_stage': 'first_instance',
           'first_instance': {'court_domain': domain, 'case_number': NUMBER,
                              'events': [{'name': 'Старое событие', 'date': '01.01.2026'}]},
           'plaintiff': 'ПАО Сбербанк', 'defendant': 'Тестовый Ответчик', 'bank_role': 'Истец'}
    out.update(changes)
    return out


def import_rows(monkeypatch, rows=None, card=None):
    monkeypatch.setattr(isd, '_fetch_cassation_card', lambda *a, **k: (deepcopy(card or info()), ''))
    return isd.import_cassation_rows(get_region().cassation_court, rows or [row()], 'Тест', False)['counters']


def main_cases():
    return json.loads(Path(config.JSON_PATH).read_text())['cases']


def test_real_6kas_structure_and_region(env):
    parsed = parse_cassation_card(HTML, 'https://6kas.sudrf.ru')
    assert parsed['court_domain'] == '6kas.sudrf.ru'
    assert parsed['page_case_number'] == '8Г-7652/2026'
    assert parsed['fi_region_code'] == '03'
    assert parsed['fi_court_config'].domain == DOMAIN
    assert parsed['hearing_time'] == '09:00'
    assert parsed['act_published'] and parsed['act_text']
    assert not parsed['sber_present']  # этот реальный образец не банковский


def test_resolve_ksou_and_pre_may_case_import(env, monkeypatch):
    assert isd.resolve_court('6kas.sudrf.ru', 2800001) == get_region().cassation_court
    counts = import_rows(monkeypatch)
    assert counts['added'] == 1 and counts['skipped_old'] == 0
    c = main_cases()[0]
    assert c['import']['source'] == 'dump_cassation'
    assert c['cassation']['timezone'] == 'Europe/Samara'
    assert c['cassation']['new'] == 2800001 and c['cassation']['srv_num'] == 1
    assert c['bank_role'] == 'Истец'
    # Повтор не читает карточку и не создаёт второй анонс.
    monkeypatch.setattr(isd, '_fetch_cassation_card', lambda *a, **k: pytest.fail('duplicate fetch'))
    counts = isd.import_cassation_rows(get_region().cassation_court, [row()], 'Тест', False)['counters']
    assert counts['already'] == 1 and len(main_cases()) == 1
    assert not runs.announce_imported_cases([c])
    assert runs.announce_imported_presidium_cases([c]) == [c]
    assert runs.announce_imported_presidium_cases([c]) == []


def test_region_filter_before_fetch_and_card_cross_check(env, monkeypatch):
    monkeypatch.setattr(isd, '_fetch_cassation_card', lambda *a, **k: pytest.fail('foreign fetch'))
    counts = isd.import_cassation_rows(get_region().cassation_court,
        [row(fi_court_long='Ленинский районный суд г. Самары Самарской области')], 'Тест', False)['counters']
    assert counts['skipped_region'] == 1 and not main_cases()
    counts = import_rows(monkeypatch, card=info(fi_court_long='Демский районный суд Самарской области'))
    assert counts['skipped_region'] == 1 and not main_cases()


def test_unavailable_card_not_stamped_and_retried(env, monkeypatch):
    monkeypatch.setattr(isd, '_fetch_cassation_card', lambda *a, **k: (None, 'failed'))
    for _ in range(2):
        counts = isd.import_cassation_rows(get_region().cassation_court, [row()], 'Тест', False)['counters']
        assert counts['fetch_fail'] == 1 and not main_cases()
    assert import_rows(monkeypatch)['added'] == 1


@pytest.mark.parametrize('archive', [False, True])
def test_bank_case_moves_without_losing_events_or_siblings(env, monkeypatch, archive):
    target = case(track='plaintiff_light')
    sibling = case(domain=OTHER, track='plaintiff_light')
    list_path = config.JSON_BANK_ARCHIVE_PATH if archive else config.JSON_BANK_PATH
    events_path = config.JSON_BANK_ARCHIVE_EVENTS_PATH if archive else config.JSON_BANK_EVENTS_PATH
    save_bank_json({'version': 1, 'custom': 'preserve', 'cases': [target, sibling]}, list_path, events_path)
    counts = import_rows(monkeypatch)
    assert counts['linked'] == 1 and counts['added'] == 0
    c = main_cases()[0]
    assert c['first_instance']['court_domain'] == DOMAIN
    assert c['first_instance']['events'] == target['first_instance']['events']
    assert c['track_origin'] == 'plaintiff_light' and 'track' not in c
    remaining = load_bank_json(list_path, events_path)
    assert remaining['custom'] == 'preserve'
    assert len(remaining['cases']) == 1
    assert remaining['cases'][0]['first_instance']['court_domain'] == OTHER
    assert remaining['cases'][0]['first_instance']['events'] == sibling['first_instance']['events']
    assert json.loads(Path(config.JSON_ARCHIVE_PATH).read_text())['cases'] == []


@pytest.mark.parametrize('archive', [False, True])
def test_same_number_different_courts_links_only_own(env, archive):
    own, other = case(), case(OTHER)
    records = [other, own]
    active, archived = ([], records) if archive else (records, [])
    out, changes, discovered = linking.link_cassation_cases(active, [info()], archived)
    assert not discovered and len(changes) == 1
    assert own['cassation']['court_domain'] == '6kas.sudrf.ru'
    assert 'cassation' not in other
    if archive:
        assert archived == [other] and out == [own]


@pytest.mark.parametrize('records', [[case(), case()], [case(domain='')]])
def test_ambiguous_or_missing_court_requires_review(env, records):
    find = info()
    original = deepcopy(records)
    out, changes, discovered = linking.link_cassation_cases(deepcopy(records), [find], [])
    assert out == original and not changes and not discovered
    assert find['_link_status'] == 'needs_review'


def test_import_reports_unresolved_instead_of_false_link(env, monkeypatch):
    Path(config.JSON_PATH).write_text(json.dumps({'cases': [case(), case()]}))
    counts = import_rows(monkeypatch)
    assert counts['needs_review'] == 1 and counts['linked'] == counts['added'] == 0


@pytest.mark.parametrize('flag', ['search_gated', 'search_disabled'])
def test_gated_search_makes_no_http_but_court_stays_enabled(env, monkeypatch, flag):
    court = deepcopy(get_region().cassation_court)
    court.search_gated = court.search_disabled = False
    setattr(court, flag, True)
    monkeypatch.setattr(runs, 'fetch_page', lambda *a, **k: pytest.fail('gated search called'))
    monkeypatch.setattr(runs, 'polite_delay', lambda: pytest.fail('gated search delayed'))
    assert runs.fetch_cassation_search(court, 'cassation:6kas:total', set()) == ('', '', True)
    assert court.enabled and '6kas.sudrf.ru' in court.card_url('24413318', 'abc-def')


def test_open_search_works_and_successful_today_is_skipped(env, monkeypatch):
    court = deepcopy(get_region().cassation_court)
    court.search_gated = court.search_disabled = False
    calls = []
    monkeypatch.setattr(runs, 'polite_delay', lambda: None)
    monkeypatch.setattr(runs, 'fetch_page', lambda url, **k: calls.append(url) or '<html>result</html>')
    assert runs.fetch_cassation_search(court, 'key', set()) == ('<html>result</html>', court.search_url(), False)
    assert runs.fetch_cassation_search(court, 'key', {'key'}) == ('', '', True)
    assert len(calls) == 1


def test_targeted_cassation_redirects_to_working_dump_channel(env):
    court, reason = resolve_link_target({'domain': '6kas.sudrf.ru'})
    assert court is None and 'выдачу' in reason and '«Импорт»' in reason
    assert 'по делу 1-й инстанции' not in reason


def test_main_dispatch_real_card_structure_and_repeat(env, monkeypatch):
    """Синтетическая строка выдачи → настоящий парсер карточки → сохранение.
    В структуре реальной карточки заменено одно имя участника на банк."""
    before, mark, participants = HTML.partition('УЧАСТНИКИ')
    bank_html = before + mark + participants.replace('Информация скрыта', 'ПАО Сбербанк', 1)
    assert parse_cassation_card(bank_html, 'https://6kas.sudrf.ru')['sber_present']
    calls = []
    monkeypatch.setattr(isd, 'fetch_card_checked', lambda url, **k: calls.append(url) or bank_html)
    summaries = []
    monkeypatch.setattr(isd, 'write_github_output', lambda summary: summaries.append(deepcopy(summary)))
    dump = env / 'dump.html'
    dump.write_text(f'''<html><body><table><tr><th>№ дела</th><th>Дата поступления</th><th>Категория</th></tr>
    <tr><td><a href="https://6kas.sudrf.ru/modules.php?name=sud_delo&amp;srv_num=1&amp;name_op=case&amp;case_id=24413318&amp;case_uid=abc-def&amp;new=2800001&amp;delo_id=2800001">8Г-7652/2026</a></td>
    <td>13.04.2026</td><td>Жалобу подал(а): ПАО Сбербанк<br>Суд (судебный участок) первой инстанции: {LONG}<br>Номер дела в первой инстанции: {NUMBER}</td></tr></table></body></html>''')
    args = [str(dump), '--court-domain', '6kas.sudrf.ru', '--operator', 'Тест']
    assert isd.main(args) == isd.EXIT_OK
    assert summaries[-1]['cassation_kind'] == 'court'
    assert summaries[-1]['section'] == 'cassation'
    assert summaries[-1]['added'] == 1
    assert isd.main(args) == isd.EXIT_OK
    assert summaries[-1]['already'] == 1 and len(calls) == 1


def test_number_ambiguity_across_active_and_archive(env):
    active, archived = [case()], [case()]
    out, changes, discovered = linking.link_cassation_cases(active, [info()], archived)
    assert len(out) == len(archived) == 1 and not changes and not discovered
    assert all('cassation' not in c for c in out + archived)


def test_archive_uid_has_priority_over_active_number_match(env):
    active_case, archived_case = case(), case()
    archived_case['first_instance']['judicial_uid'] = '03RS0004-01-2025-000820-18'
    archive = [archived_case]
    out, changes, discovered = linking.link_cassation_cases(
        [active_case], [info(judicial_uid='03RS0004-01-2025-000820-18')], archive,
    )
    assert archive == [] and len(out) == 2 and not discovered
    assert 'cassation' not in active_case
    assert archived_case['cassation']['court_domain'] == '6kas.sudrf.ru'
    assert changes[0]['details']['timezone'] == 'Europe/Samara'


def test_malformed_cassation_dump_is_not_successful_empty_import(env, monkeypatch):
    summaries = []
    monkeypatch.setattr(isd, 'write_github_output', lambda summary: summaries.append(deepcopy(summary)))
    dump = env / 'broken.html'
    dump.write_text('<table><tr><th>№ дела</th><th>Дата поступления</th><th>Категория</th></tr><tr><td>8Г-123/2026</td><td>01.09.2026</td><td>Сбербанк</td></tr></table>')
    assert isd.main([str(dump), '--court-domain', '6kas.sudrf.ru']) == isd.EXIT_NO_TABLE
    assert 'ссылки' in summaries[-1]['error'] and not main_cases()


@pytest.mark.parametrize('source', ['main', 'archive', 'bank', 'bank_archive'])
def test_linked_import_survives_refresh_and_enters_replay_once(env, monkeypatch, source):
    target = case()
    path = {'main': config.JSON_PATH, 'archive': config.JSON_ARCHIVE_PATH,
            'bank': config.JSON_BANK_PATH, 'bank_archive': config.JSON_BANK_ARCHIVE_PATH}[source]
    if source.startswith('bank'):
        target['track'] = 'plaintiff_light'
        events_path = config.JSON_BANK_EVENTS_PATH if source == 'bank' else config.JSON_BANK_ARCHIVE_EVENTS_PATH
        save_bank_json({'cases': [target]}, path, events_path)
    else:
        Path(path).write_text(json.dumps({'cases': [target]}))
    card = info(outcome='', review_result='', result_text='', result_for_appeal='', act_published=False,
                act_text='', hearing_date='25.12.2026', hearing_time='09:30')
    assert import_rows(monkeypatch, card=card)['linked'] == 1
    data = json.loads(Path(config.JSON_PATH).read_text())
    assert len(data['pending_cassation_changes']) == 1
    assert 'new_cassation' in data['pending_cassation_changes'][0]['type']
    # Тот же дамп не дублирует событие, неизменная карточка уже не даёт дельту.
    assert import_rows(monkeypatch, card=card)['already'] == 1
    data = json.loads(Path(config.JSON_PATH).read_text())
    cases, fresh, discovered = linking.link_cassation_cases(data['cases'], [deepcopy(card)], [])
    assert not fresh and not discovered
    assert not runs.announce_imported_presidium_cases(cases)
    changes = runs.merge_imported_cassation_changes(data, fresh)
    assert len(changes) == 1 and changes[0]['details']['hearing_time'] == '09:30'
    assert changes[0]['details']['timezone'] == 'Europe/Samara'
    # Контекст — тот же файл, который читает replay; транспорт не вызывается.
    issue_key = runs.save_digest_context([], [], cass_changes=changes)
    runs.acknowledge_imported_cassation_changes(data, changes, issue_key)
    stored = json.loads(Path(config.JSON_PATH).read_text())
    assert 'pending_cassation_changes' not in stored
    assert runs.merge_imported_cassation_changes(stored, []) == []
    runs.save_digest_context([], [], cass_changes=[])
    ctx = json.loads(Path(config.LAST_DIGEST_CONTEXT_PATH).read_text())
    assert ctx['cass_changes'] == changes
    from court_monitor.digest.template import generate_template_digest
    digest = generate_template_digest([], [], cass_changes=ctx['cass_changes'])
    assert '25.12.2026' in digest and '09:30' in digest and 'Самара, UTC+4' in digest


def test_pending_cassation_keeps_latest_hearing_and_court_identity(env):
    old = {'case': NUMBER, 'cassation_internal_number': '8Г-1/2026', 'type': ['new_cassation'],
           'details': {'court_domain': '6kas.sudrf.ru', 'hearing_date': '25.12.2026', 'hearing_time': '09:30'}}
    newer = deepcopy(old)
    newer['type'] = ['cass_hearing_scheduled']
    newer['details'].update(hearing_date='28.12.2026', hearing_time='10:00')
    other = deepcopy(old)
    other['details']['court_domain'] = 'oblsud--hmao.sudrf.ru'
    merged = runs.merge_imported_cassation_changes({'pending_cassation_changes': [old]}, [newer, other])
    assert len(merged) == 2
    assert merged[0]['type'] == ['new_cassation', 'cass_hearing_scheduled']
    assert merged[0]['details']['hearing_date'] == '28.12.2026'
    assert old['details']['hearing_date'] == '25.12.2026'  # очередь до ACK не мутирует


def test_pending_cassation_not_cleared_without_durable_context(env, monkeypatch):
    Path(config.JSON_PATH).write_text(json.dumps({'cases': [case()]}))
    import_rows(monkeypatch)
    data = json.loads(Path(config.JSON_PATH).read_text())
    changes = runs.merge_imported_cassation_changes(data, [])
    with pytest.raises(RuntimeError, match='не сохранены'):
        runs.acknowledge_imported_cassation_changes(data, changes, 'missing-context')
    assert data['pending_cassation_changes']
    assert json.loads(Path(config.JSON_PATH).read_text())['pending_cassation_changes']


def test_run_transfers_import_queue_before_digest_and_acks_after_context():
    import inspect
    source = inspect.getsource(runs.main_json)
    merge_at = source.index('cass_changes = merge_imported_cassation_changes(data, cass_changes)')
    save_at = source.index('digest_issue_key = save_digest_context(')
    ack_at = source.index('acknowledge_imported_cassation_changes(data, cass_changes, digest_issue_key)')
    render_at = source.index('digest = generate_digest(', save_at)
    assert merge_at < save_at < ack_at < render_at
