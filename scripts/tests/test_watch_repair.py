"""Восстановление по доказательствам, без новых событий и потери кругов."""
from copy import deepcopy
from datetime import date
import json
import socket

import pytest
from court_monitor import act_anchor, fi_act_watch, writ_watch, card_refresh, config
from court_monitor.watch_repair import reconcile
from reconcile_watch_queues import run, load_documents

DAY = date(2026, 11, 11)
DOMAIN = 'surggor--hmao.sudrf.ru'
RESULT = 'Возвращение иска (заявления, жалобы) заявления'


def case():
    return {'id': '9-12/2026', 'bank_role': 'Ответчик', 'current_stage': 'first_instance',
            'first_instance': {'case_number': '9-12/2026', 'court_domain': DOMAIN,
                'srv_num': 1, 'delo_id': 1540005, 'link': '123|card-uid', 'judicial_uid': '86-uid',
                'status': 'Решено', 'result': RESULT, 'decision_date': '', 'act_date': '06.10.2026',
                'last_checked_at': '2026-10-06', 'events': [
                    {'date': '24.09.2026', 'name': 'Решение вопроса о принятии иска к рассмотрению',
                     'result_event': RESULT}]}}


def queue(c, **extra):
    block = deepcopy(c['first_instance'])
    key = fi_act_watch.identity(block).rsplit('|', 1)[0] + '|undated'
    task = {'status': 'needs_review', 'reason': 'missing_decision_date',
            'created_at': '2026-10-07', 'block': block, **extra}
    return {fi_act_watch.FIELD: {key: task}}, key


def test_event_date_is_distinct_from_merits_and_publication_dates():
    c = case(); block = c['first_instance']; before = deepcopy(c)
    assert act_anchor.evidence(block)['kind'] == 'return'
    assert fi_act_watch.anchor_date(block) == '24.09.2026'
    assert not fi_act_watch.decision_date(block)
    data, old_key = queue(c)
    actions = reconcile(data, [c], DAY)
    task = data[fi_act_watch.FIELD][actions[0]['key']]
    assert task['status'] == 'waiting' and old_key not in data[fi_act_watch.FIELD]
    assert task['block']['act_date'] == '06.10.2026'
    assert c == before and not data.get(fi_act_watch.PENDING)
    first = deepcopy(data)
    assert reconcile(data, [c], DAY) == [] and data == first
    cards, _ = card_refresh.plan(data, [c], DAY)
    assert len(cards) == 1 and task['status'] == 'waiting'
    assert not block['decision_date'] and not data.get(fi_act_watch.PENDING)


@pytest.mark.parametrize('change', ['posting_only', 'conflicting_result', 'reopened', 'same_day_conflict', 'transport'])
def test_ambiguous_events_do_not_supply_dates(change):
    block = case()['first_instance']
    if change == 'posting_only':
        block['events'] = []
    elif change == 'conflicting_result':
        block['result'] = 'Отказано в принятии заявления'
    elif change == 'reopened':
        block['events'].append({'date': '25.09.2026', 'name': 'Производство возобновлено'})
    elif change == 'same_day_conflict':
        block['events'].append({'date': '24.09.2026', 'name': 'Судебное заседание',
                                'result_event': 'Отказано в принятии заявления'})
    else:
        block['events'][0]['name'] = 'Выдача копии определения'
    assert not act_anchor.evidence(block)


@pytest.mark.parametrize('field,value', [('judicial_uid', 'other'), ('link', 'other|uid'),
    ('court_domain', 'hmray--hmao.sudrf.ru'), ('srv_num', 2), ('delo_id', 9)])
def test_identity_conflicts_cannot_be_repaired(field, value):
    c = case(); data, _ = queue(c); c['first_instance'][field] = value
    before = deepcopy(data)
    assert reconcile(data, [c], DAY) == [] and data == before


def test_later_round_cannot_replace_undated_task_even_in_normal_sync():
    c = case(); data, old_key = queue(c)
    data[fi_act_watch.FIELD][old_key]['block']['events'] = []
    c['first_instance']['events'][0]['date'] = '10.11.2026'
    assert reconcile(data, [c], DAY) == []
    fi_act_watch.sync(data, [c], DAY)
    assert data[fi_act_watch.FIELD][old_key]['status'] == 'needs_review'
    assert len(data[fi_act_watch.FIELD]) == 2


def test_known_text_and_expired_watch_do_not_create_news():
    for text, day, expected in [('Определение суда', '24.09.2026', 'complete'), ('', '01.01.2026', 'expired')]:
        c = case(); c['first_instance']['act_text'] = text
        c['first_instance']['events'][0]['date'] = day
        data, _ = queue(c)
        assert reconcile(data, [c], DAY)
        fi_act_watch.sync(data, [c], DAY)
        assert next(iter(data[fi_act_watch.FIELD].values()))['status'] == expected
        assert not data.get(fi_act_watch.PENDING)


def test_historical_episode_preserves_current_stage_and_date():
    old = case(); data, _ = queue(old)
    current = case(); current.update(id='2-20/2026', current_stage='appeal', history=[old])
    current['first_instance'].update(case_number='2-20/2026', link='456|new', decision_date='01.11.2026')
    before = deepcopy(current)
    assert reconcile(data, [current], DAY)
    assert current == before


def test_missing_link_requires_unique_compatible_donor():
    c = case(); data, old_key = queue(c)
    task = data[fi_act_watch.FIELD][old_key]
    task['reason'] = 'missing_card_link'; task['block']['link'] = ''
    assert reconcile(data, [c], DAY)[0]['action'] == 'identifiers_recovered'
    assert task['block']['link'] == c['first_instance']['link']
    assert task['status'] == 'waiting'
    conflict = deepcopy(c); conflict['first_instance']['link'] = 'different|uid'
    task.update(status='needs_review', reason='missing_card_link'); task['block']['link'] = ''
    assert reconcile(data, [c, conflict], DAY) == []
    assert not task['block']['link']


def test_writ_duplicate_preserves_changed_basis():
    c = case(); c['bank_role'] = 'Истец'; c['appeal'] = {'result': 'Решение отменено'}
    c['first_instance'].update(decision_date='01.09.2026', result='Иск удовлетворён', events=[])
    new_key = writ_watch.identity(c['first_instance'])
    old = deepcopy(c['first_instance']); old['decision_date'] = ''
    old_key = writ_watch.identity(old)
    data = {writ_watch.FIELD: {
        old_key: {'block': old, 'status': 'needs_review', 'reason': 'basis_changed', 'created_at': '2026-10-07'},
        new_key: {'block': deepcopy(c['first_instance']), 'status': 'needs_review', 'reason': 'basis_changed'}}}
    before = deepcopy(c)
    assert reconcile(data, [c], DAY)[0]['action'] == 'duplicate_writ_task_closed'
    assert c == before and data[writ_watch.FIELD][new_key]['status'] == 'needs_review'
    stale = deepcopy(c); stale['first_instance'] = deepcopy(old)
    writ_watch.sync(data, [stale, c], DAY)
    assert data[writ_watch.FIELD][old_key]['status'] == 'closed'
    assert data[writ_watch.FIELD][new_key]['reason'] == 'basis_changed'
    assert not reconcile(data, [c], DAY)


def test_copy_command_keeps_inputs_events_and_notifications(tmp_path, monkeypatch):
    from court_monitor.storage import save_bank_json
    source, output = tmp_path / 'original', tmp_path / 'copy'
    (source / 'data').mkdir(parents=True)
    c = case(); data, _ = queue(c)
    data.update(cases=[], pending_fi_act_changes=[{'type': 'preserve'}], delivered_at='2026-11-11')
    (source / 'data/cases.json').write_text(json.dumps(data))
    save_bank_json({'cases': [c]}, str(source / 'data/cases_bank.json'), str(source / 'data/cases_bank_events.json'))
    original = {p.name: p.read_bytes() for p in (source / 'data').iterdir()}
    real_reconcile = __import__('court_monitor.watch_repair', fromlist=['reconcile']).reconcile
    def verify_network(*args):
        with pytest.raises(RuntimeError, match='Сеть запрещена'):
            socket.create_connection(('example.invalid', 443))
        return real_reconcile(*args)
    monkeypatch.setattr('court_monitor.watch_repair.reconcile', verify_network)
    report = run(source, output, config.REGION, DAY)
    assert len(report['actions']) == 1 and all(report['checks'].values())
    assert {p.name: p.read_bytes() for p in (source / 'data').iterdir()} == original
    saved = {p.name: d for p, _, d in load_documents(output)}
    assert saved['cases_bank.json']['cases'][0]['first_instance']['events'] == c['first_instance']['events']
    assert saved['cases.json']['pending_fi_act_changes'] == data['pending_fi_act_changes']
    assert (output / 'manual_review.csv').read_bytes().startswith(b'\xef\xbb\xbf')
    with pytest.raises(ValueError, match='новым каталогом'):
        run(source, output, config.REGION, DAY)
    with pytest.raises(ValueError, match='совпадать'):
        run(source, source / 'nested', config.REGION, DAY)
