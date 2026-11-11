"""Границы исправления оснований ИЛ и разделения одноимённых дел."""
from copy import deepcopy
from datetime import date
import hashlib

import pytest

from court_monitor import lifecycle, act_anchor, writ_watch, runs, config
from court_monitor.writ_repair import reconcile, fingerprint, same, repair_identity

DAY = date(2026, 11, 11)


def case():
    return {'id': '2-12/2026', 'bank_role': 'Истец', 'current_stage': 'first_instance',
            'first_instance': {'court_domain': 'surggor--hmao.sudrf.ru', 'srv_num': 1,
                'court': 'Сургутский городской суд', 'case_number': '2-12/2026',
                'link': '1|uuid', 'judicial_uid': 'test-uid', 'decision_date': '01.09.2026',
                'status': 'Решено', 'result': 'Иск УДОВЛЕТВОРЕН', 'events': [
                    {'date': '01.09.2026', 'text': 'Вынесено решение по делу'}]}}


def observation(c):
    return {**deepcopy(c['first_instance']), 'id': 'obs', 'stage': 'first_instance',
            'url': 'https://surggor--hmao.sudrf.ru/', 'checked_at': '2026-11-11T10:00:00+05:00',
            'html_sha256': 'a' * 64, 'parsed': {'_writs': []}}


@pytest.mark.parametrize('text', [
    'Рассмотрение заявления. Вынесено решение (определение)',
    'Судебное заседание. Вынесено решение (определение).',
    'Вынесено решение   (определение)',
])
def test_generic_ruling_is_not_new_merits_decision(text):
    events = case()['first_instance']['events'] + [{'date': '10.10.2026', 'text': text}]
    assert lifecycle.fi_decision_date_from_events(events) == '01.09.2026'
    assert act_anchor.kind(text) == ''
    assert lifecycle.fi_decision_date_from_events(events + [
        {'date': '20.10.2026', 'text': 'Вынесено заочное решение по делу'}]) == '20.10.2026'


def test_conflicting_live_uid_cannot_update_case_or_success_date():
    c = case()
    c['first_instance']['last_checked_at'] = '2026-11-01'
    old = deepcopy(c['first_instance'])
    info = {'УИД': 'foreign-uid', 'Результат': 'Отказано', '_writs': [
        {'issue_date': '01.10.2026', 'electronic_id': 'foreign', 'status': 'Выдан'}]}
    changed, event = runs.apply_first_instance_card(c, info, None, DAY)
    assert not changed and not event['type']
    assert c['first_instance'] == {**old, 'writ_source_review_reason': 'identity_conflict'}


def test_inconsistent_instance_uids_cannot_confirm_a_writ():
    c = case()
    c['appeal'] = {'judicial_uid': 'another-production'}
    assert writ_watch.state(c, DAY) == ('needs_review', 'identity_conflict')


@pytest.mark.parametrize('field,value', [('court_domain', 'other.sudrf.ru'),
                                      ('srv_num', 2), ('judicial_uid', 'other'),
                                      ('link', '2|another'), ('case_number', '2-13/2026')])
def test_evidence_never_matches_only_by_case_number(field, value):
    c = case()
    assert not same(c['first_instance'], {**observation(c), field: value})


def test_writ_additions_preserve_old_status_and_do_not_close_returned_writ():
    c = case()
    old = {'issue_date': '01.10.2026', 'electronic_id': 'old', 'status': 'Возвращен'}
    new = {'issue_date': '05.10.2026', 'electronic_id': 'new', 'status': 'Выдан'}
    c['first_instance']['writs'] = [deepcopy(old)]
    obs = observation(c)
    obs['parsed']['_writs'] = [{**old, 'status': 'Выдан'}, new]
    data = {'cases': [c], 'pending_retry_context': {'unchanged': True}}
    actions = reconcile(data, [c], {}, {'obs': obs}, DAY)
    assert c['first_instance']['writs'] == [old, new]
    assert 'last_checked_at' not in c['first_instance']
    task = data['writ_watch'][writ_watch.identity(c['first_instance'])]
    assert task['reason'] == 'writ_recalled_or_returned'
    assert data['pending_retry_context'] == {'unchanged': True}
    assert len(actions) == 1 and actions[0]['added_writs'] == [new]
    before = deepcopy(data)
    assert reconcile(data, [c], {}, {'obs': obs}, DAY) == [] and data == before


def test_date_correction_requires_text_and_rekeys_existing_tasks():
    c = case()
    fi = c['first_instance']
    fi['act_text'] = 'test-uid Решение 2 сентября 2026 года'
    fi['writ_source_review_reason'] = 'decision_date_changed'
    old_key = writ_watch.identity(fi)
    data = {'cases': [c], 'writ_watch': {old_key: {
        'block': deepcopy(fi), 'status': 'needs_review', 'reason': 'decision_date_changed'}}}
    obs = observation(c)
    obs['parsed'].update({'Дата рассмотрения (карточка)': '02.09.2026',
                         '_events': [{'date': '02.09.2026', 'text': 'Вынесено решение по делу'}]})
    fix = {'observation': 'obs', 'old_date': '01.09.2026', 'new_date': '02.09.2026',
           'act_text_sha256': hashlib.sha256(fi['act_text'].encode()).hexdigest(),
           'evidence_quote': '2 сентября 2026 года'}
    bad = deepcopy(fix)
    bad['act_text_sha256'] = '0' * 64
    with pytest.raises(ValueError):
        reconcile(deepcopy(data), [deepcopy(c)], {'date_corrections': [bad]}, {'obs': obs}, DAY)
    reconcile(data, [c], {'date_corrections': [fix]}, {'obs': obs}, DAY)
    assert fi['decision_date'] == '02.09.2026' and old_key not in data['writ_watch']
    assert data['writ_watch'][writ_watch.identity(fi)]['status'] == 'waiting'
    assert fi.get('writ_source_review_reason') is None


def test_generic_ruling_does_not_clear_other_frozen_reason():
    c = case()
    c['first_instance']['writ_source_review_reason'] = 'fi_production_reopened'
    obs = observation(c)
    obs['parsed'].update({'Дата рассмотрения (карточка)': '01.09.2026',
                         '_events': c['first_instance']['events']})
    with pytest.raises(ValueError, match='иное основание'):
        reconcile({'cases': [c]}, [c], {'generic_ruling_reviews': [
            {'observation': 'obs', 'decision_date': '01.09.2026'}]}, {'obs': obs}, DAY)


def test_identity_repair_preserves_original_and_two_distinct_productions(monkeypatch):
    monkeypatch.setattr(config, 'REGION', 'sverdlovsk_yanao')
    real = case()
    real['id'] = '33-real/2026'
    real['first_instance'].update(court='Муравленковский городской суд',
        court_domain='muravlenkovsky--ynao.sudrf.ru', judicial_uid='murav-uid')
    target = case()
    target['first_instance'].update(court='Североуральский городской суд',
        court_domain='severouralsky--svd.sudrf.ru', judicial_uid='north-uid', link='2|north')
    bad = deepcopy(target)
    bad['first_instance'] = deepcopy(real['first_instance'])
    bad['first_instance']['judicial_uid'] = 'north-uid'
    bad['appeal'] = {'court_domain': 'oblsud--svd.sudrf.ru', 'link': '3|ap',
                     'case_number': '33-1/2026', 'result': 'Решение отменено'}
    data = {'cases': [real, bad, target]}
    prior = deepcopy(bad)
    obs = observation(real)
    ap = {**observation(target), 'id': 'ap', 'court_domain': 'oblsud--svd.sudrf.ru',
          'link': '3|ap', 'lower_court': 'Североуральский городской суд',
          'parsed': {'Номер дела 1 инстанции': '2-12/2026'}}
    spec = {'id': 'repair', 'wrong_case_sha256': fingerprint(bad),
            'correct_first_instance': deepcopy(target['first_instance']),
            'linked_card_identity': deepcopy(real['first_instance']),
            'appeal_observation': 'ap', 'fi_observation': 'obs'}
    assert len(repair_identity(data, data['cases'], spec, {'ap': ap, 'obs': obs}, DAY)) == 2
    assert data['cases'] == [real, target]
    assert target['appeal']['judicial_uid'] == 'north-uid'
    assert real['first_instance']['judicial_uid'] == 'murav-uid'
    assert data['writ_repair_log'][0]['original_case'] == prior
    assert repair_identity(data, data['cases'], spec, {'ap': ap, 'obs': obs}, DAY) == []
