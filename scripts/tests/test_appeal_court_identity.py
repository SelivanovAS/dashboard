"""Связка апелляции: реальные коллизии номеров первого импорта Башкортостана."""
from copy import deepcopy

import pytest

from court_monitor import config, linking, runs

AP_DOMAIN = 'vs--bkr.sudrf.ru'
OWN = 'salavatsky--bkr.sudrf.ru'


@pytest.fixture(autouse=True)
def region(monkeypatch):
    monkeypatch.setattr(config, 'REGION', 'bashkortostan')
    monkeypatch.setattr(config, 'BANK_TRACK', True)


def appeal(ap_number, fi_number, **fi_fields):
    return {
        'id': ap_number, 'current_stage': 'appeal',
        'first_instance': {'court': 'Салаватский городской суд', 'court_domain': '',
                           'case_number': fi_number, **fi_fields},
        'appeal': {'case_number': ap_number, 'court_domain': AP_DOMAIN,
                   'link': '123|uid', 'events': [{'date': '11.09.2026', 'text': 'Передано судье'}],
                   'act_text': 'Сохранённый текст апелляционного акта'},
    }


def first_instance(number, domain=OWN, **fields):
    return {
        'id': number, 'current_stage': 'first_instance',
        'first_instance': {'case_number': number, 'court_domain': domain,
                           'events': [{'date': '01.06.2026', 'text': 'Решение вынесено'}]},
        **fields,
    }


@pytest.mark.parametrize('ap_number,number,foreign_domain', [
    ('33-18171/2026', '2-739/2026', 'baimaksky--bkr.sudrf.ru'),
    ('33-18527/2026', '2-1236/2026', 'ishimbaisky--bkr.sudrf.ru'),
])
def test_confirmed_bootstrap_collisions_do_not_link(ap_number, number, foreign_domain):
    ap, foreign = appeal(ap_number, number), first_instance(number, foreign_domain)
    original = deepcopy([ap, foreign])
    out = linking.link_cases([ap, foreign], {(AP_DOMAIN, ap_number): number})
    assert out == original
    assert 'appeal' not in foreign


@pytest.mark.parametrize('is_bank', [False, True])
def test_own_court_wins_after_foreign_number_and_keeps_events(is_bank):
    number = '2-739/2026'
    ap = appeal('33-18171/2026', number)
    foreign = first_instance(number, 'baimaksky--bkr.sudrf.ru')
    own = first_instance(number, **({'track': 'plaintiff_light'} if is_bank else {}))
    ap_before, fi_before = deepcopy(ap['appeal']), deepcopy(own['first_instance'])
    out = linking.link_cases([foreign, ap, own], {(AP_DOMAIN, '33-18171/2026'): number})
    assert out == [foreign, own] and 'appeal' not in foreign
    assert own['appeal'] == ap_before and own['first_instance'] == fi_before
    assert own['current_stage'] == 'appeal'
    if is_bank:
        main, bank, archived, moved = runs.split_bank_track(out)
        assert main == out and not bank and not archived and moved == 1
        assert own['track_origin'] == 'plaintiff_light' and 'track' not in own
        assert own['first_instance']['events'] == fi_before['events']


def test_two_same_court_targets_are_left_for_review():
    number = '2-739/2026'
    cases = [appeal('33-18171/2026', number), first_instance(number), first_instance(number)]
    original = deepcopy(cases)
    assert linking.link_cases(cases, {(AP_DOMAIN, '33-18171/2026'): number}) == original


def test_unknown_source_court_does_not_pick_known_target():
    number = '2-739/2026'
    ap = appeal('33-18171/2026', number, court='')
    cases = [ap, first_instance(number)]
    assert linking.link_cases(cases, {(AP_DOMAIN, '33-18171/2026'): number}) == cases
    assert 'appeal' not in cases[1]


def test_registered_alias_links_without_domain():
    from court_monitor.regions import get_region
    ct = next(c for c in get_region().first_instance_courts if c.name_aliases)
    number = '2-10/2026'
    ap = appeal('33-10/2026', number, court=ct.name_aliases[0])
    fi = first_instance(number, ct.domain)
    assert linking.link_cases([ap, fi], {(AP_DOMAIN, '33-10/2026'): number}) == [fi]


def test_same_domain_server_collision_requires_matching_site(monkeypatch):
    monkeypatch.setattr(config, 'REGION', 'hmao')
    number = '2-739/2026'
    shared = 'vartovray--hmao.sudrf.ru'
    ap = appeal('33-18171/2026', number, court_domain=shared, srv_num=2,
                court='Нижневартовский районный суд (г. Покачи)')
    a, b = first_instance(number, shared), first_instance(number, shared)
    a['first_instance']['srv_num'] = 1
    b['first_instance']['srv_num'] = 2
    out = linking.link_cases([ap, a, b], {(AP_DOMAIN, '33-18171/2026'): number})
    assert out == [a, b] and 'appeal' not in a and b['appeal']['case_number'] == '33-18171/2026'
