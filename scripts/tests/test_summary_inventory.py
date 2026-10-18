"""Inventory is read-only; historical texts never seed the ordinary replay."""
from copy import deepcopy
import json

import pytest

from court_monitor import config, runs
from court_monitor.act_preparation import source_hash
from court_monitor.digest import llm, summary_queue
from review_act_summaries import inventory


TEXT = ('РЕШЕНИЕ. Дело 2-17/2026. ' + 'Суд исследовал представленные доказательства. ' * 8
        + 'РЕШИЛ: В удовлетворении исковых требований отказать в полном объёме.')
META = {'stage': 'first_instance', 'court_domain': 'test.sudrf.ru', 'case_number': '2-17/2026'}


def write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')


def snapshot(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    (tmp_path / 'REGION').write_text('hmao')
    block = {**META, 'act_text': TEXT[:100], 'act_summary_needs_source': True,
             'decision_date': '01.01.2026'}
    case = {'id': META['case_number'], 'current_stage': 'appeal', 'first_instance': block,
            'events': [{'text': 'SECRET EVENT'}]}
    write(data / 'cases.json', {'region': {'code': 'hmao'}, 'cases': [case], 'fi_act_watch': {
        'old-act': {'status': 'expired', 'reason': 'age_limit', 'block': deepcopy(block)}}})
    write(data / '.act_summaries.pending.json', {
        'incomplete': {'status': 'source_incomplete', 'text': TEXT[:100], 'meta': META,
                       'source_hash': source_hash(TEXT[:100])},
        'conflict': {'status': 'case_mismatch', 'text': TEXT, 'meta': {**META, 'case_number': '2-99/2026'},
                     'source_hash': source_hash(TEXT)},
        'ready': {'status': 'ready', 'text': TEXT, 'meta': META, 'summary': 'SECRET SUMMARY'},
    })
    return data


def test_inventory_exposes_expired_source_without_active_retry_or_writes(tmp_path):
    data = snapshot(tmp_path)
    before = {p.name: p.read_bytes() for p in data.iterdir()}
    result = inventory(data, 'hmao')
    assert result['counts'] == {'ready': 1, 'superseded': 0, 'waiting_source': 1,
                                'needs_review': 1, 'retry_pending': 0}
    assert result['source_watch_counts'] == {'expired': 1}
    assert next(x for x in result['items'] if x['job_id'] == 'incomplete')['matches'] == 1
    assert {p.name: p.read_bytes() for p in data.iterdir()} == before
    rendered = json.dumps(result, ensure_ascii=False)
    assert 'SECRET' not in rendered and TEXT[:100] not in rendered
    second = inventory(data, 'hmao')
    assert second['items'] == result['items'] and second['files'] == result['files']


def test_inventory_does_not_choose_first_round_or_namesake(tmp_path):
    data = snapshot(tmp_path)
    doc = json.loads((data / 'cases.json').read_text())
    case = doc['cases'][0]
    case['history'] = [{'first_instance': deepcopy(case['first_instance'])}]
    write(data / 'cases.json', doc)
    result = inventory(data, 'hmao')
    row = next(x for x in result['items'] if x['job_id'] == 'incomplete')
    assert row['matches'] == 2 and row['source_watch'] == 'unmatched'
    assert row['store'] == ''


def test_inventory_keeps_malformed_job_visible_without_linking(tmp_path):
    data = snapshot(tmp_path)
    write(data / '.act_summaries.pending.json', {'bad': {
        'status': 'pending', 'meta': {**META, 'case_number': ['SECRET INVALID FIELD']},
        'text': TEXT}})
    result = inventory(data, 'hmao')
    assert result['counts']['needs_review'] == 1
    assert result['items'][0]['matches'] == 0
    assert result['items'][0]['source_watch'] == 'unmatched'
    assert 'SECRET' not in json.dumps(result)


def test_inventory_rejects_wrong_region_and_missing_queue(tmp_path):
    data = snapshot(tmp_path)
    with pytest.raises(ValueError, match='REGION'):
        inventory(data, 'tyumen')
    (data / '.act_summaries.pending.json').unlink()
    with pytest.raises(FileNotFoundError):
        inventory(data, 'hmao')


@pytest.mark.parametrize('stage,field', [('appeal', 'appeal_act_watch'),
                                        ('cassation', 'cassation_act_watch')])
def test_inventory_matches_upper_instance_watch(tmp_path, stage, field):
    data = snapshot(tmp_path)
    meta = dict(META, stage=stage)
    block = {**meta, 'act_text': 'Обрыв сохранённого текста', 'decision_date': '01.01.2026'}
    write(data / 'cases.json', {'cases': [{stage: block}], field: {
        'task': {'status': 'waiting', 'next_check_at': '2026-01-05', 'block': block}}})
    write(data / '.act_summaries.pending.json', {'job': {
        'status': 'source_incomplete', 'meta': meta, 'text': block['act_text'],
        'source_hash': source_hash(block['act_text'])}})
    assert inventory(data, 'hmao')['source_watch_counts'] == {'waiting': 1}


@pytest.mark.parametrize('due,expected', [('2026-01-05', 'waiting'),
                                        ('2026-11-13', 'scheduled_after_expiry'),
                                        ('', 'unscheduled')])
def test_inventory_checks_real_retry_schedule(tmp_path, due, expected):
    data = snapshot(tmp_path)
    path = data / 'cases.json'
    doc = json.loads(path.read_text())
    task = doc['fi_act_watch']['old-act']
    task.update(status='waiting', next_check_at=due)
    write(path, doc)
    assert inventory(data, 'hmao')['source_watch_counts'] == {expected: 1}


def test_regular_replay_does_not_enroll_archived_raw_acts(tmp_path, monkeypatch):
    fields = ('JSON_PATH', 'JSON_ARCHIVE_PATH', 'JSON_BANK_PATH', 'JSON_BANK_EVENTS_PATH',
              'JSON_BANK_ARCHIVE_PATH', 'JSON_BANK_ARCHIVE_EVENTS_PATH')
    for name in fields:
        monkeypatch.setattr(config, name, str(tmp_path / (name + '.json')))
    case = {'id': META['case_number'], 'first_instance': {
        **META, 'act_text': TEXT, 'act_analysis': {'source': 'raw_act'}}}
    write(tmp_path / 'JSON_ARCHIVE_PATH.json', {'cases': [case]})
    before = (tmp_path / 'JSON_ARCHIVE_PATH.json').read_bytes()
    monkeypatch.setattr(llm, 'summaries_configured', lambda: True)
    monkeypatch.setattr(llm, 'summarize_act_motivation', lambda *_a, **_k: pytest.fail('Unexpected LLM'))
    runs._process_act_summary_queue(retry=True)
    assert summary_queue._load() == {}
    assert (tmp_path / 'JSON_ARCHIVE_PATH.json').read_bytes() == before
