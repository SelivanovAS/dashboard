"""Поздние акты: история, архив, расписание, повторы и аварийное сохранение."""
from datetime import date

import pytest
from court_monitor import act_watch, config

TODAY = date(2026, 10, 5)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'JSON_PATH', str(tmp_path / 'cases.json'))
    monkeypatch.setattr(config, 'CASSATION_ACTS_PATH', str(tmp_path / 'acts'))
    monkeypatch.setattr(config, 'JSON_ARCHIVE_PATH', str(tmp_path / 'archive.json'))
    monkeypatch.setattr(config, 'BANK_TRACK', False)
    monkeypatch.setattr(config, 'REGION', 'hmao')


def case():
    return {'id': '2-716/2025', 'plaintiff': 'Тестовый истец', 'defendant': 'ПАО Сбербанк',
            'bank_role': 'Ответчик', 'current_stage': 'awaiting_relink',
            'first_instance': {'case_number': '2-716/2025', 'court': 'Сургутский городской суд'},
            'cassation': {'case_number': '8Г-12188/2026', 'court_domain': '7kas.sudrf.ru',
                         'link': '12120551|228a9fc7-0e96-4596-9ae4-99bc86b65684',
                         'judicial_uid': '86-test', 'decision_date': '08.09.2026',
                         'act_absent_checked_at': '2026-09-09', 'last_checked_at': '2026-09-09', 'outcome': 'cassation_remanded',
                         'act_published': False}}


@pytest.mark.parametrize('decision,last,next_day', [
    ('08.09.2026', '2026-10-02', '2026-10-05'),
    ('01.08.2026', '2026-10-02', '2026-10-05'),
    ('01.06.2026', '2026-10-02', '2026-11-23'),
])
def test_cadence(decision, last, next_day):
    block = {'decision_date': decision, 'last_checked_at': last}
    assert act_watch.next_check(block, TODAY).isoformat() == next_day


def test_normal_parser_publication_is_durable_even_if_old_dedup_was_written(tmp_path):
    c = case(); data = {}
    act_watch.sync(data, [c], TODAY)
    c['cassation'].update(act_notification_kind='new_publication', act_published=True, act_text='Мотивировка. ' * 50)
    (tmp_path / 'acts').write_text('8Г-12188/2026|08.09.2026\n')
    act_watch.sync(data, [c], TODAY)
    assert len(data['pending_cassation_changes']) == 1
    act_watch.sync(data, [c], TODAY)
    assert len(data['pending_cassation_changes']) == 1
