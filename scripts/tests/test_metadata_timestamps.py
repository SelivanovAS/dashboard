"""Метаданные VPS и GitHub обозначают один момент, сохраняя местный день."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest


ROOT = Path(__file__).resolve().parents[2]
INSTANT = datetime(2026, 9, 11, 19, 30, tzinfo=timezone.utc)

# Отдельный процесс изолирует TZ: тесты не меняют часы/пояс остального suite.
WRITE_SNAPSHOT = r'''
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from court_monitor import config, health, storage

instant = datetime(2026, 9, 11, 19, 30, tzinfo=timezone.utc)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromtimestamp(instant.timestamp(), tz)

storage.datetime = health.datetime = FrozenDatetime
config.JSON_PATH = str(Path('cases.json').resolve())
config.PARSE_HEALTH_PATH = str(Path('parse_health.json').resolve())
day = FrozenDatetime.now().date().isoformat()
case = {'id': '2-1/2026', 'first_instance': {
    'court_domain': 'example.sudrf.ru', 'case_number': '2-1/2026',
    'last_checked_at': day, 'hearing_date': '15.09.2026', 'hearing_time': '09:30',
    'events': [{'date': '11.09.2026', 'text': 'Назначено заседание'}],
}}
main = {'version': 1, 'cases': [deepcopy(case)]}
storage.save_json(main, config.JSON_PATH)
bank = {'version': 1, 'track': 'plaintiff_light', 'cases': [deepcopy(case)]}
storage.save_bank_json(bank, 'cases_bank.json', 'cases_bank_events.json')
state, _ = health.update_parse_health(
    {'civil:ok': 3, 'civil:failed': None, 'civil:captcha': 0},
    state={'version': 1, 'sources': {}},
    captcha={'civil:captcha': 'example.sudrf.ru'},
)
health.save_parse_health(state)
documents = {name: json.loads(Path(name).read_text()) for name in (
    'cases.json', 'cases_bank.json', 'cases_bank_events.json', 'parse_health.json')}
assert documents['cases.json']['cases'][0] == case
assert storage.load_bank_json('cases_bank.json', 'cases_bank_events.json')['cases'][0] == case
assert bank['cases'][0] == case
assert bank['updated_at'] == documents['cases_bank.json']['updated_at']
assert health.load_parse_health() == state
stamps = [doc['updated_at'] for doc in documents.values()]
stamps += [source['last_run_at'] for source in state['sources'].values()]
stamps.append(state['sources']['civil:captcha']['captcha_since'])
print(json.dumps({'timestamps': stamps, 'local_day': day,
                  'searched_today': sorted(health.searched_ok_today(state))}))
'''


@pytest.mark.parametrize('zone, offset, day', [
    ('UTC', 0, '2026-09-11'),
    ('Asia/Yekaterinburg', 5, '2026-09-12'),
])
def test_real_metadata_writers_keep_instant_and_local_day(tmp_path, zone, offset, day):
    result = subprocess.run(
        [sys.executable, '-c', WRITE_SNAPSHOT], cwd=tmp_path,
        env={**os.environ, 'TZ': zone, 'PYTHONPATH': str(ROOT / 'scripts')},
        text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    written = json.loads(result.stdout.splitlines()[-1])
    assert written['local_day'] == day
    assert written['searched_today'] == ['civil:ok']
    assert len(written['timestamps']) == 8
    for stamp in written['timestamps']:
        parsed = datetime.fromisoformat(stamp)
        assert parsed.utcoffset() == timedelta(hours=offset)
        assert parsed == INSTANT
        assert stamp[:10] == day  # Дочитка сравнивает местную календарную дату.


@pytest.mark.parametrize('browser_zone', ['UTC', 'Asia/Yekaterinburg', 'America/Los_Angeles'])
def test_existing_frontend_parser_accepts_offset_and_legacy_utc(browser_zone):
    node = shutil.which('node')
    if not node:
        pytest.skip('node недоступен')
    source = (ROOT / 'app.js').read_text(encoding='utf-8')
    parser = re.search(r'function parseIsoUtc\(s\)\{[\s\S]*?\n\}', source)
    assert parser
    script = parser.group(0) + '''
console.log(JSON.stringify([
  '2026-09-12T00:30:00+05:00',
  '2026-09-11T19:30:00+00:00',
  '2026-09-11T19:30:00Z',
  '2026-09-11T19:30:00'
].map(s => parseIsoUtc(s).toISOString())));
'''
    result = subprocess.run([node, '-e', script], text=True, capture_output=True,
                            env={**os.environ, 'TZ': browser_zone})
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ['2026-09-11T19:30:00.000Z'] * 4
