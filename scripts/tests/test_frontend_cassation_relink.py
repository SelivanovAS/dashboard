"""Основной номер, ссылка и суд после кассационной отмены."""
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

APP = Path(__file__).resolve().parents[2] / 'app.js'


@pytest.mark.skipif(not shutil.which('node'), reason='Node required')
@pytest.mark.parametrize('stage,number,domain', [
    ('cassation', '8Г-12188/2026', '7kas.sudrf.ru'),
    ('awaiting_relink', '8Г-12188/2026', '7kas.sudrf.ru'),
    ('appeal', '33-30/2026', 'oblsud--hmao.sudrf.ru'),
    ('first_instance', '2-716/2025', 'surggor--hmao.sudrf.ru'),
])
def test_primary_identity_and_court(stage, number, domain):
    source = APP.read_text()
    functions = '\n'.join(re.search(r'function ' + name + r'\([^\n]*\)\{.*?\n\}', source, re.S).group(0)
                          for name in ['stageGroup', 'isCassationStage', 'isAppealStage', 'regionCassation', 'buildCourtLink', 'courtTitle', 'courtJudge'])
    # Реальный участок преобразования до вычисления статуса и оформления.
    identity = source[source.index('function jsonToCase(j){'):source.index("  const evText=primary.last_event||'';")]
    identity += 'return {caseNumber,link};}\n'
    record = {'id': '2-716/2025', 'current_stage': stage,
              'first_instance': {'case_number': '2-716/2025', 'court_domain': 'surggor--hmao.sudrf.ru', 'link': '123|abc'},
              'appeal': {'case_number': '33-30/2026', 'court_domain': 'oblsud--hmao.sudrf.ru', 'link': '456|def'},
              'cassation': {'case_number': '8Г-12188/2026', 'court_domain': '7kas.sudrf.ru', 'link': '12120551|228a9fc7-0e96-4596-9ae4-99bc86b65684'}}
    script = 'const window={};\n' + functions + '\n' + identity
    script += 'console.log(JSON.stringify(jsonToCase(' + json.dumps(record) + ')));'
    result = subprocess.run(['node', '-e', script], capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    assert data['caseNumber'] == number
    assert data['link'].startswith('https://' + domain + '/')
    if stage in ('cassation', 'awaiting_relink'):
        assert 'delo_id=2800001&new=2800001' in data['link']
