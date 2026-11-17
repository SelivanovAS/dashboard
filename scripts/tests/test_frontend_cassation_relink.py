"""Основной номер дела и ссылки на карточки судов в таблице и боковой панели."""
import json
from html import unescape
from pathlib import Path
import shutil
import subprocess

import pytest

APP = Path(__file__).resolve().parents[2] / 'app.js'
pytestmark = pytest.mark.skipif(not shutil.which('node'), reason='Node required')


def run_app(record, scenario):
    script = """
const fs = require('node:fs'), vm = require('node:vm');
const drawer = {};
const document = {getElementById: id => id === 'drawer' ? drawer : null, addEventListener() {}};
const context = vm.createContext({window: {addEventListener() {}}, navigator: {}, document,
                                  record: JSON.parse(process.argv[2])});
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), context);
console.log(JSON.stringify(vm.runInContext(process.argv[3], context)));
"""
    out = subprocess.run(['node', '-e', script, str(APP), json.dumps(record), scenario],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


@pytest.mark.parametrize('stage,number,domain', [
    ('cassation', '8Г-12188/2026', '7kas.sudrf.ru'),
    ('awaiting_relink', '8Г-12188/2026', '7kas.sudrf.ru'),
    ('appeal', '33-30/2026', 'oblsud--hmao.sudrf.ru'),
    ('first_instance', '2-716/2025', 'surggor--hmao.sudrf.ru'),
])
def test_primary_identity_and_court(stage, number, domain):
    record = {'id': '2-716/2025', 'current_stage': stage,
              'first_instance': {'case_number': '2-716/2025', 'court_domain': 'surggor--hmao.sudrf.ru', 'link': '123|abc'},
              'appeal': {'case_number': '33-30/2026', 'court_domain': 'oblsud--hmao.sudrf.ru', 'link': '456|def'},
              'cassation': {'case_number': '8Г-12188/2026', 'court_domain': '7kas.sudrf.ru', 'link': '12120551|228a9fc7-0e96-4596-9ae4-99bc86b65684'}}
    data = run_app(record, 'jsonToCase(record)')
    assert data['caseNumber'] == number
    assert data['link'].startswith('https://' + domain + '/')
    if stage in ('cassation', 'awaiting_relink'):
        assert 'delo_id=2800001&new=2800001' in data['link']


@pytest.mark.parametrize('appeal_link', ['456|def', 'https://oblsud--hmao.sudrf.ru/appeal/card', ''])
def test_drawer_link_follows_tab(appeal_link):
    # Апелляция и президиум находятся на одном сайте, но в разных разделах.
    district = 'surggor--hmao.sudrf.ru'
    regional = 'oblsud--hmao.sudrf.ru'
    record = {
        'id': '2-716/2025', 'current_stage': 'cassation',
        'first_instance': {'case_number': '2-716/2025', 'court_domain': district,
                           'link': '123|abc', 'srv_num': 2},
        'appeal': {'case_number': '33-30/2026', 'court_domain': regional, 'link': appeal_link},
        'cassation': {'case_number': '4Г-1/2026', 'court_domain': regional, 'link': '789|aaa'},
    }
    urls = run_app(record, r"""
const c = jsonToCase(record);
filteredCases = [c];
activeCaseNumber = c.caseNumber;
['cs', 'ap', 'fi', 'cs'].map(stage => {
  setDrawerStage(stage);
  const footer = document.getElementById('drawer').innerHTML.split('<div class="drawer-footer">')[1];
  const link = footer.match(/<a\b[^>]*href="([^"]+)"/);
  return link ? link[1] : '';
});
""")

    def card(domain, srv, cid, uid, did, new):
        return (f'https://{domain}/modules.php?name=sud_delo&srv_num={srv}&name_op=case'
                f'&case_id={cid}&case_uid={uid}&delo_id={did}&new={new}')

    cs = card(regional, 1, 789, 'aaa', 2800001, 2800001)
    ap = card(regional, 1, 456, 'def', 5, 5) if appeal_link == '456|def' else appeal_link
    assert [unescape(url) for url in urls] == [cs, ap, card(district, 2, 123, 'abc', 1540005, 0), cs]
