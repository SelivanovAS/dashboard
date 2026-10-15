"""One-off authorized generation of 24 pinned sources; no production writes or delivery."""
import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from urllib.parse import urlparse

root = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2]).resolve()
targets = json.loads(Path(__file__).with_name('hmao_recovered_summary_targets.json').read_text())
assert subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip() == targets['head']
assert json.loads((root / '.program-release.json').read_text())['source_commit'] == targets['source']
os.chdir(root)
os.environ['REGION'] = 'hmao'
os.environ['LLM_PROVIDER'] = 'openrouter'
out.mkdir(parents=True, exist_ok=True)
for name, env in [('data/.act_summaries.pending.json', 'ACT_SUMMARY_PENDING_PATH'),
                  ('data/.act_summaries.json', 'ACT_SUMMARIES_PATH'),
                  ('data/.llm_provider_state.json', 'LLM_PROVIDER_STATE_PATH')]:
    dest = out / Path(name).name
    if (root / name).exists():
        shutil.copyfile(root / name, dest)
    os.environ[env] = str(dest)
sys.path.insert(0, str(root / 'scripts'))
from court_monitor import config
from court_monitor.digest import llm, summary_audit, summary_queue
from court_monitor.act_preparation import prepare_act

logging.disable(logging.CRITICAL)
allowed = {'openrouter.ai', 'api.anthropic.com', 'ngw.devices.sberbank.ru',
           'gigachat.devices.sberbank.ru', 'api.giga.chat'}
original_request = llm.requests.sessions.Session.request
network = []
def request(self, method, url, **kwargs):
    u = urlparse(url)
    assert u.scheme == 'https' and u.hostname in allowed, 'Only configured summary providers allowed'
    network.append({'host': u.hostname, 'method': method, 'path': u.path})
    return original_request(self, method, url, **kwargs)
llm.requests.sessions.Session.request = request
before = summary_queue._load()
assert len(targets['jobs']) == 24 and len(set(targets['jobs'])) == 24
for key, expected in targets['jobs'].items():
    job = before[key]
    assert job['source_hash'] == expected['source_hash']
    assert hashlib.sha256(json.dumps(job, ensure_ascii=False, sort_keys=True).encode()).hexdigest() == expected['job_hash']
    assert summary_queue._key(job['text'], job['meta']) == key
    assert summary_audit.classify_job(job) == 'retry_pending'
    assert prepare_act(job['text'], job['meta']).status == 'ready'
if '--validate-only' in sys.argv:
    print(json.dumps({'validated': 24, 'model_calls': 0, 'sent': False}))
    sys.exit(0)
assert llm.summaries_configured()
summary_audit.reset()
report = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'base_head': targets['head'], 'source_commit': targets['source'], 'sent': False,
          'items': [], 'network': network}
def checkpoint():
    report['audit'] = summary_audit.snapshot()
    report['backlog'] = summary_audit.backlog_snapshot(summary_queue._load())
    (out / 'result.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
checkpoint()
for key in targets['jobs']:
    job = before[key]
    summary_queue.summarize_tracked(job['text'], case_meta=job['meta'])
    result = summary_queue._load()[key]
    assert result['text'] == job['text'] and result['meta'] == job['meta']
    item = {'key': key, 'case_number': job['meta']['case_number'],
            'status': summary_audit.classify_job(result), 'model': result.get('model'),
            'attempts': result.get('attempts', []), 'cached': result.get('cached', False),
            'fallback': result.get('fallback'), 'summary_chars': len(result.get('summary', ''))}
    report['items'].append(item)
    checkpoint()
    print(json.dumps({'completed': len(report['items']), **item}, ensure_ascii=False), flush=True)
after = summary_queue._load()
assert set(after) == set(before)
assert all(after[k] == v for k, v in before.items() if k not in targets['jobs'])
assert not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], text=True)
report['completed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
report['all_ready'] = all(x['status'] == 'ready' for x in report['items'])
checkpoint()
