"""Изолированный прогон полной подготовки и пересказа. Только бесплатный маршрут."""
import json
import os
from pathlib import Path
import sys
import time

OUT = Path(os.environ['BENCHMARK_OUTPUT'])
OUT.mkdir(parents=True, exist_ok=True)
# До импорта приложения: все данные/кэш только runner.temp, доставка и резервы выключены.
for name in ('TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'ANTHROPIC_API_KEY', 'GIGACHAT_AUTH_KEY', 'WEB_PUSH_API_URL'):
    os.environ[name] = ''
os.environ.update(LLM_PROVIDER='openrouter', LLM_SUMMARY_PROVIDER_FALLBACK='0',
    OPENROUTER_SUMMARY_MODEL='apodex/apodex-1.1-mini:free',
    ACT_SUMMARIES_PATH=str(OUT/'cache.json'), ACT_SUMMARY_PENDING_PATH=str(OUT/'pending.json'),
    LLM_PROVIDER_STATE_PATH=str(OUT/'provider.json'))
sys.path.insert(0, str(Path('scripts').resolve()))
import requests
from court_monitor import config
from court_monitor.digest import llm, summary_queue
from court_monitor.act_preparation import prepare_act

model = config.OPENROUTER_SUMMARY_MODEL
catalog = requests.get('https://openrouter.ai/api/v1/models', timeout=20)
catalog.raise_for_status()
entry = next(m for m in catalog.json()['data'] if m['id'] == model)
assert model.endswith(':free') and all(float(entry['pricing'][k]) == 0 for k in ('prompt', 'completion'))
key = requests.get('https://openrouter.ai/api/v1/key', headers={'Authorization':'Bearer '+config.OPENROUTER_API_KEY}, timeout=20)
key.raise_for_status()
quota = key.json()['data'].get('free_model_daily_requests')
assert isinstance(quota, dict) and quota.get('remaining',0) >= 60, 'Недостаточная подтверждённая бесплатная квота'

post = requests.post
calls = 0
previous = 0

def bounded_post(url, **kw):
    global calls, previous
    assert url == config.OPENROUTER_API_URL
    assert kw['json']['model'] == model and kw['json']['reasoning'] == {'enabled':False}
    assert calls < 60
    time.sleep(max(0, 4-(time.monotonic()-previous)))
    previous=time.monotonic();calls+=1
    return post(url, **kw)

llm.requests.post = bounded_post
samples=json.loads(Path('model-selection/implementation-holdout.json').read_text())['samples']
results=[]
for sample in samples:
    prepared=prepare_act(sample['text'],sample['meta'])
    summary=summary_queue.summarize_tracked(sample['text'],case_meta=sample['meta'])
    job=next(j for j in summary_queue._load().values() if j['source_hash']==prepared.audit['source_hash'])
    results.append({'id':sample['id'],'case':sample['case'],'stage':sample['stage'],
        'summary':summary,'status':job['status'],'preparation':job.get('preparation'),
        'attempts':job.get('attempts'),'model':job.get('model'),'refusal_rechecked':job.get('refusal_rechecked')})
    (OUT/'pipeline-results.json').write_text(json.dumps({'results':results,'calls':calls,'planned_acts':len(samples)},ensure_ascii=False,indent=2))
    print(sample['id'],job['status'],'calls',calls,flush=True)
