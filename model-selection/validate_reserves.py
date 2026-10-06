"""Five identical full sources per reserve; isolated files, no delivery."""
import json, os, sys, time
from pathlib import Path
OUT = Path(os.environ['BENCHMARK_OUTPUT']); OUT.mkdir(parents=True, exist_ok=True)
for key in ('OPENROUTER_API_KEY','TELEGRAM_BOT_TOKEN','TELEGRAM_CHAT_ID','WEB_PUSH_API_URL'):
    os.environ[key] = ''
os.environ['LLM_SUMMARY_PROVIDER_FALLBACK'] = '0'
sys.path.insert(0, str(Path('scripts').resolve()))
import requests
from court_monitor import config
from court_monitor.digest import llm, summary_queue
from court_monitor.act_preparation import prepare_act
samples = [s for s in json.loads(Path('model-selection/implementation-holdout.json').read_text())['samples']
           if s['id'] in os.environ.get('BENCHMARK_SAMPLE_IDS','H02,H08,H09,H19,H24').split(',')]
post = requests.post
records=[]; estimates={'claude':0.0}; counts={'gigachat':0,'claude':0}; current=''; sample_id=''
allowed={config.GIGACHAT_OAUTH_URL,config.GIGACHAT_API_URL,config.GIGACHAT_V3_API_URL,'https://api.anthropic.com/v1/messages'}
def bounded_post(url, **kw):
    assert url in allowed
    oauth = url == config.GIGACHAT_OAUTH_URL
    if not oauth:
        assert counts[current] < 10, 'Generation call ceiling'
        counts[current] += 1
        payload=kw['json']
        if current == 'claude':
            assert payload['model'] in ('claude-haiku-4-5-20251001','claude-sonnet-5')
            rate = 2 if payload['model'] == 'claude-sonnet-5' else 1
            upper = (len(json.dumps(payload['messages'],ensure_ascii=False).encode()) + 512)/1e6*rate + payload['max_tokens']*5*rate/1e6
            assert estimates['claude'] + upper <= 0.9, 'Claude $1 upper budget reached'
            estimates['claude'] += upper
        time.sleep(2)
    started=time.monotonic(); r=post(url, **kw)
    if not oauth:
        try: data=r.json()
        except ValueError: data={}
        records.append({'provider':current,'id':sample_id,'http_status':r.status_code,
          'seconds':round(time.monotonic()-started,2), 'model':data.get('model'),
          'usage':data.get('usage'), 'finish_reason': (data.get('choices') or [{}])[0].get('finish_reason'),
          'answer': ''.join(b.get('text','') for b in (data.get('content') or []) if isinstance(b,dict)) if current=='claude' else ((data.get('choices') or [{}])[0].get('message') or {}).get('content'),
          'error_type':(data.get('error') or {}).get('type') if isinstance(data.get('error'),dict) else None})
    return r
llm.requests.post=bounded_post
results=[]
for provider in os.environ.get('BENCHMARK_PROVIDERS','gigachat,claude').split(','):
    current=provider; config.LLM_PROVIDER=provider
    folder=OUT/provider;folder.mkdir(exist_ok=True)
    config.ACT_SUMMARIES_PATH=str(folder/'cache.json')
    config.ACT_SUMMARY_PENDING_PATH=str(folder/'pending.json')
    config.LLM_PROVIDER_STATE_PATH=str(folder/'provider.json')
    config.LLM_SUMMARY_PROVIDER_FALLBACK=False
    if not llm._provider_key(provider):
        results.append({'provider':provider,'status':'missing_key'});continue
    for sample in samples:
        sample_id=sample['id']; prepared=prepare_act(sample['text'],sample['meta'])
        summary=summary_queue.summarize_tracked(sample['text'],case_meta=sample['meta'])
        job=next(j for j in summary_queue._load().values() if j['source_hash']==prepared.audit['source_hash'])
        results.append({'provider':provider,'configured_model':llm._summary_model(provider),
          'id':sample_id,'case':sample['case'],'source_hash':prepared.audit['source_hash'],
          'summary':summary,'status':job['status'],'actual_model':job.get('model'),
          'attempts':job.get('attempts'),'refusal_rechecked':job.get('refusal_rechecked')})
        (OUT/'reserve-results.json').write_text(json.dumps({'results':results,'calls':counts,'requests':records,'claude_upper_cost_usd':estimates['claude']},ensure_ascii=False,indent=2))
        print(provider, sample_id, job['status'], flush=True)
(OUT/'reserve-results.json').write_text(json.dumps({'results':results,'calls':counts,'requests':records,'claude_upper_cost_usd':estimates['claude']},ensure_ascii=False,indent=2))
