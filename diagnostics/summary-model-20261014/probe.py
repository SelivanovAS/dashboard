import sys,json,os,tempfile,time,logging
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path('scripts').resolve()))
from court_monitor import config
from court_monitor.digest import llm
logging.disable(logging.CRITICAL)
# По умолчанию только вымышленный пример, без данных реальных дел.
samples=[{'id':'synthetic_paid_debt','act':
 'ВЫМЫШЛЕННЫЙ УЧЕБНЫЙ ПРИМЕР. АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ. Дело № 33-700/2026. '
 'Банк просил взыскать долг. Заёмщик ссылался на отсутствие задолженности. '
 'Коллегия проверила расчёт и платёжные документы. Платежи полностью погасили долг. '
 'Определила: решение отменить, в удовлетворении иска банка отказать.',
 'meta':{'stage':'appeal','case_number':'33-700/2026','court_domain':'example.invalid'}}]
if os.environ.get('USE_SAVED_ACTS') == 'true':
 samples=json.loads(Path('diagnostics/summary-model-20261014/samples.json').read_text())
results=[]
with tempfile.TemporaryDirectory() as temp:
 config.LLM_PROVIDER_STATE_PATH=temp+'/providers.json'
 for model in ['google/gemma-4-26b-a4b-it:free','nvidia/nemotron-3-ultra-550b-a55b:free']:
  failures=0
  for s in samples:
   prompt=llm._build_act_summary_prompt(s['act'],s['meta']);fits,budget=llm._fits_context(prompt,'openrouter',model)
   if not fits:results.append({'model':model,'sample':s['id'],'status':'context_exceeded','budget':budget});continue
   t=time.monotonic();raw=llm._call_openrouter_simple(prompt,model=model)
   text,status=llm._response_status(raw,s['act'],s['meta'].get('verdict_label',''))
   failure=llm._summary_call_failure.get()
   results.append({'requested_model':model,'actual_model':getattr(raw,'model',None),'sample':s['id'],'status':failure or status,'summary':text or str(raw or ''),'seconds':round(time.monotonic()-t,3),'budget':budget})
   failures=failures+1 if failure else 0
   if failures>=2:break
Path(os.environ['RUNNER_TEMP'],'summary-probe.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
print(json.dumps([{k:v for k,v in r.items() if k not in ['summary','budget']} for r in results],ensure_ascii=False))
