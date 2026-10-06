from datetime import date, datetime, timedelta
from copy import deepcopy
import pytest
from court_monitor import act_publication as publication, act_watch_policy as policy
from court_monitor import act_watch, appeal_act_watch, config
from court_monitor.parsing.act_markup import document_div_text

DAY = date(2026,10,6)
TEXT = 'АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ 31 марта 2026 года. ' + 'Исследовав доказательства, коллегия установила отсутствие оснований для иска. '*8 + ' Определила: в удовлетворении исковых требований к банку отказать.'

def test_initial_text_is_silent_and_detection_date_is_immutable():
    b={}
    assert not publication.observe(b,TEXT,DAY,present=True)
    assert b['act_notification_kind']=='backfill'
    assert b['act_decision_date']=='31.03.2026'
    assert not publication.observe(b,TEXT,DAY+timedelta(days=1),present=True)
    assert b['act_detected_at']=='2026-10-06'


def test_confirmed_absence_is_required_and_document_link_is_not_absence():
    b={}
    publication.observe(b,'',DAY,present=True)
    assert 'act_absent_checked_at' not in b
    publication.observe(b,'',DAY,present=False)
    assert publication.observe(b,TEXT,DAY+timedelta(days=1),present=True)
    assert not publication.observe(b,TEXT,DAY+timedelta(days=2),present=True)


@pytest.mark.parametrize('age,expected',[(30,'daily'),(31,'weekly'),(90,'weekly'),(91,'monthly'),(180,'monthly'),(181,'expired')])
def test_calendar_boundaries(age,expected):
    b={'decision_date':(DAY-timedelta(days=age)).strftime('%d.%m.%Y'),'last_checked_at':DAY.isoformat()}
    due=policy.next_check(b,DAY,lambda b:b['decision_date'],'court|number')
    if expected=='expired':assert due is None
    elif expected=='daily':assert due==DAY+timedelta(days=1)
    elif expected=='weekly':assert 1 <= (due-DAY).days<=7 and due.weekday()<5
    else:assert due.month==11 and due.weekday()<5


def test_weekly_and_monthly_work_is_spread():
    def dates(age):
        b={'decision_date':(DAY-timedelta(days=age)).strftime('%d.%m.%Y'),'last_checked_at':DAY.isoformat()}
        return {policy.next_check(b,DAY,lambda b:b['decision_date'],str(i)) for i in range(200)}
    assert len(dates(60))==5
    assert len(dates(120))>=18


def test_shared_budget_persists_daily_backfill_and_caps_time():
    now=[0];data={};budget=policy.Budget(data,DAY,clock=lambda:now[0])
    for _ in range(10):budget.finish('backfill',budget.begin('backfill'))
    assert policy.Budget(data,DAY,clock=lambda:now[0]).remaining('backfill')==0
    assert budget.remaining('waiting')==600
    now[0]=600
    assert budget.remaining('waiting')==0
    tomorrow=policy.Budget(data,DAY+timedelta(days=1),clock=lambda:now[0])
    start=tomorrow.begin('backfill');now[0]+=180;tomorrow.finish('backfill',start)
    assert tomorrow.remaining('backfill')==0


def test_retry_interval_and_daily_limit():
    task={};now=datetime(2026,10,6,6)
    policy.attempt(task,now)
    assert policy.retry_reason(task,now+timedelta(minutes=29))=='retry_cooldown'
    assert not policy.retry_reason(task,now+timedelta(minutes=30))
    policy.attempt(task,now+timedelta(minutes=30))
    assert policy.retry_reason(task,now+timedelta(hours=2))=='daily_retry_limit'
    assert not policy.retry_reason(task,now+timedelta(days=1))


def test_complete_text_does_not_include_footer_or_scripts():
    html='<div id="cont_doc1"><p>'+TEXT+'</p><div>Подпись</div><script>secret()</script></div><footer>Чужое решение</footer>'
    parsed=document_div_text(html)
    assert ' '.join(TEXT.split()) in parsed and parsed.endswith('Подпись')
    assert 'secret' not in parsed and 'Чужое' not in parsed


def test_incomplete_argument_is_not_reasoning_and_summary_must_match_final_part():
    assert not publication.summary_source(TEXT[:200])
    assert publication.summary_source(TEXT)
    assert not publication.summary_agrees('Договор признан недействительным.',TEXT,'Отменено с новым решением')
    assert publication.summary_agrees('Оснований для признания договора недействительным нет.',TEXT,'Отменено с новым решением')


def test_dates_never_use_publication_or_hearing_as_decision():
    from court_monitor.digest.template import _act_dates_html
    rendered='\n'.join(_act_dates_html({'act_date':'01.10.2026','hearing_date':'02.10.2026','act_detected_at':'2026-10-06'}))
    assert 'Акт вынесен: дата не установлена' in rendered
    assert 'Текст обнаружен системой: 06.10.2026' in rendered
    assert 'Опубликован судом' not in rendered

@pytest.mark.parametrize('module,stage',[(act_watch,'cassation'),(appeal_act_watch,'appeal')])
def test_legacy_baseline_and_expiry_do_not_emit_or_archive(monkeypatch,tmp_path,module,stage):
    from types import SimpleNamespace
    monkeypatch.setattr(config,'JSON_PATH',str(tmp_path/'cases.json'))
    monkeypatch.setattr(config,'CASSATION_ACTS_PATH',str(tmp_path/'cass'))
    monkeypatch.setattr(config,'DIGESTED_ACTS_PATH',str(tmp_path/'appeal'))
    domain='7kas.sudrf.ru' if stage=='cassation' else 'oblsud--hmao.sudrf.ru'
    number='8Г-1/2026' if stage=='cassation' else '33-1/2026'
    c={'id':'2-1/2026','current_stage':'first_instance',stage:{'case_number':number,'court_domain':domain,'link':'1|uid','status':'Решено','decision_date':'01.10.2026','hearing_date':'01.10.2026'}}
    if stage=='cassation':
        monkeypatch.setattr(module,'parse_cassation_card',lambda *a:{'page_case_number':number,'decision_date':'01.10.2026','act_text':TEXT,'act_published':True})
    else:
        monkeypatch.setattr(module,'APPEAL_COURTS',[SimpleNamespace(domain=domain,base_url='https://'+domain)])
        monkeypatch.setattr(module,'parse_case_card',lambda *a:{'_table_count':6,'Номер дела (карточка)':number,'act_text':TEXT})
    data={};r=module.refresh(data,[c],DAY,lambda *a,**k:'html',persist=lambda d:None)
    assert r['backfilled']==1 and r['published']==0
    assert not any(data.get(k) for k in ('pending_cassation_changes','pending_appeal_act_changes'))
    old=deepcopy(c);old[stage].pop('act_text');old[stage]['decision_date']=old[stage]['hearing_date']='01.01.2026';old[stage].pop('act_decision_date',None)
    r=module.refresh({},[old],DAY,lambda *a,**k:pytest.fail('expired task fetched'),persist=lambda d:None)
    assert r['expired']==1 and old['current_stage']=='first_instance'


def test_daily_quota_stops_free_pool_retries_but_not_transient_errors(monkeypatch):
    import requests
    from court_monitor.digest import llm
    monkeypatch.setattr(config,'OPENROUTER_API_KEY','test-daily-key')
    monkeypatch.setattr(llm,'_openrouter_daily_exhausted',set())
    attempts=[]
    def post(*a,**k):
        attempts.append(1)
        r=requests.Response();r.status_code=429;r._content=b'{"error":{"message":"Rate limit exceeded: free-models-per-day"}}'
        return r
    monkeypatch.setattr(llm.requests,'post',post)
    monkeypatch.setattr(llm.time,'sleep',lambda _:pytest.fail('daily quota must not retry'))
    assert llm._openrouter_summary_attempts('prompt','model:free',3,'')[0]==''
    assert llm._openrouter_summary_attempts('prompt','openrouter/free',2,'')[0]==''
    assert len(attempts)==1
    assert not llm._free_pool_blocked('paid/model')


def test_petrova_dispositive_beats_recounted_lower_court_result(monkeypatch):
    from court_monitor.digest import llm
    monkeypatch.setattr(config,'LLM_PROVIDER','claude')
    monkeypatch.setattr(config,'ANTHROPIC_API_KEY','test')
    text=('АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ 31 марта 2026 года. '
          'Первая инстанция признала договор недействительным. '+ 'Изложение доводов сторон. '*450+
          'Повторная экспертиза подтвердила способность заёмщика понимать значение своих действий. '
          'Оснований для признания договора недействительным не имелось. '
          'Определила: решение отменить, в удовлетворении исковых требований к банку отказать.')
    prompts=[]
    def wrong(prompt):
        prompts.append(prompt)
        return 'Договор признан недействительным. Банк не проверил обстоятельства сделки.'
    monkeypatch.setattr(llm,'_call_claude_simple',wrong)
    assert llm.summarize_act_motivation(text,case_meta={'stage':'appeal'},use_cache=False) is None
    assert 'Повторная экспертиза' in prompts[0] and 'в удовлетворении исковых требований к банку отказать' in prompts[0]
    prompts.clear()
    assert llm.summarize_act_motivation(text[:8000],case_meta={'stage':'appeal'},use_cache=False) is None
    assert not prompts


def test_embedded_word_document_body_is_a_valid_boundary():
    html='<div id="cont_doc1"><html><body>'+TEXT+'</body></html><footer>Портал</footer>'
    assert document_div_text(html)==' '.join(TEXT.split())


def test_interrupted_backfill_cannot_reset_daily_time_budget():
    now=[100];data={};budget=policy.Budget(data,DAY,clock=lambda:now[0])
    budget.begin('backfill')
    now[0]=281
    resumed=policy.Budget(data,DAY,clock=lambda:now[0])
    assert resumed.remaining('backfill')==0
    assert data['act_watch_budget']['backfill_count']==1
