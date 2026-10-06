"""Регрессии потери середины, смысловых отказов, резервов и независимой очереди."""
from copy import deepcopy
import json
from datetime import datetime, timedelta, timezone

import pytest
from court_monitor import config, act_publication, runs
from court_monitor.act_preparation import prepare_act, source_hash
from court_monitor.digest import llm, summary_queue as queue
from court_monitor.parsing.act_markup import act_page_text, document_div_text

ACT = ('АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ\nДело № 33-700/2026\n'
       'Банк просил взыскать долг. Заёмщик ссылался на отсутствие задолженности. '
       'Коллегия проверила расчёт и платёжные документы. Платежи полностью погасили долг. '
       'Определила: решение отменить, в удовлетворении иска банка отказать.')
GOOD = 'Долг полностью погашен, что подтверждено платёжными документами. Оснований для взыскания нет.'
META = {'stage': 'appeal', 'case_number': '33-700/2026', 'court_domain': 'example.sudrf.ru'}


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setattr(config, 'LLM_PROVIDER', 'openrouter')
    monkeypatch.setattr(config, 'ACT_SUMMARIES_PATH', str(tmp_path/'cache.json'))
    for key in ('OPENROUTER_API_KEY', 'GIGACHAT_AUTH_KEY', 'ANTHROPIC_API_KEY'):
        monkeypatch.setattr(config, key, 'test')
    for key in config.METRICS:
        if key.startswith('llm_'):
            monkeypatch.setitem(config.METRICS, key, 0)
    monkeypatch.setattr(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', True)
    monkeypatch.setattr(llm.time, 'sleep', lambda _: None)
    return monkeypatch


def test_full_source_and_same_prompt_through_all_providers(setup):
    text = ACT.replace('Коллегия проверила', 'Описание истории. '*2500 + '\nУНИКАЛЬНЫЙ МОТИВ В СЕРЕДИНЕ\nКоллегия проверила')
    calls = []
    setup.setattr(llm, '_call_openrouter_simple', lambda p, **kw: calls.append(('or', p)))
    setup.setattr(llm, '_call_gigachat_simple', lambda p: calls.append(('giga', p)))
    setup.setattr(llm, '_call_claude_simple', lambda p: calls.append(('claude', p)) or llm._ModelText(GOOD, 'actual-haiku'))
    # Установленная модель GigaChat-2 вмещает этот полный документ.
    setup.setattr(config, 'GIGACHAT_MODEL', 'GigaChat-2')
    meta = deepcopy(META)
    assert llm.summarize_act_motivation(text, case_meta=meta) == GOOD
    assert [c[0] for c in calls] == ['or']*3 + ['giga', 'claude']
    assert len({c[1] for c in calls}) == 1
    assert 'УНИКАЛЬНЫЙ МОТИВ В СЕРЕДИНЕ' in calls[0][1]
    assert prepare_act(text, META).text in calls[0][1]
    assert meta['_summary_result']['model'] == 'claude:actual-haiku'
    cached = json.load(open(config.ACT_SUMMARIES_PATH))
    assert next(iter(cached.values()))['model'] == 'claude:actual-haiku'


def test_first_success_stops_chain(setup):
    setup.setattr(llm, '_call_openrouter_simple', lambda p, **kw: GOOD)
    setup.setattr(llm, '_call_gigachat_simple', lambda p: pytest.fail('Giga не нужна'))
    setup.setattr(llm, '_call_claude_simple', lambda p: pytest.fail('Claude не нужен'))
    assert llm.summarize_act_motivation(ACT, case_meta=deepcopy(META), use_cache=False) == GOOD
    assert config.METRICS['llm_summary_calls'] == 1


def test_missing_primary_key_still_uses_giga(setup):
    setup.setattr(config, 'OPENROUTER_API_KEY', '')
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: pytest.fail('Нет ключа'))
    setup.setattr(llm, '_call_gigachat_simple', lambda p: GOOD)
    assert llm.summarize_act_motivation(ACT, case_meta=deepcopy(META), use_cache=False) == GOOD
    assert config.METRICS['llm_summary_calls'] == 1
    assert config.METRICS['llm_summary_failed'] == 0


@pytest.mark.parametrize('refusal', ['НЕДОСТАТОЧНО_ТЕКСТА', 'Недостаточно текста акта для достоверного пересказа мотивов суда.'])
def test_refusal_has_one_recheck_and_no_provider_shopping(setup, refusal):
    prompts = []
    setup.setattr(llm, '_call_openrouter_simple', lambda p, **kw: prompts.append(p) or refusal)
    setup.setattr(llm, '_call_gigachat_simple', lambda p: pytest.fail('Не ищем согласную модель'))
    setup.setattr(llm, '_call_claude_simple', lambda p: pytest.fail('Не ищем согласную модель'))
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) is None
    assert len(prompts) == 2 and 'Дополнительная проверка' not in prompts[0] and 'Дополнительная проверка' in prompts[1]
    assert ACT in prompts[0] and ACT in prompts[1]
    assert not __import__('os').path.exists(config.ACT_SUMMARIES_PATH)
    job = next(iter(queue._load().values()))
    assert job['status'] == 'refused' and job['refusal_rechecked']
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) is None
    assert len(prompts) == 2


def test_unnecessary_refusal_can_be_recovered(setup):
    answers = iter(['НЕДОСТАТОЧНО_ТЕКСТА', GOOD])
    setup.setattr(llm, '_call_openrouter_simple', lambda p, **kw: next(answers))
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) == GOOD
    assert config.METRICS['llm_summary_calls'] == 2


@pytest.mark.parametrize('fragment', [ACT[:100], ACT[:180], 'Истец просил взыскать долг. '*10, 'Суд первой инстанции удовлетворил требования. '*10])
def test_incomplete_sources_never_spend_calls(setup, fragment):
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: pytest.fail('Нет готового акта'))
    assert llm.summarize_act_motivation(fragment, case_meta=deepcopy(META), use_cache=False) is None
    assert config.METRICS['llm_summary_calls'] == 0


def test_context_guard_preserves_source_and_skips_only_small_provider(setup):
    setup.setattr(config, 'SUMMARY_CONTEXT_TOKENS', {'openrouter': 100, 'gigachat': 128000, 'claude': 200000})
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: pytest.fail('Документ не вмещается'))
    prompts = []
    setup.setattr(llm, '_call_gigachat_simple', lambda p: prompts.append(p) or GOOD)
    meta = deepcopy(META)
    assert llm.summarize_act_motivation(ACT, case_meta=meta, use_cache=False) == GOOD
    assert ACT in prompts[0]
    assert meta['_summary_result']['attempts'][0]['status'] == 'context_exceeded'


def test_all_contexts_too_small_pending_without_network(setup):
    setup.setattr(config, 'SUMMARY_CONTEXT_TOKENS', dict.fromkeys(('openrouter', 'gigachat', 'claude'), 100))
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) is None
    assert config.METRICS['llm_summary_calls'] == 0
    assert next(iter(queue._load().values()))['status'] == 'context_exceeded'


def test_long_answer_is_not_cut_at_abbreviation_or_partial_reversal(setup):
    answer = 'Оснований для взыскания нет, т.е. задолженность отсутствует. ' * 12 + 'Отменено только взыскание расходов.'
    assert llm._clean_summary(answer) == ''
    assert llm._response_status(answer, ACT, '')[1] == 'answer_too_long'
    short = 'Долг погашен, т.е. обязательство исполнено. Решение отменено только в части расходов.'
    assert llm._clean_summary(short) == short


def test_queue_survives_new_run_updates_card_without_new_event(setup):
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: None)
    setup.setattr(llm, '_call_gigachat_simple', lambda *a, **k: None)
    setup.setattr(llm, '_call_claude_simple', lambda *a, **k: None)
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) is None
    jobs = queue._load()
    for j in jobs.values():
        j['retry_after'] = (datetime.now(timezone.utc)-timedelta(days=1)).isoformat()
    queue._save(jobs)
    case = {'id': '2-1/2026', 'events': [{'type': 'old'}], 'appeal': {**META, 'act_text': ACT}}
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: GOOD)
    queue.retry_pending([case])
    assert queue.attach_ready([case]) == 1
    assert queue.attach_ready([case]) == 0
    assert case['events'] == [{'type': 'old'}]
    assert case['appeal']['act_analysis']['source'] == 'llm_summary'
    assert case['appeal']['act_analysis']['source_hash'] == source_hash(ACT)


def test_no_attach_to_namesake_or_new_document(setup):
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: GOOD)
    queue.summarize_tracked(ACT, case_meta=deepcopy(META))
    namesake = {'appeal': {**META, 'court_domain': 'another.sudrf.ru', 'act_text': ACT}}
    changed = {'appeal': {**META, 'act_text': ACT + 'Дополнение'}}
    assert queue.attach_ready([namesake, changed]) == 0
    assert all('act_analysis' not in c['appeal'] for c in (namesake, changed))


def test_refetched_full_source_releases_waiting_job(setup):
    fragment = ACT[:170]
    queue.summarize_tracked(fragment, case_meta=deepcopy(META))
    case = {'appeal': {**META, 'act_text': ACT}}
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: GOOD)
    queue.retry_pending([case])
    assert queue.attach_ready([case]) == 1
    assert {j['status'] for j in queue._load().values()} == {'ready', 'superseded'}


def test_daily_quota_survives_process_and_does_not_block_giga(setup):
    llm._remember_daily_quota()
    setup.setattr(llm, '_openrouter_daily_exhausted', set())
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **k: pytest.fail('Квота исчерпана'))
    setup.setattr(llm, '_call_gigachat_simple', lambda p: GOOD)
    assert llm.summarize_act_motivation(ACT, case_meta=deepcopy(META), use_cache=False) == GOOD
    assert config.METRICS['llm_summary_calls'] == 1


def test_markup_keeps_paragraphs_numbers_redactions_and_tables():
    html = '<body><nav>Меню</nav><div id="cont_doc1"><p>Суд не согласился.</p><p>&lt;данные изъяты&gt; — 1 234,50 руб.</p><table><tr><td>Долг</td><td>0</td></tr></table><script>noise()</script></div><footer>Чужой акт</footer></body>'
    text = act_page_text(html)
    assert 'Суд не согласился.\n' in text and '<данные изъяты> — 1 234,50 руб.' in text
    assert 'Долг | 0' in text
    assert all(x not in text for x in ('Меню', 'Чужой акт', 'noise'))


@pytest.mark.parametrize('meta,status', [({**META, 'case_number': '33-999/2026'}, 'case_mismatch'), ({**META, 'source_url': 'https://wrong.sudrf.ru/act'}, 'court_mismatch'), ({**META, 'stage': 'first_instance'}, 'stage_mismatch')])
def test_wrong_identity_does_not_reach_model(meta, status):
    assert prepare_act(ACT, meta).status == status


def test_legacy_8000_chars_can_be_confirmed_without_hash_change(setup):
    text = ACT.replace('Коллегия', 'А'*(8000-len(ACT)) + 'Коллегия', 1)
    assert len(text) == 8000
    assert queue.summarize_tracked(text, case_meta=deepcopy(META)) is None
    case = {'appeal': {**META, 'act_text': text}}
    queue.attach_ready([case])
    assert case['appeal']['act_summary_needs_source']
    act_publication.observe(case['appeal'], text, datetime.now().date(), present=True,
                            source_url='https://example.sudrf.ru/act')
    assert not case['appeal']['act_summary_needs_source']
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **kw: GOOD)
    queue.retry_pending([case])
    assert queue.attach_ready([case]) == 1
    assert {j['status'] for j in queue._load().values()} == {'ready'}


def test_cassation_internal_number_and_document_number_are_aliases():
    text = ACT.replace('АПЕЛЛЯЦИОННОЕ', 'КАССАЦИОННОЕ').replace('33-700/2026', '88-700/2026')
    meta = dict(META, stage='cassation', case_number='8Г-799/2026', cassation_number='88-700/2026')
    assert prepare_act(text, meta).status == 'ready'
    assert prepare_act(text, dict(meta, cassation_number='88-999/2026')).status == 'case_mismatch'
    text = '86RS0001-01-2026-000001-01\n' + text
    assert prepare_act(text, dict(meta, judicial_uid='86RS0001-01-2026-000002-01')).status == 'uid_mismatch'


def test_cached_procedural_action_of_wrong_court_is_rejected(setup):
    text = ACT.replace('Определила: решение отменить, в удовлетворении иска банка отказать.',
                       'Определила: определение районного суда оставить без изменения, жалобу без удовлетворения.')
    wrong = 'Кассационная инстанция оставила заявление без рассмотрения из-за наличия спора о праве.'
    right = 'Кассационная инстанция согласилась с оставлением заявления без рассмотрения из-за наличия спора о праве.'
    assert llm._response_status(wrong, text, '')[1] == 'outcome_conflict'
    assert llm._response_status(right, text, '')[1] == 'ready'
    job = {'text': text, 'meta': META, 'status': 'ready', 'summary': wrong,
           'source_hash': source_hash(text), 'updated_at': '2026-10-06', 'runs': 1}
    queue._save({queue._key(text, META): job})
    case = {'appeal': {**META, 'act_text': text}}
    assert queue.attach_ready([case]) == 0
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **kw: right)
    queue.retry_pending([case])
    assert queue.attach_ready([case]) == 1
    assert right in case['appeal']['act_analysis']['html']


def test_run_queue_preserves_bank_events_and_history_in_cold_archive(setup, tmp_path):
    from court_monitor.storage import save_bank_json, load_bank_json, save_json, load_json
    paths = {'JSON_PATH': 'cases.json', 'JSON_ARCHIVE_PATH': 'cases_archive.json',
             'JSON_BANK_PATH': 'cases_bank.json', 'JSON_BANK_EVENTS_PATH': 'cases_bank_events.json',
             'JSON_BANK_ARCHIVE_PATH': 'cases_bank_archive.json',
             'JSON_BANK_ARCHIVE_EVENTS_PATH': 'cases_bank_archive_events.json'}
    for field, filename in paths.items():
        setup.setattr(config, field, str(tmp_path/filename))
    events = [{'date':'2026-01-01', 'text':'Событие до пересказа'}]
    bank = {'version': 1, 'track': 'plaintiff_light', 'cases': [
        {'id':'2-1/2026', 'first_instance':{'events':events, 'court_domain':'example.sudrf.ru'},
         'appeal': {**META, 'act_text': ACT}}]}
    save_bank_json(bank, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    other_text = ACT.replace('33-700/2026', '33-701/2026')
    other_meta = dict(META, case_number='33-701/2026')
    cold = {'version':1, 'cases':[{'id':'2-2/2026', 'current_stage':'first_instance',
              'history':[{'appeal':{**other_meta, 'act_text':other_text},
                          'first_instance':{'events':events}}]}]}
    cold_path = config.bank_cold_archive_path(2025)
    save_json(cold, cold_path)
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **kw: GOOD)
    queue.summarize_tracked(ACT, case_meta=deepcopy(META))
    queue.summarize_tracked(other_text, case_meta=other_meta)
    setup.setattr(runs, 'send_telegram', lambda *a, **kw: pytest.fail('Новая рассылка'))
    setup.setattr(runs, 'send_web_push', lambda *a, **kw: pytest.fail('Новый push'))
    runs._process_act_summary_queue(retry=True)
    result = load_bank_json(config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    assert result['track'] == 'plaintiff_light'
    assert result['cases'][0]['first_instance']['events'] == events
    assert result['cases'][0]['appeal']['act_analysis']['source'] == 'llm_summary'
    archived = load_json(cold_path)['cases'][0]
    assert archived['current_stage'] == 'first_instance'
    assert archived['history'][0]['first_instance']['events'] == events
    assert archived['history'][0]['appeal']['act_analysis']['source'] == 'llm_summary'


@pytest.mark.parametrize('payload,http_status', [(None,200), ({'access_token':None},200),
    ({'code':6,'message':'credentials does not match'},401)])
def test_giga_oauth_failure_still_reaches_claude(setup, payload, http_status):
    import requests
    setup.setattr(llm, '_call_openrouter_simple', lambda *a, **kw: None)
    calls=[]
    def post(url, **kw):
        calls.append(url)
        response=requests.Response()
        response.status_code=http_status if url==config.GIGACHAT_OAUTH_URL else 200
        data=payload if url==config.GIGACHAT_OAUTH_URL else {
            'content':[{'type':'text','text':GOOD}], 'model':'actual-haiku', 'stop_reason':'end_turn'}
        response._content=json.dumps(data).encode()
        return response
    setup.setattr(llm.requests,'post',post)
    meta=deepcopy(META)
    assert llm.summarize_act_motivation(ACT,case_meta=meta,use_cache=False)==GOOD
    assert calls==[config.GIGACHAT_OAUTH_URL,'https://api.anthropic.com/v1/messages']
    assert meta['_summary_result']['model']=='claude:actual-haiku'


@pytest.mark.parametrize('action',['отказал в восстановлении срока','отклонил требования истца',
                                   'прекратил производство по делу'])
def test_upholding_is_not_first_instance_procedural_action(action):
    source=ACT.rsplit('Определила:',1)[0]+'Определила: определение районного суда оставить без изменения, жалобу без удовлетворения.'
    assert not act_publication.summary_agrees('Кассационный суд '+action+'.',source,'')
    assert act_publication.summary_agrees('Суд первой инстанции '+action+'. Кассация согласилась с его выводами.',source,'')


def test_result_check_uses_last_spaced_heading_not_quoted_old_judgment():
    text = ('Районный суд решил: исковые требования удовлетворить. '
            'Апелляция проверила новые документы. Платежи полностью погасили долг. '
            'О П Р Е Д Е Л И Л А: решение отменить, в удовлетворении иска отказать.')
    assert act_publication.summary_source(text)
    assert not act_publication.summary_agrees('Исковые требования удовлетворены.', text, '')
    assert act_publication.summary_agrees(GOOD, text, '')


@pytest.mark.parametrize('answer', [
    'Как и любая языковая модель, GigaChat не обладает собственным мнением. Разговоры на некоторые темы временно ограничены.',
    'К сожалению, иногда генеративные языковые модели могут создавать некорректные ответы, основанные на открытых источниках. Во избежание неправильного толкования, ответы на вопросы, связанные с чувствительными темами, временно ограничены. Благодарим за понимание.',
    'Генеративные языковые модели не обладают собственным мнением — их ответы являются обобщением информации, находящейся в открытом доступе. Чтобы избежать ошибок и неправильного толкования, разговоры на чувствительные темы могут быть ограничены.',
])
def test_provider_topic_refusal_uses_reserve_without_rechecking_source(setup, answer):
    setup.setattr(config, 'LLM_PROVIDER', 'gigachat')
    calls=[]
    setup.setattr(llm, '_call_gigachat_simple', lambda *a, **kw: calls.append(1) or answer)
    setup.setattr(llm, '_call_claude_simple', lambda *a, **kw: GOOD)
    assert queue.summarize_tracked(ACT,case_meta=deepcopy(META)) == GOOD
    assert queue.summarize_tracked(ACT,case_meta=deepcopy(META)) == GOOD
    assert calls==[1]
    job=next(iter(queue._load().values()))
    assert job['status']=='ready' and job['attempts'][0]['status']=='provider_refusal'
    assert not job['refusal_rechecked']
    assert all(row['summary'] == GOOD for row in llm._load_act_summaries().values())


def test_ultra_topic_refusal_is_not_cached_without_reserve(setup):
    setup.setattr(config, 'LLM_PROVIDER', 'gigachat')
    setup.setattr(config, 'GIGACHAT_MODEL', 'GigaChat-3-Ultra')
    setup.setattr(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', False)
    answer = ('К сожалению, иногда генеративные языковые модели могут создавать некорректные ответы, '
              'основанные на открытых источниках. Во избежание неправильного толкования, ответы на вопросы, '
              'связанные с чувствительными темами, временно ограничены. Благодарим за понимание.')
    calls = []
    setup.setattr(llm, '_call_gigachat_simple', lambda *a, **kw: calls.append(1) or answer)
    setup.setattr(llm, '_call_claude_simple', lambda *a, **kw: pytest.fail('Резерв выключен'))
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) is None
    assert calls == [1]
    job = next(iter(queue._load().values()))
    assert job['status'] == 'provider_refusal'
    assert not job['refusal_rechecked']
    assert not llm._load_act_summaries()


@pytest.mark.parametrize('answer', ['', 'Поставщик ограничил обработку этого запроса.'])
@pytest.mark.parametrize('fallback', [False, True])
def test_giga_blacklist_response_never_becomes_summary(setup, answer, fallback):
    import requests
    setup.setattr(config, 'LLM_PROVIDER', 'gigachat')
    setup.setattr(config, 'GIGACHAT_MODEL', 'GigaChat-2-Pro')
    setup.setattr(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', fallback)
    setup.setattr(llm, '_gigachat_access_token', lambda: 'test-token')
    calls = []
    def post(*args, **kw):
        calls.append(1)
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({'model': 'GigaChat-2-Pro:test', 'choices': [
            {'message': {'content': answer}, 'finish_reason': 'blacklist'}
        ]}).encode()
        return response
    setup.setattr(llm.requests, 'post', post)
    def claude(*args, **kw):
        assert fallback, 'Резерв выключен'
        return GOOD
    setup.setattr(llm, '_call_claude_simple', claude)
    assert queue.summarize_tracked(ACT, case_meta=deepcopy(META)) == (GOOD if fallback else None)
    assert calls == [1]
    job = next(iter(queue._load().values()))
    assert job['status'] == ('ready' if fallback else 'provider_refusal')
    assert job['attempts'][0]['status'] == 'provider_refusal'
    assert job['attempts'][0]['model'] == 'GigaChat-2-Pro:test'
    assert not job['refusal_rechecked']
    cache = llm._load_act_summaries()
    assert all(row['summary'] == GOOD for row in cache.values())
    assert bool(cache) == fallback


def test_giga_oauth_cache_expires_and_is_isolated_by_credentials(setup):
    import requests
    calls=[]; now=[1000.0]
    setup.setattr(llm.time, 'time', lambda: now[0])
    def post(*args, **kw):
        calls.append(1)
        r=requests.Response();r.status_code=200
        r._content=json.dumps({'access_token':f'token-{len(calls)}','expires_at':(now[0]+1800)*1000}).encode()
        return r
    setup.setattr(llm.requests,'post',post)
    assert llm._gigachat_access_token()=='token-1'
    assert llm._gigachat_access_token()=='token-1'
    now[0]+=1741
    assert llm._gigachat_access_token()=='token-2'
    setup.setattr(config,'GIGACHAT_AUTH_KEY','another-key')
    assert llm._gigachat_access_token()=='token-3'
