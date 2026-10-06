"""Одна загрузка, несколько попыток: сумма приёма и актуальный остаток."""
from scripts.tests.test_import_queue_api import run_worker


def test_retries_accumulate_once_without_summing_errors_or_already():
    run_worker(r'''
const kv = kvStore(), env = environment(kv), job = record(1, 1000), key = kv.add(job);
const first = {attempt_id:'a', attempt_started_at:'2026-09-22T05:15:00Z'};
const second = {attempt_id:'b', attempt_started_at:'2026-09-22T05:20:00Z'};
await postResult(env, job, {...first, status:'started'});
await postResult(env, job, {...first, added_bank:5, fetch_fail:7, rows:25, lines:['added first'], card_fail_reason:'заглушка'});
let r = JSON.parse(kv.data.get(key));
assert.equal(r.totals.added_bank, 5); assert.equal(r.fetch_fail, 7);
assert.equal(kv.metadata.get(key).queue_pending, true);
await postResult(env, job, {...second, status:'started'});
await postResult(env, job, {...second, added_bank:7, already:5, rows:25, lines:['added second'], card_fail_reason:''});
r = JSON.parse(kv.data.get(key));
assert.equal(r.added_bank, 7); assert.equal(r.totals.added_bank, 12);
assert.equal(r.already, 5); assert.equal(r.fetch_fail, 0);
assert.equal(r.attempts.length, 2);
assert.deepEqual(r.attempts[0].lines, ['added first']);
assert.equal(kv.metadata.get(key).queue_pending, false);
assert.equal(JSON.parse(kv.data.get('import:last:'+job.court_domain)).added_bank, 12);
const puts = kv.puts.length, ts = r.updated_at;
await postResult(env, job, {...first, added_bank:5, fetch_fail:7});
await postResult(env, job, {...first, status:'started'});
await postResult(env, job, {...second, added_bank:7});
r = JSON.parse(kv.data.get(key));
assert.equal(kv.puts.length, puts); assert.equal(r.updated_at, ts);
assert.equal(r.totals.added_bank, 12); assert.equal(r.fetch_fail, 0);
output({ok:true});
''')


def test_late_report_keeps_newer_attempt_state_and_freshness():
    run_worker(r'''
const kv = kvStore(), env = environment(kv), job = record(1, 1000), key = kv.add(job);
await postResult(env, job, {attempt_id:'b',attempt_started_at:'2026-09-22T07:00:00Z',added_bank:3,fetch_fail:1,lines:['new']});
const last = JSON.parse(kv.data.get(key)).updated_at;
await postResult(env, job, {attempt_id:'a',attempt_started_at:'2026-09-22T06:00:00Z',added_bank:9,fetch_fail:4,lines:['old']});
const r = JSON.parse(kv.data.get(key));
assert.equal(r.attempt_id,'b'); assert.equal(r.updated_at,last);
assert.equal(r.added_bank,3); assert.equal(r.fetch_fail,1);
assert.equal(r.totals.added_bank,12); assert.deepEqual(r.lines,['new']);
assert.equal(kv.metadata.get(key).queue_pending,true);
assert.equal(kv.data.has('import:last:'+job.court_domain),false);
output({ok:true});
''')


def test_legacy_result_is_retained_as_known_minimum():
    run_worker(r'''
const kv = kvStore(), env = environment(kv), job = record(1,1000,{status:'done',added:2,added_bank:4,fetch_fail:3}), key=kv.add(job);
await postResult(env,job,{attempt_id:'a',status:'started'});
await postResult(env,job,{attempt_id:'a',added_bank:3});
const r=JSON.parse(kv.data.get(key));
assert.equal(r.totals.added,2); assert.equal(r.totals.added_bank,7);
assert.equal(r.attempt_history_incomplete,true); assert.equal(r.attempts.length,2);
output({ok:true});
''')


def test_failed_publication_not_counted_until_same_attempt_is_confirmed():
    run_worker(r'''
const kv = kvStore(), env = environment(kv), job = record(1,1000), key=kv.add(job);
await postResult(env,job,{attempt_id:'a',status:'failed',added:2,error:'push failed',lines:['saved locally']});
let r=JSON.parse(kv.data.get(key));
assert.equal(r.totals.added,0); assert.equal(r.error,'push failed');
await postResult(env,job,{attempt_id:'a',added:2,lines:['published']});
r=JSON.parse(kv.data.get(key));
assert.equal(r.totals.added,2); assert.equal(r.error,undefined);
assert.equal(r.attempts.length,1); assert.equal(r.attempts[0].error,'');
output({ok:true});
''')


def test_case_batches_use_the_same_attempt_contract_and_keep_fetch_error_current():
    run_worker(r'''
const kv = kvStore(), env = environment(kv), job = record(1,1000,{kind:'case'}), key=kv.add(job);
const route={dump_key:null,job_key:'import:case:'+job.uuid};
await postResult(env,job,{...route,attempt_id:'a',attempt_started_at:'2026-09-22T06:00:00Z',added_main:2,fetch_error:1});
await postResult(env,job,{...route,attempt_id:'b',attempt_started_at:'2026-09-22T07:00:00Z',added_main:1,fetch_error:0});
const r=JSON.parse(kv.data.get(key));
assert.equal(r.totals.added_main,3); assert.equal(r.fetch_error,0);
assert.equal(kv.metadata.get(key).queue_pending,false);
output({ok:true});
''')


def test_restore_verified_history_keeps_current_queue_and_rejects_changed_report():
    run_worker(r"""
const kv=kvStore(),env=environment(kv),job=record(1,1000,{status:'done',added_bank:7,fetch_fail:0}),key=kv.add(job);
kv.data.set('import:last:'+job.court_domain,JSON.stringify({ts:job.updated_at,added_bank:7}));
const earlier=new Date(Date.parse(job.updated_at)-20000).toISOString();
const attempt=(id,time,counts)=>({id,started_at:time,finished_at:time,status:'done',counts,lines:['verified log']});
const body={expected_updated_at:job.updated_at,restore_attempts:[attempt('recovered-one',earlier,{added_bank:5,fetch_fail:7}),attempt('recovered-two',job.updated_at,{added_bank:7})]};
let res=await postResult(env,job,{...body,restore_attempts:[{...body.restore_attempts[0],counts:{added_bank:-5}},body.restore_attempts[1]]});
assert.equal(res.status,400);assert.equal(kv.puts.length,0);
res=await postResult(env,job,{...body,expected_updated_at:'wrong'});
assert.equal(res.status,409);assert.equal(kv.puts.length,0);
res=await postResult(env,job,{...body,restore_attempts:[body.restore_attempts[0]]});
assert.equal(res.status,409);assert.equal(kv.puts.length,0);
res=await postResult(env,job,body);assert.equal(res.status,200);
const r=JSON.parse(kv.data.get(key));
assert.equal(r.totals.added_bank,12);assert.equal(r.added_bank,7);assert.equal(r.fetch_fail,0);
assert.equal(r.updated_at,job.updated_at);assert.equal(r.attempts.length,2);
assert.equal(r.attempts[0].counts.fetch_fail,7);assert.ok(r.attempt_history_recovered);
assert.equal(kv.metadata.get(key).queue_pending,false);
assert.equal(JSON.parse(kv.data.get('import:last:'+job.court_domain)).added_bank,12);
res=await postResult(env,job,body);assert.equal(res.status,409);
output({ok:true});
""")
