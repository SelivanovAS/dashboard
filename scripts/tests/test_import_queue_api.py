"""Полная очередь импортов: пагинация KV, FIFO, отслеживание и права доступа.

Worker выполняется в Node с KV в памяти: HTTP-запросы и запись данных проекта
не используются. Проверяем реальный обработчик и исполняемый jq-фильтр очереди.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node требуется для проверки Worker")


HARNESS = r"""
const assert = require('node:assert/strict');
console.log = () => {};
const now = Date.now();
function uuid(n) { return '00000000-0000-4000-8000-' + String(n).padStart(12, '0'); }
function record(n, ageSeconds, extra = {}) {
  const ts = new Date(now - ageSeconds * 1000).toISOString();
  return { uuid: uuid(n), ts, updated_at: ts, court_domain: 'court' + n + '--svd.sudrf.ru',
    operator: 'Оператор', status: 'queued', executor: 'vps', ...extra };
}
function kvStore() {
  const data = new Map(), metadata = new Map(), gets = [], lists = [], puts = [];
  const kv = {
    data, metadata, gets, lists, puts,
    add(r, withMetadata = true) {
      const key = 'import:log:' + r.ts + '|' + r.uuid;
      data.set(key, JSON.stringify(r));
      if (withMetadata) metadata.set(key, importLogWriteOptions(r).metadata);
      return key;
    },
    async list(opts) {
      lists.push({...opts});
      const names = [...data.keys()].filter(k => k.startsWith(opts.prefix)).sort();
      const begin = Number(opts.cursor || 0), end = begin + (opts.limit || 1000);
      return { keys: names.slice(begin, end).map(name => ({name, metadata: metadata.get(name)})),
        list_complete: end >= names.length, cursor: end >= names.length ? '' : String(end) };
    },
    async get(key) { gets.push(key); return data.get(key) ?? null; },
    async put(key, value, opts) {
      puts.push({key, value, opts}); data.set(key, value);
      if (opts && opts.metadata) metadata.set(key, opts.metadata);
    },
  };
  return kv;
}
function environment(kv) {
  return { PUSH_SUBSCRIPTIONS: kv, OWNER_SECRET: 'owner', OPERATOR_SECRET: 'operator',
    PUSH_SECRET: 'push', IMPORT_EXECUTOR: 'vps' };
}
async function getLog(env, query = '', secret = 'owner') {
  return workerExport.fetch(new Request('https://worker.invalid/admin/import-log?secret=' + secret
    + '&logonly=1' + query), env);
}
async function postDump(env, body) {
  return workerExport.fetch(new Request('https://worker.invalid/admin/import-dump?secret=operator', {
    method: 'POST', body: JSON.stringify(body),
  }), env);
}
async function postResult(env, job, extra = {}) {
  return workerExport.fetch(new Request('https://worker.invalid/import-result', {
    method: 'POST', headers: {Authorization: 'Bearer push'},
    body: JSON.stringify({dump_key: 'import:dump:' + job.uuid, status: 'done', ...extra}),
  }), env);
}
function dumpHtml(deloId, extra = '') {
  return '<html><a href="/modules.php?name=sud_delo&amp;delo_id=' + deloId
    + '&amp;case_id=123&amp;srv_num=1">дело</a>' + extra + 'x'.repeat(1100) + '</html>';
}
function output(value) { process.stdout.write(JSON.stringify(value)); }
"""


def run_worker(script: str) -> dict:
    source = (ROOT / "cloudflare-worker/worker.js").read_text(encoding="utf-8")
    source = source.replace('import { renderAdminHtml } from "./admin_page.js";', "")
    # Форк сохраняет уже развёрнутую загрузку через шлюз: исполняем её
    # модуль в том же VM, а обычные тесты очереди не обращаются к сети.
    gateway = ROOT / "cloudflare-worker/import_gateway.js"
    if gateway.exists():
        helper = gateway.read_text(encoding="utf-8").replace("export async function", "async function")
        source = source.replace('import { readGatewayImportBody } from "./import_gateway.js";', helper)
    # Этот стенд открывает приватные функции очереди через склейку исходника.
    # Зависимость профилей изолируем, чтобы её внутренние имена не столкнулись
    # с worker.js; жизненный цикл отдельно проверяется настоящим ESM-loader.
    lifecycle = (ROOT / "cloudflare-worker/profile_lifecycle.js").read_text(encoding="utf-8")
    lifecycle_names = "readProfile, recordProfileUse, profileLifecycleFields, cleanupProfiles, PROFILE_CLEANUP_CRON"
    source = source.replace(
        'import { ' + lifecycle_names + ' } from "./profile_lifecycle.js";',
        'const { ' + lifecycle_names + ' } = (() => {\n'
        + lifecycle.replace("export ", "") + '\nreturn { ' + lifecycle_names + ' };\n})();',
    )
    source = source.replace("export default {", "const workerExport = {")
    result = subprocess.run(
        [NODE, "-"], input=source + "\n" + HARNESS + "\n(async () => {\n"
        + script + "\n})().catch(e => { console.error(e); process.exit(1); });",
        text=True, capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def select_queue(payload: dict) -> list[str]:
    jq = shutil.which("jq")
    if jq is None:
        pytest.skip("jq требуется для проверки FIFO исполнителя")
    result = subprocess.run(
        [jq, "-r", "--argjson", "now", str(payload["now"] / 1000),
         "--argjson", "ttl", "259200", "--argjson", "grace", "900",
         "--argjson", "cgrace", "3000", "-f", str(ROOT / "ops/mac-local-run/import_queue.jq")],
        input=json.dumps(payload), text=True, capture_output=True, check=True,
    )
    return [line.split("\t")[1] for line in result.stdout.splitlines()]


def test_all_pending_jobs_survive_history_window_and_kv_pages_in_fifo_order():
    result = run_worker(r"""
const kv = kvStore(), env = environment(kv);
// Первая страница KV целиком занята старой историей.
for (let i = 1; i <= 1103; i++) kv.add(record(i, 30 * 86400 + i, {status: 'done'}));
const expected = [];
for (let i = 1200; i < 1275; i++) {
  const r = record(i, 7200 - i); kv.add(r); expected.push(r.uuid);
}
// Завершённые задания не скрывают очередь и не требуют get по всей истории.
for (let i = 1300; i < 1360; i++) kv.add(record(i, 3600 - i, {status: 'done'}));
const finished = record(1400, 10800, {status: 'done', added: 7}); kv.add(finished);
kv.add(record(1401, 14400, {status: 'done'}), false);
kv.add(record(1402, 14401, {status: 'done'}), false);
const response = await getLog(env, '&include_queue=1&tracked=' + finished.uuid);
assert.equal(response.status, 200);
const body = await response.json();
assert.equal(body.items.length, 50);
assert.equal(body.items[0].uuid, uuid(1359));
assert.deepEqual(body.queue.map(r => r.uuid), expected);
assert.ok(body.queue.every(r => r.queue_pending === true));
assert.equal(body.tracked[0].added, 7);
assert.equal(kv.lists.length, 2);
assert.equal(kv.lists[1].cursor, '1000');
assert.equal(kv.gets.length, 128); // 75 очередь + 50 история + 1 tracked + 2 legacy.
assert.equal(new Set(kv.gets).size, kv.gets.length);
output({...body, queue: body.queue.slice().reverse(), expected, now});
""")
    assert select_queue(result) == result["expected"]


def test_queue_selection_preserves_retry_rules_and_legacy_records():
    result = run_worker(r"""
const kv = kvStore(), env = environment(kv);
const rows = [
  record(1, 10000, {status: 'done', fetch_fail: 2}),
  record(2, 9999, {status: 'done', card_failed: 1}),
  record(3, 9998, {kind: 'case', status: 'done', fetch_error: 3}),
  record(4, 9997, {status: 'failed', error: 'HTTP 503'}),
  record(5, 9996, {status: 'failed', error: 'повтор не поможет — вставьте выдачу заново'}),
  record(6, 9995, {status: 'done'}),
  record(7, 9994, {kind: 'case', status: 'done', fetch_fail: 3}),
  record(8, 9993, {kind: 'writ_waiver', status: 'failed'}),
  record(9, 20, {status: 'started'}),
  record(10, 300, {kind: 'case', status: 'dispatched'}),
  record(11, 300000),
  record(12, 9000),
];
rows.forEach((r, i) => kv.add(r, i % 2 === 0));
const body = await (await getLog(env, '&include_queue=1')).json();
assert.deepEqual(body.queue.map(r => r.uuid), [1,2,3,4,12,10,9].map(uuid));
assert.equal(body.queue.find(r => r.uuid === uuid(1)).status, 'done');
assert.equal(body.queue.find(r => r.uuid === uuid(4)).status, 'failed');
output({...body, now});
""")
    assert select_queue(result) == [f"00000000-0000-4000-8000-{n:012d}" for n in [1, 2, 3, 4, 12]]


def test_default_history_contract_and_empty_queue_are_distinct():
    result = run_worker(r"""
const kv = kvStore(), env = environment(kv);
kv.add(record(1, 60, {status: 'done'}));
const history = await (await getLog(env)).json();
assert.equal(history.items.length, 1);
assert.equal(Object.hasOwn(history, 'queue'), false);
const full = await (await getLog(env, '&include_queue=1')).json();
assert.deepEqual(full.queue, []);
output({...full, now});
""")
    # [] означает очередь действительно пуста; fallback на items допустим
    # только для старого Worker, который не вернул поля queue вовсе.
    result["items"][0]["status"] = "queued"
    assert select_queue(result) == []


@pytest.mark.parametrize("secret,status", [("owner", 200), ("operator", 200), ("", 401), ("wrong", 401), ("push", 401)])
def test_full_queue_keeps_admin_authorization(secret, status):
    result = run_worker(f"""
const kv = kvStore(), env = environment(kv);
const response = await getLog(env, '&include_queue=1&tracked=' + uuid(1), {json.dumps(secret)});
output({{status: response.status, lists: kv.lists.length, gets: kv.gets.length}});
""")
    assert result["status"] == status
    if status == 401:
        assert result["lists"] == result["gets"] == 0


def test_new_submissions_are_distinct_and_store_queue_metadata():
    run_worker(r"""
const kv = kvStore(), env = environment(kv), accepted = [];
for (let i = 0; i < 3; i++) {
  const req = new Request('https://worker.invalid/admin/import-dump?secret=operator', {
    method: 'POST', body: JSON.stringify({court_domain: 'test--svd.sudrf.ru', html: '<html>' + 'x'.repeat(1100) + '</html>'})});
  const response = await workerExport.fetch(req, env);
  assert.equal(response.status, 200);
  const body = await response.json(); assert.equal(body.ok, true); accepted.push(body.key);
}
assert.equal(new Set(accepted).size, 3);
const logs = [...kv.data.keys()].filter(k => k.startsWith('import:log:'));
assert.equal(logs.length, 3);
assert.ok(logs.every(k => kv.metadata.get(k).queue_pending === true));
assert.ok(logs.every(k => JSON.parse(kv.data.get(k)).status === 'queued'));
output({ok: true});
""")


def test_result_updates_job_after_first_thousand_keys_and_refreshes_metadata():
    run_worker(r"""
const kv = kvStore(), env = environment(kv);
for (let i = 1; i <= 1005; i++) kv.add(record(i, 86400 + i, {status: 'done'}));
const job = record(2000, 600), key = kv.add(job);
const response = await workerExport.fetch(new Request('https://worker.invalid/import-result', {
  method: 'POST', headers: {Authorization: 'Bearer push'},
  body: JSON.stringify({dump_key: 'import:dump:' + job.uuid, status: 'done', added: 4, source: 'vps'})
}), env);
assert.equal(response.status, 200);
assert.equal(kv.lists.length, 2);
assert.equal(JSON.parse(kv.data.get(key)).added, 4);
assert.equal(kv.metadata.get(key).queue_pending, false);
assert.equal(JSON.parse(kv.data.get('import:last:' + job.court_domain)).added, 4);
output({ok: true});
""")


@pytest.mark.parametrize("section,delo_id", [
    ("first_instance", "1540005"), ("appeal", "5"), ("cassation", "2800001"),
])
def test_dump_acceptance_records_selected_instance_and_normalizes_alias(section, delo_id):
    run_worker(r"""
const kv = kvStore(), env = environment(kv);
const section = SECTION, deloId = DELO_ID;
// Меню другого раздела не является выдачей. Раздел стоит ПЕРЕД case_id.
const response = await postDump(env, {court_domain: 'oblsud.hmao.sudrf.ru', section,
  delo_id: Number(deloId), html: dumpHtml(deloId, '<a href="?delo_id=777">раздел</a>')});
assert.equal(response.status, 200);
const body = await response.json();
const job = JSON.parse([...kv.data.entries()].find(([k]) => k.endsWith('|' + body.key))[1]);
assert.equal(job.court_domain, 'oblsud--hmao.sudrf.ru');
assert.equal(job.section, section); assert.equal(job.delo_id, deloId);
assert.equal(job.section_key, 'oblsud--hmao.sudrf.ru:' + deloId);
output({ok: true});
""".replace("SECTION", json.dumps(section)).replace("DELO_ID", json.dumps(delo_id)))


@pytest.mark.parametrize("body_fields,card_id,extra", [
    ({"section": "appeal", "delo_id": "5"}, "2800001", ""),
    ({"section": "cassation", "delo_id": "2800001"}, "5", ""),
    ({"section": "appeal", "delo_id": "2800001"}, "2800001", ""),
    ({"section": "unknown"}, "5", ""),
    ({"delo_id": "777"}, "777", ""),
    ({}, "5", '<a href="?case_id=456&delo_id=2800001">другая инстанция</a>'),
    ({}, "5", '<a href="?case_id=456&delo_id=5&delo_id=2800001">смешанная ссылка</a>'),
])
def test_wrong_or_mixed_instance_is_rejected_before_writing_queue(body_fields, card_id, extra):
    run_worker(r"""
const kv = kvStore(), env = environment(kv);
const response = await postDump(env, {court_domain: 'oblsud--hmao.sudrf.ru',
  ...FIELDS, html: dumpHtml(CARD_ID, EXTRA)});
assert.equal(response.status, 400); assert.equal(kv.puts.length, 0);
assert.equal((await response.json()).ok, false);
output({ok: true});
""".replace("FIELDS", json.dumps(body_fields)).replace("CARD_ID", json.dumps(card_id))
       .replace("EXTRA", json.dumps(extra)))


def test_legacy_form_infers_only_the_card_instance_and_empty_dump_keeps_explicit_selection():
    run_worker(r"""
const kv = kvStore(), env = environment(kv), domain = 'oblsud--hmao.sudrf.ru';
const inferred = await postDump(env, {court_domain: domain, html: dumpHtml('2800001')});
assert.equal(inferred.status, 200);
const inferredId = (await inferred.json()).key;
const empty = await postDump(env, {court_domain: domain, delo_id: '5', section: 'appeal',
  html: '<html>' + 'ничего не найдено '.repeat(100) + '</html>'});
assert.equal(empty.status, 200);
const jobs = [...kv.data.entries()].filter(([k]) => k.startsWith('import:log:')).map(([,v]) => JSON.parse(v));
assert.equal(jobs.find(r => r.uuid === inferredId).section, 'cassation');
assert.equal(jobs.find(r => r.uuid !== inferredId).section_key, domain + ':5');
output({ok: true});
""")


def test_github_fallback_preserves_the_selected_instance_in_workflow_inputs():
    run_worker(r"""
const kv = kvStore(), env = {...environment(kv), IMPORT_EXECUTOR: 'github', GITHUB_PAT: 'test-only'};
const sent = [];
globalThis.fetch = async (url, opts) => { sent.push(JSON.parse(opts.body)); return new Response(null, {status: 204}); };
const response = await postDump(env, {court_domain: 'oblsud--hmao.sudrf.ru',
  section: 'cassation', delo_id: '2800001', html: dumpHtml('2800001')});
assert.equal(response.status, 200); assert.equal(sent.length, 1);
assert.equal(sent[0].inputs.section, 'cassation');
assert.equal(sent[0].inputs.delo_id, '2800001');
assert.equal(kv.data.has('import:pending'), false);
output({ok: true});
""")


def test_results_keep_appeal_and_presidium_freshness_separate_for_same_domain():
    run_worker(r"""
const kv = kvStore(), env = environment(kv), domain = 'oblsud--hmao.sudrf.ru';
for (const [i, section, deloId] of [[1, 'appeal', '5'], [2, 'cassation', '2800001']]) {
  const job = record(i, 600, {court_domain: domain, section, delo_id: deloId}); kv.add(job);
  assert.equal((await postResult(env, job, {section, delo_id: deloId, added: i})).status, 200);
  assert.equal(JSON.parse(kv.data.get('import:last:' + domain + ':' + deloId)).added, i);
}
assert.equal(kv.data.has('import:last:' + domain), false);
const response = await workerExport.fetch(new Request('https://worker.invalid/admin/import-log?secret=owner'), env);
const body = await response.json();
assert.equal(body.last_sections[domain + ':5'].added, 1);
assert.equal(body.last_sections[domain + ':2800001'].added, 2);
assert.deepEqual(body.last, {});
output({ok: true});
""")


@pytest.mark.parametrize("extra", [
    {"fetch_fail": 1}, {"card_failed": 1}, {"needs_review": 1}, {"status": "failed"},
])
def test_incomplete_dump_never_refreshes_either_instance(extra):
    run_worker(r"""
const kv = kvStore(), env = environment(kv), domain = 'oblsud--hmao.sudrf.ru';
const job = record(1, 600, {court_domain: domain, section: 'appeal', delo_id: '5'}); kv.add(job);
assert.equal((await postResult(env, job, EXTRA)).status, 200);
assert.equal([...kv.data.keys()].some(k => k.startsWith('import:last:')), false);
const body = await (await workerExport.fetch(new Request('https://worker.invalid/admin/import-log?secret=owner'), env)).json();
assert.deepEqual(body.last_sections, {});
output({ok: true});
""".replace("EXTRA", json.dumps(extra)))


@pytest.mark.parametrize("kind,key_prefix", [("case", "case"), ("writ_waiver", "writ")])
def test_non_dump_results_never_refresh_freshness_even_with_court_and_section(kind, key_prefix):
    run_worker(r"""
const kv = kvStore(), env = environment(kv), domain = 'oblsud--hmao.sudrf.ru';
const job = record(1, 600, {court_domain: domain, kind: KIND, section: 'appeal', delo_id: '5'});
const logKey = kv.add(job);
const response = await postResult(env, job, {dump_key: null,
  job_key: 'import:' + PREFIX + ':' + job.uuid, status: 'done', section: 'appeal',
  delo_id: '5', waived: 1, added: 1});
assert.equal(response.status, 200);
assert.equal(JSON.parse(kv.data.get(logKey)).status, 'done');
assert.equal([...kv.data.keys()].some(k => k.startsWith('import:last:')), false);
const body = await (await workerExport.fetch(new Request('https://worker.invalid/admin/import-log?secret=owner'), env)).json();
assert.deepEqual(body.last_sections, {}); assert.deepEqual(body.last, {});
output({ok: true});
""".replace("KIND", json.dumps(kind)).replace("PREFIX", json.dumps(key_prefix)))


def test_result_cannot_change_selected_instance_but_failed_report_remains_visible():
    run_worker(r"""
const kv = kvStore(), env = environment(kv);
const job = record(1, 600, {court_domain: 'oblsud--hmao.sudrf.ru', section: 'appeal', delo_id: '5'});
const key = kv.add(job), before = kv.data.get(key);
assert.equal((await postResult(env, job, {section: 'cassation', delo_id: '2800001'})).status, 409);
assert.equal(kv.data.get(key), before); assert.equal(kv.puts.length, 0);
assert.equal((await postResult(env, job, {status: 'failed', section: 'cassation', error: 'Неверный раздел'})).status, 200);
const failed = JSON.parse(kv.data.get(key));
assert.equal(failed.section, 'appeal'); assert.equal(failed.delo_id, '5');
assert.equal(failed.status, 'failed');
assert.equal([...kv.data.keys()].some(k => k.startsWith('import:last:')), false);
output({ok: true});
""")


def test_legacy_results_and_history_recover_only_proven_instance():
    run_worker(r"""
const kv = kvStore(), env = environment(kv), domain = 'oblsud--hmao.sudrf.ru';
const legacy = record(1, 600, {court_domain: domain}); kv.add(legacy);
assert.equal((await postResult(env, legacy, {section: 'cassation', added: 4})).status, 200);
assert.equal(JSON.parse(kv.data.get('import:last:' + domain + ':2800001')).section, 'cassation');
const htmlLegacy = record(2, 500, {court_domain: domain}); kv.add(htmlLegacy);
kv.data.set('import:dump:' + htmlLegacy.uuid, dumpHtml('5'));
assert.equal((await postResult(env, htmlLegacy, {added: 2})).status, 200);
assert.equal(JSON.parse(kv.data.get('import:last:' + domain + ':5')).added, 2);
// Старый domain-only светофор не доказывает какую-либо инстанцию.
const other = 'oblsud--tumen.sudrf.ru';
kv.data.set('import:last:' + other, JSON.stringify({court_domain: other, ts: new Date(now).toISOString(), added: 99}));
// Но старый журнал с section позволяет восстановить именно апелляцию.
kv.add(record(3, 400, {court_domain: other, status: 'done', section: 'appeal', added: 3}));
kv.add(record(4, 300, {court_domain: other, status: 'done', section: 'cassation', fetch_fail: 1}));
kv.add(record(5, 200, {court_domain: other, status: 'done', kind: 'case', section: 'cassation', added: 8}));
const body = await (await workerExport.fetch(new Request('https://worker.invalid/admin/import-log?secret=owner'), env)).json();
assert.equal(body.last[other].added, 99);
assert.equal(body.last_sections[other + ':5'].added, 3);
assert.equal(body.last_sections[other + ':2800001'], undefined);
output({ok: true});
""")


def test_pagination_continues_after_empty_page_and_never_returns_partial_queue():
    run_worker(r"""
const kv = kvStore(), env = environment(kv), job = record(1, 600);
kv.add(job);
const originalList = kv.list;
kv.list = async opts => opts.cursor === undefined
  ? {keys: [], list_complete: false, cursor: 'next'}
  : originalList({...opts, cursor: ''});
const body = await (await getLog(env, '&include_queue=1')).json();
assert.deepEqual(body.queue.map(r => r.uuid), [job.uuid]);
kv.list = async () => ({keys: [], list_complete: false, cursor: ''});
assert.equal((await getLog(env, '&include_queue=1')).status, 500);
output({ok: true});
""")
