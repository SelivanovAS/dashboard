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
function output(value) { process.stdout.write(JSON.stringify(value)); }
"""


def run_worker(script: str) -> dict:
    source = (ROOT / "cloudflare-worker/worker.js").read_text(encoding="utf-8")
    source = source.replace('import { renderAdminHtml } from "./admin_page.js";', "")
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
