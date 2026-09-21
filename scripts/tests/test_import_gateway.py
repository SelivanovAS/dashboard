"""Обратная загрузка: настоящий ESM Worker, fetch и KV только в памяти."""
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
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {createHash, webcrypto} = require('node:crypto');
const root = process.argv[2];
const requests = [];
let respond = () => { throw new Error('Unexpected fetch'); };
const context = vm.createContext({
  console: {log() {}, warn() {}, error() {}}, crypto: webcrypto,
  Request, Response, URL, AbortController, TextEncoder, TextDecoder,
  setTimeout, clearTimeout,
  fetch: async (url, options) => { requests.push({url, options}); return respond(url, options); },
});
const modules = new Map();
async function loadModule(filename) {
  filename = path.resolve(filename);
  if (modules.has(filename)) return modules.get(filename);
  const module = new vm.SourceTextModule(fs.readFileSync(filename, 'utf8'), {
    context, identifier: filename,
  });
  modules.set(filename, module);
  await module.link((specifier, owner) => {
    assert.ok(specifier.startsWith('./'), 'Only local Worker dependencies are allowed');
    return loadModule(path.resolve(path.dirname(owner.identifier), specifier));
  });
  return module;
}
async function loadWorker() {
  const module = await loadModule(path.join(root, 'cloudflare-worker/worker.js'));
  await module.evaluate();
  return module.namespace.default;
}
async function loadGateway() {
  const module = await loadModule(path.join(root, 'cloudflare-worker/import_gateway.js'));
  await module.evaluate();
  return module.namespace.readGatewayImportBody;
}
function upload(raw) {
  const bytes = Buffer.from(raw);
  return {bytes, envelope: {__gateway_upload: {
    id: '12345678-1234-4123-8123-123456789abc', token: 'a'.repeat(64),
    bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex'),
  }}};
}
function payload(domain = 'kirovsky--bkr.sudrf.ru') {
  return {court_domain: domain, operator: 'Юрист',
    html: '<html><a href="https://' + domain + '/modules.php?name=sud_delo&name_op=case&case_id=1">Дело</a>'
      + 'данные '.repeat(180) + '</html>'};
}
function kvStore() {
  const data = new Map(), puts = [];
  return {data, puts,
    async get(key) { return data.get(key) ?? null; },
    async put(key, value, options) { puts.push({key, value, options}); data.set(key, value); },
  };
}
function environment(kv, extra = {}) {
  return {PUSH_SUBSCRIPTIONS: kv, OWNER_SECRET: 'owner', OPERATOR_SECRET: 'operator',
    PUSH_SECRET: 'push', IMPORT_EXECUTOR: 'vps',
    IMPORT_GATEWAY_ORIGIN: 'https://gateway.invalid', ...extra};
}
async function submit(worker, env, body, secret = 'operator') {
  return worker.fetch(new Request('https://worker.invalid/admin/import-dump?secret=' + secret, {
    method: 'POST', body: JSON.stringify(body),
  }), env);
}
function jsonResponse(bytes, extra = {}) {
  return new Response(bytes, {headers: {'Content-Type': 'application/json; charset=utf-8', ...extra}});
}
function output(value) { process.stdout.write(JSON.stringify(value)); }
"""


def run_worker(script: str) -> dict:
    result = subprocess.run(
        [NODE, "--experimental-vm-modules", "-", str(ROOT)],
        input=HARNESS + "\n(async () => {\n" + script
        + "\n})().catch(e => { console.error(e); process.exit(1); });",
        text=True, capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("secret", ["owner", "operator"])
def test_gateway_stream_reaches_real_worker_queue_and_returns_verified_digest(secret):
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore(), body = payload();
const item = upload(JSON.stringify(body));
respond = () => new Response(new ReadableStream({start(controller) {
  // Разрыв внутри UTF-8 текста: хеш и декодирование относятся ко всему телу.
  const split = item.bytes.indexOf(Buffer.from('Юрист')) + 1;
  controller.enqueue(item.bytes.subarray(0, split));
  controller.enqueue(item.bytes.subarray(split)); controller.close();
}}), {headers: {'Content-Type': 'application/json', 'Content-Length': String(item.bytes.length)}});
const response = await submit(worker, environment(kv), item.envelope, SECRET);
assert.equal(response.status, 200);
assert.equal(response.headers.get('X-Import-Gateway-SHA256'), item.envelope.__gateway_upload.sha256);
const accepted = await response.json();
assert.equal(accepted.ok, true);
assert.equal(accepted.executor, 'vps');
assert.equal(kv.data.get('import:dump:' + accepted.key), body.html);
const log = kv.puts.find(p => p.key.startsWith('import:log:'));
assert.equal(JSON.parse(log.value).court_domain, body.court_domain);
assert.equal(JSON.parse(log.value).operator, body.operator);
assert.equal(JSON.parse(log.value).status, 'queued');
assert.equal(log.options.metadata.queue_pending, true);
assert.equal(JSON.parse(kv.data.get('import:pending')).uuid, accepted.key);
assert.equal(requests.length, 1);
assert.equal(requests[0].url, 'https://gateway.invalid/_gateway-upload/' + item.envelope.__gateway_upload.id);
assert.equal(requests[0].options.redirect, 'manual');
assert.deepEqual({...requests[0].options.headers}, {Authorization: 'Bearer ' + 'a'.repeat(64)});
assert.equal(requests[0].options.signal.aborted, true);
output({ok: true});
""".replace("SECRET", json.dumps(secret)))


@pytest.mark.parametrize("failure", ["sha", "declared_bytes", "short", "overflow", "redirect"])
def test_integrity_and_redirect_failures_never_write_to_queue(failure):
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore(), item = upload(JSON.stringify(payload()));
const failure = FAILURE;
if (failure === 'sha') item.envelope.__gateway_upload.sha256 = '0'.repeat(64);
respond = () => {
  if (failure === 'redirect') return new Response(null, {status: 302,
    headers: {Location: 'https://another.invalid/steal-token'}});
  if (failure === 'declared_bytes') return jsonResponse(item.bytes, {'Content-Length': String(item.bytes.length + 1)});
  if (failure === 'short') return jsonResponse(item.bytes.subarray(0, -1));
  if (failure === 'overflow') return jsonResponse(Buffer.concat([item.bytes, Buffer.from(' ')]));
  return jsonResponse(item.bytes);
};
const response = await submit(worker, environment(kv), item.envelope);
assert.equal(response.status, 502);
assert.equal((await response.json()).ok, false);
assert.equal(response.headers.get('X-Import-Gateway-SHA256'), null);
assert.equal(kv.puts.length, 0);
assert.equal(requests.length, 1);
assert.equal(requests[0].options.redirect, 'manual');
output({ok: true});
""".replace("FAILURE", json.dumps(failure)))


def test_disabled_gateway_invalid_envelope_and_origin_fail_before_fetch():
    run_worker(r"""
const readGateway = await loadGateway(), item = upload('{}'), env = environment(kvStore());
const ref = item.envelope.__gateway_upload;
const cases = [
  [item.envelope, {...env, IMPORT_GATEWAY_ORIGIN: ''}, 400],
  [{...item.envelope, court_domain: 'other.sudrf.ru'}, env, 400],
  [{__gateway_upload: []}, env, 400],
  [{__gateway_upload: {...ref, id: '../elsewhere'}}, env, 400],
  [{__gateway_upload: {...ref, token: 'wrong'}}, env, 400],
  [{__gateway_upload: {...ref, bytes: 10 * 1024 * 1024 + 1}}, env, 400],
  [{__gateway_upload: {...ref, bytes: 1.5}}, env, 400],
];
for (const origin of ['http://gateway.invalid', 'https://user:password@gateway.invalid',
  'https://gateway.invalid/path', 'https://gateway.invalid:8443']) {
  cases.push([item.envelope, {...env, IMPORT_GATEWAY_ORIGIN: origin}, 502]);
}
for (const [envelope, config, status] of cases) {
  await assert.rejects(readGateway(envelope, config), error => error.status === status);
}
assert.equal(requests.length, 0);
output({ok: true});
""")


@pytest.mark.parametrize("invalid_body", ["bad_json", "bad_utf8", "nested"])
def test_verified_but_invalid_body_reports_digest_without_enqueuing(invalid_body):
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore();
const raw = KIND === 'bad_json' ? '{broken' : KIND === 'bad_utf8'
  ? Buffer.from([0xff]) : JSON.stringify({__gateway_upload: {}});
const item = upload(raw);
respond = () => jsonResponse(item.bytes);
const response = await submit(worker, environment(kv), item.envelope);
assert.equal(response.status, 400);
assert.equal(response.headers.get('X-Import-Gateway-SHA256'), item.envelope.__gateway_upload.sha256);
assert.equal((await response.json()).ok, false);
assert.equal(kv.puts.length, 0);
output({ok: true});
""".replace("KIND", json.dumps(invalid_body)))


def test_gateway_requires_admin_role_before_any_fetch_or_write():
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore(), env = environment(kv);
const item = upload(JSON.stringify(payload()));
for (const secret of ['', 'wrong', 'push']) {
  const response = await submit(worker, env, item.envelope, secret);
  assert.equal(response.status, 401, secret);
}
assert.equal(requests.length, 0);
assert.equal(kv.puts.length, 0);
output({ok: true});
""")


@pytest.mark.parametrize("region,domain", [
    ("hmao", "surggor--hmao.sudrf.ru"),
    ("sverdlovsk_yanao", "leninsky--svd.sudrf.ru"),
    ("bashkortostan", "kirovsky--bkr.sudrf.ru"),
    ("tyumen", "centralny--tum.sudrf.ru"),
])
def test_direct_import_keeps_working_with_each_territory_configuration(region, domain):
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore(), body = payload(DOMAIN);
const region = REGION, repo = {
  hmao: 'dashboard', sverdlovsk_yanao: 'dashboard-ural',
  bashkortostan: 'dashboard-bashkortostan', tyumen: 'dashboard-tyumen',
}[region];
const env = environment(kv, {
  GH_REPO: 'SelivanovAS/' + repo,
  CASES_DATA_URL: 'https://selivanovas.github.io/' + repo + '/data/cases.json',
  IMPORT_GATEWAY_ORIGIN: ['bashkortostan', 'tyumen'].includes(region) ? 'https://gateway.invalid' : '',
});
const response = await submit(worker, env, body);
assert.equal(response.status, 200);
assert.equal(response.headers.get('X-Import-Gateway-SHA256'), null);
const accepted = await response.json();
assert.equal(accepted.ok, true);
assert.equal(kv.data.get('import:dump:' + accepted.key), body.html);
assert.equal(requests.length, 0);
output({ok: true});
""".replace("DOMAIN", json.dumps(domain)).replace("REGION", json.dumps(region)))


def test_gateway_preserves_original_court_validation():
    run_worker(r"""
const worker = await loadWorker(), kv = kvStore(), body = payload();
body.court_domain = 'another--bkr.sudrf.ru';
const item = upload(JSON.stringify(body));
respond = () => jsonResponse(item.bytes);
const response = await submit(worker, environment(kv), item.envelope);
assert.equal(response.status, 400);
assert.match((await response.json()).error, /а выбран another--bkr/);
assert.equal(response.headers.get('X-Import-Gateway-SHA256'), item.envelope.__gateway_upload.sha256);
assert.equal(kv.puts.length, 0);
output({ok: true});
""")
