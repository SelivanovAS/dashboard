"""Worker upload gateway contracts in Node VM, with no network or real KV."""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is required for Worker VM tests")
VALIDATION_ERROR = "court_domain не похож на домен sudrf.ru"
ORIGIN = "https://api2-bashkortostan.delosud.ru"

HARNESS = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const { webcrypto, createHash } = require('node:crypto');
const options = JSON.parse(fs.readFileSync(0, 'utf8'));
const enc = new TextEncoder();
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
let original = enc.encode(JSON.stringify({court_domain: '', operator: 'test operator', html: 'я' + 'x'.repeat(options.size || 1024)}));
if (options.content === 'invalid_json') original = enc.encode('{"html":');
if (options.content === 'invalid_utf8') original = Uint8Array.from([0xff, 0xfe]);
if (options.content === 'nested') original = enc.encode(JSON.stringify({__gateway_upload: {id: 'nested'}}));
const expectedHash = hash(original);
let envelope = {__gateway_upload: {
  id: '11111111-2222-4333-8444-555555555555',
  token: 'a'.repeat(64),
  bytes: original.byteLength,
  sha256: expectedHash,
}};
const ref = envelope.__gateway_upload;
switch (options.invalidRef) {
  case 'null': envelope.__gateway_upload = null; break;
  case 'array': envelope.__gateway_upload = []; break;
  case 'mixed_envelope': envelope.court_domain = ''; break;
  case 'uuid': ref.id = '../../unexpected'; break;
  case 'uuid_version': ref.id = ref.id.replace('-4333-', '-1333-'); break;
  case 'uuid_array': ref.id = [ref.id]; break;
  case 'token': ref.token = 'z'.repeat(64); break;
  case 'token_short': ref.token = 'a'.repeat(63); break;
  case 'token_array': ref.token = [ref.token]; break;
  case 'hash': ref.sha256 = 'g'.repeat(64); break;
  case 'hash_array': ref.sha256 = [ref.sha256]; break;
  case 'bytes_zero': ref.bytes = 0; break;
  case 'bytes_negative': ref.bytes = -1; break;
  case 'bytes_string': ref.bytes = String(ref.bytes); break;
  case 'bytes_fraction': ref.bytes = 1.5; break;
  case 'bytes_oversize': ref.bytes = 10 * 1024 * 1024 + 1; break;
  case 'bytes_unsafe': ref.bytes = Number.MAX_SAFE_INTEGER + 1; break;
}
const fetches = [];
const kvCalls = [];
const timers = new Map();
const clearedTimers = [];
let nextTimer = 0;
let streamReads = 0;
let streamCanceled = 0;
let streamOpened = 0;
let signal;
let abortObserved = false;
let jsonReads = 0;
const kv = Object.fromEntries(['get', 'put', 'delete', 'list'].map(method => [method, async (...args) => {
  kvCalls.push({method, key: args[0]});
  throw new Error('Unexpected KV access');
}]));
const env = {
  OWNER_SECRET: 'test-owner', OPERATOR_SECRET: 'test-operator',
  IMPORT_EXECUTOR: 'vps', IMPORT_GATEWAY_ORIGIN: 'https://api2-bashkortostan.delosud.ru',
  PUSH_SUBSCRIPTIONS: kv,
};
if ('origin' in options) env.IMPORT_GATEWAY_ORIGIN = options.origin;
if (options.disabled) delete env.IMPORT_GATEWAY_ORIGIN;
const sandbox = {
  URL, Request, Response, Headers, TextEncoder, TextDecoder, Uint8Array,
  AbortController, crypto: webcrypto, renderAdminHtml() { throw new Error('Unexpected HTML render'); },
  console: {log() {}, error() {}, warn() {}},
  setTimeout(callback, delay) {
    const id = ++nextTimer;
    timers.set(id, {callback, delay});
    return id;
  },
  clearTimeout(id) { clearedTimers.push(id); timers.delete(id); },
  async fetch(url, init) {
    fetches.push({url, method: init.method, headers: Object.fromEntries(new Headers(init.headers)),
      redirect: init.redirect, hasBody: Object.prototype.hasOwnProperty.call(init, 'body')});
    signal = init.signal;
    if (options.download === 'timeout_fetch') {
      return new Promise((_resolve, reject) => {
        const onAbort = () => { abortObserved = true; reject(new DOMException('Timed out', 'AbortError')); };
        if (signal.aborted) onAbort();
        else signal.addEventListener('abort', onAbort, {once: true});
        queueMicrotask(() => {
          for (const timer of timers.values()) {
            if (timer.delay !== 20000) throw new Error('Unexpected timeout');
            timer.callback();
          }
        });
      });
    }
    if (options.download === 'reject') throw new Error('synthetic network failure');
    const headers = new Headers({'Content-Type': 'application/json; charset=utf-8',
      'Content-Length': String(original.byteLength)});
    let status = 200;
    if (options.download === 'status') {
      status = options.status || 503;
      if (status >= 300 && status < 400) headers.set('Location', 'https://unexpected.invalid/steal');
    }
    if (options.download === 'content_type') headers.set('Content-Type', options.contentType || 'text/html');
    if (options.download === 'missing_type') headers.delete('Content-Type');
    if (options.download === 'content_length') headers.set('Content-Length', options.contentLength);
    if (options.download === 'no_length') headers.delete('Content-Length');
    let delivered = original;
    if (options.download === 'overstream') {
      delivered = new Uint8Array(original.byteLength + 1); delivered.set(original); delivered[original.byteLength] = 32;
      headers.delete('Content-Length');
    }
    if (options.download === 'understream') {
      delivered = original.slice(0, -1); headers.delete('Content-Length');
    }
    if (options.download === 'bad_hash') {
      delivered = original.slice(); delivered[delivered.length - 1] ^= 1;
    }
    let position = 0;
    const body = options.download === 'missing_body' ? null : {
      getReader() {
        streamOpened++;
        return {
          async read() {
            streamReads++;
            if (options.download === 'timeout_stream') {
              return new Promise((_resolve, reject) => {
                signal.addEventListener('abort', () => { abortObserved = true; reject(new DOMException('Timed out', 'AbortError')); }, {once: true});
                queueMicrotask(() => { for (const timer of timers.values()) timer.callback(); });
              });
            }
            if (position >= delivered.byteLength) return {done: true};
            const end = Math.min(position + 4093, delivered.byteLength);
            const value = delivered.slice(position, end);
            position = end;
            return {done: false, value};
          },
          async cancel() { streamCanceled++; },
        };
      },
    };
    return {status, headers, body};
  },
};
const context = vm.createContext(sandbox);
const helper = fs.readFileSync('cloudflare-worker/import_gateway.js', 'utf8').replace(/^export\s+/gm, '');
const source = fs.readFileSync('cloudflare-worker/worker.js', 'utf8')
  .replace(/^import\s+.*?;\s*$/gm, '')
  .replace('export default {', 'globalThis.__worker = {');
vm.runInContext(helper + '\n' + source, context, {timeout: 2000});
(async () => {
  const secret = options.role === 'operator' ? 'test-operator' : options.role === 'wrong' ? 'wrong-key' : 'test-owner';
  const url = 'https://api-bashkortostan.delosud.ru/admin/import-dump' + (options.noSecret ? '' : '?secret=' + secret);
  const body = options.normal ? (options.invalidRequestJson ? '{' : JSON.stringify({court_domain: '', html: 'normal request'})) : JSON.stringify(envelope);
  const requestHeaders = {'Content-Type': 'application/json'};
  if (options.bearerOnly) requestHeaders.Authorization = 'Bearer test-owner';
  const request = new Request(url, {method: 'POST', headers: requestHeaders, body});
  const originalJson = request.json.bind(request);
  request.json = async () => { jsonReads++; return originalJson(); };
  const response = await context.__worker.fetch(request, env);
  const responseText = await response.text();
  let responseBody;
  try { responseBody = JSON.parse(responseText); } catch (_) { responseBody = responseText; }
  process.stdout.write(JSON.stringify({status: response.status,
    proof: response.headers.get('X-Import-Gateway-SHA256'), body: responseBody,
    expectedHash, originalBytes: original.byteLength, fetches, kvCalls,
    streamOpened, streamReads, streamCanceled, jsonReads, timersRemaining: timers.size,
    timersCleared: clearedTimers.length, signalAborted: signal ? signal.aborted : null, abortObserved}));
})().catch(error => { process.stderr.write(error.stack); process.exitCode = 1; });
"""


def run_worker(**scenario):
    completed = subprocess.run(
        [NODE, "-e", HARNESS], input=json.dumps(scenario), text=True,
        capture_output=True, cwd=REPO, timeout=8,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["kvCalls"] == [], "an invalid import must never access KV"
    return result


def assert_no_fetch(result, status):
    assert result["status"] == status
    assert result["fetches"] == []
    assert result["proof"] is None
    assert result["timersRemaining"] == 0


@pytest.mark.parametrize("scenario", [
    {"role": "wrong"},
    {"noSecret": True},
    {"noSecret": True, "bearerOnly": True},
    {"role": "wrong", "normal": True, "invalidRequestJson": True},
])
def test_auth_precedes_parsing_and_reverse_fetch(scenario):
    result = run_worker(**scenario)
    assert_no_fetch(result, 401)
    assert result["jsonReads"] == 0


@pytest.mark.parametrize("disabled", [False, True])
@pytest.mark.parametrize("role", ["owner", "operator"])
def test_normal_import_keeps_existing_validation_without_gateway_fetch(disabled, role):
    result = run_worker(normal=True, disabled=disabled, role=role)
    assert_no_fetch(result, 400)
    assert result["body"]["error"] == VALIDATION_ERROR


def test_gateway_is_disabled_without_regional_opt_in():
    result = run_worker(disabled=True)
    assert_no_fetch(result, 400)


@pytest.mark.parametrize("invalid_ref", [
    "null", "array", "mixed_envelope", "uuid", "uuid_version", "uuid_array",
    "token", "token_short", "token_array", "hash", "hash_array",
    "bytes_zero", "bytes_negative", "bytes_string", "bytes_fraction", "bytes_oversize", "bytes_unsafe",
])
def test_invalid_reference_is_rejected_before_fetch(invalid_ref):
    assert_no_fetch(run_worker(invalidRef=invalid_ref), 400)


@pytest.mark.parametrize("origin", [
    "http://api2-bashkortostan.delosud.ru",
    "https://user:password@api2-bashkortostan.delosud.ru",
    "https://api2-bashkortostan.delosud.ru/path",
    "https://api2-bashkortostan.delosud.ru/?secret=unexpected",
    "https://api2-bashkortostan.delosud.ru/#fragment",
    "https://api2-bashkortostan.delosud.ru:444",
    "not a URL",
])
def test_invalid_configured_origin_is_rejected_before_fetch(origin):
    assert_no_fetch(run_worker(origin=origin), 502)


@pytest.mark.parametrize("size", [32768, 409600])
@pytest.mark.parametrize("role", ["owner", "operator"])
def test_verified_large_body_reaches_existing_domain_validation_for_both_roles(size, role):
    result = run_worker(size=size, role=role)
    assert result["status"] == 400
    assert result["body"]["error"] == VALIDATION_ERROR
    assert result["proof"] == result["expectedHash"]
    assert result["fetches"] == [{
        "url": ORIGIN + "/_gateway-upload/11111111-2222-4333-8444-555555555555",
        "method": "GET", "headers": {"authorization": "Bearer " + "a" * 64},
        "redirect": "manual", "hasBody": False,
    }]
    assert result["streamReads"] > 1
    assert result["streamCanceled"] == 1
    assert result["signalAborted"] is True
    assert result["timersRemaining"] == 0
    assert result["timersCleared"] == 1


@pytest.mark.parametrize("scenario", [
    {"download": "status", "status": 302},
    {"download": "status", "status": 403},
    {"download": "status", "status": 503},
    {"download": "content_type", "contentType": "text/html"},
    {"download": "content_type", "contentType": "application/jsonp"},
    {"download": "missing_type"},
    {"download": "content_length", "contentLength": "-1"},
    {"download": "content_length", "contentLength": "broken"},
    {"download": "content_length", "contentLength": "1"},
    {"download": "content_length", "contentLength": "10000000"},
    {"download": "missing_body"},
])
def test_bad_gateway_response_fails_before_stream_read(scenario):
    result = run_worker(**scenario)
    assert result["status"] == 502
    assert result["proof"] is None
    assert len(result["fetches"]) == 1
    assert result["fetches"][0]["redirect"] == "manual"
    assert result["fetches"][0]["url"].startswith(ORIGIN + "/_gateway-upload/")
    assert result["streamOpened"] == 0
    assert result["streamReads"] == 0
    assert result["signalAborted"] is True
    assert result["timersRemaining"] == 0


@pytest.mark.parametrize("download", ["overstream", "understream", "bad_hash"])
def test_incomplete_oversized_or_corrupted_stream_is_not_imported(download):
    result = run_worker(download=download)
    assert result["status"] == 502
    assert result["proof"] is None
    assert len(result["fetches"]) == 1
    assert result["streamCanceled"] == 1
    assert result["signalAborted"] is True
    assert result["timersRemaining"] == 0


def test_absent_content_length_still_uses_stream_size_and_hash():
    result = run_worker(download="no_length")
    assert result["status"] == 400
    assert result["body"]["error"] == VALIDATION_ERROR
    assert result["proof"] == result["expectedHash"]


@pytest.mark.parametrize("content,error", [
    ("invalid_json", "Bad JSON"),
    ("invalid_utf8", "Bad JSON"),
    ("nested", "Вложенная обратная загрузка запрещена"),
])
def test_verified_but_invalid_json_or_nested_envelope_never_reaches_kv(content, error):
    result = run_worker(content=content)
    assert result["status"] == 400
    assert result["body"]["error"] == error
    assert result["proof"] == result["expectedHash"]
    assert len(result["fetches"]) == 1
    assert result["streamCanceled"] == 1


@pytest.mark.parametrize("download", ["reject", "timeout_fetch", "timeout_stream"])
def test_download_failure_or_abort_has_no_retry_and_clears_timer(download):
    result = run_worker(download=download)
    assert result["status"] == 502
    assert result["proof"] is None
    assert len(result["fetches"]) == 1
    assert result["signalAborted"] is True
    assert result["timersRemaining"] == 0
    assert result["timersCleared"] == 1
    if download.startswith("timeout"):
        assert result["abortObserved"] is True
    if download == "timeout_stream":
        assert result["streamCanceled"] == 1
