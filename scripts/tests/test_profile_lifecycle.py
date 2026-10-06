"""Очистка и восстановление: настоящий Worker/ESM, часы и KV в памяти."""
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Проверка Worker требует Node")

HARNESS = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {webcrypto} = require('node:crypto');
const root = process.argv[2];
const DAY = 86400000;
let now = Date.parse('2026-09-15T08:00:00Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const network = [];
let casesUnavailable = false;
const context = vm.createContext({
  Date: Clock, crypto: webcrypto, Request, Response, URL, TextEncoder, TextDecoder,
  AbortController, setTimeout, clearTimeout,
  console: {log() {}, warn() {}, error() {}},
  fetch: async (url, options) => {
    network.push({url: String(url), options});
    if (String(url).includes('api.github.com')) throw new Error('Unexpected GitHub dispatch');
    return new Response(JSON.stringify({cases: []}), {status: casesUnavailable ? 503 : 200});
  },
});
const modules = new Map();
async function load(filename) {
  filename = path.resolve(filename);
  if (modules.has(filename)) return modules.get(filename);
  const module = new vm.SourceTextModule(fs.readFileSync(filename, 'utf8'), {context, identifier: filename});
  modules.set(filename, module);
  await module.link((specifier, owner) => load(path.resolve(path.dirname(owner.identifier), specifier)));
  return module;
}
function store(pageSize = 1000) {
  const data = new Map(), writes = [], deletes = [], reads = [];
  return {data, writes, deletes, reads, beforeGet: null, failList: false,
    async get(key) {
      reads.push(key);
      if (this.beforeGet) await this.beforeGet(key);
      return data.get(key) ?? null;
    },
    async put(key, value, options) { writes.push({key, value, options}); data.set(key, value); },
    async delete(key) { deletes.push(key); data.delete(key); },
    async list({prefix, cursor}) {
      if (this.failList && prefix === 'sub:') throw new Error('KV list unavailable');
      const keys = [...data.keys()].filter(k => k.startsWith(prefix)).sort();
      const start = Number(cursor || 0), end = Math.min(start + pageSize, keys.length);
      return {keys: keys.slice(start, end).map(name => ({name})), list_complete: end >= keys.length,
        ...(end < keys.length ? {cursor: String(end)} : {})};
    },
    json(key) { return data.has(key) ? JSON.parse(data.get(key)) : null; },
    seed(key, value) { data.set(key, JSON.stringify(value)); },
    clearOperations() { writes.length = 0; deletes.length = 0; },
  };
}
const id = '11111111-1111-4111-8111-111111111111';
const id2 = '22222222-2222-4222-8222-222222222222';
const token = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const oldToken = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
function profile(kv, pid = id, extra = {}) {
  kv.seed('profile:' + pid, {schema_version: 1, watchlist: [], updated_at: now - 300 * DAY,
    created_at: new Clock(now - 300 * DAY).toISOString(), ...extra});
}
function env(kv, extra = {}) {
  return {PUSH_SUBSCRIPTIONS: kv, OWNER_SECRET: 'owner', OPERATOR_SECRET: 'operator',
    PUSH_SECRET: 'push', PROFILE_CLEANUP_ENABLED: '1', CRON_UTC: '', ...extra};
}
async function call(worker, environment, pathname, body, secret = '') {
  const request = new Request('https://worker.invalid' + pathname + (secret ? '?secret=' + secret : ''), {
    method: body === undefined ? 'GET' : 'POST',
    headers: {'Content-Type': 'application/json', Authorization: 'Bearer push'},
    ...(body === undefined ? {} : {body: JSON.stringify(body)}),
  });
  return worker.fetch(request, environment);
}
const sweep = (worker, environment) => worker.scheduled({cron: '17 21 * * *'}, environment);
async function setup(pageSize) {
  const module = await load(path.join(root, 'cloudflare-worker/worker.js'));
  await module.evaluate();
  const kv = store(pageSize);
  return {worker: module.namespace.default, kv, environment: env(kv)};
}
"""


def run_node(tmp_path, body):
    script = tmp_path / "profile-lifecycle.cjs"
    script.write_text(HARNESS + "\n(async () => {\n" + body +
                      "\nconsole.log('OK');\n})().catch(e => { console.error(e); process.exit(1); });\n")
    result = subprocess.run([NODE, "--experimental-vm-modules", str(script), str(ROOT)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "OK"


def test_legacy_starts_full_seven_day_window_and_admin_reads_do_not_restore(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv);
const original = kv.data.get('profile:' + id);
await sweep(worker, environment);
assert.equal(kv.json('profile-activity:' + id).last_used_at, null);
assert.equal(kv.json('profile-removed:' + id), null);
now += 7 * DAY - 1;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id), null);
now++;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id).restore_until, now + 7 * DAY);
assert.equal(kv.data.get('profile:' + id), original);
kv.clearOperations();
const result = await (await call(worker, environment, '/admin/data', undefined, 'owner')).json();
assert.equal(result.profiles[0].lifecycle_status, 'recoverable');
assert.equal(result.profiles[0].last_used_at, '');
await call(worker, environment, '/subscriptions');
assert.equal(kv.writes.length, 0);
assert.equal(kv.deletes.length, 0);
""")


@pytest.mark.parametrize("extra", ["{watchlist: ['2-1/2026']}", "{feed_token: token}"])
def test_nonempty_or_calendar_profile_gets_thirty_days(tmp_path, extra):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, EXTRA);
await sweep(worker, environment);
now += 30 * DAY - 1;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id), null);
now++;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id).reason, 'inactive');
""".replace("EXTRA", extra))


def test_linked_push_protects_profile_on_later_kv_pages(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup(1);
profile(kv); profile(kv, id2);
kv.seed('sub:a', {endpoint: 'https://push.invalid/a'});
kv.seed('sub:z', {endpoint: 'https://push.invalid/z', profile_id: id});
await sweep(worker, environment);
now += 100 * DAY;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id), null);
assert.ok(kv.json('profile-removed:' + id2));
assert.equal(kv.json('profile-maintenance:last').protected_by_push, 1);
""")


def test_device_restores_without_touching_watchlist_or_lww_and_resets_idle(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, {watchlist: ['2-1/2026']});
const original = kv.data.get('profile:' + id);
await sweep(worker, environment);
now += 30 * DAY;
await sweep(worker, environment);
now += 6 * DAY;
kv.clearOperations();
const response = await call(worker, environment, '/profile/get', {profile_id: id});
assert.equal(response.status, 200);
assert.deepEqual((await response.json()).watchlist, ['2-1/2026']);
assert.equal(kv.json('profile-removed:' + id), null);
assert.equal(kv.data.get('profile:' + id), original);
assert.ok(kv.writes.every(w => w.key.startsWith('profile-activity:')));
const activeUntil = kv.json('profile-activity:' + id).active_until;
now = activeUntil + 30 * DAY - 1;
await sweep(worker, environment);
assert.equal(kv.json('profile-removed:' + id), null);
""")


def test_activity_once_per_utc_day_does_not_overwrite_lww(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, {watchlist: ['2-1/2026']});
const original = kv.data.get('profile:' + id);
for (let i = 0; i < 10; i++) {
  assert.equal((await call(worker, environment, '/profile/get', {profile_id: id})).status, 200);
  now += 1000;
}
assert.equal(kv.writes.length, 1);
now += DAY;
await call(worker, environment, '/profile/get', {profile_id: id});
assert.equal(kv.writes.length, 2);
assert.equal(kv.data.get('profile:' + id), original);
const conflict = await call(worker, environment, '/profile/watchlist', {profile_id: id, watchlist: [], base_ts: 0});
assert.equal(conflict.status, 409);
assert.equal(kv.data.get('profile:' + id), original);
""")


def test_parallel_startup_requests_share_one_activity_write(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv);
const results = await Promise.all(Array.from({length: 10}, () =>
  call(worker, environment, '/profile/get', {profile_id: id})));
assert.ok(results.every(r => r.status === 200));
assert.equal(kv.writes.length, 1);
""")


def test_calendar_counts_use_restores_and_rejects_old_token_without_activity(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, {feed_token: token});
kv.seed('calfeed:' + token, {profile_id: id});
kv.seed('calfeed:' + oldToken, {profile_id: id});
await sweep(worker, environment);
now += 30 * DAY;
await sweep(worker, environment);
kv.clearOperations();
assert.equal((await call(worker, environment, '/calendar/' + oldToken + '.ics')).status, 404);
assert.equal(kv.writes.length, 0);
assert.equal(kv.deletes.length, 0);
const response = await call(worker, environment, '/calendar/' + token + '.ics');
assert.equal(response.status, 200);
assert.ok((await response.text()).includes('BEGIN:VCALENDAR'));
assert.equal(kv.json('profile-removed:' + id), null);
assert.equal(kv.json('profile-activity:' + id).last_used_at, now);
""")


def test_calendar_data_outage_still_counts_legitimate_use(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, {feed_token: token, watchlist: ['2-1/2026']});
kv.seed('calfeed:' + token, {profile_id: id});
casesUnavailable = true;
const response = await call(worker, environment, '/calendar/' + token + '.ics');
assert.equal(response.status, 503);
assert.equal(kv.json('profile-activity:' + id).last_used_at, now);
""")


def test_recovery_closes_after_seven_days_then_purges_all_owned_keys(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv, id, {feed_token: token});
kv.seed('calfeed:' + token, {profile_id: id});
kv.seed('unrelated:data', {keep: true});
await sweep(worker, environment);
now += 30 * DAY;
await sweep(worker, environment);
now += 7 * DAY;
kv.clearOperations();
assert.equal((await call(worker, environment, '/profile/get', {profile_id: id})).status, 404);
assert.equal((await call(worker, environment, '/calendar/' + token + '.ics')).status, 404);
assert.equal(kv.writes.length, 0);
await sweep(worker, environment);
assert.ok(kv.json('profile:' + id));
const rows = await (await call(worker, environment, '/admin/data', undefined, 'owner')).json();
assert.equal(rows.profiles.length, 0);
now += DAY;
await sweep(worker, environment);
for (const key of ['profile:' + id, 'profile-activity:' + id, 'profile-removed:' + id, 'calfeed:' + token]) {
  assert.equal(kv.json(key), null, key);
}
assert.deepEqual(kv.json('unrelated:data'), {keep: true});
assert.equal((await call(worker, environment, '/profile/get', {profile_id: id})).status, 404);
""")


@pytest.mark.parametrize("failure", ["kv.failList = true", "kv.data.set('sub:broken', '{broken')"])
def test_incomplete_push_snapshot_aborts_before_deletion(tmp_path, failure):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv);
await sweep(worker, environment);
now += 7 * DAY;
await sweep(worker, environment);
now += 8 * DAY;
kv.clearOperations();
FAILURE;
await assert.rejects(() => sweep(worker, environment));
assert.equal(kv.deletes.length, 0);
assert.equal(kv.writes.length, 0);
assert.ok(kv.json('profile:' + id));
""".replace("FAILURE", failure))


def test_final_recheck_preserves_returning_user_and_changed_profile(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv); profile(kv, id2);
await sweep(worker, environment);
now += 7 * DAY;
await sweep(worker, environment);
now += 8 * DAY;
let activityReads = 0;
kv.beforeGet = async key => {
  if (key === 'profile-activity:' + id && ++activityReads === 2) {
    const activity = kv.json(key);
    // Вход был в разрешённое окно, но устаревший маркер ещё виден.
    activity.last_used_at = now - 2 * DAY; activity.active_until = now - DAY;
    kv.seed(key, activity);
  }
};
const changed = kv.json('profile:' + id2);
changed.watchlist = ['2-new/2026']; changed.updated_at = now;
kv.seed('profile:' + id2, changed);
await sweep(worker, environment);
assert.ok(kv.json('profile:' + id));
assert.deepEqual(kv.json('profile:' + id2).watchlist, ['2-new/2026']);
assert.equal(kv.json('profile-removed:' + id), null);
assert.equal(kv.json('profile-removed:' + id2), null);
""")


def test_restore_requires_owner_and_expires_at_deadline(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
profile(kv); profile(kv, id2);
await sweep(worker, environment);
now += 7 * DAY;
await sweep(worker, environment);
kv.clearOperations();
for (const secret of ['', 'operator']) {
  const response = await call(worker, environment, '/admin/profile/restore', {profile_id: id}, secret);
  assert.equal(response.status, secret ? 403 : 401);
}
assert.equal(kv.writes.length, 0);
assert.equal((await call(worker, environment, '/admin/profile/restore', {profile_id: id}, 'owner')).status, 200);
assert.equal(kv.json('profile-removed:' + id), null);
now += 7 * DAY;
assert.equal((await call(worker, environment, '/admin/profile/restore', {profile_id: id2}, 'owner')).status, 404);
""")


def test_new_profiles_and_subscribe_record_use(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
const created = await (await call(worker, environment, '/profile/calendar-token', {watchlist: []})).json();
assert.ok(kv.json('profile-activity:' + created.profile_id));
const raw = kv.data.get('profile:' + created.profile_id);
now += DAY;
await call(worker, environment, '/subscribe', {endpoint: 'https://push.invalid/device', profile_id: created.profile_id});
assert.equal(kv.json('profile-activity:' + created.profile_id).last_used_at, now);
assert.equal(kv.data.get('profile:' + created.profile_id), raw);
""")


def test_maintenance_is_separate_from_parser_including_weekends(tmp_path):
    run_node(tmp_path, r"""
const {worker, kv, environment} = await setup();
now = Date.parse('2026-09-20T21:17:00Z'); // воскресенье
profile(kv);
await sweep(worker, environment);
assert.ok(kv.json('profile-activity:' + id));
assert.equal(network.length, 0);
kv.clearOperations();
await worker.scheduled({cron: '30 3 * * mon-fri'}, environment);
assert.equal(network.length, 0);
await sweep(worker, {...environment, PROFILE_CLEANUP_ENABLED: '0'});
assert.equal(kv.writes.length, 0);
assert.equal(kv.deletes.length, 0);
""")
