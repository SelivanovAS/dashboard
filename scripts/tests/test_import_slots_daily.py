"""Слоты повторов из настоящего Worker: VPS ежедневно, прежний резерв в будни."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Проверка Worker требует Node")

HARNESS = r"""
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
class Clock extends Date {
  static now() { return Date.parse(input.now); }
}
const context = vm.createContext({
  Date: Clock, URL, Request, Response, TextEncoder, TextDecoder,
  console: {log() {}, warn() {}, error() {}},
  fetch() { throw new Error('Unexpected network request'); },
});
const modules = new Map();
async function load(filename) {
  filename = path.resolve(filename);
  if (modules.has(filename)) return modules.get(filename);
  const module = new vm.SourceTextModule(fs.readFileSync(filename, 'utf8'), {
    context, identifier: filename,
  });
  modules.set(filename, module);
  await module.link((specifier, owner) => load(path.resolve(path.dirname(owner.identifier), specifier)));
  return module;
}
(async () => {
  const module = await load(path.join(process.argv[2], 'cloudflare-worker/worker.js'));
  await module.evaluate();
  const kv = {
    async list() { return {keys: [], list_complete: true}; },
    async get() { throw new Error('Unexpected KV read'); },
    async put() { throw new Error('Unexpected KV write'); },
  };
  const response = await module.namespace.default.fetch(
    new Request('https://worker.invalid/admin/import-log?secret=test-owner&logonly=1'),
    {OWNER_SECRET: 'test-owner', PUSH_SUBSCRIPTIONS: kv, ...input.vars},
  );
  if (response.status !== 200) throw new Error('Unexpected status ' + response.status);
  process.stdout.write(JSON.stringify(await response.json()));
})().catch(error => { console.error(error); process.exit(1); });
"""


def _slots(tmp_path, now, variables):
    harness = tmp_path / "import-slots.cjs"
    harness.write_text(HARNESS, encoding="utf-8")
    result = subprocess.run(
        [NODE, "--experimental-vm-modules", str(harness), str(ROOT)],
        input=json.dumps({"now": now, "vars": variables}),
        text=True, capture_output=True, timeout=15, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["slots"]


@pytest.mark.parametrize(
    ("now", "variables", "last", "next_slot"),
    [
        pytest.param("2026-09-18T20:01:00+05:00", {},
                     "2026-09-18T15:00:00.000Z", "2026-09-19T07:00:00.000Z",
                     id="friday-evening-to-saturday"),
        pytest.param("2026-09-19T13:00:00+05:00", {},
                     "2026-09-19T07:00:00.000Z", "2026-09-19T09:00:00.000Z",
                     id="saturday-retries"),
        pytest.param("2026-09-20T19:59:00+05:00", {},
                     "2026-09-20T13:00:00.000Z", "2026-09-20T15:00:00.000Z",
                     id="sunday-retries"),
        pytest.param("2026-09-20T00:00:00+05:00", {},
                     "2026-09-19T15:00:00.000Z", "2026-09-20T07:00:00.000Z",
                     id="local-midnight-is-still-saturday-in-utc"),
        pytest.param("2026-09-20T00:00:00+05:00", {"IMPORT_SLOTS_LOCAL": "00:00,12:00"},
                     "2026-09-19T19:00:00.000Z", "2026-09-20T07:00:00.000Z",
                     id="exact-midnight-slot-is-last-not-next"),
        pytest.param("2026-09-19T00:00:00+04:00", {"CAL_TZ_OFFSET_MIN": "240"},
                     "2026-09-18T16:00:00.000Z", "2026-09-19T08:00:00.000Z",
                     id="configured-timezone"),
    ],
)
def test_vps_import_slots_include_every_day(tmp_path, now, variables, last, next_slot):
    result = _slots(tmp_path, now, {"IMPORT_EXECUTOR": "vps", **variables})
    assert result == {"last_slot_at": last, "next_slot_at": next_slot}


@pytest.mark.parametrize("executor", [None, "github"])
@pytest.mark.parametrize("now", ["2026-09-18T20:01:00+05:00", "2026-09-20T00:00:00+05:00"])
def test_legacy_executor_keeps_weekday_slots(tmp_path, executor, now):
    variables = {} if executor is None else {"IMPORT_EXECUTOR": executor}
    result = _slots(tmp_path, now, variables)
    assert result == {
        "last_slot_at": "2026-09-18T15:00:00.000Z",
        "next_slot_at": "2026-09-21T07:00:00.000Z",
    }
