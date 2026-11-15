const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const worker = fs.readFileSync(path.join(__dirname, '../../ops/vps-run/public-site/territory-worker.js'), 'utf8');
const scope = 'https://delosud.ru/ural/';
function boot(clients, extra = {}) {
  const events = new Map();
  const self = { registration: { scope }, clients, addEventListener: (type, fn) => events.set(type, fn) };
  vm.runInNewContext(worker, { self, URL, ...extra });
  return events;
}

test('Клик по уведомлению выбирает окно своей картотеки', async () => {
  const focused = [];
  const window = url => ({ url, focus: () => focused.push(url), postMessage() {} });
  const click = boot({ matchAll: async () => [window('https://delosud.ru/hmao/'), window(scope)] }).get('notificationclick');
  let done;
  click({ notification: { close() {} }, stopImmediatePropagation() {}, waitUntil: p => { done = p; } });
  await done;
  assert.deepEqual(focused, [scope]);
});

test('Запросы профилей и соседней территории проходят мимо обработчика кэша', () => {
  const handle = boot({}).get('fetch');
  for (const url of ['https://delosud.ru/_api/ural/profile/get', 'https://delosud.ru/hmao/data/cases.json']) {
    let stopped = false;
    handle({ request: { method: 'GET', url }, stopImmediatePropagation() { stopped = true; } });
    assert.ok(stopped);
  }
});

test('При первом открытии пользователь получает ответ до завершения записи офлайн-копии', async () => {
  const response = { ok: true, clone: () => 'copy' };
  let finishWrite;
  const write = new Promise(resolve => { finishWrite = resolve; });
  const fetchHandler = boot({}, {
    DATA_CACHE: 'regional-data', isDataRequest: () => true, isLastDigestRequest: () => false,
    caches: { open: async () => ({ match: async () => undefined }) },
    fetch: async () => response,
    cachePutSafe: (_cache, _request, body) => { assert.equal(body, 'copy'); return write; },
  }).get('fetch');
  let result, background;
  fetchHandler({
    request: { method: 'GET', url: scope + 'data/cases.json' }, stopImmediatePropagation() {},
    respondWith: p => { result = p; }, waitUntil: p => { background = p; },
  });
  assert.equal(await result, response);
  assert.equal(background, write);
  finishWrite();
  await background;
});
