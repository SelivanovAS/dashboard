"""Поведение формы и общей очереди импортов в настоящем JS страницы.

Рендерим HTML обеих ролей и выполняем блок импортов в Node VM. DOM, сеть и
часы подменены: проверки не отправляют дампы, не обращаются к KV и не ждут
боевые интервалы. Проверяем пользовательские гонки при нескольких судах,
а не только наличие функций в исходнике.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
ADMIN = ROOT / "cloudflare-worker" / "admin_page.js"


HARNESS = r"""
import assert from "node:assert/strict";
import vm from "node:vm";
import { renderAdminHtml } from "./admin_page.mjs";

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function response(body, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

async function settle() {
  for (let i = 0; i < 12; i++) await Promise.resolve();
}

function dump(domain = "first--test.sudrf.ru", caseId = 1) {
  return '<a href="https://' + domain
    + '/modules.php?name=sud_delo&name_op=case&case_id=' + caseId
    + '&delo_id=1540005">2-' + caseId + '/2026</a>' + "Выдача суда ".repeat(140);
}

function fileOf(html, name = "court.html") {
  const bytes = new TextEncoder().encode(html);
  return { name, size: bytes.byteLength, arrayBuffer: async () => bytes.buffer };
}

function makePage(role, storage = new Map()) {
  const html = renderAdminHtml("test-secret", role, {});
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
  const script = scripts.find(src => src.includes("var impCourts ="));
  assert.ok(script, "В отрендеренной странице отсутствует блок импортов");
  const start = script.indexOf("var impCourts =");
  const end = script.indexOf('// Плитка «Дайджест»', start);
  assert.ok(end > start, "Не найдена граница блока импортов");
  const nodes = new Map();
  function el(id) {
    if (!nodes.has(id)) {
      const attrs = new Map();
      const classes = new Set();
      const events = new Map();
      nodes.set(id, {
        id, value: "", innerHTML: "", textContent: "", disabled: false,
        hidden: false, open: false, className: "", style: {}, dataset: {},
        files: [], offsetWidth: 100,
        classList: {
          add: (...names) => names.forEach(n => classes.add(n)),
          remove: (...names) => names.forEach(n => classes.delete(n)),
          contains: n => classes.has(n),
          toggle: (n, force) => {
            const on = force === undefined ? !classes.has(n) : force;
            if (on) classes.add(n); else classes.delete(n);
            return on;
          },
        },
        setAttribute: (k, v) => attrs.set(k, String(v)),
        getAttribute: k => attrs.get(k) ?? null,
        removeAttribute: k => attrs.delete(k),
        addEventListener: (kind, fn) => {
          if (!events.has(kind)) events.set(kind, []);
          events.get(kind).push(fn);
        },
        dispatchEvent: e => (events.get(e.type) || []).forEach(fn => fn.call(el(id), e)),
        querySelector: () => null, querySelectorAll: () => [],
        closest: () => el(id), focus() {}, scrollIntoView() {},
        showModal() { this.open = true; }, close() { this.open = false; },
      });
    }
    return nodes.get(id);
  }
  const timers = new Map();
  const tiles = new Map();
  let now = Date.parse("2026-09-12T12:00:00Z");
  class TestDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  let nextTimer = 1;
  const requests = [];
  let handler = async url => {
    throw new Error("Неожиданный запрос: " + url);
  };
  const ctx = {
    console, URL, TextDecoder, TextEncoder, Date: TestDate,
    SECRET: "test-secret", IS_OWNER: role === "owner", ROLE: role,
    ICON_X: "×", ICON_REFRESH: "↻",
    localStorage: {
      getItem: k => storage.has(k) ? storage.get(k) : null,
      setItem: (k, v) => storage.set(k, String(v)),
      removeItem: k => storage.delete(k),
    },
    document: {
      hidden: false, visibilityState: "visible",
      getElementById: el, addEventListener() {},
      querySelector: () => null, querySelectorAll: () => [],
    },
    window: { addEventListener() {} },
    location: { origin: "https://worker.example", pathname: "/admin", hash: "#import" },
    setTimeout: (fn, delay) => { const id = nextTimer++; timers.set(id, { fn, delay }); return id; },
    clearTimeout: id => timers.delete(id),
    setInterval: (fn, delay) => { const id = nextTimer++; timers.set(id, { fn, delay, interval: true }); return id; },
    clearInterval: id => timers.delete(id),
    fetch: async (url, opts = {}) => {
      requests.push({ url, opts });
      return handler(String(url), opts);
    },
    escHtml: value => String(value ?? "").replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]),
    plural: (n, one, few, many) => n === 1 ? one : n < 5 ? few : many,
    nPlural: (n, one, few, many) => n + " " + (n === 1 ? one : n < 5 ? few : many),
    parseIso: value => Date.parse(value), relTime: value => value || "",
    fullDate: value => value || "",
    setTile: (key, color, value, sub) => tiles.set(key, { color, value, sub }),
    loadErrorHtml: (message, _key, error) => message + ": " + String(error),
    IMP_T: {},
  };
  vm.createContext(ctx);
  vm.runInContext(script.slice(start, end), ctx, { timeout: 2000 });
  const restoreStart = script.indexOf("impRestoreQueue();", end);
  const restoreEnd = script.indexOf("initTabs();", restoreStart);
  assert.ok(restoreStart > end && restoreEnd > restoreStart,
    "Не найдена стартовая загрузка сохранённой очереди");
  vm.runInContext(script.slice(restoreStart, restoreEnd), ctx, { timeout: 2000 });
  el("imp-name").value = "Оператор";
  el("imp-court").value = "first--test.sudrf.ru|1";
  ctx.impCourts = [
    { domain: "first--test.sudrf.ru", name: "Первый районный суд", srv_num: 1, search_gated: true },
    { domain: "second--test.sudrf.ru", name: "Второй районный суд", srv_num: 2, search_gated: true },
  ];
  ctx.impCourtNameByDomain = Object.fromEntries(ctx.impCourts.map(c => [c.domain, c.name]));
  ctx.impCourtTouched = true;
  return {
    ctx, el, nodes, timers, requests, storage, tiles,
    advance: milliseconds => { now += milliseconds; },
    setFetch: fn => { handler = fn; },
    async tick() {
      assert.equal(timers.size, 1, "Для всей очереди должна быть одна цепочка опроса");
      const [id, timer] = [...timers.entries()][0];
      timers.delete(id);
      await timer.fn();
      await settle();
    },
  };
}

function job(uuid, fields = {}) {
  return {
    uuid, status: "queued", kind: "dump", court_domain: "first--test.sudrf.ru",
    operator: "Оператор", ts: "2026-09-12T08:00:00Z", ...fields,
  };
}

function queueBody(items) {
  return {
    ok: true, executor: "vps", items, last: {}, tracked: [],
    queue: items.filter(it => ["queued", "dispatched", "started"].includes(it.status))
      .map(it => ({ ...it, queue_pending: true })),
  };
}

async function runScenario(role) {
__SCENARIO__
}

for (const role of ["owner", "operator"]) await runScenario(role);
"""


def run_scenario(tmp_path: Path, scenario: str) -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node недоступен: поведение JS админки не проверено")
    shutil.copyfile(ADMIN, tmp_path / "admin_page.mjs")
    check = tmp_path / "check.mjs"
    check.write_text(HARNESS.replace("__SCENARIO__", scenario), encoding="utf-8")
    result = subprocess.run(
        [node, str(check)], capture_output=True, text=True, cwd=tmp_path, timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_accepted_dump_releases_form_for_the_next_court(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const accepted = [];
  p.setFetch(async (url, opts) => {
    if (opts.method === "POST") {
      const sent = JSON.parse(opts.body);
      const key = "dump-" + (accepted.length + 1);
      accepted.push(job(key, { court_domain: sent.court_domain }));
      return response({ ok: true, executor: "vps", key });
    }
    return response(queueBody(accepted));
  });
  p.el("imp-paste").innerHTML = dump();
  await p.ctx.impSend();
  await settle();
  assert.equal(p.ctx.impSending, false, "Кнопка ждёт лишь подтверждения приёма");
  assert.equal(p.el("imp-paste").innerHTML, "", "Принятая вставка очищается сразу");
  assert.equal(accepted[0].status, "queued", "Первый суд ещё ждёт обработки");

  p.el("imp-court").value = "second--test.sudrf.ru|2";
  p.el("imp-paste").innerHTML = dump("second--test.sudrf.ru", 2);
  p.ctx.impUpdateSendState();
  assert.equal(p.el("imp-send").disabled, false, "Следующий суд доступен к отправке");
  await p.ctx.impSend();
  await settle();
  const posts = p.requests.filter(r => r.opts.method === "POST");
  assert.equal(posts.length, 2);
  assert.deepEqual(posts.map(r => JSON.parse(r.opts.body).court_domain),
    ["first--test.sudrf.ru", "second--test.sudrf.ru"], "В API уходит домен без srv_num");
  assert.match(p.el("imp-queue-list").innerHTML, /Первый районный суд/);
  assert.match(p.el("imp-queue-list").innerHTML, /Второй районный суд/);
  assert.equal(p.timers.size, 1, "Два дампа разделяют один таймер опроса");
""")


def test_completed_old_dump_keeps_new_paste_and_selected_file(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  let latest = job("old-dump");
  p.setFetch(async (_url, opts) => opts.method === "POST"
    ? response({ ok: true, executor: "vps", key: latest.uuid })
    : response(queueBody([latest])));
  p.el("imp-paste").innerHTML = dump();
  await p.ctx.impSend();
  await settle();
  const newPaste = dump("second--test.sudrf.ru", 2);
  const newFile = fileOf(newPaste, "second.html");
  p.el("imp-paste").innerHTML = newPaste;
  p.ctx.impSelectedFile = newFile;
  p.el("imp-file").value = "C:\\fakepath\\second.html";
  p.ctx.impUpdateSendState();
  latest = job("old-dump", { status: "done", added: 7, lines: ["[ADDED] 2-1/2026"] });
  await p.tick();
  assert.equal(p.el("imp-paste").innerHTML, newPaste,
    "Завершение старого дампа не стирает новую вставку");
  assert.equal(p.ctx.impSelectedFile, newFile, "Новый файл остаётся выбранным");
  assert.equal(p.el("imp-file").value, "C:\\fakepath\\second.html");
  assert.equal(p.el("imp-send").disabled, false);
  assert.match(p.el("imp-queue-list").innerHTML, /\+7 в картотеку/);
  assert.match(p.el("imp-queue-list").innerHTML, /2-1\/2026/);
""")


def test_dumps_and_case_batches_share_polling_and_keep_new_case_input(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const jobs = [];
  p.setFetch(async (url, opts) => {
    if (opts.method === "POST") {
      const point = url.includes("/add-case?");
      const key = point ? "case-batch" : "court-dump";
      jobs.push(job(key, point ? { kind: "case", items_count: 2 } : {}));
      return response({ ok: true, executor: "vps", key });
    }
    return response(queueBody(jobs));
  });
  p.el("imp-paste").innerHTML = dump();
  await p.ctx.impSend();
  p.el("ac-input").value = "2-42/2026\n2-43/2026";
  await p.ctx.acSend();
  await settle();
  assert.equal(p.ctx.acSending, false);
  assert.equal(p.el("ac-input").value, "", "Принятая пачка сразу освобождает ввод");
  assert.equal(p.timers.size, 1, "Дамп и пачка используют один таймер");
  p.el("ac-input").value = "2-44/2026";
  p.ctx.acUpdateState();
  jobs[1] = job("case-batch", {
    kind: "case", items_count: 2, status: "done", added_main: 2,
    lines: ["[ADDED] 2-42/2026", "[ADDED] 2-43/2026"],
  });
  const readsBefore = p.requests.filter(r => r.url.includes("/import-log?")).length;
  await p.tick();
  assert.equal(p.requests.filter(r => r.url.includes("/import-log?")).length, readsBefore + 1);
  assert.equal(p.el("ac-input").value, "2-44/2026", "Итог старой пачки сохраняет новый номер");
  assert.equal(p.el("ac-send").disabled, false);
  assert.equal(p.timers.size, 1, "Опрос продолжает следить за незавершённым дампом");
""")


def test_reload_recovers_tracked_job_even_outside_recent_history(tmp_path):
    run_scenario(tmp_path, r"""
  const storage = new Map();
  const oldId = "00000000-0000-4000-a000-000000000051";
  const p = makePage(role, storage);
  p.setFetch(async (_url, opts) => opts.method === "POST"
    ? response({ ok: true, executor: "vps", key: oldId })
    : response(queueBody([job(oldId)])));
  p.el("imp-paste").innerHTML = dump();
  await p.ctx.impSend();
  await settle();
  const restored = makePage(role, storage);
  restored.setFetch(async () => response({
    ...queueBody([]), tracked: [job(oldId, { status: "done", added: 9 })],
  }));
  await restored.ctx.loadImportLog(true);
  await settle();
  const request = restored.requests.find(r => r.url.includes("/import-log?"));
  assert.ok(request);
  const query = new URL(request.url, "https://worker.example").searchParams;
  assert.equal(query.get("include_queue"), "1");
  assert.ok((query.get("tracked") || "").split(",").includes(oldId),
    "Перезагрузка запрашивает сохранённый UUID за пределами последних 50 строк");
  assert.match(restored.el("imp-queue-list").innerHTML, /Первый районный суд/);
  assert.match(restored.el("imp-queue-list").innerHTML, /\+9 в картотеку/);
  assert.equal(restored.timers.size, 0, "У завершённой очереди нет постоянного опроса");
""")


def test_double_click_during_file_read_sends_one_dump(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const reading = deferred();
  let reads = 0;
  p.ctx.impSelectedFile = {
    name: "court.html", size: 2000,
    arrayBuffer() { reads++; return reading.promise; },
  };
  p.setFetch(async (_url, opts) => opts.method === "POST"
    ? response({ ok: true, executor: "vps", key: "only-once" })
    : response(queueBody([job("only-once")])));
  const first = p.ctx.impSend();
  const second = p.ctx.impSend();
  assert.equal(p.ctx.impSending, true, "Гард ставится до асинхронного чтения файла");
  assert.equal(reads, 1, "Повторный клик не начинает второе чтение");
  reading.resolve(new TextEncoder().encode(dump()).buffer);
  await Promise.all([first, second]);
  await settle();
  assert.equal(p.requests.filter(r => r.opts.method === "POST").length, 1);
  assert.equal(p.ctx.impSending, false);
  assert.equal(p.ctx.impSelectedFile, null, "Подтверждённый файл освобождает форму");
""")


@pytest.mark.parametrize("failure", ["http", "network", "read"])
def test_dump_acceptance_failure_preserves_input_and_unlocks(tmp_path, failure):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const original = dump();
  const selected = fileOf(original);
  p.el("imp-paste").innerHTML = original;
  p.ctx.impSelectedFile = selected;
  const failure = "__FAILURE__";
  if (failure === "read") selected.arrayBuffer = async () => { throw new Error("file unreadable"); };
  p.setFetch(async () => {
    if (failure === "network") throw new Error("network unavailable");
    return response({ ok: false, error: "Дамп отклонён" }, 400);
  });
  await p.ctx.impSend();
  await settle();
  assert.equal(p.el("imp-paste").innerHTML, original);
  assert.equal(p.ctx.impSelectedFile, selected);
  assert.equal(p.ctx.impSending, false, "Отказ приёма не оставляет форму заблокированной");
  assert.equal(p.el("imp-send").disabled, false);
  assert.equal(p.timers.size, 0, "Непринятое задание не запускает ожидание");
  assert.equal(Object.keys(p.ctx.impQueueJobs).length, 0);
""".replace("__FAILURE__", failure))


@pytest.mark.parametrize("input_kind", ["paste", "file", "case"])
def test_late_acceptance_does_not_clear_input_changed_while_sending(tmp_path, input_kind):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const accepting = deferred();
  p.setFetch(async (_url, opts) => opts.method === "POST"
    ? accepting.promise : response(queueBody([job("late-accept")] )));
  const kind = "__KIND__";
  let send;
  let nextFile;
  if (kind === "case") {
    p.el("ac-input").value = "2-1/2026";
    send = p.ctx.acSend();
    p.el("ac-input").value = "2-2/2026";
  } else {
    if (kind === "file") p.ctx.impSelectedFile = fileOf(dump(), "first.html");
    else p.el("imp-paste").innerHTML = dump();
    send = p.ctx.impSend();
    await settle();
    if (kind === "file") {
      nextFile = fileOf(dump("second--test.sudrf.ru", 2), "second.html");
      p.ctx.impSelectedFile = nextFile;
    }
    p.el("imp-paste").innerHTML = dump("second--test.sudrf.ru", 2);
  }
  accepting.resolve(response({ ok: true, executor: "vps", key: "late-accept" }));
  await send;
  await settle();
  if (kind === "case") {
    assert.equal(p.el("ac-input").value, "2-2/2026");
    assert.equal(p.ctx.acSending, false);
    assert.equal(p.el("ac-send").disabled, false);
  } else {
    assert.equal(p.el("imp-paste").innerHTML, dump("second--test.sudrf.ru", 2));
    if (kind === "file") assert.equal(p.ctx.impSelectedFile, nextFile);
    assert.equal(p.ctx.impSending, false);
    assert.equal(p.el("imp-send").disabled, false);
  }
""".replace("__KIND__", input_kind))


def test_case_double_submit_and_rejected_acceptance_preserve_batch(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const accepting = deferred();
  p.setFetch(async () => accepting.promise);
  const original = "2-11/2026\n2-12/2026";
  p.el("ac-input").value = original;
  const first = p.ctx.acSend();
  const second = p.ctx.acSend();
  assert.equal(p.ctx.acSending, true);
  assert.equal(p.requests.filter(r => r.opts.method === "POST").length, 1);
  accepting.resolve(response({ ok: false, error: "Не удалось принять пачку" }, 503));
  await Promise.all([first, second]);
  await settle();
  assert.equal(p.el("ac-input").value, original);
  assert.equal(p.ctx.acSending, false);
  assert.equal(p.el("ac-send").disabled, false);
  assert.equal(p.timers.size, 0);
  assert.equal(Object.keys(p.ctx.impQueueJobs).length, 0);
""")


def test_done_job_waiting_for_retry_stays_pending_in_queue_and_tile(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const id = "00000000-0000-4000-a000-000000000061";
  p.ctx.impRememberAccepted({ key: id, executor: "vps" }, {
    kind: "dump", court_domain: "first--test.sudrf.ru", operator: "Оператор",
  });
  const incomplete = job(id, {
    status: "done", queue_pending: true, fetch_fail: 2, source: "vps", executor: "vps",
  });
  p.setFetch(async () => response({ ...queueBody([incomplete]), queue: [incomplete] }));
  await p.ctx.loadImportLog(true);
  await settle();
  assert.equal(p.ctx.impQueuePending(p.ctx.impQueueJobs[id]), true);
  assert.match(p.el("imp-queue-count").textContent, /1 в работе/);
  assert.match(p.el("imp-queue-list").innerHTML, /ожидает повтора/);
  const tile = p.tiles.get("queue");
  assert.ok(tile, "Плитка очереди перерисована после загрузки журнала");
  assert.match(tile.value, /1 ждёт/, "done с непрочитанными карточками остаётся в счётчике");
  assert.equal(tile.color, "amber");
  assert.doesNotMatch(tile.sub, /все импорты обработаны/);
  assert.equal(p.timers.size, 1, "Повтор задания продолжает отслеживаться");
""")


def test_reload_retains_scalar_result_warnings_outside_recent_history(tmp_path):
    run_scenario(tmp_path, r"""
  const storage = new Map();
  const p = makePage(role, storage);
  const id = "00000000-0000-4000-a000-000000000062";
  p.ctx.impRememberAccepted({ key: id, executor: "vps" }, {
    kind: "dump", court_domain: "first--test.sudrf.ru", operator: "Оператор",
    court_label: "Первый районный суд — президиум",
  });
  const details = {
    status: "done", queue_pending: false, executor: "vps", source: "vps",
    needs_review: 2, skipped_region: 3, card_failed: 1,
    card_fail_reason: "HTTP 403 — страница защиты", fetch_fail: 1,
    section: "cassation", cassation_kind: "presidium",
    lines: ["[REVIEW] подробный отчёт остаётся на сервере"],
  };
  p.setFetch(async () => response({ ...queueBody([]), tracked: [job(id, details)] }));
  await p.ctx.loadImportLog(true);
  const resultBeforeReload = p.ctx.impResultHtml(p.ctx.impQueueJobs[id]);
  const saved = JSON.parse(storage.get("admin_import_jobs_v1"));
  assert.equal(saved.length, 1);
  for (const [field, value] of Object.entries(details)) {
    if (field !== "lines") assert.equal(saved[0][field], value, "Потеряно поле " + field);
  }
  assert.equal(Object.hasOwn(saved[0], "lines"), false, "Построчный отчёт не копируется в localStorage");

  const restored = makePage(role, storage);
  restored.setFetch(async () => response(queueBody([])));
  await restored.ctx.loadImportLog(true);
  await settle();
  for (const [field, value] of Object.entries(details)) {
    if (field !== "lines") assert.equal(restored.ctx.impQueueJobs[id][field], value);
  }
  assert.equal(restored.ctx.impResultHtml(restored.ctx.impQueueJobs[id]), resultBeforeReload,
    "Перезагрузка сохраняет исходную сводку результата со всеми предупреждениями");
  const summary = restored.el("imp-queue-list").innerHTML;
  assert.match(summary, /HTTP 403 — страница защиты/);
  assert.match(summary, /дел кассации \(президиум\) не заведено/);
  assert.match(summary, /повторит сервер в следующий слот/);
  assert.match(summary, /обработано сервером/);
  assert.doesNotMatch(summary, /новых дел нет — всё уже в базе/,
    "После reload неполный импорт нельзя превращать в безусловный успех");
  assert.doesNotMatch(summary, /подробный отчёт остаётся на сервере/);
""")


def test_polling_pauses_when_hidden_or_idle_and_keeps_tracking_on_errors(tmp_path):
    run_scenario(tmp_path, r"""
  const p = makePage(role);
  const id = "00000000-0000-4000-a000-000000000063";
  p.ctx.impRememberAccepted({ key: id, executor: "vps" }, {
    kind: "dump", court_domain: "first--test.sudrf.ru", operator: "Оператор",
  });
  p.setFetch(async () => response(queueBody([job(id)])));
  await p.ctx.loadImportLog(true);
  for (let i = 0; i < 3; i++) p.ctx.impScheduleQueuePoll();
  assert.equal(p.timers.size, 1, "Повторное планирование не плодит цепочки");

  p.ctx.document.hidden = true;
  p.ctx.impScheduleQueuePoll();
  assert.equal(p.timers.size, 0, "Скрытая вкладка не запрашивает KV");
  assert.equal(p.ctx.impQueuePending(p.ctx.impQueueJobs[id]), true);
  p.ctx.document.hidden = false;
  p.ctx.impScheduleQueuePoll();
  assert.equal(p.timers.size, 1);

  p.setFetch(async () => { throw new Error("journal temporarily unavailable"); });
  await p.tick();
  assert.equal(p.ctx.impQueuePending(p.ctx.impQueueJobs[id]), true,
    "Ошибка журнала не теряет квитанцию приёма");
  assert.match(p.el("imp-queue-list").innerHTML, /Первый районный суд/);
  assert.match(p.el("imp-queue-note").textContent, /Не удалось обновить статусы/);
  assert.equal(p.timers.size, 1, "После ошибки остаётся одна повторная попытка");

  p.advance(20 * 60 * 1000 + 1);
  const requestsBeforePause = p.requests.length;
  p.ctx.impScheduleQueuePoll();
  assert.equal(p.timers.size, 0, "Забытая вкладка останавливает polling через 20 минут");
  assert.equal(p.requests.length, requestsBeforePause);
  assert.equal(p.ctx.impQueuePending(p.ctx.impQueueJobs[id]), true);

  p.setFetch(async () => response(queueBody([job(id)])));
  p.el("imp-queue-refresh").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(p.requests.length, requestsBeforePause + 1, "Кнопка обновляет очередь вручную");
  assert.equal(p.timers.size, 1, "Ручное обновление продлевает окно опроса");
  assert.equal(p.ctx.impQueuePending(p.ctx.impQueueJobs[id]), true);
""")
