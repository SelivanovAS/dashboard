"""Tyumen source URL aliases keep one court identity at every import boundary.

All data is synthetic and stored under pytest tmp_path; HTTP, real KV and
delivery are never used. The existing VM harnesses execute the actual Worker
handler and the rendered owner/operator import forms.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys

import pytest
import requests

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import import_search_dump as importer
from court_monitor import config, courts, targeted_add
from court_monitor.regions import get_region
from test_admin_import_queue import run_scenario
from test_import_queue_api import run_worker
from test_import_search_dump import import_env, _read_summary


def aliases(domain: str) -> tuple[str, str]:
    original = domain.replace("--tum.sudrf.ru", ".tum.sudrf.ru")
    return original, original.replace(".tum.sudrf.ru", "--tum.sudrf.ru")


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Real HTTP is forbidden in domain alias tests")

    monkeypatch.setattr(requests.Session, "request", forbidden)


@pytest.fixture
def tyumen(monkeypatch):
    monkeypatch.setattr(config, "REGION", "tyumen")
    return get_region()


def card_url(host: str, delo_id: int = 1540005) -> str:
    return (f"https://{host}/modules.php?name=sud_delo&name_op=case"
            f"&srv_num=1&case_id=123&case_uid=aaaa-1234&delo_id={delo_id}")


def test_all_tyumen_original_and_legacy_urls_resolve_to_the_same_court(tyumen):
    for court in (*tyumen.first_instance_courts, *tyumen.appeal_courts):
        for host in aliases(court.domain):
            assert courts.canon_sudrf_domain(host) == court.domain
            assert importer.resolve_court(host, court.delo_id) is court
            if court.court_type == "first_instance":
                assert courts.fi_court_by_domain(host, court.srv_num) is court
                target, error = targeted_add.resolve_link_target(
                    targeted_add.parse_card_link(card_url(host, court.delo_id)))
                assert target is court and not error
    for host in ("leninsky.hmao.sudrf.ru", "leninsky.svd.sudrf.ru",
                 "unknown.tum.sudrf.ru", "centralny.ynao.sudrf.ru"):
        assert importer.resolve_court(host) is None
        target, error = targeted_add.resolve_link_target(
            targeted_add.parse_card_link(card_url(host)))
        assert target is None and "не из нашего региона" in error


@pytest.mark.parametrize("source_form", [0, 1])
@pytest.mark.parametrize("selected_form", [0, 1])
def test_python_import_aliases_share_one_stored_identity(
        import_env, monkeypatch, source_form, selected_form):
    monkeypatch.setattr(config, "REGION", "tyumen")
    court = get_region().first_instance_courts[0]
    original = (TESTS / "fixtures/search_fi_all_roles.html").read_text(encoding="utf-8")
    host = aliases(court.domain)[source_form]
    html = original.replace('href="/modules.php', f'href="https://{host}/modules.php')
    html = f'<!-- saved from url=(0080)https://{host}/modules.php?name=sud_delo -->\n' + html
    import_env["dump"].write_text(html, encoding="utf-8")
    for selected in (aliases(court.domain)[selected_form], aliases(court.domain)[1 - selected_form]):
        rc = importer.main([str(import_env["dump"]), "--court-domain", selected,
                            "--operator", "Тестовый оператор"])
        assert rc == importer.EXIT_OK
    cases = json.loads(import_env["json"].read_text(encoding="utf-8"))["cases"]
    assert len(cases) == 2
    assert {c["first_instance"]["court_domain"] for c in cases} == {court.domain}
    assert _read_summary(import_env["gh_out"])["added"] == 0


@pytest.mark.parametrize("foreign", ["centralny.tum.sudrf.ru", "leninsky.svd.sudrf.ru"])
def test_python_rejects_another_court_before_reading_cards(import_env, monkeypatch, foreign):
    monkeypatch.setattr(config, "REGION", "tyumen")
    import_env["dump"].write_text(f'<a href="{card_url(foreign)}">2-123/2026</a>', encoding="utf-8")
    rc = importer.main([str(import_env["dump"]), "--court-domain", "leninsky.tum.sudrf.ru"])
    assert rc == importer.EXIT_WRONG_COURT
    assert import_env["card_calls"]["n"] == 0
    assert not import_env["json"].exists()


@pytest.mark.skipif(shutil.which("node") is None, reason="Node is required for the Worker test")
def test_worker_accepts_both_aliases_and_rejects_wrong_court_before_kv(tyumen):
    court = tyumen.first_instance_courts[0]
    result = run_worker("const own = " + json.dumps(court.domain) + ";\n"
                        + "const aliases = " + json.dumps(aliases(court.domain)) + ";\n" + r"""
const kv = kvStore(), env = environment(kv);
globalThis.fetch = async () => { throw new Error('Network is forbidden'); };
const html = host => '<a href="https://' + host
  + '/modules.php?name=sud_delo&name_op=case&case_id=123&delo_id=1540005">2-123/2026</a>'
  + 'Выдача суда '.repeat(150);
for (const selected of aliases) for (const source of aliases) {
  const request = new Request('https://worker.invalid/admin/import-dump?secret=operator', {
    method: 'POST', body: JSON.stringify({court_domain: selected, html: html(source)})});
  const response = await workerExport.fetch(request, env);
  assert.equal(response.status, 200, await response.text());
}
const logs = [...kv.data.keys()].filter(k => k.startsWith('import:log:'));
assert.equal(logs.length, 4);
assert.ok(logs.every(k => JSON.parse(kv.data.get(k)).court_domain === own));
const before = kv.puts.length;
for (const foreign of ['centralny.tum.sudrf.ru', 'leninsky.svd.sudrf.ru']) {
  const request = new Request('https://worker.invalid/admin/import-dump?secret=operator', {
    method: 'POST', body: JSON.stringify({court_domain: own, html: html(foreign)})});
  const response = await workerExport.fetch(request, env);
  assert.equal(response.status, 400);
}
assert.equal(kv.puts.length, before, 'Rejected courts must not create jobs or dumps');
output({ok: true});
""")
    assert result == {"ok": True}


def test_owner_and_operator_form_accept_aliases_and_protect_chosen_court(tmp_path, tyumen):
    info = json.dumps(tyumen.public_info(), ensure_ascii=False)
    run_scenario(tmp_path, "  const region = " + info + ";\n" + r"""
  const p = makePage(role);
  p.ctx.acRegion = region;
  p.ctx.impCourts = region.fi_courts;
  p.ctx.impCourtNameByDomain = Object.fromEntries(region.fi_courts.map(c => [c.domain, c.name]));
  const own = region.fi_courts[0];
  const original = own.domain.replace('--tum.sudrf.ru', '.tum.sudrf.ru');
  const aliases = [original, original.replace('.tum.sudrf.ru', '--tum.sudrf.ru')];
  p.el('imp-court').value = own.domain + '|1';
  const accepted = [];
  p.setFetch(async (url, opts) => {
    if (opts.method === 'POST') {
      const sent = JSON.parse(opts.body); accepted.push(sent);
      return response({ok: true, key: 'alias-' + accepted.length, uuid: 'alias-' + accepted.length});
    }
    return response({items: [], queue: [], imports_available: true});
  });
  for (const host of aliases) {
    const url = 'https://' + host + '/modules.php?name=sud_delo&name_op=case&case_id=123&case_uid=aaaa-1234&delo_id=1540005';
    assert.equal(p.ctx.acCheckLink(url), '', 'Original and legacy card URLs resolve to Tyumen');
    p.el('imp-paste').innerHTML = dump(host);
    await p.ctx.impSend(); await settle();
  }
  assert.equal(accepted.length, 2);
  assert.ok(accepted.every(body => body.court_domain === own.domain));
  assert.match(p.ctx.acCheckLink('https://leninsky.svd.sudrf.ru/modules.php?case_id=123'), /не из нашего региона/);
  p.el('imp-paste').innerHTML = dump('centralny.tum.sudrf.ru');
  await p.ctx.impSend(); await settle();
  assert.equal(accepted.length, 2, 'Another local court must not be submitted under the selected court');
  assert.match(p.el('imp-status').innerHTML, /страница другого суда/);
""")
