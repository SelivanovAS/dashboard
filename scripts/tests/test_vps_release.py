"""Маршрутизация выпуска без сетевых запросов, писем или отправки Telegram."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

path = Path(__file__).resolve().parents[2] / 'ops/vps-run/local_release.py'
spec = importlib.util.spec_from_file_location('vps_release', path)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def repository(tmp, monkeypatch):
    repo = tmp / 'region'
    repo.mkdir()
    monkeypatch.chdir(repo)
    subprocess.run(['git', 'init', '-q'], check=True)
    (repo / 'data').mkdir()
    (repo / 'data/last_digest_context.json').write_text('{"issue_key":"edition"}')
    (repo / 'data/last_digest.json').write_text('{"issues":[]}')
    (repo / 'data/unrelated.json').write_text('original')
    release.git('add', '.')
    release.git('-c', 'user.name=test', '-c', 'user.email=test@invalid', 'commit', '-qm', 'start')
    state = tmp / 'state'
    state.mkdir()
    return repo, state


def test_local_render_commit_retry_and_reserve(tmp_path, monkeypatch):
    repo, state = repository(tmp_path, monkeypatch)
    start = release.git('rev-parse', 'HEAD')
    value = {'issue_key': 'edition', 'region': 'hmao', 'summaries': [{'status': 'network'}]}
    def render(region, draft, target):
        (draft / 'data/last_digest.json').write_text('{"issues":[{"key":"edition","html":"готово"}]}')
        (draft / 'data/unrelated.json').write_text('must stay inside the temporary checkout')
        return value
    monkeypatch.setattr(release, 'render', render)
    def prepare():
        return release.prepare(repo, 'hmao', state, 'hmao:edition')
    assert prepare() == 10
    assert release.git('rev-parse', 'HEAD') == start and not (state / 'job.json').exists()
    value['summaries'] = [{'status': 'ready'}]
    assert prepare() == 0
    assert (repo / 'data/unrelated.json').read_text() == 'original'
    head = release.git('rev-parse', 'HEAD')
    assert head != start and prepare() == 0 and head == release.git('rev-parse', 'HEAD')
    assert not release.git('status', '--porcelain')


def test_wrong_artifact_and_offline_deadline(tmp_path, monkeypatch):
    job = {'created': 1000, 'marker': 'c' * 40}
    release.save(tmp_path / 'job.json', job)
    release.save(tmp_path / 'render.json', {'issue_key': 'A', 'region': 'hmao'})
    monkeypatch.setattr(release, 'github_report', lambda *args: {
        'issue_key': 'B', 'region': 'hmao', 'publication': {'data': 'confirmed'}})
    monkeypatch.setattr(release.time, 'time', lambda: 2199)
    monkeypatch.setattr(release.report, 'telegram_parts', lambda value: ['part'])
    monkeypatch.setattr(release, 'deliver_parts', lambda state, value: True)
    release.drain(tmp_path, 'SelivanovAS/dashboard', 'hmao')
    assert not (tmp_path / 'telegram.json').exists()
    monkeypatch.setattr(release.time, 'time', lambda: 2201)
    release.drain(tmp_path, 'SelivanovAS/dashboard', 'hmao')
    assert release.load(tmp_path / 'final.json')['publication']['data'] == 'not_confirmed'
    assert release.load(tmp_path / 'job.json')['complete']


def test_uncertain_send_does_not_repeat_prior_parts(tmp_path, monkeypatch):
    calls = []
    frozen = {'parts': ['a', 'b', 'c'], 'receipts': [{}, {}, {}]}
    monkeypatch.setattr(release.vps_telegram, 'config', lambda path: {'chat_id': 'test', 'token': 'test'})
    def request(method, token, fields, before_post):
        if fields['text'] == 'b' and not calls.count('waiting'):
            calls.append('waiting')
            return {'ok': False, 'reason': 'no_connection'}
        before_post()
        calls.append(fields['text'])
        return {'ok': True} if fields['text'] == 'a' else {'ok': False, 'reason': 'unconfirmed'}
    monkeypatch.setattr(release.vps_telegram, 'request', request)
    assert not release.deliver_parts(tmp_path, frozen)
    assert not release.deliver_parts(tmp_path, release.load(tmp_path / 'telegram.json'))
    assert not release.deliver_parts(tmp_path, release.load(tmp_path / 'telegram.json'))
    assert calls == ['a', 'waiting', 'b']


def test_cloud_verifies_exact_issue_hash(tmp_path, monkeypatch):
    repo, _ = repository(tmp_path, monkeypatch)
    def write(name, data):
        (repo / name).write_text(json.dumps(data))
    write('.program-release.json', {'region': 'hmao'})
    (repo / 'REGION').write_text('hmao')
    write('data/last_digest_context.json', {'issue_key': 'edition', 'delivery_id': 'hmao:edition'})
    write('data/last_digest.json', {'issues': [{'key': 'edition', 'html': 'test'}]})
    sha = hashlib.sha256((repo / 'data/last_digest.json').read_bytes()).hexdigest()
    release.git('add', '.')
    release.git('-c', 'user.name=test', '-c', 'user.email=test@invalid', 'commit', '-qm',
                '(Mac-парсинг) [vps-release ' + sha + ']')
    monkeypatch.setattr(release.sys, 'argv', ['local_release', 'verify'])
    monkeypatch.setattr(release.report, 'begin', lambda mode: None)
    monkeypatch.setattr(release.report, 'record_digest', lambda *a, **k: None)
    assert release.main() == 0
    (repo / 'data/last_digest.json').write_text('{"issues":[]}')
    with pytest.raises(ValueError):
        release.main()


def test_telegram_handshake_both_routes_before_http(monkeypatch):
    from unittest.mock import MagicMock
    transport = release.vps_telegram
    raw, secure = MagicMock(), MagicMock()
    monkeypatch.setattr(transport.socket, 'create_connection', lambda *a, **k: raw)
    tls_context = MagicMock()
    tls_context.wrap_socket.return_value = secure
    monkeypatch.setattr(transport.ssl, 'create_default_context', lambda: tls_context)
    reserve = MagicMock()
    monkeypatch.setattr(transport, 'GithubCaptchaClient', lambda: reserve)
    monkeypatch.setattr(transport, 'send_message', lambda *a: None)
    monkeypatch.setattr(transport, 'receive_message', lambda *a: {'type': 'connected'})
    for backup, expected in [(False, 'vps'), (True, 'github')]:
        with transport.connection(backup_only=backup) as (http, route):
            assert http.sock is secure and route == expected
    assert reserve.start.call_count == 1


def test_local_push_owns_issue_across_retry_and_cloud_is_never_called(tmp_path, monkeypatch):
    region = 'hmao'
    repo, state = repository(tmp_path, monkeypatch)
    def render(reg, draft, target):
        (draft / 'data/last_digest.json').write_text('{"issues":[{"key":"edition","html":"ok"}]}')
        return {'region':reg, 'issue_key':'edition', 'summaries':[]}
    monkeypatch.setattr(release, 'render', render)
    monkeypatch.setenv('CM_VPS_WEB_PUSH','1')
    monkeypatch.setattr(release.local_webpush, 'select', lambda *a: 'a' * 40)
    assert release.prepare(repo, region, state, 'hmao:edition') == 0
    job = release.load(state / 'job.json')
    assert job['push_owner'] == 'vps' and job['push_revision'] == 'a' * 40
    monkeypatch.setattr(release.local_webpush,'select',lambda *a:pytest.fail('second selection'))
    assert release.prepare(repo, region, state, 'hmao:edition') == 0
    monkeypatch.setattr(release, 'github_report', lambda *a: pytest.fail('unexpected cloud path'))
    monkeypatch.setattr(release.local_webpush,'deliver',lambda *a:{'status':'no_subscriptions'})
    monkeypatch.setattr(release.report, 'telegram_parts', lambda value:['test'])
    monkeypatch.setattr(release, 'deliver_parts', lambda *a: True)
    job['marker'] = 'b' * 40
    release.save(state / 'job.json',job)
    release.drain(state,'SelivanovAS/dashboard',region)
    assert release.load(state / 'final.json')['publication']['site'] == 'vps'


def test_receipts_prevent_uncertain_or_accepted_repeat(tmp_path):
    from court_monitor.webpush_receipts import Receipts, valid_endpoint
    ledger = tmp_path / 'private.sqlite'
    ep = 'https://web.push.apple.com/example'
    assert valid_endpoint(ep)
    assert not valid_endpoint('https://web.push.apple.com@localhost/x')
    assert not valid_endpoint('https://127.0.0.1/x')
    assert Receipts(ledger).claim(ep) is None
    assert Receipts(ledger).claim(ep) == 'unconfirmed'
    Receipts(ledger).finish(ep, 'accepted')
    assert Receipts(ledger).claim(ep) == 'accepted'
