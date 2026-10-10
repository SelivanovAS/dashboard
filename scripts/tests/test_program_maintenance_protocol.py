"""Real pipe disconnects around durable intents; no production or network I/O."""
import io
import json
from pathlib import Path
import sys

import pytest

import program_maintenance_protocol as p

NONCE = 'e' * 32
BASE = 'a' * 40
TARGET = 'b' * 40
SOURCE = 'c' * 40
RELEASE = 'd' * 64
MANIFEST = '6' * 64


def bootstrap(state, *, lost_ack=False, slow=False, registration=False, lost_registration_ack=False):
    scripts = Path(__file__).resolve().parents[1]
    code = f'''import sys, os, time
from pathlib import Path
sys.path.insert(0, {str(scripts)!r})
sys.path.insert(0, {str(scripts / 'tests')!r})
from test_program_maintenance import Hooks, m
from program_maintenance_protocol import serve
class RegistrationHooks(Hooks):
    # Synthetic framing/ACK fixture. Connected tests separately import real Git.
    def register_target(self, journal, **fields):
        import hashlib, json
        assert hashlib.sha256(fields['bundle_bytes']).hexdigest() == fields['bundle_sha256']
        receipt = dict(nonce=journal['nonce'], target_id=journal['active_target_id'],
                       base_sha=fields['base_sha'], target_sha=fields['target_sha'],
                       bundle_sha256=fields['bundle_sha256'])
        path = Path({str(state)!r}) / 'registration.json'
        with path.open('w') as stream:
            json.dump(receipt, stream); stream.flush(); os.fsync(stream.fileno())
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
        return {{'evidence_sha256': '7' * 64}}
    def validate_publication(self, journal, attempt):
        import json
        record = json.loads((Path({str(state)!r}) / 'registration.json').read_text())
        assert record['nonce'] == journal['nonce']
        assert all(record[key] == attempt[key] for key in ('target_id', 'base_sha', 'target_sha'))
class Lease(m.MaintenanceLease):
    def prepare(self, nonce):
        if {slow!r}: time.sleep(1)
        return super().prepare(nonce)
class Writer:
    def write(self, value):
        if {lost_ack!r} and b'"attempt_id"' in value: os._exit(23)
        if {lost_registration_ack!r} and b'"bundle_sha256"' in value: os._exit(24)
        return sys.stdout.buffer.write(value)
    def flush(self): sys.stdout.buffer.flush()
serve(Lease(Path({str(state)!r}), RegistrationHooks() if {registration!r} else Hooks()), writer=Writer())
'''
    return code.encode()


def reserve(client):
    return client.request('reserve', region='hmao', release_id=RELEASE,
                          source_commit=SOURCE, manifest_sha256=MANIFEST, nonce=NONCE)


def test_real_pipe_push_callback_only_after_durable_exact_intent(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        def push(target):
            journal = json.loads((state / 'journal.json').read_text())
            assert (state / 'blocked').is_file()
            assert journal['phase'] == 'publish_intent'
            assert journal['attempts'] == [{'attempt_id': 1, 'target_id': 1,
                                            'base_sha': BASE, 'target_sha': target}]
            return 'accepted'
        permit, result = client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=push)
        assert result == 'accepted' and permit['target_id'] == 1
        client.request('begin_apply', nonce=NONCE, attempt_id=permit['attempt_id'])
        client.request('mark_verified', nonce=NONCE)
        finished = client.request('finish', nonce=NONCE)
        assert finished['outcome'] == 'installed'
    assert not (state / 'blocked').exists()


def test_real_pipe_requires_registration_before_exact_publication_ack(tmp_path):
    state = tmp_path.resolve() / 'state'
    called = []
    with p.LeaseClient.local(bootstrap(state, registration=True)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        with pytest.raises(p.MaintenanceProtocolError):
            client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=called.append)
    assert called == []
    assert json.loads((state / 'journal.json').read_text())['attempts'] == []
    with p.LeaseClient.local(bootstrap(state, registration=True)) as client:
        client.request('recover', nonce=NONCE)
        proof = client.register_target(nonce=NONCE, base_sha=BASE, target_sha=TARGET,
                                       bundle=b'synthetic pipe payload')
        assert proof['target_id'] == 1
        assert json.loads((state / 'registration.json').read_text())['target_sha'] == TARGET
        assert json.loads((state / 'journal.json').read_text())['attempts'] == []
        client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=called.append)
    assert called == [TARGET] and (state / 'blocked').exists()


def test_lost_registration_ack_preserves_receipt_but_grants_no_push(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state, registration=True, lost_registration_ack=True)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        with pytest.raises(p.MaintenanceProtocolError):
            client.register_target(nonce=NONCE, base_sha=BASE, target_sha=TARGET, bundle=b'payload')
    assert (state / 'registration.json').is_file() and (state / 'blocked').is_file()
    assert json.loads((state / 'journal.json').read_text())['attempts'] == []
    called = []
    with p.LeaseClient.local(bootstrap(state, registration=True)) as client:
        client.request('recover', nonce=NONCE)
        client.register_target(nonce=NONCE, base_sha=BASE, target_sha=TARGET, bundle=b'payload')
        client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=called.append)
    assert called == [TARGET]


def test_oversized_bundle_fails_before_transport_or_publication(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state, registration=True)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        before = client.sequence
        with pytest.raises(p.MaintenanceProtocolError, match='512 KiB'):
            client.register_target(nonce=NONCE, base_sha=BASE, target_sha=TARGET,
                                   bundle=b'x' * (p.MAX_TARGET_BUNDLE_BYTES + 1))
        assert client.sequence == before and client.process is not None
        assert json.loads((state / 'journal.json').read_text())['attempts'] == []


def test_maximum_registration_fits_single_json_frame(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state, registration=True)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        client.register_target(nonce=NONCE, base_sha=BASE, target_sha=TARGET,
                               bundle=b'x' * p.MAX_TARGET_BUNDLE_BYTES)
        assert client.sequence == 3


def test_disconnect_after_intent_never_cancels_reservation(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=lambda target: None)
    assert (state / 'blocked').exists()
    with p.LeaseClient.local(bootstrap(state)) as recovered:
        assert recovered.request('recover', nonce=NONCE)['phase'] == 'publish_intent'
        with pytest.raises(p.MaintenanceProtocolError):
            recovered.request('cancel_before_publish', nonce=NONCE)
    assert (state / 'blocked').exists()


def test_lost_intent_ack_does_not_execute_push(tmp_path):
    state = tmp_path.resolve() / 'state'
    called = []
    with p.LeaseClient.local(bootstrap(state, lost_ack=True)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        with pytest.raises(p.MaintenanceProtocolError):
            client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=called.append)
    assert called == []
    assert json.loads((state / 'journal.json').read_text())['phase'] == 'publish_intent'
    assert (state / 'blocked').exists()


def test_ambiguous_push_exception_keeps_marker(tmp_path):
    state = tmp_path.resolve() / 'state'
    def uncertain(target):
        raise TimeoutError('simulated lost receive-pack acknowledgement')
    with p.LeaseClient.local(bootstrap(state)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        with pytest.raises(TimeoutError):
            client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=uncertain)
    assert (state / 'blocked').exists()


def test_timeout_during_prepare_closes_only_transport_and_preserves_gate(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state, slow=True)) as client:
        reserve(client)
        client.timeout = .05
        with pytest.raises(p.MaintenanceProtocolError, match='ожидание'):
            client.request('prepare', nonce=NONCE)
        assert client.process is None
    assert (state / 'blocked').exists()


def test_recovery_rollback_over_same_connection_keeps_old_attempt(tmp_path):
    state = tmp_path.resolve() / 'state'
    with p.LeaseClient.local(bootstrap(state)) as client:
        reserve(client)
        client.request('prepare', nonce=NONCE)
        client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=lambda _: None)
        client.request('select_recovery_target', nonce=NONCE, release_id='8' * 64,
                       source_commit=SOURCE, manifest_sha256='9' * 64, reason='rollback')
        permit, _ = client.publish(nonce=NONCE, base_sha=TARGET, target_sha='f' * 40, push=lambda _: None)
        assert permit['target_id'] == 2 and permit['attempt_id'] == 2
        client.request('begin_apply', nonce=NONCE, attempt_id=2)
        client.request('mark_verified', nonce=NONCE)
        result = client.request('finish', nonce=NONCE)
    assert len(result['attempts']) == 2
    assert result['safe_evidence']['covered_attempt_ids'] == [1, 2]


class FakeLease:
    entered = False
    def __enter__(self): self.entered = True; return self
    def __exit__(self, *args): self.entered = False
    def inspect(self): return {'blocked': True}


def request(action='inspect', args=None, sequence=1):
    return p.frame({'protocol': p.PROTOCOL, 'id': sequence, 'action': action, 'args': args or {}})


@pytest.mark.parametrize('body', [
    request('__exit__'), request('inspect', {'unexpected': True}), request(sequence=2),
    b'{"id":1,"id":2}\n', b'{"number":NaN}\n', b'[]\n', b'{}',
])
def test_server_fails_closed_on_unknown_or_malformed_requests(body):
    output = io.BytesIO()
    lease = FakeLease()
    p.serve(lease, io.BytesIO(body), output)
    lines = [json.loads(line) for line in output.getvalue().splitlines()]
    assert lines[0]['ready'] and not lines[-1]['ok']
    assert not lease.entered


def test_server_does_not_reflect_arbitrary_hook_error_content():
    class BadLease(FakeLease):
        def inspect(self): raise RuntimeError('secret transport value')
    output = io.BytesIO()
    p.serve(BadLease(), io.BytesIO(request()), output)
    assert b'secret transport value' not in output.getvalue()


@pytest.mark.parametrize('answer', [
    {'protocol': p.PROTOCOL, 'id': True, 'ok': True, 'result': {}},
    {'protocol': p.PROTOCOL, 'id': 1.0, 'ok': True, 'result': {}},
    {'protocol': p.PROTOCOL, 'id': 2, 'ok': True, 'result': {}},
    {'protocol': 'legacy/1', 'id': 1, 'ok': True, 'result': {}},
    {'protocol': p.PROTOCOL, 'id': 1, 'ok': True, 'result': {'target_sha': 'f' * 40}},
])
def test_foreign_reply_or_permit_never_calls_push(answer):
    code = ('import sys\n'
            f'sys.stdout.buffer.write({p.frame({"protocol":p.PROTOCOL,"ready":True})!r}); sys.stdout.flush()\n'
            'sys.stdin.buffer.readline()\n'
            f'sys.stdout.buffer.write({p.frame(answer)!r}); sys.stdout.flush()\n')
    called = []
    with p.LeaseClient.local(code.encode()) as client:
        with pytest.raises(p.MaintenanceProtocolError):
            client.publish(nonce=NONCE, base_sha=BASE, target_sha=TARGET, push=called.append)
    assert not called


def test_numeric_hello_is_not_protocol_confirmation():
    code = ('import sys\n'
            f'sys.stdout.buffer.write({p.frame({"protocol": p.PROTOCOL, "ready": 1})!r}); sys.stdout.flush()\n'
            'sys.stdin.buffer.readline()\n')
    with pytest.raises(p.MaintenanceProtocolError):
        p.LeaseClient.local(code.encode())


def test_remote_command_contains_no_transferred_source_or_interpolated_host(tmp_path, monkeypatch):
    key = tmp_path / 'identity'
    key.write_text('fixture-placeholder')
    observed = {}
    def fake_init(self, argv, bootstrap, **kwargs):
        observed.update(argv=argv, bootstrap=bootstrap)
    monkeypatch.setattr(p.LeaseClient, '__init__', fake_init)
    p.LeaseClient.ssh(b'payload-not-shell', host='root@127.0.0.1', identity=key)
    assert 'payload-not-shell' not in ' '.join(observed['argv'])
    assert observed['argv'][-2] == 'root@127.0.0.1'
    assert observed['bootstrap'] == b'payload-not-shell'
    with pytest.raises(p.MaintenanceProtocolError):
        p.LeaseClient.ssh(b'x', host='host;false', identity=key)
