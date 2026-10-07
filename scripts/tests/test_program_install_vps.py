"""Установка проверяет код до checkout и сохраняет безопасное состояние VPS."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

SPEC = importlib.util.spec_from_file_location("program_install", Path(__file__).parents[1] / "program_install_vps.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
SHA = "a" * 40
SOURCE = "b" * 40


def sign(document):
    document = copy.deepcopy(document)
    document.pop("release_id", None)
    canonical = (json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    document["release_id"] = hashlib.sha256(canonical).hexdigest()
    return document


def document(files):
    return sign({"schema_version": 1, "region": "hmao", "source_commit": SOURCE,
                 "kind": "program", "source_repo": "SelivanovAS/dashboard",
                 "repository": "SelivanovAS/dashboard", "profile_sha256": "c" * 64,
                 "baseline_sha256": "d" * 64, "files": files})


def setup_remote(monkeypatch, tmp_path, *, busy=False, journal=False, wrong_region=False):
    calls = []
    repo = tmp_path / 'dashboard'
    for name in m.REPOSITORIES.values():
        local = tmp_path / name / 'ops/mac-local-run'
        (local / '.runtime').mkdir(parents=True)
        (local / 'run_lock.py').write_text('# lock protocol stub\n')
    (repo / 'program.py').write_text('print(1)\n')
    lock = document({'program.py': {'sha256': hashlib.sha256((repo / 'program.py').read_bytes()).hexdigest(), 'mode': '100644'}})
    if journal:
        (repo / 'ops/mac-local-run/.runtime/delivery_txn.json').write_text('{}')
    def fake_git(cwd, *args):
        calls.append(['git', *args])
        if args[0] == 'branch': return 'main'
        if args[0] == 'status': return ''
        if args[0] == 'show': return json.dumps(lock)
        if args[0] == 'rev-parse': return SHA
        if args[0] == 'diff-tree': return '.program-release.json\nprogram.py'
        return ''
    def fake_run(args, **kwargs):
        calls.append(args)
        if args[:2] == ['systemctl', 'is-active']:
            return SimpleNamespace(returncode=1 if args[-1] == 'court-retry.timer' else 0, stdout='', stderr='')
        if args[:2] == ['systemctl', 'show']:
            state = '0' if '--property=Job' in args else ('activating' if busy else 'inactive')
            return SimpleNamespace(returncode=0, stdout=state, stderr='')
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    def preflight(repo, revision, lock, region):
        calls.append(['preflight', revision])
        if wrong_region:
            raise m.InstallError('не подтверждён регион запуска')
    monkeypatch.setattr(m, 'git', fake_git)
    monkeypatch.setattr(m, 'run', fake_run)
    monkeypatch.setattr(m, 'preflight_revision', preflight)
    return calls, lock


def test_success_uses_all_locks_and_restores_only_original_timers(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    result = m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert result['installed'] and not result['normal_cycle_verified']
    assert len([a for a in calls if 'acquire' in a]) == 4
    assert len([a for a in calls if 'release' in a]) == 4
    restore = next(a for a in calls if a[:2] == ['systemctl', 'start'])
    assert 'court-retry.timer' not in restore and len(restore[2:]) == 4
    original_services = {name + '.service' for name in m.SERVICES}
    assert not any(a[:2] == ['systemctl', 'stop'] and set(a[2:]) & original_services for a in calls)
    assert not any('enable' in a or '--force' in a or '--hard' in a for a in calls)
    assert calls.index(['preflight', SHA]) < next(i for i, a in enumerate(calls) if a[:2] == ['systemctl', 'stop'])
    assert next(i for i, a in enumerate(calls) if a[:2] == ['git', 'status']) > max(i for i, a in enumerate(calls) if 'acquire' in a)
    recovery = next(a for a in calls if a[0] == 'systemd-run')
    assert '/usr/bin/flock' in recovery and str(tmp_path / '.program-install.lock') in recovery
    assert not list(tmp_path.glob('.program-install-*.json'))


def test_busy_service_restores_timers_without_mutating_checkout(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path, busy=True)
    with pytest.raises(m.InstallError, match='Службы заняты'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path, timeout=0)
    assert any(a[:2] == ['systemctl', 'start'] for a in calls)
    assert not any(a[:2] == ['git', 'merge'] for a in calls)


def test_pending_delivery_is_left_for_normal_service(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path, journal=True)
    with pytest.raises(m.InstallError, match='Незавершённая транзакция'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert (tmp_path/'dashboard/ops/mac-local-run/.runtime/delivery_txn.json').read_text() == '{}'
    assert len([a for a in calls if 'release' in a]) == 1
    assert any(a[:2] == ['systemctl', 'start'] for a in calls)
    assert not any(a[:2] == ['git', 'merge'] for a in calls)


def test_wrong_effective_region_fails_before_stopping_timers_or_checkout(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path, wrong_region=True)
    with pytest.raises(m.InstallError, match='регион запуска'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert not any(a[0] == 'systemctl' or 'acquire' in a or a[:2] == ['git', 'merge'] for a in calls)


def test_source_version_drift_while_waiting_is_not_installed(monkeypatch, tmp_path):
    calls, lock = setup_remote(monkeypatch, tmp_path)
    previous = m.git
    current = 'e' * 40
    reads = 0
    def git(repo, *args):
        nonlocal reads
        if args == ('rev-parse', 'origin/main'):
            reads += 1
            return SHA if reads == 1 else current
        if args == ('show', f'{current}:{m.STAMP}'):
            newer = dict(lock, source_commit='f' * 40)
            return json.dumps(sign(newer))
        return previous(repo, *args)
    monkeypatch.setattr(m, 'git', git)
    with pytest.raises(m.InstallError, match='изменилась во время ожидания'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert not any(a[:2] == ['git', 'merge'] for a in calls)
    assert any(a[:2] == ['systemctl', 'start'] for a in calls)


def test_new_data_head_with_same_version_is_preflighted_and_preserved(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    previous = m.git
    current = 'e' * 40
    reads = 0
    def git(repo, *args):
        nonlocal reads
        if args == ('rev-parse', 'origin/main'):
            reads += 1
            return SHA if reads == 1 else current
        return previous(repo, *args)
    monkeypatch.setattr(m, 'git', git)
    result = m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert result['head'] == current
    assert ['preflight', current] in calls
    assert ['git', 'merge', '--ff-only', current] in calls


def test_incomplete_checkout_never_restarts_timers(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    def failed_verification(*args):
        raise m.InstallError('испорченный файл после checkout')
    monkeypatch.setattr(m, 'verify_files', failed_verification)
    with pytest.raises(m.InstallError, match='таймеры оставлены остановленными'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert len([a for a in calls if 'release' in a]) == 4
    assert not any(a[:2] == ['systemctl', 'start'] for a in calls)
    state = json.loads(next(tmp_path.glob('.program-install-*.json')).read_text())
    assert state['phase'] == 'applying'


def test_concurrent_installation_is_rejected_before_systemctl(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    with m.installation_guard(tmp_path):
        with pytest.raises(m.InstallError, match='уже выполняется'):
            m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert calls == []


@pytest.mark.parametrize('phase,expected', [('applying', False), ('waiting', True), ('verified', True)])
def test_recovery_only_restarts_verified_or_untouched_tree(monkeypatch, tmp_path, phase, expected):
    state_file = tmp_path / 'recovery.json'
    state_file.write_text(json.dumps({'id': 'test', 'phase': phase, 'timers': ['court-import.timer']}))
    calls = []
    monkeypatch.setattr(sys, 'argv', ['recovery', str(state_file), 'test'])
    monkeypatch.setattr(subprocess, 'run', lambda args: (calls.append(args) or SimpleNamespace(returncode=0)))
    with pytest.raises(SystemExit):
        exec(m.RECOVERY_CODE, {})
    assert calls == ([['/bin/systemctl', 'start', 'court-import.timer']] if expected else [])


def test_file_verification_rejects_symlink_and_traversal(tmp_path):
    file = tmp_path / 'file'
    file.write_text('x')
    record = {'sha256': hashlib.sha256(b'x').hexdigest(), 'mode': '100644'}
    for name in ('../file', '/file', 'data/cases.json'):
        with pytest.raises(m.InstallError): m.verify_files(tmp_path, {'files': {name: record}})
    (tmp_path/'link').symlink_to(file)
    with pytest.raises(m.InstallError): m.verify_files(tmp_path, {'files': {'link': record}})


def test_document_checksum_and_baseline_rollback_format(tmp_path):
    lock = document({'program.py': {'sha256': 'a' * 64, 'mode': '100644'}})
    m.validate_document(lock)
    lock['files']['program.py']['sha256'] = 'b' * 64
    with pytest.raises(m.InstallError, match='контрольная сумма'):
        m.validate_document(lock)
    rollback = sign(dict(lock, kind='baseline', region='tyumen', repository='SelivanovAS/dashboard-tyumen',
                         source_repo='SelivanovAS/dashboard-tyumen'))
    m.validate_document(rollback)


def real_preflight_repo(tmp_path):
    repo = tmp_path / 'repo'
    (repo / 'scripts/court_monitor').mkdir(parents=True)
    (repo / 'REGION').write_text('hmao\n')
    (repo / 'scripts/court_monitor/config.py').write_text('''import os
from pathlib import Path
assert 'PUSH_SECRET' not in os.environ
assert 'REGION' not in os.environ
assert not Path('data').exists()
region_file = Path(__file__).resolve().parents[2] / 'REGION'
REGION = region_file.read_text().strip() if region_file.is_file() else 'hmao'
''')
    (repo / 'data').mkdir()
    (repo / 'data/cases.json').write_text('private live data')
    for args in (('init', '-q', '-b', 'main'), ('config', 'user.name', 'Fixture'),
                 ('config', 'user.email', 'fixture@example.invalid'), ('add', '.'), ('commit', '-qm', 'fixture')):
        subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True)
    paths = ('REGION', 'scripts/court_monitor/config.py')
    lock = document({name: {'sha256': hashlib.sha256((repo/name).read_bytes()).hexdigest(), 'mode': '100644'} for name in paths})
    revision = m.git(repo, 'rev-parse', 'HEAD')
    return repo, revision, lock


def test_preflight_uses_git_bytes_without_live_data_or_secrets(monkeypatch, tmp_path):
    repo, revision, lock = real_preflight_repo(tmp_path)
    monkeypatch.setenv('PUSH_SECRET', 'must-not-reach-preflight')
    monkeypatch.setenv('REGION', 'tyumen')
    # The live working tree is allowed to be busy; preflight reads Git objects.
    (repo / 'REGION').write_text('dirty live state\n')
    before = (repo / 'data/cases.json').read_bytes()
    m.preflight_revision(repo, revision, lock, 'hmao')
    assert (repo / 'REGION').read_text() == 'dirty live state\n'
    assert (repo / 'data/cases.json').read_bytes() == before


def test_preflight_rejects_bad_blob_hash_and_mode_before_execution(tmp_path):
    repo, revision, lock = real_preflight_repo(tmp_path)
    lock['files']['REGION']['sha256'] = '0' * 64
    with pytest.raises(m.InstallError, match='хеш'):
        m.preflight_revision(repo, revision, lock, 'hmao')
    lock['files']['REGION']['mode'] = '100755'
    with pytest.raises(m.InstallError, match='тип/права'):
        m.preflight_revision(repo, revision, lock, 'hmao')


def test_repair_restores_timer_snapshot_saved_before_failed_install(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    previous = m.run
    def stopped_timers(args, **kwargs):
        if args[:2] == ['systemctl', 'is-active']:
            return SimpleNamespace(returncode=3, stdout='', stderr='')
        return previous(args, **kwargs)
    monkeypatch.setattr(m, 'run', stopped_timers)
    nonce = '1' * 32
    state_file = tmp_path / ('.program-install-' + nonce + '.json')
    state_file.write_text(json.dumps({'id': nonce, 'region': 'hmao', 'phase': 'applying',
                                     'timers': ['court-parse.timer', 'court-delivery.timer']}))
    result = m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert result['installed']
    assert ['systemctl', 'start', 'court-parse.timer', 'court-delivery.timer'] in calls
    assert not state_file.exists()


def test_pending_systemd_job_blocks_installation_even_if_service_is_inactive(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    previous = m.run
    def pending_job(args, **kwargs):
        if args[:2] == ['systemctl', 'show'] and '--property=Job' in args:
            return SimpleNamespace(returncode=0, stdout='12 /org/freedesktop/systemd1/job/12', stderr='')
        return previous(args, **kwargs)
    monkeypatch.setattr(m, 'run', pending_job)
    with pytest.raises(m.InstallError, match='Службы заняты'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path, timeout=0)
    assert not any(a[:2] == ['git', 'merge'] for a in calls)
    assert any(a[:2] == ['systemctl', 'start'] for a in calls)


def test_new_service_after_lock_acquisition_prevents_checkout(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    states = iter([[], ['court-import']])
    monkeypatch.setattr(m, 'busy_services', lambda: next(states))
    with pytest.raises(m.InstallError, match='во время захвата'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path)
    assert not any(a[:2] == ['git', 'merge'] for a in calls)
    assert len([a for a in calls if 'release' in a]) == 4
    assert any(a[:2] == ['systemctl', 'start'] for a in calls)


def test_failed_repair_does_not_revive_previously_unverified_checkout(monkeypatch, tmp_path):
    calls, _ = setup_remote(monkeypatch, tmp_path)
    previous = m.run
    def stopped_timers(args, **kwargs):
        if args[:2] == ['systemctl', 'is-active']:
            return SimpleNamespace(returncode=3, stdout='', stderr='')
        return previous(args, **kwargs)
    monkeypatch.setattr(m, 'run', stopped_timers)
    monkeypatch.setattr(m, 'busy_services', lambda: ['court-import'])
    nonce = '2' * 32
    (tmp_path / ('.program-install-' + nonce + '.json')).write_text(json.dumps({
        'id': nonce, 'region': 'hmao', 'phase': 'applying', 'timers': ['court-parse.timer']}))
    with pytest.raises(m.InstallError, match='таймеры оставлены остановленными'):
        m.remote_install('hmao', SHA, SOURCE, root=tmp_path, timeout=0)
    assert not any(a[:2] == ['systemctl', 'start'] for a in calls)
    assert all(json.loads(path.read_text())['phase'] == 'applying'
               for path in tmp_path.glob('.program-install-*.json'))


def preflight_repo_without_region(tmp_path, *, kind="baseline", fallback="hmao"):
    repo, _, lock = real_preflight_repo(tmp_path)
    subprocess.run(['git', 'rm', '--', 'REGION'], cwd=repo, check=True, capture_output=True)
    config = repo / 'scripts/court_monitor/config.py'
    config.write_text(config.read_text().replace("else 'hmao'", "else " + repr(fallback)))
    for args in (('add', 'scripts/court_monitor/config.py'), ('commit', '-qm', 'original HMAO without REGION')):
        subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True)
    lock['files'].pop('REGION')
    lock['files']['scripts/court_monitor/config.py']['sha256'] = hashlib.sha256(config.read_bytes()).hexdigest()
    lock['kind'] = kind
    return repo, m.git(repo, 'rev-parse', 'HEAD'), sign(lock)


def test_first_hmao_baseline_preflight_accepts_verified_default_without_region(monkeypatch, tmp_path):
    repo, revision, lock = preflight_repo_without_region(tmp_path)
    # The actual config default, never the operator's environment, decides.
    monkeypatch.setenv('REGION', 'tyumen')
    m.validate_document(lock)
    m.preflight_revision(repo, revision, lock, 'hmao')
    assert not (repo / 'REGION').exists()
    assert (repo / 'data/cases.json').read_text() == 'private live data'


def test_program_preflight_still_requires_explicit_region_file(tmp_path):
    repo, revision, lock = preflight_repo_without_region(tmp_path, kind='program')
    with pytest.raises(m.InstallError, match='файл REGION'):
        m.preflight_revision(repo, revision, lock, 'hmao')


def test_other_baseline_region_cannot_use_hmao_missing_file_exception(tmp_path):
    repo, revision, lock = preflight_repo_without_region(tmp_path)
    lock.update(region='tyumen', repository='SelivanovAS/dashboard-tyumen', source_repo='SelivanovAS/dashboard-tyumen')
    lock = sign(lock)
    m.validate_document(lock)
    with pytest.raises(m.InstallError, match='файл REGION'):
        m.preflight_revision(repo, revision, lock, 'tyumen')


def test_hmao_baseline_missing_region_still_checks_effective_config(tmp_path):
    repo, revision, lock = preflight_repo_without_region(tmp_path, fallback='tyumen')
    with pytest.raises(m.InstallError, match='регион запуска'):
        m.preflight_revision(repo, revision, lock, 'hmao')
