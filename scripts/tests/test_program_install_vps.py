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
        if args == ['systemctl', '--version']:
            return SimpleNamespace(returncode=0, stdout='systemd 259 (259.1)\n', stderr='')
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
    # Legacy global-path internal contract only. Public classification blocks
    # global publication until VPS reservation before push is implemented.
    monkeypatch.setattr(m, 'installation_mode', lambda *args, **kwargs: {'mode': 'global'})
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


def git_cmd(repo, *args):
    return subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


def setup_narrow(monkeypatch, tmp_path, *, version='255', region='bashkortostan'):
    """Real Git trees and hashes, fake only systemd/SSH and the lock subprocess."""
    root = tmp_path / 'vps'
    repo = root / m.REPOSITORIES[region]
    repo.mkdir(parents=True)
    shared = root / 'dashboard'
    shared.mkdir(exist_ok=True)
    routing = {}
    for name in m.ROUTING_HASHES:
        path = shared/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('audited shared routing fixture: ' + name)
        path.chmod(0o644 if name.endswith(('vps_env.sh', 'lib_sber_net.sh')) else 0o755)
        routing[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(m, 'ROUTING_HASHES', routing)
    registry_path = 'scripts/court_monitor/regions/__init__.py'
    registry = Path(__file__).parents[1] / 'court_monitor/regions/__init__.py'
    files = {'REGION': region + '\n', 'runtime.py': 'runtime code does not change\n',
             'docs/release.md': 'old documentation\n', 'ops/mac-local-run/run_lock.py': '# lock fixture\n',
             registry_path: registry.read_text()}
    for name, content in files.items():
        path = repo/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    (repo/'ops/mac-local-run/.runtime').mkdir()
    for args in [('init', '-q', '-b', 'main'), ('config', 'user.name', 'Test'),
                 ('config', 'user.email', 'fixture@example.invalid'), ('add', '.'), ('commit', '-qm', 'baseline')]:
        git_cmd(repo, *args)
    before = git_cmd(repo, 'rev-parse', 'HEAD')
    (repo/'docs/release.md').write_text('new documentation\n')
    lock = document({name: {'sha256': hashlib.sha256((repo/name).read_bytes()).hexdigest(), 'mode': '100644'} for name in files})
    lock = sign(dict(lock, region=region, repository='SelivanovAS/' + m.REPOSITORIES[region]))
    (repo/m.STAMP).write_text(json.dumps(lock))
    git_cmd(repo, 'checkout', '-qb', 'release-fixture')
    git_cmd(repo, 'add', '.')
    git_cmd(repo, 'commit', '-qm', 'release')
    target = git_cmd(repo, 'rev-parse', 'HEAD')
    git_cmd(repo, 'update-ref', 'refs/remotes/origin/main', target)
    git_cmd(repo, 'checkout', '-q', 'main')
    home = tmp_path/'home'
    config = home/'.config/court-monitor'
    config.mkdir(parents=True)
    (config/'territories').write_text('\n'.join(str(root/name) for name in m.REPOSITORIES.values()))
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: home))
    calls, units = [], {}
    launchers = {'court-parse': 'parse_all.sh', 'court-import': 'import_all.sh',
                 'court-import-poll': 'import_poll.sh', 'court-delivery': 'delivery_all.sh',
                 'court-retry': 'parse_all.sh --retry-only'}
    for name, script in launchers.items():
        units[name+'.service'] = dict(LoadState='loaded', NeedDaemonReload='no', User='root', Environment='',
             ExecStart='{ path=/bin/bash ; argv[]=/bin/bash ' + str(shared/'ops/vps-run') + '/' + script + ' ; ignore_errors=no ; }')
    original = m.run
    def run(args, **kwargs):
        calls.append(args)
        value = ''
        if args == ['systemctl', '--version']:
            value = 'systemd ' + version + ' (fixture)\n' if version else 'unrecognized version\n'
        elif args[:2] == ['systemctl', 'show-environment']:
            value = 'LANG=C.UTF-8\nPATH=/usr/bin:/bin\n'
        elif args[:2] == ['systemctl', 'show']:
            if '--property=Job' in args: value = '0\n'
            elif '--property=ActiveState' in args: value = 'inactive\n'
            else: value = '\n'.join(key+'='+v for key,v in units.get(args[2], {}).items())
        elif args[0] == 'git' and args[1] == 'fetch': pass
        elif args[0] in ('systemctl', 'systemd-run'):
            assert args[1] in ('list-units', 'list-unit-files'), 'unexpected systemd mutation: '+repr(args)
        elif args[0] == sys.executable and args[2] in ('acquire', 'release'): pass
        else: return original(args, **kwargs)
        return SimpleNamespace(returncode=0, stdout=value, stderr='')
    monkeypatch.setattr(m, 'run', run)
    monkeypatch.setattr(m, 'preflight_revision', lambda *args: None)
    return SimpleNamespace(root=root, repo=repo, document=lock, before=before, target=target,
                           calls=calls, units=units, config=config)


def assert_no_timer_mutation(calls):
    assert not any(call[0] == 'systemd-run' or (call[0] == 'systemctl' and
                   call[1] not in ('--version', 'show', 'show-environment', 'list-units', 'list-unit-files')) for call in calls)


def test_255_narrow_install_changes_only_dormant_files_and_uses_one_lock(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    (fx.repo/'data').mkdir()
    (fx.repo/'data/protected.json').write_text('keep live data')
    result = m.remote_install('bashkortostan', fx.target, SOURCE, root=fx.root)
    assert result['installed'] and result['mode'] == 'narrow' and result['timers_changed'] is False
    assert result['from'] == fx.before and result['head'] == fx.target
    assert (fx.repo/'data/protected.json').read_text() == 'keep live data'
    assert len([a for a in fx.calls if 'acquire' in a]) == 1
    assert len([a for a in fx.calls if 'release' in a]) == 1
    assert not list(fx.root.glob('.program-install-*.json'))
    assert_no_timer_mutation(fx.calls)


def test_readonly_preflight_does_not_fetch_lock_write_or_install(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    result = m.remote_preflight(fx.document, root=fx.root)
    assert result['preflight_passed'] and result['mode'] == 'narrow' and result['head'] == fx.before
    assert result['release_id'] == fx.document['release_id']
    assert not any(a[0] == 'git' and a[1] in ('fetch', 'merge') or 'acquire' in a for a in fx.calls)
    assert not (fx.root/'.program-install.lock').exists()
    assert_no_timer_mutation(fx.calls)


@pytest.mark.parametrize('version', ['255', '258', ''])
def test_legacy_or_unknown_systemd_blocks_runtime_before_checkout_or_timer_calls(monkeypatch, tmp_path, version):
    fx = setup_narrow(monkeypatch, tmp_path, version=version)
    desired = copy.deepcopy(fx.document)
    desired['files']['runtime.py']['sha256'] = '1'*64
    desired = sign(desired)
    with pytest.raises(m.InstallError, match='systemd'):
        m.remote_preflight(desired, root=fx.root)
    assert git_cmd(fx.repo, 'rev-parse', 'HEAD') == fx.before
    assert_no_timer_mutation(fx.calls)


def test_259_runtime_change_is_blocked_until_prepublication_reservation_exists(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path, version='259')
    desired = copy.deepcopy(fx.document)
    desired['files']['runtime.py']['sha256'] = '1'*64
    with pytest.raises(m.InstallError, match='резервирование VPS до публикации'):
        m.remote_preflight(sign(desired), root=fx.root)
    assert_no_timer_mutation(fx.calls)


@pytest.mark.parametrize('case', ['dirty', 'assume_unchanged', 'collision', 'parent_symlink', 'parent_file', 'removed_runtime'])
def test_actual_diff_cannot_hide_runtime_drift_or_collisions(monkeypatch, tmp_path, case):
    fx = setup_narrow(monkeypatch, tmp_path)
    desired = copy.deepcopy(fx.document)
    if case in ('dirty', 'assume_unchanged'):
        if case == 'assume_unchanged': git_cmd(fx.repo, 'update-index', '--assume-unchanged', 'runtime.py')
        (fx.repo/'runtime.py').write_text('manual runtime mutation')
    elif case == 'collision':
        (fx.repo/'docs/new.md').write_text('operator file')
        desired['files']['docs/new.md'] = {'sha256': '1'*64, 'mode': '100644'}
    elif case in ('parent_symlink', 'parent_file'):
        if case == 'parent_symlink':
            (fx.repo/'docs/external').symlink_to(tmp_path, target_is_directory=True)
        else:
            (fx.repo/'docs/external').write_text('operator file')
        desired['files']['docs/external/new.md'] = {'sha256': '1'*64, 'mode': '100644'}
    else:
        git_cmd(fx.repo, 'merge', '--ff-only', fx.target)
        desired['files'].pop('runtime.py')
    with pytest.raises(m.InstallError):
        m.remote_preflight(sign(desired), root=fx.root)
    assert_no_timer_mutation(fx.calls)


@pytest.mark.parametrize('case', ['busy', 'journal', 'lock', 'receipt', 'recovery_unit', 'recovery_unit_file'])
def test_narrow_preflight_defers_occupied_or_unfinished_state_without_mutation(monkeypatch, tmp_path, case):
    fx = setup_narrow(monkeypatch, tmp_path)
    original = m.run
    def run(args, **kwargs):
        if case == 'busy' and '--property=ActiveState' in args:
            return SimpleNamespace(returncode=0, stdout='active', stderr='')
        if case.startswith('recovery_unit') and args[:2] == ['systemctl', 'list-unit-files' if case.endswith('_file') else 'list-units']:
            return SimpleNamespace(returncode=0, stdout='court-program-recover-old.timer', stderr='')
        return original(args, **kwargs)
    monkeypatch.setattr(m, 'run', run)
    if case == 'journal': (fx.repo/'ops/mac-local-run/.runtime/delivery_txn.json').write_text('{}')
    if case == 'lock': (fx.repo/'ops/mac-local-run/.run.lock').mkdir()
    if case == 'receipt': (fx.root/'.program-install-any.json').write_text('{}')
    with pytest.raises(m.InstallError): m.remote_preflight(fx.document, root=fx.root)
    assert_no_timer_mutation(fx.calls)


@pytest.mark.parametrize('case', ['exec', 'pre_exec', 'unit_env', 'env_file', 'manager_env', 'shared_bytes', 'shared_mode', 'region', 'territories'])
def test_narrow_rejects_unproven_loaded_routing_and_environment(monkeypatch, tmp_path, case):
    fx = setup_narrow(monkeypatch, tmp_path)
    if case == 'exec': fx.units['court-parse.service']['ExecStart'] = '{ path=/bin/bash ; argv[]=/bin/bash '+str(fx.repo/'ops/mac-local-run/parse_all.sh')+' ; }'
    elif case == 'pre_exec': fx.units['court-parse.service']['ExecStartPre'] = '{ malicious command }'
    elif case == 'unit_env': fx.units['court-parse.service']['Environment'] = 'CM_WORKER=hidden-value'
    elif case == 'env_file': (fx.config/'env.bashkortostan').write_text('CM_WORKER=hidden-value\n')
    elif case == 'manager_env':
        original = m.run
        def run(args, **kwargs):
            if args == ['systemctl', 'show-environment']:
                return SimpleNamespace(returncode=0, stdout='BASH_ENV=hidden-value', stderr='')
            return original(args, **kwargs)
        monkeypatch.setattr(m, 'run', run)
    elif case == 'shared_bytes': (fx.root/'dashboard/ops/mac-local-run/parse_all.sh').write_text('drift')
    elif case == 'shared_mode': (fx.root/'dashboard/ops/mac-local-run/parse_all.sh').chmod(0o644)
    elif case == 'region': (fx.config/'env.bashkortostan').write_text('REGION=tyumen\n')
    elif case == 'territories': (fx.config/'territories').write_text(str(fx.repo)+'\n')
    with pytest.raises(m.InstallError) as error: m.remote_preflight(fx.document, root=fx.root)
    assert 'hidden-value' not in str(error.value)
    assert_no_timer_mutation(fx.calls)


def test_noop_255_keeps_timers_and_busy_data_untouched_but_rejects_old_receipt(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    git_cmd(fx.repo, 'merge', '--ff-only', fx.target)
    monkeypatch.setattr(m, 'busy_services', lambda: ['court-parse'])
    (fx.repo/'data').mkdir()
    (fx.repo/'data/current.json').write_text('ongoing normal data writer')
    result = m.remote_preflight(fx.document, root=fx.root)
    assert result['mode'] == 'noop'
    (fx.root/'.program-install-old.json').write_text('{}')
    with pytest.raises(m.InstallError, match='прежней установки'):
        m.remote_preflight(fx.document, root=fx.root)
    assert_no_timer_mutation(fx.calls)


def test_narrow_rechecks_busy_after_acquiring_lock_and_releases_it(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    states = iter([[], ['court-import']])
    monkeypatch.setattr(m, 'busy_services', lambda: next(states))
    with pytest.raises(m.InstallError, match='Службы заняты'):
        m.remote_install('bashkortostan', fx.target, SOURCE, root=fx.root)
    assert git_cmd(fx.repo, 'rev-parse', 'HEAD') == fx.before
    assert len([a for a in fx.calls if 'release' in a]) == 1
    assert_no_timer_mutation(fx.calls)


def test_narrow_refetch_rejects_unknown_program_changes_after_publication(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    git_cmd(fx.repo, 'checkout', '-q', 'release-fixture')
    (fx.repo/'runtime.py').write_text('unexpected published runtime mutation')
    git_cmd(fx.repo, 'add', 'runtime.py')
    git_cmd(fx.repo, 'commit', '-qm', 'unknown drift')
    next_head = git_cmd(fx.repo, 'rev-parse', 'HEAD')
    git_cmd(fx.repo, 'checkout', '-q', 'main')
    original = m.git
    fetches = 0
    def git(repo, *args):
        nonlocal fetches
        if args[0] == 'fetch':
            fetches += 1
            if fetches == 2: git_cmd(repo, 'update-ref', 'refs/remotes/origin/main', next_head)
        return original(repo, *args)
    monkeypatch.setattr(m, 'git', git)
    with pytest.raises(m.InstallError, match='вне данных'):
        m.remote_install('bashkortostan', fx.target, SOURCE, root=fx.root)
    assert git_cmd(fx.repo, 'rev-parse', 'HEAD') == fx.before
    assert_no_timer_mutation(fx.calls)


def test_narrow_refetch_preserves_new_data_commits(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    git_cmd(fx.repo, 'checkout', '-q', 'release-fixture')
    (fx.repo/'data').mkdir()
    (fx.repo/'data/current.json').write_text('newest normal data')
    git_cmd(fx.repo, 'add', 'data')
    git_cmd(fx.repo, 'commit', '-qm', 'new data')
    next_head = git_cmd(fx.repo, 'rev-parse', 'HEAD')
    git_cmd(fx.repo, 'checkout', '-q', 'main')
    original = m.git
    fetches = 0
    def git(repo, *args):
        nonlocal fetches
        if args[0] == 'fetch':
            fetches += 1
            if fetches == 2: git_cmd(repo, 'update-ref', 'refs/remotes/origin/main', next_head)
        return original(repo, *args)
    monkeypatch.setattr(m, 'git', git)
    result = m.remote_install('bashkortostan', fx.target, SOURCE, root=fx.root)
    assert result['head'] == next_head
    assert (fx.repo/'data/current.json').read_text() == 'newest normal data'
    assert_no_timer_mutation(fx.calls)


@pytest.mark.parametrize('property_name', ['WorkingDirectory', 'RootDirectory', 'RootImage', 'BindPaths', 'BindReadOnlyPaths'])
def test_other_loaded_service_cannot_select_clone_via_relative_execution(monkeypatch, tmp_path, property_name):
    fx = setup_narrow(monkeypatch, tmp_path)
    original = m.run
    def run(args, **kwargs):
        if args[:2] == ['systemctl', 'list-units'] and '--type=service' in args:
            return SimpleNamespace(returncode=0, stdout='unexpected.service loaded inactive dead other\n', stderr='')
        if args[:3] == ['systemctl', 'show', 'unexpected.service']:
            assert property_name in args[-1]
            return SimpleNamespace(returncode=0, stdout=property_name+'='+str(fx.repo)+'\nExecStart=/bin/bash ops/mac-local-run/parse_all.sh', stderr='')
        return original(args, **kwargs)
    monkeypatch.setattr(m, 'run', run)
    with pytest.raises(m.InstallError, match='Другая загруженная служба'):
        m.remote_preflight(fx.document, root=fx.root)
    assert_no_timer_mutation(fx.calls)


def test_255_absent_recovery_unit_files_exit_one_is_not_an_error(monkeypatch, tmp_path):
    fx = setup_narrow(monkeypatch, tmp_path)
    original = m.run
    def run(args, **kwargs):
        if args[:2] == ['systemctl', 'list-unit-files']:
            return SimpleNamespace(returncode=1, stdout='', stderr='')
        return original(args, **kwargs)
    monkeypatch.setattr(m, 'run', run)
    assert m.remote_preflight(fx.document, root=fx.root)['mode'] == 'narrow'
    assert_no_timer_mutation(fx.calls)
