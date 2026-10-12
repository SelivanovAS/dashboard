"""Public orchestration with real HostHooks, Git, inventory and lease.

Only SSH/systemd/atomic Linux boundaries are replaced here; the disposable VM
must independently prove those boundaries before production use.
"""
import copy
import json
import importlib.util
from pathlib import Path

import pytest
import program_maintenance as core
import program_maintenance_client as client
import program_maintenance_host as host
import program_maintenance_protocol as protocol
import program_release as release
_spec=importlib.util.spec_from_file_location('client_host_fixture',Path(__file__).with_name('test_program_maintenance_host.py'))
_host_fixture=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_host_fixture)
fixture,git,write,commit,FakeLocks=(_host_fixture.fixture,_host_fixture.git,_host_fixture.write,_host_fixture.commit,_host_fixture.FakeLocks)


class LocalClient(protocol.LeaseClient):
    def __init__(self, hooks):
        self.lease = core.MaintenanceLease(hooks.state, hooks)
        self.lease.__enter__()
        self.closed = False

    def request(self, action, **args):
        return getattr(self.lease, action)(**args)

    def close(self):
        if not self.closed:
            self.closed = True
            self.lease.__exit__(None, None, None)


@pytest.fixture
def world(fixture, monkeypatch):
    f = fixture
    package = copy.deepcopy(f['new'])
    package['baseline'] = {'schema_version':1, 'region':'hmao',
        'source_commit': f['initial']['hmao'], 'files': f['old']['hmao']['files']}
    package['baseline_sha256'] = release.digest(release.canonical(package['baseline']))
    package['release_id'] = release.release_id(release.lock_from_package(package))
    folder = f['root'] / 'package'
    write(folder, 'package.json', release.canonical(package))
    for name, record in package['files'].items():
        write(folder / 'files', name, (f['authors']['hmao'] / name).read_bytes(),
              0o755 if record['mode']=='100755' else 0o644)
    release.load_package(folder)
    f['config']['manifests'][package['release_id']] = release.lock_from_package(package)
    config = {k:v for k,v in f['config'].items() if k != 'capability'}
    config_path = f['root'] / 'input.json'
    config_path.write_bytes(release.canonical(config))
    def connect(operation, spec, config, *, bootstrap=False):
        # Keep actual host algorithms; service and file-lock boundaries alone
        # are synthetic. The real transport has its own connected/VM tests.
        hooks = host.HostHooks(f['config'], gate=f['boundary'], runner=f['boundary'].run,
                              locks_factory=FakeLocks, fetcher=f['hooks'].fetcher)
        return LocalClient(hooks)
    monkeypatch.setattr(client, '_connect', connect)
    f['pushes'] = []
    f['push_behavior'] = None
    def transport(repo, remote, operation, revision=None):
        assert remote == config['profiles']['hmao']['remote']
        if operation == 'fetch':
            git(repo, 'fetch', str(f['remotes']['hmao']), 'refs/heads/main')
            return git(repo, 'rev-parse', 'FETCH_HEAD')
        journal = json.loads((f['state']/'journal.json').read_text())
        assert (f['state']/'blocked').exists()
        assert journal['attempts'][-1]['target_sha'] == revision
        assert journal['phase'] == 'publish_intent'
        assert all((Path(p['path'])/'ops/mac-local-run/.run.lock').is_dir() for p in config['profiles'].values())
        f['pushes'].append(revision)
        if f['push_behavior'] is not None:
            return f['push_behavior'](repo, revision)
        return git(repo, 'push', str(f['remotes']['hmao']), revision+':refs/heads/main', check=False).returncode
    monkeypatch.setattr(client, 'remote_git', transport)
    key=f['root']/'identity';key.write_text('synthetic; never used')
    f.update(package=folder, package_record=package, config_path=config_path,
             operation=f['root']/'operation', key=key)
    return f


def promote(f):
    source = Path(client.__file__).resolve().parents[1]
    return release.promote_release(f['package'], Path(f['config']['profiles']['hmao']['path']), 'hmao', push=True,
        remote=f['config']['profiles']['hmao']['remote'], vps_host='fixture.invalid', ssh_key=f['key'],
        maintenance_config=f['config_path'], coordinator_sha=git(source,'rev-parse','HEAD'),
        operation_dir=f['operation'])


def test_public_promote_holds_before_push_preserves_data_and_recovers_complete(world):
    f=world
    repo=Path(f['config']['profiles']['hmao']['path'])
    before=(repo/'data/cases.json').read_bytes()
    result=promote(f)
    assert result['status']=='installed',result
    assert len(f['pushes'])==1 and not (f['state']/'blocked').exists()
    assert (repo/'script.py').read_text()=='new program\n'
    assert (repo/'data/cases.json').read_bytes()==before
    assert (repo/'untracked.json').read_text()=='preserved\n'
    assert client.recover(f['operation'])['status']=='installed'
    assert len(f['pushes'])==1


def test_data_race_keeps_both_intents_and_new_data(world):
    f=world
    def race(repo, revision):
        f['push_behavior']=None
        author=f['authors']['hmao']
        git(author,'checkout','--detach',f['initial']['hmao'])
        write(author,'data/cases.json',b'{"cases":[1,2]}\n')
        newer=commit(author,'new data')
        git(author,'push',str(f['remotes']['hmao']),newer+':refs/heads/main')
        return git(repo,'push',str(f['remotes']['hmao']),revision+':refs/heads/main',check=False).returncode
    f['push_behavior']=race
    result=promote(f)
    assert result['status']=='installed',result
    assert len(result['attempts'])==2 and len(f['pushes'])==2
    repo=Path(f['config']['profiles']['hmao']['path'])
    assert json.loads((repo/'data/cases.json').read_text())=={'cases':[1,2]}


def test_unknown_program_race_stays_closed(world):
    f=world
    def race(repo, revision):
        author=f['authors']['hmao']
        git(author,'checkout','--detach',f['initial']['hmao'])
        write(author,'script.py',b'unknown manual code\n')
        newer=commit(author,'manual program')
        git(author,'push',str(f['remotes']['hmao']),newer+':refs/heads/main')
        return 1
    f['push_behavior']=race
    result=promote(f)
    assert result['status']=='recovery_required'
    assert (f['state']/'blocked').exists() and len(f['pushes'])==1
    assert (Path(f['config']['profiles']['hmao']['path'])/'script.py').read_text()=='old program\n'


def test_uncertain_push_recovers_exact_journal_without_second_push(world):
    f=world
    def lost(repo, revision):
        f['push_behavior']=None
        git(repo,'push',str(f['remotes']['hmao']),revision+':refs/heads/main')
        raise OSError('synthetic lost reply')
    f['push_behavior']=lost
    assert promote(f)['status']=='recovery_required'
    assert (f['state']/'blocked').exists()
    result=client.recover(f['operation'])
    assert result['status']=='installed',result
    assert len(f['pushes'])==1


def test_candidate_substitution_fails_before_git_mutation(world):
    f=world
    def reject(repo, revision): return 1
    f['push_behavior']=reject
    assert promote(f)['status']=='recovery_required'
    clone=f['operation']/'candidate'
    clone.rename(f['operation']/'saved-candidate')
    clone.symlink_to(f['authors']['hmao'],target_is_directory=True)
    before=git(f['authors']['hmao'],'rev-parse','HEAD')
    assert client.recover(f['operation'])['status']=='recovery_required'
    assert git(f['authors']['hmao'],'rev-parse','HEAD')==before
    assert len(f['pushes'])==1 and (f['state']/'blocked').exists()


def test_rollback_after_lost_push_keeps_latest_data_and_all_intents(world):
    f=world
    package=copy.deepcopy(f['old']['hmao'])
    package['baseline']=f['package_record']['baseline']
    package['baseline_sha256']=release.digest(release.canonical(package['baseline']))
    package['release_id']=release.release_id(release.lock_from_package(package))
    folder=f['root']/'rollback'
    write(folder,'package.json',release.canonical(package))
    for name,record in package['files'].items():
        raw=client.git_proof._git(f['authors']['hmao'],'show',f['initial']['hmao']+':'+name).stdout
        write(folder/'files',name,raw,0o755 if record['mode']=='100755' else 0o644)
    f['config']['manifests'][package['release_id']]=release.lock_from_package(package)
    config={k:v for k,v in f['config'].items() if k!='capability'}
    f['config_path'].write_bytes(release.canonical(config))
    def lost(repo,revision):
        f['push_behavior']=None
        git(repo,'push',str(f['remotes']['hmao']),revision+':refs/heads/main')
        raise OSError('lost acknowledgement')
    f['push_behavior']=lost
    initial=promote(f)
    assert initial['status']=='recovery_required' and initial['publication_uncertain']
    author=f['authors']['hmao']
    git(author,'fetch',str(f['remotes']['hmao']),'main')
    git(author,'checkout','--detach','FETCH_HEAD')
    write(author,'data/cases.json',b'{"cases":[1,2,3]}\n')
    newer=commit(author,'fresh data after uncertain push')
    git(author,'push',str(f['remotes']['hmao']),newer+':refs/heads/main')
    result=client.recover(f['operation'],rollback_package=folder)
    assert result['status']=='installed',result
    assert result['nonce']==initial['nonce'] and len(result['attempts'])==2
    repo=Path(f['config']['profiles']['hmao']['path'])
    assert (repo/'script.py').read_text()=='old program\n'
    assert json.loads((repo/'data/cases.json').read_text())=={'cases':[1,2,3]}
    assert git(repo,'rev-parse','HEAD^')==newer


def test_missing_window_parameters_cannot_fall_back_to_legacy_push(world):
    f=world
    with pytest.raises(release.ReleaseError,match='Общее окно требует'):
        release.promote_release(f['package'],Path(f['config']['profiles']['hmao']['path']),'hmao',
            push=True,maintenance_config=f['config_path'])
    assert not f['pushes'] and not (f['state']/'blocked').exists()


def test_repeated_new_noop_operation_never_reserves_or_pushes(world, monkeypatch):
    f = world
    installed = promote(f)
    assert installed['status'] == 'installed'
    journal_before = (f['state'] / 'journal.json').read_bytes()
    f['operation'] = f['root'] / 'unchanged-operation'
    proofs = []
    def verify(lock, host_name, key):
        repo = Path(f['config']['profiles']['hmao']['path'])
        assert release.read_installed(repo, git(repo, 'rev-parse', 'HEAD'), 'hmao') == lock
        proofs.append(lock['release_id'])
        return {'verified': True}
    monkeypatch.setattr(release, 'verify_vps', verify)
    def forbidden(*args, **kwargs): raise AssertionError('No-op opened a coordinator')
    monkeypatch.setattr(client, '_connect', forbidden)
    assert promote(f)['unchanged']
    assert promote(f)['unchanged']
    assert client.recover(f['operation'])['unchanged']
    assert len(proofs) == 3 and len(f['pushes']) == 1
    assert (f['state'] / 'journal.json').read_bytes() == journal_before
    assert not (f['state'] / 'blocked').exists()
