import fcntl
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile

source = Path(__file__).with_name('court-history.py')
compile(source.read_text(), str(source), 'exec')
mod = runpy.run_path(str(source))
compact = mod['compact']
fingerprint = mod['fingerprint']

with tempfile.TemporaryDirectory(prefix='history-test-', dir=source.parent) as temporary:
    root = Path(temporary)
    repo = root / 'dashboard'
    repo.mkdir()
    def run(*args, **kwargs):
        return subprocess.run(['git', '-C', str(repo), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout
    run('init')
    run('config', 'gc.auto', '0')
    for i in range(300):
        (repo / f'data-{i}.txt').write_text(f'regional data {i}\n' * 10)
    run('add', '.')
    run('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'fixture')
    run('hash-object', '-w', '--stdin', input=b'unreferenced recovery object')
    helper_path = repo / 'ops/mac-local-run/run_lock.py'
    helper_path.parent.mkdir(parents=True)
    shutil.copyfile(Path(__file__).resolve().parents[2] / 'mac-local-run/run_lock.py', helper_path)
    helper = runpy.run_path(str(helper_path))
    lock = str(helper_path.with_name('.run.lock'))
    before = fingerprint(repo)
    data_before = {p.name: p.read_bytes() for p in repo.glob('data-*.txt')}
    compact(repo)
    assert fingerprint(repo) == before
    assert {p.name: p.read_bytes() for p in repo.glob('data-*.txt')} == data_before
    assert not Path(lock).exists()
    assert b'count: 0\n' in run('count-objects', '-v')
    compact(repo)
    assert helper['acquire'](lock, os.getpid()) == 0
    compact(repo)
    assert Path(lock).is_dir()
    assert helper['release'](lock, os.getpid()) == 0
    for i in range(260):
        run('hash-object', '-w', '--stdin', input=f'new unreachable {i}'.encode())
    real_git = compact.__globals__['git']
    def failing_git(repo, *args):
        if args[0] == 'fsck':
            raise RuntimeError('injected integrity-check failure')
        return real_git(repo, *args)
    compact.__globals__['git'] = failing_git
    try:
        compact(repo)
        raise AssertionError('failure must propagate')
    except RuntimeError as exc:
        assert str(exc) == 'injected integrity-check failure'
    finally:
        compact.__globals__['git'] = real_git
    assert not Path(lock).exists()
    mod['main'].__globals__['ROOT'] = root
    mod['main'].__globals__['REPOS'] = ('dashboard',)
    with (root / '.program-install.lock').open('w') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        mod['main']()
        assert not Path(lock).exists()
    marker = root / '.program-install-test.json'
    marker.write_text('{}')
    try:
        mod['main']()
        raise AssertionError('unfinished installation must block maintenance')
    except RuntimeError as exc:
        assert 'Unfinished program installation' in str(exc)
    assert marker.exists()
    assert not Path(lock).exists()
print('PASS: history including unreachable objects, data, busy locks, failure cleanup, installation exclusion')
