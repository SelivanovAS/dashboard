#!/usr/bin/env python3
"""Isolated Linux bind-mount/EXDEV recovery proof; no production paths/services.

Requires root + unshare/mount, uses a private mount namespace and only a fresh
/tmp tree. No network, job dispatch or delivery. Run explicitly, outside pytest.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import program_maintenance_locks as locks


def inside(root):
    root = Path(root)
    real = root / 'real'
    real.mkdir()
    clones, state = real / 'clones', real / 'state'
    clones.mkdir()
    state.mkdir(mode=0o700)
    clone_alias, state_alias = root / 'clones', root / 'state'
    clone_alias.mkdir()
    state_alias.mkdir()
    for source, destination in ((clones, clone_alias), (state, state_alias)):
        subprocess.run(['mount', '--bind', str(source), str(destination)], check=True)
    nonce = 'd' * 32
    marker = state_alias / 'blocked'
    marker.write_text(json.dumps({'schema_version': 2, 'nonce': nonce}))
    marker.chmod(0o600)
    paths = {r: clone_alias / r / 'ops/mac-local-run/.run.lock' for r in locks.REGIONS}
    for path in paths.values(): path.parent.mkdir(parents=True)
    def adapter(**kw):
        return locks.AtomicRunLocks(paths, state_alias, marker, nonce, check_admission=lambda: None, **kw)
    # Reproduce the old cross-bind rename and leave its durable prepared receipt.
    child = os.fork()
    if child == 0:
        locks.common_mount_directories = lambda paths, descriptors: descriptors
        try:
            with adapter() as current: current.acquire()
        except locks.AtomicLockError as exc:
            os._exit(0 if 'EXDEV' in str(exc) else 3)
        os._exit(4)
    assert os.waitpid(child, 0)[1] == 0
    before = json.loads((state_alias / nonce / 'receipt.json').read_text())
    assert not any(path.exists() for path in paths.values())
    child = os.fork()
    if child == 0:
        with adapter() as current:
            current.recover()
            assert all(path.is_dir() for path in paths.values())
            current.assert_owned()
            receipt = current.receipt()
            assert receipt['generation'] == 2 and receipt['history'][0]['locks'] == before['locks']
            assert all(record['target'] == str(paths[r]) for r, record in receipt['locks'].items())
            assert len({locks._mount_id(current.folder_fd), *(locks._mount_id(current._parent(r)) for r in paths)}) == 1
            current.release()
        os._exit(0)
    assert os.waitpid(child, 0)[1] == 0
    assert not any(path.exists() for path in paths.values())
    assert marker.exists()
    # Crash after one real publication, then recover the exact same nonce/inodes.
    child = os.fork()
    if child == 0:
        def crash(event, region):
            if event == 'recovery_published': os._exit(17)
        with adapter(checkpoint=crash) as current: current.recover()
        os._exit(5)
    assert os.waitpid(child, 0)[1] == 17 << 8
    child = os.fork()
    if child == 0:
        with adapter() as current:
            current.recover()
            current.assert_owned()
            current.release()
        os._exit(0)
    assert os.waitpid(child, 0)[1] == 0
    assert not any(path.exists() for path in paths.values())
    # A foreign lock is preserved; it must never be copied over or reclaimed.
    foreign = paths['hmao']
    foreign.mkdir()
    identity = foreign.stat().st_ino
    try:
        with adapter() as current: current.recover()
    except locks.AtomicLockError:
        pass
    else:
        raise AssertionError('foreign lock was accepted')
    assert foreign.stat().st_ino == identity and list(foreign.iterdir()) == []
    # Git installation uses the same verified mount aliases for its index owner
    # hardlink and staged-file replacement, preserving real inode identity.
    import program_maintenance_git as git_proof
    source = state_alias / 'index.owner'
    source.write_text('owner')
    target = clone_alias / 'index.lock'
    git_proof._atomic_between('link', source, target)
    assert source.stat().st_ino == target.stat().st_ino
    staged = state_alias / 'staged'
    staged.write_text('new contents')
    inode = staged.stat().st_ino
    git_proof._atomic_between('replace', staged, target)
    assert target.read_text() == 'new contents' and target.stat().st_ino == inode
    assert not staged.exists() and source.read_text() == 'owner'
    print(json.dumps({'status': 'passed', 'old_failure': 'EXDEV', 'prepared_recovery': True,
        'crash_after_publication_recovery': True, 'foreign_lock_preserved': True,
        'canonical_receipt_paths_preserved': True, 'atomic_rename_only': True, 'git_hardlink_and_replace': True}))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--inside':
        inside(sys.argv[2])
    else:
        with tempfile.TemporaryDirectory(prefix='court-bind-mount-proof-') as directory:
            subprocess.run(['unshare', '--mount', '--propagation', 'private', sys.executable,
                            str(Path(__file__).resolve()), '--inside', directory], check=True)
