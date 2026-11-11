#!/usr/bin/env python3
"""Compact the four VPS repositories without expiring any history."""
import fcntl
import hashlib
import os
from pathlib import Path
import runpy
import shutil
import signal
import subprocess

ROOT = Path('/opt/court-monitor')
REPOS = ('dashboard', 'dashboard-tyumen', 'dashboard-bashkortostan', 'dashboard-ural')


def git(repo, *args):
    command = ['git', '-C', str(repo), *args]
    child = subprocess.Popen(command, stdout=subprocess.PIPE, start_new_session=True)
    try:
        output = child.communicate()[0]
    except BaseException:
        # Keep the repository lock until the entire Git process group stops.
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.communicate()
        raise
    if child.returncode:
        raise subprocess.CalledProcessError(child.returncode, command)
    return output


def fingerprint(repo):
    objects = git(repo, 'cat-file', '--batch-all-objects', '--batch-check=%(objectname)')
    return (hashlib.sha256(b'\n'.join(sorted(objects.splitlines()))).digest(),
            git(repo, 'show-ref', '--head'))


def compact(repo):
    if not (repo / '.git').is_dir():
        raise RuntimeError(f'{repo.name}: repository missing')
    helper = runpy.run_path(str(repo / 'ops/mac-local-run/run_lock.py'))
    lock = str(repo / 'ops/mac-local-run/.run.lock')
    pid = os.getpid()
    if helper['acquire'](lock, pid):
        print(repo.name, 'SKIP busy', flush=True)
        return
    try:
        counts = dict(line.split(': ', 1) for line in git(repo, 'count-objects', '-v').decode().splitlines())
        count, size, packs, packed = (int(counts[k]) for k in ('count', 'size', 'packs', 'size-pack'))
        if count < 256 and size < 16384 and packs < 8:
            print(repo.name, 'OK already compact', count, size, packs, flush=True)
            return
        if shutil.disk_usage(repo).free < 512 * 1024**2 + 2 * (size + packed) * 1024:
            raise RuntimeError(f'{repo.name}: insufficient temporary disk space')
        print(repo.name, 'START', count, size, packs, flush=True)
        git(repo, 'fsck', '--full', '--no-dangling')
        before = fingerprint(repo)
        git(repo, 'repack', '-a', '-d', '-n', '--cruft', '--threads=1', '--window-memory=32m')
        git(repo, 'fsck', '--full', '--no-dangling')
        if fingerprint(repo) != before:
            raise RuntimeError(f'{repo.name}: object or ref mismatch')
        print(repo.name, 'OK repacked; all objects and refs preserved', flush=True)
    finally:
        if helper['release'](lock, pid):
            raise RuntimeError(f'{repo.name}: lock release failed')


def main():
    os.umask(0o077)
    fd = os.open(ROOT / '.program-install.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('SKIP installation or maintenance busy', flush=True)
            return
        if list(ROOT.glob('.program-install-*.json')):
            raise RuntimeError('Unfinished program installation; maintenance refused')
        for name in REPOS:
            compact(ROOT / name)
    finally:
        os.close(fd)


if __name__ == '__main__':
    def stop(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, stop)
    main()
