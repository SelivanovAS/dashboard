#!/usr/bin/env python3
"""Harmless guest workload for the real HostHooks/SSH/systemd rehearsal.

Refuses machines without the cloud-init disposable-guest sentinel. This harness
never imports or replaces HostHooks, gates, locks, Git transactions or protocol.
The only fault injection is an external inotify observer killing the coordinator
AFTER a real managed file replacement; no production fault API is introduced.
"""
from __future__ import annotations
import argparse
import base64
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import struct
import subprocess
import sys
import time

ROOT = Path('/opt/host-vm')
INSTALL = Path('/opt/court-monitor')
STATE = INSTALL / '.program-maintenance'
CONFIG = Path('/root/.config/court-monitor')
REPOS = {'hmao': 'dashboard', 'sverdlovsk_yanao': 'dashboard-ural',
         'bashkortostan': 'dashboard-bashkortostan', 'tyumen': 'dashboard-tyumen'}
LAUNCHERS = {'court-parse': ('parse_all.sh', 'parse'), 'court-retry': ('parse_all.sh --retry-only', 'parse'),
             'court-import': ('import_all.sh', 'import'), 'court-import-poll': ('import_poll.sh', 'poll'),
             'court-delivery': ('delivery_all.sh', 'delivery')}
ROUTING = ('ops/vps-run/parse_all.sh', 'ops/vps-run/import_all.sh', 'ops/vps-run/delivery_all.sh',
           'ops/vps-run/import_poll.sh', 'ops/vps-run/vps_env.sh', 'ops/vps-run/shims/netstat',
           'ops/mac-local-run/parse_all.sh', 'ops/mac-local-run/import_all.sh',
           'ops/mac-local-run/delivery_all.sh', 'ops/mac-local-run/lib_sber_net.sh')
PROGRAM_COUNT = 64


def run(args, *, check=True, timeout=60):
    try:
        return subprocess.run(args, capture_output=True, text=True, check=check, timeout=timeout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        # Only disposable synthetic guest commands reach this helper. Include
        # bounded captured streams: CalledProcessError's default text omits them.
        def tail(value, limit):
            if isinstance(value, bytes):
                value = value.decode('utf-8', 'replace')
            return (value or '')[-limit:]
        detail = {'program': Path(args[0]).name, 'failure': type(exc).__name__,
                  'returncode': getattr(exc, 'returncode', None),
                  'stdout_tail': tail(exc.stdout, 2000), 'stderr_tail': tail(exc.stderr, 4000)}
        print('FIXTURE_COMMAND_FAILURE ' + json.dumps(detail, sort_keys=True), file=sys.stderr, flush=True)
        raise


def git(repo, *args):
    return run(['git', '-C', str(repo), *args]).stdout.strip()


def initialize_empty_author(repo):
    # SSH protocol v0 cannot advertise an unborn remote HEAD. Empty clones may
    # choose the client's default (master) even when the bare HEAD is main.
    assert run(['git', '-C', str(repo), 'rev-parse', '--verify', 'HEAD'], check=False).returncode != 0
    git(repo, 'symbolic-ref', 'HEAD', 'refs/heads/main')
    assert git(repo, 'symbolic-ref', 'HEAD') == 'refs/heads/main'


def canonical(value):
    # This is the same stable package representation as program_release.canonical.
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def atomic(path, raw, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + '.tmp').open('wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(stream.name, mode)
    os.replace(stream.name, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def check_guest():
    assert os.geteuid() == 0 and Path('/etc/host-vm-disposable-guest').read_text().strip() == 'isolated-host-proof'
    assert Path('/proc/1/comm').read_text().strip() == 'systemd'
    version = run(['systemctl', '--version']).stdout
    assert re.search(r'\b255\.4(?:[-\s)]|$)', version), 'Real systemd 255.4 required'
    return version


def read_meta():
    return json.loads((ROOT / 'setup.json').read_text())


def ssh_transport():
    assert not Path('/SelivanovAS').exists(), 'Guest fixture was already initialized'
    run(['useradd', '--create-home', '--shell', '/usr/bin/git-shell', 'git'])
    # Invalid non-locked password hash; both sshd instances disallow password auth.
    run(['usermod', '--password', 'fixture-disabled-password', 'git'])
    ssh = Path('/root/.ssh')
    ssh.mkdir(mode=0o700, exist_ok=True)
    key = ssh / 'id_ed25519'
    assert not key.exists()
    run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)])
    git_ssh = Path('/home/git/.ssh')
    git_ssh.mkdir(mode=0o700)
    atomic(git_ssh / 'authorized_keys', key.with_suffix('.pub').read_bytes())
    run(['chown', '-R', 'git:git', str(git_ssh)])
    host_public = Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().split()
    atomic(ssh / 'known_hosts', ('[ssh.github.com]:443 ' + ' '.join(host_public[:2]) + '\n').encode())
    with Path('/etc/hosts').open('a') as stream:
        stream.write('\n127.0.0.1 ssh.github.com\n')
    config = ('Port 443\nListenAddress 127.0.0.1\nHostKey /etc/ssh/ssh_host_ed25519_key\n'
        'PidFile /run/host-vm-git-sshd.pid\nAllowUsers git\nPasswordAuthentication no\n'
        'KbdInteractiveAuthentication no\nUsePAM yes\nAuthorizedKeysFile .ssh/authorized_keys\n')
    atomic(ROOT / 'git-sshd.conf', config.encode())
    unit = ('[Unit]\nDescription=Disposable local Git SSH transport\nAfter=network.target\n'
        '[Service]\nType=simple\nExecStart=/usr/sbin/sshd -D -e -f /opt/host-vm/git-sshd.conf\n'
        '[Install]\nWantedBy=multi-user.target\n')
    atomic('/etc/systemd/system/host-vm-git-ssh.service', unit.encode(), 0o644)
    run(['systemctl', 'daemon-reload'])
    run(['systemctl', 'enable', '--now', 'host-vm-git-ssh.service'])
    Path('/SelivanovAS').mkdir()
    run(['chown', 'git:git', '/SelivanovAS'])


def remote(repo):
    return 'ssh://git@ssh.github.com:443/SelivanovAS/' + repo + '.git'


def program(repo, version):
    for number in range(PROGRAM_COUNT):
        atomic(repo / 'program' / ('part-%03d.txt' % number),
               (version + '\n' + 'harmless fixture content\n' * 256).encode(), 0o644)


def manifest(repo, region, source_commit):
    files = {path.relative_to(repo).as_posix(): {'sha256': sha(path.read_bytes()),
             'mode': '100755' if path.stat().st_mode & 0o111 else '100644'}
             for path in repo.rglob('*') if path.is_file() and '.git' not in path.parts
             and path.relative_to(repo).as_posix() != '.program-release.json'
             and not path.relative_to(repo).as_posix().startswith('data/')}
    result = {'schema_version': 1, 'region': region, 'source_commit': source_commit,
        'source_repo': 'SelivanovAS/dashboard', 'repository': 'SelivanovAS/' + REPOS[region],
        'profile_sha256': sha(region.encode()), 'baseline_sha256': sha((region + '-baseline').encode()),
        'protected_prefixes': ['data/'], 'files': files}
    result['release_id'] = sha(canonical(result))
    atomic(repo / '.program-release.json', canonical(result), 0o644)
    return result


def commit(repo, message):
    git(repo, 'add', '.')
    git(repo, 'commit', '-m', message)
    return git(repo, 'rev-parse', 'HEAD')


def setup(source_commit):
    check_guest()
    run(['git', '--version'])
    ssh_transport()
    INSTALL.mkdir()
    CONFIG.mkdir(parents=True)
    profiles, records = {}, {}
    authors = ROOT / 'authors'
    authors.mkdir()
    for region, name in REPOS.items():
        bare = Path('/SelivanovAS') / (name + '.git')
        run(['runuser', '-u', 'git', '--', 'git', 'init', '--bare', '--initial-branch=main', str(bare)])
        author = authors / name
        git(authors, 'clone', remote(name), str(author))
        initialize_empty_author(author)
        git(author, 'config', 'user.name', 'Disposable VM fixture')
        git(author, 'config', 'user.email', 'fixture@example.invalid')
        atomic(author / 'REGION', (region + '\n').encode(), 0o644)
        atomic(author / '.gitignore', b'.runtime/\nops/mac-local-run/.runtime/\n', 0o644)
        atomic(author / 'data/cases.json', b'["original"]\n', 0o644)
        for path in ROUTING:
            label = {'parse_all.sh': 'parse', 'import_all.sh': 'import', 'import_poll.sh': 'poll',
                     'delivery_all.sh': 'delivery'}.get(Path(path).name)
            body = ('#!/bin/bash\nset -euo pipefail\nexec /usr/bin/python3 /opt/host-vm/host_vm_guest.py worker ' + label + '\n'
                    if label and path.startswith('ops/vps-run/') else '#!/bin/bash\n# Harmless approved fixture helper.\n')
            atomic(author / path, body.encode(), 0o755)
        program(author, 'old')
        baseline = manifest(author, region, source_commit)
        base = commit(author, 'fixture baseline')
        git(author, 'push', 'origin', 'main')
        clone = INSTALL / name
        git(INSTALL, 'clone', remote(name), str(clone))
        atomic(clone / '.runtime/queue.json', b'["pending"]\n')
        atomic(clone / 'ops/mac-local-run/.runtime/delivery-log.json', b'{"accepted": 3}\n')
        atomic(clone / 'operator-note.txt', b'preserve unmanaged note\n')
        (clone / 'empty-operator-directory').mkdir()
        profiles[region] = {'path': str(clone), 'repository': 'SelivanovAS/' + name,
                            'remote': remote(name), 'baseline': baseline}
        records[region] = {'base': base, 'author': str(author)}
    author = Path(records['hmao']['author'])
    program(author, 'new')
    target_manifest = manifest(author, 'hmao', source_commit)
    target = commit(author, 'prepared harmless program change')
    CONFIG.joinpath('territories').write_text('\n'.join(str(INSTALL / name) for name in REPOS.values()) + '\n')
    for region in REPOS:
        atomic(CONFIG / ('env.' + region), ('REGION=' + region + '\n').encode())
    for unit, (launcher, _) in LAUNCHERS.items():
        chain = {'court-parse': ('court-import.service', 'court-import.service'),
                 'court-import': ('court-delivery.service', 'court-delivery.service'),
                 'court-import-poll': ('', 'court-delivery.service')}.get(unit, ('', ''))
        continuation = ''.join(key + '=' + value + '\n' for key, value in zip(('OnSuccess', 'OnFailure'), chain) if value)
        body = ('[Unit]\nDescription=Disposable ordinary workload\n' + continuation +
                '[Service]\nType=oneshot\nUser=root\nExecStart=/bin/bash ' + str(INSTALL / 'dashboard/ops/vps-run') + '/' + launcher + '\n')
        timer = ('[Unit]\nDescription=Disposable ordinary schedule\n[Timer]\n'
                 'OnCalendar=*-*-* *:*:00,10,20,30,40,50 UTC\nAccuracySec=10ms\nRandomizedDelaySec=0\n'
                 'Persistent=false\nUnit=' + unit + '.service\n[Install]\nWantedBy=timers.target\n')
        atomic('/etc/systemd/system/' + unit + '.service', body.encode(), 0o644)
        atomic('/etc/systemd/system/' + unit + '.timer', timer.encode(), 0o644)
    baseline = profiles['hmao']['baseline']
    config = {'schema_version': 1, 'state_dir': str(STATE), 'config_dir': str(CONFIG), 'profiles': profiles,
              'manifests': {baseline['release_id']: baseline, target_manifest['release_id']: target_manifest}}
    meta = {'config': config, 'target': target, 'manifest': target_manifest, 'records': records}
    atomic(ROOT / 'setup.json', canonical(meta))
    run(['systemctl', 'daemon-reload'])
    run(['systemctl', 'enable', '--now', *(name + '.timer' for name in LAUNCHERS)])
    return meta


def worker(label):
    check_guest()
    # Observe entry before the clone locks as a separate proof of service gate.
    fd = os.open(ROOT / 'starts.jsonl', os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.write(fd, (json.dumps({'label': label, 'time': time.time(),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'marker_exists': (STATE / 'blocked').exists(), 'invocation_id': os.environ.get('INVOCATION_ID')},
            sort_keys=True) + '\n').encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    for region, name in REPOS.items():
        repo = INSTALL / name
        lock = repo / 'ops/mac-local-run/.run.lock'
        try:
            lock.mkdir()
        except FileExistsError:
            continue
        try:
            git(repo, 'pull', '--ff-only', 'origin', 'main')
            versions = sorted({path.read_text().splitlines()[0] for path in (repo / 'program').glob('*.txt')})
            event = {'time': time.time(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'region': region, 'label': label, 'versions': versions,
                'head': git(repo, 'rev-parse', 'HEAD'), 'marker_exists': (STATE / 'blocked').exists(),
                'invocation_id': os.environ.get('INVOCATION_ID'), 'data_sha256': sha((repo / 'data/cases.json').read_bytes())}
            fd = os.open(ROOT / 'events.jsonl', os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
            try:
                os.write(fd, (json.dumps(event, sort_keys=True) + '\n').encode())
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            lock.rmdir()


def events():
    path = ROOT / 'events.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def status():
    meta = read_meta()
    protected, versions, heads = {}, {}, {}
    for region, name in REPOS.items():
        repo = INSTALL / name
        protected[region] = {path: sha((repo / path).read_bytes()) for path in
            ('data/cases.json', '.runtime/queue.json', 'ops/mac-local-run/.runtime/delivery-log.json', 'operator-note.txt')}
        protected[region]['empty-directory-mode'] = stat.S_IMODE((repo / 'empty-operator-directory').stat().st_mode)
        versions[region] = {value: sum(path.read_text().splitlines()[0] == value for path in (repo / 'program').glob('*.txt'))
                            for value in ('old', 'new')}
        heads[region] = git(repo, 'rev-parse', 'HEAD')
    unit_directory = Path('/etc/systemd/system')
    units = {path.name: sha(path.read_bytes()) for path in unit_directory.glob('court-*.*') if path.is_file()}
    guard_files = {path.relative_to(unit_directory).as_posix(): sha(path.read_bytes())
                   for path in unit_directory.glob('court-*.service.d/*') if path.is_file()}
    timer_links = {path.name: os.readlink(path) for path in (unit_directory / 'timers.target.wants').glob('court-*.timer') if path.is_symlink()}
    timers = {unit: run(['systemctl', 'show', unit + '.timer', '--property=ActiveState,UnitFileState,Persistent,LastTriggerUSec']).stdout
              for unit in LAUNCHERS}
    journal = STATE / 'journal.json'
    journal_value = json.loads(journal.read_text()) if journal.exists() else None
    receipt = STATE / 'host/locks' / journal_value['nonce'] / 'receipt.json' if journal_value else None
    lock_owners = {region: json.loads((INSTALL / name / 'ops/mac-local-run/.run.lock/owner.json').read_text())
                   for region, name in REPOS.items() if (INSTALL / name / 'ops/mac-local-run/.run.lock/owner.json').exists()}
    return {'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'systemd_version': check_guest(),
        'heads': heads, 'protected': protected, 'versions': versions, 'unit_hashes': units, 'guard_hashes': guard_files, 'timer_links': timer_links, 'timers': timers,
        'events': events(), 'starts': [json.loads(line) for line in (ROOT / 'starts.jsonl').read_text().splitlines()] if (ROOT / 'starts.jsonl').exists() else [],
        'marker_exists': (STATE / 'blocked').exists(),
        'journal': journal_value, 'lock_owners': lock_owners,
        'lock_receipt': json.loads(receipt.read_text()) if receipt and receipt.exists() else None}


def candidate_bundle(value):
    base, target = value.split(':')
    assert re.fullmatch(r'[0-9a-f]{40}', base) and re.fullmatch(r'[0-9a-f]{40}', target)
    author = Path(read_meta()['records']['hmao']['author'])
    assert git(author, 'rev-parse', target + '^') == base
    path = ROOT / ('candidate-' + target + '.bundle')
    ref = 'refs/program-maintenance/targets/' + target
    git(author, 'update-ref', ref, target)
    git(author, 'bundle', 'create', '--version=2', str(path), ref, '^' + base)
    raw = path.read_bytes()
    assert len(raw) <= 512 * 1024
    return {'base_sha': base, 'target_sha': target, 'bundle_sha256': sha(raw),
            'bundle_base64': base64.b64encode(raw).decode('ascii')}


def publish(target):
    assert re.fullmatch(r'[0-9a-f]{40}', target)
    journal = json.loads((STATE / 'journal.json').read_text())
    assert journal['phase'] == 'publish_intent' and journal['attempts'][-1]['target_sha'] == target
    assert (STATE / 'blocked').exists()
    before = time.time()
    git(Path(read_meta()['records']['hmao']['author']), 'push', 'origin', target + ':refs/heads/main')
    return {'published': target, 'durable_nonce_before_push': journal['nonce'], 'before_push': before,
            'attempt_id': journal['active_attempt_id'], 'target_id': journal['active_target_id']}


def fresh_rollback():
    meta = read_meta()
    author = Path(meta['records']['hmao']['author'])
    atomic(author / 'data/cases.json', b'["original", "fresh during maintenance"]\n', 0o644)
    fresh = commit(author, 'fresh protected records')
    git(author, 'push', 'origin', fresh + ':refs/heads/main')
    baseline = meta['config']['profiles']['hmao']['baseline']
    for path, entry in baseline['files'].items():
        raw = subprocess.run(['git', '-C', str(author), 'show', meta['records']['hmao']['base'] + ':' + path],
                             capture_output=True, check=True).stdout
        atomic(author / path, raw, 0o755 if entry['mode'] == '100755' else 0o644)
    atomic(author / '.program-release.json', canonical(baseline), 0o644)
    rollback = commit(author, 'rollback known old program over fresh data')
    return {'fresh': fresh, 'rollback': rollback, 'manifest': baseline}


def watch_partial():
    """Real fault boundary: first managed rename, SIGSTOP then SIGKILL, no mocks."""
    journal = json.loads((STATE / 'journal.json').read_text())
    pid = journal['owner']['pid']
    repo = INSTALL / 'dashboard'
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.inotify_init1(os.O_CLOEXEC)
    assert fd >= 0
    assert libc.inotify_add_watch(fd, os.fsencode(repo), 0x80) >= 0  # IN_MOVED_TO
    atomic(ROOT / 'watcher-ready.json', canonical({'pid': os.getpid(), 'coordinator': pid}))
    while True:
        block = os.read(fd, 8192)
        offset = 0
        while offset < len(block):
            _, mask, _, length = struct.unpack_from('iIII', block, offset)
            name = block[offset + 16:offset + 16 + length].split(b'\0')[0].decode()
            offset += 16 + length
            if mask & 0x80 and name == '.program-release.json':
                os.kill(pid, signal.SIGSTOP)
                current = status()
                # Stamp has changed but at least one program file must remain old.
                # If the observer missed the partial state, FAIL the proof.
                observed = json.loads((repo / '.program-release.json').read_text())
                partial = observed['release_id'] == read_meta()['manifest']['release_id'] and current['versions']['hmao']['old'] > 0
                os.kill(pid, signal.SIGKILL)
                atomic(ROOT / 'crash-proof.json', canonical({'partial_observed': partial, 'status': current,
                    'killed_coordinator': pid, 'fault': 'external inotify after real managed rename'}))
                assert partial, 'Did not observe a partially installed tree'
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('setup', 'worker', 'status', 'publish', 'fresh-rollback', 'watch-partial', 'candidate-bundle'))
    parser.add_argument('value', nargs='?')
    args = parser.parse_args()
    check_guest()
    if args.action == 'setup': value = setup(args.value)
    elif args.action == 'worker': return worker(args.value)
    elif args.action == 'status': value = status()
    elif args.action == 'publish': value = publish(args.value)
    elif args.action == 'candidate-bundle': value = candidate_bundle(args.value)
    elif args.action == 'fresh-rollback': value = fresh_rollback()
    else: return watch_partial()
    print(json.dumps(value, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
