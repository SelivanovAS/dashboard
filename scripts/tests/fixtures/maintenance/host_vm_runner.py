#!/usr/bin/env python3
"""Real isolated SSH + HostHooks + systemd/Git/locks recovery rehearsal.

Run only from the dedicated disposable GitHub Actions runner. The signed Ubuntu
cloud image is offline after boot; its loopback sshd serves synthetic bare repos
under the exact repository URLs accepted by HostHooks. There are no production
credentials or delivery operations. Every failed prerequisite fails the proof.
"""
from __future__ import annotations
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import traceback

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
SCRIPTS = REPO / 'scripts'
sys.path.insert(0, str(SCRIPTS))
from program_maintenance_bundle import build_bundle, bootstrap_source
from program_maintenance_protocol import BOOTSTRAP_LOADER, LeaseClient, MaintenanceProtocolError
from program_release import canonical, digest, validate_lock

spec = importlib.util.spec_from_file_location('disposable_reboot', SCRIPTS / 'tests/integration/program_maintenance_reboot.py')
vm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vm)
GUEST = 'python3 /opt/host-vm/host_vm_guest.py '


def save(path, document):
    path.write_bytes(canonical(document))


def timer_config(status):
    return {unit: '\n'.join(line for line in values.splitlines() if not line.startswith('LastTriggerUSec='))
            for unit, values in status['timers'].items()}


def timer_ticks(status):
    return {unit: next(line.partition('=')[2] for line in values.splitlines() if line.startswith('LastTriggerUSec='))
            for unit, values in status['timers'].items()}


class Rehearsal:
    def __init__(self, ssh, bundle, output):
        self.ssh, self.bundle, self.output = ssh, bundle, output
        self.client = None
        self.watcher = None
        self.proof = {}

    def guest(self, action, value=None, timeout=90):
        command = GUEST + shlex.quote(action) + ((' ' + shlex.quote(value)) if value else '')
        return json.loads(vm.run([*self.ssh, command], timeout=timeout).stdout)

    def file(self, path):
        return json.loads(vm.run([*self.ssh, 'cat ' + shlex.quote(path)]).stdout)

    def status(self, name=None):
        state = self.guest('status')
        if name:
            save(self.output / (name + '.json'), state)
        return state

    def open(self, config, bootstrap=False):
        remote = 'python3 -I -B -u -c ' + shlex.quote(BOOTSTRAP_LOADER)
        self.client = LeaseClient([*self.ssh, remote], bootstrap_source(self.bundle, config, bootstrap_guard=bootstrap), timeout=120)
        return self.client

    def closed_ticks(self, initial, label):
        evidence = []
        previous = timer_ticks(initial)
        for number in range(2):
            current = None
            def ticked():
                nonlocal current
                current = self.status()
                assert current['events'] == initial['events'], 'Ordinary workload ran through a closed gate'
                assert current['starts'] == initial['starts'], 'Service started despite permanent gate'
                assert current['marker_exists'], 'Marker vanished before explicit verified finish'
                observed = timer_ticks(current)
                return all(observed[unit] and observed[unit] != previous[unit] for unit in previous)
            vm.wait(ticked, timeout=45, label=label + ' natural timer round ' + str(number + 1))
            previous = timer_ticks(current)
            evidence.append(previous)
        save(self.output / (label + '-ticks.json'), evidence)
        return evidence

    def run(self, source_commit, reboot):
        meta = self.guest('setup', source_commit, timeout=150)
        config, manifest = meta['config'], meta['manifest']
        for record in [manifest, *(v['baseline'] for v in config['profiles'].values())]:
            validate_lock(record)
        vm.wait(lambda: {event['region'] for event in self.status()['events']} == set(config['profiles']),
                timeout=60, label='ordinary auto-pull in all four harmless clones')
        before = self.status('initial')
        client = self.open(config, bootstrap=True)
        reserved = client.request('reserve', region='hmao', release_id=manifest['release_id'],
            source_commit=manifest['source_commit'], manifest_sha256=digest(canonical(manifest)))
        nonce = reserved['nonce']
        client.request('prepare', nonce=nonce)
        held = self.status('held')
        assert held['marker_exists'] and held['journal']['phase'] == 'ready'
        assert held['heads'] == before['heads'] and held['protected'] == before['protected']
        assert held['unit_hashes'] == before['unit_hashes'], 'Base service/timer files changed in bootstrap'
        assert len(held['guard_hashes']) == 5 and held['timer_links'] == before['timer_links']
        self.proof.update(nonce=nonce, bundle_sha256=self.bundle['bundle_sha256'],
            full_systemd_version=held['systemd_version'], before_boot_id=held['boot_id'])
        receipt = held['lock_receipt']
        assert receipt['nonce'] == nonce and receipt['phase'] == 'held' and len(receipt['locks']) == 4
        assert set(held['lock_owners']) == set(config['profiles'])
        assert all(owner['pid'] == receipt['pid'] for owner in held['lock_owners'].values())
        ticks_before = self.closed_ticks(held, 'before-push')
        registration = self.guest('candidate-bundle', meta['records']['hmao']['base'] + ':' + meta['target'])
        client.request('register_target', nonce=nonce, **registration)
        permit, pushed = client.publish(nonce=nonce, base_sha=meta['records']['hmao']['base'],
            target_sha=meta['target'], push=lambda target: self.guest('publish', target))
        assert pushed['durable_nonce_before_push'] == nonce and pushed['attempt_id'] == permit['attempt_id']
        save(self.output / 'first-publication.json', {'permit': permit, 'observed_before_real_push': pushed})
        published = self.status('published')
        assert published['heads'] == held['heads'] and published['events'] == held['events'], 'Auto-pull crossed publication reservation'
        watcher_log = (self.output / 'fault-observer.log').open('w')
        self.watcher = subprocess.Popen([*self.ssh, GUEST + 'watch-partial'], stdout=watcher_log, stderr=subprocess.STDOUT)
        try:
            vm.wait(lambda: vm.probe_guest(self.ssh, 'test -f /opt/host-vm/watcher-ready.json') is not None,
                    timeout=20, label='external inotify fault observer')
            try:
                client.request('apply', nonce=nonce, attempt_id=permit['attempt_id'])
            except MaintenanceProtocolError:
                pass
            else:
                raise AssertionError('Coordinator unexpectedly survived the real partial-checkout crash')
            self.watcher.wait(timeout=30)
            assert self.watcher.returncode == 0, 'External observer did not prove and kill a partial checkout'
        finally:
            watcher_log.close()
        crash = self.file('/opt/host-vm/crash-proof.json')
        save(self.output / 'partial-crash.json', crash)
        assert crash['partial_observed'] and crash['status']['marker_exists']
        assert crash['status']['events'] == held['events'] and crash['status']['starts'] == held['starts']
        assert crash['status']['protected'] == held['protected']
        assert crash['status']['journal']['nonce'] == nonce
        client.close()
        self.client = None
        reboot(held['boot_id'])
        booted = self.status('booted-blocked')
        assert booted['boot_id'] != held['boot_id'], 'Guest kernel did not reboot'
        assert booted['events'] == held['events'] and booted['starts'] == held['starts'] and booted['marker_exists']
        assert booted['versions'] == crash['status']['versions'], 'Partial state changed without recovery'
        assert booted['protected'] == held['protected']
        assert booted['unit_hashes'] == held['unit_hashes'] and timer_config(booted) == timer_config(held)
        assert booted['guard_hashes'] == held['guard_hashes'] and booted['timer_links'] == held['timer_links']
        ticks_after = self.closed_ticks(booted, 'after-reboot')
        client = self.open(config)
        recovered = client.request('recover', nonce=nonce)
        assert recovered['nonce'] == nonce
        recovered_locks = self.status()['lock_receipt']
        assert recovered_locks['nonce'] == nonce and recovered_locks['generation'] > receipt['generation']
        assert recovered_locks['boot_id'] != receipt['boot_id'] and len(recovered_locks['locks']) == 4
        assert recovered_locks['session_id'] != receipt['session_id']
        client.request('apply', nonce=nonce, attempt_id=permit['attempt_id'])
        installed = self.status('recovered-first-target')
        assert installed['versions']['hmao'] == {'old': 0, 'new': 64}
        assert installed['marker_exists'] and installed['events'] == held['events']
        fresh = self.guest('fresh-rollback')
        old = fresh['manifest']
        selected = client.request('select_recovery_target', nonce=nonce, release_id=old['release_id'],
            source_commit=old['source_commit'], manifest_sha256=digest(canonical(old)),
            reason='rollback')
        assert len(selected['targets']) == 2 and len(selected['attempts']) == 1
        registration2 = self.guest('candidate-bundle', fresh['fresh'] + ':' + fresh['rollback'])
        client.request('register_target', nonce=nonce, **registration2)
        permit2, pushed2 = client.publish(nonce=nonce, base_sha=fresh['fresh'], target_sha=fresh['rollback'],
            push=lambda target: self.guest('publish', target))
        assert permit2['attempt_id'] == 2 and pushed2['durable_nonce_before_push'] == nonce
        save(self.output / 'rollback-publication.json', {'fresh': fresh, 'permit': permit2, 'observed_before_real_push': pushed2})
        client.request('apply', nonce=nonce, attempt_id=2)
        verified = client.request('mark_verified', nonce=nonce)
        assert verified['phase'] == 'verified' and len(verified['attempts']) == 2
        closed_final = self.status('verified-blocked')
        assert closed_final['events'] == held['events'] and closed_final['starts'] == held['starts'] and closed_final['marker_exists']
        complete = client.request('finish', nonce=nonce)
        assert complete['phase'] == 'complete' and complete['outcome'] == 'installed'
        assert complete['safe_evidence']['covered_attempt_ids'] == [1, 2]
        assert [a['target_sha'] for a in complete['attempts']] == [meta['target'], fresh['rollback']]
        assert len(complete['targets']) == 2 and complete['nonce'] == nonce
        client.close()
        self.client = None
        def resumed():
            current = self.status()
            return {event['region'] for event in current['events'][len(held['events']):]} == set(config['profiles'])
        vm.wait(resumed, timeout=60, label='ordinary auto-pull resumed on verified rollback')
        final = self.status('final')
        assert not final['marker_exists']
        assert all(value == {'old': 64, 'new': 0} for value in final['versions'].values())
        assert final['heads']['hmao'] == fresh['rollback']
        assert final['unit_hashes'] == held['unit_hashes'] and timer_config(final) == timer_config(held)
        assert final['guard_hashes'] == held['guard_hashes'] and final['timer_links'] == held['timer_links']
        assert final['protected']['hmao']['data/cases.json'] == digest(b'["original", "fresh during maintenance"]\n')
        for region in config['profiles']:
            for path, value in held['protected'][region].items():
                if region == 'hmao' and path == 'data/cases.json':
                    continue
                assert final['protected'][region][path] == value, 'Protected runtime/foreign data changed'
            if region != 'hmao':
                assert final['heads'][region] == held['heads'][region]
        resumed_events = final['events'][len(held['events']):]
        assert all(not event['marker_exists'] and event['versions'] == ['old'] and event['boot_id'] == final['boot_id']
                   for event in resumed_events)
        self.proof.update(passed=True, real_ssh_protocol=True, concrete_host_hooks=True,
            four_atomic_locks=True, durable_intent_before_push=True, partial_checkout_kill=True,
            reboot_tested=True, after_boot_id=final['boot_id'], same_nonce_recovery=True,
            retained_publication_attempts=2, retained_targets=2,
            blocked_tick_rounds_before=len(ticks_before), blocked_tick_rounds_after=len(ticks_after),
            timers_per_round=5, unit_files_unchanged=True, timer_configuration_unchanged=True,
            protected_runtime_and_foreign_files_unchanged=True, fresh_data_preserved_on_rollback=True,
            verified_program_resumed=True, protected_hashes_before=held['protected'], protected_hashes_after=final['protected'],
            unit_hashes=final['unit_hashes'], final_journal_evidence=complete['safe_evidence'])
        return self.proof

    def close(self):
        if self.client:
            self.client.close()
        if self.watcher and self.watcher.poll() is None:
            self.watcher.terminate()
            self.watcher.wait(timeout=5)


def host(output):
    assert os.environ.get('GITHUB_ACTIONS') == 'true' and sys.platform == 'linux', 'Disposable GitHub Linux runner required'
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = os.environ['GITHUB_SHA']
    assert re.fullmatch(r'[0-9a-f]{40}', source)
    report = {'schema_version': 1, 'source_sha': source, 'passed': False,
        'limits': ['Only synthetic approved programs/data in a disposable Ubuntu guest; no production installation',
                   'Published-target crash and rollback exercised; rejected never-published target fencing is a separate scenario',
                   'Graceful kernel reboot after coordinator SIGKILL; sudden power loss is not exercised'],
        'image_url': vm.IMAGE_BASE + vm.IMAGE_NAME}
    process = serial = rehearsal = None
    try:
        bundle = build_bundle(REPO, source)
        with tempfile.TemporaryDirectory(prefix='court-host-vm-') as directory:
            work = Path(directory)
            for name in ('SHA256SUMS', 'SHA256SUMS.gpg', vm.IMAGE_NAME):
                vm.run(['curl', '--fail', '--location', '--silent', '--show-error', '--retry', '2', '--max-time', '180',
                        '--output', str(work / name), vm.IMAGE_BASE + name], timeout=210)
            keyring = Path('/usr/share/keyrings/ubuntu-cloudimage-keyring.gpg')
            assert keyring.is_file()
            signature = vm.run(['gpgv', '--keyring', str(keyring), str(work / 'SHA256SUMS.gpg'), str(work / 'SHA256SUMS')])
            image = work / vm.IMAGE_NAME
            expected = [line.split()[0] for line in (work / 'SHA256SUMS').read_text().splitlines()
                        if line.split()[-1].lstrip('*') == vm.IMAGE_NAME]
            assert len(expected) == 1 and vm.digest(image) == expected[0]
            report.update(image_sha256=expected[0], image_signature_verified=True, signature_verification=signature.stderr)
            key = work / 'guest-key'
            vm.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)])
            public = key.with_suffix('.pub').read_text().strip()
            userdata = '''#cloud-config
users:
  - name: root
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - PUBLIC_KEY
ssh_pwauth: false
disable_root: false
package_update: false
package_upgrade: false
write_files:
  - path: /etc/host-vm-disposable-guest
    permissions: '0600'
    content: isolated-host-proof
'''.replace('PUBLIC_KEY', public)
            (work / 'user-data').write_text(userdata)
            (work / 'meta-data').write_text('instance-id: court-host-vm-proof\nlocal-hostname: court-host-vm-proof\n')
            vm.run(['cloud-localds', str(work / 'seed.img'), str(work / 'user-data'), str(work / 'meta-data')])
            vm.run(['qemu-img', 'resize', str(image), '8G'])
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            acceleration = 'kvm' if os.access('/dev/kvm', os.R_OK | os.W_OK) else 'tcg,thread=multi'
            report['acceleration'] = acceleration
            serial = (output / 'guest-serial.log').open('w')
            process = subprocess.Popen(['qemu-system-x86_64', '-accel', acceleration, '-m', '2048', '-smp', '2',
                '-display', 'none', '-monitor', 'none', '-serial', 'stdio',
                '-drive', f'file={image},format=qcow2,if=virtio',
                '-drive', f"file={work / 'seed.img'},format=raw,if=virtio,readonly=on",
                '-netdev', f'user,id=net0,restrict=on,hostfwd=tcp:127.0.0.1:{port}-:22',
                '-device', 'virtio-net-pci,netdev=net0'], stdout=serial, stderr=subprocess.STDOUT)
            options = ['-i', str(key), '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes',
                '-o', 'StrictHostKeyChecking=accept-new', '-o', 'UserKnownHostsFile=' + str(work / 'known_hosts'),
                '-o', 'ConnectTimeout=3', '-o', 'LogLevel=ERROR']
            ssh = ['ssh', '-T', *options, '-p', str(port), 'root@127.0.0.1']
            def ready():
                assert process.poll() is None, 'QEMU exited before proof completed'
                return vm.probe_guest(ssh, 'test -e /var/lib/cloud/instance/boot-finished') is not None
            vm.wait(ready, timeout=180, label='disposable guest cloud-init')
            vm.run([*ssh, 'mkdir -p /opt/host-vm'])
            vm.run(['scp', *options, '-P', str(port), str(HERE / 'host_vm_guest.py'), 'root@127.0.0.1:/opt/host-vm/host_vm_guest.py'])
            def reboot(previous):
                try:
                    result = vm.run([*ssh, 'systemctl reboot'], timeout=10, check=False)
                    assert result.returncode in (0, 255)
                    report['reboot_request_returncode'] = result.returncode
                except subprocess.TimeoutExpired:
                    report['reboot_request_returncode'] = 'timeout-unknown'
                def rebooted():
                    if not ready():
                        return False
                    value = vm.probe_guest(ssh, 'cat /proc/sys/kernel/random/boot_id')
                    return value is not None and value.strip() != previous
                vm.wait(rebooted, timeout=180, label='actual changed guest kernel boot ID')
            rehearsal = Rehearsal(ssh, bundle, output)
            try:
                report.update(rehearsal.run(source, reboot))
            finally:
                report.update(rehearsal.proof)
                rehearsal.close()
                for label, command in [('guest-unit-journal.log', "journalctl --no-pager -o short-precise -u 'court-*' -u host-vm-git-ssh"),
                        ('guest-final-status.json', GUEST + 'status')]:
                    result = vm.probe_guest(ssh, command)
                    if result is not None:
                        (output / label).write_text(result)
                process.terminate()
                process.wait(timeout=10)
                process = None
    except BaseException as exc:
        report['passed'] = False
        report['error'] = str(exc)
        report['traceback'] = traceback.format_exc()
        if isinstance(exc, subprocess.CalledProcessError):
            report['failed_command_stdout'] = (exc.stdout or '')[-8000:]
            report['failed_command_stderr'] = (exc.stderr or '')[-8000:]
        raise
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if serial:
            serial.close()
        save(output / 'host-vm-report.json', report)
        print('HOST_VM_PROOF ' + json.dumps(report, sort_keys=True), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    host(parser.parse_args().output)
