#!/usr/bin/env python3
"""Actual disposable Ubuntu VM reboot proof for the persistent systemd gate.

The guest imports the real SystemdMaintenanceGate. Publication and a partial
checkout are explicitly simulated with two harmless text files. No coordinator,
Git, production data, secrets, network delivery, or production installation is
exercised. A failure to boot/reboot/observe timer ticks is a FAILED proof.

Run only in the dedicated GitHub Ubuntu runner. Image verification follows:
https://ubuntu.com/docs/public-images/public-images-reference/artifacts/
NoCloud: https://docs.cloud-init.io/en/latest/reference/datasources/nocloud.html
Networking: https://www.qemu.org/docs/master/system/invocation.html
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time
import traceback

PREFIX = "court-disposable-reboot"
GUEST_ROOT = Path("/var/lib/" + PREFIX)
NAMES = ("parse", "retry", "import", "poll", "delivery")
IMAGE_BASE = "https://cloud-images.ubuntu.com/noble/current/"
IMAGE_NAME = "noble-server-cloudimg-amd64.img"


def run(args, *, timeout=30, check=True, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=check, **kwargs)


def probe_guest(ssh, command):
    """Temporary SSH loss during boot is pending; the caller owns the deadline.

    Only the short probe's transport timeout/nonzero exit is retried. Unexpected
    local execution errors propagate, as does the outer bounded wait failure.
    """
    try:
        result = run([*ssh, command], timeout=6, check=False)
    except subprocess.TimeoutExpired:
        return None
    return result.stdout if result.returncode == 0 else None


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic(path, content):
    path = Path(path)
    with path.with_suffix(path.suffix + ".tmp").open("w") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(stream.name, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def wait(predicate, timeout=45, label="condition"):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("Timed out proving " + label)
        time.sleep(.2)


def worker(name):
    # Guest timers run only this harmless local recorder.
    record = {"name": name, "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
              "time": time.time(), "monotonic": time.monotonic(),
              "marker_exists": (GUEST_ROOT / "blocked").exists(),
              "version": [(GUEST_ROOT / part).read_text() for part in ("program-a", "program-b")],
              "invocation_id": os.environ.get("INVOCATION_ID")}
    fd = os.open(GUEST_ROOT / "events.jsonl", os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.write(fd, (json.dumps(record, sort_keys=True) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)


class Guest:
    def __init__(self):
        if os.geteuid() != 0 or not Path("/etc/court-disposable-reboot-guest").is_file():
            raise AssertionError("Refusing any machine except the seeded disposable guest")
        if Path("/proc/1/comm").read_text().strip() != "systemd":
            raise AssertionError("Guest PID 1 is not systemd")
        self.version = run(["systemctl", "--version"]).stdout
        if not re.search(r"\b255\.4(?:[-\s)]|$)", self.version):
            raise AssertionError("Requires real systemd 255.4, no skipped or substituted proof")
        self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        self.services = tuple(PREFIX + "-" + name + ".service" for name in NAMES)
        self.timers = tuple(PREFIX + "-" + name + ".timer" for name in NAMES)
        self.commands = []
        sys.path.insert(0, str(Path(__file__).parent))
        from program_maintenance_systemd import SystemdMaintenanceGate
        self.gate = SystemdMaintenanceGate(GUEST_ROOT / "blocked", self.services,
                                          timers=self.timers, runner=self.adapter_run)

    def adapter_run(self, args):
        if args[0] == "systemctl" and any(value in args for value in
                ("start", "stop", "restart", "try-restart", "enable", "disable", "kill", "clean")):
            raise AssertionError("Adapter created a service/timer lifecycle operation")
        self.commands.append({"owner": "adapter", "args": list(args), "time": time.time()})
        return run(args).stdout

    def control(self, *args):
        assert all(not arg.endswith((".timer", ".service")) or arg.startswith(PREFIX + "-")
                   for arg in args), "Foreign unit"
        self.commands.append({"owner": "fixture_setup", "args": list(args), "time": time.time()})
        return run(["systemctl", *args]).stdout

    def events(self):
        path = GUEST_ROOT / "events.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def triggers(self):
        return {unit: run(["systemctl", "show", unit, "--property=LastTriggerUSec", "--value"]).stdout.strip()
                for unit in self.timers}

    def two_ticks(self):
        observed = []
        for _ in range(2):
            previous = self.triggers()
            wait(lambda: all(value and value != previous[unit]
                             for unit, value in self.triggers().items()), label="all five natural timer ticks")
            observed.append(self.triggers())
        return observed

    def inventory(self):
        result = {}
        folder = Path("/etc/systemd/system")
        for unit in (*self.services, *self.timers):
            paths = [folder / unit, *(folder / (unit + ".d")).glob("*")]
            if unit.endswith(".timer"):
                paths.append(folder / "timers.target.wants" / unit)
            for path in paths:
                info = path.lstat()
                result[str(path)] = {"mode": stat.S_IMODE(info.st_mode),
                    "symlink": os.readlink(path) if path.is_symlink() else None,
                    "sha256": None if path.is_symlink() else digest(path)}
        return result

    def timer_configuration(self):
        names = "UnitFileState,ActiveState,Persistent,AccuracyUSec,RandomizedDelayUSec,Unit"
        return {unit: run(["systemctl", "show", unit, "--property=" + names]).stdout
                for unit in self.timers}

    def prepare(self):
        GUEST_ROOT.mkdir(mode=0o700)
        for part in ("program-a", "program-b"):
            atomic(GUEST_ROOT / part, "old\n")
        atomic(GUEST_ROOT / "protected-data", "unchanged data and queue fixture\n")
        for name, service, timer in zip(NAMES, self.services, self.timers):
            Path("/etc/systemd/system", service).write_text(
                "[Unit]\nDescription=Harmless disposable reboot recorder\n[Service]\nType=oneshot\n"
                f"ExecStart=/usr/bin/python3 /opt/court-reboot/fixture.py --worker {name}\n")
            Path("/etc/systemd/system", timer).write_text(
                "[Unit]\nDescription=Harmless disposable timer\n[Timer]\n"
                "OnCalendar=*-*-* *:*:00,10,20,30,40,50 UTC\nAccuracySec=10ms\n"
                f"RandomizedDelaySec=0\nPersistent=false\nUnit={service}\n"
                "[Install]\nWantedBy=timers.target\n")
        self.control("daemon-reload")
        self.control("enable", "--now", *self.timers)
        wait(lambda: set(event["name"] for event in self.events()) == set(NAMES), label="pre-gate timer executions")
        self.gate.bootstrap()
        # This is a SIMULATED durable publication intent, not MaintenanceLease.
        atomic(GUEST_ROOT / "journal.json", json.dumps({"phase": "publish_intent", "fixture_only": True}))
        atomic(GUEST_ROOT / "blocked", "fixture-publish-intent\n")
        self.gate.drain(timeout=15)
        baseline = self.events()
        atomic(GUEST_ROOT / "program-a", "new\n")  # simulated interrupted partial checkout
        ticks = self.two_ticks()
        assert self.events() == baseline, "Service executed with marker closed before reboot"
        report = {"boot_id": self.boot_id, "systemd_version": self.version, "events": baseline,
            "inventory": self.inventory(), "timer_configuration": self.timer_configuration(),
            "marker_sha256": digest(GUEST_ROOT / "blocked"),
            "journal_sha256": digest(GUEST_ROOT / "journal.json"),
            "protected_sha256": digest(GUEST_ROOT / "protected-data"), "closed_ticks": ticks,
            "snapshot": self.gate.snapshot(), "commands": self.commands,
            "partial_checkout": ["new\n", "old\n"], "passed": True}
        atomic(GUEST_ROOT / "before.json", json.dumps(report, sort_keys=True))
        return report

    def after(self):
        before = json.loads((GUEST_ROOT / "before.json").read_text())
        assert self.boot_id != before["boot_id"], "A real kernel reboot did not occur"
        assert digest(GUEST_ROOT / "blocked") == before["marker_sha256"]
        assert digest(GUEST_ROOT / "journal.json") == before["journal_sha256"]
        self.gate.check_guard()
        assert self.inventory() == before["inventory"], "Unit or enablement changed across reboot"
        assert self.timer_configuration() == before["timer_configuration"], "Timer settings changed across reboot"
        assert self.events() == before["events"], "Service executed before post-boot verification"
        ticks = self.two_ticks()
        assert self.events() == before["events"], "Service executed while partial checkout remained blocked"
        assert [(GUEST_ROOT / part).read_text() for part in ("program-a", "program-b")] == ["new\n", "old\n"]
        self.gate.drain(timeout=15)
        atomic(GUEST_ROOT / "program-b", "new\n")
        assert digest(GUEST_ROOT / "program-a") == digest(GUEST_ROOT / "program-b")
        assert digest(GUEST_ROOT / "protected-data") == before["protected_sha256"]
        verified_at = time.time()
        atomic(GUEST_ROOT / "journal.json", json.dumps({"phase": "verified", "fixture_only": True,
                                                        "verified_at": verified_at}))
        self.gate.check_guard()
        (GUEST_ROOT / "blocked").unlink()
        fd = os.open(GUEST_ROOT, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        # No systemctl start/restart/daemon-reload calls after setup. Natural ticks
        # resume the fixture. This deliberately does not reject delayed timer jobs.
        initial_count = len(before["events"])
        wait(lambda: set(event["name"] for event in self.events()[initial_count:]) == set(NAMES),
             label="ordinary timer recovery after verified files")
        resumed = self.events()[initial_count:]
        assert all(not event["marker_exists"] and event["version"] == ["new\n", "new\n"]
                   and event["time"] >= verified_at and event["boot_id"] == self.boot_id for event in resumed)
        assert self.inventory() == before["inventory"]
        assert self.timer_configuration() == before["timer_configuration"]
        assert digest(GUEST_ROOT / "protected-data") == before["protected_sha256"]
        return {"passed": True, "boot_id": self.boot_id, "previous_boot_id": before["boot_id"],
            "systemd_version": self.version, "closed_ticks": ticks, "verified_at": verified_at,
            "resumed_events": resumed, "commands": self.commands, "snapshot": self.gate.snapshot(),
            "inventory_unchanged": True, "timer_configuration_unchanged": True, "protected_data_unchanged": True}


def host(output):
    if os.environ.get("GITHUB_ACTIONS") != "true" or sys.platform != "linux":
        raise AssertionError("Host orchestration is permitted only on a disposable GitHub Linux runner")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "source_sha": os.environ.get("GITHUB_SHA"),
        "passed": False, "reboot_tested": False, "installer_protocol_tested": False,
        "limits": ["Simulated publication intent and two-file partial checkout only",
                   "No MaintenanceLease coordinator, Git publication, SSH production installer, or rollback tested",
                   "Graceful kernel reboot tested; hard power-loss recovery is not covered",
                   "Only disposable guest recorder services and synthetic data"], "image_url": IMAGE_BASE + IMAGE_NAME}
    process = None
    serial = None
    try:
        with tempfile.TemporaryDirectory(prefix="court-reboot-") as work:
            work = Path(work)
            sums = work / "SHA256SUMS"
            image = work / IMAGE_NAME
            for name in ("SHA256SUMS", "SHA256SUMS.gpg", IMAGE_NAME):
                run(["curl", "--fail", "--location", "--silent", "--show-error", "--retry", "2",
                     "--max-time", "180", "--output", str(work / name), IMAGE_BASE + name], timeout=210)
            keyring = Path("/usr/share/keyrings/ubuntu-cloudimage-keyring.gpg")
            assert keyring.is_file(), "Official Ubuntu cloudimage signing keyring missing"
            signature = run(["gpgv", "--keyring", str(keyring), str(work / "SHA256SUMS.gpg"), str(sums)])
            report["image_signature_verified"] = True
            report["signature_verification"] = signature.stderr
            matches = [line.split()[0] for line in sums.read_text().splitlines()
                       if line.split()[-1].lstrip("*") == IMAGE_NAME]
            assert len(matches) == 1 and re.fullmatch(r"[0-9a-f]{64}", matches[0]), "Unambiguous official image checksum missing"
            report["image_sha256"] = digest(image)
            assert report["image_sha256"] == matches[0], "Official image SHA256 mismatch"
            (output / "SHA256SUMS").write_bytes(sums.read_bytes())
            (output / "SHA256SUMS.gpg").write_bytes((work / "SHA256SUMS.gpg").read_bytes())
            key = work / "guest-key"
            run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)])
            public = key.with_suffix(".pub").read_text().strip()
            user_data = """#cloud-config
users:
  - name: fixture
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - PUBLIC_KEY
ssh_pwauth: false
disable_root: true
package_update: false
package_upgrade: false
write_files:
  - path: /etc/court-disposable-reboot-guest
    permissions: '0600'
    content: disposable-guest-only
""".replace("PUBLIC_KEY", public)
            (work / "user-data").write_text(user_data)
            (work / "meta-data").write_text("instance-id: court-reboot-fixture\nlocal-hostname: court-reboot-fixture\n")
            run(["cloud-localds", str(work / "seed.img"), str(work / "user-data"), str(work / "meta-data")])
            run(["qemu-img", "resize", str(image), "8G"])
            with socket.socket() as port_socket:
                port_socket.bind(("127.0.0.1", 0))
                port = port_socket.getsockname()[1]
            acceleration = "kvm" if os.access("/dev/kvm", os.R_OK | os.W_OK) else "tcg,thread=multi"
            report["acceleration"] = acceleration
            serial = (output / "guest-serial.log").open("w")
            process = subprocess.Popen(["qemu-system-x86_64", "-accel", acceleration, "-m", "1536", "-smp", "2",
                "-display", "none", "-monitor", "none", "-serial", "stdio",
                "-drive", f"file={image},format=qcow2,if=virtio",
                "-drive", f"file={work / 'seed.img'},format=raw,if=virtio,readonly=on",
                "-netdev", f"user,id=net0,restrict=on,hostfwd=tcp:127.0.0.1:{port}-:22",
                "-device", "virtio-net-pci,netdev=net0"], stdout=serial, stderr=subprocess.STDOUT)
            options = ["-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                       "-o", "StrictHostKeyChecking=accept-new", "-o", "UserKnownHostsFile=" + str(work / "known_hosts"),
                       "-o", "ConnectTimeout=3", "-o", "LogLevel=ERROR"]
            ssh = ["ssh", *options, "-p", str(port), "fixture@127.0.0.1"]
            def ready():
                assert process.poll() is None, "QEMU exited before proof completed"
                return probe_guest(ssh, "test -e /var/lib/cloud/instance/boot-finished") is not None
            wait(ready, timeout=180, label="first guest boot")
            run([*ssh, "sudo mkdir -p /opt/court-reboot && sudo chown fixture /opt/court-reboot"])
            adapter = Path(__file__).resolve().parents[2] / "program_maintenance_systemd.py"
            report["adapter_sha256"] = digest(adapter)
            for source, target in ((Path(__file__), "fixture.py"), (adapter, adapter.name)):
                run(["scp", *options, "-P", str(port), str(source), "fixture@127.0.0.1:/opt/court-reboot/" + target])
            before = json.loads(run([*ssh, "sudo python3 /opt/court-reboot/fixture.py --guest prepare"], timeout=90).stdout)
            (output / "before-reboot.json").write_text(json.dumps(before, indent=2, sort_keys=True) + "\n")
            assert before["passed"]
            # Reboot the GUEST, never the runner. QEMU stays alive while a new
            # guest kernel/PID1 starts from the same durable disk.
            try:
                reboot = run([*ssh, "sudo systemctl reboot"], timeout=10, check=False)
                assert reboot.returncode in (0, 255), "Guest reboot request rejected"
                report["reboot_request_returncode"] = reboot.returncode
            except subprocess.TimeoutExpired:
                # Lost SSH acknowledgement does not prove success or failure.
                # The bounded new-kernel boot_id check below decides the proof.
                report["reboot_request_returncode"] = "timeout-unknown"
            def rebooted():
                if not ready():
                    return False
                value = probe_guest(ssh, "cat /proc/sys/kernel/random/boot_id")
                return value is not None and value.strip() != before["boot_id"]
            wait(rebooted, timeout=180, label="new guest kernel boot_id")
            after = json.loads(run([*ssh, "sudo python3 /opt/court-reboot/fixture.py --guest after"], timeout=110).stdout)
            (output / "after-reboot.json").write_text(json.dumps(after, indent=2, sort_keys=True) + "\n")
            assert after["passed"] and before["boot_id"] != after["boot_id"]
            report.update(passed=True, reboot_tested=True,
                systemd_version_before=before["systemd_version"], systemd_version_after=after["systemd_version"],
                before_boot_id=before["boot_id"], after_boot_id=after["boot_id"],
                blocked_tick_rounds_before=len(before["closed_ticks"]), blocked_tick_rounds_after=len(after["closed_ticks"]),
                timers_per_round=len(NAMES), protected_data_sha256=before["protected_sha256"],
                inventory_sha256=hashlib.sha256(json.dumps(before["inventory"], sort_keys=True).encode()).hexdigest(),
                protected_data_unchanged=after["protected_data_unchanged"],
                unit_inventory_unchanged=after["inventory_unchanged"],
                timer_configuration_unchanged=after["timer_configuration_unchanged"],
                verified_at=after["verified_at"],
                verified_version_resumed=all(event["version"] == ["new\n", "new\n"]
                                             for event in after["resumed_events"]),
                resumed_service_names=sorted({event["name"] for event in after["resumed_events"]}))
            journal = run([*ssh, "sudo journalctl --no-pager --output=short-precise -u '" + PREFIX + "-*'"], timeout=30)
            (output / "guest-unit-journal.log").write_text(journal.stdout)
            # Terminate QEMU before deleting its ephemeral disk/private key.
            process.terminate()
            process.wait(timeout=10)
            process = None
    except BaseException as exc:
        report["passed"] = False
        report["error"] = str(exc)
        if isinstance(exc, subprocess.CalledProcessError):
            report["failed_command_stdout"] = (exc.stdout or "")[-8000:]
            report["failed_command_stderr"] = (exc.stderr or "")[-8000:]
        report["traceback"] = traceback.format_exc()
        raise
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        if serial is not None:
            serial.close()
        (output / "reboot-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        # Job logs remain a second evidence channel when artifact downloads are
        # unavailable. No VM disk, user-data, private key, or production values.
        print("REBOOT_PROOF " + json.dumps(report, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--guest", choices=("prepare", "after"))
    modes.add_argument("--worker", choices=NAMES)
    modes.add_argument("--host", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.worker:
        worker(args.worker)
    elif args.guest:
        guest = Guest()
        print(json.dumps(guest.prepare() if args.guest == "prepare" else guest.after(), sort_keys=True))
    else:
        if not args.output:
            parser.error("--host requires --output")
        host(args.output)


if __name__ == "__main__":
    main()
