#!/usr/bin/env python3
"""Real systemd 255.4 contract fixture; NEVER run on a production machine.

This is deliberately not a pytest unit test. The dedicated GitHub workflow uses
an expendable Ubuntu 24.04 VM, root, unique fixture units, and local counters.
No production checkout, credentials, network calls, or delivery are used.

The adapter under test is the SAME scripts/program_maintenance_systemd.py used
by the coordinator. This verifies timer/Condition/drain primitives, NOT the full
publication protocol or recovery after an actual reboot. A passed artifact must
retain these limits; the full rollout needs independent crash/push tests.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid


def run(args, *, check=True, timeout=20):
    return subprocess.run(args, text=True, capture_output=True, check=check, timeout=timeout)


def atomic_write(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def worker(root, name, phase):
    root = Path(root)
    event = {
        "wall_time": time.time(), "monotonic": time.monotonic(),
        "name": name, "phase": phase, "pid": os.getpid(),
        "invocation_id": os.environ.get("INVOCATION_ID"),
        "marker_exists": (root / "blocked").exists(),
    }
    descriptor = os.open(root / "events.jsonl", os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.write(descriptor, (json.dumps(event, sort_keys=True) + "\n").encode())
    finally:
        os.close(descriptor)
    deadline = time.monotonic() + 25
    while (root / "holds" / (name + "-" + phase)).exists():
        if time.monotonic() >= deadline:
            return 19
        time.sleep(.025)
    return 9 if phase == "execute" and (root / "fail" / name).exists() else 0


def coordinator_fixture(root):
    """Only a disposable process to demonstrate durable marker after SIGKILL."""
    root = Path(root)
    atomic_write(root / "coordinator.json", json.dumps({"phase": "publish_intent", "fixture_only": True}))
    atomic_write(root / "blocked", "fixture-publish-intent\n")
    print("READY", flush=True)
    while True:
        time.sleep(1)


class Fixture:
    def __init__(self, output):
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.identifier = uuid.uuid4().hex[:10]
        self.prefix = "court-release-fixture-" + self.identifier
        self.root = Path("/var/lib") / self.prefix
        self.marker = self.root / "blocked"
        self.unit_directory = Path("/etc/systemd/system")
        self.main_names = ("parse", "retry", "import", "poll", "delivery")
        self.guarded_names = (*self.main_names, "late", "chain-head", "chain-tail", "fail-head", "fail-tail", "queued")
        self.all_names = (*self.guarded_names, "blocker")
        self.services = tuple(self.service(name) for name in self.guarded_names)
        self.timers = tuple(self.prefix + "-" + name + ".timer" for name in self.main_names)
        # Production services are retained by their active timer routes. Unused
        # scenario services have no such references and systemd may garbage-
        # collect them between show and GetUnit. Far-future fixture timers keep
        # the same reference invariant without starting their scenario work.
        self.pinning_timers = tuple(self.prefix + "-" + name + "-pin.timer"
                                    for name in self.guarded_names if name not in self.main_names)
        self.all_timers = (*self.timers, *self.pinning_timers)
        self.all_units = (*self.services, self.service("blocker"), *self.all_timers)
        self.created_paths = []
        self.started = False
        self.adapter = None
        self.report = {
            "schema_version": 1, "fixture_id": self.identifier,
            "source_sha": os.environ.get("GITHUB_SHA"), "started_at": time.time(),
            "image_os": os.environ.get("ImageOS"), "image_version": os.environ.get("ImageVersion"),
            "checks": [], "passed": False, "reboot_tested": False,
            "installer_protocol_tested": False,
            "limits": ["No actual VM reboot tested", "No production services or data used",
                       "No SSH or Git publication protocol exercised", "SIGKILL test covers durable marker primitive only"],
            "commands": [], "snapshots": {},
        }

    def service(self, name):
        return self.prefix + "-" + name + ".service"

    def control(self, *args, check=True):
        # This guard applies to the harness lifecycle. Adapter calls are captured
        # separately by its injected runner and forbidden from starting/stopping.
        unit_arguments = [arg for arg in args if arg.endswith((".service", ".timer"))]
        if any(not arg.startswith(self.prefix + "-") for arg in unit_arguments):
            raise AssertionError("fixture attempted to control a foreign unit")
        self.report["commands"].append({"owner": "fixture", "args": list(args), "time": time.time()})
        return run(["systemctl", *args], check=check)

    def adapter_runner(self, args):
        args = list(args)
        self.report["commands"].append({"owner": "adapter", "args": args, "time": time.time()})
        forbidden = {"start", "stop", "restart", "try-restart", "reload-or-restart", "enable", "disable", "kill", "clean"}
        if args and args[0] == "systemctl" and any(arg in forbidden for arg in args[1:]):
            raise AssertionError("maintenance adapter changed a service/timer lifecycle")
        result = self.adapter_command(args)
        if args and args[0] == "busctl":
            self.report.setdefault("busctl_results", []).append({"args": args, "stdout": result})
        return result

    def events(self, name=None, phase="execute"):
        path = self.root / "events.jsonl"
        events = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        return [event for event in events if (name is None or event["name"] == name)
                and (phase is None or event["phase"] == phase)]

    def wait(self, predicate, *, timeout=20, label="condition"):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError("timeout waiting for " + label)
            time.sleep(.05)

    def properties(self, unit):
        values = self.control("show", unit, "--no-pager").stdout
        return dict(line.split("=", 1) for line in values.splitlines() if "=" in line)

    def snapshot(self, label):
        properties = ("ActiveState", "SubState", "ActiveEnterTimestampMonotonic", "InvocationID",
                      "LastTriggerUSec", "LastTriggerUSecMonotonic", "NextElapseUSecRealtime",
                      "UnitFileState", "TimersCalendar", "Conditions", "ConditionResult", "Job",
                      "MainPID", "ControlPID", "ControlGroup", "DropInPaths", "NeedDaemonReload")
        state = {unit: {key: value for key, value in self.properties(unit).items() if key in properties}
                 for unit in self.all_units}
        self.report["snapshots"][label] = state
        return state

    def checked(self, name, **evidence):
        self.report["checks"].append({"name": name, "passed": True, **evidence})

    def write_unit(self, name, body):
        path = self.unit_directory / name
        if path.exists():
            raise AssertionError("refusing existing unit " + str(path))
        path.write_text(body)
        self.created_paths.append(path)

    def set_hold(self, name, phase):
        path = self.root / "holds" / (name + "-" + phase)
        path.touch()
        return path

    def close(self):
        atomic_write(self.marker, "fixture\n")

    def open(self):
        self.marker.unlink()

    def assert_busy(self, label):
        state = self.adapter.busy()
        assert state, "adapter failed to detect " + label
        started = time.monotonic()
        try:
            self.adapter.drain(timeout=.15)
        except Exception as error:
            self.checked("drain refuses " + label, busy=state, error=str(error))
        else:
            raise AssertionError("drain accepted " + label)
        assert time.monotonic() - started < 5, "drain timeout was not bounded"

    def timer_count(self):
        return {name: len(self.events(name)) for name in self.main_names}

    def setup(self):
        self.report["systemd_version"] = run(["systemctl", "--version"]).stdout
        if not re.search(r"\b255\.4(?:[-\s)]|$)", self.report["systemd_version"]):
            raise AssertionError("requires real systemd package 255.4; refusing to skip/version-substitute")
        self.report["pid1"] = Path("/proc/1/comm").read_text().strip()
        assert self.report["pid1"] == "systemd", "real systemd must be PID 1"
        self.root.mkdir(mode=0o700)
        (self.root / "holds").mkdir()
        (self.root / "fail").mkdir()
        local_helper = self.root / "fixture.py"
        shutil.copyfile(__file__, local_helper)
        helper = f"/usr/bin/python3 {local_helper} --worker {self.root}"
        for name in self.all_names:
            extra = ""
            if name == "chain-head":
                extra = f"OnSuccess={self.service('chain-tail')}\n"
            if name == "fail-head":
                extra = f"OnFailure={self.service('fail-tail')}\n"
            if name == "queued":
                extra = f"Requires={self.service('blocker')}\nAfter={self.service('blocker')}\n"
            self.write_unit(self.service(name),
                f"[Unit]\nDescription=Disposable release fixture {name}\n{extra}"
                f"[Service]\nType=oneshot\nExecStartPre={helper} {name} pre\n"
                f"ExecStart={helper} {name} execute\nExecStopPost={helper} {name} post\n"
                "TimeoutStartSec=30\nTimeoutStopSec=30\n")
        for name, timer in zip(self.main_names, self.timers):
            self.write_unit(timer,
                f"[Unit]\nDescription=Disposable release timer {name}\n[Timer]\n"
                f"OnCalendar=*-*-* *:*:00,12,24,36,48 UTC\nUnit={self.service(name)}\n"
                "AccuracySec=10ms\nRandomizedDelaySec=0\nPersistent=false\n"
                "[Install]\nWantedBy=timers.target\n")
        extra_names = [name for name in self.guarded_names if name not in self.main_names]
        for name, timer in zip(extra_names, self.pinning_timers):
            self.write_unit(timer,
                f"[Unit]\nDescription=Disposable reference pin for {name}\n[Timer]\n"
                f"OnCalendar=2099-01-01 00:00:00 UTC\nUnit={self.service(name)}\n"
                "Persistent=false\n[Install]\nWantedBy=timers.target\n")
        self.control("daemon-reload")
        self.control("enable", "--now", *self.all_timers)
        self.started = True
        self.wait(lambda: all(self.timer_count().values()), label="first natural timer tick")
        self.wait(lambda: all(self.properties(self.service(name))["ActiveState"] == "inactive"
                              for name in self.main_names), label="first services finishing")
        assert all(self.properties(timer)["ActiveState"] == "active" for timer in self.pinning_timers)
        assert not any(self.events(name, phase=None) for name in extra_names), "reference pins started scenario work"
        self.checked("far-future timer references retain scenario units without work",
                     timers=list(self.pinning_timers), on_calendar="2099-01-01 00:00:00 UTC")
        self.timer_baseline = self.snapshot("initial_tick")
        self.timer_hashes = {timer: hashlib.sha256((self.unit_directory / timer).read_bytes()).hexdigest()
                             for timer in self.timers}
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from program_maintenance_systemd import SystemdMaintenanceGate, command
        self.adapter_command = command
        self.adapter = SystemdMaintenanceGate(marker=self.marker, services=self.services, timers=self.timers,
                                              unit_directory=self.unit_directory, runner=self.adapter_runner)
        self.report["adapter_sha256"] = hashlib.sha256(
            (Path(__file__).resolve().parents[2] / "program_maintenance_systemd.py").read_bytes()).hexdigest()

    def timer_guard_test(self):
        self.close()
        before_count = self.timer_count()
        self.adapter.bootstrap()
        self.adapter.check_guard()
        reloads = sum(command["owner"] == "adapter" and "daemon-reload" in command["args"]
                      for command in self.report["commands"])
        self.adapter.bootstrap()
        assert reloads == sum(command["owner"] == "adapter" and "daemon-reload" in command["args"]
                              for command in self.report["commands"]), "unchanged guard was reloaded"
        self.checked("guard bootstrap is idempotent without repeated daemon-reload")
        self.adapter.drain(timeout=10)
        before = self.snapshot("guard_bootstrapped")
        self.wait(lambda: all(self.properties(timer).get("LastTriggerUSecMonotonic") !=
                              before[timer].get("LastTriggerUSecMonotonic") for timer in self.timers),
                  label="natural ticks while closed")
        time.sleep(.4)
        assert self.timer_count() == before_count, "guarded natural timer tick ran a service"
        self.checked("natural ticks skipped with durable guard", counts=before_count)
        self.adapter.drain(timeout=10)
        self.open()
        time.sleep(2)
        assert self.timer_count() == before_count, "removing marker caused spontaneous service start"
        self.checked("opening between slots does not start services")
        self.wait(lambda: all(self.timer_count()[name] > before_count[name] for name in self.main_names),
                  label="next ordinary slot after opening")
        self.checked("next ordinary slot works after opening", counts=self.timer_count())
        self.close()
        self.adapter.drain(timeout=10)

    def late_activation_test(self):
        hold = self.set_hold("late", "pre")
        self.open()
        self.control("start", "--no-block", self.service("late"))
        self.wait(lambda: bool(self.events("late", "pre")), label="late activation pre command")
        self.close()
        self.assert_busy("activating ExecStartPre")
        hold.unlink()
        self.adapter.drain(timeout=10)
        event = self.events("late")
        assert len(event) == 1 and event[0]["marker_exists"], "late activation behavior not observed"
        self.checked("already activating service finishes naturally under guard", event=event[0])

    def chain_test(self, head, tail, failure=False):
        if failure:
            (self.root / "fail" / head).touch()
        hold = self.set_hold(head, "post")
        self.open()
        self.control("start", "--no-block", self.service(head))
        self.wait(lambda: bool(self.events(head, "post")), label=head + " ExecStopPost")
        self.close()
        self.assert_busy(head + " ExecStopPost")
        hold.unlink()
        self.adapter.drain(timeout=10)
        self.wait(lambda: self.properties(self.service(tail)).get("ConditionResult") == "no",
                  label="chain tail condition evaluated")
        assert not self.events(tail), "chained service bypassed guard"
        assert self.events(head), "current service was interrupted"
        self.checked(("OnFailure" if failure else "OnSuccess") + " chain blocked after natural finish")

    def queued_dependency_test(self):
        hold = self.set_hold("blocker", "execute")
        self.control("start", "--no-block", self.service("queued"))
        self.wait(lambda: bool(self.events("blocker")), label="queued dependency blocker")
        queued = self.properties(self.service("queued"))
        assert queued.get("ActiveState") == "inactive" and queued.get("Job") not in (None, "", "0"), queued
        self.assert_busy("inactive service with pending dependency job")
        hold.unlink()
        self.adapter.drain(timeout=10)
        assert not self.events("queued"), "pending dependency job bypassed guard"
        self.open()
        time.sleep(2)
        assert not self.events("queued"), "opening released a forgotten pending dependency job"
        self.close()
        self.adapter.drain(timeout=10)
        self.checked("pending dependency job drains while marker exists", pending=queued["Job"])

    def killed_coordinator_test(self):
        process = subprocess.Popen([sys.executable, __file__, "--coordinator", str(self.root)],
                                   stdout=subprocess.PIPE, text=True)
        try:
            self.wait(lambda: self.marker.exists() and self.marker.read_text() == "fixture-publish-intent\n",
                      timeout=5, label="coordinator durable marker")
            process.send_signal(signal.SIGKILL)
            assert process.wait(timeout=5) == -signal.SIGKILL
            assert self.marker.exists(), "SIGKILL removed durable marker"
            assert json.loads((self.root / "coordinator.json").read_text())["phase"] == "publish_intent"
            before_count = self.timer_count()
            before = self.snapshot("after_coordinator_sigkill")
            self.wait(lambda: all(self.properties(timer).get("LastTriggerUSecMonotonic") !=
                                  before[timer].get("LastTriggerUSecMonotonic") for timer in self.timers),
                      label="timer ticks after coordinator death")
            time.sleep(.4)
            assert self.timer_count() == before_count, "dead coordinator allowed guarded work"
            self.checked("SIGKILL leaves durable guard closed", reboot_tested=False)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def final_invariants(self):
        self.adapter.drain(timeout=10)
        final = self.snapshot("final_closed")
        for timer in self.timers:
            assert final[timer]["ActiveState"] == "active"
            assert final[timer]["UnitFileState"] == self.timer_baseline[timer]["UnitFileState"] == "enabled"
            assert final[timer]["ActiveEnterTimestampMonotonic"] == self.timer_baseline[timer]["ActiveEnterTimestampMonotonic"]
            # TimersCalendar includes a naturally advancing next_elapse value;
            # the actual unchanged unit-file hash proves the schedule instead.
            assert hashlib.sha256((self.unit_directory / timer).read_bytes()).hexdigest() == self.timer_hashes[timer]
        self.checked("all five timers retain config, enabled state and active lifecycle", timer_hashes=self.timer_hashes)
        self.report["passed"] = True

    def collect(self):
        if self.root.exists():
            self.report["events"] = self.events(phase=None)
            self.report["marker_exists_before_cleanup"] = self.marker.exists()
        if self.started:
            journal = run(["journalctl", "--no-pager", "-o", "json", "--since", "@" + str(int(self.report["started_at"])) ,
                           *[part for unit in self.all_units for part in ("-u", unit)]], check=False)
            (self.output / "journal.jsonl").write_text(journal.stdout)
            units = self.control("cat", *self.all_units, check=False)
            (self.output / "units.txt").write_text(units.stdout)
        self.report["finished_at"] = time.time()

    def cleanup(self):
        # Harmless fixture processes only. Never part of maintenance measurements.
        if self.root.exists():
            for hold in (self.root / "holds").glob("*"):
                hold.unlink()
        if self.created_paths:
            self.control("disable", "--now", *self.all_timers, check=False)
            self.control("stop", *self.services, self.service("blocker"), check=False)
            self.control("reset-failed", *self.services, self.service("blocker"), check=False)
        for path in self.created_paths:
            path.unlink(missing_ok=True)
            drop_in = path.with_name(path.name + ".d")
            if drop_in.exists():
                shutil.rmtree(drop_in)
        if self.created_paths:
            self.control("daemon-reload", check=False)
        if self.root.exists():
            shutil.rmtree(self.root)

    def execute(self):
        try:
            self.setup()
            self.timer_guard_test()
            self.late_activation_test()
            self.chain_test("chain-head", "chain-tail")
            self.chain_test("fail-head", "fail-tail", failure=True)
            self.queued_dependency_test()
            self.killed_coordinator_test()
            self.final_invariants()
        except BaseException as error:
            self.report["error"] = repr(error)
            self.report["traceback"] = traceback.format_exc()
        finally:
            try:
                self.collect()
            except BaseException as error:
                self.report["collection_error"] = repr(error)
                self.report["passed"] = False
            try:
                self.cleanup()
            except BaseException as error:
                self.report["cleanup_error"] = repr(error)
                self.report["passed"] = False
            (self.output / "summary.json").write_text(json.dumps(self.report, ensure_ascii=False, indent=2) + "\n")
            print(json.dumps({"passed": self.report["passed"], "checks": len(self.report["checks"]),
                              "error": self.report.get("error"), "summary": str(self.output / "summary.json")}))
            print(json.dumps({"systemd_version": self.report.get("systemd_version"),
                              "checks": self.report["checks"], "limits": self.report["limits"]},
                             ensure_ascii=False))
            if not self.report["passed"]:
                # Artifact download is not always available to the observer;
                # these contain only disposable fixture names and paths.
                print(self.report.get("traceback", "Failure during collection or cleanup"))
                print(json.dumps({"last_busctl_results": self.report.get("busctl_results", [])[-3:],
                                  "last_commands": self.report["commands"][-6:],
                                  "collection_error": self.report.get("collection_error"),
                                  "cleanup_error": self.report.get("cleanup_error")}, ensure_ascii=False))
        return 0 if self.report["passed"] else 1


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--worker":
        return worker(*sys.argv[2:])
    if len(sys.argv) >= 2 and sys.argv[1] == "--coordinator":
        return coordinator_fixture(sys.argv[2])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--ack-disposable-runner", action="store_true")
    args = parser.parse_args()
    if not args.ack_disposable_runner or os.geteuid() != 0 or os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("requires root on a disposable GitHub Actions VM and --ack-disposable-runner")
    return Fixture(args.output).execute()


if __name__ == "__main__":
    raise SystemExit(main())
