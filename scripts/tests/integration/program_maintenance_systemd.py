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
    if name == "natural-pending" and phase == "execute":
        # Fixture evidence: a dependency-delayed ordinary timer job must see the
        # complete version that was durably verified before admission reopened.
        tree = root / "verified-tree"
        event["program_sha256"] = {
            part: hashlib.sha256((tree / part).read_bytes()).hexdigest()
            for part in ("part-a", "part-b")
        }
        event["verification"] = json.loads((root / "verified-receipt.json").read_text())
    descriptor = os.open(root / "events.jsonl", os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.write(descriptor, (json.dumps(event, sort_keys=True) + "\n").encode())
    finally:
        os.close(descriptor)
    deadline = time.monotonic() + (90 if name == "natural-blocker" else 25)
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
        self.guarded_names = (*self.main_names, "late", "chain-head", "chain-tail", "fail-head", "fail-tail", "queued", "natural-pending")
        self.dependency_names = ("blocker", "natural-blocker")
        self.all_names = (*self.guarded_names, *self.dependency_names)
        self.services = tuple(self.service(name) for name in self.guarded_names)
        self.timers = tuple(self.prefix + "-" + name + ".timer" for name in self.main_names)
        # Production services are retained by their active timer routes. Unused
        # scenario services have no such references and systemd may garbage-
        # collect them between show and GetUnit. Far-future fixture timers keep
        # the same reference invariant without starting their scenario work.
        self.pinning_timers = tuple(self.prefix + "-" + name + "-pin.timer"
                                    for name in self.guarded_names if name not in self.main_names)
        self.natural_pending_timer = self.prefix + "-natural-pending.timer"
        self.startup_timers = (*self.timers, *self.pinning_timers)
        self.all_timers = (*self.startup_timers, self.natural_pending_timer)
        self.all_units = (*self.services, *(self.service(name) for name in self.dependency_names), *self.all_timers)
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

    def timer_monotonic_trigger(self, unit):
        """Read the D-Bus uint64; systemctl show formats this as a duration."""
        assert unit in self.all_timers, "refusing foreign timer property"
        prefix = ["busctl", "--system", "--json=short"]
        lookup = prefix + ["call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
                           "org.freedesktop.systemd1.Manager", "GetUnit", "s", unit]
        self.report["commands"].append({"owner": "fixture", "args": lookup, "time": time.time()})
        result = json.loads(run(lookup).stdout)
        assert isinstance(result, dict) and result.get("type") == "o", result
        path = result.get("data")
        if isinstance(path, list) and len(path) == 1:
            path = path[0]
        assert isinstance(path, str) and re.fullmatch(r"/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+", path), result
        query = prefix + ["get-property", "org.freedesktop.systemd1", path,
                          "org.freedesktop.systemd1.Timer", "LastTriggerUSecMonotonic"]
        self.report["commands"].append({"owner": "fixture", "args": query, "time": time.time()})
        result = json.loads(run(query).stdout)
        assert isinstance(result, dict) and result.get("type") == "t", result
        value = result.get("data")
        assert type(value) is int and 0 <= value <= 2**64 - 1, result
        return value

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
            if name == "natural-pending":
                extra = f"Requires={self.service('natural-blocker')}\nAfter={self.service('natural-blocker')}\n"
            timeout = 120 if name == "natural-blocker" else 30
            self.write_unit(self.service(name),
                f"[Unit]\nDescription=Disposable release fixture {name}\n{extra}"
                f"[Service]\nType=oneshot\nExecStartPre={helper} {name} pre\n"
                f"ExecStart={helper} {name} execute\nExecStopPost={helper} {name} post\n"
                f"TimeoutStartSec={timeout}\nTimeoutStopSec=30\n")
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
        # This separate one-shot fixture timer is armed only inside its scenario.
        # Its natural expiry creates the service job; the harness never requests
        # start/restart of natural-pending or its dependency directly.
        self.write_unit(self.natural_pending_timer,
            "[Unit]\nDescription=Disposable natural pending-job timer\n[Timer]\n"
            f"OnActiveSec=2s\nUnit={self.service('natural-pending')}\n"
            "AccuracySec=10ms\nRandomizedDelaySec=0\nPersistent=false\n"
            "[Install]\nWantedBy=timers.target\n")
        self.control("daemon-reload")
        self.control("enable", "--now", *self.startup_timers)
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

    def natural_pending_after_verified_test(self):
        """Finite-exit contract only; NOT an end-to-end installer adapter test.

        A real timer creates a dependency-delayed job under a closed marker. The
        fixture records verified bytes, opens admission without touching jobs,
        and then releases its harmless dependency. Existing timers are unchanged.
        """
        assert self.marker.exists()
        self.adapter.check_guard()
        self.adapter.drain(timeout=10)
        hold = self.set_hold("natural-blocker", "execute")
        tree = self.root / "verified-tree"
        tree.mkdir()
        for part in ("part-a", "part-b"):
            atomic_write(tree / part, "old fixture program " + part + "\n")
        assert not self.events("natural-pending", phase=None)
        bus_path = self.output / "natural-pending-bus.txt"
        bus_error = self.output / "natural-pending-bus-stderr.txt"
        # JobNew/JobRemoved are actual PID 1 signals. The text is retained as an
        # artifact as well as parsed, so Job origin is not inferred from counters.
        monitor_args = ["busctl", "--system",
                        "--match=type='signal',interface='org.freedesktop.systemd1.Manager'",
                        "monitor", "org.freedesktop.systemd1"]
        self.report["commands"].append({"owner": "fixture-monitor", "args": monitor_args, "time": time.time()})
        def job_signals():
            records = []
            raw = bus_path.read_text(errors="replace")
            for block in re.split(r"(?=^.*\bType=signal\b)", raw, flags=re.MULTILINE):
                member = re.search(r"\bMember=(JobNew|JobRemoved)\b", block)
                job = re.search(r"\bUINT32\s+(\d+);", block)
                if member and job and f'STRING "{self.service("natural-pending")}";' in block:
                    record = {"signal": member.group(1), "job_id": int(job.group(1))}
                    # A bus monitor may see the same signal addressed to several
                    # subscribers. A repeated job ID is the same PID 1 job.
                    if record not in records:
                        records.append(record)
            return records
        with bus_path.open("w") as stdout, bus_error.open("w") as stderr:
            monitor = subprocess.Popen(monitor_args, stdout=stdout, stderr=stderr, text=True)
            timer_client = None
            try:
                self.wait(lambda: "Monitoring bus message stream" in bus_error.read_text(),
                          timeout=5, label="systemd signal monitor subscription")
                assert monitor.poll() is None, bus_error.read_text()
                commands_begin = len(self.report["commands"])
                # --wait keeps a real systemd subscriber connected while the
                # timer is active, so PID 1 emits JobNew/JobRemoved even on an
                # otherwise idle VM. This arms ONLY the fixture timer; its
                # service still starts exclusively from the natural timer tick.
                timer_args = ["systemctl", "--wait", "start", self.natural_pending_timer]
                self.report["commands"].append({"owner": "fixture", "args": timer_args[1:], "time": time.time()})
                timer_client = subprocess.Popen(timer_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.wait(lambda: self.properties(self.natural_pending_timer)["ActiveState"] == "active",
                          timeout=5, label="fixture natural timer armed")
                assert timer_client.poll() is None, "timer wait subscriber disconnected"
                timer_before = self.properties(self.natural_pending_timer)
                self.wait(lambda: bool(self.events("natural-blocker")),
                          timeout=8, label="natural timer dependency started")
                pending = self.properties(self.service("natural-pending"))
                assert pending.get("ActiveState") == "inactive", pending
                job_id = int(pending["Job"].split()[0])
                assert job_id > 0 and not self.events("natural-pending", phase=None)
                self.wait(lambda: {"signal": "JobNew", "job_id": job_id} in job_signals(),
                          timeout=5, label="natural pending JobNew signal")
                timer_triggered = self.properties(self.natural_pending_timer)
                trigger_us = self.timer_monotonic_trigger(self.natural_pending_timer)
                assert trigger_us > 0
                observed_pending = time.monotonic()

                # Deliberately replace two files separately while the job waits.
                # This is fixture data, not a checkout or production program.
                write_started = time.monotonic()
                expected = {}
                for part in ("part-a", "part-b"):
                    value = "verified fixture program " + part + "\n"
                    atomic_write(tree / part, value)
                    expected[part] = hashlib.sha256(value.encode()).hexdigest()
                    time.sleep(.1)
                assert not self.events("natural-pending", phase=None), "job executed during partial replacement"
                assert {part: hashlib.sha256((tree / part).read_bytes()).hexdigest()
                        for part in expected} == expected, "fixture version did not verify"
                verified = time.monotonic()
                receipt = {"fixture_only": True, "phase": "verified", "monotonic": verified,
                           "program_sha256": expected, "pending_job_id": job_id}
                atomic_write(self.root / "verified-receipt.json", json.dumps(receipt, sort_keys=True))
                before_open_signals = job_signals()
                assert before_open_signals == [{"signal": "JobNew", "job_id": job_id}], before_open_signals
                self.adapter.check_guard()
                marker_removed = time.monotonic()
                self.open()
                time.sleep(.5)
                pending_after_open = self.properties(self.service("natural-pending"))
                timer_after_open = self.properties(self.natural_pending_timer)
                trigger_after_open_us = self.timer_monotonic_trigger(self.natural_pending_timer)
                assert pending_after_open["Job"] == pending["Job"], "opening replaced/created pending job"
                assert not self.events("natural-pending", phase=None), "marker removal caused a worker start"
                assert job_signals() == before_open_signals, "marker removal changed the service job queue"
                assert timer_after_open["ActiveEnterTimestampMonotonic"] == timer_triggered["ActiveEnterTimestampMonotonic"], "opening changed timer lifecycle"
                assert trigger_after_open_us == trigger_us, "opening changed timer trigger"

                dependency_released = time.monotonic()
                hold.unlink()  # Release only the harmless fixture dependency.
                self.wait(lambda: bool(self.events("natural-pending")),
                          timeout=8, label="same natural job executes verified version")
                self.wait(lambda: self.properties(self.service("natural-pending"))["ActiveState"] == "inactive",
                          timeout=8, label="natural pending job completed")
                self.wait(lambda: {"signal": "JobRemoved", "job_id": job_id} in job_signals(),
                          timeout=5, label="original natural JobRemoved signal")
                events = self.events("natural-pending")
                assert len(events) == 1, "timer job ran more than once"
                event = events[0]
                assert event["invocation_id"] and not event["marker_exists"]
                assert event["program_sha256"] == expected and event["verification"] == receipt
                assert trigger_us / 1_000_000 <= observed_pending <= write_started <= verified <= marker_removed <= dependency_released <= event["monotonic"]
                observed_signals = job_signals()
                assert observed_signals == [{"signal": "JobNew", "job_id": job_id},
                                            {"signal": "JobRemoved", "job_id": job_id}], observed_signals
                assert timer_client.poll() is None, "timer wait subscriber disconnected before completion"
                scenario_commands = self.report["commands"][commands_begin:]
                controlled_services = {self.service("natural-pending"), self.service("natural-blocker")}
                assert not any(set(command["args"]) & controlled_services
                               and set(command["args"]) & {"start", "restart", "stop", "kill"}
                               for command in scenario_commands), "harness created a service start job"
                self.checked("ordinary pending timer job runs verified version after dependency release",
                             fixture_only=True, production_installer_tested=False,
                             job_id=job_id, job_signals=observed_signals,
                             timer_last_trigger_us=trigger_us,
                             timer_active_enter=timer_before["ActiveEnterTimestampMonotonic"],
                             write_started=write_started, verified=verified,
                             marker_removed=marker_removed, dependency_released=dependency_released,
                             pending_before_open=pending["Job"], pending_after_open=pending_after_open["Job"],
                             execution=event, bus_artifact=bus_path.name)
            finally:
                self.close()
                hold.unlink(missing_ok=True)
                if timer_client is not None and timer_client.poll() is None:
                    # Terminating the observer does not stop/restart its timer.
                    timer_client.terminate()
                    try:
                        timer_client.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        timer_client.kill()
                        timer_client.wait(timeout=5)
                monitor.terminate()
                try:
                    monitor.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    monitor.kill()
                    monitor.wait(timeout=5)
        self.adapter.drain(timeout=10)

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
            self.control("stop", *self.services, *(self.service(name) for name in self.dependency_names), check=False)
            self.control("reset-failed", *self.services, *(self.service(name) for name in self.dependency_names), check=False)
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
            self.natural_pending_after_verified_test()
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
