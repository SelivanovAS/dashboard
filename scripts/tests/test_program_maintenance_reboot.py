"""Probe retry boundaries only; these unit tests do not claim a VM boot proof."""
import importlib.util
from pathlib import Path
import subprocess

import pytest

SPEC = importlib.util.spec_from_file_location("reboot_fixture", Path(__file__).parent / "integration/program_maintenance_reboot.py")
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


def test_temporary_ssh_timeout_and_offline_exit_are_pending_then_recover(monkeypatch):
    replies = iter((subprocess.TimeoutExpired("ssh", 6),
                    subprocess.CompletedProcess("ssh", 255, "", "offline"),
                    subprocess.CompletedProcess("ssh", 0, "new-boot-id\n", "")))
    def probe(*args, **kwargs):
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply
    monkeypatch.setattr(fixture, "run", probe)
    assert fixture.probe_guest(["ssh"], "read boot id") is None
    assert fixture.probe_guest(["ssh"], "read boot id") is None
    assert fixture.probe_guest(["ssh"], "read boot id") == "new-boot-id\n"


def test_probe_does_not_hide_unexpected_local_failure(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("ssh binary missing")
    monkeypatch.setattr(fixture, "run", missing)
    with pytest.raises(FileNotFoundError, match="ssh binary missing"):
        fixture.probe_guest(["ssh"], "read boot id")


def test_permanently_unreachable_guest_still_fails_outer_deadline(monkeypatch):
    def offline(*args, **kwargs):
        raise subprocess.TimeoutExpired("ssh", 6)
    monkeypatch.setattr(fixture, "run", offline)
    clock = iter((0.0, 2.0))
    monkeypatch.setattr(fixture.time, "monotonic", lambda: next(clock))
    with pytest.raises(AssertionError, match="Timed out proving real reboot"):
        fixture.wait(lambda: fixture.probe_guest(["ssh"], "read boot id") is not None,
                     timeout=1, label="real reboot")
