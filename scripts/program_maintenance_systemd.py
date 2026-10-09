#!/usr/bin/env python3
"""Persistent service gate for a future release coordinator (not enabled in CLI).

Timers stay running. A one-time drop-in bootstrap adds a non-trigger, negative
ConditionPathExists to each service; leases subsequently change only the marker.
This adapter does not own the marker, acquire repository locks, or publish code.
Its caller must drain and lock all writers before allowing publication/install.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time


class MaintenanceGateError(RuntimeError):
    pass


def command(args):
    try:
        cp = subprocess.run(args, capture_output=True, text=True, timeout=30,
                            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise MaintenanceGateError("Не удалось прочитать состояние systemd") from exc
    if cp.returncode:
        # Service/environment output can contain secrets; never echo stderr.
        raise MaintenanceGateError("systemd не подтвердил требуемую операцию")
    return cp.stdout


def safe_absolute(value):
    path = Path(value)
    if (not path.is_absolute() or ".." in path.parts
            or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path))):
        raise MaintenanceGateError("Нужен простой абсолютный путь без подстановок systemd")
    return path


def no_links(path, *, file=False):
    """Reject linked ancestors as well as a substituted final file."""
    for parent in (*reversed(path.parents), path):
        try:
            info = parent.lstat()
        except FileNotFoundError:
            if parent == path and not file:
                return
            raise MaintenanceGateError("Отсутствует каталог защиты") from None
        if stat.S_ISLNK(info.st_mode):
            raise MaintenanceGateError("Символическая ссылка в пути защиты")
        if parent != path and not stat.S_ISDIR(info.st_mode):
            raise MaintenanceGateError("Путь защиты проходит через обычный файл")
        if parent == path and file and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
            raise MaintenanceGateError("Нужен отдельный обычный файл защиты")


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class SystemdMaintenanceGate:
    DROPIN = "90-program-maintenance.conf"
    SERVICE_PROPERTIES = ("LoadState", "ActiveState", "SubState", "Job", "MainPID",
                          "ControlPID", "ControlGroup", "NeedDaemonReload", "DropInPaths")
    TIMER_PROPERTIES = ("LoadState", "ActiveState", "UnitFileState", "FragmentPath",
                        "DropInPaths", "TimersCalendar", "TimersMonotonic", "Persistent",
                        "LastTriggerUSec", "NextElapseUSecRealtime", "NextElapseUSecMonotonic")

    def __init__(self, marker, services, *, timers=(),
                 unit_directory=Path("/etc/systemd/system"),
                 cgroup_directory=Path("/sys/fs/cgroup"), runner=command):
        self.marker = safe_absolute(marker)
        self.unit_directory = safe_absolute(unit_directory)
        self.cgroup_directory = safe_absolute(cgroup_directory)
        self.services = tuple(services)
        self.timers = tuple(timers)
        if (not self.services or len(set(self.services)) != len(self.services)
                or len(set(self.timers)) != len(self.timers)):
            raise MaintenanceGateError("Нужен непустой уникальный список служб")
        for names, suffix in ((self.services, "service"), (self.timers, "timer")):
            if any(not isinstance(name, str) or not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9_.-]*\." + suffix, name) for name in names):
                raise MaintenanceGateError("Неизвестное имя службы или таймера")
        self.run = runner

    @property
    def content(self):
        return ("# Managed release gate; schedules are unchanged.\n[Unit]\n"
                f"ConditionPathExists=!{self.marker}\n").encode()

    def properties(self, unit, names):
        raw = self.run(["systemctl", "show", unit, "--property=" + ",".join(names)])
        props = {}
        for line in raw.splitlines():
            key, sep, value = line.partition("=")
            if not sep or key in props or key not in names:
                raise MaintenanceGateError("Неоднозначный ответ systemd")
            props[key] = value
        if set(props) != set(names) or props.get("LoadState") != "loaded":
            raise MaintenanceGateError("Не подтверждено загруженное состояние службы")
        return props

    def snapshot(self):
        return {
            "services": {unit: self.properties(unit, self.SERVICE_PROPERTIES) for unit in self.services},
            "timers": {unit: self.properties(unit, self.TIMER_PROPERTIES) for unit in self.timers},
        }

    def _conditions(self, unit):
        prefix = ["busctl", "--system", "--json=short"]
        try:
            result = json.loads(self.run(prefix + ["call", "org.freedesktop.systemd1",
                "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "GetUnit", "s", unit]))
            data = result["data"]
            # busctl wraps method return values in an array, even for one value.
            object_path = data[0] if isinstance(data, list) and len(data) == 1 else data
            if result["type"] != "o" or not isinstance(object_path, str) or not re.fullmatch(
                    r"/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+", object_path):
                raise ValueError("object path")
            result = json.loads(self.run(prefix + ["get-property", "org.freedesktop.systemd1",
                object_path, "org.freedesktop.systemd1.Unit", "Conditions"]))
            if result["type"] != "a(sbbsi)" or not isinstance(result["data"], list):
                raise ValueError("conditions")
            conditions = result["data"]
            for item in conditions:
                if (not isinstance(item, list) or len(item) != 5 or not isinstance(item[0], str)
                        or type(item[1]) is not bool or type(item[2]) is not bool
                        or not isinstance(item[3], str) or type(item[4]) is not int):
                    raise ValueError("condition")
            return conditions
        except (ValueError, KeyError, TypeError) as exc:
            raise MaintenanceGateError("Не удалось проверить эффективные Conditions") from exc

    def check_guard(self):
        no_links(self.marker.parent)
        if not self.marker.parent.is_dir():
            raise MaintenanceGateError("Отсутствует постоянный каталог marker")
        no_links(self.marker)
        for unit in self.services:
            path = self.unit_directory / (unit + ".d") / self.DROPIN
            no_links(path, file=True)
            if path.read_bytes() != self.content:
                raise MaintenanceGateError("Изменён файл постоянной защиты " + unit)
            props = self.properties(unit, self.SERVICE_PROPERTIES)
            if (props["NeedDaemonReload"] != "no"
                    or str(path) not in props["DropInPaths"].split()):
                raise MaintenanceGateError("Защита не загружена менеджером systemd " + unit)
            matches = [item for item in self._conditions(unit)
                       if item[:4] == ["ConditionPathExists", False, True, str(self.marker)]]
            if len(matches) != 1:
                raise MaintenanceGateError("Условие защиты сброшено или изменено " + unit)
        return {"guarded": True, "services": list(self.services), "marker": str(self.marker)}

    def bootstrap(self):
        """One-time bootstrap. Caller audits routing/permissions BEFORE this call.

        Writes only absent exact drop-ins and reloads the manager if needed.
        Does not create/open the marker, or start/stop/enable any unit. A partial
        failed bootstrap is safe to retry, but never a publication permit.
        """
        no_links(self.unit_directory)
        no_links(self.marker.parent)
        if not self.unit_directory.is_dir() or not self.marker.parent.is_dir():
            raise MaintenanceGateError("Нужны существующие постоянные каталоги защиты")
        snapshot = self.snapshot()
        changed = False
        for unit in self.services:
            folder = self.unit_directory / (unit + ".d")
            no_links(folder)
            if not folder.exists():
                folder.mkdir(mode=0o755)
                sync_directory(self.unit_directory)
            path = folder / self.DROPIN
            no_links(path)
            if path.exists():
                no_links(path, file=True)
                if path.read_bytes() != self.content:
                    raise MaintenanceGateError("Неизвестный существующий drop-in " + unit)
                continue
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            with os.fdopen(fd, "wb") as stream:
                stream.write(self.content)
                stream.flush()
                os.fsync(stream.fileno())
            sync_directory(folder)
            changed = True
        if changed or any(props["NeedDaemonReload"] == "yes" for props in snapshot["services"].values()):
            self.run(["systemctl", "daemon-reload"])
        result = self.check_guard()
        return dict(result, bootstrapped=changed, before=snapshot, after=self.snapshot())

    def busy(self):
        busy = {}
        for unit in self.services:
            props = self.properties(unit, self.SERVICE_PROPERTIES)
            if props["ActiveState"] not in ("inactive", "failed"):
                busy[unit] = props
                continue
            if not re.fullmatch(r"0(?: /)?", props["Job"]):
                busy[unit] = props
                continue
            if props["MainPID"] != "0" or props["ControlPID"] != "0":
                busy[unit] = props
                continue
            group = props["ControlGroup"]
            if group:
                if not group.startswith("/") or ".." in Path(group).parts:
                    raise MaintenanceGateError("Неизвестный cgroup службы")
                directory = self.cgroup_directory / group.lstrip("/")
                # cgroup.events populated includes children in cgroup v2.
                if directory.exists():
                    no_links(directory)
                    try:
                        events = dict(line.split() for line in (directory / "cgroup.events").read_text().splitlines())
                    except (OSError, ValueError) as exc:
                        raise MaintenanceGateError("Не удалось проверить дочерние процессы службы") from exc
                    if events.get("populated") != "0":
                        busy[unit] = props
        return busy

    def drain(self, timeout=300):
        deadline = time.monotonic() + timeout
        while True:
            no_links(self.marker, file=True)
            self.check_guard()
            pending = self.busy()
            if not pending:
                no_links(self.marker, file=True)
                return {}
            if time.monotonic() >= deadline:
                raise MaintenanceGateError("Службы ещё заняты; процессы не остановлены")
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))
