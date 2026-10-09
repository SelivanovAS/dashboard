"""Gate tests; real manager/timer behaviour is a separate Linux fixture."""
import json
from pathlib import Path

import pytest

from program_maintenance_systemd import MaintenanceGateError, SystemdMaintenanceGate


@pytest.fixture
def gate(tmp_path):
    root = tmp_path.resolve()
    units = root / 'units'
    units.mkdir()
    state = root / 'state'
    state.mkdir()
    groups = root / 'cgroups'
    groups.mkdir()
    calls = []
    conditions = [['ConditionPathExists', False, True, str(state / 'blocked'), 0]]
    properties = dict(LoadState='loaded', ActiveState='inactive', SubState='dead',
        Job='0', MainPID='0', ControlPID='0', ControlGroup='', NeedDaemonReload='no',
        DropInPaths=str(units / 'fixture.service.d/90-program-maintenance.conf'))
    def run(args):
        calls.append(args)
        if args[:2] == ['systemctl', 'show']:
            return '\n'.join(k + '=' + v for k, v in properties.items()) + '\n'
        if args == ['systemctl', 'daemon-reload']:
            properties['NeedDaemonReload'] = 'no'
            return ''
        if 'GetUnit' in args:
            return json.dumps({'type': 'o', 'data': ['/org/freedesktop/systemd1/unit/fixture_2eservice']})
        if 'get-property' in args:
            return json.dumps({'type': 'a(sbbsi)', 'data': conditions})
        raise AssertionError(args)
    result = SystemdMaintenanceGate(state / 'blocked', ['fixture.service'],
        unit_directory=units, cgroup_directory=groups, runner=run)
    result.test_properties = properties
    result.test_conditions = conditions
    result.test_calls = calls
    return result


def test_bootstrap_persists_one_guard_and_never_restarts_units(gate):
    first = gate.bootstrap()
    assert first['bootstrapped'] and not gate.marker.exists()
    path = gate.unit_directory / 'fixture.service.d' / gate.DROPIN
    assert path.read_bytes() == gate.content
    assert not gate.bootstrap()['bootstrapped']
    assert gate.test_calls.count(['systemctl', 'daemon-reload']) == 1
    assert all(not (call[0] == 'systemctl' and call[1] in
                    ('start', 'stop', 'restart', 'reload', 'enable', 'disable')) for call in gate.test_calls)


@pytest.mark.parametrize('condition', [
    [], [['ConditionPathExists', True, True, '/somewhere', 0]],
    [['ConditionPathExists', False, False, '/somewhere', 0]],
])
def test_effective_condition_must_be_negative_and_nontrigger(gate, condition):
    gate.bootstrap()
    gate.test_conditions[:] = condition
    with pytest.raises(MaintenanceGateError, match='Условие'):
        gate.check_guard()


def test_later_dropin_reset_or_manager_reload_pending_is_rejected(gate):
    gate.bootstrap()
    gate.test_properties['NeedDaemonReload'] = 'yes'
    with pytest.raises(MaintenanceGateError, match='не загружена'):
        gate.check_guard()
    gate.test_properties['NeedDaemonReload'] = 'no'
    gate.test_properties['DropInPaths'] = ''
    with pytest.raises(MaintenanceGateError, match='не загружена'):
        gate.check_guard()


def test_unknown_dropin_is_preserved_and_no_reload(gate):
    path = gate.unit_directory / 'fixture.service.d' / gate.DROPIN
    path.parent.mkdir()
    path.write_text('[Unit]\nConditionPathExists=/unrelated\n')
    with pytest.raises(MaintenanceGateError, match='Неизвестный'):
        gate.bootstrap()
    assert 'unrelated' in path.read_text()
    assert ['systemctl', 'daemon-reload'] not in gate.test_calls


def test_symlinked_guard_directory_is_rejected(gate):
    target = gate.unit_directory / 'other'
    target.mkdir()
    (gate.unit_directory / 'fixture.service.d').symlink_to(target, target_is_directory=True)
    with pytest.raises(MaintenanceGateError, match='Символическая'):
        gate.bootstrap()
    assert not list(target.iterdir())


@pytest.mark.parametrize(('key', 'value'), [
    ('ActiveState', 'activating'), ('ActiveState', 'deactivating'),
    ('ActiveState', 'reloading'), ('ActiveState', 'active'),
    ('Job', '123 /org/freedesktop/systemd1/job/123'),
    ('MainPID', '3'), ('ControlPID', '8'),
])
def test_drain_detects_active_helpers_and_queued_jobs_without_killing(gate, key, value):
    gate.bootstrap()
    gate.marker.write_text('blocked')
    gate.test_properties[key] = value
    assert 'fixture.service' in gate.busy()
    with pytest.raises(MaintenanceGateError, match='ещё заняты'):
        gate.drain(timeout=0)
    assert all(call[1] != 'stop' for call in gate.test_calls if call[0] == 'systemctl')


def test_exited_main_with_live_cgroup_children_is_busy(gate):
    gate.bootstrap()
    folder = gate.cgroup_directory / 'system.slice/fixture.service'
    folder.mkdir(parents=True)
    gate.test_properties['ControlGroup'] = '/system.slice/fixture.service'
    (folder / 'cgroup.events').write_text('populated 1\nfrozen 0\n')
    assert gate.busy()
    (folder / 'cgroup.events').write_text('populated 0\nfrozen 0\n')
    assert not gate.busy()


def test_unreadable_live_cgroup_cannot_be_treated_as_idle(gate):
    gate.bootstrap()
    folder = gate.cgroup_directory / 'system.slice/fixture.service'
    folder.mkdir(parents=True)
    gate.test_properties['ControlGroup'] = '/system.slice/fixture.service'
    with pytest.raises(MaintenanceGateError, match='дочерние процессы'):
        gate.busy()


def test_drain_without_closed_gate_is_not_a_reservation(gate):
    gate.bootstrap()
    with pytest.raises(MaintenanceGateError):
        gate.drain(timeout=0)


def test_marker_disappearing_during_poll_never_reports_safe_drain(gate, monkeypatch):
    gate.bootstrap()
    gate.marker.write_text('blocked')
    def disappeared():
        gate.marker.unlink()
        return {}
    monkeypatch.setattr(gate, 'busy', disappeared)
    with pytest.raises(MaintenanceGateError):
        gate.drain(timeout=1)


def test_invalid_systemd_names_or_expanded_paths_are_rejected(tmp_path):
    for marker in ['/tmp/%N', '/tmp/foo\n[Service]', '/tmp/../etc', '/tmp/with space']:
        with pytest.raises(MaintenanceGateError):
            SystemdMaintenanceGate(marker, ['fixture.service'])
    for name in ['--all', 'fixture.timer', 'other@.service', 'fixture.service\n']:
        with pytest.raises(MaintenanceGateError):
            SystemdMaintenanceGate(tmp_path.resolve() / 'blocked', [name])
