"""Operation-bound release exercises real temporary guard state and its lock."""
from contextlib import contextmanager
import os
import sys
import uuid

import pytest

from agent import backend_isolation as backend
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from hermes_cli import backend_isolation as cli
from hermes_cli import maintenance_inference as legacy

pytestmark = pytest.mark.platforms('linux')


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, 'directory', lambda: tmp_path / 'isolation')
    monkeypatch.setattr(accounting, 'directory', lambda: tmp_path / 'accounting')
    # Only the remote inference probe is doubled; state, identity and locks are real.
    monkeypatch.setattr(legacy, 'probe', lambda model: None)
    legacy.write_state({'version': 1, 'phase': 'normal'})
    cli.migrate({'pair': ['http://fixture-pair:8000'],
                 'other': ['http://fixture-other:8000']}, approval='fixture migration')
    return guard.directory() / 'state.json'


def enter():
    return backend.enter(['pair'], 'fixture-independent-model', executor_pid=os.getpid())


@pytest.mark.parametrize('surface', ['api', 'cli'])
def test_stale_operation_cannot_release_new_window(fleet, monkeypatch, capsys, surface):
    first = enter()
    cli.release('explicit fixture release')
    second = enter()
    before = fleet.read_bytes()
    assert first['operation'] != second['operation']

    def release(expected):
        if surface == 'api':
            cli.release('fixture post-QA approval', expected_operation=expected)
            return 0
        monkeypatch.setattr(sys, 'argv', ['backend-isolation', 'release',
            '--approval', 'fixture post-QA approval', '--expected-operation', expected])
        return cli.main()

    if surface == 'api':
        with pytest.raises(guard.MaintenanceIsolationError, match='operation changed'):
            release(first['operation'])
    else:
        assert release(first['operation']) == 1
        assert 'operation changed' in capsys.readouterr().out
    assert fleet.read_bytes() == before
    assert backend.observed()['phase'] == 'active'

    assert release(second['operation']) == 0
    after = backend.configuration()
    assert after['phase'] == 'normal'
    assert after['affected'] == []
    assert after['backends'] == second['backends']


def test_operation_comparison_uses_state_read_under_lock(fleet, monkeypatch):
    first = enter()
    original_lock = cli.locked
    replacement = dict(first, operation=uuid.uuid4().hex)
    boundary_state = []

    @contextmanager
    def changed_before_acquisition():
        with original_lock():
            # Model another operation becoming current before our locked read.
            legacy.write_state(replacement)
            boundary_state.append(fleet.read_bytes())
            yield

    monkeypatch.setattr(cli, 'locked', changed_before_acquisition)
    with pytest.raises(guard.MaintenanceIsolationError, match='operation changed'):
        cli.release('fixture approval', expected_operation=first['operation'])
    assert fleet.read_bytes() == boundary_state[0]
    assert backend.configuration()['operation'] == replacement['operation']


def test_operation_expectation_is_not_silently_ignored_by_other_actions(fleet, monkeypatch):
    current = enter()
    before = fleet.read_bytes()
    monkeypatch.setattr(sys, 'argv', ['backend-isolation', 'status',
        '--expected-operation', current['operation']])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert fleet.read_bytes() == before


def test_workflow_release_checks_executor_under_same_state_lock(fleet, monkeypatch):
    first = enter()
    original_lock = cli.locked
    replacement = dict(first, executor=dict(first['executor'], start='replacement'))
    boundary_state = []

    @contextmanager
    def changed_before_acquisition():
        with original_lock():
            legacy.write_state(replacement)
            boundary_state.append(fleet.read_bytes())
            yield

    monkeypatch.setattr(cli, 'locked', changed_before_acquisition)
    with pytest.raises(guard.MaintenanceIsolationError, match='executor'):
        cli.release('fixture approval', expected_operation=first['operation'],
                    expected_executor=first['executor'])
    assert fleet.read_bytes() == boundary_state[0]


def test_workflow_release_expiry_is_checked_under_state_lock(fleet, monkeypatch):
    first = enter()
    before = fleet.read_bytes()
    original_lock = cli.locked
    deadline = cli.time.time() + 1

    @contextmanager
    def time_passes_before_acquisition():
        with original_lock():
            monkeypatch.setattr(cli.time, 'time', lambda: deadline + 1)
            yield

    monkeypatch.setattr(cli, 'locked', time_passes_before_acquisition)
    with pytest.raises(guard.MaintenanceIsolationError, match='expired'):
        cli.release('fixture approval', expected_operation=first['operation'],
                    expected_executor=first['executor'], not_after=deadline)
    assert fleet.read_bytes() == before
