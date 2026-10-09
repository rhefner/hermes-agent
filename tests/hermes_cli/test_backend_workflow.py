"""Preparation CLI integration; no production guard or remote controller access."""
import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.platforms('linux')


@pytest.fixture
def cli(tmp_path):
    root = tmp_path / 'fleet'
    bootstrap = """
import sys
from pathlib import Path
from agent import maintenance_inference as g
from agent import maintenance_admission as a
from hermes_cli.backend_isolation import main
root=Path(sys.argv.pop(1))
g.directory=lambda:root
a.directory=lambda:root.parent/'accounting'
sys.exit(main())
"""

    def run(*args):
        return subprocess.run([sys.executable, '-c', bootstrap, str(root), 'workflow', *args],
                              capture_output=True, text=True, timeout=20)
    return root, run


@pytest.fixture
def enrolled_cli(cli, monkeypatch):
    from agent import maintenance_inference as guard
    from agent import maintenance_admission as accounting
    from hermes_cli import maintenance_inference as legacy
    from hermes_cli import backend_isolation as controller
    root, _ = cli
    monkeypatch.setattr(guard, 'directory', lambda: root)
    monkeypatch.setattr(accounting, 'directory', lambda: root.parent / 'accounting')
    legacy.write_state({'version': 1, 'phase': 'normal'})
    controller.migrate({'spark-pair': ['http://fixture-pair:8000']}, approval='fixture enrollment')
    return cli


def test_status_is_read_only_even_before_fleet_directory_exists(cli):
    root, run = cli
    result = run('status')
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value['guard'] is None
    assert value['workflow'] is None
    assert value['execution_supported'] is False
    assert not root.exists()


def test_real_cli_plan_approval_and_status_do_not_claim_execution(enrolled_cli, tmp_path):
    root, run = enrolled_cli
    guard_before = (root / 'state.json').read_bytes()
    plan = {'adapter': 'spark-pair-eight-stream', 'backends': ['spark-pair'],
            'controller_revision': 'a' * 40,
            'delta': {'PARALLEL': '8'},
            'preserve': {'CONTEXT': '1048576', 'MODEL_ID': 'fixture/model'},
            'qa': ['direct', 'facade', 'unaffected', 'capacity', 'health', 'settings'],
            'budget_seconds': 900, 'margin_seconds': 300, 'tasks': []}
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    result = run('plan', '--file', str(path))
    assert result.returncode == 0, result.stderr
    created = json.loads(result.stdout)
    before = (root / 'operations' / (created['id'] + '.json')).read_bytes()
    denied = run('approve', created['id'], '--digest', 'b' * 64,
                 '--reference', 'message-1', '--approval', 'approved')
    assert denied.returncode == 1
    assert (root / 'operations' / (created['id'] + '.json')).read_bytes() == before
    result = run('approve', created['id'], '--digest', created['plan_digest'],
                 '--reference', 'message-1', '--approval', 'approved')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['approval']['release_policy'] == 'separate'
    result = run('inspect', created['id'])
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status['workflow']['stage'] == 'planned'
    assert status['activity'] == 'blocked'
    assert status['execution_supported'] is False
    assert 'not implemented' in status['next_action']
    assert (root / 'state.json').read_bytes() == guard_before
    assert not (root.parent / 'accounting').exists()
    # No implementation means no fake run, mutation, or acceptance result.
    refused = run('run', created['id'])
    assert refused.returncode != 0
    assert (root / 'state.json').read_bytes() == guard_before


def test_status_exposes_actual_guard_separately_from_preparation(enrolled_cli):
    root, run = enrolled_cli
    state = root / 'state.json'
    before = state.read_bytes()
    result = run('status')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['guard']['phase'] == 'normal'
    assert state.read_bytes() == before
    assert not (root / 'operations').exists()


@pytest.mark.parametrize('interrupted', [False, True])
def test_status_never_reports_idle_for_retained_isolation(enrolled_cli, monkeypatch, interrupted):
    import os
    from agent import backend_isolation as backend
    from hermes_cli import maintenance_inference as legacy
    root, run = enrolled_cli
    monkeypatch.setattr(legacy, 'probe', lambda model: None)
    active = backend.enter(['spark-pair'], 'fixture-independent', executor_pid=os.getpid())
    if interrupted:
        active['executor']['start'] = 'not-the-live-process'
        legacy.write_state(active)
    before = (root / 'state.json').read_bytes()
    result = run('status')
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status['activity'] == ('interrupted' if interrupted else 'maintenance_active')
    assert status['guard']['affected'] == ['spark-pair']
    assert (root / 'state.json').read_bytes() == before
