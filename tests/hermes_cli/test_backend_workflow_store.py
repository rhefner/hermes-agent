"""Preparation records use the fleet directory, not profile-local homes."""
import copy
import importlib
import json
import subprocess
import sys

import pytest

from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from hermes_cli import maintenance_inference as legacy
from hermes_cli import backend_isolation as controller

pytestmark = pytest.mark.platforms('linux')


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    root = tmp_path / 'fleet'
    monkeypatch.setattr(guard, 'directory', lambda: root)
    monkeypatch.setattr(accounting, 'directory', lambda: tmp_path / 'accounting')
    legacy.write_state({'version': 1, 'phase': 'normal'})
    controller.migrate({'spark-pair': ['http://fixture-pair:8000']}, approval='fixture enrollment')
    return root


@pytest.fixture
def store():
    return importlib.import_module('hermes_cli.backend_workflow_store')


def plan():
    return {'adapter': 'spark-pair-eight-stream', 'backends': ['spark-pair'],
            'controller_revision': 'a' * 40,
            'delta': {'PARALLEL': '8', 'TF_GLM_MULTI_WINDOW': '64',
                      'TF_ROCE_MAX_KB': '1024', 'MEMORY_RESERVE_GIB': '19.6'},
            'preserve': {'CONTEXT': '1048576', 'MODEL_ID': 'fixture/model'},
            'qa': ['direct', 'facade', 'unaffected', 'capacity', 'health', 'settings'],
            'budget_seconds': 900, 'margin_seconds': 300, 'tasks': []}


def test_profile_independence_and_immutable_digest_bound_approval(fleet, store, monkeypatch):
    guard_before = (fleet / 'state.json').read_bytes()
    operation = store.create(plan())
    original = copy.deepcopy(operation)
    assert (fleet / 'operations' / (operation['id'] + '.json')).stat().st_mode & 0o777 == 0o600
    for profile in ('A', 'B', 'A'):
        monkeypatch.setenv('HERMES_HOME', str(fleet.parent / profile))
        assert store.load(operation['id']) == original
    with pytest.raises(guard.MaintenanceIsolationError):
        store.approve(operation['id'], 'b' * 64, reference='message-1', text='approved')
    assert store.load(operation['id']) == original
    approved = store.approve(operation['id'], operation['plan_digest'],
                             reference='message-1', text='approved')
    assert approved['approval']['release_policy'] == 'separate'
    assert approved['approval']['plan_digest'] == operation['plan_digest']
    assert approved['stage'] == 'planned'  # Approval is not a preflight or execution receipt.
    with pytest.raises(guard.MaintenanceIsolationError):
        store.approve(operation['id'], operation['plan_digest'], reference='message-2',
                      text='broader approval', release_policy='conditional')
    assert store.load(operation['id']) == approved
    assert (fleet / 'state.json').read_bytes() == guard_before
    assert not (fleet.parent / 'accounting').exists()


@pytest.mark.parametrize('change', [
    {'adapter': 'arbitrary-shell'}, {'backends': ['spark-pair', 'spark-pair']},
    {'backends': ['spark-3']}, {'controller_revision': 'main'},
    {'command': 'docker restart anything'}, {'budget_seconds': True},
    {'margin_seconds': -1}, {'qa': ['direct']}, {'tasks': ['../../unsafe']},
    {'delta': {'PARALLEL': '8', 'API_KEY': 'must-not-be-recorded'}},
    {'delta': {'PARALLEL': '8', 'command': 'restart'}},
    {'delta': {'PARALLEL': '8; reboot'}},
    {'preserve': {'MODEL_ID': 'fixture/model', 'CONTEXT': '1048576', 'PARALLEL': '4'}},
    {'preserve': {'MODEL_ID': 'fixture/model', 'CONTEXT': '1048576', 'HF_TOKEN': 'secret'}},
    {'preserve': {'MODEL_ID': 'fixture/model; execute', 'CONTEXT': '1048576'}},
    {'delta': {'MEMORY_RESERVE_GIB': 'NaN'}},
    {'delta': {'MEMORY_RESERVE_GIB': '1.0'}},

])
def test_invalid_plans_do_not_create_operation_storage(fleet, store, change):
    value = plan(); value.update(change)
    with pytest.raises(guard.MaintenanceIsolationError):
        store.create(value)
    assert not (fleet / 'operations').exists()


def test_duplicate_operation_id_never_overwrites_existing_record(fleet, store, monkeypatch):
    op = store.create(plan())
    path = fleet / 'operations' / (op['id'] + '.json')
    before = path.read_bytes()
    monkeypatch.setattr(store.uuid, 'uuid4', lambda: store.uuid.UUID(op['id']))
    with pytest.raises(guard.MaintenanceIsolationError, match='already exists'):
        store.create(plan())
    assert path.read_bytes() == before


def test_tamper_truncation_and_path_escape_fail_closed(fleet, store):
    op = store.create(plan())
    path = fleet / 'operations' / (op['id'] + '.json')
    op['plan']['delta']['PARALLEL'] = '16'
    path.write_text(json.dumps(op))
    with pytest.raises(guard.MaintenanceIsolationError):
        store.load(op['id'])
    path.write_text('{')
    with pytest.raises(guard.MaintenanceIsolationError):
        store.load(op['id'])
    for bad in ('../state', '', 'x' * 32):
        with pytest.raises(guard.MaintenanceIsolationError):
            store.load(bad)
    assert controller.backend.configuration()['phase'] == 'normal'


def test_operation_lock_is_exclusive_between_processes(fleet, store):
    op = store.create(plan())
    code = """
from pathlib import Path
from agent import maintenance_inference as g
from agent import maintenance_admission as a
from hermes_cli import backend_workflow_store as s
import sys
g.directory=lambda:Path(sys.argv[1])
a.directory=lambda:Path(sys.argv[1]).parent/'accounting'
try:
    with s.operation_lock(sys.argv[2]):
        print('LOCK_ACQUIRED')
except g.MaintenanceIsolationError:
    print('LOCK_BUSY')
"""
    args = [sys.executable, '-c', code, str(fleet), op['id']]
    with store.operation_lock(op['id']):
        result = subprocess.run(args, text=True, capture_output=True, timeout=20, check=True)
        assert result.stdout.strip() == 'LOCK_BUSY'
    result = subprocess.run(args, text=True, capture_output=True, timeout=20, check=True)
    assert result.stdout.strip() == 'LOCK_ACQUIRED'


def test_unenrolled_plan_cannot_create_a_broken_guard_directory(fleet, store, monkeypatch):
    absent = fleet.parent / 'unenrolled'
    monkeypatch.setattr(guard, 'directory', lambda: absent)
    with pytest.raises(guard.MaintenanceIsolationError, match='enrolled|Enrolled'):
        store.create(plan())
    assert not absent.exists()


@pytest.mark.parametrize('target', ['directory', 'record'])
def test_permissive_operation_storage_is_refused_without_repair(fleet, store, target):
    op = store.create(plan())
    path = fleet / 'operations'
    if target == 'record':
        path = path / (op['id'] + '.json')
    before = (fleet / 'state.json').read_bytes()
    path.chmod(0o755 if target == 'directory' else 0o644)
    with pytest.raises(guard.MaintenanceIsolationError):
        store.load(op['id'])
    with pytest.raises(guard.MaintenanceIsolationError):
        store.approve(op['id'], op['plan_digest'], reference='message', text='approved')
    assert path.stat().st_mode & 0o077
    assert (fleet / 'state.json').read_bytes() == before


def test_explicit_conditional_approval_binds_complete_qa_checklist(fleet, store):
    op = store.create(plan())
    approved = store.approve(op['id'], op['plan_digest'], reference='message-3',
        text='release only after required QA passes', release_policy='conditional')
    approval = approved['approval']
    assert approval['release_policy'] == 'conditional'
    assert approval['qa_digest'] == store.digest(op['plan']['qa'])
    assert store.approve(op['id'], op['plan_digest'], reference='message-3',
        text='release only after required QA passes', release_policy='conditional') == approved
