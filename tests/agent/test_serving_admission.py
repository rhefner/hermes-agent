"""Serving admission regressions: real private SQLite, mocked deployment/probes.

No live provider, SSH, container or production state mutation.
"""
import json
import os
from pathlib import Path
import time
from unittest.mock import Mock

import pytest
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from agent import serving_admission as a
from agent import serving_execution_policy as policy
from hermes_cli import maintenance_inference as ctl

HOMES = ctl.profiles()
MODEL = 'external-fixture'


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    root = tmp_path / 'maintenance-inference'
    monkeypatch.setattr(guard, 'directory', lambda: root)
    monkeypatch.setattr(a, 'profile_homes', lambda: [str(tmp_path)])
    return root


def activate():
    value = {'version': 1, 'phase': 'active', 'provider': 'openai-codex',
             'model': MODEL, 'base_url': guard.CLOUD_BASE, 'verified_profiles': a.profile_homes()}
    ctl.write_state(value)
    return value


@pytest.mark.parametrize('home', HOMES, ids=str)
@pytest.mark.parametrize('alias', ['primary', 'primary-ablit', 'secondary'])
def test_every_profile_and_alias_without_entry_is_denied(isolated, monkeypatch, home, alias):
    monkeypatch.setenv('HERMES_HOME', str(home))
    with pytest.raises(guard.MaintenanceIsolationError, match='persisted verified active'):
        policy.check(f'ssh spark-3 docker restart {alias}')
    # Normal inference choices are not rewritten or prohibited.
    guard.require_route('custom:hef', alias, 'http://llm-lab:8002/v1')


@pytest.mark.parametrize('text', [
    'ssh spark-1 sudo docker stop glm53-flash-tf',
    'ssh hef@192.168.100.11 "docker restart glm53-flash-tf"',
    'systemctl --user restart spark3-secondary.service',
    'docker compose -f spark-lanes/compose.yaml up -d --force-recreate',
    'sudo /usr/bin/docker rm -f glm53-flash-tf',
    "ssh spark-2 do'ck'er st'op' glm53-flash-tf",
    "subprocess.run(['ssh', 'spark-3', 'systemctl --user stop spark3-secondary.service'])",
    "client.containers.get('glm53-flash-tf').stop(); import docker",
    "ssh.exec_command('systemctl --user restart ollama-spark3.service')",
    'pkill -f vllm',
    'docker stop $(docker ps -q)',
    'systemctl restart docker.service',
    'bash glm53-tf-cutover-v1.7.1.sh',
    'python3 glm53-mia-ablit-cutover.py',
    'spark3-lane-switch secondary',
])
def test_recognized_execution_routes_fail_before_entry(isolated, text):
    assert policy.recognizable_mutation(text)
    with pytest.raises(guard.MaintenanceIsolationError):
        policy.check(text)


@pytest.mark.parametrize('text', [
    'ssh spark-1 docker inspect glm53-flash-tf',
    'ssh spark-2 docker ps --format "{{.Names}}"',
    'ssh spark-3 systemctl --user status spark3-secondary.service',
    'ssh spark-1 nvidia-smi',
    'ssh spark-2 git -C ~/nix pull --ff-only hef-run main',
    "subprocess.run(['ssh','spark-1','docker logs glm53-flash-tf'])",
    'curl -fsS http://spark-1:8000/health',
    'git status --short',
])
def test_read_only_ssh_and_normal_work_remain_available(isolated, text):
    assert not policy.recognizable_mutation(text)
    policy.check(text)


def receipt_fixture(isolated, monkeypatch):
    value = activate()
    proof = {'executor': {'pid': 123}, 'control': {'owner': 'a' * 32, 'expires': time.time() + 1000}}
    monkeypatch.setattr(a, 'observe', lambda model: proof)
    receipt = {'version': 1, 'verified_at': time.time(), 'state_sha256': a.digest(value),
               'proof': proof, 'completion': 'MAINTENANCE_OK'}
    path = isolated / 'serving.json'
    path.write_text(json.dumps(receipt)); path.chmod(0o600)
    return receipt, path


def test_valid_receipt_rechecks_live_proof(isolated, monkeypatch):
    receipt, path = receipt_fixture(isolated, monkeypatch)
    assert a.require_admission() == receipt
    monkeypatch.setattr(a, 'observe', lambda model: {'changed': True})
    with pytest.raises(guard.MaintenanceIsolationError, match='changed'):
        a.require_admission()


@pytest.mark.parametrize('change', ['absent', 'stale', 'future', 'state', 'profiles', 'mode', 'malformed', 'symlink'])
def test_bad_receipts_fail_closed(isolated, monkeypatch, change):
    receipt, path = receipt_fixture(isolated, monkeypatch)
    if change == 'absent': path.unlink()
    elif change == 'stale': receipt['verified_at'] -= 121; path.write_text(json.dumps(receipt))
    elif change == 'future': receipt['verified_at'] += 100; path.write_text(json.dumps(receipt))
    elif change == 'state': receipt['state_sha256'] = '0' * 64; path.write_text(json.dumps(receipt))
    elif change == 'profiles': monkeypatch.setattr(a, 'profile_homes', lambda: ['future'])
    elif change == 'mode': path.chmod(0o644)
    elif change == 'malformed': path.write_text('null')
    elif change == 'symlink':
        other = path.with_name('other'); path.rename(other); path.symlink_to(other)
    with pytest.raises(guard.MaintenanceIsolationError):
        a.require_admission()


def test_drained_flag_cannot_forge_observed_drain(isolated, monkeypatch):
    probe = Mock()
    monkeypatch.setattr(ctl, 'probe_profile', probe)
    with pytest.raises(guard.MaintenanceIsolationError, match='accounting'):
        ctl.enter(MODEL, drained=True)
    assert guard.state()['phase'] == 'verifying'
    probe.assert_not_called()


def test_certification_requires_actual_probe_and_preserves_failure(isolated, monkeypatch):
    activate()
    monkeypatch.setattr(a, 'observe', lambda model: {'profile_home': str(isolated.parent)})
    probe = Mock(side_effect=RuntimeError('fixture external failure'))
    monkeypatch.setattr(ctl, 'probe_profile', probe)
    with pytest.raises(RuntimeError): a.certify()
    assert not (isolated / 'serving.json').exists()
    assert guard.state()['phase'] == 'active'
    probe.assert_called_once()


@pytest.fixture
def observed(isolated, monkeypatch):
    root = isolated.parent / 'maintenance-admission'
    accounting.initialize(root)
    accounting.close(root, owner='a' * 32, executor_pid=os.getpid(), expires=time.time() + 600)
    marker = root / 'enrollment.json'
    marker.write_text(json.dumps({'version': 1, 'enrolled_at': 0}))
    identity = {**accounting.identity(), 'argv': ['python', '-m', 'hermes_cli.main', '--provider', 'openai-codex', '--model', MODEL], 'cgroup': 'fixture-external'}
    monkeypatch.setattr(a, 'process', lambda pid: identity)
    monkeypatch.setattr(a, 'runtime_inventory', lambda: [identity])
    monkeypatch.setattr(a, 'source_identity', lambda: ('fixture-source', 0))
    monkeypatch.setattr(a, 'profile_homes', lambda: [str((Path.home() / '.hermes').resolve())])
    return root, identity


def test_real_closed_zero_accounting_observation(observed):
    assert a.observe(MODEL)['control']['closed'] is True


@pytest.mark.parametrize('provider,model', [('custom:hef', 'primary'), ('custom:hef', 'secondary'), ('openai-codex', 'secondary')])
def test_real_executor_not_just_profile_alias(observed, provider, model):
    root, identity = observed
    identity['argv'][-3] = provider
    identity['argv'][-1] = model
    with pytest.raises(guard.MaintenanceIsolationError, match='independent'):
        a.observe(MODEL)


def test_gateway_cannot_be_external_executor(observed):
    observed[1]['cgroup'] = 'hermes-gateway.service'
    with pytest.raises(guard.MaintenanceIsolationError, match='independent'):
        a.observe(MODEL)


def test_old_runtime_is_not_attested(observed, monkeypatch):
    monkeypatch.setattr(a, 'source_identity', lambda: ('source', time.time()))
    with pytest.raises(guard.MaintenanceIsolationError, match='predates source'):
        a.observe(MODEL)


def test_pre_enrollment_runtime_is_not_attested(observed):
    (observed[0] / 'enrollment.json').write_text(json.dumps({'version': 1, 'enrolled_at': time.time()}))
    with pytest.raises(guard.MaintenanceIsolationError, match='predates accounting'):
        a.observe(MODEL)


def test_outstanding_nonexecutor_debt_is_not_drain(observed):
    root, identity = observed
    with accounting.transaction(root) as c:
        c.execute('INSERT INTO work VALUES (?, ?, ?)', ('fixture', json.dumps({'pid': -1}), 'unreconciled'))
    with pytest.raises(guard.MaintenanceIsolationError, match='accounting'):
        a.observe(MODEL)


def test_terminal_hook_refuses_before_backend_and_approval(isolated, monkeypatch):
    from tools import terminal_tool as terminal
    lifecycle = Mock(side_effect=AssertionError('must not reach another guard'))
    monkeypatch.setattr(terminal, 'gateway_lifecycle_block', lifecycle)
    with pytest.raises(terminal._Rejected):
        terminal._pre_exec_block('ssh spark-1 docker stop glm53-flash-tf', env=None,
            env_type='ssh', cwd='/', workdir=None, session_key='fixture')
    lifecycle.assert_not_called()
