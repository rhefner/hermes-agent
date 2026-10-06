"""Offline exact-PID transport and >120s renewal tests; no external inference.

Real Unix sockets/SO_PEERCRED, real private SQLite, fake authenticated SDK edge.
Runtime observation is fixture-only; this is not production certification.
"""
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from agent import executor_attestation as att
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from agent import serving_admission as a
from hermes_cli import maintenance_inference as ctl

MODEL = 'external-fixture'


class Agent:
    provider = 'openai-codex'
    model = MODEL
    api_mode = 'codex_responses'
    base_url = guard.CLOUD_BASE


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    root = tmp_path / 'maintenance-inference'
    monkeypatch.setattr(guard, 'directory', lambda: root)
    homes = [str((Path.home() / '.hermes').resolve())]
    import hermes_constants
    monkeypatch.setattr(hermes_constants, 'get_hermes_home', lambda: Path(homes[0]))
    monkeypatch.setattr(a, 'profile_homes', lambda: homes)
    control = tmp_path / 'maintenance-admission'
    accounting.initialize(control)
    accounting.close(control, owner='a' * 32, executor_pid=os.getpid(), expires=time.time() + 1000)
    marker = control / 'enrollment.json'
    marker.write_text(json.dumps({'version': 1, 'enrolled_at': 0})); marker.chmod(0o600)
    ident = {**accounting.identity(), 'argv': ['python', '-m', 'hermes_cli.main',
             '--provider', 'openai-codex', '--model', MODEL], 'cgroup': 'fixture-cli'}
    monkeypatch.setattr(a, 'process', lambda pid: ident)
    monkeypatch.setattr(a, 'runtime_inventory', lambda: [ident])
    monkeypatch.setattr(a, 'source_identity', lambda: ('fixture-source', 0))
    ctl.write_state({'version': 1, 'phase': 'active', 'provider': 'openai-codex',
                     'model': MODEL, 'base_url': guard.CLOUD_BASE, 'verified_profiles': homes})
    clock = [time.time()]
    monkeypatch.setattr(att.time, 'time', lambda: clock[0])
    calls = []
    def probe(model, *, challenge=None):
        assert model == MODEL and len(challenge) == 64
        calls.append((os.getpid(), challenge))
    monkeypatch.setattr(ctl, 'probe', probe)
    agent = Agent()
    server = att.RuntimeAttestor(agent)
    yield SimpleNamespace(root=root, control=control, server=server, agent=agent,
                          clock=clock, calls=calls, ident=ident, homes=homes)
    server.sock.shutdown(socket.SHUT_RDWR)
    server.sock.close()
    server.thread.join(timeout=2)
    assert not server.thread.is_alive()


def test_exact_pid_runtime_completion_required_not_cli_or_json(runtime):
    proof = a.observe(MODEL)
    with pytest.raises(guard.MaintenanceIsolationError):
        att.request(proof, a.digest(a.active_state()), 'check')
    receipt = a.certify()
    assert runtime.calls[0][0] == proof['executor']['pid'] == os.getpid()
    assert receipt['attestation']['completion_nonce'] == runtime.calls[0][1]
    assert a.require_admission() == receipt
    assert len(runtime.calls) == 1


def test_renew_beyond_120_is_new_completion_not_extended_expiry(runtime):
    first = a.certify()
    runtime.clock[0] += 121
    with pytest.raises(guard.MaintenanceIsolationError, match='stale'):
        a.require_admission()
    second = a.renew()
    assert len(runtime.calls) == 2
    assert second['verified_at'] - first['verified_at'] == 121
    assert first['attestation']['completion_nonce'] != second['attestation']['completion_nonce']
    assert first['proof'] == second['proof']  # Accounting deadline never extended.
    assert a.require_admission() == second
    runtime.clock[0] += 121
    assert a.renew()['verified_at'] == runtime.clock[0]
    assert len(runtime.calls) == 3


def test_renew_within_window_keeps_actual_completion_time(runtime):
    first = a.certify()
    runtime.clock[0] += 20
    assert a.renew() == first
    assert len(runtime.calls) == 1


@pytest.mark.parametrize('fault', ['source', 'inventory', 'profiles', 'accounting', 'outstanding',
                                    'executor', 'isolation', 'auth', 'live-route', 'deadline'])
def test_beyond_120_invalidation_is_fail_closed(runtime, monkeypatch, fault):
    a.certify()
    runtime.clock[0] += 121
    if fault == 'source': monkeypatch.setattr(a, 'source_identity', lambda: ('changed', 0))
    elif fault == 'inventory': monkeypatch.setattr(a, 'runtime_inventory', lambda: [])
    elif fault == 'profiles': runtime.homes.append('/fixture/new-profile')
    elif fault == 'accounting':
        with accounting.transaction(runtime.control) as c:
            c.execute("UPDATE control SET body=? WHERE id=1", (json.dumps({'version': 1, 'closed': False}),))
    elif fault == 'outstanding':
        with accounting.transaction(runtime.control) as c:
            c.execute('INSERT INTO work VALUES (?, ?, ?)', ('fixture', json.dumps({'pid': -1}), 'unreconciled'))
    elif fault == 'executor': runtime.ident['argv'][-1] = 'other-model'
    elif fault == 'isolation':
        value = guard.state(); value['phase'] = 'verifying'; ctl.write_state(value)
    elif fault == 'auth': monkeypatch.setattr(ctl, 'probe', Mock(side_effect=RuntimeError('fake SDK failure')))
    elif fault == 'live-route': runtime.agent.provider = 'custom:hef'
    elif fault == 'deadline': runtime.clock[0] += 1001
    with pytest.raises((guard.MaintenanceIsolationError, accounting.AdmissionClosed)):
        a.renew()
    assert json.loads((runtime.root / 'serving.json').read_text())['invalidated'] is True
    with pytest.raises(guard.MaintenanceIsolationError): a.require_admission()


@pytest.mark.parametrize('fault', ['nonce', 'time', 'pid', 'version'])
def test_forged_persisted_receipt_rejected(runtime, fault):
    receipt = a.certify()
    if fault == 'nonce': receipt['attestation']['completion_nonce'] = 'f' * 64
    elif fault == 'time':
        runtime.clock[0] += 10
        receipt['verified_at'] += 10; receipt['attestation']['completed_at'] += 10
    elif fault == 'pid': receipt['attestation']['executor']['pid'] += 1
    elif fault == 'version': receipt['version'] = 1
    a.save_receipt(receipt)
    with pytest.raises(guard.MaintenanceIsolationError): a.require_admission()
    with pytest.raises(guard.MaintenanceIsolationError): a.renew()


def test_forged_initial_renewal_cannot_mint_authority(runtime):
    evidence = {'completed_at': time.time(), 'completion_nonce': 'f' * 64}
    a.save_receipt({'version': 2, 'verified_at': evidence['completed_at'],
                    'state_sha256': a.digest(a.active_state()), 'proof': a.observe(MODEL),
                    'attestation': evidence})
    with pytest.raises(guard.MaintenanceIsolationError): a.renew()
    assert runtime.calls == []


def test_other_pid_socket_cannot_impersonate_executor(monkeypatch, tmp_path):
    # Child deliberately binds an address claiming the parent's PID. Kernel peer
    # identity, not address/JSON/argv, must expose this impersonation.
    ident = accounting.identity()
    script = tmp_path / 'impostor.py'
    script.write_text('import socket,sys,time\ns=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)\n'
                      's.bind(bytes.fromhex(sys.argv[1]));s.listen(1)\nprint("READY",flush=True)\n'
                      'c,_=s.accept()\nc.recv(8192)\n')
    p = subprocess.Popen([sys.executable, str(script), att.address(ident).encode().hex()],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        import selectors
        with selectors.DefaultSelector() as sel:
            sel.register(p.stdout, selectors.EVENT_READ)
            assert sel.select(5)
        assert p.stdout.readline().strip() == 'READY'
        with pytest.raises(guard.MaintenanceIsolationError, match='exact executor PID'):
            att.request({'executor': ident}, 'fixture', 'certify')
    finally:
        p.terminate(); p.communicate(timeout=5)


def test_stale_nonce_and_evidence_response_rejected(runtime, monkeypatch):
    a.certify()
    proof = a.observe(MODEL)
    real = att.receive
    def replay(conn):
        result = real(conn)
        if 'ok' in result: result['nonce'] = '0' * 64
        return result
    monkeypatch.setattr(att, 'receive', replay)
    with pytest.raises(guard.MaintenanceIsolationError):
        att.request(proof, a.digest(a.active_state()), 'check')


def test_old_runtime_evidence_cannot_be_used_after_server_loses_it(runtime):
    a.certify()
    runtime.server.evidence = None
    with pytest.raises(guard.MaintenanceIsolationError): a.require_admission()
    runtime.clock[0] += 121
    with pytest.raises(guard.MaintenanceIsolationError): a.renew()


def test_completion_runtime_route_is_rechecked_after_probe(runtime, monkeypatch):
    def probe(*args, **kwargs): runtime.ident['argv'][-1] = 'changed'
    monkeypatch.setattr(ctl, 'probe', probe)
    with pytest.raises(guard.MaintenanceIsolationError): a.certify()


def test_authenticated_probe_requires_nonce_exactly(monkeypatch):
    from agent import auxiliary_client
    from hermes_cli import runtime_provider
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(runtime_provider, 'resolve_runtime_provider', lambda **kw:
        {'provider': 'openai-codex', 'base_url': guard.CLOUD_BASE, 'api_mode': 'codex_responses'})
    create = Mock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='MAINTENANCE_OK_' + 'a' * 64))]))
    client = SimpleNamespace(base_url=guard.CLOUD_BASE, chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    monkeypatch.setattr(auxiliary_client, 'resolve_provider_client', lambda *args, **kw: (client, MODEL))
    ctl.probe(MODEL, challenge='a' * 64)
    assert create.call_args.kwargs['timeout'] == 25
    with pytest.raises(guard.MaintenanceIsolationError): ctl.probe(MODEL, challenge='b' * 64)


def test_changed_live_agent_route_during_completion_rejected(runtime, monkeypatch):
    def probe(*args, **kwargs): runtime.agent.provider = 'custom:hef'
    monkeypatch.setattr(ctl, 'probe', probe)
    with pytest.raises(guard.MaintenanceIsolationError): a.certify()


@pytest.mark.parametrize('field', ['start', 'boot'])
def test_recycled_pid_or_boot_identity_rejected(runtime, field):
    proof = copy.deepcopy(a.observe(MODEL))
    proof['executor'][field] = 'not-current'
    with pytest.raises(guard.MaintenanceIsolationError):
        att.request(proof, a.digest(a.active_state()), 'certify')


def test_executor_socket_disappeared_is_fail_closed(runtime, monkeypatch):
    a.certify()
    monkeypatch.setattr(att, 'address', lambda identity: '\0nonexistent-' + str(os.getpid()))
    runtime.clock[0] += 121
    with pytest.raises(guard.MaintenanceIsolationError): a.renew()
    assert json.loads((runtime.root / 'serving.json').read_text())['invalidated'] is True


def test_slow_authenticated_completion_is_not_certification(runtime, monkeypatch):
    monkeypatch.setattr(ctl, 'probe', lambda *args, **kwargs: runtime.clock.__setitem__(0, runtime.clock[0] + 31))
    with pytest.raises(guard.MaintenanceIsolationError): a.certify()
    assert runtime.server.evidence is None


def test_profile_env_disagrees_with_executor_argv(runtime, monkeypatch):
    import hermes_constants
    monkeypatch.setattr(hermes_constants, 'get_hermes_home', lambda: Path('/wrong-profile'))
    with pytest.raises(guard.MaintenanceIsolationError): a.certify()
    assert runtime.calls == []


def test_non_codex_cli_never_registers_or_probes(monkeypatch):
    create = Mock(side_effect=AssertionError('must not bind'))
    monkeypatch.setattr(att, 'RuntimeAttestor', create)
    att.register(SimpleNamespace(provider='custom:hef', api_mode='chat_completions'))
    att.register(SimpleNamespace())
    create.assert_not_called()


def test_root_cli_registration_uses_initialized_runtime(runtime, monkeypatch):
    # Test the real registration seam separately from transport fixtures.
    create = Mock(return_value=runtime.server)
    monkeypatch.setattr(att, 'RuntimeAttestor', create)
    monkeypatch.setattr(att, '_server', None)
    att.register(runtime.agent)
    create.assert_called_once_with(runtime.agent)
    assert runtime.calls == []  # Startup never invokes inference.
    a.certify()
    replacement = Agent()
    att.register(replacement)
    assert runtime.server.agent() is replacement and runtime.server.evidence is None
    with pytest.raises(guard.MaintenanceIsolationError): a.renew()
