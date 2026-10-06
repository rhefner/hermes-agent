"""Synthetic diagnostic tests; never call a real provider or production gate."""
import json
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from agent import attestation_diagnostics as d
from agent import executor_attestation as att
from agent import serving_admission as a
from agent import maintenance_inference as guard
from hermes_cli import maintenance_inference as ctl
from tests.agent.test_executor_attestation import runtime, MODEL

SECRET = 'SENTINEL-secret-owner-challenge-payload'


def reports(caplog):
    records = [r for r in caplog.records if r.name == d.__name__]
    assert all(r.exc_info is None and r.stack_info is None for r in records)
    assert SECRET not in ''.join(r.getMessage() for r in records)
    return [json.loads(r.getMessage()) for r in records]


@pytest.mark.parametrize('stage', sorted(d._STAGES))
def test_fixed_stage_and_original_exception(stage, caplog):
    error = ValueError(SECRET)
    with pytest.raises(ValueError) as caught:
        with d.capture('renew'):
            d.stage(stage)
            raise error
    assert caught.value is error
    record, = reports(caplog)
    assert record['stage'] == stage and record['action'] == 'renew'
    assert record['exception'] == 'ValueError' and record['elapsed_ms'] >= 0
    assert d._current.get() is None


def test_unknown_values_and_exception_name_never_leak(caplog):
    error = type(SECRET, (Exception,), {})(SECRET)
    with pytest.raises(Exception):
        with d.capture(SECRET):
            d.stage(SECRET)
            raise error
    r, = reports(caplog)
    assert r['stage'] == r['action'] == 'unknown'
    assert r['exception'] == 'other'


def test_nested_single_record_preserves_operation(caplog):
    with pytest.raises(RuntimeError):
        with d.capture('renew'):
            with d.capture('probe'):
                d.stage('completion_check')
                d.completion(True, False)
                raise RuntimeError(SECRET)
    r, = reports(caplog)
    assert r['action'] == 'renew' and r['content_present'] is True
    assert r['exact_match'] is False


def test_broken_logger_does_not_change_error(monkeypatch):
    monkeypatch.setattr(d._log, 'warning', Mock(side_effect=RuntimeError(SECRET)))
    original = ValueError('original')
    with pytest.raises(ValueError) as caught:
        with d.capture('certify'):
            raise original
    assert caught.value is original and d._current.get() is None


@pytest.mark.parametrize('fault,stage', [
    ('active', 'active_state'), ('observe', 'observe'),
    ('route', 'runtime_match'), ('prior', 'prior_evidence'),
    ('probe', 'probe'), ('post', 'post_probe'), ('fresh', 'fresh_evidence'),
])
def test_real_socket_failure_diagnostics(runtime, monkeypatch, caplog, fault, stage):
    proof = a.observe(MODEL)
    digest = a.digest(a.active_state())
    action = 'certify'
    if fault == 'active':
        monkeypatch.setattr(a, 'active_state', Mock(side_effect=RuntimeError(SECRET)))
    elif fault == 'observe':
        monkeypatch.setattr(a, 'observe', Mock(side_effect=RuntimeError(SECRET)))
    elif fault == 'route': runtime.agent.provider = SECRET
    elif fault == 'prior': action = 'renew'
    elif fault == 'probe': monkeypatch.setattr(ctl, 'probe', Mock(side_effect=RuntimeError(SECRET)))
    elif fault == 'post':
        monkeypatch.setattr(ctl, 'probe', lambda *args, **kw: setattr(runtime.agent, 'provider', SECRET))
    elif fault == 'fresh': action = 'check'
    with pytest.raises(guard.MaintenanceIsolationError, match='runtime did not attest'):
        att.request(proof, digest, action)
    record, = reports(caplog)
    assert record['stage'] == stage and record['action'] == action
    assert runtime.server.evidence is None


@pytest.mark.parametrize('fault,stage', [
    ('runtime_resolve','runtime_resolve'), ('runtime_check','runtime_check'),
    ('client_resolve','client_resolve'), ('client_check','client_check'),
    ('call','completion_call'), ('mismatch','completion_check'),
    ('empty','completion_check'), ('malformed','completion_check'),
])
def test_probe_diagnostics_no_response_leak(monkeypatch, caplog, fault, stage):
    from agent import auxiliary_client
    from hermes_cli import runtime_provider
    for key in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(guard, 'require_route', lambda *a, **kw: None)
    runtime_value = {'provider':'openai-codex','base_url':guard.CLOUD_BASE,'api_mode':'codex_responses'}
    resolve = Mock(return_value=runtime_value)
    create = Mock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=SECRET))]))
    client = SimpleNamespace(base_url=guard.CLOUD_BASE,chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    resolve_client = Mock(return_value=(client, MODEL))
    monkeypatch.setattr(runtime_provider, 'resolve_runtime_provider', resolve)
    monkeypatch.setattr(auxiliary_client, 'resolve_provider_client', resolve_client)
    if fault == 'runtime_resolve': resolve.side_effect = RuntimeError(SECRET)
    if fault == 'runtime_check': runtime_value['provider'] = SECRET
    if fault == 'client_resolve': resolve_client.side_effect = RuntimeError(SECRET)
    if fault == 'client_check': resolve_client.return_value = (None, MODEL)
    if fault == 'call': create.side_effect = TimeoutError(SECRET)
    if fault == 'empty': create.return_value.choices[0].message.content = None
    if fault == 'malformed': create.return_value = None
    with pytest.raises(Exception): ctl.probe(MODEL, challenge='a'*64)
    r, = reports(caplog)
    assert r['stage'] == stage
    assert 'a'*64 not in caplog.text
    if fault in ('empty','mismatch'):
        assert r['exact_match'] is False
        assert r['content_present'] is (fault == 'mismatch')


def test_imports_are_from_candidate_checkout():
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    for module in (d, att, ctl):
        assert Path(module.__file__).resolve().is_relative_to(root)


@pytest.mark.parametrize('fault', ['frame', 'action'])
def test_wire_failure_remains_opaque(runtime, caplog, fault):
    import socket
    proof = a.observe(MODEL)
    request = {'nonce': 'b'*64, 'action': 'certify', 'proof': proof,
               'state_sha256': a.digest(a.active_state()), 'previous': None}
    request['nonce' if fault == 'frame' else 'action'] = SECRET
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(5)
        conn.connect(att.address(runtime.ident))
        conn.sendall(json.dumps(request).encode() + b'\n')
        response = att.receive(conn)
    assert response == {'ok': False}
    r, = reports(caplog)
    assert r['stage'] == fault
    assert runtime.server.evidence is None


def test_context_is_thread_local(caplog):
    import threading
    barrier = threading.Barrier(2)
    def fail(operation, stage):
        try:
            with d.capture(operation):
                d.stage(stage)
                barrier.wait(timeout=5)
                raise ValueError(SECRET)
        except ValueError:
            pass
    threads = [threading.Thread(target=fail, args=args) for args in
               [('renew','observe'), ('certify','completion_call')]]
    for thread in threads: thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    assert {(r['action'],r['stage']) for r in reports(caplog)} == {
        ('renew','observe'), ('certify','completion_call')}
    assert d._current.get() is None


@pytest.mark.parametrize('name', ['AuthenticationError','RateLimitError','APITimeoutError'])
def test_sdk_exception_categories_without_payload(name, caplog):
    error = type(name, (Exception,), {'__module__':'openai'})(SECRET)
    with pytest.raises(Exception):
        with d.capture('renew'):
            d.stage('completion_call')
            raise error
    r, = reports(caplog)
    assert r['exception'] == name


def test_logging_failure_still_invalidates_authority(runtime, monkeypatch):
    a.certify()
    runtime.clock[0] += 121
    monkeypatch.setattr(d._log, 'warning', Mock(side_effect=RuntimeError(SECRET)))
    monkeypatch.setattr(ctl, 'probe', Mock(side_effect=RuntimeError(SECRET)))
    with pytest.raises(guard.MaintenanceIsolationError):
        a.renew()
    assert runtime.server.evidence is None
    assert json.loads((runtime.root / 'serving.json').read_text())['invalidated'] is True


def test_success_silent_and_renewal_behavior_unchanged(runtime, caplog):
    first = a.certify()
    runtime.clock[0] += 121
    renewed = a.renew()
    assert renewed['proof'] == first['proof']
    assert renewed['verified_at'] - first['verified_at'] == 121
    assert len(runtime.calls) == 2
    assert a.require_admission() == renewed
    assert reports(caplog) == []
