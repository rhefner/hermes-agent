import json
import time

import pytest
from agent import maintenance_admission as gate
from agent import executor_recovery as recovery

OLD = {'pid': 100, 'start': '1000', 'boot': 'test'}
NEW = {'pid': 200, 'start': '2000', 'boot': 'test'}
OWNER = 'a' * 32

@pytest.fixture
def store(tmp_path, monkeypatch):
    root = tmp_path / 'accounting'
    gate.initialize(root)
    control = dict(version=1, closed=True, owner=OWNER, executor=OLD,
                   expires=time.time()-10, closed_at=1)
    with gate.transaction(root) as c:
        c.execute('UPDATE control SET body=?', (json.dumps(control),))
        c.execute('INSERT INTO work VALUES (?, ?, ?)', ('debt', json.dumps(OLD), 'conversation'))
    monkeypatch.setattr(recovery, 'replacement', lambda pid: NEW.copy())
    monkeypatch.setattr(recovery, 'still_live', lambda ident: False)
    return root

def run(root, **kw):
    args = dict(owner=OWNER, executor_pid=200, seconds=3600, acknowledgement='Recover lost executor')
    args.update(kw)
    return recovery.recover(root, **args)

def test_recovery_keeps_gate_closed_and_debt_unchanged_with_atomic_audit(store):
    result = run(store)
    status = gate.status(store)
    assert status['control']['closed'] is True
    assert status['control']['executor'] == NEW
    assert status['control']['closed_at'] == 1
    assert status['outstanding'][0]['identity'] == OLD
    assert result['serving_authorized'] is False
    with gate.transaction(store) as c:
        audit = json.loads(c.execute('SELECT body FROM recovery_audit').fetchone()[0])
    assert audit['previous']['executor'] == OLD
    assert audit['replacement'] == NEW

@pytest.mark.parametrize('kw', [dict(owner='b'*32), dict(seconds=0), dict(seconds=14401), dict(seconds=True), dict(acknowledgement=' ')])
def test_invalid_recovery_leaves_control_and_work_unchanged(store, kw):
    before = gate.status(store)
    with pytest.raises(gate.AdmissionClosed):
        run(store, **kw)
    assert gate.status(store) == before

@pytest.mark.parametrize('change', [{'closed': False}, {'expires': time.time()+3600}])
def test_refuses_open_or_unexpired_control(store, change):
    with gate.transaction(store) as c:
        value = gate._control(c); value.update(change)
        c.execute('UPDATE control SET body=?', (json.dumps(value),))
    before = gate.status(store)
    with pytest.raises(gate.AdmissionClosed): run(store)
    assert gate.status(store) == before

def test_live_old_executor_refused_even_after_expiry(store, monkeypatch):
    monkeypatch.setattr(recovery, 'still_live', lambda ident: True)
    with pytest.raises(gate.AdmissionClosed, match='old executor'): run(store)

def test_ambiguous_inventory_refuses_without_mutation(store, monkeypatch):
    before = gate.status(store)
    def ambiguous(_): raise PermissionError('unreadable')
    monkeypatch.setattr(recovery, 'still_live', ambiguous)
    with pytest.raises(PermissionError): run(store)
    assert gate.status(store) == before


def test_live_foreign_reservation_refuses(store, monkeypatch):
    monkeypatch.setattr(recovery, 'still_live', lambda ident: ident != OLD)
    with gate.transaction(store) as c:
        c.execute('INSERT INTO work VALUES (?, ?, ?)', ('foreign', json.dumps(NEW), 'background-review'))
    before = gate.status(store)
    with pytest.raises(gate.AdmissionClosed, match='Live outstanding'): run(store)
    assert gate.status(store) == before


def test_replacement_race_rolls_back(store, monkeypatch):
    values = iter([NEW, dict(NEW, start='9999')])
    monkeypatch.setattr(recovery, 'replacement', lambda pid: next(values))
    before = gate.status(store)
    with pytest.raises(gate.AdmissionClosed, match='identity changed'): run(store)
    assert gate.status(store) == before


@pytest.mark.parametrize('provider,model,kind,cgroup,args', [
    ('auto', 'gpt-6-astra', 'cli', '', []),
    ('openai-codex', 'primary', 'cli', '', []),
    ('openai-codex', 'gpt-6-astra', 'gateway', '', []),
    ('openai-codex', 'gpt-6-astra', 'cli', 'hermes-gateway.service', []),
    ('openai-codex', 'gpt-6-astra', 'cli', '', ['--query=hello']),
])
def test_replacement_rejects_wrong_runtime(monkeypatch, provider, model, kind, cgroup, args):
    from agent import serving_admission as serving
    from agent import runtime_argv as runtime
    argv = ['hermes', '--provider', provider, '--model', model] + args
    monkeypatch.setattr(serving, 'process', lambda pid: dict(NEW, argv=argv, cgroup=cgroup))
    monkeypatch.setattr(runtime, 'runtime_kind', lambda argv: kind)
    monkeypatch.setattr(runtime, 'runtime_argv', lambda argv: argv)
    monkeypatch.setattr(serving, 'profile_homes', lambda: [str(recovery.Path.home()/'.hermes')])
    with pytest.raises(gate.AdmissionClosed): recovery.replacement(200)
