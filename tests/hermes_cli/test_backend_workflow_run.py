"""Real guard/journal transitions with deterministic serving transport doubles."""
import copy
import importlib
import time

import pytest

from agent import backend_isolation as backend
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from hermes_cli import backend_isolation as controller
from hermes_cli import backend_workflow_store as store
from hermes_cli import maintenance_inference as legacy

pytestmark = pytest.mark.platforms('linux')


class Host:
    def __init__(self, mode='ok'):
        self.mode = mode
        self.identity = {'source': 'a' * 40, 'containers': ['before-0', 'before-1']}
        self.changed = False
        self.expires = time.time() + 10000
        self.calls = []

    def inspect(self):
        return {'identity': copy.deepcopy(self.identity), 'credentials_expires_at': self.expires,
                'already_applied': self.changed, 'healthy': True}

    def preflight(self):
        self.calls.append('preflight')
        return self.inspect()

    def apply(self, operation, record):
        assert backend.require_mutation(['spark-pair'])['operation'] == operation
        self.calls.append('apply')
        record('replace-ranks', 'intent', {'before': self.identity})
        if self.mode == 'crash':
            raise SystemExit(17)
        self.changed = True
        self.identity['containers'] = ['after-0', 'after-1']
        record('replace-ranks', 'done', {'after': self.identity})
        if self.mode == 'expired-after-apply':
            self.expires = time.time() - 1
        return self.inspect()

    def qa(self, operation, record):
        assert backend.require_mutation(['spark-pair'])['operation'] == operation
        self.calls.append('qa')
        checks = {name: True for name in store.QA_CHECKS}
        if self.mode == 'failed-qa':
            checks['facade'] = False
        if self.mode == 'missing-qa':
            checks.pop('capacity')
        result = {'checks': checks, 'identity': copy.deepcopy(self.identity), 'evidence': {'fixture': True}}
        if self.mode == 'drift':
            self.identity['containers'][0] = 'unexpected-restart'
        return result

    def normal_routes(self):
        self.calls.append('normal_routes')
        assert backend.configuration()['phase'] == 'normal'
        return {'primary': self.mode != 'failed-post-release', 'secondary': True}


@pytest.fixture
def operation(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, 'directory', lambda: tmp_path / 'fleet')
    monkeypatch.setattr(accounting, 'directory', lambda: tmp_path / 'accounting')
    monkeypatch.setattr(legacy, 'probe', lambda model: None)
    legacy.write_state({'version': 1, 'phase': 'normal'})
    controller.migrate({'spark-pair': ['http://fixture-pair:8000'],
                        'spark-3': ['http://fixture-secondary:8000']}, approval='fixture enrollment')
    plan = {'adapter': 'spark-pair-eight-stream', 'backends': ['spark-pair'],
            'controller_revision': 'a' * 40,
            'delta': {'PARALLEL': '8'}, 'preserve': {'CONTEXT': '1048576', 'MODEL_ID': 'fixture/model'},
            'qa': sorted(store.QA_CHECKS), 'budget_seconds': 60, 'margin_seconds': 30, 'tasks': []}
    return store.create(plan)


@pytest.fixture
def engine():
    return importlib.import_module('hermes_cli.backend_workflow_run')


def approve(op, policy='conditional'):
    store.approve(op['id'], op['plan_digest'], reference='user-message', text='fixture approval',
                  release_policy=policy)


@pytest.mark.parametrize('action', ['preflight', 'run'])
def test_slow_independent_probe_rechecks_credential_window_before_isolation(operation, engine, monkeypatch, action):
    now = time.time()
    host = Host()
    host.expires = now + operation['plan']['budget_seconds'] + operation['plan']['margin_seconds'] + 1
    approve(operation)

    def slow_probe(model):
        monkeypatch.setattr(engine.time, 'time', lambda: now + 2)

    monkeypatch.setattr(legacy, 'probe', slow_probe)
    before = (guard.directory() / 'state.json').read_bytes()
    with pytest.raises(guard.MaintenanceIsolationError, match='Credential'):
        getattr(engine, action)(operation['id'], model='fixture-independent', adapter=host)
    assert (guard.directory() / 'state.json').read_bytes() == before
    assert host.calls == ['preflight']


def test_conditional_workflow_and_completed_rerun_do_not_repeat_mutation(operation, engine):
    host = Host()
    before = (guard.directory() / 'state.json').read_bytes()
    prepared = engine.preflight(operation['id'], model='fixture-independent', adapter=host)
    assert prepared['stage'] == 'preflight_ready'
    assert (guard.directory() / 'state.json').read_bytes() == before
    approve(operation)
    result = engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert result['stage'] == 'complete'
    assert backend.configuration()['phase'] == 'normal'
    assert backend.configuration()['affected'] == []
    assert result['execution']['qa']['checks'] == {x: True for x in store.QA_CHECKS}
    assert result['execution']['release']['plan_digest'] == operation['plan_digest']
    assert result['execution']['post_release'] == {'primary': True, 'secondary': True}
    assert host.calls.count('apply') == 1
    engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert host.calls.count('apply') == 1


def test_separate_release_requires_explicit_approval_and_fresh_qa(operation, engine):
    host = Host()
    engine.preflight(operation['id'], model='fixture-independent', adapter=host)
    approve(operation, 'separate')
    value = engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert value['stage'] == 'awaiting_release'
    assert backend.configuration()['affected'] == ['spark-pair']
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.release(operation['id'], model='fixture-independent', adapter=host)
    value = engine.release(operation['id'], model='fixture-independent', adapter=host,
                           reference='post-QA-message', approval='release after QA')
    assert value['stage'] == 'complete'
    assert host.calls.count('apply') == 1
    assert host.calls.count('qa') == 2


@pytest.mark.parametrize('mode', ['failed-qa', 'missing-qa', 'drift', 'expired-after-apply'])
def test_bad_qa_or_drift_never_releases(operation, engine, mode):
    host = Host(mode)
    engine.preflight(operation['id'], model='fixture-independent', adapter=host)
    approve(operation)
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert backend.configuration()['affected'] == ['spark-pair']
    assert store.load(operation['id'])['stage'] != 'complete'
    assert 'normal_routes' not in host.calls


def test_expired_credentials_or_missing_approval_block_before_isolation(operation, engine):
    host = Host()
    before = (guard.directory() / 'state.json').read_bytes()
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert host.calls == []
    host.expires = time.time() + 5
    approve(operation)
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert (guard.directory() / 'state.json').read_bytes() == before
    assert 'apply' not in host.calls


def test_uncertain_apply_never_replays_and_explicit_resume_recovers_proven_result(operation, engine):
    host = Host('crash')
    engine.preflight(operation['id'], model='fixture-independent', adapter=host)
    approve(operation)
    with pytest.raises(SystemExit):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    interrupted = store.load(operation['id'])
    assert interrupted['execution']['pending'] is not None
    assert backend.configuration()['affected'] == ['spark-pair']
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(operation['id'], model='fixture-independent', adapter=host, resume=True)
    assert host.calls.count('apply') == 1
    # Simulate an observed completed controller transaction whose response was lost.
    host.mode = 'ok'
    host.changed = True
    host.identity['containers'] = ['after-0', 'after-1']
    result = engine.run(operation['id'], model='fixture-independent', adapter=host, resume=True)
    assert result['stage'] == 'complete'
    assert host.calls.count('apply') == 1


def test_post_release_failure_is_degraded_not_rollback_or_reisolation(operation, engine):
    host = Host('failed-post-release')
    engine.preflight(operation['id'], model='fixture-independent', adapter=host)
    approve(operation)
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    result = store.load(operation['id'])
    assert result['stage'] == 'released'
    assert result['execution']['error'] is not None
    assert backend.configuration()['phase'] == 'normal'
    assert host.calls.count('apply') == 1


def test_cli_run_and_status_distinguish_lock_activity_from_completion(operation, engine, monkeypatch, capsys):
    from hermes_cli import backend_workflow as cli
    host = Host()
    monkeypatch.setattr(engine, 'adapter_for', lambda plan: host)
    with store.operation_lock(operation['id']):
        assert cli.status(operation['id'])['activity'] == 'running'
    assert cli.status(operation['id'])['runner_lock_held'] is False
    assert cli.main(['preflight', operation['id'], '--model', 'fixture-independent']) == 0
    assert cli.main(['approve', operation['id'], '--digest', operation['plan_digest'],
                     '--reference', 'fixture-message', '--approval', 'fixture approval',
                     '--release-policy', 'conditional']) == 0
    assert cli.main(['run', operation['id'], '--model', 'fixture-independent']) == 0
    assert cli.status(operation['id'])['activity'] == 'complete'
    assert host.calls.count('apply') == 1
    capsys.readouterr()


@pytest.mark.parametrize('include_child', [False, True])
def test_task_projection_is_atomic_idempotent_and_never_readies_old_jobs(operation, engine, monkeypatch, include_child):
    import sqlite3
    from hermes_cli import kanban_db as kb
    from hermes_cli import backend_workflow as cli
    db = guard.directory().parent / 'kanban.db'
    conn = sqlite3.connect(db)
    conn.executescript(kb.SCHEMA_SQL)
    conn.executemany('INSERT INTO tasks(id,title,status,created_at) VALUES (?,?,?,0)',
                     [('t_11111111', 'deployment', 'ready'), ('t_22222222', 'old deployment child', 'blocked')])
    conn.execute('INSERT INTO task_links(parent_id,child_id) VALUES (?,?)', ('t_11111111', 't_22222222'))
    conn.commit()
    monkeypatch.setattr(kb, 'recompute_ready', lambda *a, **kw: pytest.fail('projection scheduled work'))
    monkeypatch.setattr(kb, '_cleanup_workspace', lambda *a, **kw: pytest.fail('projection removed workspace'))
    monkeypatch.setattr(kb, '_fire_task_hook', lambda *a, **kw: pytest.fail('projection fired hook'))
    plan = copy.deepcopy(operation['plan'])
    plan['tasks'] = ['t_11111111', 't_22222222'] if include_child else ['t_11111111']
    value = store.create(plan)
    approve(value)
    host = Host()
    try:
        if not include_child:
            with pytest.raises(guard.MaintenanceIsolationError, match='reconciliation'):
                engine.run(value['id'], model='fixture-independent', adapter=host)
            assert dict(conn.execute('SELECT id,status FROM tasks')) == {
                't_11111111': 'ready', 't_22222222': 'blocked'}
            assert cli.status(value['id'])['activity'] == 'bookkeeping_pending'
        else:
            engine.run(value['id'], model='fixture-independent', adapter=host)
            engine.reconcile(value['id'])
            assert dict(conn.execute('SELECT id,status FROM tasks')) == {
                't_11111111': 'done', 't_22222222': 'done'}
            assert conn.execute("SELECT count(*) FROM task_events WHERE kind='maintenance_reconciled'").fetchone()[0] == 2
            assert conn.execute("SELECT count(*) FROM task_events WHERE kind='completed'").fetchone()[0] == 2
            assert conn.execute('SELECT count(*) FROM task_runs WHERE started_at=ended_at').fetchone()[0] == 2
        assert backend.configuration()['phase'] == 'normal'
        assert host.calls.count('apply') == 1
    finally:
        conn.close()


@pytest.mark.parametrize('worker_state', ['spawning', 'live'])
def test_projection_waits_for_active_or_unresolved_worker_claim(operation, engine, monkeypatch, worker_state):
    import os
    import sqlite3
    from hermes_cli import kanban_db as kb
    conn = sqlite3.connect(guard.directory().parent / 'kanban.db')
    conn.executescript(kb.SCHEMA_SQL)
    conn.execute('INSERT INTO tasks(id,title,status,created_at,claim_lock,worker_pid) VALUES (?,?,?,0,?,?)',
                 ('t_11111111', 'claimed deployment', 'running', 'fixture-claim',
                  os.getpid() if worker_state == 'live' else None))
    conn.commit()
    plan = copy.deepcopy(operation['plan'])
    plan['tasks'] = ['t_11111111']
    value = store.create(plan)
    approve(value)
    host = Host()
    try:
        with pytest.raises(guard.MaintenanceIsolationError, match='reconciliation'):
            engine.run(value['id'], model='fixture-independent', adapter=host)
        assert store.load(value['id'])['stage'] == 'reconciling'
        assert conn.execute('SELECT status FROM tasks').fetchone()[0] == 'running'
        assert conn.execute('SELECT count(*) FROM task_events').fetchone()[0] == 0
        assert backend.configuration()['phase'] == 'normal'
        # Model the existing dispatcher settling/reaping its own claim. Recovery
        # projects bookkeeping only; it neither cancels workers nor re-applies.
        conn.execute("UPDATE tasks SET status='blocked',claim_lock=NULL,worker_pid=NULL")
        conn.commit()
        monkeypatch.setattr(engine, 'adapter_for', lambda *args: pytest.fail('bookkeeping constructed adapter'))
        assert engine.reconcile(value['id'])['stage'] == 'complete'
        assert host.calls.count('apply') == 1
    finally:
        conn.close()


@pytest.mark.parametrize('damage', ['qa', 'release', 'post_release', 'pending'])
def test_invalid_execution_evidence_fails_closed(operation, engine, damage):
    import json
    host = Host()
    approve(operation)
    result = engine.run(operation['id'], model='fixture-independent', adapter=host)
    if damage == 'qa':
        result['execution']['qa']['checks']['facade'] = False
    elif damage == 'release':
        result['execution']['release']['guard_operation'] = 'f' * 32
    elif damage == 'post_release':
        result['execution']['post_release']['primary'] = False
    else:
        result['execution']['pending'] = {'step': 'release', 'phase': 'done'}
    (store.directory() / (operation['id'] + '.json')).write_text(json.dumps(result))
    before = (guard.directory() / 'state.json').read_bytes()
    with pytest.raises(guard.MaintenanceIsolationError):
        store.load(operation['id'])
    assert (guard.directory() / 'state.json').read_bytes() == before


def test_concurrent_qa_callbacks_persist_all_ordered_evidence(operation, engine):
    from concurrent.futures import ThreadPoolExecutor
    import hashlib
    from pathlib import Path

    class ConcurrentHost(Host):
        def qa(self, operation, record):
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(record, 'sample-' + str(i), 'evidence', {'sample': i}) for i in range(32)]
                for future in futures:
                    future.result()
            return super().qa(operation, record)

    approve(operation)
    result = engine.run(operation['id'], model='fixture-independent', adapter=ConcurrentHost())
    events = result['execution']['events']
    assert [event['sequence'] for event in events] == list(range(34))
    for event in events:
        path = Path(event['path'])
        assert hashlib.sha256(path.read_bytes()).hexdigest() == event['sha256']
        assert path.stat().st_mode & 0o777 == 0o600
    assert store.load(operation['id'])['execution']['events'] == events


def test_sigkill_keeps_exclusion_and_uncertain_apply_is_not_replayed(operation, engine, tmp_path):
    import subprocess
    import sys
    from pathlib import Path
    approve(operation)
    marker = tmp_path / 'intent-ready'
    code = '''
import sys,time
from pathlib import Path
from agent import maintenance_inference as g, maintenance_admission as a
root=Path(sys.argv[1]); g.directory=lambda:root; a.directory=lambda:root.parent/'accounting'
from hermes_cli import maintenance_inference as legacy
legacy.probe=lambda model:None
from hermes_cli import backend_workflow_run as engine
class Host:
    def preflight(self):
        return {'identity': {'source':'a'*40,'containers':['before-0','before-1']},
                'credentials_expires_at':time.time()+10000, 'already_applied':False, 'healthy':True}
    def apply(self, operation, record):
        record('replace-ranks','intent',{'fixture':True})
        Path(sys.argv[3]).write_text('intent persisted')
        time.sleep(60)
        raise AssertionError('parent should terminate only this test child')
engine.run(sys.argv[2],model='fixture-independent',adapter=Host())
'''
    child = subprocess.Popen([sys.executable, '-c', code, str(guard.directory()), operation['id'], str(marker)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 15
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(.05)
        assert marker.exists(), 'fixture child never reached persisted intent'
        child.kill()
        child.wait(timeout=5)
        assert backend.observed()['phase'] == 'interrupted'
        assert store.load(operation['id'])['execution']['pending']['step'] == 'replace-ranks'
        host = Host()
        with pytest.raises(guard.MaintenanceIsolationError, match='Uncertain partial apply'):
            engine.run(operation['id'], model='fixture-independent', adapter=host, resume=True)
        assert 'apply' not in host.calls
        assert backend.configuration()['affected'] == ['spark-pair']
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


def test_deadline_interrupts_guard_and_status_while_controller_is_blocked(operation, engine):
    from hermes_cli import backend_workflow as cli
    observations = []

    class BlockedHost(Host):
        def apply(self, operation, record):
            record('replace-ranks', 'intent', {'fixture': True})
            time.sleep(1.3)
            observations.append(backend.observed())
            observations.append(cli.status(value['id']))
            raise RuntimeError('fixture blocked operation ended')

    plan = copy.deepcopy(operation['plan'])
    plan['budget_seconds'] = 1
    value = store.create(plan)
    approve(value)
    with pytest.raises(guard.MaintenanceIsolationError):
        engine.run(value['id'], model='fixture-independent', adapter=BlockedHost())
    assert observations[0]['phase'] == 'interrupted'
    assert observations[0]['affected'] == ['spark-pair']
    assert observations[1]['activity'] == 'interrupted'
    assert store.read_heartbeat(value['id'])['failed'] is True


@pytest.mark.parametrize('point', ['after_mutation', 'after_release', 'before_projection', 'after_projection'])
def test_sigkill_boundaries_resume_without_duplicate_deployment_or_projection(operation, engine, tmp_path, point):
    import signal
    import sqlite3
    import subprocess
    import sys
    from hermes_cli import kanban_db as kb
    from hermes_cli import backend_workflow as cli
    plan = copy.deepcopy(operation['plan'])
    plan['tasks'] = ['t_33333333']
    value = store.create(plan)
    approve(value)
    db = guard.directory().parent / 'kanban.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(kb.SCHEMA_SQL)
        conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES ('t_33333333','historical deployment','ready',0)")
    goal = tmp_path / 'applied-goal'
    code = '''
import os,runpy,signal,sys
from pathlib import Path
from agent import maintenance_inference as g, maintenance_admission as a
root=Path(sys.argv[1]); g.directory=lambda:root; a.directory=lambda:root.parent/'accounting'
from hermes_cli import maintenance_inference as legacy
legacy.probe=lambda model:None
from hermes_cli import backend_workflow_run as engine, backend_workflow_tasks as tasks
Base=runpy.run_path('tests/hermes_cli/test_backend_workflow_run.py')['Host']
point=sys.argv[3]; goal=Path(sys.argv[4])
def die():
    os.kill(os.getpid(),signal.SIGKILL)
class Host(Base):
    def apply(self, operation, record):
        if point=='after_mutation':
            record('replace-ranks','intent',{'fixture':True})
            goal.write_text('approved target reached; response lost')
            die()
        result=super().apply(operation,record)
        goal.write_text('approved target reached')
        return result
if point=='after_release':
    original_save=engine._save
    def save(value,stage=None):
        if stage=='released': die()
        return original_save(value,stage)
    engine._save=save
if point in ('before_projection','after_projection'):
    original_project=tasks.project
    def project(value):
        if point=='before_projection': die()
        result=original_project(value)
        die()
    tasks.project=project
engine.run(sys.argv[2],model='fixture-independent',adapter=Host())
'''
    child = subprocess.run([sys.executable, '-c', code, str(guard.directory()), value['id'], point, str(goal)],
                           capture_output=True, text=True, timeout=20)
    assert child.returncode == -signal.SIGKILL, child.stderr
    assert goal.exists()
    crashed = store.load(value['id'])
    assert crashed['stage'] != 'complete'
    if point in ('before_projection', 'after_projection'):
        assert cli.status(value['id'])['activity'] == 'bookkeeping_pending'
    if point == 'after_mutation':
        assert backend.observed()['phase'] == 'interrupted'
    host = Host()
    host.changed = True
    host.identity['containers'] = ['after-0', 'after-1']
    result = engine.run(value['id'], model='fixture-independent', adapter=host, resume=True)
    assert result['stage'] == 'complete'
    assert 'apply' not in host.calls
    assert backend.configuration()['phase'] == 'normal'
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status FROM tasks WHERE id='t_33333333'").fetchone()[0] == 'done'
        assert conn.execute("SELECT count(*) FROM task_events WHERE kind='maintenance_reconciled'").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM task_events WHERE kind='completed'").fetchone()[0] == 1


@pytest.mark.parametrize('position', range(9))
@pytest.mark.parametrize('timing', ['before', 'after'])
def test_every_controller_mutation_boundary_retains_intent_without_replay(operation, engine, position, timing):
    steps = ['rank-0-config', 'rank-0-stop', 'rank-1-stop', 'rank-1-archive',
             'rank-1-create', 'rank-1-start', 'rank-0-archive', 'rank-0-create', 'rank-0-start']

    class MultiStepHost(Host):
        completed_steps = 0

        def apply(self, operation, record):
            self.calls.append('apply')
            for index, step in enumerate(steps):
                record(step, 'intent', {'fixture_step': index})
                if index == position and timing == 'before':
                    raise SystemExit(17)
                self.completed_steps += 1
                if self.completed_steps == len(steps):
                    self.changed = True
                    self.identity['containers'] = ['after-0', 'after-1']
                if index == position and timing == 'after':
                    raise SystemExit(17)
                record(step, 'done', {'fixture_step': index})
            return self.inspect()

    host = MultiStepHost()
    approve(operation)
    with pytest.raises(SystemExit):
        engine.run(operation['id'], model='fixture-independent', adapter=host)
    assert store.load(operation['id'])['execution']['pending']['step'] == steps[position]
    assert backend.configuration()['affected'] == ['spark-pair']
    before = host.completed_steps
    if host.changed:
        assert engine.run(operation['id'], model='fixture-independent', adapter=host, resume=True)['stage'] == 'complete'
    else:
        with pytest.raises(guard.MaintenanceIsolationError, match='Uncertain partial apply'):
            engine.run(operation['id'], model='fixture-independent', adapter=host, resume=True)
        assert backend.configuration()['affected'] == ['spark-pair']
    assert host.calls.count('apply') == 1 and host.completed_steps == before
