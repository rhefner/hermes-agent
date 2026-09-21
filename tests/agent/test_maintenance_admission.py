"""Real SQLite/process/thread tests for the opt-in admission fence.

No providers or production homes are accessed. Imported runtime entrypoints must
reject before their mocked side-effecting body; storage and races are real.
"""
import asyncio
import concurrent.futures
import contextvars
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from agent import maintenance_admission as gate


@pytest.fixture
def enrolled(tmp_path, monkeypatch):
    root = tmp_path / 'maintenance-admission'
    gate.initialize(root)
    monkeypatch.setattr(gate, 'directory', lambda: root)
    # A real separate executor identity, never the process performing admissions.
    executor = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])
    yield root, executor.pid
    executor.terminate()
    executor.wait(timeout=10)


def close(enrolled):
    root, pid = enrolled
    return gate.close(root, owner='a' * 32, executor_pid=pid, expires=time.time() + 60)


def zero(enrolled):
    return gate.status(enrolled[0], owner='a' * 32, require_zero=True)


def test_admission_atomic_with_close_and_crash_debt(enrolled):
    root, _ = enrolled
    admitted = threading.Event()
    finish = threading.Event()
    def job():
        with gate.work('idle-plugin'):
            admitted.set()
            assert finish.wait(10)
            # Previously accepted work can complete nested work after closure.
            with gate.work('nested-auxiliary'):
                pass
    thread = threading.Thread(target=job)
    thread.start()
    assert admitted.wait(10)
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    with pytest.raises(gate.AdmissionClosed):
        with gate.work('new-session-override'):
            pytest.fail('admitted after closure')
    finish.set()
    thread.join(10)
    assert not thread.is_alive()
    assert zero(enrolled)['outstanding'] == []
    # Reboot/expiry is never an implicit resume, even if state is stale.
    with gate.transaction(root) as c:
        value = gate._control(c)
        value['expires'] = 0
        import json
        c.execute('UPDATE control SET body=?', (json.dumps(value),))
    with pytest.raises(gate.AdmissionClosed):
        zero(enrolled)
    with pytest.raises(gate.AdmissionClosed):
        gate.reserve('new-work')
    assert gate.status(root)['control']['closed']


def test_detached_child_keeps_reservation_after_parent_finishes(enrolled):
    with gate.work('gateway-inbound'):
        run, _ = gate.reserved_target(lambda: None, 'bg-review')
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    run()
    assert zero(enrolled)['outstanding'] == []
    with pytest.raises(gate.AdmissionClosed):
        gate.reserved_target(lambda: None, 'new-bg-review')


def test_stale_copied_context_cannot_resurrect_parent(enrolled):
    with gate.work('parent'):
        copied = contextvars.copy_context()
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed):
        copied.run(gate.reserve, 'late-child')


def test_mem0_title_thread_is_reserved_before_start(enrolled):
    from agent.memory_provider import spawn_context_thread
    started, finish = threading.Event(), threading.Event()
    def target():
        started.set()
        assert finish.wait(10)
    with gate.work('parent'):
        t = spawn_context_thread(target, name='memory-test')
        t.start()
    assert started.wait(10)
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    finish.set()
    t.join(10)
    assert zero(enrolled)['outstanding'] == []
    late = spawn_context_thread(lambda: pytest.fail('late start'), name='late')
    with pytest.raises(gate.AdmissionClosed):
        late.start()


@pytest.mark.asyncio
async def test_real_gateway_entry_blocks_before_plugins_and_internal_events(enrolled):
    from gateway.run_inbound import GatewayInboundMixin
    close(enrolled)
    runner = SimpleNamespace(_hm_admit_event=AsyncMock(side_effect=AssertionError('plugin entered')))
    for event in [SimpleNamespace(text='/refine', internal=False),
                  SimpleNamespace(text='/some-plugin', internal=False),
                  SimpleNamespace(text='continuation', internal=True)]:
        assert await GatewayInboundMixin._handle_message(runner, event) == gate.NOTICE
    runner._hm_admit_event.assert_not_called()


@pytest.mark.asyncio
async def test_real_gateway_executor_cancellation_does_not_forge_drain(enrolled):
    from gateway.run import GatewayRunner
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    runner = SimpleNamespace(_get_executor=lambda: executor)
    started, finish = threading.Event(), threading.Event()
    def body():
        started.set()
        assert finish.wait(10)
    waiter = asyncio.create_task(GatewayRunner._run_in_executor_with_context(runner, body))
    while not started.is_set():
        await asyncio.sleep(.01)
    close(enrolled)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    finish.set()
    executor.shutdown(wait=True)
    assert zero(enrolled)['outstanding'] == []


def test_real_conversation_cron_and_dispatcher_are_closed_before_body(enrolled):
    from agent.turn_facade import TurnFacadeMixin
    from cron.scheduler import run_job
    from hermes_cli.kanban_db_dispatch import dispatch_once
    close(enrolled)
    for call in [lambda: TurnFacadeMixin.run_conversation(object(), 'hi'),
                 lambda: run_job({'id': 'synthetic', 'no_agent': True}),
                 lambda: dispatch_once(None)]:
        with pytest.raises(gate.AdmissionClosed):
            call()
    assert zero(enrolled)['outstanding'] == []


def test_crashed_worker_debt_not_swept_as_zero(enrolled):
    root, _ = enrolled
    code = '''import os,sys
from pathlib import Path
from agent import maintenance_admission as g
g.directory=lambda:Path(sys.argv[1])
g.reserve('crash')
os._exit(0)
'''
    result = subprocess.run([sys.executable, '-c', code, str(root)],
                            env={'PATH': '/usr/bin:/bin', 'PYTHONPATH': str(Path(__file__).resolve().parents[2])},
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='Outstanding'):
        gate.reopen(root, owner='a'*32, approval='fixture post-QA')


def test_reopen_owner_approval_and_missing_storage(enrolled):
    root, _ = enrolled
    close(enrolled)
    for owner, approval in [('b'*32, 'qa'), ('a'*32, '')]:
        with pytest.raises(gate.AdmissionClosed):
            gate.reopen(root, owner=owner, approval=approval)
    gate.reopen(root, owner='a'*32, approval='fixture post-QA')
    with gate.work('restored'):
        pass
    (root / 'work.sqlite').unlink()
    with pytest.raises(gate.AdmissionClosed):
        gate.reserve('broken-store')


def test_close_race_all_admitted_work_is_accounted(enrolled):
    barrier = threading.Barrier(17)
    release = threading.Event()
    accepted = []
    def contender():
        barrier.wait(timeout=10)
        try:
            reservation = gate.reserve('racer')
        except gate.AdmissionClosed:
            return
        accepted.append(reservation.key)
        assert release.wait(10)
        reservation.finish()
    threads = [threading.Thread(target=contender) for _ in range(16)]
    for t in threads:
        t.start()
    barrier.wait(timeout=10)
    close(enrolled)
    # Every row inserted before close is retained; every later admission fails.
    rows = gate.status(enrolled[0])['outstanding']
    if rows:
        with pytest.raises(gate.AdmissionClosed):
            zero(enrolled)
    release.set()
    for t in threads:
        t.join(10)
        assert not t.is_alive()
    assert set(accepted) == {r['id'] for r in rows}
    assert zero(enrolled)['outstanding'] == []


def test_real_background_review_spawn_reserves_detached_work(enrolled, monkeypatch):
    from run_agent import AIAgent
    from agent import background_review as review
    started, finish = threading.Event(), threading.Event()
    def target():
        started.set()
        assert finish.wait(10)
    monkeypatch.setattr(review, 'prepare_background_review_run', lambda _: object())
    monkeypatch.setattr(review, 'spawn_background_review_thread', lambda *a, **k: (target, ''))
    runner = SimpleNamespace(_maybe_requeue_preempted_review=lambda *a: None)
    with gate.work('idle-refine'):
        AIAgent._spawn_background_review_now(runner, [], explicit=True)
    assert started.wait(10)
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    finish.set()
    deadline = time.monotonic()+10
    while gate.status(enrolled[0])['outstanding'] and time.monotonic()<deadline:
        time.sleep(.01)
    assert zero(enrolled)['outstanding'] == []


def test_protected_auxiliary_survives_waiter_cancellation_in_ledger(enrolled, monkeypatch):
    from agent import auxiliary_client as aux
    started, finish, cancelled, owner_done = (threading.Event() for _ in range(4))
    monkeypatch.setattr(aux, '_capture_aux_cancel_check', lambda: cancelled.is_set)
    monkeypatch.setattr(aux, '_aux_interrupt_protected', lambda: True)
    failures = []
    def provider(_):
        started.set()
        assert finish.wait(10)
    def owner():
        try:
            with gate.work('aux-owner'):
                aux._run_protected_sync_provider_call(provider, {})
        except aux.AuxiliaryExplicitCancellation:
            pass
        except BaseException as e:
            failures.append(e)
        finally:
            owner_done.set()
    waiter = threading.Thread(target=owner)
    waiter.start()
    assert started.wait(10)
    close(enrolled)
    cancelled.set()
    assert owner_done.wait(10)
    waiter.join(10)
    assert not failures
    with pytest.raises(gate.AdmissionClosed, match='not drained'):
        zero(enrolled)
    finish.set()
    deadline = time.monotonic()+10
    while gate.status(enrolled[0])['outstanding'] and time.monotonic()<deadline:
        time.sleep(.01)
    assert zero(enrolled)['outstanding'] == []


@pytest.mark.asyncio
async def test_busy_commands_and_platform_hooks_do_not_bypass_fence(enrolled):
    from gateway.run_busy import GatewayBusySessionMixin
    from gateway.run_adapters import GatewayAdapterLifecycleMixin
    close(enrolled)
    assert await GatewayBusySessionMixin._dispatch_busy_slash_command(
        object(), object(), object(), 'synthetic', object()) == gate.NOTICE
    with pytest.raises(gate.AdmissionClosed):
        await GatewayAdapterLifecycleMixin._handle_gateway_platform_event(object(), {}, object())


def test_only_exact_executor_process_is_exempt_and_pid_identity_is_live(enrolled):
    root, pid = enrolled
    close(enrolled)
    with pytest.raises(gate.AdmissionClosed):
        gate.reserve('same-profile-but-different-process')
    os.kill(pid, 15)
    # Identity of a zombie still exists until reaped; explicitly waitpid before
    # verification so a dead process cannot be an executor authority.
    os.waitpid(pid, 0)
    with pytest.raises((gate.AdmissionClosed, OSError)):
        zero(enrolled)


def test_profile_switch_does_not_switch_away_from_fleet_gate(enrolled, monkeypatch):
    import hermes_constants
    root, _ = enrolled
    monkeypatch.setattr(gate, 'directory', lambda: gate.get_default_hermes_root() / 'maintenance-admission')
    monkeypatch.setattr(gate, 'get_default_hermes_root', lambda: root.parent)
    close(enrolled)
    for profile in ('default','coding','full','media','ops','research','reviewer','robinhood','default'):
        home = root.parent if profile == 'default' else root.parent/'profiles'/profile
        token = hermes_constants.set_hermes_home_override(home)
        try:
            with pytest.raises(gate.AdmissionClosed):
                gate.reserve('profile-scoped-session-override')
        finally:
            hermes_constants.reset_hermes_home_override(token)
