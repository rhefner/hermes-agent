"""Current-base origin/notifier/native final-ledger contracts, real runner/SQLite."""
import asyncio
import dataclasses
import json
import sqlite3
from types import SimpleNamespace

import pytest

from tests.gateway.test_queued_wake_outcomes import lane, drain
from gateway.run_turn_runner import TurnRunner
from gateway.wake import deliver_wake
from gateway.wake_receipts import ReceiptStore, boundary, DEFAULTS
from gateway.wake_monitor import reconcile
from gateway.config import Platform
from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn


def model_final(self):
    result = {'final_response': 'correlated final', 'messages': [], 'api_calls': 1}
    self._ctx.result_holder[0] = result
    return result


def bot_ok(**kwargs):
    return SimpleNamespace(message_id=1, chat_id=42)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['completed', 'review_requested', 'blocked', 'crashed'])
async def test_native_notifier_idle_and_duplicate(lane, monkeypatch, tmp_path, kind):
    runner, adapter, source, entry, store = lane
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner._kanban_dispatcher_lock_handle = object()
    monkeypatch.setattr(TurnRunner, 'run_sync', model_final)
    adapter._bot.send_message.side_effect = bot_ok
    conn = kbc.connect()
    tid = kb.create_task(conn, title='outcome', assignee='worker', session_id='worker-not-origin')
    kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id=source.chat_id,
                       user_id=source.user_id, thread_id=source.thread_id, chat_type='dm',
                       delivery_mode='wake', delivery_metadata={'origin_session_id': entry.session_id})
    # Native event schema; no dispatcher/model is substituted or started.
    conn.execute('INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)',
                 (tid, kind, json.dumps({'summary': 'test handoff', 'kind': 'needs_input'}), 1))
    conn.commit()
    conn.close()
    for _ in range(3):
        deliveries = await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=False, gc_retention_days=30)
        for delivery in deliveries:
            await _KanbanNotification(runner, delivery, platform_cls=Platform, sub_fail_counts={}).deliver()
        await drain(adapter)
    row, = store.rows()
    assert row['session_id'] == entry.session_id
    assert row['state'] == 'delivered' and row['attempts'] == 1
    assert row['generation'] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['unknown', 'closed', 'route', 'profile', 'config_route'])
async def test_changed_origin_never_runs_or_sends(lane, monkeypatch, change, tmp_path):
    runner, adapter, source, entry, store = lane
    calls = []
    def model(self):
        calls.append(1)
        return model_final(self)
    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    adapter._bot.send_message.side_effect = bot_ok
    sid = entry.session_id
    db = runner.session_store._db
    if change == 'unknown':
        sid = 'missing-original'
    elif change == 'closed':
        db.end_session(sid, 'reset')
    elif change == 'profile':
        with sqlite3.connect(tmp_path / 'state.db') as conn:
            conn.execute("UPDATE sessions SET profile_name='foreign' WHERE id=?", (sid,))
    elif change == 'route':
        source = dataclasses.replace(source, thread_id='other')
    else:
        (tmp_path / 'config.yaml').write_text('gateway:\n  wake_outcomes:\n    enabled: true\n  profile_routes:\n    - platform: telegram\n      chat_id: "42"\n      profile: foreign\n')
    await deliver_wake(adapter, text='must not run', source=source, session_id=sid,
                       identity=('kanban', 'board', 'task', change))
    await drain(adapter)
    assert not calls
    adapter._bot.send_message.assert_not_called()
    sent = []
    async def notice(*args):
        sent.append(args)
        return {'success': True}
    await reconcile(store, DEFAULTS, notice)
    assert not sent


@pytest.mark.asyncio
async def test_duplicate_identity_cannot_rebind_after_reset(lane, monkeypatch):
    runner, adapter, source, entry, store = lane
    monkeypatch.setattr(TurnRunner, 'run_sync', model_final)
    adapter._bot.send_message.side_effect = bot_ok
    await deliver_wake(adapter, text='once', source=source, session_id=entry.session_id, identity=('test', 'one'))
    await drain(adapter)
    calls = adapter._bot.send_message.call_count
    other = dataclasses.replace(source, thread_id='different')
    other_entry = runner.session_store.get_or_create_session(other)
    await deliver_wake(adapter, text='rebound duplicate', source=other, session_id=other_entry.session_id,
                       identity=('test', 'one'))
    await drain(adapter)
    assert len(store.rows()) == 1
    assert adapter._bot.send_message.call_count == calls


@pytest.mark.asyncio
@pytest.mark.parametrize('fork', ['compression', 'reset', 'branch'])
async def test_only_verified_compression_descendant_can_execute(lane, monkeypatch, tmp_path, fork):
    runner, adapter, source, entry, store = lane
    monkeypatch.setattr(TurnRunner, 'run_sync', model_final)
    adapter._bot.send_message.side_effect = bot_ok
    original = entry.session_id
    db = runner.session_store._db
    db.end_session(original, 'compression' if fork != 'reset' else 'reset')
    db.create_session('tip', source='telegram', parent_session_id=original,
                      model_config={'_branched_from': original} if fork == 'branch' else None)
    entry.session_id = 'tip'
    runner.session_store._save_entry(entry.session_key)
    await deliver_wake(adapter, text='continuation', source=source, session_id=original, identity=('test', fork))
    await drain(adapter)
    row, = store.rows()
    assert row['session_id'] == original
    assert (row['state'] == 'delivered') == (fork == 'compression')
    if fork != 'compression':
        adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('closed', [False, True])
@pytest.mark.parametrize('ack_gap', [False, True])
async def test_native_final_recovery_correlates_without_model_replay(lane, monkeypatch, closed, ack_gap, tmp_path):
    runner, adapter, source, entry, store = lane
    from gateway import delivery_ledger
    calls = []
    def model(self):
        calls.append(1)
        return model_final(self)
    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    (tmp_path / 'config.yaml').write_text('gateway:\n  delivery_ledger: true\n  wake_outcomes:\n    enabled: true\n')
    from telegram.error import TimedOut
    adapter._bot.send_message.side_effect = TimedOut('synthetic uncertain transport')
    await deliver_wake(adapter, text='wake', source=source, session_id=entry.session_id, identity=('test','recovery'))
    await drain(adapter)
    row, = store.rows()
    assert row['state'] == 'failed'
    with sqlite3.connect(tmp_path / 'state.db') as conn:
        conn.row_factory = sqlite3.Row
        obligation = dict(conn.execute('SELECT * FROM delivery_obligations WHERE obligation_id LIKE "wake-%"').fetchone())
    if closed:
        runner.session_store._db.end_session(entry.session_id, 'reset')
    adapter._bot.send_message.side_effect = bot_ok
    adapter._bot.send_message.reset_mock()
    if ack_gap:
        from gateway import wake_delivery
        monkeypatch.setattr(wake_delivery, 'settle', lambda oid: None)
    await runner._redeliver_claimed_obligations([obligation])
    if ack_gap:
        assert store.get(row['id'])['state'] != 'delivered'
        async def forbidden_send(*args):
            raise AssertionError('ACKed or closed wake must not send a notice')
        await reconcile(ReceiptStore(), DEFAULTS, forbidden_send)
    assert len(calls) == 1
    assert (store.get(row['id'])['state'] == 'delivered') == (not closed)
    if closed:
        adapter._bot.send_message.assert_not_called()
    # Cancel any native bounded retry timers armed by failure handling.
    for task in list(asyncio.all_tasks()):
        if task is not asyncio.current_task() and 'redeliver_after_wait' in repr(task.get_coro()):
            task.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize('started', [False, True])
async def test_model_free_stall_notice_through_live_queue(lane, monkeypatch, started):
    import threading
    runner, adapter, source, entry, store = lane
    from gateway.platforms.event import MessageEvent
    loop = asyncio.get_running_loop()
    entered, release = asyncio.Event(), threading.Event()
    def model(self):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(20)
        return model_final(self)
    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    adapter._bot.send_message.side_effect = bot_ok
    try:
        if not started:
            await adapter.handle_message(MessageEvent(text='parent busy', source=source))
            await asyncio.wait_for(entered.wait(), 20)
        await deliver_wake(adapter, text='stalled wake', source=source, session_id=entry.session_id,
                           identity=('stall', started))
        if started:
            await asyncio.wait_for(entered.wait(), 20)
        row, = store.rows()
        assert row['state'] == ('executing' if started else 'admitted')
        with store.connect() as db:
            db.execute('UPDATE receipts SET updated=0,activity=0')
        notices = []
        async def send(route, text):
            notices.append(text)
            return {'success': True}
        for _ in range(3):
            await reconcile(ReceiptStore(), DEFAULTS, send)
        assert len(notices) == 1
        assert store.get(row['id'])['notice_attempts'] == 1
        assert store.get(row['id'])['state'] != 'delivered'
    finally:
        release.set()
        await drain(adapter)


@pytest.mark.asyncio
async def test_reset_during_model_suppresses_final_and_error(lane, monkeypatch):
    runner, adapter, source, entry, store = lane
    def model(self):
        runner.session_store._db.end_session(entry.session_id, 'reset')
        return model_final(self)
    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    adapter._bot.send_message.side_effect = bot_ok
    await deliver_wake(adapter, text='reset while running', source=source,
                       session_id=entry.session_id, identity=('test','reset-mid-model'))
    await drain(adapter)
    assert all('correlated final' not in str(c) and 'encountered an error' not in str(c)
               for c in adapter._bot.send_message.call_args_list)
    row, = store.rows()
    assert row['state'] == 'closed_boundary'


@pytest.mark.asyncio
async def test_executable_canary_is_single_use_and_verifies_transport(lane, monkeypatch, tmp_path):
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location('wake_canary', Path(__file__).parents[2] / 'scripts' / 'wake-canary.py')
    canary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(canary)
    runner, adapter, source, entry, store = lane
    source = dataclasses.replace(source, thread_id='16755')
    entry = runner.session_store.get_or_create_session(source)
    board = tmp_path / 'canary-board.db'
    monkeypatch.setenv('HERMES_KANBAN_DB', str(board))
    runner._kanban_dispatcher_lock_handle = object()
    monkeypatch.setattr(TurnRunner, 'run_sync', model_final)
    adapter._bot.send_message.side_effect = bot_ok
    receipt = canary.prepare(tmp_path, board, entry.session_id, 'default')
    marker = tmp_path / 'wake_outcomes' / 'canary-t_7b7eabfa.json'
    assert receipt['state'] == 'emitted'
    assert canary.verify(tmp_path, marker, 0) == 1
    with pytest.raises(FileExistsError):
        canary.prepare(tmp_path, board, entry.session_id, 'default')
    deliveries = await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile='default', gc_due=False, gc_retention_days=30)
    for delivery in deliveries:
        await _KanbanNotification(runner, delivery, platform_cls=Platform, sub_fail_counts={}).deliver()
    await drain(adapter)
    assert canary.verify(tmp_path, marker, 0) == 0


@pytest.mark.asyncio
async def test_explicit_non_delivery_cannot_be_native_ack(lane):
    from gateway.platforms.event import MessageEvent
    runner, adapter, source, entry, store = lane
    receipt = store.prepare(('test','suppressed-transport'), source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    receipt('execution', session_key=entry.session_key, session_id=entry.session_id, generation=1)
    event = MessageEvent(text='wake', source=source, internal=True)
    event._outcome_observer = receipt
    oid = await adapter._record_delivery_obligation(event, entry.session_key, 'final', adapter, False)
    assert oid.startswith('wake-')
    await adapter._finalize_delivery_obligation(oid, SimpleNamespace(success=True, delivered=False), event, adapter)
    assert store.get(receipt.key)['state'] == 'suppressed'
    from gateway.wake_delivery import recover_settled
    recover_settled(store)
    assert store.get(receipt.key)['state'] != 'delivered'
