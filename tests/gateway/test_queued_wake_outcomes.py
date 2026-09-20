"""Production Telegram queue -> real runner drain -> SQLite outcome contracts.

Only the model executor body and Telegram Bot API are substituted. In particular
_run_agent, queue wiring, recursive draining, streaming and adapter sends are real.
"""
import asyncio
import copy
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.run_turn_runner import TurnRunner
from gateway.session import SessionSource
from gateway.shutdown_flush import flush_pending_to_file, flush_overflow_to_file
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from gateway.wake import deliver_wake
from gateway.wake_receipts import ReceiptStore
from plugins.platforms.telegram.adapter import TelegramAdapter


@pytest.fixture
def lane(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    import gateway.run as gateway_run
    monkeypatch.setattr(gateway_run, '_hermes_home', tmp_path)
    (tmp_path / 'config.yaml').write_text('gateway:\n  wake_outcomes:\n    enabled: true\ndisplay:\n  streaming: true\n')
    runner = GatewayRunner(GatewayConfig())
    from hermes_state import SessionDB, AsyncSessionDB
    db = SessionDB(tmp_path / 'state.db')
    runner.session_store._db = db
    runner._session_db = AsyncSessionDB(db)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='42', chat_type='dm', user_id='42', thread_id='123')
    entry = runner.session_store.get_or_create_session(source)
    adapter = TelegramAdapter(PlatformConfig(enabled=True, typing_indicator=False))
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._is_user_authorized = lambda *args, **kwargs: True
    runner._wire_adapter_handlers(adapter)
    adapter.gateway_runner = runner
    adapter._bot = SimpleNamespace(send_message=AsyncMock(), send_chat_action=AsyncMock(),
                                   edit_message_text=AsyncMock(), delete_message=AsyncMock())
    return runner, adapter, source, entry, ReceiptStore()


async def drain(adapter):
    while adapter._background_tasks:
        await asyncio.wait_for(asyncio.gather(*list(adapter._background_tasks)), 20)


@pytest.mark.asyncio
@pytest.mark.parametrize('arrival', ['busy', 'ending', 'idle'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('accepted', [True, False])
async def test_each_queued_wake_owns_execution_and_transport(lane, monkeypatch, arrival, streamed, accepted):
    runner, adapter, source, entry, store = lane
    loop = asyncio.get_running_loop()
    started, model_release = asyncio.Event(), threading.Event()
    sends = []
    snapshots = []
    model_inputs = []
    consumers = []
    identities = [('async_delegation', 'child-a', ''), ('kanban', 'board', 'task', 'done')]

    async def wake():
        # One completion batch coalesces two immutable identities; the Kanban wake
        # behind it exercises the actual runner FIFO overflow, not text merging.
        await deliver_wake(adapter, text='child batch', source=source, session_id=entry.session_id,
                           identity=[identities[0], ('async_delegation', 'child-b', '')])
        if arrival == 'idle':
            await drain(adapter)  # genuinely idle control, not two back-to-back wakes
        await deliver_wake(adapter, text='kanban completion', source=source, session_id=entry.session_id,
                           identity=identities[1])
        # Queue copies must keep trusted observers and the no-replay flag. The
        # serializer must omit them, never reconstruct a callable from metadata.
        for key, event in list(adapter._pending_messages.items()):
            adapter._pending_messages[key] = copy.copy(event)
        assert flush_pending_to_file(adapter._pending_messages) == 0
        assert flush_overflow_to_file({entry.session_key: runner._overflow_queue(entry.session_key)}) == 0

    async def send_message(**kwargs):
        text = kwargs['text']
        snapshots.append((text, {r['id']: (r['state'], r['generation']) for r in store.rows()}))
        if text == 'parent final' and arrival == 'ending':
            await wake()
        if text != 'parent final' and not accepted:
            from telegram.error import TimedOut
            raise TimedOut('synthetic transport uncertainty')
        sends.append(text)
        return SimpleNamespace(message_id=len(sends), chat_id=42)

    adapter._bot.send_message.side_effect = send_message

    def model(self):
        ctx = self._ctx
        model_inputs.append(ctx.message)
        parent = 'parent request' in ctx.message
        if parent:
            loop.call_soon_threadsafe(started.set)
            assert model_release.wait(20)
        text = 'parent final' if parent else ('child final' if 'child batch' in ctx.message else 'kanban final')
        result = {'final_response': text, 'messages': [], 'api_calls': 1, 'completed': True}
        ctx.result_holder[0] = result
        if streamed:
            consumer = GatewayStreamConsumer(adapter, source.chat_id,
                StreamConsumerConfig(transport='edit', chat_type='dm', buffer_threshold=10000, cursor=''),
                metadata={'thread_id': source.thread_id})
            ctx.stream_consumer_holder[0] = consumer
            consumers.append((consumer, text))
            consumer.on_delta(text)
            consumer.finish(text)
        return result

    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    try:
        if arrival != 'idle':
            await adapter.handle_message(MessageEvent(text='parent request', source=source, message_id='1'))
            await asyncio.wait_for(started.wait(), 20)
        if arrival != 'ending':
            await wake()
            if arrival == 'busy':
                assert all(r['state'] == 'admitted' for r in store.rows())
        model_release.set()
        await drain(adapter)
        rows = store.rows()
        summary = [(r['state'], r['generation'], r['attempts']) for r in rows]
        print('arrival=', arrival, 'streamed=', streamed, 'accepted=', accepted,
              'receipts=', summary, 'model_turns=', len(model_inputs), 'wire=', sends)
        assert len(rows) == 3
        if streamed:
            assert consumers
            for consumer, text in consumers:
                assert (consumer.delivered_final_matches(text) is True) == (accepted or text == 'parent final')
        assert all(r['generation'] is not None for r in rows), summary
        assert all(r['state'] == ('delivered' if accepted else 'failed') for r in rows), summary
        keys = {text: store.prepare(identity, source, entry.session_key, entry.session_id).key
                for text, identity in zip(['child final', 'kanban final'], identities)}
        # Check before Bot API acceptance, even when the preceding reply succeeded.
        for text, states in snapshots:
            if text == 'parent final':
                assert all(state != 'delivered' for state, generation in states.values())
            elif text in keys:
                assert states[keys[text]][0] != 'delivered'
        count = len(model_inputs)
        await deliver_wake(adapter, text='duplicate', source=source, session_id=entry.session_id,
                           identity=identities[0])
        await drain(adapter)
        assert len(model_inputs) == count  # admitted/uncertain/failed work never replayed
        assert 'child final' in sends if accepted else 'child final' not in sends
    finally:
        model_release.set()
        await drain(adapter)


@pytest.mark.asyncio
@pytest.mark.parametrize('followup', ['queued', 'steer'])
async def test_unrelated_queued_reply_cannot_settle_failed_wake(lane, monkeypatch, followup):
    runner, adapter, source, entry, store = lane
    loop = asyncio.get_running_loop()
    started, release = asyncio.Event(), threading.Event()
    sent = []

    def model(self):
        ctx = self._ctx
        if 'wake first' in ctx.message:
            loop.call_soon_threadsafe(started.set)
            assert release.wait(20)
            text = 'wake final'
        else:
            text = 'human final'
        result = {'final_response': text, 'messages': [], 'api_calls': 1}
        if text == 'wake final' and followup == 'steer':
            result['pending_steer'] = 'important human correction'
        elif followup == 'steer':
            assert 'important human correction' in ctx.message
            from gateway.event_outcome import observer
            assert observer.get() is None
        ctx.result_holder[0] = result
        return result

    async def send_message(**kwargs):
        if 'wake final' in kwargs['text']:
            from telegram.error import TimedOut
            raise TimedOut('synthetic uncertainty')
        sent.append(kwargs['text'])
        return SimpleNamespace(message_id=len(sent), chat_id=42)

    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    adapter._bot.send_message.side_effect = send_message
    try:
        await deliver_wake(adapter, text='wake first', source=source,
                           session_id=entry.session_id, identity=('async_delegation', 'first', ''))
        await asyncio.wait_for(started.wait(), 20)
        if followup == 'queued':
            await adapter.handle_message(MessageEvent(text='human followup', source=source, message_id='2'))
        release.set()
        await drain(adapter)
        assert 'human final' in sent
        row, = store.rows()
        assert row['state'] == 'failed'  # a later successful human reply is irrelevant
        with store.connect() as db:
            stages = [r[0] for r in db.execute('SELECT stage FROM transitions WHERE receipt_id=?', (row['id'],))]
        assert 'executing' in stages and 'delivered' not in stages
    finally:
        release.set()
        await drain(adapter)


@pytest.mark.asyncio
@pytest.mark.parametrize('opening_observed', [False, True])
async def test_draining_does_not_spawn_observed_followups(lane, monkeypatch, opening_observed):
    runner, adapter, source, entry, store = lane
    loop = asyncio.get_running_loop()
    started, release = asyncio.Event(), threading.Event()
    inputs, spawned = [], []
    real_spawn = adapter._spawn_drain_task

    def spawn(event, key):
        spawned.append(event)
        return real_spawn(event, key)

    def model(self):
        ctx = self._ctx
        inputs.append(ctx.message)
        loop.call_soon_threadsafe(started.set)
        assert release.wait(20)
        result = {'final_response': 'opening final', 'messages': [], 'api_calls': 1,
                  'pending_steer': 'late correction during drain'}
        ctx.result_holder[0] = result
        return result

    monkeypatch.setattr(TurnRunner, 'run_sync', model)
    monkeypatch.setattr(adapter, '_spawn_drain_task', spawn)  # spy; real task machinery still runs
    adapter._bot.send_message.return_value = SimpleNamespace(message_id=1, chat_id=42)
    try:
        if opening_observed:
            await deliver_wake(adapter, text='opening', source=source,
                               session_id=entry.session_id, identity=('async_delegation', 'opening', ''))
        else:
            await adapter.handle_message(MessageEvent(text='opening', source=source, message_id='1'))
        await asyncio.wait_for(started.wait(), 20)
        for identity in ['queued-a', 'queued-b']:
            await deliver_wake(adapter, text=identity, source=source,
                               session_id=entry.session_id, identity=('async_delegation', identity, ''))
        assert entry.session_key in adapter._pending_messages
        assert runner._overflow_queue(entry.session_key)
        runner._draining = True
        release.set()
        await drain(adapter)
        assert len(inputs) == 1, inputs
        assert not spawned
        assert entry.session_key not in adapter._pending_messages
        assert not runner._overflow_queue(entry.session_key)
        rows = [r for r in store.rows() if r['generation'] is None]
        assert len(rows) == 2
        assert all(r['state'] == 'admitted' for r in rows)
    finally:
        release.set()
        await drain(adapter)
