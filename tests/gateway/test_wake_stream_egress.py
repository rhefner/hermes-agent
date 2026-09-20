"""Round-one review regressions: real consumer/Telegram rail, temporary SQLite.

The model is not invoked; only external transports are substituted. Origin and
receipt resolution use production code. Closed boundaries must propagate rather
than turn into a transport-failure fallback.
"""
import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.gateway.test_queued_wake_outcomes import lane
from gateway.event_outcome import WakeBoundaryClosed, observer
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from gateway.wake_monitor import reconcile
from gateway.wake_receipts import DEFAULTS, ReceiptStore


def close_origin(lane, change):
    runner, adapter, source, entry, store = lane
    if change == 'reset':
        runner.session_store._db.end_session(entry.session_id, 'reset')
    elif change == 'route':
        with closing(sqlite3.connect(store.home / 'state.db')) as db, db:
            raw = db.execute('SELECT entry_json FROM gateway_routing WHERE session_key=?',
                             (entry.session_key,)).fetchone()[0]
            data = json.loads(raw)
            data['origin']['thread_id'] = 'other-thread'
            db.execute('UPDATE gateway_routing SET entry_json=? WHERE session_key=?',
                       (json.dumps(data), entry.session_key))
    elif change == 'profile':
        (store.home / 'config.yaml').write_text(
            'gateway:\n  wake_outcomes:\n    enabled: true\n  profile_routes:\n'
            '    - platform: telegram\n      chat_id: "42"\n      profile: other\n')


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['open', 'reset', 'route', 'profile'])
@pytest.mark.parametrize('path', ['boundary_native', 'boundary_fallback', 'frame', 'draft',
    'abandon', 'edit', 'first', 'fresh', 'chunk', 'commentary', 'tail', 'flood',
    'empty_final', 'strip', 'no_unseen', 'try_frame', 'send_or_edit', 'queue'])
async def test_all_content_egress_respects_origin(lane, monkeypatch, change, path):
    runner, adapter, source, entry, store = lane
    receipt = store.prepare(('review', path, change), source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    receipt('execution', session_key=entry.session_key, session_id=entry.session_id, generation=1)
    consumer = GatewayStreamConsumer(adapter, source.chat_id,
        StreamConsumerConfig(transport='edit', chat_type='dm', cursor='~'),
        metadata={'thread_id': source.thread_id})
    consumer._accumulated = 'PRIVATE CONTENT'
    consumer._message_id = '1'
    consumer._last_sent_text = 'PRIVATE CONTENT~'
    consumer._draft_id = 1
    consumer._use_draft_streaming = True
    adapter.send_stream_frame = AsyncMock(return_value=path != 'boundary_fallback')
    adapter.send_draft = AsyncMock(return_value=SimpleNamespace(success=True))
    abandon = AsyncMock()
    monkeypatch.setattr(type(adapter), 'abandon_open_draft', abandon, raising=False)
    adapter._bot.send_message.return_value = SimpleNamespace(message_id=2, chat_id=42)
    adapter._bot.edit_message_text.return_value = SimpleNamespace(message_id=1, chat_id=42)
    consumer.finish('PRIVATE CONTENT')
    close_origin(lane, change)
    calls = {
        'queue': consumer.run,
        'boundary_native': lambda: consumer._finalize_boundary_stream('Approval'),
        'boundary_fallback': lambda: consumer._finalize_boundary_stream('Approval'),
        'frame': lambda: consumer._send_frame('PRIVATE CONTENT', finalize=True),
        'draft': lambda: consumer._send_draft_frame('PRIVATE CONTENT'),
        'abandon': consumer._abandon_native_stream,
        'edit': lambda: consumer._edit_message(message_id='1', content='PRIVATE CONTENT'),
        'first': lambda: consumer._first_send('PRIVATE CONTENT', finalize=True),
        'fresh': lambda: consumer._try_fresh_final('PRIVATE CONTENT'),
        'chunk': lambda: consumer._send_new_chunk('PRIVATE CONTENT', None),
        'commentary': lambda: consumer._send_commentary('PRIVATE CONTENT'),
        'tail': consumer._flush_segment_tail_on_edit_failure,
        'flood': lambda: consumer._send_with_flood_retry(content='PRIVATE CONTENT', retry_log='%s'),
        'empty_final': lambda: consumer._send_empty_fallback_final('PRIVATE CONTENT'),
        'strip': consumer._try_strip_cursor,
        'no_unseen': lambda: consumer._fallback_when_nothing_unseen('PRIVATE CONTENT'),
        'try_frame': lambda: consumer._try_frame(consumer._send_frame('PRIVATE CONTENT', finalize=True), '%s'),
        'send_or_edit': lambda: consumer._send_or_edit('PRIVATE CONTENT', finalize=True),
    }
    token = observer.set(receipt)
    try:
        if change == 'open':
            await calls[path]()
        else:
            with pytest.raises(WakeBoundaryClosed):
                await calls[path]()
    finally:
        observer.reset(token)
    transports = [adapter._bot.send_message, adapter._bot.edit_message_text,
                  adapter.send_stream_frame, adapter.send_draft, abandon]
    if change != 'open':
        assert not any(t.call_count for t in transports)
        assert store.get(receipt.key)['state'] == 'closed_boundary'
    else:
        assert any(t.call_count for t in transports)
        if path == 'boundary_fallback':
            assert adapter._bot.send_message.call_args.kwargs['message_thread_id'] == 123
        assert store.get(receipt.key)['state'] == 'executing'


@pytest.mark.asyncio
@pytest.mark.parametrize('native_result', [False, 'error'])
async def test_boundary_rechecks_after_transport_await(lane, native_result):
    runner, adapter, source, entry, store = lane
    receipt = store.prepare(('review', 'race'), source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    receipt('execution', session_key=entry.session_key, session_id=entry.session_id, generation=1)
    consumer = GatewayStreamConsumer(adapter, source.chat_id, metadata={'thread_id': source.thread_id})
    consumer._accumulated = 'PRIVATE CONTENT'
    async def frame(*args, **kwargs):
        close_origin(lane, 'reset')
        if native_result == 'error':
            raise RuntimeError('transport failed')
        return False
    adapter.send_stream_frame = AsyncMock(side_effect=frame)
    token = observer.set(receipt)
    try:
        with pytest.raises(WakeBoundaryClosed):
            await consumer._finalize_boundary_stream('Approval')
    finally:
        observer.reset(token)
    assert adapter.send_stream_frame.call_count == 1
    assert adapter._bot.send_message.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['dict', 'object'])
@pytest.mark.parametrize('ack,expected', [({'success': True, 'delivered': False}, 'failed'),
    ({'success': True, 'delivered': True}, 'sent'), ({'success': True}, 'sent'),
    ({'success': False, 'delivered': True}, 'failed'), ({}, 'failed')])
async def test_notice_ack_parity_and_restart_budget(lane, shape, ack, expected):
    runner, adapter, source, entry, store = lane
    receipt = store.prepare(('review', 'notice'), source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    receipt('failed')
    send = AsyncMock(return_value=ack if shape == 'dict' else SimpleNamespace(**ack))
    await reconcile(store, DEFAULTS, send)
    for _ in range(3):
        await reconcile(ReceiptStore(store.home), DEFAULTS, send)
    row = store.get(receipt.key)
    assert row['notice'] == expected
    assert row['notice_attempts'] == 1
    assert row['state'] == 'failed'
    assert send.call_count == 1
