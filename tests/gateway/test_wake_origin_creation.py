"""Wake-origin regressions (t_3cc8c89d).

Covers the two defects behind silently-lost coordinator wakes:
- creation: the gateway turn env must publish HERMES_SESSION_ID so
  ``_resolve_notify_target`` stamps ``delivery_metadata['origin_session_id']``
  (cached-agent turns never run agent init, so the agent-init rebind alone
  is not enough);
- acceptance: a wake that is not admitted raises (never logs success), the
  unknown-origin receipt gets exactly one bounded operator notice from the
  wake monitor, and a closed origin is a plain delivery failure.
"""
import pytest

from tests.gateway.test_queued_wake_outcomes import lane
from gateway.run_turn_runner import TurnRunner
from gateway.session import SessionContext
from gateway.session_context import get_session_env
from gateway.wake import WakeNotAccepted, deliver_wake
from gateway.wake_receipts import DEFAULTS
from gateway.wake_monitor import reconcile
from tools.kanban_tools import _resolve_notify_target


def bot_ok(**kwargs):
    from types import SimpleNamespace
    return SimpleNamespace(message_id=1, chat_id=42)


@pytest.mark.asyncio
async def test_gateway_turn_env_publishes_session_id_to_notify_target(lane):
    runner, adapter, source, entry, store = lane
    context = SessionContext(
        source=source, connected_platforms=[], home_channels={},
        session_key=entry.session_key, session_id=entry.session_id)
    tokens = runner._set_session_env(context)
    try:
        assert get_session_env("HERMES_SESSION_ID", "") == entry.session_id
        target = _resolve_notify_target()
        assert target is not None
        assert target["delivery_metadata"]["origin_session_id"] == entry.session_id
    finally:
        runner._clear_session_env(tokens)


@pytest.mark.asyncio
async def test_missing_origin_wake_raises_and_notice_is_bounded(lane):
    runner, adapter, source, entry, store = lane
    adapter._bot.send_message.side_effect = bot_ok
    # Subscription metadata without origin_session_id: the boundary is
    # unresolvable, so admission refuses (previously a silent no-op that still
    # logged "woke agent" and advanced the cursor).
    with pytest.raises(WakeNotAccepted):
        await deliver_wake(adapter, text='must not run', source=source, session_id='',
                           identity=('kanban', 'board', 't_missing', 1))
    row, = store.rows()
    assert row['session_id'] == '' and row['state'] == 'uncertain'

    sent = []

    async def notice(route, text):
        sent.append((route, text))
        return {'success': True}

    await reconcile(store, DEFAULTS, notice)
    assert len(sent) == 1  # exactly one bounded operator notice
    await reconcile(store, DEFAULTS, notice)
    assert len(sent) == 1  # notice budget spent; never a second


@pytest.mark.asyncio
async def test_closed_origin_wake_is_a_delivery_failure(lane):
    runner, adapter, source, entry, store = lane
    adapter._bot.send_message.side_effect = bot_ok
    runner.session_store._db.end_session(entry.session_id, 'reset')
    # A closed origin can never be admitted again: the verified-permanent
    # quiet terminal, never a second agent turn.
    await deliver_wake(adapter, text='must not run', source=source,
                       session_id=entry.session_id,
                       identity=('kanban', 'board', 't_closed', 1))
    row, = store.rows()
    assert row['state'] == 'closed_boundary'
