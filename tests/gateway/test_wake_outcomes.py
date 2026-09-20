"""Wake outcome contracts through real SQLite, adapter and runner; no network/model."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.event_outcome import report
from gateway.platforms.base import SendResult
from gateway.run import GatewayRunner
from gateway.session import SessionSource, build_session_key
from gateway.wake import deliver_wake
from gateway.wake_monitor import reconcile
from gateway.wake_receipts import ReceiptStore, DEFAULTS, boundary
from evals.heartbeat_idle_wire import WireAdapter


@pytest.fixture
def route(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import gateway.run as gateway_run
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setenv("TELEGRAM_HOME_CHANNEL", "42")
    (tmp_path / "config.yaml").write_text("gateway:\n  wake_outcomes:\n    enabled: true\n")
    runner = GatewayRunner(GatewayConfig())
    from hermes_state import SessionDB, AsyncSessionDB
    db = SessionDB(tmp_path / "state.db")
    runner.session_store._db = db
    runner._session_db = AsyncSessionDB(db)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="42", chat_type="dm", user_id="42")
    entry = runner.session_store.get_or_create_session(source)
    adapter = WireAdapter(PlatformConfig(enabled=True, typing_indicator=False), Platform.TELEGRAM)
    adapter.wire = []
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._is_user_authorized = lambda source, **kwargs: True
    adapter.set_message_handler(runner._handle_message)
    adapter.gateway_runner = runner
    return runner, adapter, source, entry, ReceiptStore()


async def drain(adapter):
    while adapter._background_tasks:
        await asyncio.gather(*list(adapter._background_tasks))


@pytest.mark.asyncio
async def test_actual_runner_final_and_duplicate(route, monkeypatch):
    runner, adapter, source, entry, store = route
    assert boundary(store, {"route_json": json.dumps(__import__('gateway.wake_receipts', fromlist=['route_of']).route_of(source)),
                            "session_id": entry.session_id, "session_key": entry.session_key})

    async def model(**kw):
        return {"final_response": "Supervisor saw child failure", "messages": [
            {"role": "user", "content": kw["message"]},
            {"role": "assistant", "content": "Supervisor saw child failure"}], "api_calls": 1}

    runner._run_agent = AsyncMock(side_effect=model)
    await deliver_wake(adapter, text="Child failed", source=source, session_id=entry.session_id,
                       identity=("async_delegation", "child-1", ""))
    await drain(adapter)
    assert adapter.wire == ["Supervisor saw child failure"]
    row = store.rows()[0]
    assert row["state"] == "delivered"
    assert row["generation"] is not None
    await deliver_wake(adapter, text="Child failed", source=source, session_id=entry.session_id,
                       identity=("async_delegation", "child-1", ""))
    await drain(adapter)
    assert runner._run_agent.call_count == 1


@pytest.mark.asyncio
async def test_missing_outcomes_and_bounded_notice(route):
    runner, adapter, source, entry, store = route
    receipt = store.prepare(("kanban", "b", 1), source, entry.session_key, entry.session_id)
    assert boundary(store, store.get(receipt.key)), (store.home, entry.to_dict(), runner.session_store.sessions_dir)
    assert store.claim_admission(receipt.key)
    store.transition(receipt.key, "admitted")
    with store.connect() as db:
        db.execute("UPDATE receipts SET updated=0")
    sent = []

    async def send(route, text):
        sent.append(text)
        return SendResult(success=True, message_id="1")

    cfg = {**DEFAULTS, "enabled": True}
    assert len(await reconcile(store, cfg, send)) == 1
    assert await reconcile(ReceiptStore(), cfg, send) == []
    assert len(sent) == 1
    assert store.get(receipt.key)["state"] == "admitted"
    assert store.get(receipt.key)["notice"] == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("second", [False, None])
@pytest.mark.parametrize("prior_attempts", [0, 1])
async def test_notice_boundary_race_preserves_disposition_and_budget(route, monkeypatch, second, prior_attempts):
    import gateway.wake_monitor as monitor
    runner, adapter, source, entry, store = route
    receipt = store.prepare("boundary-race", source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    store.transition(receipt.key, "admitted")
    with store.connect() as db:
        db.execute("UPDATE receipts SET updated=0,notice=?,notice_at=0,notice_attempts=?",
                   ("uncertain" if prior_attempts else "", prior_attempts))
    send = AsyncMock(return_value={"success": True})
    for _ in range(3):
        verdicts = iter([True, second])
        monkeypatch.setattr(monitor, "boundary", lambda *args: next(verdicts))
        await reconcile(ReceiptStore(), DEFAULTS, send)
        row = store.get(receipt.key)
        assert row["state"] == ("closed_boundary" if second is False else "admitted")
        assert row["notice_attempts"] == prior_attempts
        send.assert_not_called()
    monkeypatch.setattr(monitor, "boundary", boundary)
    await reconcile(ReceiptStore(), DEFAULTS, send)
    await reconcile(ReceiptStore(), DEFAULTS, send)
    assert send.call_count == (0 if second is False else 1)
    if second is None:
        assert store.get(receipt.key)["notice"] == "sent"
        assert store.get(receipt.key)["notice_attempts"] == prior_attempts + 1
        assert ("single recovery notice" in send.call_args.args[1]) == bool(prior_attempts)
    else:
        with store.connect() as db:
            assert db.execute("SELECT count(*) FROM transitions WHERE receipt_id=? AND stage='closed_boundary'",
                              (receipt.key,)).fetchone()[0] == 1


@pytest.mark.parametrize("unknown", [False, True])
def test_parallel_notice_pollers_share_atomic_budget(route, monkeypatch, unknown):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, local
    import gateway.wake_monitor as monitor
    runner, adapter, source, entry, store = route
    receipt = store.prepare("parallel", source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    store.transition(receipt.key, "admitted")
    with store.connect() as db:
        db.execute("UPDATE receipts SET updated=0")
    gate, calls = Barrier(4), local()

    def ownership(*args):
        calls.count = getattr(calls, "count", 0) + 1
        if calls.count == 1:
            gate.wait(timeout=10)  # all pollers hold the same stale row
            return True
        return None if unknown else True

    monkeypatch.setattr(monitor, "boundary", ownership)
    sent = []

    async def send(route, text):
        sent.append(text)
        return {"success": True}

    def poll(_):
        return asyncio.run(reconcile(ReceiptStore(), DEFAULTS, send))

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(poll, range(4)))
    row = store.get(receipt.key)
    assert len(sent) == row["notice_attempts"] == (0 if unknown else 1)
    monkeypatch.setattr(monitor, "boundary", boundary)
    for _ in range(3):
        poll(None)
    assert len(sent) == store.get(receipt.key)["notice_attempts"] == 1
    assert store.get(receipt.key)["notice"] == "sent"


@pytest.mark.asyncio
async def test_progress_not_elapsed_time_controls_stall(route):
    runner, adapter, source, entry, store = route
    receipt = store.prepare("long", source, entry.session_key, entry.session_id)
    assert boundary(store, store.get(receipt.key)), (store.home, entry.to_dict(), runner.session_store.sessions_dir)
    store.claim_admission(receipt.key)
    receipt("execution", session_key=entry.session_key, session_id=entry.session_id, generation=7)
    with store.connect() as db:
        db.execute("UPDATE receipts SET created=?,updated=0", (time.time() - 7200,))
    receipt("progress")
    send = AsyncMock(return_value={"success": True})
    assert await reconcile(store, DEFAULTS, send) == []
    with store.connect() as db:
        db.execute("UPDATE receipts SET activity=0")
    assert len(await reconcile(store, DEFAULTS, send)) == 1
    assert send.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_idle_child_completion_reaches_origin_without_user_input(route, monkeypatch, failure):
    from tools import async_delegation as delegation
    from tools.process_registry import process_registry
    import queue
    runner, adapter, source, entry, store = route
    monkeypatch.setattr(process_registry, "completion_queue", queue.Queue())
    visible = asyncio.Event()
    original_send = adapter.send

    async def send(chat_id, content, **kwargs):
        result = await original_send(chat_id, content, **kwargs)
        if "Supervisor final" in content:
            visible.set()
        return result

    adapter.send = send
    runner._run_agent = AsyncMock(return_value={"final_response": "Supervisor final", "messages": [], "api_calls": 1})

    def child():
        if failure:
            raise RuntimeError("synthetic child failure")
        return {"summary": "synthetic child completion", "status": "completed"}

    runner._running = True
    watcher = asyncio.create_task(runner._async_delegation_watcher(interval=0.05))
    try:
        handle = delegation.dispatch_async_delegation(goal="test", context=None, toolsets=None,
            role="leaf", model=None, session_key=entry.session_key, parent_session_id=entry.session_id,
            runner=child)
        assert handle["status"] == "dispatched"
        await asyncio.wait_for(visible.wait(), 15)
        await drain(adapter)
        rows = store.rows()
        assert len(rows) == 1 and rows[0]["state"] == "delivered"
        assert rows[0]["session_id"] == entry.session_id
        assert rows[0]["generation"] is not None
        assert runner._run_agent.call_count == 1
    finally:
        runner._running = False
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        await drain(adapter)


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["send_failure", "silent", "unknown", "new", "profile"])
async def test_explicit_dispositions_never_borrow_later_reply(route, disposition):
    runner, adapter, source, entry, store = route
    if disposition == "new":
        runner.session_store.suspend_session(entry.session_key)
    elif disposition == "profile":
        source.profile = "other"
    elif disposition == "send_failure":
        adapter.send = AsyncMock(return_value=SendResult(success=False, error="fake-secret-not-recorded"))
    reply = "NO_REPLY" if disposition == "silent" else "Final"
    runner._run_agent = AsyncMock(return_value={"final_response": reply, "messages": [], "api_calls": 1})
    if disposition == "unknown":
        adapter.set_message_handler(AsyncMock(return_value=None))
    await deliver_wake(adapter, text="child", source=source, session_id=entry.session_id, identity=disposition)
    await drain(adapter)
    expected = {"send_failure": "failed", "silent": "suppressed", "unknown": "uncertain",
                "new": "closed_boundary", "profile": "closed_boundary"}[disposition]
    assert store.rows()[0]["state"] == expected
    if disposition in {"new", "profile"}:
        runner._run_agent.assert_not_called()
    assert "fake-secret" not in store.path.read_bytes().decode(errors="replace")


@pytest.mark.asyncio
async def test_refused_admission_gets_only_one_reconciliation_attempt(route):
    runner, adapter, source, entry, store = route
    handler = adapter._message_handler
    adapter.set_message_handler(None)
    from gateway.wake import WakeNotAccepted
    for _ in range(2):
        with pytest.raises(WakeNotAccepted):
            await deliver_wake(adapter, text="child", source=source, session_id=entry.session_id, identity="refused")
    adapter.set_message_handler(handler)
    runner._run_agent = AsyncMock()
    await deliver_wake(adapter, text="child", source=source, session_id=entry.session_id, identity="refused")
    await drain(adapter)
    runner._run_agent.assert_not_called()
    assert store.rows()[0]["attempts"] == 2


def test_crash_recovery_does_not_replay_wake_but_preserves_human_recovery(route):
    from gateway.session import SessionStore
    runner, adapter, source, entry, store = route
    key = entry.session_key
    runner.session_store.mark_turn_active(key, replay_allowed=False)
    restarted = SessionStore(runner.config.sessions_dir, runner.config)
    restarted._db = runner.session_store._db
    assert restarted.recover_interrupted_turns() == 0
    assert restarted.suspend_recently_active() == 0
    assert not restarted.mark_resume_pending(key)
    restarted.mark_turn_active(key)  # a real new turn explicitly owns the route
    assert restarted.recover_interrupted_turns() == 1


def test_independent_consumer_crash_restart_and_dedup(route):
    import os
    import subprocess
    import sys
    runner, adapter, source, entry, store = route
    receipt = store.prepare("lost-gateway", source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    with store.connect() as db:
        db.execute("UPDATE receipts SET updated=0")
    program = '''
import asyncio, json, os
from pathlib import Path
from gateway.wake_receipts import ReceiptStore, DEFAULTS
from gateway.wake_monitor import reconcile
async def send(route, text):
    if os.environ.get("TEST_CRASH") == "1":
        os._exit(23)
    with (Path(os.environ["HERMES_HOME"]) / "wire.jsonl").open("a") as f:
        f.write(json.dumps({"route":route,"text":text}) + "\\n")
    return {"success": True}
print(asyncio.run(reconcile(ReceiptStore(), DEFAULTS, send)))
'''
    env = {**os.environ, "HERMES_HOME": str(store.home), "TEST_CRASH": "1"}
    result = subprocess.run([sys.executable, "-c", program], env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 23, result.stderr
    assert store.get(receipt.key)["notice"] == "uncertain"
    assert store.get(receipt.key)["notice_attempts"] == 1
    with store.connect() as db:
        db.execute("UPDATE receipts SET notice_at=0")
    env["TEST_CRASH"] = "0"
    for _ in range(2):
        result = subprocess.run([sys.executable, "-c", program], env=env, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
    lines = (store.home / "wire.jsonl").read_text().splitlines()
    assert len(lines) == 1 and "single recovery notice" in lines[0]
    assert store.get(receipt.key)["notice"] == "sent"
    assert not store.claim_admission(receipt.key)


@pytest.mark.asyncio
async def test_standalone_uses_guarded_native_transport(route, monkeypatch):
    from gateway.wake_monitor import standalone_send
    from gateway.wake_receipts import route_of
    from types import SimpleNamespace
    import gateway.config
    import tools.send_message_tool as transport
    runner, adapter, source, entry, store = route
    monkeypatch.setattr(gateway.config, "load_gateway_config", lambda: object())
    monkeypatch.setattr(transport, "_resolve_platform_config",
                        lambda platform, config: (platform, SimpleNamespace(token=None), None, None))
    guard = []
    monkeypatch.setattr(transport, "_authorize_relay_target", lambda *args, **kw: guard.append(args))
    send = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(transport, "_send_to_platform", send)
    assert await standalone_send(route_of(source), "failure notice") == {"success": True}
    assert guard == [(source.platform.value, source.chat_id, source.thread_id or None)]
    assert send.call_args.args[2:] == (source.chat_id, "failure notice")
    monkeypatch.setattr(transport, "_authorize_relay_target", lambda *args, **kw: "denied")
    assert await standalone_send(route_of(source), "blocked") == {"success": False}
    assert send.call_count == 1


def test_finite_module_entrypoint_with_empty_private_home(route):
    import os
    import subprocess
    import sys
    store = route[-1]
    result = subprocess.run([sys.executable, "-m", "gateway.wake_monitor"],
                            env={**os.environ, "HERMES_HOME": str(store.home)},
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"enabled": True, "notices": []}


def test_expiry_preserves_replay_tombstone(route):
    runner, adapter, source, entry, store = route
    receipt = store.prepare("expired", source, entry.session_key, entry.session_id)
    store.claim_admission(receipt.key)
    with store.connect() as db:
        db.execute("UPDATE receipts SET created=0")
    store.expire(30)
    row = store.get(receipt.key)
    assert row["state"] == "expired" and row["route_json"] == "{}"
    assert row["session_id"] == row["session_key"] == ""
    duplicate = store.prepare("expired", source, entry.session_key, entry.session_id)
    assert duplicate.key == receipt.key
    assert not store.claim_admission(duplicate.key)


@pytest.mark.parametrize("key,value", [("enabled", "false"), ("idle_seconds", float("inf")),
                                       ("admission_seconds", float("nan")), ("batch_size", 0.5)])
def test_invalid_bounds_fail_closed(monkeypatch, key, value):
    import hermes_cli.config_effective
    from gateway.wake_receipts import settings
    monkeypatch.setattr(hermes_cli.config_effective, "load_user_config_effective",
                        lambda **kwargs: {"gateway": {"wake_outcomes": {key: value}}})
    with pytest.raises(ValueError):
        settings()


@pytest.mark.asyncio
async def test_unknown_origin_cannot_starve_later_failure(route):
    runner, adapter, source, entry, store = route
    missing = store.prepare("missing", source, entry.session_key, "")
    failed = store.prepare("failed", source, entry.session_key, entry.session_id)
    failed("failed")
    with store.connect() as db:
        db.execute("UPDATE receipts SET updated=0 WHERE id=?", (missing.key,))
    send = AsyncMock(return_value={"success": True})
    cfg = {**DEFAULTS, "batch_size": 1}
    assert await reconcile(store, cfg, send) == []
    assert await reconcile(store, cfg, send) == [{"receipt": failed.key, "notice": "sent"}]
    assert send.call_count == 1
