"""Finite independent wake-outcome consumer: python -m gateway.wake_monitor.

Runs without a model or gateway. A private durable claim precedes every notice;
uncertain network sends are never blindly retried. Run under a systemd timer,
NOT as a gateway child process. Uses the same reconciliation function in-loop.
"""
from __future__ import annotations

import asyncio
import json
import time

from gateway.wake_receipts import ReceiptStore, boundary, settings


async def reconcile(store, cfg, send):
    """Return notice states, never re-execute an agent turn or tool side effect."""
    now = time.time()
    from gateway.wake_delivery import recover_settled
    recover_settled(store)
    store.expire(cfg["retention_days"])
    results = []
    for row in store.rows(pending=True, limit=int(cfg["batch_size"])):
        # Fair finite batches: unknown origins and healthy turns must not starve
        # later failed receipts forever. This timestamp is not agent activity.
        with store.connect() as db:
            db.execute("UPDATE receipts SET checked=? WHERE id=?", (now, row["id"]))
        if row["notice"] == "uncertain" and now < row["notice_at"] + cfg["send_seconds"] + 30:
            continue
        deadline = row["activity"] + cfg["idle_seconds"] if row["state"] == "executing" else row["updated"] + cfg["admission_seconds"]
        if row["state"] not in {"failed", "uncertain"} and now < deadline:
            continue
        verdict = boundary(store, row)
        if verdict is not True:
            if verdict is False:
                store.transition(row["id"], "closed_boundary")
            continue
        with store.connect() as db:
            # Reserve under the SQLite writer lock, but commit an attempt only
            # after the final ownership check. Unknown ownership rolls back the
            # reservation, preserving any prior uncertain send and its budget.
            claimed_at = time.time()
            claimed = db.execute("UPDATE receipts SET notice='uncertain',notice_at=?,notice_attempts=notice_attempts+1 "
                                 "WHERE id=? AND notice_attempts=? AND notice_attempts<2 "
                                 "AND (notice='' OR (notice='uncertain' AND notice_at<=?)) "
                                 "AND state NOT IN ('delivered','suppressed','cancelled','closed_boundary','expired')",
                                 (claimed_at, row["id"], row["notice_attempts"],
                                  claimed_at - cfg["send_seconds"] - 30)).rowcount
            if not claimed:
                continue
            verdict = boundary(store, row)
            if verdict is None:
                db.rollback()
                continue
            if verdict is False:
                db.execute("UPDATE receipts SET state='closed_boundary',updated=?,activity=?,"
                           "notice='closed_boundary',notice_at=?,notice_attempts=? WHERE id=?",
                           (claimed_at, claimed_at, row["notice_at"], row["notice_attempts"], row["id"]))
                db.execute("INSERT INTO transitions VALUES(?,'closed_boundary',?)", (row["id"], claimed_at))
                results.append({"receipt": row["id"], "notice": "closed_boundary"})
                continue
            # No SQLite transaction is held across the external transport. A
            # crash after commit remains uncertain, bounded by one recovery.

        # Never include session identifiers, raw events, errors, or model output.
        text = (f"Supervisor wake {row['id'][:12]} has no verified final reply "
                f"(last observed: {row['state']}). No turn or side effect was replayed. "
                "Operator review is required; delivery may be uncertain.")
        if row["notice_attempts"]:
            text += " A prior failure-notice send has uncertain delivery; this is the single recovery notice."
        state = "uncertain"
        try:
            result = await asyncio.wait_for(send(json.loads(row["route_json"]), text), cfg["send_seconds"])
            if isinstance(result, dict):
                success = result.get("success") is True and result.get("delivered") is not False
            else:
                success = getattr(result, "success", None) is True
            state = "sent" if success else "failed"
        except Exception:
            # Network exceptions contain secrets on some adapters. Keep them out
            # of receipts and stdout, and do not retry a possibly accepted send.
            state = "uncertain"
        with store.connect() as db:
            db.execute("UPDATE receipts SET notice=? WHERE id=? AND notice='uncertain' "
                       "AND notice_attempts=? AND notice_at=?",
                       (state, row["id"], row["notice_attempts"] + 1, claimed_at))
        results.append({"receipt": row["id"], "notice": state})
    return results


async def standalone_send(route, text):
    """Reuse the native standalone send rail without transcript mirroring.

    No fallback to home channels, no guessed destination, no model turn. Profile
    env/secrets must be loaded by the same external launcher as the gateway.
    """
    from hermes_cli.profiles import get_active_profile_name
    if (route.get("profile") or "default") != get_active_profile_name():
        return {"success": False}
    from gateway.config import load_gateway_config
    from tools.send_message_tool import _resolve_platform_config, _send_to_platform, _authorize_relay_target
    config = load_gateway_config()
    platform, pconfig, _entry, error = _resolve_platform_config(route["platform"], config)
    if error or not route["chat_id"]:
        return {"success": False}
    denied = _authorize_relay_target(route["platform"], route["chat_id"], route["thread_id"] or None,
                                     native_token=getattr(pconfig, "token", None))
    if denied:
        return {"success": False}
    return await _send_to_platform(platform, pconfig, route["chat_id"], text,
                                   thread_id=route["thread_id"] or None)


async def poll_runner(runner):
    from gateway.session import SessionSource
    from gateway.kanban_watchers_notifier import _adapter_for_subscription
    from gateway.config import Platform
    cfg = settings()
    if not cfg["enabled"]:
        return []

    async def send(route, text):
        source = SessionSource.from_dict(route)
        sub = {**route, "delivery_metadata": route}
        adapter = _adapter_for_subscription(runner, Platform(route["platform"]), sub, route["profile"] or None)
        if adapter is None:
            return {"success": False}
        return await adapter.send(source.chat_id, text, metadata=runner._thread_metadata_for_source(source))

    return await reconcile(ReceiptStore(), cfg, send)


async def main():
    cfg = settings()
    if not cfg["enabled"]:
        print(json.dumps({"enabled": False}))
        return 0
    results = await reconcile(ReceiptStore(), cfg, standalone_send)
    print(json.dumps({"enabled": True, "notices": results}))
    return int(any(row["notice"] in {"failed", "uncertain"} for row in results))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
