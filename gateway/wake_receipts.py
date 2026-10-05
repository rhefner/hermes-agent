"""Private, versioned wake outcome ledger; admission is deliberately not delivery.

No prompts, tool results, exception strings, credentials or response bodies are
stored. An admission claim is committed BEFORE calling a transport: a crash in
that window is uncertain and MUST NOT be replayed. Only an explicit refusal can
be retried, once, by the original producer's existing admission path.
"""
from __future__ import annotations

import hashlib
from contextlib import contextmanager, closing
import json
import math
import os
from pathlib import Path
import sqlite3
import time

from hermes_constants import get_hermes_home
from hermes_cli.config_defaults import DEFAULT_CONFIG

try:
    from yaml import YAMLError as _PyYAMLError
    YAML_ERRORS: tuple = (_PyYAMLError,)
except ImportError:  # gateway runtime python ships no PyYAML (config uses the internal reader);
    YAML_ERRORS = ()  # the marker can then never be raised here.

ROUTE_FIELDS = ("platform", "chat_id", "chat_type", "thread_id", "user_id", "user_id_alt",
                "scope_id", "parent_chat_id", "profile")
TERMINAL = {"delivered", "suppressed", "cancelled", "closed_boundary", "failed", "uncertain", "expired"}
DEFAULTS = dict(DEFAULT_CONFIG["gateway"]["wake_outcomes"])


def settings():
    from hermes_cli.config_effective import load_user_config_effective
    cfg = load_user_config_effective(fail_closed=True)
    values = {**DEFAULTS, **((cfg.get("gateway") or {}).get("wake_outcomes") or {})}
    if not isinstance(values["enabled"], bool):
        raise ValueError("gateway.wake_outcomes.enabled must be boolean")
    for name in DEFAULTS.keys() - {"enabled"}:
        if (isinstance(values[name], bool) or not isinstance(values[name], (float, int))
                or not math.isfinite(values[name]) or values[name] <= 0):
            raise ValueError(f"gateway.wake_outcomes.{name} must be positive")
    if int(values["batch_size"]) != values["batch_size"]:
        raise ValueError("gateway.wake_outcomes.batch_size must be an integer")
    return values


def route_of(source):
    return {name: str(getattr(value, "value", value) or "")
            for name in ROUTE_FIELDS for value in [getattr(source, name, None)]}


def same_route(left, right):
    # The canonical default profile also has legacy NULL/empty spellings.
    return ({**left, "profile": left.get("profile") or "default"} ==
            {**right, "profile": right.get("profile") or "default"})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class ReceiptStore:
    def __init__(self, home=None):
        self.home = Path(home or get_hermes_home()).resolve()
        directory = self.home / "wake_outcomes"
        directory.mkdir(mode=0o700, parents=False, exist_ok=True)
        if directory.is_symlink() or directory.stat().st_mode & 0o077:
            raise PermissionError("wake_outcomes must be a private real directory")
        self.path = directory / "receipts.sqlite3"
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        if self.path.stat().st_mode & 0o077:
            raise PermissionError("wake receipt database must be private")
        with self.connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("unsupported wake receipt version")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS receipts (
                    id TEXT PRIMARY KEY, event_json TEXT NOT NULL, route_json TEXT NOT NULL,
                    session_key TEXT NOT NULL, session_id TEXT NOT NULL,
                    state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL, activity REAL NOT NULL,
                    generation INTEGER, notice TEXT NOT NULL DEFAULT '', notice_at REAL,
                    checked REAL NOT NULL DEFAULT 0,
                    notice_attempts INTEGER NOT NULL DEFAULT 0,
                    CHECK (attempts >= 0 AND attempts <= 2));
                CREATE TABLE IF NOT EXISTS transitions (
                    receipt_id TEXT NOT NULL, stage TEXT NOT NULL, at REAL NOT NULL);
                PRAGMA user_version=1;
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def prepare(self, identity, source, session_key, session_id):
        route = route_of(source)
        # Hash identities rather than arbitrary producer-controlled strings.
        event = hashlib.sha256(canonical(identity).encode()).hexdigest()
        key = hashlib.sha256(canonical([event, route, session_key, session_id]).encode()).hexdigest()
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            # Event identity, not its mutable destination, is the replay barrier.
            # Also recognizes historical v1 IDs without rewriting their origin.
            prior = db.execute("SELECT id FROM receipts WHERE event_json=? ORDER BY created LIMIT 1",
                               (canonical({"identity_sha256": event}),)).fetchone()
            if prior is not None:
                return Receipt(self, prior[0])
            db.execute("INSERT OR IGNORE INTO receipts(id,event_json,route_json,session_key,session_id,"
                       "state,created,updated,activity) VALUES(?,?,?,?,?,'never_admitted',?,?,?)",
                       (key, canonical({"identity_sha256": event}), canonical(route), session_key, session_id, now, now, now))
        return Receipt(self, key)

    def get(self, key):
        with self.connect() as db:
            row = db.execute("SELECT * FROM receipts WHERE id=?", (key,)).fetchone()
            return dict(row) if row else None

    def transition(self, key, stage, *, generation=None):
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM receipts WHERE id=?", (key,)).fetchone()
            if row is None or (row[0] in TERMINAL and not (row[0] in {"failed", "uncertain"} and stage in {"delivered", "closed_boundary"})):
                return
            db.execute("UPDATE receipts SET state=?,updated=?,activity=?,generation=COALESCE(?,generation) WHERE id=?",
                       (stage, now, now, generation, key))
            db.execute("INSERT INTO transitions VALUES(?,?,?)", (key, stage, now))

    def claim_admission(self, key):
        now = time.time()
        with self.connect() as db:
            # uncertain admission is an absorbing replay barrier, even if the
            # gateway died before its adapter could acknowledge queueing.
            result = db.execute("UPDATE receipts SET state='admitting',attempts=attempts+1,updated=? "
                                "WHERE id=? AND state='never_admitted' AND attempts<2", (now, key))
            if result.rowcount:
                db.execute("INSERT INTO transitions VALUES(?, 'admitting', ?)", (key, now))
            return bool(result.rowcount)

    def activity(self, key):
        with self.connect() as db:
            db.execute("UPDATE receipts SET activity=? WHERE id=? AND state='executing'", (time.time(), key))

    def rows(self, *, pending=False, limit=100):
        with self.connect() as db:
            where = " WHERE notice IN ('','uncertain') AND notice_attempts<2 AND state NOT IN ('delivered','suppressed','cancelled','closed_boundary')" if pending else ""
            order = "checked,updated" if pending else "updated"
            return [dict(row) for row in db.execute("SELECT * FROM receipts" + where + f" ORDER BY {order} LIMIT ?", (limit,))]

    def expire(self, days):
        # Keep an identity tombstone forever: deleting it would re-authorize a
        # duplicate producer event. Routing PII and stage history expire.
        with self.connect() as db:
            cutoff = time.time() - days * 86400
            db.execute("DELETE FROM transitions WHERE receipt_id IN (SELECT id FROM receipts WHERE created<?)", (cutoff,))
            db.execute("UPDATE receipts SET state='expired',notice='expired',route_json='{}',session_id='',session_key='' "
                       "WHERE created<? AND state!='expired'", (cutoff,))


def origin_target(store, row):
    """Read-only, tri-state boundary: verified tip id, False (closed), None (unknown).

    Original identity is never rewritten. Only the native compression chain can
    authorize a descendant; reset/branch/delegate children cannot redirect a wake.
    """
    if not row["session_id"] or not row["session_key"]:
        return None
    try:
        from hermes_state import SessionDB
        db = SessionDB(store.home / "state.db", read_only=True)
        try:
            route = json.loads(row["route_json"])
            session = db.get_session(row["session_id"])
            if session is None:
                return None
            chain = db.get_compression_chain(row["session_id"])
            target = chain[-1]
            for sid in chain:
                member = db.get_session(sid)
                if not member or (member.get("profile_name") or route["profile"] or "default") != (route["profile"] or "default"):
                    return False
            tip = db.get_session(target)
            if tip.get("ended_at") is not None:
                return False
            with closing(sqlite3.connect((store.home / "state.db").as_uri() + "?mode=ro", uri=True, timeout=5)) as conn:
                entries = conn.execute("SELECT entry_json FROM gateway_routing WHERE session_key=?", (row["session_key"],)).fetchall()
            if len(entries) != 1:
                return None
            entry = json.loads(entries[0][0])
            origin = entry.get("origin") or {}
            current = {name: str(origin.get(name) or "") for name in ROUTE_FIELDS}
            if entry.get("session_id") != target or entry.get("suspended") or not same_route(current, route):
                return False
            # Durable route snapshots alone cannot detect a config-only reroute.
            from hermes_cli.config_effective import load_user_config_effective
            from gateway.profile_routing import parse_profile_routes
            config = load_user_config_effective(store.home / "config.yaml", fail_closed=True)
            for rule in parse_profile_routes((config.get("gateway") or {}).get("profile_routes") or []):
                if rule.matches(route["platform"], guild_id=route["scope_id"] or None,
                                chat_id=route["chat_id"], thread_id=route["thread_id"] or None,
                                parent_chat_id=route["parent_chat_id"] or None, user_id=route["user_id"] or None):
                    if rule.profile != (route["profile"] or "default"):
                        return False
                    break
            return target
        finally:
            db.close()
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError, *YAML_ERRORS):
        return None


def boundary(store, row):
    target = origin_target(store, row)
    return True if isinstance(target, str) and target else target


class Receipt:
    def __init__(self, store, key):
        self.store, self.key = store, key
        self._last_activity_written = 0.0

    def __call__(self, stage, **facts):
        if stage == "finalizing":
            from gateway.wake_delivery import bind
            bind(facts["obligation_id"], self)
            return
        if stage == "activity":
            stamp = facts.get("at")
            if isinstance(stamp, (int, float)) and stamp > self._last_activity_written + 60:
                self._last_activity_written = stamp
                with self.store.connect() as db:
                    db.execute("UPDATE receipts SET activity=MAX(activity,?) WHERE id=? AND state='executing'",
                               (min(stamp, time.time()), self.key))
            return
        if stage == "progress":
            self.store.activity(self.key)
            return
        if stage in {"execution", "resolve", "before_delivery"}:
            row = self.store.get(self.key)
            target = origin_target(self.store, row)
            if (not isinstance(target, str) or
                    (stage != "before_delivery" and
                     (facts.get("session_id") != target or facts.get("session_key") != row["session_key"]))):
                self.store.transition(self.key, "closed_boundary" if target is False else "uncertain")
                from gateway.event_outcome import WakeBoundaryClosed
                raise WakeBoundaryClosed("wake origin no longer authorized")
            if stage == "execution":
                self.store.transition(self.key, "executing", generation=facts.get("generation"))
        elif stage in TERMINAL:
            self.store.transition(self.key, stage)


def record_closed_completion(runner, evt):
    from gateway.wake import adapter_supports_push
    if not settings()["enabled"]:
        return
    source = runner._build_process_event_source(evt)
    if source is None:
        return
    adapter = runner._resolve_injection_adapter(source.platform.value, source)
    if adapter is None or not adapter_supports_push(adapter):
        return
    receipt = ReceiptStore().prepare(runner._completion_delivery_identity(evt), source,
                                     evt.get("session_key", ""), evt.get("parent_session_id", ""))
    receipt("closed_boundary")


async def admit(adapter, event, receipts):
    from gateway.wake import WakeNotAccepted
    receipts = list({receipt.key: receipt for receipt in receipts}.values())
    if not receipts:
        raise ValueError("wake requires immutable event identities")
    store = receipts[0].store
    rows = [store.get(receipt.key) for receipt in receipts]
    if any(not same_route(json.loads(row["route_json"]), route_of(event.source)) for row in rows):
        # A repeated producer cannot rebind an immutable event to a new route.
        return
    verdicts = [boundary(store, row) for row in rows]
    if not all(verdicts):
        for receipt, verdict in zip(receipts, verdicts):
            store.transition(receipt.key, "closed_boundary" if verdict is False else "uncertain")
        return
    claimed = [receipt for receipt in receipts if store.claim_admission(receipt.key)]
    if len(claimed) != len(receipts):
        # A changed/coalesced batch includes a previously admitted event. Do not
        # replay its text or side effects. New siblings get an operator outcome
        # notice instead of silently vanishing or running the whole batch twice.
        for receipt in claimed:
            store.transition(receipt.key, "uncertain")
        return

    def observe(stage, **facts):
        for receipt in receipts:
            receipt(stage, **facts)

    row = rows[0]
    event._outcome_observer = observe
    event.ledger_message_id = "wake:" + hashlib.sha256(canonical(sorted(r.key for r in receipts)).encode()).hexdigest()
    event._replay_on_restart = False
    event.metadata.update(gateway_session_key=row["session_key"], gateway_session_id=row["session_id"],
                          gateway_session_strict=True)
    try:
        event._gateway_accepted = False
        await adapter.handle_message(event)
    except BaseException:
        observe("uncertain")
        raise
    if event._gateway_accepted is not True:
        for receipt in receipts:
            store.transition(receipt.key, "never_admitted")
        raise WakeNotAccepted("internal wake not accepted by adapter")
    # The spawned task can already be executing: admission never overwrites it.
    with store.connect() as db:
        for receipt in receipts:
            db.execute("UPDATE receipts SET state='admitted',updated=? WHERE id=? AND state='admitting'",
                       (time.time(), receipt.key))
            db.execute("INSERT INTO transitions VALUES(?, 'admitted', ?)", (receipt.key, time.time()))
