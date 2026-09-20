#!/usr/bin/env python3
"""Coordinator-only one-shot canary producer and read-only receipt verifier.

No transport/model code here. prepare emits one native Kanban completed event;
verify waits for its exact correlated transport ACK. Never run prepare on retries.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
from contextlib import closing
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def prepare(home, db_path, session_id, board):
    from gateway.wake_receipts import ReceiptStore, boundary, ROUTE_FIELDS, settings
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
    if not settings()['enabled']:
        raise RuntimeError('observation is disabled')
    with closing(sqlite3.connect((home / 'state.db').as_uri() + '?mode=ro', uri=True)) as db:
        entries = db.execute('SELECT session_key,entry_json FROM gateway_routing').fetchall()
    matches = [(key, json.loads(raw)) for key, raw in entries if json.loads(raw).get('session_id') == session_id]
    if len(matches) != 1:
        raise RuntimeError('original session must have exactly one current routing entry')
    key, entry = matches[0]
    origin = entry.get('origin') or {}
    if (origin.get('platform') != 'telegram' or str(origin.get('thread_id')) != '16755'
            or origin.get('profile') not in (None, '', 'default')):
        raise RuntimeError('only the already-authorized default Telegram thread16755 is permitted')
    route = {name: str(origin.get(name) or '') for name in ROUTE_FIELDS}
    store = ReceiptStore(home)
    if boundary(store, {'session_id': session_id, 'session_key': key, 'route_json': canonical(route)}) is not True:
        raise RuntimeError('origin boundary is not positively verified')
    # Durable exclusive marker BEFORE the first board write. A crash means inspect,
    # not rerun. The source task permits at most one later live canary.
    marker = home / 'wake_outcomes' / 'canary-t_7b7eabfa.json'
    with marker.open('x') as out:
        out.write(canonical({'state': 'preparing', 'board': board}))
    conn = kbc.connect(db_path, board=board)
    try:
        tid = kb.create_task(conn, title='SUPERVISION TEST — single no-inbound wake canary',
                            body='Inert canary; no worker and no infrastructure changes.',
                            assignee=None, created_by='default', initial_status='blocked',
                            idempotency_key='source-wake-canary-t_7b7eabfa',
                            session_id=session_id, board=board, completion_contract='local-only')
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id=route['chat_id'],
                           thread_id=route['thread_id'], user_id=route['user_id'],
                           chat_type=route['chat_type'], notifier_profile='default', delivery_mode='wake',
                           delivery_metadata={**origin, 'origin_session_id': session_id})
        if not kb.complete_task(conn, tid, summary='SUPERVISION TEST: reply once with SUPERVISION_CANARY_OK. No tools or follow-up work are requested.',
                                fire_lifecycle_hook=False):
            raise RuntimeError('native completion refused; inspect the reserved marker; do not retry')
        event_id = conn.execute("SELECT id FROM task_events WHERE task_id=? AND kind='completed' ORDER BY id DESC LIMIT 1", (tid,)).fetchone()[0]
    finally:
        conn.close()
    identity = ('kanban', board, tid, event_id)
    payload = {'state': 'emitted', 'board': board, 'task': tid, 'event': event_id,
               'identity_sha256': hashlib.sha256(canonical(identity).encode()).hexdigest()}
    marker.write_text(canonical(payload))
    return payload


def verify(home, marker, timeout):
    identity = json.loads(marker.read_text())['identity_sha256']
    deadline = time.monotonic() + timeout
    while True:
        path = home / 'wake_outcomes' / 'receipts.sqlite3'
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            rows = db.execute('SELECT id,state,generation,attempts FROM receipts WHERE event_json=?',
                              (canonical({'identity_sha256': identity}),)).fetchall()
        if len(rows) == 1 and rows[0][1] == 'delivered' and rows[0][2] is not None and rows[0][3] == 1:
            print(canonical({'verified_final': True, 'receipt': rows[0][0], 'attempts': 1}))
            return 0
        if time.monotonic() >= deadline:
            print(canonical({'verified_final': False, 'states': [r[1] for r in rows]}))
            return 1
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['prepare', 'verify'])
    p.add_argument('--home', type=Path, required=True)
    p.add_argument('--db', type=Path)
    p.add_argument('--session-id')
    p.add_argument('--board', default='default')
    p.add_argument('--authorize-one-live-canary', action='store_true')
    p.add_argument('--timeout', type=float, default=900)
    args = p.parse_args()
    home = args.home.resolve()
    if args.mode == 'prepare':
        from hermes_constants import get_hermes_home
        from hermes_cli.profiles import get_active_profile_name
        if (get_active_profile_name() != 'default' or not args.authorize_one_live_canary or
                not args.db or not args.session_id or home != get_hermes_home().resolve()):
            p.error('prepare requires explicit authorization, --db, --session-id and the active default home')
        print(canonical(prepare(home, args.db.resolve(), args.session_id, args.board)))
        return 0
    return verify(home, home / 'wake_outcomes' / 'canary-t_7b7eabfa.json', max(0, args.timeout))


if __name__ == '__main__':
    raise SystemExit(main())
