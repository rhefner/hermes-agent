"""Correlate observed wakes with the existing final-only delivery ledger.

No resend implementation: the native delivery ledger owns all final retries.
A reserved obligation prefix makes a missing binding fail closed after a crash.
"""
from pathlib import Path
from contextlib import closing
import sqlite3

from hermes_constants import get_hermes_home

PREFIX = "wake-"


def bind(obligation_id, receipt):
    from gateway.delivery_ledger import _transaction
    with _transaction() as db:
        db.execute("CREATE TABLE IF NOT EXISTS wake_delivery_bindings ("
                   "obligation_id TEXT NOT NULL, home TEXT NOT NULL, receipt_id TEXT NOT NULL, "
                   "PRIMARY KEY(obligation_id,home,receipt_id))")
        db.execute("INSERT OR IGNORE INTO wake_delivery_bindings VALUES(?,?,?)",
                   (obligation_id, str(receipt.store.home), receipt.key))
    with receipt.store.connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS final_links (receipt_id TEXT, ledger_home TEXT, "
                   "obligation_id TEXT, PRIMARY KEY(receipt_id,ledger_home,obligation_id))")
        db.execute("INSERT OR IGNORE INTO final_links VALUES(?,?,?)",
                   (receipt.key, str(get_hermes_home()), obligation_id))


def recover_settled(store, limit=100):
    """Read the native ACK after a crash between ledger ACK and observer update."""
    try:
        with store.connect() as db:
            links = db.execute("SELECT l.receipt_id,l.ledger_home,l.obligation_id FROM final_links l "
                               "JOIN receipts r ON r.id=l.receipt_id WHERE r.state IN ('executing','failed','uncertain') "
                               "ORDER BY r.checked,r.updated LIMIT ?", (limit,)).fetchall()
    except sqlite3.OperationalError:
        return
    for key, home, oid in links:
        try:
            path = (Path(home) / 'state.db').resolve()
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
                row = db.execute("SELECT state FROM delivery_obligations WHERE obligation_id=?", (oid,)).fetchone()
            if row and row[0] == 'delivered':
                store.transition(key, 'delivered')
        except (sqlite3.Error, OSError):
            continue


def bindings(obligation_id):
    path = get_hermes_home() / "state.db"
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            return db.execute("SELECT home,receipt_id FROM wake_delivery_bindings WHERE obligation_id=?",
                              (obligation_id,)).fetchall()
    except sqlite3.Error:
        return []


def recovery_allowed(row):
    """Called just before the native recovery transport, including when disabled.

    Disabling new observation cannot erase an already-admitted origin boundary.
    """
    oid = row["obligation_id"]
    if not oid.startswith(PREFIX):
        return True
    from gateway.wake_receipts import ReceiptStore, boundary
    owners = bindings(oid)
    if not owners:
        return False
    for home, key in owners:
        if not (Path(home) / "wake_outcomes" / "receipts.sqlite3").is_file():
            return False
        store = ReceiptStore(home)
        receipt = store.get(key)
        if receipt is None or boundary(store, receipt) is not True:
            return False
    return True


def settle(obligation_id):
    """A positively acknowledged native FINAL can settle only its bound wake."""
    if not obligation_id.startswith(PREFIX):
        return
    from gateway.wake_receipts import ReceiptStore
    for home, key in bindings(obligation_id):
        store = ReceiptStore(home)
        store.transition(key, "delivered")
