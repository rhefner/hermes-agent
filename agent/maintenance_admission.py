"""Opt-in, durable maintenance admission for finite runtime work.

An initialized fleet-root directory enables accounting from process startup.
Closing admission and reserving work serialize in one SQLite transaction. A dead
worker is NOT a completed worker: its durable reservation blocks drain until an
operator investigates it. Expiry/reboot never opens admission. This is a runtime
coordination control, not a sandbox against same-user code or direct TCP clients.
"""
from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from hermes_constants import get_default_hermes_root

_parent: contextvars.ContextVar[tuple[Path, str] | None] = contextvars.ContextVar("maintenance_work", default=None)
_seen = set()
NOTICE = "Hermes maintenance admission is closed; use the external maintenance executor."


class AdmissionClosed(RuntimeError):
    pass


def directory():
    return get_default_hermes_root() / "maintenance-admission"


def identity(pid=None):
    pid = os.getpid() if pid is None else pid
    if type(pid) is not int or pid <= 1:
        raise AdmissionClosed('Invalid process identity')
    root = Path('/proc')
    tail = (root / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    if tail[0] in ('Z', 'X'):
        raise AdmissionClosed('Executor/process has exited')
    return {"pid": pid, "start": tail[19],
            "boot": (root / 'sys/kernel/random/boot_id').read_text().strip()}


def _safe(path, *, directory=False):
    st = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise AdmissionClosed("Unsafe maintenance admission storage")


@contextlib.contextmanager
def transaction(root=None):
    root = directory() if root is None else Path(root)
    _safe(root, directory=True)
    path = root / 'work.sqlite'
    _safe(path)
    conn = sqlite3.connect(f'file:{path}?mode=rw', uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN IMMEDIATE')
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize(root):
    """Offline enrollment only, before starting the instrumented runtime."""
    root = Path(root)
    root.mkdir(mode=0o700)  # refuse existing state, never reset a closed gate
    path = root / 'work.sqlite'
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    with transaction(root) as c:
        c.execute('CREATE TABLE control (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)')
        c.execute('CREATE TABLE work (id TEXT PRIMARY KEY, identity TEXT NOT NULL, kind TEXT NOT NULL)')
        c.execute('INSERT INTO control VALUES (1, ?)', (json.dumps({'version': 1, 'closed': False}),))


def _control(c):
    value = json.loads(c.execute('SELECT body FROM control WHERE id=1').fetchone()[0])
    if value.get('version') != 1 or type(value.get('closed')) is not bool:
        raise AdmissionClosed('Malformed maintenance control')
    return value


def _enabled():
    root = directory()
    # Once enrolled, deleting storage is an error, not an opt-out. A deployment
    # supervisor must also require this directory before EVERY process restart.
    try:
        root.lstat()
    except FileNotFoundError:
        if root in _seen:
            raise AdmissionClosed('Maintenance storage disappeared')
        return False
    _seen.add(root)
    return True


class Reservation:
    def __init__(self, root, key):
        self.root, self.key = root, key

    def finish(self):
        with transaction(self.root) as c:
            if c.execute('DELETE FROM work WHERE id=?', (self.key,)).rowcount != 1:
                raise AdmissionClosed('Maintenance work reservation lost')

    @contextlib.contextmanager
    def bound(self):
        token = _parent.set((self.root, self.key))
        try:
            yield
        finally:
            _parent.reset(token)


def reserve(kind):
    if not _enabled():
        return None
    root = directory()
    try:
        ident = identity()
        with transaction(root) as c:
            control = _control(c)
            parent = _parent.get()
            inherited = False
            if parent is not None and parent[0] == root:
                row = c.execute('SELECT identity FROM work WHERE id=?', (parent[1],)).fetchone()
                inherited = bool(row and json.loads(row[0]) == ident)
            executor = control.get('executor') == ident and time.time() < control.get('expires', 0)
            if control['closed'] and not inherited and not executor:
                raise AdmissionClosed(NOTICE)
            key = uuid.uuid4().hex
            c.execute('INSERT INTO work VALUES (?, ?, ?)', (key, json.dumps(ident), kind))
        return Reservation(root, key)
    except AdmissionClosed:
        raise
    except (OSError, sqlite3.Error, ValueError, TypeError, IndexError):
        raise AdmissionClosed('Maintenance accounting unavailable') from None


@contextlib.contextmanager
def work(kind):
    reservation = reserve(kind)
    if reservation is None:
        yield
        return
    with reservation.bound():
        try:
            yield
        finally:
            reservation.finish()


def tracked(kind, *, refused=None):
    """Admission is outside the function, before hooks, providers or side effects."""
    def decorate(fn):
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def asynchronous(*a, **kw):
                try:
                    with work(kind):
                        return await fn(*a, **kw)
                except AdmissionClosed:
                    if refused is None:
                        raise
                    return refused
            return asynchronous
        @functools.wraps(fn)
        def synchronous(*a, **kw):
            with work(kind):
                return fn(*a, **kw)
        return synchronous
    return decorate


def reserved_target(target, kind):
    """Reserve BEFORE scheduling detached work; invocation always releases it.

    Caller MUST call cancel() if submission/start fails. A target never scheduled
    deliberately remains outstanding: silently forgetting it would forge drain.
    """
    reservation = reserve(kind)
    if reservation is None:
        return target, lambda: None
    @functools.wraps(target)
    def run(*a, **kw):
        with reservation.bound():
            try:
                return target(*a, **kw)
            finally:
                reservation.finish()
    return run, reservation.finish


def close(root, *, owner, executor_pid, expires):
    if not isinstance(owner, str) or len(owner) != 32 or any(ch not in '0123456789abcdef' for ch in owner):
        raise AdmissionClosed('Invalid owner')
    now = time.time()
    if not now < expires <= now + 14400:
        raise AdmissionClosed('Invalid lease')
    executor = identity(executor_pid)
    with transaction(root) as c:
        old = _control(c)
        if old['closed']:
            raise AdmissionClosed('Already closed; cannot resurrect or replace a lease')
        value = {'version': 1, 'closed': True, 'owner': owner, 'executor': executor,
                 'expires': expires, 'closed_at': now}
        c.execute('UPDATE control SET body=? WHERE id=1', (json.dumps(value),))
    return value


def status(root, *, owner=None, require_zero=False):
    with transaction(root) as c:
        value = _control(c)
        rows = [dict(row) for row in c.execute('SELECT * FROM work ORDER BY id')]
        for row in rows:
            row['identity'] = json.loads(row['identity'])
        outstanding = [r for r in rows if r['identity'] != value.get('executor')]
        if require_zero:
            if (not value['closed'] or value.get('owner') != owner
                    or not time.time() < value.get('expires', 0)
                    or identity(value['executor']['pid']) != value['executor']):
                raise AdmissionClosed('Closed admission lease/executor is invalid')
            if outstanding:
                raise AdmissionClosed('Accepted runtime work has not drained')
        return {'control': value, 'outstanding': outstanding}


def reopen(root, *, owner, approval):
    if not approval or not approval.strip():
        raise AdmissionClosed('Post-QA approval required')
    with transaction(root) as c:
        old = _control(c)
        if not old['closed'] or old.get('owner') != owner:
            raise AdmissionClosed('Not the closed admission owner')
        if c.execute('SELECT count(*) FROM work WHERE identity != ?',
                     (json.dumps(old['executor']),)).fetchone()[0]:
            raise AdmissionClosed('Outstanding work must be reconciled before reopening')
        c.execute('UPDATE control SET body=? WHERE id=1',
                  (json.dumps({'version': 1, 'closed': False}),))
