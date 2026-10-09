"""Executor heartbeat and fail-closed deadline observation, never lifecycle control.

Controller RPCs retain their own finite I/O timeouts. This watchdog independently
interrupts admission while a call is blocked; it never kills/restarts a service,
releases traffic, retries a serving action, or overwrites a replacement executor.
"""
from contextlib import contextmanager
import threading
import time

from agent import backend_isolation as backend
from hermes_cli import backend_workflow_store as store
from hermes_cli import maintenance_inference as legacy

_ACTIVE = {}
STALE_SECONDS = 10


class Watch:
    def __init__(self, value):
        execution = value['execution']
        self.workflow = value['id']
        self.operation = execution['guard_operation']
        self.executor = dict(execution['executor'])
        self.deadline = execution['deadline']
        current = backend.configuration()
        self.verified_at = current.get('verified_at') if current is not None else None
        self.stop = threading.Event()
        self.failed = threading.Event()
        self.last_beat = time.monotonic()
        self.thread = threading.Thread(target=self._run, name='maintenance-heartbeat', daemon=True)

    def beat(self, running):
        store._atomic_json(store.directory() / (self.workflow + '.heartbeat.json'), {
            'version': 1, 'workflow': self.workflow, 'operation': self.operation,
            'executor': self.executor, 'deadline': self.deadline, 'updated_at': time.time(),
            'running': running, 'failed': self.failed.is_set()})
        self.last_beat = time.monotonic()

    def interrupt(self):
        try:
            with legacy.locked():
                current = backend.configuration()
                if (current is not None and current['phase'] == 'active'
                        and current.get('operation') == self.operation
                        and current.get('executor') == self.executor
                        and current.get('verified_at') == self.verified_at):
                    legacy.write_state(dict(current, phase='interrupted'))
        except Exception:
            # The local failed flag fences every subsequent controller callback.
            # A busy state lock is retried by the next tick; malformed state is
            # already fail-closed and must not be repaired by a watchdog.
            pass

    def check(self):
        healthy = (not self.failed.is_set() and time.time() < self.deadline
                   and time.monotonic() - self.last_beat < STALE_SECONDS)
        if not healthy:
            self.failed.set()
            self.interrupt()
        store.require(healthy, 'Executor deadline or heartbeat failed; isolation retained')

    def _run(self):
        while True:
            delay = 1 if self.failed.is_set() else min(1, max(.02, self.deadline - time.time()))
            if self.stop.wait(delay):
                return
            try:
                if time.time() >= self.deadline:
                    self.failed.set()
                if self.failed.is_set():
                    self.interrupt()
                self.beat(True)
            except Exception:
                self.failed.set()
                self.interrupt()
                return


@contextmanager
def monitor(value):
    watch = Watch(value)
    watch.beat(True)
    _ACTIVE[value['id']] = watch
    watch.thread.start()
    try:
        yield
    finally:
        watch.stop.set()
        watch.thread.join(timeout=3)
        _ACTIVE.pop(value['id'], None)
        try:
            watch.beat(False)
        except Exception:
            # Progress/QA is in the separately fsynced journal. A failed final
            # liveness write cannot turn a real completion into new maintenance.
            pass


def check(operation):
    watch = _ACTIVE.get(operation)
    if watch is not None:
        watch.check()
