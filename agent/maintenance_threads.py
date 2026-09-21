"""Detached memory/title work: reserve before Thread.start, release at real exit."""
import contextvars
import threading

from agent.maintenance_admission import reserved_target


class AdmissionThread(threading.Thread):
    def __init__(self, *, target, args=(), kwargs=None, name=None, daemon=True):
        self._work_context = contextvars.copy_context()
        self._work_target = target
        self._work_args = args
        self._work_kwargs = kwargs or {}
        super().__init__(name=name, daemon=daemon)

    def start(self):
        if self._started.is_set():
            raise RuntimeError('threads can only be started once')
        target, cancel = self._work_context.run(
            reserved_target, self._work_target, 'memory-title-thread')
        self._work_target = target
        try:
            super().start()
        except BaseException:
            cancel()
            raise

    def run(self):
        self._work_context.run(self._work_target, *self._work_args, **self._work_kwargs)
