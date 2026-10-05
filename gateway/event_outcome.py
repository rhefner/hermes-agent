"""Process-local, event-correlated lifecycle observations (never transcript content).

An observer belongs to one inbound event, not to a routing key. ContextVars carry
it into the existing executor context without allowing a later turn to settle it.
"""
from contextvars import ContextVar
from typing import Callable, Optional

observer: ContextVar[Optional[Callable]] = ContextVar("gateway_event_outcome", default=None)


class WakeBoundaryClosed(RuntimeError):
    """A trusted observed event lost its immutable origin; do not send error text."""


def report(stage: str, **facts) -> None:
    callback = observer.get()
    if callback is not None:
        callback(stage, **facts)
