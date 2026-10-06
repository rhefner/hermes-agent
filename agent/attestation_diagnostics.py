"""Non-authoritative, fixed-vocabulary attestation failure diagnostics.

Never log exception text, request/proof data, challenge, response, or credentials.
Diagnostics must not change refusal behavior, evidence, deadlines, or authority.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import json
import logging
import time

_log = logging.getLogger(__name__)
_current: ContextVar[dict | None] = ContextVar('attestation_diagnostic', default=None)
_STAGES = frozenset({
    'receive', 'peer', 'frame', 'imports', 'active_state', 'observe',
    'runtime_match', 'action', 'prior_evidence', 'probe', 'post_probe',
    'fresh_evidence', 'send', 'runtime_resolve', 'runtime_check',
    'client_resolve', 'client_check', 'completion_call', 'completion_check',
})
_ACTIONS = frozenset({'certify', 'renew', 'check', 'probe'})
# Exact classes only: unknown/subclass names never become log content.
_CLASSES = {
    ('builtins', n): n for n in ('TimeoutError', 'ConnectionError', 'OSError',
        'ValueError', 'TypeError', 'KeyError', 'AttributeError', 'RuntimeError')
}
_CLASSES.update({('openai', n): n for n in (
    'AuthenticationError', 'PermissionDeniedError', 'RateLimitError',
    'APITimeoutError', 'APIConnectionError', 'BadRequestError',
    'InternalServerError', 'APIStatusError')})
_CLASSES.update({
    ('agent.maintenance_inference', 'MaintenanceIsolationError'): 'isolation_refusal',
    ('agent.maintenance_admission', 'AdmissionClosed'): 'accounting_refusal',
})


def stage(name):
    current = _current.get()
    if current is not None:
        current['stage'] = name if type(name) is str and name in _STAGES else 'unknown'


def action(name):
    current = _current.get()
    if current is not None:
        current['action'] = name if type(name) is str and name in _ACTIONS else 'unknown'


def completion(present, matches):
    current = _current.get()
    if current is not None:
        current['content_present'] = present is True
        current['exact_match'] = matches is True


@contextmanager
def capture(operation='unknown'):
    # Nested probes enrich the same request, rather than emitting duplicate logs.
    if _current.get() is not None:
        yield
        return
    record = {'stage': 'imports', 'action': 'unknown'}
    token = _current.set(record)
    started = time.monotonic()
    action(operation)
    try:
        yield
    except Exception as exc:
        try:
            cls = type(exc)
            report = dict(record, event='attestation_failure',
                exception=_CLASSES.get((cls.__module__, cls.__name__), 'other'),
                elapsed_ms=max(0, round((time.monotonic() - started) * 1000)))
            # No exc_info, stack_info, repr/str(exc), or caller-owned values.
            _log.warning('%s', json.dumps(report, sort_keys=True))
        except Exception:
            pass  # A broken logging sink must never replace the original failure.
        raise
    finally:
        _current.reset(token)
