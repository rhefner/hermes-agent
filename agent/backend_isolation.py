"""Backend-scoped maintenance contract, independent of executor leases.

Origins are inventory, not DNS guesses: all aliases and shared routers must be
listed together. A facade origin serving several backends is attributed only
through the inventory's explicit model ownership (the reserved
``facade_models`` entry); a request model is never inferred from its string.
Kernel-held reader locks cover physical HTTP response lifetimes. An interrupted
executor leaves exclusions in place; only explicit release removes those
exclusions. This is cooperative runtime coordination, not a same-user sandbox.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import time
from urllib.parse import urlsplit

from agent import maintenance_inference as guard

# A shared facade the request model cannot attribute to one inventoried backend.
UNRESOLVED = object()

_MODEL_ID = re.compile(r'[A-Za-z0-9._:@/+~-]{1,128}')


def origin(url):
    try:
        u = urlsplit(str(url))
        if u.scheme not in ('http', 'https') or not u.hostname or u.username or u.password:
            raise ValueError()
        host = u.hostname.lower().rstrip('.')
        port = u.port or (443 if u.scheme == 'https' else 80)
        return f'{u.scheme}://{host}:{port}'
    except (ValueError, TypeError):
        raise guard.MaintenanceIsolationError('Invalid backend endpoint identity') from None


def validate(value):
    if value.get('phase') not in ('normal', 'isolating', 'active', 'interrupted'):
        raise ValueError('Invalid backend phase')
    backends = value.get('backends')
    if not isinstance(backends, dict) or not backends:
        raise ValueError('Explicit backend inventory required')
    # Reserved inventory entry declaring which backend owns each model on a
    # shared facade; hoisted out of the endpoint mapping. A migration snapshot
    # taken before the hoist carries it in the affected set.
    models = backends.pop('facade_models', None)
    if models is not None:
        value['facade_models'] = models
        if isinstance(value.get('affected'), list):
            value['affected'] = [name for name in value['affected'] if name in backends]
    if 'facade_models' in value:
        models = value['facade_models']
        if not isinstance(models, dict) or any(
                not isinstance(model, str) or not _MODEL_ID.fullmatch(model)
                or not isinstance(name, str) or name not in backends
                for model, name in models.items()):
            raise ValueError('Invalid facade model ownership')
    for name, endpoints in backends.items():
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', name):
            raise ValueError('Invalid backend name')
        if not isinstance(endpoints, list) or not endpoints:
            raise ValueError('Backend endpoints required')
        if any(origin(endpoint) == origin(guard.CLOUD_BASE) for endpoint in endpoints):
            raise ValueError('Independent external probe route cannot be a maintained backend')
    # A facade origin may be listed under every backend that serves it; which
    # one a request hits is decided by facade_models, else it fails closed.
    affected = value.get('affected')
    if (not isinstance(affected, list) or len(set(affected)) != len(affected)
            or not set(affected) <= set(backends)
            or bool(affected) != (value['phase'] != 'normal')):
        raise ValueError('Invalid affected backend set')
    if affected and (value.get('provider') != 'openai-codex' or not value.get('model')
                     or value.get('base_url') != guard.CLOUD_BASE):
        raise ValueError('Independent route required')


def configuration():
    value = guard.state(include_normal=True)
    return value if value and value['version'] == 2 else None


def backends_at(value, url):
    """Inventoried backends whose endpoints serve this origin."""
    target = origin(url)
    return [name for name, endpoints in value['backends'].items()
            if isinstance(endpoints, list)
            and any(origin(endpoint) == target for endpoint in endpoints)]


def backend(value, url, model=None):
    """Physical backend for a request: its name, or None when uninventoried.

    UNRESOLVED marks a shared facade the request model cannot attribute: the
    inventory's explicit ownership decides, the model string is never guessed.
    """
    matches = backends_at(value, url)
    if not matches:
        return None
    mapped = value.get('facade_models', {}).get(model) if isinstance(model, str) else None
    if mapped in matches:
        return mapped
    if len(matches) == 1:
        return matches[0]
    return UNRESOLVED


def request_model(request):
    """The request's declared model, for shared-facade attribution only."""
    try:
        model = json.loads(request.content)['model']
    except Exception:
        return None
    return model if isinstance(model, str) and model.strip() else None


def require_route(value, provider, model, url, api_mode, *, resolved):
    # Unresolved provider names/aliases are not physical endpoints. The resolver
    # rechecks its result, then the transport checks every retry and redirect.
    if not url and not resolved:
        return
    if value['phase'] == 'normal':
        # Enrollment alone rejects nothing: unrelated, uninventoried and proxied
        # inference keeps working; inventoried routes stay tracked (begin_request).
        return
    if any(os.environ.get(k) for k in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY',
                                      'http_proxy', 'https_proxy', 'all_proxy')):
        raise guard.MaintenanceIsolationError('Maintenance cannot verify proxy inference routing')
    if guard.cloud_url(url):
        return
    name = backend(value, url, model)
    if name is None:
        raise guard.MaintenanceIsolationError('Maintenance refuses an uninventoried inference backend; inventory its aliases before use')
    if name is UNRESOLVED:
        raise guard.MaintenanceIsolationError('Maintenance cannot attribute the shared-facade model to an inventoried backend; declare its ownership in the inventory')
    if name in value['affected']:
        raise guard.MaintenanceIsolationError(f'Maintenance excludes backend {name}; select an explicitly verified independent route')
    if api_mode not in (None, '', 'chat_completions', 'responses', 'codex_responses'):
        raise guard.MaintenanceIsolationError('Maintenance cannot verify this inference transport')


def lock_backend(name, *, exclusive=False):
    import fcntl
    root = guard.directory() / 'backend-locks'
    root.mkdir(mode=0o700, exist_ok=True)
    path = root / (hashlib.sha256(name.encode()).hexdigest() + '.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
    except BaseException:
        os.close(fd)
        raise
    return fd


def begin_request(request):
    value = configuration()
    if value is None:
        guard.check_http_request(request)
        return []
    model = request_model(request)
    resolved = backend(value, request.url, model)
    if resolved is UNRESOLVED:
        # A shared facade without a decidable model may be any of its backends:
        # hold a reader on each instead of guessing one.
        targets = backends_at(value, request.url)
    else:
        targets = [] if resolved is None else [resolved]
    fds = []
    try:
        for name in targets:
            fds.append(lock_backend(name))
        # Entry publishes exclusions before testing the writer locks. A request
        # racing publication either owns a reader first, or sees the exclusion.
        fresh = configuration()
        if fresh is not None:
            require_route(fresh, None, model, str(request.url), None, resolved=True)
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise
    return fds


def require_drained(value):
    with contextlib.ExitStack() as stack:
        try:
            for name in sorted(value['affected']):
                fd = lock_backend(name, exclusive=True)
                stack.callback(os.close, fd)
        except BlockingIOError:
            raise guard.MaintenanceIsolationError('Affected backend responses are still in flight; retry after their real completion') from None


def observed():
    value = configuration()
    if value is None or value['phase'] == 'normal':
        return value
    from agent.maintenance_admission import identity, AdmissionClosed
    try:
        alive = identity(value.get('executor', {}).get('pid')) == value.get('executor')
    except (OSError, ValueError, IndexError, AdmissionClosed):
        alive = False
    if not alive:
        # Observation does not mutate state or reopen anything. Persisted affected
        # backends remain authoritative even before an operator reads status.
        return dict(value, phase='interrupted')
    return value


def require_mutation(backends):
    value = observed()
    if not value or value['phase'] != 'active' or not value.get('verified_at'):
        raise guard.MaintenanceIsolationError('Backend isolation must be active with a successful independent probe; resume interrupted work explicitly')
    if not backends or not set(backends) <= set(value['affected']):
        raise guard.MaintenanceIsolationError('Mutation target is not covered by backend exclusions')
    require_drained(value)
    return {'version': 3, 'status': 'BACKEND_ADMISSION_OK', 'operation': value['operation'],
            'backends': sorted(backends), 'verified_at': value['verified_at']}


def enter(affected, model, *, executor_pid, resume=False):
    from hermes_cli.maintenance_inference import locked, write_state, probe
    from agent.maintenance_admission import identity
    import uuid
    if type(executor_pid) is not int or executor_pid <= 1:
        raise guard.MaintenanceIsolationError('Explicit live executor PID required for interruption reporting')
    with locked():
        value = configuration()
        if value is None:
            raise guard.MaintenanceIsolationError('Migrate and enroll the backend inventory before entry')
        if resume:
            if value['phase'] == 'normal' or set(affected) != set(value['affected']):
                raise guard.MaintenanceIsolationError('Resume must retain the exact affected backend set')
        elif value['phase'] != 'normal':
            raise guard.MaintenanceIsolationError('Existing operation requires explicit resume or approved release')
        value = dict(value, affected=sorted(affected), phase='isolating', provider='openai-codex',
                     model=model, base_url=guard.CLOUD_BASE, executor=identity(executor_pid),
                     operation=value.get('operation') if resume else uuid.uuid4().hex,
                     verified_at=None)
        validate(value)
        write_state(value)
        try:
            probe(model)
            require_drained(value)
        except Exception:
            write_state(dict(value, phase='interrupted'))
            raise
        value.update(phase='active', verified_at=time.time())
        write_state(value)
        return value
