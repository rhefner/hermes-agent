"""Private preparation records; never a source of backend mutation authority."""
from contextlib import contextmanager
from decimal import Decimal
import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import uuid

from agent import backend_isolation as backend
from agent import maintenance_inference as guard

PLAN_FIELDS = {'adapter', 'backends', 'controller_revision', 'delta', 'preserve',
               'qa', 'budget_seconds', 'margin_seconds', 'tasks'}
QA_CHECKS = {'direct', 'facade', 'unaffected', 'capacity', 'health', 'settings'}
RECORD_FIELDS = {'version', 'id', 'plan', 'plan_digest', 'approval', 'stage'}
APPROVAL_FIELDS = {'reference', 'text', 'plan_digest', 'release_policy', 'qa_digest'}
MAX_BYTES = 1024 * 1024
# Preparation accepts only non-secret settings of the one supported adapter.
# Actual deployment/source compatibility is a separate preflight responsibility.
DELTA_PATTERNS = {'PARALLEL': '8', 'TF_GLM_MULTI_WINDOW': '64', 'TF_ROCE_MAX_KB': '1024',
                  'MEMORY_RESERVE_GIB': r'\d{1,3}(?:\.\d{1,3})?'}
PRESERVE_PATTERNS = {
    'MODEL_ID': r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',
    'DFLASH2_ID': r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',
    'MODEL_REVISION': '[a-f0-9]{40}', 'DFLASH2_REVISION': '[a-f0-9]{40}',
    'RECIPE_REVISION': '[a-f0-9]{40}', 'IMAGE_DIGEST': 'sha256:[a-f0-9]{64}',
    'CONTEXT': '1048576', 'THINKING': '1', 'VISION': '1', 'TP': '2',
    'KV': 'fp8', 'DENSE': 'q4', 'DRAFTER': 'dflash2', 'PORT': '8000',
    'SERVED_NAME': r'[A-Za-z0-9_.-]+',
}


def require(condition, message):
    if not condition:
        raise guard.MaintenanceIsolationError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def _text(value, limit=4096):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= limit


def _string_map(value):
    return (isinstance(value, dict) and bool(value) and len(value) <= 64
            and all(_text(k, 128) and _text(v) for k, v in value.items()))


def validate_plan(value):
    require(isinstance(value, dict) and set(value) == PLAN_FIELDS, 'Invalid maintenance plan fields')
    require(value['adapter'] == 'spark-pair-eight-stream', 'Unsupported maintenance adapter')
    require(value['backends'] == ['spark-pair'], 'Explicit supported backend targets required')
    require(isinstance(value['controller_revision'], str)
            and re.fullmatch('[a-f0-9]{40}', value['controller_revision']),
            'Pinned controller revision required')
    require(_string_map(value['delta']) and _string_map(value['preserve']),
            'Explicit delta and preserved settings required')
    for field, patterns in (('delta', DELTA_PATTERNS), ('preserve', PRESERVE_PATTERNS)):
        require(all(key in patterns and re.fullmatch(patterns[key], setting)
                    for key, setting in value[field].items()),
                'Only supported non-secret maintenance settings are permitted')
    require({'MODEL_ID', 'CONTEXT'} <= set(value['preserve']),
            'Explicit model and context preservation required')
    if 'MEMORY_RESERVE_GIB' in value['delta']:
        require(Decimal(value['delta']['MEMORY_RESERVE_GIB']) >= Decimal('19.6'),
                'Memory reserve cannot be below the supported recipe floor')
    qa = value['qa']
    require(isinstance(qa, list) and all(isinstance(x, str) for x in qa)
            and len(qa) == len(QA_CHECKS) and set(qa) == QA_CHECKS,
            'Complete direct, facade and preservation QA checklist required')
    for key in ('budget_seconds', 'margin_seconds'):
        require(type(value[key]) is int and 0 < value[key] <= 86400,
                'Positive bounded maintenance time budget and margin required')
    tasks = value['tasks']
    require(isinstance(tasks, list) and len(tasks) <= 64
            and all(isinstance(t, str) and re.fullmatch('t_[a-f0-9]{8}', t) for t in tasks)
            and len(set(tasks)) == len(tasks), 'Invalid linked maintenance tasks')
    require(len(json.dumps(value).encode()) <= MAX_BYTES, 'Maintenance plan too large')


def _identifier(value):
    require(isinstance(value, str) and re.fullmatch('[a-f0-9]{32}', value),
            'Invalid maintenance operation identifier')
    return value


def directory():
    # Fleet-wide by intent: profile-local HERMES_HOME is not a separate operation namespace.
    return guard.directory() / 'operations'


def _private_directory(root):
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and not info.st_mode & 0o077, 'Private maintenance operation directory required')


def _validate_record(value, operation):
    require(isinstance(value, dict) and set(value) == RECORD_FIELDS,
            'Invalid maintenance operation record')
    require(type(value['version']) is int and value['version'] == 1
            and value['id'] == operation and value['stage'] == 'planned',
            'Unsupported maintenance preparation record')
    validate_plan(value['plan'])
    require(value['plan_digest'] == digest(value['plan']), 'Maintenance plan digest mismatch')
    approval = value['approval']
    if approval is not None:
        require(isinstance(approval, dict) and set(approval) == APPROVAL_FIELDS,
                'Invalid maintenance approval')
        require(_text(approval['reference'], 512) and _text(approval['text'])
                and approval['plan_digest'] == value['plan_digest']
                and approval['release_policy'] in ('separate', 'conditional')
                and approval['qa_digest'] == digest(value['plan']['qa']),
                'Maintenance approval does not match plan and QA')
    return value


def load(operation):
    operation = _identifier(operation)
    try:
        root = directory()
        _private_directory(root)
        fd = os.open(root / (operation + '.json'), os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                    and not info.st_mode & 0o077, 'Private regular operation record required')
            raw = f.read(MAX_BYTES + 1)
        require(len(raw) <= MAX_BYTES, 'Maintenance operation record too large')
        return _validate_record(json.loads(raw), operation)
    except (OSError, ValueError, TypeError, RecursionError):
        raise guard.MaintenanceIsolationError('Maintenance operation unreadable; no state changed') from None


@contextmanager
def operation_lock(operation):
    operation = _identifier(operation)
    root = directory()
    try:
        # Never create the guard parent: a bare guard directory fails closed.
        root.mkdir(exist_ok=True, mode=0o700)
        _private_directory(root)
        fd = os.open(root / (operation + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    except OSError:
        raise guard.MaintenanceIsolationError('Maintenance operation lock unavailable') from None
    with os.fdopen(fd, 'a') as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise guard.MaintenanceIsolationError('Maintenance operation already running') from None
        yield


def _write(value):
    _validate_record(value, value['id'])
    root = directory()
    fd, temporary = tempfile.mkstemp(prefix='.operation-', dir=root)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(value, f, sort_keys=True, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, root / (value['id'] + '.json'))
        d = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def create(plan):
    validate_plan(plan)
    enrolled = backend.configuration()
    require(enrolled is not None and set(plan['backends']) <= set(enrolled['backends']),
            'Enrolled backend inventory required before planning')
    # Snapshot caller-owned data, not a mutable reference retained by the workflow.
    plan = json.loads(json.dumps(plan))
    operation = uuid.uuid4().hex
    value = {'version': 1, 'id': operation, 'plan': plan, 'plan_digest': digest(plan),
             'approval': None, 'stage': 'planned'}
    with operation_lock(operation):
        require(not (directory() / (operation + '.json')).exists(), 'Operation already exists')
        _write(value)
    return value


def approve(operation, expected_digest, *, reference, text, release_policy='separate'):
    # Refuse nonexistent/corrupt records without creating a lock or directory.
    load(operation)
    with operation_lock(operation):
        value = load(operation)
        require(expected_digest == value['plan_digest'], 'Approval plan digest mismatch')
        approval = {'reference': reference, 'text': text, 'release_policy': release_policy,
                    'plan_digest': expected_digest, 'qa_digest': digest(value['plan']['qa'])}
        require(value['approval'] in (None, approval),
                'Approval already recorded; create a new plan for changed scope')
        if value['approval'] == approval:
            return value
        value['approval'] = approval
        _validate_record(value, operation)
        _write(value)
    return value
