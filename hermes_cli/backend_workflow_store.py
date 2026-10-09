"""Private preparation records; never a source of backend mutation authority."""
from contextlib import contextmanager
from decimal import Decimal
import fcntl
import hashlib
import json
import math
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
STAGES = {'planned', 'preflight_ready', 'approved', 'isolated', 'applying',
          'verifying', 'awaiting_release', 'released', 'reconciling', 'complete'}
EXECUTION_FIELDS = {'model', 'preflight', 'guard_operation', 'executor', 'heartbeat',
                    'pending', 'applied', 'qa', 'events', 'error', 'release',
                    'post_release', 'reconciliation', 'deadline'}
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


def _timestamp(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _validate_execution(value):
    execution, stage = value['execution'], value['stage']
    require(_timestamp(execution['heartbeat'])
            and (execution['deadline'] is None or _timestamp(execution['deadline']))
            and (execution['error'] is None or _text(execution['error'], 1024)),
            'Invalid workflow progress metadata')
    executor = execution['executor']
    if executor is not None:
        require(set(executor) == {'pid', 'start', 'boot'} and type(executor['pid']) is int
                and executor['pid'] > 1 and _text(executor['start'], 128) and _text(executor['boot'], 128),
                'Invalid recorded executor identity')
    for field in ('preflight', 'applied'):
        snapshot = execution[field]
        if snapshot is not None:
            require(isinstance(snapshot.get('identity'), dict) and bool(snapshot['identity'])
                    and _timestamp(snapshot.get('credentials_expires_at'))
                    and type(snapshot.get('already_applied')) is bool and snapshot.get('healthy') is True,
                    'Invalid controller observation')
            if field == 'preflight':
                require(_timestamp(snapshot.get('checked_at')), 'Invalid preflight timestamp')
    for index, event in enumerate(execution['events']):
        require(isinstance(event, dict) and event.get('sequence') == index
                and _text(event.get('step'), 128) and event.get('phase') in ('intent', 'done', 'evidence')
                and _timestamp(event.get('at')) and _text(event.get('path'))
                and isinstance(event.get('sha256'), str) and re.fullmatch('[a-f0-9]{64}', event['sha256']),
                'Invalid workflow evidence event')
    pending = execution['pending']
    if pending is not None:
        require(_text(pending.get('step'), 128) and pending.get('phase') == 'intent',
                'Invalid unresolved workflow intent')
    qa = execution['qa']
    if qa is not None:
        require(isinstance(qa.get('checks'), dict) and set(qa['checks']) == set(value['plan']['qa'])
                and all(passed is True for passed in qa['checks'].values())
                and qa.get('plan_digest') == value['plan_digest']
                and qa.get('qa_digest') == digest(value['plan']['qa'])
                and qa.get('guard_operation') == execution['guard_operation']
                and execution['applied'] is not None
                and qa.get('identity') == execution['applied']['identity']
                and _timestamp(qa.get('checked_at')) and isinstance(qa.get('evidence'), dict)
                and _text(qa['evidence'].get('path'))
                and isinstance(qa['evidence'].get('sha256'), str)
                and re.fullmatch('[a-f0-9]{64}', qa['evidence']['sha256']),
                'Invalid or mismatched workflow QA receipt')
    release = execution['release']
    if release is not None:
        require(_text(release.get('reference'), 512) and _text(release.get('approval'))
                and release.get('plan_digest') == value['plan_digest']
                and release.get('qa_digest') == digest(value['plan']['qa'])
                and release.get('guard_operation') == execution['guard_operation'] and qa is not None,
                'Invalid workflow release approval')
    if stage in ('planned', 'preflight_ready', 'approved'):
        require(execution['guard_operation'] is None and executor is None,
                'Preparation stage cannot own a maintenance executor')
    if stage in ('isolated', 'applying', 'verifying', 'awaiting_release', 'released', 'reconciling', 'complete'):
        require(execution['guard_operation'] is not None and executor is not None,
                'Maintenance stage lacks its guard operation and executor')
    if stage in ('verifying', 'awaiting_release', 'released', 'reconciling', 'complete'):
        require(execution['applied'] is not None and execution['applied']['already_applied'],
                'Maintenance stage lacks proven applied settings')
    if stage in ('awaiting_release', 'released', 'reconciling', 'complete'):
        require(qa is not None, 'Maintenance stage lacks QA evidence')
    if stage in ('released', 'reconciling', 'complete'):
        require(release is not None, 'Released stage lacks case-specific approval')
    if stage in ('reconciling', 'complete'):
        require(execution['post_release'] == {'primary': True, 'secondary': True}
                and pending is None, 'Completed serving workflow lacks normal-route verification')
    if stage == 'complete':
        reconciliation = execution['reconciliation']
        require(reconciliation is not None and reconciliation.get('status') == 'complete'
                and reconciliation.get('completed') == value['plan']['tasks'],
                'Completed workflow lacks task reconciliation')


def _validate_record(value, operation):
    require(isinstance(value, dict) and RECORD_FIELDS <= set(value)
            and set(value) <= RECORD_FIELDS | {'execution'}, 'Invalid maintenance operation record')
    require(type(value['version']) is int and value['version'] == 1
            and value['id'] == operation and value['stage'] in STAGES,
            'Unsupported maintenance operation record')
    validate_plan(value['plan'])
    execution = value.get('execution')
    if execution is None:
        require(value['stage'] == 'planned', 'Missing workflow execution record')
    else:
        require(isinstance(execution, dict) and set(execution) == EXECUTION_FIELDS,
                'Invalid workflow execution record')
        require(_text(execution['model'], 128) and isinstance(execution['events'], list)
                and len(execution['events']) <= 4096, 'Invalid workflow execution metadata')
        if execution['guard_operation'] is not None:
            _identifier(execution['guard_operation'])
        for field in ('preflight', 'executor', 'pending', 'applied', 'qa', 'release',
                      'post_release', 'reconciliation'):
            require(execution[field] is None or isinstance(execution[field], dict),
                    'Invalid workflow evidence shape')
        _validate_execution(value)
    require(value['plan_digest'] == digest(value['plan']), 'Maintenance plan digest mismatch')
    approval = value['approval']
    if value['stage'] not in ('planned', 'preflight_ready'):
        require(approval is not None, 'Execution stage lacks case-specific plan approval')
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


def read_heartbeat(operation):
    operation = _identifier(operation)
    try:
        fd = os.open(directory() / (operation + '.heartbeat.json'), os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    try:
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                    and not info.st_mode & 0o077, 'Private executor heartbeat required')
            raw = f.read(4097)
        require(len(raw) <= 4096, 'Executor heartbeat exceeds bound')
        value = json.loads(raw)
        require(isinstance(value, dict) and value.get('version') == 1
                and value.get('workflow') == operation and isinstance(value.get('executor'), dict)
                and _timestamp(value.get('deadline')) and _timestamp(value.get('updated_at'))
                and type(value.get('failed')) is bool and type(value.get('running')) is bool,
                'Invalid executor heartbeat')
        _identifier(value.get('operation'))
        return value
    except (OSError, ValueError, TypeError):
        raise guard.MaintenanceIsolationError('Executor heartbeat unreadable; guard state unchanged') from None


def running(operation):
    """Observe an existing runner lock without creating files or changing state."""
    operation = _identifier(operation)
    try:
        fd = os.open(directory() / (operation + '.lock'), os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return False
    with os.fdopen(fd, 'rb') as f:
        info = os.fstat(f.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077, 'Private operation lock required')
        try:
            fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False


def _atomic_json(path, value):
    raw = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    require(len(raw) <= MAX_BYTES, 'Maintenance record exceeds bounded storage')
    root = path.parent
    _private_directory(root)
    fd, temporary = tempfile.mkstemp(prefix='.operation-', dir=root)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
        d = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _write(value):
    _validate_record(value, value['id'])
    _atomic_json(directory() / (value['id'] + '.json'), value)


def evidence(operation, sequence, data):
    """Caller holds operation_lock; filenames are generated, not caller paths."""
    operation = _identifier(operation)
    require(type(sequence) is int and 0 <= sequence <= 4096, 'Invalid evidence sequence')
    root = directory() / (operation + '.evidence')
    root.mkdir(mode=0o700, exist_ok=True)
    _private_directory(root)
    path = root / (str(sequence) + '-' + uuid.uuid4().hex + '.json')
    _atomic_json(path, data)
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


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
        if value['stage'] == 'preflight_ready':
            value['stage'] = 'approved'
        _validate_record(value, operation)
        _write(value)
    return value
