"""Resumable orchestration around the existing backend guard; no rollback path."""
import math
import os
import threading
import time

from agent import backend_isolation as backend
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from hermes_cli import backend_isolation as control
from hermes_cli import backend_workflow_store as store
from hermes_cli import backend_workflow_watch as watchdog
from hermes_cli import maintenance_inference as legacy

need = store.require


def adapter_for(plan):
    from hermes_cli.backend_workflow_spark import SparkAdapter
    return SparkAdapter(plan)


def _execution(value, model):
    need(isinstance(model, str) and model.strip(), 'Explicit independent model required')
    if value.get('execution') is None:
        value['execution'] = dict(model=model, preflight=None, guard_operation=None,
            executor=None, heartbeat=time.time(), pending=None, applied=None, qa=None,
            events=[], error=None, release=None, post_release=None, reconciliation=None, deadline=None)
    value['execution']['model'] = model
    return value['execution']


def _save(value, stage=None):
    if stage is not None:
        value['stage'] = stage
    value['execution']['heartbeat'] = time.time()
    store._write(value)


def _snapshot(snapshot, value, *, full_window=False):
    need(isinstance(snapshot, dict) and isinstance(snapshot.get('identity'), dict)
         and bool(snapshot['identity']) and snapshot.get('healthy') is True
         and type(snapshot.get('already_applied')) is bool, 'Incomplete or unhealthy controller observation')
    expiry = snapshot.get('credentials_expires_at')
    need(type(expiry) in (int, float) and math.isfinite(expiry), 'Verified credential expiry required')
    window = value['plan']['margin_seconds'] + (value['plan']['budget_seconds'] if full_window else 0)
    need(expiry > time.time() + window, 'Credential lifetime insufficient; explicit renewal required')
    # Ensure observations can be safely recorded before any maintenance entry.
    store.digest(snapshot['identity'])
    return snapshot


def _owned(value):
    watchdog.check(value['id'])
    execution = value['execution']
    receipt = backend.require_mutation(value['plan']['backends'])
    need(receipt['operation'] == execution['guard_operation'], 'Maintenance operation changed')
    observed = backend.observed()
    need(observed['executor'] == accounting.identity() == execution['executor'],
         'Workflow executor changed; explicit resume required')
    deadline = execution['deadline']
    need(type(deadline) in (int, float) and time.time() < deadline,
         'Maintenance execution budget exhausted; retain isolation')


def _record(value, step, phase, data):
    need(isinstance(step, str) and 0 < len(step) <= 128
         and phase in ('intent', 'done', 'evidence'), 'Invalid controller progress event')
    _owned(value)
    execution = value['execution']
    sequence = len(execution['events'])
    artifact = store.evidence(value['id'], sequence, data)
    event = dict(sequence=sequence, step=step, phase=phase, at=time.time(), **artifact)
    if phase == 'intent':
        need(execution['pending'] is None, 'Unresolved mutation intent; explicit recovery required')
        execution['pending'] = event
    elif phase == 'done':
        need(execution['pending'] is not None and execution['pending']['step'] == step,
             'Mutation result has no matching intent')
        execution['pending'] = None
    execution['events'].append(event)
    _save(value)


def _progress(value):
    # The reviewed QA runner reports from eight request threads and its sampler.
    lock = threading.RLock()

    def report(step, phase, data):
        with lock:
            _record(value, step, phase, data)
    return report


def preflight(operation, *, model, adapter=None):
    store.load(operation)
    with store.operation_lock(operation):
        value = store.load(operation)
        execution = _execution(value, model)
        need(execution['guard_operation'] is None and value['stage'] in ('planned', 'preflight_ready', 'approved'),
             'Existing maintenance requires explicit resume, not new preflight')
        host = adapter if adapter is not None else adapter_for(value['plan'])
        try:
            snapshot = _snapshot(host.preflight(), value, full_window=True)
            legacy.probe(model)
            _snapshot(snapshot, value, full_window=True)
            execution['preflight'] = dict(snapshot, checked_at=time.time())
            execution['error'] = None
            _save(value, 'approved' if value['approval'] else 'preflight_ready')
            return value
        except Exception:
            execution['error'] = 'Preflight failed; no isolation or serving mutation performed'
            _save(value)
            raise


def _prepare(value, host, model, resume):
    execution = value['execution']
    observed = backend.observed()
    current = execution['guard_operation']
    if current is None:
        need(not resume, 'No recorded guard operation to resume')
        need(observed is not None and observed['phase'] == 'normal',
             'Another maintenance operation exists; do not adopt or release it')
    else:
        need(resume, 'Existing maintenance requires explicit resume')
        need(observed is not None and observed['phase'] != 'normal'
             and observed['operation'] == current
             and sorted(observed['affected']) == value['plan']['backends'],
             'Recorded maintenance operation is not current; explicit diagnosis required')

    snapshot = _snapshot(host.preflight(), value, full_window=True)
    old = execution['preflight']
    if current is None and old is not None:
        need(snapshot['identity'] == old['identity'], 'Preflight identity drift; prepare a new plan')
    if current is not None and value['stage'] in ('applying', 'verifying', 'awaiting_release'):
        need(snapshot['already_applied'], 'Uncertain partial apply; inspect controller evidence, never replay')
    if current is not None and value['stage'] == 'isolated' and not snapshot['already_applied']:
        need(old is not None and snapshot['identity'] == old['identity'],
             'Isolated deployment identity drift; do not adopt new mutation inputs')
    recovery = {'prior_executor': execution['executor'], 'unresolved_intent': execution['pending'],
                'observed_identity': snapshot['identity'], 'approved_goal_observed': snapshot['already_applied']}
    legacy.probe(model)
    # Probing can be slow: the declared window must still fit at entry, not
    # merely before cloud/auxiliary probes began.
    _snapshot(snapshot, value, full_window=True)
    execution['deadline'] = time.time() + value['plan']['budget_seconds']
    execution['error'] = None
    # Persist intent first. An entry crash without a returned operation ID is
    # intentionally not auto-adopted: another operation cannot be distinguished.
    execution['pending'] = {'step': 'isolate', 'phase': 'intent'}
    _save(value)
    active = backend.enter(value['plan']['backends'], model, executor_pid=os.getpid(), resume=resume)
    execution['guard_operation'] = active['operation']
    execution['executor'] = active['executor']
    execution['pending'] = None
    execution['qa'] = None
    execution['release'] = None
    execution['preflight'] = dict(snapshot, checked_at=time.time())
    _save(value, 'isolated')
    if resume:
        _record(value, 'explicit-resume-observation', 'evidence', recovery)
    return snapshot


def _verify(value, host):
    execution = value['execution']
    _owned(value)
    before = _snapshot(host.inspect(), value)
    need(before['already_applied'] and before['identity'] == execution['applied']['identity'],
         'Deployment identity or approved settings drift before QA')
    _save(value, 'verifying')
    result = host.qa(execution['guard_operation'], _progress(value))
    _owned(value)
    after = _snapshot(host.inspect(), value)
    need(isinstance(result, dict) and isinstance(result.get('checks'), dict)
         and set(result['checks']) == set(value['plan']['qa'])
         and all(passed is True for passed in result['checks'].values()), 'Required QA did not pass')
    need(result.get('identity') == before['identity'] == after['identity']
         and after['already_applied'], 'Deployment identity changed during QA')
    artifact = store.evidence(value['id'], len(execution['events']), result.get('evidence', {}))
    execution['qa'] = dict(checks=result['checks'], identity=after['identity'],
        plan_digest=value['plan_digest'], qa_digest=store.digest(value['plan']['qa']),
        guard_operation=execution['guard_operation'], checked_at=time.time(), evidence=artifact)
    _save(value, 'awaiting_release')


def _finish_routes(value, host):
    execution = value['execution']
    observed = backend.observed()
    need(observed is not None and observed['phase'] == 'normal'
         and observed.get('operation') == execution['guard_operation'],
         'Guard changed after release; no automatic isolation or rollback')
    result = host.normal_routes()
    execution['post_release'] = result
    _save(value, 'released')
    need(isinstance(result, dict) and result.get('primary') is True and result.get('secondary') is True,
         'Post-release routing degraded; no automatic rollback or re-isolation')
    execution['error'] = None
    _save(value, 'reconciling')
    _reconcile(value)


def _reconcile(value):
    from hermes_cli.backend_workflow_tasks import project
    need(value['stage'] in ('reconciling', 'complete'), 'Serving workflow must complete before task reconciliation')
    execution = value['execution']
    execution['reconciliation'] = {'status': 'pending', 'completed': []}
    _save(value, 'reconciling')
    try:
        execution['reconciliation'] = project(value)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, guard.MaintenanceIsolationError) else 'Task board projection failed'
        execution['reconciliation'] = {'status': 'blocked', 'completed': [], 'reason': reason}
        _save(value)
        raise guard.MaintenanceIsolationError('Serving completed; task reconciliation remains blocked') from None
    execution['error'] = None
    _save(value, 'complete')
    return value


def reconcile(operation):
    store.load(operation)
    with store.operation_lock(operation):
        return _reconcile(store.load(operation))


def _release(value, host, explicit):
    execution = value['execution']
    approval = value['approval']
    need(approval['plan_digest'] == value['plan_digest']
         and approval['qa_digest'] == store.digest(value['plan']['qa']), 'Approval no longer matches plan and QA')
    if explicit is None and approval['release_policy'] != 'conditional':
        return
    release = explicit or {'reference': approval['reference'], 'approval': approval['text']}
    need(store._text(release.get('reference'), 512) and store._text(release.get('approval')),
         'Case-specific post-QA release approval required')
    _owned(value)
    fresh = _snapshot(host.inspect(), value)
    qa = execution['qa']
    need(qa is not None and qa['identity'] == fresh['identity'] and fresh['already_applied']
         and qa['guard_operation'] == execution['guard_operation']
         and qa['plan_digest'] == value['plan_digest']
         and qa['qa_digest'] == store.digest(value['plan']['qa'])
         and 0 <= time.time() - qa['checked_at'] <= value['plan']['margin_seconds'],
         'Release QA stale or deployment changed')
    execution['release'] = dict(release, guard_operation=execution['guard_operation'],
        plan_digest=value['plan_digest'], qa_digest=qa['qa_digest'])
    execution['pending'] = {'step': 'release', 'phase': 'intent'}
    _save(value)
    control.release(release['approval'], expected_operation=execution['guard_operation'],
                    expected_executor=execution['executor'],
                    not_after=min(execution['deadline'], qa['checked_at'] + value['plan']['margin_seconds']))
    execution['pending'] = None
    _save(value, 'released')
    _finish_routes(value, host)


def run(operation, *, model, adapter=None, resume=False, release_approval=None):
    store.load(operation)
    with store.operation_lock(operation):
        value = store.load(operation)
        need(value['approval'] is not None, 'Explicit plan approval required before execution')
        execution = _execution(value, model)
        if value['stage'] in ('reconciling', 'complete'):
            return _reconcile(value)
        host = adapter if adapter is not None else adapter_for(value['plan'])
        try:
            # A lost release response is recoverable by observing the exact
            # operation and approval, never by replaying serving changes.
            observed = backend.observed()
            released = execution['release'] is not None and observed is not None \
                and observed['phase'] == 'normal' and observed.get('operation') == execution['guard_operation'] \
                and observed.get('release_approval') == execution['release']['approval']
            if value['stage'] == 'released' or released:
                need(resume, 'Released operation requires explicit verification resume')
                execution['pending'] = None
                _save(value, 'released')
                _finish_routes(value, host)
                return value
            snapshot = _prepare(value, host, model, resume)
            with watchdog.monitor(value):
                _owned(value)
                if not snapshot['already_applied']:
                    _save(value, 'applying')
                    snapshot = host.apply(execution['guard_operation'], _progress(value))
                    _owned(value)
                need(execution['pending'] is None, 'Unresolved mutation intent; explicit resume required')
                snapshot = _snapshot(snapshot, value)
                need(snapshot['already_applied'], 'Controller did not prove the approved target state')
                execution['applied'] = snapshot
                _save(value, 'verifying')
                _verify(value, host)
                _release(value, host, release_approval)
            return value
        except BaseException as exc:
            if value['stage'] in ('reconciling', 'complete'):
                raise
            execution['error'] = 'Workflow interrupted at ' + value['stage'] + '; explicit diagnosis/resume required'
            _save(value)
            if isinstance(exc, (guard.MaintenanceIsolationError, KeyboardInterrupt, SystemExit)):
                raise
            raise guard.MaintenanceIsolationError(execution['error']) from None


def release(operation, *, model, reference=None, approval=None, adapter=None):
    need(isinstance(reference, str) and reference.strip()
         and isinstance(approval, str) and approval.strip(), 'Explicit case-specific release approval required')
    value = store.load(operation)
    need(value['stage'] in ('awaiting_release', 'released'), 'Workflow is not awaiting release')
    return run(operation, model=model, adapter=adapter, resume=True,
               release_approval={'reference': reference, 'approval': approval})
