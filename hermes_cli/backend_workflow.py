"""Linux fleet maintenance CLI. The existing backend guard remains authoritative."""
import argparse
import json
import time
from pathlib import Path

import hermes_bootstrap  # noqa: F401
from agent import backend_isolation as backend
from agent import maintenance_inference as guard
from hermes_cli import backend_workflow_store as store
from hermes_cli import backend_workflow_watch as watchdog


def status(operation=None):
    value = store.load(operation) if operation else None
    observed = backend.observed()
    running = store.running(operation) if value else False
    heartbeat = store.read_heartbeat(operation) if value else None
    activity, next_action = ('blocked', 'Run preflight, review the exact plan, then approve and run.') if value else (
        'idle', 'Prepare a case-specific maintenance plan.')
    execution = (value or {}).get('execution') or {}
    if observed is not None and observed['phase'] != 'normal':
        activity = {'active': 'maintenance_active', 'isolating': 'maintenance_isolating',
                    'interrupted': 'interrupted'}.get(observed['phase'], 'blocked')
        next_action = 'Isolation is retained; inspect this exact operation before explicit resume. Never delete state.'
        if value and execution.get('guard_operation') == observed['operation']:
            if execution.get('error'):
                activity = 'interrupted'
            elif running and observed['phase'] == 'active':
                fresh = (heartbeat is not None and heartbeat['running'] and not heartbeat['failed']
                         and heartbeat['executor'] == execution.get('executor') == observed.get('executor')
                         and heartbeat['deadline'] == execution.get('deadline')
                         and time.time() < heartbeat['deadline']
                         and 0 <= time.time() - heartbeat['updated_at'] < watchdog.STALE_SECONDS)
                activity = 'running' if fresh else 'blocked'
                next_action = ('Observe progress; do not start another executor.' if fresh else
                               'Executor heartbeat/deadline is not current; retain isolation and diagnose the supervised job.')
            elif value['stage'] == 'awaiting_release' and not running:
                activity = 'awaiting_release'
                next_action = 'Provide case-specific release approval; release explicitly resumes and reruns fresh QA.'
    elif value and value['stage'] in ('reconciling', 'complete'):
        projection = execution.get('reconciliation') or {}
        activity = 'complete' if value['stage'] == 'complete' and projection.get('status') == 'complete' else 'bookkeeping_pending'
        next_action = 'Reconcile task projections only; never redispatch deployment.' if activity != 'complete' else 'No serving action required.'
    elif value and value['stage'] == 'released':
        activity, next_action = 'degraded', 'Explicitly resume post-release verification; no automatic rollback.'
    elif value and execution.get('guard_operation') is not None:
        activity, next_action = 'blocked', 'Guard no longer matches the recorded operation; diagnose without replaying deployment.'
    elif running:
        activity, next_action = 'running', 'Preflight is running without traffic exclusion; observe progress.'
    return {'workflow': value,
            'guard': ({k: observed.get(k) for k in ('version', 'phase', 'affected', 'operation')}
                      if observed is not None else None),
            'activity': activity, 'runner_lock_held': running, 'heartbeat': heartbeat,
            'execution_supported': True, 'next_action': next_action}


def _plan(args):
    with args.file.open('rb') as f:
        raw = f.read(store.MAX_BYTES + 1)
    store.require(len(raw) <= store.MAX_BYTES, 'Maintenance plan too large')
    return store.create(json.loads(raw))


def _approve(args):
    return store.approve(args.operation, args.digest, reference=args.reference,
                         text=args.approval, release_policy=args.release_policy)


def _status(args):
    value = status(args.operation)
    if getattr(args, 'live', False):
        store.require(args.operation is not None, 'Live inspection requires an operation')
        from hermes_cli.backend_workflow_run import adapter_for
        value['controller'] = adapter_for(value['workflow']['plan']).inspect()
    return value


def _execute(args):
    from hermes_cli import backend_workflow_run as engine
    if args.action == 'reconcile':
        engine.reconcile(args.operation)
    elif args.action == 'release':
        engine.release(args.operation, model=args.model, reference=args.reference, approval=args.approval)
    elif args.action == 'preflight':
        engine.preflight(args.operation, model=args.model)
    else:
        engine.run(args.operation, model=args.model, resume=args.action == 'resume')
    return status(args.operation)


HANDLERS = {'plan': _plan, 'approve': _approve, 'status': _status, 'inspect': _status,
            **{name: _execute for name in ('preflight', 'run', 'resume', 'release', 'reconcile')}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    plan = sub.add_parser('plan', help='Record a plan without invoking a controller')
    plan.add_argument('--file', type=Path, required=True)
    approve = sub.add_parser('approve', help='Record immutable case-specific plan approval')
    approve.add_argument('operation')
    approve.add_argument('--digest', required=True)
    approve.add_argument('--reference', required=True)
    approve.add_argument('--approval', required=True)
    approve.add_argument('--release-policy', choices=('separate', 'conditional'), default='separate')
    for name in ('status', 'inspect'):
        observe = sub.add_parser(name, help='Read journal and guard state without writes')
        observe.add_argument('operation', nargs='?')
        if name == 'inspect':
            observe.add_argument('--live', action='store_true', help='Also perform bounded read-only controller inspection')
    for name in ('preflight', 'run', 'resume', 'release', 'reconcile'):
        action = sub.add_parser(name)
        action.add_argument('operation')
        if name != 'reconcile':
            action.add_argument('--model', required=True, help='Explicit independent coordinator/probe model')
        if name == 'release':
            action.add_argument('--reference', required=True)
            action.add_argument('--approval', required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(HANDLERS[args.action](args), sort_keys=True))
        return 0
    except guard.MaintenanceIsolationError as exc:
        print(json.dumps({'error': str(exc)}))
        return 1
    except Exception:
        print(json.dumps({'error': 'Maintenance workflow failed; inspect operation and guard state before retrying'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
