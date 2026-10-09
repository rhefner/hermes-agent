"""Linux fleet preparation CLI for the incremental maintenance workflow.

This surface records plans and approvals. It cannot run a controller, enter
isolation or release traffic; those paths require the reviewed execution adapter.
"""
import argparse
import json
from pathlib import Path

import hermes_bootstrap  # noqa: F401
from agent import backend_isolation as backend
from agent import maintenance_inference as guard
from hermes_cli import backend_workflow_store as store


def status(operation=None):
    value = store.load(operation) if operation else None
    observed = backend.observed()
    activity = 'blocked' if value else 'idle'
    if observed is not None and observed['phase'] != 'normal':
        activity = {'active': 'maintenance_active', 'isolating': 'maintenance_isolating',
                    'interrupted': 'interrupted'}.get(observed['phase'], 'blocked')
    return {'workflow': value,
            'guard': ({k: observed.get(k) for k in ('version', 'phase', 'affected', 'operation')}
                      if observed is not None else None),
            'activity': activity,
            'execution_supported': False,
            'next_action': 'Controller preflight/run adapter is not implemented; do not start maintenance.'}


def _plan(args):
    with args.file.open('rb') as f:
        raw = f.read(store.MAX_BYTES + 1)
    store.require(len(raw) <= store.MAX_BYTES, 'Maintenance plan too large')
    return store.create(json.loads(raw))


def _approve(args):
    return store.approve(args.operation, args.digest, reference=args.reference,
                         text=args.approval, release_policy=args.release_policy)


def _status(args):
    return status(args.operation)


HANDLERS = {'plan': _plan, 'approve': _approve, 'status': _status, 'inspect': _status}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    plan = sub.add_parser('plan', help='Record a plan, without invoking any controller')
    plan.add_argument('--file', type=Path, required=True)
    approve = sub.add_parser('approve', help='Record case-specific plan approval, not execution')
    approve.add_argument('operation')
    approve.add_argument('--digest', required=True)
    approve.add_argument('--reference', required=True)
    approve.add_argument('--approval', required=True)
    approve.add_argument('--release-policy', choices=('separate', 'conditional'), default='separate')
    for name in ('status', 'inspect'):
        observe = sub.add_parser(name, help='Read operation and actual guard state without writes')
        observe.add_argument('operation', nargs='?')
    args = parser.parse_args(argv)
    try:
        print(json.dumps(HANDLERS[args.action](args), sort_keys=True))
        return 0
    except guard.MaintenanceIsolationError as exc:
        print(json.dumps({'error': str(exc)}))
        return 1
    except (OSError, ValueError, TypeError, RecursionError):
        print(json.dumps({'error': 'Maintenance preparation failed; no serving action attempted'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
