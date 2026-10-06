"""Serving admission controls. No service restarts, routing edits, or rollback."""
import argparse
import json
import time

import hermes_bootstrap  # noqa: F401
from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from agent import serving_admission as admission


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'renew', 'certify', 'status', 'enroll', 'close'))
    parser.add_argument('--owner')
    parser.add_argument('--executor-pid', type=int)
    parser.add_argument('--seconds', type=int, default=3600)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    root = guard.directory().parent / 'maintenance-admission'
    try:
        if args.action == 'enroll':
            if admission.runtime_inventory():
                admission.refuse('offline enrollment requires all Hermes runtimes stopped; preserve/resume conversations first')
            accounting.initialize(root)
            accounting.status(root)  # exact-target readback
            print('ACCOUNTING_ENROLLED; start current runtimes before close/entry')
        elif args.action == 'close':
            accounting.close(root, owner=args.owner, executor_pid=args.executor_pid,
                             expires=time.time() + args.seconds)
            accounting.status(root, owner=args.owner, require_zero=True)
            print('ACCOUNTING_CLOSED_ZERO; old-process/source verification still required')
        elif args.action == 'certify':
            receipt = admission.certify()
            print(json.dumps({'status': 'SERVING_ADMISSION_VERIFIED', 'verified_at': receipt['verified_at'],
                              'expires_at': receipt['verified_at'] + admission.MAX_AGE}))
        elif args.action in ('check', 'renew'):
            receipt = admission.renew() if args.action == 'renew' else admission.require_admission()
            control = receipt['proof']['control']
            result = {'status': 'SERVING_ADMISSION_OK', 'owner': control['owner'],
                      'expires_at': min(control['expires'], receipt['verified_at'] + admission.MAX_AGE)}
            print(json.dumps(result) if args.json else 'SERVING_ADMISSION_OK')
        else:
            value = guard.state()
            print(json.dumps({'isolation': value['phase'] if value else 'normal',
                              'accounting_enrolled': root.exists(),
                              'receipt_present': (guard.directory() / 'serving.json').exists(),
                              'admitted': False, 'note': 'status is not admission; use check'}))
        return 0
    except Exception as exc:
        print(str(exc) if isinstance(exc, (guard.MaintenanceIsolationError, accounting.AdmissionClosed))
              else 'Maintenance admission refused: unavailable or malformed prerequisite; no serving action performed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
