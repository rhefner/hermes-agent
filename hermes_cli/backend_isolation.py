"""Explicit v1 -> backend-scoped migration and release; never rewrites a profile."""
import argparse
import json
import os
import time
import uuid
from pathlib import Path

import hermes_bootstrap  # noqa: F401
from agent import backend_isolation as backend
from agent import maintenance_inference as guard
from hermes_cli.maintenance_inference import locked, write_state


def migrate(backends, *, approval):
    if not approval or not approval.strip():
        raise guard.MaintenanceIsolationError('Explicit inventory/deployment migration approval required')
    with locked():
        old = guard.state(include_normal=True)
        if old and old['version'] == 2:
            raise guard.MaintenanceIsolationError('Already migrated; backend inventory cannot change under live clients')
        from agent import maintenance_admission as accounting
        root = accounting.directory()
        historical = accounting.status(root) if root.exists() else None
        disrupted = bool((old and old['phase'] != 'normal') or
                         (historical and historical['control']['closed']))
        value = {'version': 2, 'phase': 'interrupted' if disrupted else 'normal',
                 'backends': backends, 'affected': sorted(backends) if disrupted else [],
                 'provider': 'openai-codex', 'model': (old or {}).get('model', 'gpt-6-astra'),
                 'base_url': guard.CLOUD_BASE, 'operation': uuid.uuid4().hex if disrupted else None,
                 'verified_at': None, 'migration_approval': approval, 'migrated_at': time.time()}
        backend.validate(value)
        # Immutable archive first. Existing enrollment/audits/reservations remain
        # byte-for-byte in their original DB, no DELETE/UPDATE/reset or fake finish.
        archive = guard.directory() / ('legacy-' + uuid.uuid4().hex + '.json')
        with open(archive, 'x', opener=lambda p, flags: os.open(p, flags, 0o600)) as f:
            json.dump({'isolation': old, 'accounting': historical}, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        write_state(value)
        return value


def release(approval):
    if not approval or not approval.strip():
        raise guard.MaintenanceIsolationError('Explicit case-specific post-QA release approval required')
    with locked():
        value = backend.configuration()
        if not value or value['phase'] == 'normal':
            raise guard.MaintenanceIsolationError('No backend operation to release')
        # Keep the inventory: future requests must be tracked before next entry.
        write_state(dict(value, phase='normal', affected=[], release_approval=approval,
                         released_at=time.time()))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=('migrate', 'enter', 'resume', 'status', 'check', 'release'))
    p.add_argument('--inventory', type=Path)
    p.add_argument('--backend', action='append', default=[])
    p.add_argument('--model')
    p.add_argument('--executor-pid', type=int)
    p.add_argument('--approval')
    a = p.parse_args()
    try:
        if a.action == 'migrate':
            value = migrate(json.loads(a.inventory.read_text()), approval=a.approval)
        elif a.action in ('enter', 'resume'):
            value = backend.enter(a.backend, a.model, executor_pid=a.executor_pid, resume=a.action == 'resume')
        elif a.action == 'release':
            release(a.approval)
            value = backend.observed()
        elif a.action == 'check':
            value = backend.require_mutation(a.backend)
        else:
            value = backend.observed()
        print(json.dumps(value, sort_keys=True))
        return 0
    except Exception as exc:
        print(str(exc) if isinstance(exc, guard.MaintenanceIsolationError) else
              'Backend operation failed; state retained. Check inventory, independent provider authentication/quota, and drain.')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
