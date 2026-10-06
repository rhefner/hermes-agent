"""Always-on serving mutation admission, not an arbitrary same-user sandbox.

Requires closed, zero-outstanding durable work accounting AND active verified
inference isolation. Receipts expire; inference isolation never auto-expires.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import time
from typing import NoReturn

from agent import maintenance_admission as accounting
from agent import maintenance_inference as guard
from agent.runtime_argv import runtime_argv, runtime_kind

MAX_AGE = 120
SOURCE = Path(__file__).resolve().parent.parent
CODE_DIRS = ('agent', 'tools', 'hermes_cli', 'gateway', 'cron')


def refuse(message) -> NoReturn:
    raise guard.MaintenanceIsolationError('Maintenance admission refused: ' + message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def private_json(path):
    try:
        meta = path.lstat()
        if not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.getuid() or meta.st_mode & 0o077:
            refuse('unsafe receipt storage')
        return json.loads(path.read_text())
    except (OSError, ValueError, TypeError):
        refuse('missing or malformed receipt')


def source_identity():
    files = sorted(p for folder in CODE_DIRS for p in (SOURCE / folder).rglob('*.py'))
    files += [SOURCE / 'run_agent.py', SOURCE / 'trajectory_compressor.py', SOURCE / 'hermes_constants.py']
    h = hashlib.sha256()
    newest = 0
    for path in files:
        h.update(str(path.relative_to(SOURCE)).encode() + b'\0' + path.read_bytes())
        newest = max(newest, path.stat().st_mtime)
    return h.hexdigest(), newest


def process(pid):
    try:
        root = Path('/proc') / str(pid)
        if root.stat().st_uid != os.getuid():
            refuse('process belongs to another uid')
        ident = accounting.identity(pid)
        return {**ident, 'argv': [s.decode() for s in (root / 'cmdline').read_bytes().split(b'\0') if s],
                'cgroup': (root / 'cgroup').read_text()}
    except (OSError, ValueError, IndexError, UnicodeError, accounting.AdmissionClosed):
        refuse('executor/process not live and identifiable')


def option(argv, *names):
    argv = runtime_argv(argv) or []
    value = None
    for i, word in enumerate(argv):
        for name in names:
            if word == name and i + 1 < len(argv):
                value = argv[i + 1]
            elif word.startswith(name + '='):
                value = word.split('=', 1)[1]
    return value


def is_runtime(argv):
    return runtime_argv(argv) is not None


def runtime_inventory():
    result = []
    for root in Path('/proc').iterdir():
        if not root.name.isdigit():
            continue
        try:
            if root.stat().st_uid != os.getuid():
                continue
            argv = [s.decode() for s in (root / 'cmdline').read_bytes().split(b'\0') if s]
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError):
            refuse('cannot inspect same-user process inventory')
        if is_runtime(argv):
            result.append(process(int(root.name)))
    return sorted(result, key=lambda p: p['pid'])


def profile_homes():
    from hermes_cli.maintenance_inference import profiles
    return [str(p) for p in profiles()]


def observe(model):
    """Re-read closed admission and real work reservations; never trust --drained."""
    try:
        # Fixed fleet root, not profile/env-reroutable accounting paths.
        root = guard.directory().parent / 'maintenance-admission'
        control = accounting.status(root)['control']
        status = accounting.status(root, owner=control.get('owner'), require_zero=True)
        if status['control'] != control:
            refuse('accounting control changed while verifying')
    except (OSError, ValueError, KeyError, accounting.AdmissionClosed):
        refuse('durable accounting must be enrolled, closed and drained to zero')
    fingerprint, newest = source_identity()
    executor = process(control['executor']['pid'])
    argv = executor['argv']
    if (runtime_kind(argv) != 'cli' or option(argv, '--provider') != 'openai-codex'
            or option(argv, '-m', '--model') != model
            or 'gateway' in argv or 'hermes-gateway' in executor['cgroup']):
        refuse('executor must be a live independent Hermes CLI with explicit Codex provider/model')
    homes = profile_homes()
    profile = option(argv, '-p', '--profile') or 'default'
    home = Path.home() / '.hermes' if profile == 'default' else Path.home() / '.hermes/profiles' / profile
    if str(home.resolve()) not in homes:
        refuse('executor profile is not discovered')
    btime = next(int(s.split()[1]) for s in Path('/proc/stat').read_text().splitlines() if s.startswith('btime '))
    hz = os.sysconf('SC_CLK_TCK')
    inventory = runtime_inventory()
    if executor not in inventory:
        refuse('executor not in observed runtime inventory')
    for p in inventory:
        if btime + int(p['start']) / hz <= newest + 1:
            refuse('old Hermes process predates source; load runtime outside this CLI first')
    marker = private_json(root / 'enrollment.json')
    if marker.get('version') != 1 or not isinstance(marker.get('enrolled_at'), (int, float)):
        refuse('missing verified offline enrollment marker')
    if any(btime + int(p['start']) / hz <= marker['enrolled_at'] for p in inventory):
        refuse('Hermes process predates accounting enrollment; untracked work cannot be certified')
    return {'source_sha256': fingerprint, 'executor': executor, 'profiles': homes,
            'profile_home': str(home.resolve()), 'runtime_inventory': inventory, 'control': control}


def active_state():
    value = guard.state()
    if not value or value['phase'] != 'active':
        refuse('persisted verified active isolation is required before any serving mutation')
    if set(value['verified_profiles']) != set(profile_homes()):
        refuse('profile inventory changed or is not fully verified; re-enter isolation')
    return value


def save_receipt(receipt):
    import tempfile
    fd, name = tempfile.mkstemp(prefix='.serving-', dir=guard.directory())
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(receipt, f, sort_keys=True); f.flush(); os.fsync(f.fileno())
        os.replace(name, guard.directory() / 'serving.json')
        d = os.open(guard.directory(), os.O_RDONLY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def verified_receipt(value, proof, action, previous=None):
    from agent.executor_attestation import request
    evidence = request(proof, digest(value), action, previous)
    if observe(value['model']) != proof or active_state() != value:
        refuse('runtime/state changed during exact-process verification')
    return {'version': 2, 'verified_at': evidence['completed_at'],
            'state_sha256': digest(value), 'proof': proof, 'attestation': evidence}


def certify():
    from hermes_cli.maintenance_inference import locked
    with locked():
        # Explicit certification invalidates prior authority even on failure.
        save_receipt({'version': 0, 'invalidated': True})
        try:
            value = active_state()
            receipt = verified_receipt(value, observe(value['model']), 'certify')
            save_receipt(receipt)
            require_admission()
            return receipt
        except Exception:
            save_receipt({'version': 0, 'invalidated': True})
            raise


def existing_receipt(value, *, allow_expired=False):
    receipt = private_json(guard.directory() / 'serving.json')
    try:
        age = time.time() - receipt['verified_at']
        if (receipt['version'] != 2 or age < 0
                or (not allow_expired and age > MAX_AGE)
                or receipt['state_sha256'] != digest(value)
                or receipt['verified_at'] != receipt['attestation']['completed_at']):
            refuse('receipt stale/unverified or isolation state changed; certify again')
        if observe(value['model']) != receipt['proof']:
            refuse('executor/runtime/profile/deployment changed; certify again')
    except (KeyError, TypeError, ValueError):
        refuse('malformed admission receipt')
    return receipt


def require_admission():
    value = active_state()
    receipt = existing_receipt(value)
    # A private JSON file, argv, or separately completed probe is not authority.
    if verified_receipt(value, receipt['proof'], 'check') != receipt:
        refuse('receipt does not match completion held by exact executor runtime')
    return receipt


def renew():
    """Renew only unchanged, explicitly certified authority; never reopen/extend control.

    Stale receipts may be replaced by a NEW authenticated exact-PID completion,
    not by refreshing the timestamp. Any invalidation requires explicit certify.
    """
    from hermes_cli.maintenance_inference import locked
    with locked():
        try:
            value = active_state()
            old = existing_receipt(value, allow_expired=True)
            receipt = verified_receipt(value, old['proof'], 'renew', old['attestation'])
            save_receipt(receipt)
            require_admission()
            return receipt
        except Exception:
            save_receipt({'version': 0, 'invalidated': True})
            raise
