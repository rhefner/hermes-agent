"""Explicit recovery of a closed executor gate; never a drain or serving proof."""
import json
import time
import uuid
from pathlib import Path

from agent import maintenance_admission as gate


def still_live(recorded):
    """Only a missing PID or different kernel identity proves absence.

    Permission errors, malformed proc data and zombies conservatively refuse.
    """
    try:
        return gate.identity(recorded['pid']) == recorded
    except FileNotFoundError:
        return False


def replacement(pid):
    from agent.serving_admission import process, option, profile_homes
    from agent.runtime_argv import runtime_kind, runtime_argv
    p = process(pid)
    argv = runtime_argv(p['argv']) or []
    profile = option(p['argv'], '-p', '--profile') or 'default'
    home = Path.home() / '.hermes'
    if profile != 'default':
        home = home / 'profiles' / profile
    if (runtime_kind(p['argv']) != 'cli'
            or option(p['argv'], '--provider') != 'openai-codex'
            or option(p['argv'], '-m', '--model') != 'gpt-6-astra'
            or 'gateway' in p['cgroup'] or 'kanban' in p['cgroup']
            or any(a.split('=', 1)[0] in ('-q', '--query', '-z', '--yolo') for a in argv)
            or str(home.resolve()) not in profile_homes()):
        raise gate.AdmissionClosed('Replacement must be an independent interactive Codex CLI')
    return {k: p[k] for k in ('pid', 'start', 'boot')}


def recover(root, *, owner, executor_pid, seconds, acknowledgement):
    """Keep all outstanding work. Old receipts fail their exact control check.

    The audit is in the SAME private SQLite transaction as control, so crashes
    cannot publish a takeover without its audit. No separate files are trusted.
    """
    if (type(seconds) is not int or not 1 <= seconds <= 14400
            or not isinstance(acknowledgement, str) or not acknowledgement.strip()):
        raise gate.AdmissionClosed('Bounded lease and explicit recovery acknowledgement required')
    new = replacement(executor_pid)
    with gate.transaction(root) as c:
        old = gate._control(c)
        now = time.time()
        if not old['closed'] or old.get('owner') != owner:
            raise gate.AdmissionClosed('Closed admission owner mismatch')
        if not isinstance(old.get('expires'), (int, float)) or not old['expires'] <= now:
            raise gate.AdmissionClosed('Recovery requires an expired lease')
        if still_live(old['executor']):
            raise gate.AdmissionClosed('Stop the old executor before recovery; its work may still run')
        rows = [dict(r) for r in c.execute('SELECT * FROM work ORDER BY id')]
        for row in rows:
            if still_live(json.loads(row['identity'])):
                raise gate.AdmissionClosed('Live outstanding work blocks recovery')
        # Reobserve the exact replacement immediately before committing authority.
        if replacement(executor_pid) != new:
            raise gate.AdmissionClosed('Replacement identity changed')
        event = {'id': uuid.uuid4().hex, 'at': now, 'previous': old,
                 'replacement': new, 'acknowledgement': acknowledgement,
                 'preserved_reservations': rows}
        c.execute('CREATE TABLE IF NOT EXISTS recovery_audit (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
        c.execute('INSERT INTO recovery_audit VALUES (?, ?)', (event['id'], json.dumps(event)))
        updated = {**old, 'executor': new, 'expires': now + seconds, 'recovery_id': event['id']}
        c.execute('UPDATE control SET body=? WHERE id=1', (json.dumps(updated),))
    return {'status': 'EXECUTOR_RECOVERED_GATE_CLOSED', 'executor': new,
            'expires': updated['expires'], 'preserved_reservations': len(rows),
            'serving_authorized': False, 'recovery_id': event['id']}
