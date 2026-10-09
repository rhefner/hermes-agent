"""Non-dispatching projections onto the fleet's existing default task board.

Never calls complete_task: its readiness recomputation, hooks and workspace
cleanup are inappropriate for historical maintenance projections.
"""
from contextlib import closing
import json
import sqlite3
import time

from agent import maintenance_inference as guard
from hermes_cli import backend_workflow_store as store


def project(value):
    execution = value.get('execution') or {}
    store.require(value['stage'] in ('reconciling', 'complete')
                  and execution.get('post_release') == {'primary': True, 'secondary': True}
                  and execution.get('release', {}).get('plan_digest') == value['plan_digest'],
                  'Only completed, released workflows can reconcile tasks')
    tasks = value['plan']['tasks']
    if not tasks:
        return {'status': 'complete', 'completed': []}
    from hermes_cli import kanban_db as kb
    from hermes_cli.sqlite_safe_read import connect_tracked

    # Pin the fleet board, not a transient profile/worker's ambient selected
    # board. No DB creation, migration, corruption repair or automatic routing.
    path = guard.directory().parent / 'kanban.db'
    store.require(path.is_file() and not path.is_symlink(), 'Existing fleet task board required')
    kb._assert_not_delegated_child_mutation(path)
    placeholders = ','.join('?' for _ in tasks)
    with closing(connect_tracked(path.resolve().as_uri() + '?mode=rw', uri=True,
                                 isolation_level=None, timeout=5)) as conn:
        conn.row_factory = sqlite3.Row
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            rows = conn.execute(f'SELECT * FROM tasks WHERE id IN ({placeholders})', tasks).fetchall()
            store.require(len(rows) == len(tasks), 'Approved task projection contains missing cards')
            # Merely skipping recompute_ready is insufficient: the next scheduler
            # tick would ready historical descendants. Require the full open DAG
            # closure to be explicitly included in the approved projection.
            unselected = conn.execute(f'''
                WITH RECURSIVE descendants(id) AS (
                    SELECT child_id FROM task_links WHERE parent_id IN ({placeholders})
                    UNION
                    SELECT l.child_id FROM task_links l JOIN descendants d ON l.parent_id=d.id
                ) SELECT t.id FROM tasks t JOIN descendants d ON t.id=d.id
                  WHERE t.status NOT IN ('done','archived') AND t.id NOT IN ({placeholders})
                LIMIT 1''', tasks + tasks).fetchone()
            store.require(unselected is None, 'Unapproved open descendant prevents non-dispatching reconciliation')
            for row in rows:
                # Dispatch claims a row before its child PID is published. A
                # missing PID is not proof that this in-flight spawn is dead.
                unresolved = not row['worker_pid'] and (row['status'] == 'running' or row['claim_lock'])
                store.require(not unresolved, 'Unresolved worker claim prevents reconciliation')
                alive = row['worker_pid'] and kb._worker_alive(row['worker_pid'], row['worker_started_at'])
                store.require(not alive, 'Live task worker prevents reconciliation; never force completion')
            for row in rows:
                previous = conn.execute("SELECT payload FROM task_events WHERE task_id=? AND kind='maintenance_reconciled'",
                                        (row['id'],)).fetchall()
                if any(json.loads(event['payload']).get('operation') == value['id'] for event in previous):
                    store.require(row['status'] in ('done', 'archived'),
                                  'Reconciled task was reopened; explicit diagnosis required')
                    continue
                result = 'Maintenance workflow ' + value['id'] + ' completed; deployment must not be redispatched.'
                metadata = {'operation': value['id'], 'plan_digest': value['plan_digest'],
                            'qa_digest': execution['release']['qa_digest'], 'prior_status': row['status'],
                            'dispatch': False}
                run_id = None
                if row['status'] not in ('done', 'archived'):
                    run_id = kb._end_run(conn, row['id'], outcome='completed', status='done',
                                         summary=result, metadata=metadata)
                    if run_id is None:
                        run_id = kb._synthesize_ended_run(conn, row['id'], outcome='completed',
                                                        summary=result, metadata=metadata)
                    conn.execute('''UPDATE tasks SET status='done', result=?, completed_at=?,
                        claim_lock=NULL, claim_expires=NULL, worker_pid=NULL, worker_started_at=NULL,
                        block_kind=NULL, block_recurrences=0 WHERE id=?''',
                                 (result, int(time.time()), row['id']))
                    # Existing notifier/native-wake consumers observe this only
                    # after the entire approved DAG closure commits. No hooks,
                    # readiness recomputation or new dispatcher are invoked.
                    payload = kb._completed_event_payload(result, result, [], metadata)
                    payload.update(maintenance_workflow=value['id'], dispatch=False)
                    kb._append_event(conn, row['id'], 'completed', payload, run_id=run_id)
                kb._append_event(conn, row['id'], 'maintenance_reconciled', metadata, run_id=run_id)
    return {'status': 'complete', 'completed': tasks}
