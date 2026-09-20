# Wake outcome reconciliation: current-base integration

SOURCE ONLY. Base `cf30144906b5f6e4da12f3bdc281293f7ed67782`.
No installed source/config, Nix repository, service, subscription, credential,
model route or production database was modified. No live canary was sent.
Independent source approval is required before coordinator task t_c98e0595
owns deployment and the separately approved restart/live verification.

## Native facilities retained

- Native Kanban notifier collects durable events and maintains its own cursors.
- Existing push adapter admission, busy/FIFO queues, runner, strict session
  metadata and native compression ancestry remain authoritative.
- Native `delivery_ledger` owns finalized-message retries/recovery. The new
  `wake_delivery` bridge only binds its exact obligation to event receipts,
  gates observed-wake recovery against origin ownership and observes its ACK.
  It does not dispatch tasks, replay models, or send an alternative final.
- Native session housekeeping invokes the finite model-free reconciler. A
  default-only external timer proposal gives it an independent process when
  the gateway cannot run. Both consumers share the same persisted notice claim.
- Existing cron 0dd69b29a808 remains unchanged: its periodic task-progress
  inspection complements, but cannot replace, admitted-wake outcome evidence.

Historical source selectively salvaged from ea032d65c60750f1638373669bc3c596bcbbffbe,
d6744186e50222b50ba9ce1474f960f3a06c5f5b and reviewed queue repair
7114a2d35e8e173c4fc648888f0487d19b38423c. No blanket cherry-pick.

## API and schema mapping

`deliver_wake(..., identity=tuple_or_list, session_id=original_id)` and
`admit_internal_event(..., identity=..., session_id=...)` opt into observation
ONLY when `gateway.wake_outcomes.enabled` is true. Calls without an identity
and non-push/API-server behavior stay native. Existing notification-category
and served-profile parameters are preserved.

Kanban identities are `(kanban, board_slug, task_id, event_id)`; coalesced wakes
retain every identity. Subscription `delivery_metadata.origin_session_id`
comes from `HERMES_SESSION_ID` when subscribed through tools. Missing legacy
origins are never inferred from task.session_id (often the worker). Coordinator
must migrate existing subscriptions from historical evidence before activation.

Private `wake_outcomes/receipts.sqlite3` (directory0700/file0600):
- v1 `receipts`: hashed producer identity, original route/session key/id,
  state, attempts, execution generation, timestamps and separate notice budget.
- `transitions`: event-scoped lifecycle checkpoints, no prompts/content/errors.
- `final_links`: exact native obligation id and its owning ledger home.
- Admission: never_admitted -> admitting -> admitted; execution: executing;
  outcome: delivered, failed, uncertain, suppressed, cancelled, closed_boundary.
  A delivered row requires correlated positive transport evidence, not a later
  human reply or merely an accepted model result. Failed means no verified final;
  it does not prove the remote server received nothing.
- A pre-admission durable claim is a replay barrier even after a process crash.
  Only explicit non-admission can consume the second admission attempt.
- Original identity dedup is independent of destination changes, including
  compatibility lookup of historical v1 receipt IDs. No origin is rewritten.
- 30-day expiry clears receipt route/session/stage history and retains identity
  tombstones. Native binding/link hashes and owner-home references remain as
  conservative recovery bookkeeping; no response content is duplicated there.

Native state.db gains `wake_delivery_bindings(obligation_id, home, receipt_id)`
only when an observed final is recorded. Reserved `wake-` obligation IDs fail
closed if their binding is absent. Each wake batch supplies a unique ledger
message reference, so identical text in different wakes cannot collide.
Native `mark_delivered` settles only its bound receipts; a crash after native
ACK but before observer update is recovered by exact read-only ACK lookup.

Origin checking reuses native compression-chain selection, rejects closed/reset/
branch/delegate origins and changed route/profile/config routing, and compares
the original routing snapshot. NULL/empty default-profile spellings are treated
as the same canonical default profile, not as authority for another profile.
Strict runner guards recheck queued events; final/stream delivery and native
observed-final recovery recheck ownership. Unknown ownership never authorizes
execution or a failure notice. These are checks immediately before transport,
not an atomic transaction with a remote messaging server: a reset racing an
already-in-flight network request cannot retract that request.

## Configuration

Defaults registered in `hermes_cli/config_defaults.py` (disabled):

    gateway:
      wake_outcomes:
        enabled: false
        admission_seconds: 300
        idle_seconds: 1800
        send_seconds: 30
        retention_days: 30
        batch_size: 100

Reconciler examines finite oldest-checked batches. Progress extends the idle
clock; wall-clock execution age alone is not a stall. It never invokes a model.
The notice claim is committed before transport. Success is terminal; uncertain
notice delivery allows ONE explicitly labelled recovery notice after the lease,
then stops. Definitive failure is retained, not endlessly retried. Closed
boundaries stop; unknown boundaries preserve budget. Fair polling prevents an
unknown or healthy row from starving later failures. No exactly-once claim:
transport acceptance and SQLite commit cannot be atomic. Native final retry
markers/budgets are unchanged and may produce a labelled duplicate FINAL, never
replay a model or tool side effect.

Observed events are not restart-spooled; their active-turn marker disallows
automatic agent continuation. A later fresh human turn can resume normally.
Busy/coalesced events stay on the existing event-scoped adapter drain so a later
human final cannot settle the opening wake. Late steer has its own unobserved
event. The runner drain gate discards queued work rather than spawning it.

## Verification and limitations

`queue-red.log` reproduces 13 failures before queue/stream repair: real Telegram
adapter + real runner executed and sent busy wakes while receipts remained
admitted, and unrelated replies/drain boundaries were wrong. `base-red.log`
also records missing-module collection failures on exact base; that log alone
is NOT the behavioral red proof. `final-tests.log` records the first broad run
with a canary default-profile-alias failure; fixed without weakening named-profile
boundaries. Final passing log is named in HANDOFF.md.

New tests use real adapter/runner/temporary SQLite and native notifier collection,
substituting model executor and Telegram transport. They cover completed/review/
blocked/crashed, no-inbound idle delivery, busy/coalesced/streamed queues, no-start,
no-final stalls, bounded monitor notices, native final recovery, post-ACK crash
gap, duplicates, unknown/closed/reset/profile/route origins, verified compression
and rejected forks, unrelated replies, late steer and drain task-spawn gates.
Historical unit fault-injection tests additionally cover atomic concurrent notice
polling and unknown-vs-closed boundary races. These are source evidence, NOT live
transport or production restart evidence. Nix proposal was syntax-parsed only;
coordinator must integrate/evaluate it against the real fleet flake.

## Coordinator integration (not executed)

1. Review exact-base patch/bundle, verify installed base/clean tree again. Preserve
   current gateway/GLM workers and Astra session override. Use approved source
   delivery workflow; do not reset installed checkout to this scratch clone.
2. Reconstruct subscription origins historically; do not set them to whichever
   session happens to own the route now. Unknown or genuinely closed originals
   need an explicit owner decision, not a guessed reassignment.
3. Import `docs/proposals/hermes-wake-outcomes.nix` into the default llm-lab home
   configuration, enable its option and verify the fleet's Nix activation diff.
   It replaces the same service/timer names; do not add a parallel monitor.
   Launcher lives in the reviewed source, NOT a backup directory. No gateway
   restart dependency is declared. The unit's120s hard bound can interrupt a
   large outage batch; persisted uncertain claims and finite budget survive.
4. Run the launcher `--check`. Enable only default outcome configuration via
   Hermes config CLI. Restart the gateway only through the separately approved
   gateway-maintenance path, after active-worker protection. Verify actual loaded
   source SHA, native notifier, external unit import and no restart loop.
5. Keep failure notices bounded; do not replay uncertain/admitted historical
   work. Disable observation before rollback, retain receipts and bindings, and
   resolve live obligations. An older binary does not know these new recovery
   guards: drain/reconcile observed obligations before downgrading it.

## One live no-inbound canary (coordinator only; NOT sent)

The script has an exclusive durable marker BEFORE any board mutation. A crash
or existing marker requires inspection, never another prepare call or deletion
to force a rerun. It creates one inert unassigned blocked card, subscribes in
wake-only mode to the verified original default Telegram thread16755, and
completes it through the native lifecycle. No worker is started, no raw message
is sent. The real gateway must wake and send the resulting single test final
without user input. Failure notices, if needed, remain explicitly bounded.

Resolve ORIGINAL_SESSION_ID and DEFAULT_BOARD_DB from coordinator's verified
historical origin and board, not this document. From an independent terminal
with the verified default profile active:

    cd /home/hef/.hermes/hermes-agent
    venv/bin/python scripts/wake-canary.py prepare \
      --home /home/hef/.hermes --db "$DEFAULT_BOARD_DB" \
      --session-id "$ORIGINAL_SESSION_ID" --board default \
      --authorize-one-live-canary

End the originating coordinator turn; do not send a human ping. From the
independent process, run the read-only verifier:

    venv/bin/python scripts/wake-canary.py verify \
      --home /home/hef/.hermes --timeout 900

PASS requires exactly one native event receipt, delivered, execution generation
present, attempts=1. Also verify the actual no-inbound timeline and platform
final acceptance. Failure is NOT PROVEN; do not retry the canary automatically.
The entire prepare->notifier->runner->transport->verify path was exercised in
temporary SQLite with fake Telegram transport, including negative pre-delivery
verification and refusal of a second prepare. No production canary is consumed.
