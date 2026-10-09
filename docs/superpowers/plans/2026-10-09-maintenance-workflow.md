# Resumable Backend Maintenance Implementation Plan

> Execute with the executing-plans skill. Keep all serving activity out of this development worktree.

**Goal:** Implement the approved workflow without changing guard authority or performing production maintenance.

**Architecture:** Existing request enforcement/controller admission remains authoritative. A private shared-fleet journal binds plans, approvals and evidence; an external CLI drives one fixed reviewed adapter. No scheduler, generic command runner or permanent model tool.

**Stack:** Python standard library, existing HTTPX/guard/controller dependencies, pytest through `scripts/run_tests.sh`.

## Execution boundaries

User approved the design October 9, 2026. Worktree: `/home/hef/.hermes/hermes-agent/.worktrees/maintenance-workflow-design`; branch: `design/maintenance-workflow`. Runtime main is not a development target. No live SSH/HTTP/inference, credential changes, production isolation, model mutation, restart or task redispatch. Explicitly isolate both guard and accounting roots in stateful tests.

Source implementation does not equal installation, running-loaded activation or live acceptance. The detailed evidence and final gate disposition belong in [the progress record](2026-10-09-maintenance-workflow-progress.md).

## Ownership

- `hermes_cli/backend_isolation.py`: compatible low-level release extension; existing guard lock.
- `hermes_cli/backend_workflow_store.py`: plan/approval/execution validation, private journal/evidence, process lock and heartbeat observation.
- `hermes_cli/backend_workflow.py`: real CLI, read-only status and next-action guidance.
- `hermes_cli/backend_workflow_run.py`: stage transitions, scoped execution, intent/results, fresh QA/release and explicit recovery.
- `hermes_cli/backend_workflow_watch.py`: independent matching-generation heartbeat/deadline monitor; never a permission authority.
- `hermes_cli/backend_workflow_spark.py`: fixed source-pinned controller/credential/metadata/QA adapter.
- `hermes_cli/backend_workflow_tasks.py`: non-dispatching native task projection.
- `tests/hermes_cli/test_backend_operation_release.py`, `test_backend_workflow_store.py`, `test_backend_workflow.py`, `test_backend_workflow_run.py`, `test_backend_workflow_spark.py`: behavioral and crash regressions.
- `docs/maintenance-workflow.md`: supported operator interface; external/global skill publication belongs to activation, not this worktree.

## Batch 1 — Operation-bound release

Completed in `55aaba6e3d`, with later executor/expiry hardening in the execution batch.

- [x] Read existing CLI/guard fixtures; use real temporary v2 state and explicit guard/accounting roots.
- [x] Observe failing stale-operation API/CLI regressions before implementation.
- [x] Compare expected operation under the existing state lock; preserve the legacy explicitly authorized recovery release.
- [x] Reject `--expected-operation` on unrelated low-level commands.
- [x] Exercise replacement exactly at the lock boundary, not merely an asserted mock call.
- [x] Extend optional under-lock checks to expected executor and QA/budget `not_after`; test expiry at lock acquisition.
- [x] Run focused and deployed guard regression suites. No deployment.

## Batch 2 — Journal and preparation CLI

Completed in `2612f1ed04`; foundation integration passed 127 tests across eight files.

- [x] Strict supported plan schema, literal settings, internal IDs and canonical plan/QA digests; reject arbitrary commands/plugins/settings.
- [x] Private fleet-scoped atomic durable journal/evidence and nonblocking per-operation execution lock; no guard schema migration.
- [x] Existing v2 enrollment required before planning creates journal storage.
- [x] Corruption, traversal, duplicate IDs, approval immutability, process-lock contention, private permissions and profile A/B/A coverage.
- [x] Real subprocess `plan`, `approve`, `status`, `inspect` flows; actual guard state overlays historical progress.
- [x] Separate release default and case-bound conditional opt-in; no execution capability claimed in the historical preparation-only checkpoint.
- [x] Independent review corrections reproduced red and verified green.

## Batch 3 — Concrete Spark adapter

- [x] Read actual Nix controller/QA/admission/SSH sources and prior facade QA; never execute them against hosts during development.
- [x] Map concrete preflight, observation, apply, direct/facade QA and guarded normal-route contracts; preserve reviewed launch/backup/order logic.
- [x] Pin local revision/clean source and both nodes' controller/admission/SSH bytes against immutable approved Git blobs.
- [x] Match exact preserved settings and observed reserve; reject drift rather than silently overriding the plan.
- [x] Read installed restricted authorization metadata, not installer expiry constants; verify budget/margin and recheck after slow independent probes.
- [x] Check fixed facade route capability before isolation. Reusable bounded text/SSE/tools/thinking/vision transport, no retries/redirect/proxy inheritance.
- [x] Track actual container/config/local hashes and `StartedAt`; verify originals, retained containers and approved candidates around every mutation and final readiness.
- [x] Deterministic Docker/controller/HTTP transport fixtures, including real guarded HTTPX transport/reader locks and a full nine-mutation positive lifecycle. No remote execution is claimed.
- [x] Real source import/constant/blob contract at Nix `5a9d85b2492c752efecf722eedd6ab1ac73f3f5e`, with credentials/network/non-Git subprocesses forbidden.
- [x] Document the one supported operation and reject unsupported targets/adapters; do not imply generic model-update support.

## Batch 4 — Execution and explicit recovery

- [x] Approved transitions around existing entry/probe/drain APIs; current exact scope, operation and PID/start/boot identity.
- [x] Durable intent before mutation and verified result afterward; redacted recovery IDs/backups and serialized concurrent callbacks.
- [x] Bounded controller/HTTP operations plus independent heartbeat/deadline interruption of only the matching executor/probe generation.
- [x] No automatic mutation retries, rollback, affected-inference fallback or gateway activation.
- [x] Eighteen before/after mutation-boundary cases; uncertain partial apply blocks and completed goal must be freshly observed before skipping replay.
- [x] Real child SIGKILL at intent and lost-result boundaries; explicit resume rebinds executor and reruns fresh QA.
- [x] Real CLI/import/state flow with temporary fleet state and serving doubles; distinguish lock/heartbeat activity from completion.

## Batch 5 — QA-bound release

- [x] Bind complete passing checks to workflow, plan, checklist, guard operation, source/config/container/start identities and time.
- [x] Default separate approval and per-case conditional release; implementation approval is never release permission.
- [x] Recheck actual identities, executor, health and receipt freshness before under-lock operation/executor/deadline comparison. Do not claim transactional remote observation.
- [x] Reject missing/failed/stale QA, drift/restart, queued-only capacity and mismatched scope; retain exclusion.
- [x] Prove normal primary/unaffected routes with supported guarded clients after release; report degraded/released on failure without rollback/re-isolation.
- [x] Real SIGKILL after guard release/before receipt; recover verification without repeating deployment.

## Batch 6 — Task projection, docs and source acceptance

- [x] Read existing closure/dependency/dispatch/event APIs; do not call readiness recomputation, lifecycle hooks or cleanup.
- [x] Approve full open descendant closure; atomically close selected tasks with existing native completion events and deduplicated audit events.
- [x] Refuse live workers and unresolved pre-PID spawn claims; independently retry after the existing lifecycle settles.
- [x] Real SIGKILL before projection and after DB commit/before journal receipt; no duplicate completion or serving replay.
- [x] Explicit `reconciling`/`bookkeeping_pending`; only durable exact projection receipt permits `complete`.
- [x] Operator guide and activation handoff covering credentials, approval syntax, supported scope, recovery blockers and publication of external maintenance instructions.
- [x] Final whole integration rerun after every fix: 291 passed across 13 files; source-contract and diff checks passed. Exact runner/counts and red logs retained.
- [x] Final independent review: no concrete blockers. Implementation committed as `24f132f0bc54bec56365e01f3420a2191c7ef6eb`. Engine, adapter, watchdog and projection ship together as one atomic executable capability, not a half-wired `run` command.

## Coverage and rollout boundary

Approved spec mapping: authority/storage (1–2), inspection/preflight/controller reuse (2–3), execution/recovery/status (4), QA/approval/release (1,5), projection/native wake/docs (6). Narrow restart/source/claim/time-window follow-ups strengthen these same gates rather than broaden scope.

Live acceptance remains separate. Installing this source does not enter maintenance. Future authorized integration starts with inspection/preflight only; any real operation and any supervised gateway reload need their own authorization. The completed eight-stream rollout and historical task cards must not be replayed.
