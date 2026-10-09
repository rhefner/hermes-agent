# Backend maintenance: one resumable workflow

Status: user approved October 9, 2026; source implementation complete and independently reviewed on an isolated branch (291 tests passed), not deployed.
Base: Hermes 601e7f5a0c443f125efa0650845718c8ff7c016f.

## Goal and approved direction

Make future enrolled Spark model updates predictable without relaxing backend isolation. Replace repeated agent improvisation with a small CLI workflow around the existing guard and reviewed serving controllers. Do not introduce another enforcement engine, generic job scheduler, or core model tool.

The user approved the high-level direction on October 9: prepare before isolation, one operation/status, explicit read-only inspection, resumable execution, credential preflight, and optional case-specific conditional release. This approval authorizes development, not production maintenance or a standing release permission.

## Non-negotiable requirements

- Coordinator, workers, fallback and critical auxiliaries must not use an affected backend. Existing shared-profile and request/stream enforcement stays intact.
- Unaffected backends remain available; explicit enrolled targets are mandatory.
- Mutations use current admission and actual response drain, never a saved receipt alone.
- Preserve the approved model, context, image and other settings except the explicitly approved delta.
- No automatic rollback, context reduction, credential renewal, or broadening of scope.
- Release requires case-specific approval: either separate post-QA approval or advance conditional approval bound to the exact operation and QA checklist. Default is separate approval.
- Failure, lost executor, ambiguous state, stale evidence or failed QA never triggers release.
- Gateway restart remains an externally supervised, separately authorized action; a chat process never restarts itself.
- This remains a cooperative guard for supported clients/controllers, not a same-user sandbox or a guarantee about arbitrary external programs.

## Existing components to reuse

- `agent/backend_isolation.py`: `enter`, `observed`, `require_mutation`, request locks and independent probe.
- `hermes_cli/backend_isolation.py`: current inspection/entry/resume/release CLI.
- `hermes_cli/maintenance_inference.py`: shared locking and atomic writes.
- Nix Spark `maintenance_admission.py`: explicit host-to-backend admission for each serving mutation.
- Existing reviewed Spark runners and direct/facade QA: first concrete operation adapter; do not rewrite their launch logic.
- Current command semantics classifier: keep as a secondary guard; do not grant blanket Python, SSH, test, or plugin exemptions.
- Existing Kanban APIs: bounded task reconciliation only, never an additional dispatcher.

## Scope and interface

Extend the existing backend-isolation CLI family with a workflow entrypoint rather than a new permanent model tool. Implement small modules for operation storage/validation and workflow execution, following the CLI's existing conventions.

Implemented user-facing operations (exact syntax in `docs/maintenance-workflow.md`):

- `plan`: resolve targets and a supported controller, produce a digest-bound change plan and required QA checklist; no serving writes.
- `inspect`: read journal/guard without writes or network; `inspect --live` additionally reads bounded controller/credential metadata, without inference or serving mutation.
- `preflight`: run bounded prerequisite checks and explicit independent runtime/auxiliary probes; no serving writes or isolation.
- `approve`: record trusted operator approval evidence for the exact plan and release policy; it does not execute changes.
- `run`: execute the approved sequence, stopping at unmet gates.
- `resume`: explicitly recover an interrupted operation after observing actual state and verifying the new executor; never blindly repeat mutation.
- `release`: validate approval and fresh QA, then release only the matching guard operation.
- `status`: report operation progress, actual guard state, executor/heartbeat liveness and next required action.
- `reconcile`: retry only the approved task projection after serving verification, without constructing a serving adapter.

The entrypoint is `python -m hermes_cli.backend_isolation workflow`; existing low-level actions remain compatible.

No arbitrary shell-command list is accepted as a maintenance plan. The first adapter calls reviewed, source-pinned Spark controller operations using explicit arguments. Unknown controllers or targets refuse. Adapter code is part of the reviewed trusted codebase, not dynamically loaded plugin code.

## One operation, not two guards

Keep the current guard state and schema unchanged as the sole authority for exclusions and mutation admission. Add an operation journal under the configured shared fleet directory, never a profile-local HERMES_HOME directory.

The journal records:

- Workflow ID; associated guard operation ID after entry; exact target set.
- Plan digest, approved settings delta and preserved settings, controller/source identities.
- Operator approval reference and text, release policy, QA checklist digest.
- Executor identity, step intents/results, observation timestamps and evidence paths/digests.
- Original/replacement container identities and start times, pinned node dependency hashes, protected backup references and recovery observations.
- Related task IDs and reconciliation result.

Never store credentials, raw environment dumps or confidential prompt bodies. Use private permissions, atomic writes and a per-operation execution lock. Concurrent runners for the same operation refuse. The existing guard prevents concurrent conflicting entry.

The journal is authoritative for workflow progress, not for backend permissions. Status always overlays live guard observations; a journal saying complete cannot conceal active isolation. Kanban is a projection of operation progress, not permission to run or repeat deployment.

## Sequence and gates

`planned -> preflight_ready -> approved -> isolated -> applying -> verifying -> awaiting_release -> released -> reconciling -> complete`

These are workflow stages, not new guard phases. Failures record a blocked/interrupted condition and the last verified stage rather than inventing another successful transition.

### Before blocking traffic

Preflight checks source and controller compatibility; configured targets; main/auxiliary independent execution; dependency environment; credential validity and expiry; source pins; current launch/config observations; reviewed backup method; and required direct/facade QA capability.

Credential coverage must exceed the declared execution-plus-QA time budget and safety margin, including a recheck after potentially slow independent probes. Insufficient lifetime blocks before isolation and requests a specific renewal. There is no implicit renewal. Credential expiry during maintenance stops new protected actions; it does not release traffic.

Read-only inspections are fixed supported operations with bounded arguments and output redaction. Source-only fixture tests run with explicit temporary guard/accounting directories and cannot access production state. Do not run upstream startup/DRY_RUN scripts merely because they claim to be read-only.

Preflight evidence is bound to the plan and observed identities. Recheck credentials, independent execution, source/config drift and target health at entry; recheck admission and relevant identities before each mutation. Preflight is not a reusable authorization receipt.

### Execution and failure

Use existing entry/probe/drain mechanisms. Keep one live independent executor for the operation. Write the next step's intent before performing a mutation; save verified results afterward.

If a process dies between action and receipt, resume inspects real container/config identities and controller transaction evidence. Only resume from a demonstrated safe state. Unknown or partial outcome blocks with a specific recovery action; never automatically rerun a stop/create/start sequence or restore an old model.

Recoverable forward steps inside the approved delta may proceed without asking routine implementation questions. Scope expansion, rollback, gateway restart, credential changes or new model changes require the corresponding explicit authorization.

Long steps have bounded timeouts and an independent heartbeat/deadline monitor, fenced to the matching operation/executor/probe generation. Expiry interrupts admission without releasing traffic or killing/restarting serving processes; already-issued bounded I/O may still settle. Status distinguishes running, ready/queued, blocked, interrupted, awaiting approval and completed using actual executor/worker identity. A dead worker cannot remain reported as running.

### QA and release

Required QA is declared before approval, including direct engine checks, facade-path checks, preserved configuration, both rank health, and unaffected-backend availability. Operation-specific overlap/capacity checks must observe engine execution rather than accepted request counts. Long-context capacity is not inferred from a short test.

Evidence binds to the operation, plan, deployed code/config/container/start identities and checklist. Node controller/admission/SSH helpers are compared to immutable approved Git blobs before isolation and each mutation/direct QA request. Original/retained and candidate start identities are checked around every mutation and final readiness; only approved create/start transitions advance them. Configuration drift, a restart, failed checks or a changed checklist invalidates relevant evidence and prevents conditional release.

QA of an excluded backend uses the existing explicitly authorized QA transport, never a global temporary bypass or normal agent inference. If a supported facade QA path is unavailable, preflight reports that before isolation; do not invent one during execution.

For conditional release, verify that every required check passed, approval still matches the plan, the executor is valid, and current health/identities match. Then use the existing release implementation with optional expected-operation, expected-executor and QA/budget expiry checks performed under its state lock. The compatible extension closes the old approval-text-only comparison gap; checks outside the lock are insufficient.

After release, verify normal primary and unaffected routing through supported clients. If this post-release check fails, report a degraded/released operation, not success; do not automatically roll back or re-isolate without appropriate authorization.

The existing low-level release remains available for explicitly authorized operator recovery; the workflow does not claim to prevent a trusted same-user operator from using it directly.

## Task reconciliation and notifications

Accept only task IDs linked to this operation. Report completion from verified evidence; never unblock a historical deployment merely to fix its label. Use existing Kanban non-dispatchable/closure controls in an order that cannot make stale mutation tasks runnable. Reconciliation failure leaves a visible `reconciling` / `bookkeeping_pending` state; only its durable exact receipt permits `complete`. It never repeats deployment or withholds a safely approved release. The first implementation targets the existing fleet default board, requires the explicitly approved open descendant closure, and refuses live workers or unresolved pre-PID spawn claims. It does not pause unrelated scheduler activity or run readiness/lifecycle/cleanup hooks.

Send progress on stage changes, real blockers, and completion, not a stream of obsolete retry notifications. Existing event/task IDs provide deduplication. Reuse native completion wake; do not add another wake mechanism. Reconciliation and notification failures are independently retryable bookkeeping steps.

## Explicit exclusions

No new fleet schema migration, replacement of backend request hooks, regex/parser rewrite, generic DAG framework, permanent credentials, new authorization service, arbitrary script sandbox, broad refactor, automatic gateway activation, or live model change is included in this implementation.

## Verification and rollout

Use canonical `scripts/run_tests.sh`. Every test explicitly overrides both fleet isolation and accounting directories; HERMES_HOME alone is insufficient.

Required behavioral coverage:

1. Missing/expired credentials, source drift or unavailable executor block before exclusion or serving writes.
2. Profile A/B/A resolves the same fleet operation while test state remains temporary.
3. Read-only inspection and fixture tests never enter isolation; genuine serving actions still require existing admission.
4. Wrong backend, stale executor, missing drain and concurrent operation are refused.
5. Crash before/after each mutation cannot cause an unverified repeat; uncertain outcomes stop and preserve exclusions.
6. Default release waits for approval; conditional release requires matching plan/checklist, all QA, unchanged identities and fresh health.
7. A racing different operation cannot be released using an old operation's approval.
8. Failed direct/facade QA, queued-only capacity and source/config drift never count as successful acceptance.
9. Post-release verification failure is reported truthfully without rollback or unauthorized re-isolation.
10. Task reconciliation cannot dispatch old deployment work, including duplicate completion events and partial reconciliation failures.

Exercise real CLI/import/state transitions with temporary directories and deterministic host/controller doubles, plus the existing guard regression suites. Do not claim mocks prove live model deployment. First integration runs inspection/preflight only under the deployed policy. A future real update supplies separate production authorization and live acceptance; installing this workflow does not start maintenance.

Keep development on an isolated branch/worktree. No running gateway restart or model operation is required for the design. Any eventual activation requiring a gateway restart receives a separate handoff/approval.

## Design review checklist

- [x] Recover user requirements and recent operational failures.
- [x] Inspect current admission, release and interruption behavior.
- [x] Compare parser exceptions, unified workflow and OS-level isolation.
- [x] Record the approved high-level direction and conservative approval defaults.
- [x] Write concrete scope, gates, recovery semantics and acceptance criteria.
- [x] Self-review: preserve one guard authority; operation-bound release under lock; no silent post-failure actions; no placeholder sections.
- [x] User reviews this written design.
- [x] Create the implementation plan, then implement and test in isolation.
- [x] Finish final independent source review/integration gates; installation and live acceptance remain separate.
