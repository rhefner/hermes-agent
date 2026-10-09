# Maintenance workflow: source-to-activation handoff

## Boundary

This handoff does **not** authorize installation, maintenance, credential renewal, task mutation or any restart. The implementation was developed in `design/maintenance-workflow`, worktree `/home/hef/.hermes/hermes-agent/.worktrees/maintenance-workflow-design`.

- Source implementation commit: `24f132f0bc54bec56365e01f3420a2191c7ef6eb`.
- Runtime checkout HEAD, read as source metadata during handoff preparation: `601e7f5a0c443f125efa0650845718c8ff7c016f`.
- Running-loaded gateway version: **not re-inspected or changed by this development**. The earlier externally supervised activation receipt records `601e7f5a0c443f125efa0650845718c8ff7c016f`; a checkout SHA alone is not loaded-process evidence.
- Reviewed Nix controller source: `5a9d85b2492c752efecf722eedd6ab1ac73f3f5e`. Source import/contract and approved dependency blobs were checked with network, credential reads and all non-Git subprocesses denied. No real preflight was run.
- No guard migration, inventory change, live Kanban update, model operation, credential renewal or service restart occurred.

The eight-stream rollout is already complete. Historical acceptance recorded the preserved model, 1,048,576 context, eight decoding slots and 1,671,168 KV tokens. **Do not replay that deployment or redispatch its old task cards.** These are historical facts, not new live observations.

## What is ready to activate

A standalone external CLI orchestrates the existing guard and the fixed Spark-pair adapter. See [the operator guide](../../maintenance-workflow.md) for exact commands, plan schema, separate/conditional release and recovery.

There is no new service, model tool, task dispatcher, notifier or schema migration. The guard remains v2. Low-level release keeps its legacy recovery interface and adds optional under-lock operation/executor/expiry comparisons. New operation journals are separate files under the existing fleet directory.

The implementation uses existing dependencies. Controller files and adjacent node-side admission/SSH helpers must already match approved Git blobs; the adapter **will not install or repair remote code**. It also uses fixed read-only Docker `StartedAt` metadata to detect same-container restarts, including during individual apply steps and after readiness. Original/retained identities never silently advance.

## Safe activation sequence — after separate authorization

1. Identify the approved implementation commit and preserve unrelated work. Do not blindly merge/cherry-pick from a dirty checkout. Do not copy guard state or change either Spark-1 alias.
2. Prefer a **fresh external process rooted in the reviewed checkout** for this CLI. Installing source into the runtime checkout is a separate change. A standalone CLI does not need the Telegram gateway to import these modules or restart itself.
3. If an installation/update requires a gateway reload, obtain explicit authorization and hand off to its existing external supervisor. Drain/stop/restart using the established procedure, then prove the new PID/start identity and loaded SHA. Never infer loaded code from Git HEAD or restart from inside the chat process.
4. Publish the operator-guide link in the centrally managed maintenance instructions/skill as part of activation. Installed/global skills were outside this source-only worktree and were not edited. Do not add a permanent model tool or blanket shell/parser exemption.
5. Inspect the existing v2 guard/inventory read-only. Preserve Telegram session `20261005_172751_2cb691da`, fleet/accounting history, both Spark-1 aliases, current model/context and task history. Unexpected state is a blocker, not a reason to migrate, delete state or restore a backup automatically.
6. With explicit permission for read-only host/HTTP inspection and independent cloud probes, prepare a **new case-specific** plan and run `preflight`. This performs no exclusion or serving mutation. Confirm exact local/node source, actual identities, QA routes, independent runtime/auxiliaries and credential coverage.
7. Stop at any source/dependency/auth/expiry/health mismatch. The historical restricted SSH authorization expired **2026-10-09 18:28:50 UTC**, unless renewed elsewhere. Only installed metadata establishes current expiry. Any needed renewal or remote source update needs its own authorization; never alter expiry or install files silently.
8. Do not run `run`, `resume` or `release` for validation alone. They can enter isolation and issue real QA inference, even when the approved target is already applied. A future operation needs an exact plan digest, operator approval, scoped budget and separate release approval by default. Conditional release must be explicitly selected for that case.
9. Supervise any authorized executor outside the gateway. Do not terminate it with a short chat-tool timeout. Missing heartbeat/dead executor/budget expiry blocks later actions and retains isolation; bounded in-flight I/O may still settle.
10. Verify fresh QA and post-release normal routes for the authorized case. Only then reconcile its explicitly approved task closure. Live workers/unresolved spawn claims must settle; do not force task completion or ready historical children.

An already-applied target is observed and QA-checked without calling controller apply. **That is not permission to create a new operation for the old rollout.**

## Verification evidence and limits

Final canonical verification: **291 tests passed, zero failed across 13 files**; source-contract and whitespace checks passed. Independent final review reported **no concrete blockers**. The progress report contains the breakdown and review history. Local raw evidence is retained under `.review/maintenance-final/` and `.review/maintenance-adapter/`; it is intentionally not shipped as runtime code.

Source tests exercise real private guard/accounting state, journal locks, subprocess CLI, real child SIGKILL boundaries, all nine mutation interruption boundaries, concurrent QA callbacks, worker-claim races and atomic/idempotent native task events. HTTP/SSH/inference are deterministic doubles. Node source/`StartedAt` command construction is tested without executing SSH.

The local Nix source-contract check proves import compatibility and fixed constants/blob identities only. It does **not** prove deployed node bytes, current credentials, host connectivity, successful Docker operations, live eight-way decoding, million-token concurrency or delivery of a real native wake.

## Operational stop rules

- Partial/unknown apply: keep exclusions; inspect retained evidence and actual containers. No blind replay, rollback or container restart.
- Source/model/context/identity drift: stop. No silent plan adjustment.
- Failed/stale QA or lost executor: no release. Explicit resume repeats fresh QA.
- Released but failed normal-route checks: report degraded/released, without automatic rollback or re-isolation.
- `reconciling` / `bookkeeping_pending`: retry only `workflow reconcile`; never re-run deployment to fix task labels.
- Any new maintenance while an older journal says complete: actual guard state wins.
