# Maintenance workflow implementation record

## Status

Source implementation is complete, independently reviewed and committed. **291 tests pass. Not installed or active in the gateway.** This development performed no live SSH/HTTP/inference, production maintenance entry, credential change, model operation, service restart or live task mutation.

Branch/worktree: `design/maintenance-workflow`, `/home/hef/.hermes/hermes-agent/.worktrees/maintenance-workflow-design`.

- Approved design: `f4292bd05e`.
- Operation-bound release foundation: `55aaba6e3d`.
- Preparation CLI/store: `2612f1ed04`.
- Execution implementation commit: `24f132f0bc54bec56365e01f3420a2191c7ef6eb`.
- Runtime checkout remains `601e7f5a0c443f125efa0650845718c8ff7c016f` (read-only Git metadata check). No new loaded-gateway version is claimed.

## Implemented

- Full CLI: `plan`, `approve`, read-only `status`/`inspect`, explicit `inspect --live`, `preflight`, approved `run`, explicit `resume`, fresh-QA `release`, and independently retryable `reconcile`.
- Strict private fleet journal, immutable plan/approval/checklist digests, atomic evidence, per-operation process locking and validated execution/release/projection records. No guard migration or new permission authority.
- Fixed Spark-pair/eight-stream adapter reuses reviewed Nix apply/direct QA. Preserves model/context/image/settings; rejects mismatched reserve/source/config. Existing approved goal skips apply instead of redeploying.
- Preflight verifies current installed authorization expiry, exact restrictions, budget/margin, independent runtime/auxiliaries, source/controller/environment, actual identities and facade QA routes. Expiry coverage is rechecked after slow probes, before exclusion.
- Immutable Git-blob attestation of controller/admission/SSH helpers on both nodes. Fixed read-only metadata commands only; no generic remote command surface or automatic source installation.
- Actual Docker `StartedAt` in identity. Track all original/retained and approved candidate IDs before/after every mutation; only approved create/start transitions advance identities. Recheck final readiness, not just the first mutation.
- Intent-before-mutation and result evidence, fresh admission/drain, bounded transports, serialized concurrent QA callbacks, explicit no-blind-replay recovery and an independent matching-generation heartbeat/deadline monitor.
- Direct/facade text/SSE/tools/thinking/vision, observed eight-way decoding, preserved settings, both-rank health and unaffected secondary QA. Normal requests use the existing guarded transport; no global QA bypass.
- Separate release by default; case-specific conditional approval opt-in. Under-lock operation/executor/expiry comparison supplements fresh QA and actual identity checks. Post-release failure is degraded/released, not rollback or re-isolation.
- `reconciling` stage prevents false completion. Existing fleet-default-board projection requires the approved open descendant closure, rejects live workers/unresolved spawn claims, closes atomically and emits existing native completion events plus deduplicated audit events. No readiness hooks, scheduler, workspace cleanup or deployment replay.
- [Operator guide](../../maintenance-workflow.md) and [activation handoff](2026-10-09-maintenance-workflow-activation.md). Installed/global skills remain an explicitly deferred publication step outside this source-only worktree.

## Verification

Final complete integration: **291 passed, zero failed across 13 files**, after every review fix. Breakdown: release 6, store 25, CLI 5, engine/crash/projection 45, adapter 41, existing guard/runtime/worker/auxiliary suites 93, existing serving-classifier suites 76. Runner wall time: 14.7 seconds. Final source-contract check and `git diff --check` also passed.

Canonical command uses `scripts/run_tests.sh` (managed Python 3.14, per-file subprocess runner):

```sh
scripts/run_tests.sh \
  tests/hermes_cli/test_backend_operation_release.py \
  tests/hermes_cli/test_backend_workflow_store.py \
  tests/hermes_cli/test_backend_workflow.py \
  tests/hermes_cli/test_backend_workflow_run.py \
  tests/hermes_cli/test_backend_workflow_spark.py \
  tests/agent/test_backend_isolation.py \
  tests/agent/test_backend_facade.py \
  tests/agent/test_backend_request_hook.py \
  tests/agent/test_backend_dropped_response.py \
  tests/agent/test_maintenance_inference.py \
  tests/agent/test_git_diff_operands.py \
  tests/agent/test_serving_execution_policy.py \
  tests/agent/test_serving_command_semantics.py
```

Evidence includes:

- Foundation: 127 passing tests across eight files; real lock-boundary operation race, subprocess CLI, A/B/A profiles, corruption/private storage and immutable approval tests.
- Real SIGKILL at persisted intent, after mutation before response, after guard release before receipt, before task projection and after task DB commit before journal receipt.
- Eighteen before/after interruption cases across all nine controller mutations; no uncertain replay. Eight-thread progress persistence verifies ordering and evidence digests.
- Deadline interrupt during a blocked call; stale monitor cannot overwrite another executor/probe generation; QA/deadline expiry checked under the release lock.
- Node source drift, in-place restart, unsolicited candidate start, readiness-phase restart, retained-original restart and a full nine-mutation positive identity lifecycle.
- Atomic task closure, native completion event shape, duplicate projection, missing descendants, live workers and unresolved claims. No real notification was sent.
- Real Nix source import/contract and three approved dependency blob hashes at `5a9d85b2492c752efecf722eedd6ab1ac73f3f5e`. Credential reader, network connections and all non-Git subprocesses were forbidden. **Source check only**, not preflight or deployment acceptance.

All new stateful tests use explicit private guard and accounting roots, not HERMES_HOME alone. Serving/network/credential collaborators are doubles; test child kills target only owned fixture processes.

## Independent review and retained evidence

- Foundation reviewer: `01a1226d-761b-70f0-a3e4-30422cbaa1c1`; settings/status findings were reproduced red and fixed.
- Adapter implementation/review delegate: `01a1227d-8a6e-7202-b8ea-bcfe24e130a9`.
- Final independent reviewer: `01a1228f-103c-7dc1-8904-90e21e8dcba3` (gpt-5.6-sol/high, read-only). Initial findings covered false completion before reconciliation, weak persisted evidence validation, missing blocked-call watchdog and missing real crash boundaries. Follow-ups identified node dependency attestation and restart checks after the first mutation. All findings were fixed with regression evidence. Final targeted follow-up (`review-followup3.md`, exit 0) reported **no concrete blockers**, specifically accepting the every-mutation candidate/retained-container restart fence. Reviewer did not rerun tests; the coordinator independently ran the final 291-test integration afterward.

Raw local evidence is retained, not committed:

- `.review/maintenance-foundation/`
- `.review/maintenance-adapter/`
- `.review/maintenance-final/full-tests.log`, `source-contract.py`, `source-contract.log`
- `.review/maintenance-final/review*.md`, `followup*.md`, reviewer stdout/stderr and exit receipts
- Red logs: `adapter-boundary-red.log`, `facade-preflight-red.log`, `native-events-red.log`, `restart-red.log`, `node-source-red.log`, `claim-race-red.log`, `credential-window-red.log`, `mid-apply-restart-red.log`

## Remaining boundary

Source gates are complete. Implementation was committed with an explicit path list; `.review/` and the unrelated empty `LEARNINGS.md` template were inspected and left untracked. No live maintenance acceptance is claimed. Installation, external skill publication, any required supervised gateway activation, authorized real preflight and future case-specific maintenance remain separate actions.

The already accepted eight-stream deployment and its reconciled historical cards are not implementation test targets.
