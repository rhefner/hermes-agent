# Resumable Backend Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Keep all serving activity out of this development worktree.

**Goal:** Implement the approved workflow incrementally without changing the existing guard authority or performing production maintenance.

**Architecture:** Preserve backend request enforcement and existing controller admission. A shared-fleet operation journal binds plans, approvals and evidence; a CLI drives a fixed reviewed controller adapter. Operation-bound release is the first independently testable safety primitive.

**Tech Stack:** Python standard library, existing Hermes backend-isolation CLI, pytest through scripts/run_tests.sh, existing Nix Spark controllers.

## Execution boundaries

Design approved by the user October 9, 2026. Worktree: `/home/hef/.hermes/hermes-agent/.worktrees/maintenance-workflow-design`, branch `design/maintenance-workflow`. Runtime main is not a development target. No SSH, inference probes, credential changes, isolation, model mutation, service restart or task redispatch during tests. Explicitly override both isolation and accounting directories.

Implement and independently verify each batch before moving to the next. Do not report a journal or mocked adapter as a completed live maintenance workflow. Plans for later adapter integration must resolve the concrete transport and controller contracts before executable production wiring is added.

## File ownership

- `hermes_cli/backend_isolation.py`: existing low-level operations; add optional operation-bound release without changing legacy recovery behavior.
- `hermes_cli/backend_workflow_store.py`: private atomic operation records, per-operation locking, immutable digest-bound plan and approval validation.
- `hermes_cli/backend_workflow.py`: workflow CLI and read-only status; no permanent model tool.
- `hermes_cli/backend_workflow_run.py`: fixed workflow stages, intent/evidence writes, explicit recovery and release checks.
- `hermes_cli/backend_workflow_spark.py`: concrete reviewed Spark controller adapter, bounded preflight and QA contracts. No arbitrary executable paths in a user plan.
- `tests/hermes_cli/test_backend_operation_release.py`: real-state release and CLI race regressions.
- `tests/hermes_cli/test_backend_workflow_store.py`: operation/approval immutability and filesystem/process integration.
- `tests/hermes_cli/test_backend_workflow.py`: real CLI/import flows against temporary fleet state and deterministic host/controller doubles.
- User documentation and existing maintenance skill: update only after the CLI actually works.

## Batch 1 — Bind release to the actual maintenance window

**Completed:** commit `55aaba6e3d`; real-state API/CLI/race regressions passed. The checklists below retain the implementation recipe; execution evidence is in the progress report.

- [ ] Read CLI instructions and current guard fixtures.
- [ ] Add a real temporary-state fixture patching `agent.maintenance_inference.directory` and `agent.maintenance_admission.directory`; initialize a v2 guard using existing `migrate`/`enter` and a stub independent probe, not hand-written invalid state.
- [ ] Add a regression proving a stale operation cannot release a newer window: enter operation A, save its ID, explicitly release A, enter B, call `release(approval, expected_operation=A)`. Assert MaintenanceIsolationError and byte-for-byte unchanged B state. Positive control releases B with B's ID and preserves inventory.
- [ ] Add CLI coverage passing `--expected-operation` through the real parser. Wrong ID returns nonzero without writing state; matching ID releases normally.
- [ ] Run `scripts/run_tests.sh tests/hermes_cli/test_backend_operation_release.py` and record the expected red result before implementing.
- [ ] Implement the exact API change:

```python
def release(approval, *, expected_operation=None):
    # Existing approval validation stays before the lock.
    with locked():
        value = backend.configuration()
        # Existing missing/normal operation refusal stays here.
        if expected_operation is not None and value.get('operation') != expected_operation:
            raise guard.MaintenanceIsolationError('Maintenance operation changed; release refused')
        # Existing atomic normal-state write stays here.
```

- [ ] Add `p.add_argument('--expected-operation')`; forward it only to release. Reject its use with other actions rather than silently ignoring it.
- [ ] Verify stale comparison happens under the same existing lock as the write using a lock-boundary test that replaces the operation immediately before yielding the lock. Do not merely assert a mock was called or inspect source text.
- [ ] Run the new file and `tests/agent/test_backend_isolation.py`, `test_backend_facade.py`, `test_backend_request_hook.py`, `test_backend_dropped_response.py`, `test_maintenance_inference.py` through the canonical runner.
- [ ] Review diff and commit only this primitive and its tests. Do not deploy it.

## Batch 2 — Operation journal and read-only CLI

**Source-complete:** final combined run 127 passed, zero failed across eight files. Independent review findings were reproduced and corrected. Preparation-only; no controller execution is exposed.

- [ ] Define a versioned strict plan schema: explicit backend names, supported adapter ID, pinned controller identity, approved delta/preserved settings, required QA names, time budget/margin, and linked task IDs. Unknown fields, duplicate targets, arbitrary command lists, and invalid IDs fail closed.
- [ ] Canonical JSON SHA256 binds immutable plan and required QA; generate workflow IDs internally. Store under `guard.directory()/operations`, with private permissions and atomic durable replace. Do not overload or migrate guard state.
- [ ] Use a separate nonblocking per-operation lock for execution; a read-only status call must not create directories or files.
- [ ] Test corrupt/truncated records, traversal IDs, duplicate creation, immutable approval binding, concurrent process locking and A/B/A profiles against explicit temporary fleet/accounting roots.
- [ ] Add real CLI `plan`, `status`/`inspect`, and `approve` paths. Status reports guard observations separately from recorded workflow progress. Approval requires an operator reference and matching digest; default release policy is separate approval. Conditional policy names the exact QA checklist.
- [ ] Test subprocess CLI create/read/approve paths with real imports, not a mocked parser. Never label these preparation-only commands as an executable maintenance workflow.
- [ ] Verify with canonical runner; commit this batch independently.

## Batch 3 — Concrete Spark adapter contract

- [ ] Read the actual `glm53_eight_stream.py`, its QA runner, admission helper, restricted SSH checker and recorded facade-final QA invocation. Inspect source only; do not invoke scripts against hosts.
- [ ] Map existing public controller operations to explicit preflight, observe, apply, direct-QA, facade-QA and normal-routing checks. Resolve which code is reusable and which was ad-hoc coordinator code before implementing adapter calls.
- [ ] Source pins, model/context preservation, configuration backups, node ordering, credentials/expiry, dependency environment and executor probes must all have concrete output contracts. If a required QA capability lacks a callable reviewed implementation, extract it into a narrow separately tested supported adapter before allowing `run`.
- [ ] Test using realistic recorded Docker/config fixtures and a local HTTP facade/server fixture. No model calls or remote SSH. Prove read-only operations cannot fall through into apply or upstream startup/DRY_RUN paths.
- [ ] Record the supported operation set in documentation; reject all other adapters/settings rather than implying arbitrary model-update support.
- [ ] Commit only after real adapter/import/CLI integration tests pass.

## Batch 4 — Execute and resume safely

- [ ] Implement approved stage transitions around existing guard APIs. Preflight checks before entry, then fresh checks and admission at each mutation. Persist intent before action and verified result after action.
- [ ] Require the exact recorded guard operation and target set at each protected stage. Use existing process identity including start/boot identity, not PID existence alone.
- [ ] Bound controller subprocesses, capture redacted evidence and maintain progress heartbeats. No automatic retries of mutation or fallback to affected inference.
- [ ] Inject crashes before/after each mutation and before/after journal writes. Resume must observe actual state and either prove the next forward step safe or block with uncertainty. No automatic rollback, restart or release on failure.
- [ ] Verify the full run/resume path through the real CLI and temporary guard, with deterministic host doubles and current request-drain tests. Commit only after these paths actually execute.

## Batch 5 — QA-bound release and post-release verification

- [ ] Bind QA receipts to operation, plan, checklist, source/config/container identities and observation time. Missing, failed or stale direct/facade/unaffected checks block release.
- [ ] Implement separate approval as default and explicit per-operation conditional approval as opt-in. Never infer a release approval from implementation approval.
- [ ] Recheck executor, operation, identity and health; call the Batch 1 compare-under-lock release primitive. Keep identity/QA observations separate from the atomic operation check; do not claim it makes remote observations transactional.
- [ ] Test a replacement operation race, executor loss, restart/config drift, queued-only capacity and changed checklist; all retain isolation.
- [ ] Verify post-release normal primary and unaffected routes; failure records degraded/released without automatic rollback or re-isolation.
- [ ] Commit only after matching positive controls and fail-closed negative tests pass.

## Batch 6 — Task projection, docs and acceptance

- [ ] Read existing Kanban closure/dependency/dispatch APIs before wiring reconciliation. Never convert a blocked historical serving task to runnable as an intermediate step.
- [ ] Restrict reconciliation to operation-linked IDs, make it independently retryable, and test partial failures/duplicate events without repeating serving steps. Preserve native wake rather than adding another notifier.
- [ ] Update maintenance instructions to the actual supported CLI, credential expiry preflight, conditional release syntax and interrupted-operation recovery.
- [ ] Run all workflow tests plus deployed guard/dispatcher/classifier regressions. Record exact counts and interpreter/runner; retain failed results rather than replacing evidence with a success summary.
- [ ] Review changes against every requirement in the approved spec. Exercise inspection against local temporary production-shaped state without remote mutation; do not claim live deployment acceptance.
- [ ] Produce an activation handoff separating source-tested, installed and running-loaded versions. No automatic gateway restart, model update or maintenance entry.

## Coverage audit

Every approved spec section maps to a batch: operation authority/storage (2), explicit inspection/preflight (2–3), existing controller reuse (3), execution/recovery/status (4), approval and release (1,5), task reconciliation/wake/documentation (6). The live acceptance gate remains separate from source implementation.
