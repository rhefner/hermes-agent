# Maintenance workflow implementation checkpoint

## Status

Source-only implementation of batches 1 and 2. Not merged, installed or active in the gateway. The full approved workflow is NOT complete. No production operation, credential renewal, SSH call, model inference, isolation change, model restart, or gateway restart was performed by this implementation work.

## Implemented

- Optional expected-operation release comparison under the existing state lock; legacy explicit recovery release remains available.
- Real preparation CLI under `python -m hermes_cli.backend_isolation workflow`: `plan`, `approve`, `status`, `inspect`.
- Private fleet-wide operation records, source/plan/QA digests, per-operation exclusive locking and atomic writes.
- Approval defaults to separate release approval; explicit conditional approval is recorded per plan but cannot execute or release anything yet.
- Only the first Spark-pair/eight-stream adapter's non-secret setting names and bounded literal formats are accepted. This is not generic model-update support or actual controller compatibility verification.
- Read-only status never creates state, reports live guard phase separately, and cannot label retained active/interrupted isolation idle.
- Planning requires existing enrolled v2 inventory; a preparation command cannot create an empty guard root and accidentally trigger the guard's unreadable-state refusal.
- Preparation commands cannot claim execution or acceptance: status reports `execution_supported=false`; `run` is not exposed.

## Verification and review

Final canonical run: **127 passed, zero failed across eight files**, including all three new workflow files and the five existing backend isolation/facade/request-hook/dropped-response/maintenance suites. `git diff --check` passed.

- Release primitive RED: three expected failures for missing API/CLI operation binding; GREEN: four new tests and 93 existing guard tests.
- Journal and CLI regressions cover profile A/B/A, real subprocess CLI calls, process-lock contention, wrong-digest approval, immutable approval, conditional checklist binding, corruption/truncation, traversal, duplicate IDs, permissive storage, unenrolled planning, forbidden secret/command settings, and active/interrupted status.
- All test state uses explicit temporary guard AND accounting directories, with real native migration/state/lock code. The remote inference probe is doubled only where entry tests require it. No network acceptance is claimed.
- Independent Codex review (gpt-5.6-sol/high, session 01a1226d-761b-70f0-a3e4-30422cbaa1c1) found settings-map and status-label issues. Reproduced both with failing tests, then fixed them. Also added the suggested private-storage refusal tests.
- Reviewer portability concern: current feature targets the existing Linux fleet. Existing `backend_isolation` already imports POSIX locking and uses Linux executor identity; this branch does not claim Windows support. No broad platform refactor was added.
- Initial review exceeded its timebox; the same reviewer was resumed with narrower scope and returned findings. Review logs remain local under `.review/maintenance-foundation/`, not part of the runtime.

## Remaining implementation — no extra design approval needed

1. Concrete controller and credential-expiry preflight adapter, including reusable facade QA instead of relying on coordinator-only scripts.
2. `run`/explicit `resume`, step intent/result journal, identity/drift checks and fault-injection coverage.
3. Fresh QA-bound conditional/separate release and post-release normal-route verification.
4. Non-dispatching task reconciliation, operational documentation and final source review.
5. Separate activation handoff if running processes need new code; no live activation is authorized by this development checkpoint.

The existing eight-stream deployment remains an existing production deployment, not a target to repeat while developing this workflow.
