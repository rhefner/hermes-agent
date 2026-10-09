# Resumable backend maintenance

This is an **external Linux CLI workflow**, not a model tool or another guard. The existing fleet-wide backend guard remains the sole authority for exclusions, admission and response drain. Installing this code does not start maintenance.

## Supported scope

The first adapter, `spark-pair-eight-stream`, supports only the reviewed Nix `glm53_eight_stream.py` operation on `spark-pair`. It preserves the pinned model/drafter, image, recipe, 1,048,576-token context, TP2, thinking and vision. It does not support arbitrary model upgrades, command lists, controller paths, rollback, credential renewal, context reduction or gateway lifecycle operations.

The approved delta must exactly match the reviewed controller: `PARALLEL=8`, `TF_GLM_MULTI_WINDOW=64`, `TF_ROCE_MAX_KB=1024`, and the controller's observed retained `MEMORY_RESERVE_GIB` (at least 19.6). A different requested reserve is rejected, not silently substituted. A backend already at the approved goal is **verified, not redeployed**.

The adapter loads fixed source files under `~/nix/users/hef/local-ai-services/spark-lanes/`, requiring the exact approved Git revision and clean controller/QA/admission/renewal sources. Fixed read-only node attestations compare the controller, admission and restricted-SSH helper bytes against immutable approved Git blobs, before isolation and every mutation/direct QA request. This is reviewed code, not a plugin interface. Transport collaborators in Python are test seams, not CLI configuration.

## Plan and approval

The command prefix is:

```text
python -m hermes_cli.backend_isolation workflow
```

`plan --file plan.json` records the following strict, non-secret JSON fields:

| Field | Required content |
|---|---|
| `adapter` | `spark-pair-eight-stream` |
| `backends` | `["spark-pair"]` |
| `controller_revision` | Exact 40-character Nix Git revision |
| `delta` | The four supported settings above, as strings |
| `preserve` | Complete reviewed `PINS`, plus `RECIPE_REVISION` and `IMAGE_DIGEST` |
| `qa` | `direct`, `facade`, `unaffected`, `capacity`, `health`, `settings` |
| `budget_seconds` | Positive execution/QA budget, at most 86400 |
| `margin_seconds` | Positive credential safety margin and maximum release-receipt age |
| `tasks` | Explicit existing fleet-default-board task IDs, or `[]` |

Read the reviewed controller constants when preparing the plan; do not guess model revisions or copy stale production receipts. Planning validates the preparation schema. **Preflight** proves that the full settings and actual source/controller contract are executable. Existing v2 inventory enrollment is required before any journal is created; never migrate again or create an empty guard directory.

Example sequence (use the workflow ID and digest returned by `plan`, not the separate guard operation ID):

```sh
python -m hermes_cli.backend_isolation workflow plan --file plan.json
python -m hermes_cli.backend_isolation workflow preflight WORKFLOW_ID --model INDEPENDENT_MODEL
python -m hermes_cli.backend_isolation workflow approve WORKFLOW_ID \
  --digest PLAN_DIGEST --reference OPERATOR_MESSAGE --approval 'Approve this exact plan'
python -m hermes_cli.backend_isolation workflow run WORKFLOW_ID --model INDEPENDENT_MODEL
```

Approval is immutable and binds the exact plan and QA checklist. Changed scope requires a new plan, not journal editing. The operator reference/text must contain no secrets. Development approval is not production or release approval.

**Separate release approval is the default.** A successful run stops at `awaiting_release`, with affected traffic still excluded:

```sh
python -m hermes_cli.backend_isolation workflow release WORKFLOW_ID \
  --model INDEPENDENT_MODEL --reference POST_QA_MESSAGE --approval 'Release this exact operation after QA'
```

Release explicitly resumes/rebinds the executor and reruns current QA; it does not trust a dead executor's cached receipt. To authorize conditional release for one operation, add `--release-policy conditional` to that operation's `approve` command. This is not standing authorization for later operations.

## Preflight and execution

Preflight does not exclude traffic or change serving state. It does perform read-only SSH/HTTP observations and explicitly requested independent cloud runtime/auxiliary inference probes. The selected model must pass the existing independent Codex runtime checks. This CLI does not rewrite chat/profile model defaults: keep the human-facing coordinator on an independent allowed route before entry. Nothing falls back to an affected backend.

Checks include:

- Exact local and node-side source bytes, actual container/config fingerprints and preserved settings. Container `State.StartedAt` is included: an in-place restart keeps the same Docker ID/config but must invalidate QA. Original/retained and candidate starts are checked before/after every mutation and final readiness; only approved create/start transitions advance expected identity.
- Existing reviewed backup procedure, source/cache/RoCE checks and disk headroom. The controller still creates and verifies the real private backup before replacing config; preflight is not a promise that future filesystem writes cannot fail.
- Installed restricted SSH authorization metadata: exactly three expected source/forced-command restrictions, with expiry covering budget plus margin. Coverage is rechecked after potentially slow independent probes. The old installer timestamp is not installed state. Missing/expired credentials require a separately authorized renewal; the workflow never renews them.
- Read-only facade `/health` availability of primary, underlying GLM and secondary QA routes before exclusion. The probe makes no facade inference request.
- Independent execution and current guard admission/drain. Cached preflight is never mutation authorization.

`run` performs fresh preflight, enters only the declared backend scope, binds its actual PID/start/boot identity, and records intent before each controller mutation. The existing controller repeats central/node admission and configuration checks. It retains old containers and protected config backups; this workflow never rolls them back automatically.

Run the CLI as an **externally supervised foreground executor**, not inside the Telegram gateway process. Give its supervisor enough time for the approved budget and outstanding bounded I/O to settle. Do not use a short chat-tool timeout as the executor lifetime. No daemon, service restart or gateway activation is launched by this CLI.

The independent heartbeat monitor keeps status current during blocking calls. Budget expiry or heartbeat failure interrupts only the matching guard operation/executor/probe generation and retains exclusions. Already-issued I/O can finish within its transport timeout; this is not a claim to cancel an in-flight Docker action. SSH RPCs are bounded. Facade requests have a 180-second monotonic deadline, finite socket timeouts and a body cap; a blocked read can extend the deadline by at most one 180-second socket timeout. There are no hidden retries.

## Status and recovery

```sh
python -m hermes_cli.backend_isolation workflow status WORKFLOW_ID
python -m hermes_cli.backend_isolation workflow inspect WORKFLOW_ID
python -m hermes_cli.backend_isolation workflow inspect WORKFLOW_ID --live
python -m hermes_cli.backend_isolation workflow resume WORKFLOW_ID --model INDEPENDENT_MODEL
```

`status` and plain `inspect` read the journal, real guard, runner lock and heartbeat without writes or network access. `inspect --live` additionally performs bounded read-only controller/credential observations; it does not run QA inference or mutate serving. An unhealthy partial deployment can make this strict controller inspection fail; retain isolation and inspect the existing controller evidence/retained containers rather than bypassing it.

Stages are `planned`, `preflight_ready`, `approved`, `isolated`, `applying`, `verifying`, `awaiting_release`, `released`, `reconciling`, `complete`. Activity overlays actual guard state: a historical completed journal never conceals another active maintenance window. Missing/stale heartbeat, executor loss, unreadable records and uncertainty are not reported as healthy running work.

- **Lost apply response:** resume may skip mutation only when a fresh observation proves the entire exact approved goal. Fresh QA is still required. A partial or unknown outcome blocks; stop/create/start is never blindly replayed.
- **Entry response lost before its operation ID was journaled:** do not adopt an arbitrary active guard. Diagnose through the existing operator recovery controls.
- **Failed QA / drift / expired credentials:** retain isolation. Diagnose the recorded step; no automatic rollback, release or renewal.
- **Release response lost:** only the exact guard operation and recorded release approval can establish that release already occurred. Resume verifies normal routes; it does not deploy again.
- **Post-release route failure:** report `released` / degraded, without rollback or automatic re-isolation. An explicit resume retries verification only.
- **Task bookkeeping failure:** report `reconciling` / `bookkeeping_pending`; retry the projection, not serving work.

The journal is private, atomic fleet-scoped metadata under `maintenance_inference.directory/operations`; it is not a permission token. Evidence files contain validated identifiers, hashes, categorical checks and backup references, not credentials, raw environment, request bodies or completions. Do not delete or hand-edit guard/journal state to restore routing.

## QA and release guarantees

QA checks direct engine responses and facade text/SSE/tools/thinking/known-image vision, both ranks' health/settings, unaffected secondary, and **eight slots actually decoding**. Queued or merely accepted requests do not count. Short QA does not prove eight simultaneous million-token prompts.

Affected QA uses a fixed, explicitly admitted target-only transport. It is not normal agent inference or a global bypass. Secondary QA and post-release primary/secondary verification use the normal guarded HTTP transport, retaining reader locks through body consumption/close.

Release requires fresh passing evidence bound to workflow plan, checklist, guard operation and actual source/config/container identity. Operation, executor and QA/budget expiry are checked under the existing guard state lock. Remote observations are rechecked but are **not claimed to be transactional with remote Docker/HTTP state**. The low-level explicitly authorized recovery release remains available to trusted operators.

## Task projection

```sh
python -m hermes_cli.backend_isolation workflow reconcile WORKFLOW_ID
```

Only approved task IDs in the existing fleet default board (`guard.directory().parent/kanban.db`) are projected. Ambient profile/board selection does not redirect this operation. Custom boards are not supported by this first adapter.

Projection is not a scheduler pause: keep maintenance cards non-dispatchable while the external executor runs, rather than leaving an old deployment chain eligible for unrelated scheduler ticks. The complete open descendant closure must be included, or projection refuses atomically. Otherwise a later scheduler tick could ready an old child deployment. Live workers, unresolved claims whose worker PID is not yet published, missing/reopened cards and ambiguous bookkeeping block rather than being force-completed. Let the existing worker lifecycle settle the claim before retrying reconciliation; never clear a real worker's identity merely to pass this check. Existing done/archived cards are not reopened.

Projection emits the existing native `completed` event/closed-run shape and a deduplicated audit event in one transaction. It never recomputes readiness, invokes lifecycle hooks, deletes workspaces or adds another dispatcher/notifier. If the DB committed before the journal receipt, retry recognizes the event and does not duplicate completion. `complete` is recorded only after the projection receipt is durable.

## Activation boundary

This source was developed and tested in an isolated worktree. Passing fixtures and source-import checks do not constitute live maintenance acceptance. See [the activation handoff](superpowers/plans/2026-10-09-maintenance-workflow-activation.md) before installing or using it against the fleet.
