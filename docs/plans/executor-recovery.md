# Executor Recovery Implementation Plan

**Goal:** Restore the ability to talk to an independent maintenance executor without reopening admission or certifying serving mutations.

**Architecture:** Add a recovery operation to the serving-admission CLI. Atomically replace expired executor control with an explicit owner acknowledgement and audit row in the same private SQLite database. Preserve ALL work reservations unchanged: restoring a conversation does not prove old work completed. Existing certification rejects the outstanding old reservations, and any previous receipt no longer matches control.

**Safety correction to the earlier spec:** Do not transfer live reservations or delete dead ones during executor recovery. That could manufacture drain. Recovery is separate from reconciliation; an operator must investigate abandoned work before certification. Refuse an old live executor, even with expired lease, because inherited reservations still permit nested work. Auditing in the same SQLite transaction avoids filesystem/database atomicity gaps.

**Tech Stack:** Python, SQLite, pytest, Linux /proc.

## Tasks
- [ ] Add temporary-database recovery tests and run them to observe failure.
- [ ] Implement strict process validation, expired-owner checked recovery, same-transaction audit, preserved reservations, and CLI arguments.
- [ ] Run focused recovery tests and existing maintenance/serving tests.
- [ ] Review diff for failure atomicity and authority boundaries; record recovery instructions.

## Verification
Run from this isolated worktree using the installed Hermes venv Python:
`../hermes-agent/venv/bin/python -m pytest tests/agent/test_executor_recovery.py tests/agent/test_maintenance_admission.py tests/agent/test_serving_admission.py -q`

No live services or accounting state are modified during implementation. Live takeover requires an identified replacement interactive CLI and a stopped old executor. Keep source changes isolated until tests pass. Do not restart the gateway or touch Spark serving.
