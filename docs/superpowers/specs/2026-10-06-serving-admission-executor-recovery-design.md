# Serving-admission executor recovery design

## Problem

Hermes maintenance admission binds a closed gate to one exact Linux process identity: PID, process start ticks, and boot ID. Resuming the same persisted Hermes session creates a new process identity. If the original interactive executor becomes inaccessible or exits after admission closes, the resumed CLI is rejected before it can make a model request. The current commands can close, certify, check, renew, and eventually reopen admission, but cannot transfer a still-closed maintenance transaction to a replacement executor.

Reopening normal admission is not an acceptable recovery because maintenance QA may be incomplete. Editing `work.sqlite` manually is also unacceptable because it bypasses the accounting boundary and can erase live work.

## Goal

Provide an explicit, fail-closed executor recovery operation that allows maintenance QA to continue under a new independent Codex CLI while keeping normal Hermes admission closed.

## Command

Extend `hermes_cli.serving_admission` with:

```text
serving_admission recover \
  --owner EXISTING_32_HEX_OWNER \
  --executor-pid NEW_EXECUTOR_PID \
  --seconds BOUNDED_LEASE_SECONDS \
  --acknowledge-recovery 'case-specific operator acknowledgement'
```

This is an operator command, not an automatic startup repair. It performs no service restart, routing edit, inference mutation, rollback, or reopening.

## Preconditions

Recovery refuses unless all conditions hold:

1. The accounting store is enrolled, private, and structurally valid.
2. Admission is already closed.
3. `--owner` exactly matches the current owner.
4. The old lease has expired. A live authority must use normal renewal rather than takeover.
5. The acknowledgement is non-empty.
6. The requested lease is positive and no longer than four hours.
7. The replacement PID resolves to a live exact process identity.
8. The replacement is an independent Hermes CLI with explicit provider `openai-codex` and model `gpt-6-astra`; it is not a gateway process, gateway cgroup member, or headless query invocation.
9. Every reservation identity can be classified unambiguously from `/proc`.

## Reservation reconciliation

Each reservation is compared using all three recorded identity fields.

- If `/proc/PID` exists and PID/start ticks/boot ID all match, the reservation is live.
- If the PID is absent, the boot ID differs, or the PID exists with different start ticks, the reservation is abandoned by an exited process or PID reuse.
- If process state cannot be inspected reliably, recovery refuses without mutation.

Live reservations owned by an identity other than the old executor block recovery. The operation must never erase active foreign work.

Reservations belonging to the old exact executor are transferred to the replacement identity. Proven-abandoned reservations from other identities are removed only after their complete records are captured in an owner-only audit record. This preserves the resumed conversation's accounting while preventing dead reservations from permanently stranding the gate.

## Atomic state transition

Classification and mutation occur under one SQLite `BEGIN IMMEDIATE` transaction:

1. Re-read and validate control.
2. Classify every reservation.
3. Refuse on any live foreign reservation or ambiguous inspection.
4. Update old-executor reservation identities to the replacement identity.
5. Delete only proven-abandoned foreign reservations.
6. Replace `control.executor`, set a fresh bounded expiry, preserve `closed: true`, preserve owner and original `closed_at`, and append recovery metadata.
7. Commit.

The audit record is staged privately before commit and finalized with the resulting control state. If durable audit publication cannot be guaranteed, the database mutation must not proceed. The audit contains reservation IDs, kinds, recorded identities, classification reasons, old/new executor identities, owner, acknowledgement, and timestamp; it contains no prompts, credentials, or session content.

## Certification behavior

Recovery grants only the ability for the replacement CLI to run while admission remains closed. It does not grant serving-mutation authority.

Any existing `serving.json` receipt is invalidated as part of successful recovery. The replacement executor must run fresh certification/check flows before a serving mutation. Certification continues to enforce isolation, runtime inventory, source freshness, profile inventory, exact executor identity, and authenticated completion.

## Failure behavior

All validation failures are fail-closed and leave accounting unchanged. Error messages distinguish operator-correctable causes without exposing secrets:

- owner mismatch;
- lease not expired;
- invalid replacement executor;
- live foreign work;
- ambiguous process inventory;
- invalid lease or acknowledgement;
- audit publication failure.

Recovery never treats lease expiry, dead PIDs, a resumed session ID, or a clean process exit as QA completion.

## Tests

Tests use temporary accounting databases and fake process roots/injected identity inspection where necessary. They must prove:

- recovery rejects an open gate;
- recovery rejects owner mismatch and missing acknowledgement;
- recovery rejects an unexpired lease;
- lease bounds are enforced;
- replacement executor validation rejects gateway/headless/wrong provider/model processes;
- exact-live foreign work blocks recovery;
- missing, old-boot, and PID-reused identities are reconciled as abandoned;
- unreadable or malformed process identity refuses atomically;
- old-executor reservations transfer to the replacement;
- admission remains closed with owner and original closure metadata preserved;
- an audit record is durable and owner-only;
- prior certification is invalidated;
- a failed recovery changes neither control nor reservations;
- the CLI exposes the command and required arguments.

## Deployment and live recovery

Implementation is made and tested in the Hermes runtime repository, committed to the `fork` remote, and loaded by a separately controlled runtime update. The live database is backed up before invoking recovery. A fresh interactive replacement CLI is started with the exact preserved session and explicit Codex provider/model; its PID is supplied to the recovery command. After recovery, the replacement session must demonstrate a successful model request. Maintenance QA then continues; normal admission remains closed until separate post-QA approval.
