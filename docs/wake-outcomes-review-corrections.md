# Wake outcome review corrections (round two)

This is source-only. No deployment, service restart, subscription migration, Nix
activation, live transport or user-facing canary has been performed.

## R1: fail closed at each content-bearing streaming egress

The first candidate checked `_send_or_edit`, but boundary finalization called
`_send_frame` directly and its plain-send fallback omitted routing metadata.
Native finalization, draft frames, draft abandonment (which carries accumulated
content), direct edits/cursor cleanup, and first-send now check the existing
receipt observer immediately before transport. Existing fresh-final, chunk,
commentary, segment-tail and bounded flood-retry guards remain in place.

`WakeBoundaryClosed` propagates through the transport, boundary, fallback and
consumer layers; it is not converted into an ordinary transport failure or a
permission to use another rail. Boundary futures resolve false on closure.
Boundary fallback preserves thread/reply routing and `_interim_send=True`, so
pre-prompt output cannot seal a relay's turn-final stream. Every fallback checks
again after an awaited failed transport: origin closure during that await blocks
the next rail. No receipt means the observer remains a no-op.

As documented in wake-outcomes-current.md, a local pre-send guard cannot revoke
bytes already accepted by a remote transport. A reset racing after the last
local check is not an exactly-once/atomic-send guarantee. No turn is replayed.

## R2: notice ACK parity

Dictionary and object results both require `success is True` and must not carry
`delivered is False`. Missing delivered is accepted for the native SendResult
contract, whose success represents the positive transport ACK. An explicit
negative delivery result persists `notice=failed`, not sent; repeated polls and
new ReceiptStore instances neither replay agent work nor retry that failure.
Existing uncertain-send recovery remains bounded and unchanged.

## Verification

- Original independent probes reproduced on eb41f4102d2fd6ab0b9dfc56f501d8ca733148d7:
  two open-origin controls pass, three negative controls fail.
- Expanded first 80-case matrix on that same old candidate: 31 pass, 49 fail.
  Failures include real transport leakage and swallowed boundary exceptions.
- Final matrix adds the actual consumer drain-queue path: 84 cases, covering
  open/reset/thread-route/config-profile changes, direct and fallback egress,
  closure during native transport await, dict/object ACK parity and restart budget.
- Full regression: 530 passed, zero failed, 35 files, 130.3 seconds, retries off,
  through scripts/run_tests.sh with four workers. Includes the prior 24-file
  matrix and adjacent consumer/native/draft/fresh-final/thread-routing/abandonment,
  WeCom duplicate suppression and Slack native-stream tests.

All origin/receipt checks use real temporary SQLite and real consumer/Telegram
adapter code; only external transport is substituted. The existing real runner
queue and model-substituted integration tests are retained. No live sends.
