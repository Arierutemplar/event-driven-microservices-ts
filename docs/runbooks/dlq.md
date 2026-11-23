# Runbook: Dead Letter Queue

## Triage

1. List the DLQ and group messages by reason.
2. Inspect a representative message.
3. Determine whether the failure is permanent or a transient outage.

## Mitigation

- Fix the data or schema issue.
- Redrive only the corrected reason, with `dry_run=true` first.
- Never redrive `decode_error` messages until the decoder or producer contract is fixed.
