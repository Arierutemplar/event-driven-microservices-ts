# Runbook: NATS backlog

## Triage

1. Check `analytics_nats_pending_messages` and ack-pending messages.
2. Verify PostgreSQL health and circuit-breaker state.
3. Check consumer errors and pod replica status.

## Mitigation

- Restore the database and allow the consumer to drain.
- Scale consumer replicas only if the durable consumer configuration supports it.
- Do not delete pending stream messages.
