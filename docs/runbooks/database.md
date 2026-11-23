# Runbook: PostgreSQL

## Triage

1. Check `/ready`, pool saturation and PostgreSQL connectivity.
2. Inspect slow queries and lock waits.
3. Inspect the circuit-breaker transition metrics.

## Mitigation

- Restore database connectivity.
- Pause deployments and migrations.
- Verify replicas, disk capacity and connection limits.
