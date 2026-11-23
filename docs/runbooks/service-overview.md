# Runbook: Analytics service overview

## Symptoms

- Elevated 5xx rate.
- Readiness probe failing.
- Increased HTTP latency.

## Checks

1. Query `/ready` and inspect the `503` reason.
2. Check PostgreSQL, NATS, Redis and disk capacity.
3. Inspect structured application logs by `X-Request-ID`.
4. Review Grafana dashboards for latency, backlog and circuit-breaker state.

## Mitigation

- Scale API replicas if HTTP load exceeds capacity.
- Restore database or broker connectivity before forcing a restart.
- Roll back the latest deployment if a release correlates with the incident.
