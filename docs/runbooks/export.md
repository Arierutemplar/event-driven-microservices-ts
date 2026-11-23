# Runbook: Parquet export

## Triage

1. Check object-store connectivity and credentials.
2. Inspect export errors and the export watermark.
3. Verify that one exporter replica holds the advisory lock.

## Mitigation

- Restore object-store or PostgreSQL connectivity.
- Replay by leaving `pending_to` intact; the exporter will reuse deterministic keys.
- Investigate object-level failures before advancing the watermark manually.
