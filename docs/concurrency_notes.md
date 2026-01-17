# Concurrency and reliability notes (draft)

## Agreed decisions
- Ingest deduplication should treat a rerun as the same job when raw artifacts are the same.
- Use Temporal Schedule for periodic reconcile/cleanup workflows.
- Keep ScanEdge and ScanPose tables (used in registration/export workflows).
- Prefer a minimal ingest status set (QUEUED → RUNNING → SUCCEEDED/FAILED) for now.
- Use intent → S3 write → approve flow for S3 ↔ DB consistency (SAGA-style).
- Plan to enable S3 bucket versioning (reminder to turn on later).

## Open questions
- Choose the exact stale-run timeout policy (activity heartbeat is already used).
- Decide whether cleanup should be DB-driven only or include S3 listing reconciliation.
