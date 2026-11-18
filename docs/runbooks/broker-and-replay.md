# Broker outage and controlled replay

During a broker outage, committed outbox rows remain durable and retry with their original event IDs. Inspect readiness, pending count/age and retry count. Restore the broker/network/ACL and partition ISR configuration; then observe PUBLISHED and the owning inbox/domain state. Do not truncate outbox/inbox tables, reset consumer offsets, or republish hand-edited business events.

For a BLOCKED stored publication, take an isolated backup before any repair, identify the producer/schema/metadata defect, and obtain an incident-reviewed correction. Retain event ID, aggregate ID/version and original business decision. Never use an edited browser payload as repair. Once the cause is corrected, the owning database operator can make the same record due again in a short transaction:

```sql
-- Use the owning schema and the exact reviewed event ID; do not run across all events.
UPDATE order_schema.outbox_events
SET status='PENDING',next_attempt_at=now()
WHERE id=:event_id AND status IN ('PENDING','BLOCKED');
```

For consumer failure, inspect `GET /api/ops/{service}/dead-letters` and its detail. Use ADMIN `POST /api/ops/{service}/dead-letters/{id}/replay` with an explicit reason and a newly generated Idempotency-Key. Retain that key through timeout/network retries. The same key/ID/reason returns the same request; changed contents return 409. Invalid payload/schema returns 409 until compatibility is fixed. Only OPEN letters are eligible; never change a queued or resolved letter manually.

Replay writes request + operational outbox + REPLAY_QUEUED atomically. It targets only `<service-name>.replay`, preserving the original domain event ID. Success commits domain changes and SUCCEEDED/RESOLVED together. Inbox deduplication protects already-completed work. A failed replay's DLT collector records the linked failure, marks FAILED and reopens the original letter. Inspect that new failure before submitting a new action/key.

If DLT publication fails, the source offset must remain uncommitted; restore DLT topic/ISR/ACL availability before continuing. If DLT indexing fails, repair the owning database: the indexer retries without publishing a recursive DLT. Do not acknowledge or discard a record to silence an alert. Audit rejected access and invalid requests using Gateway JSON audit logs and the owning `ops_audit`/`replay_requests` tables.
