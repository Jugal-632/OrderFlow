-- Preserve existing Flyway checksums; align Order's persisted version with emitted placement version.
UPDATE orders o SET version=GREATEST(o.version,1,COALESCE((SELECT MAX(e.aggregate_version) FROM outbox_events e WHERE e.aggregate_id=o.id),1));
ALTER TABLE order_history ADD COLUMN decision_hash varchar(64);
CREATE UNIQUE INDEX uq_order_inventory_outcome ON order_history(order_id) WHERE new_status IN ('CONFIRMED','FAILED');
UPDATE outbox_events SET status='PENDING' WHERE status='FAILED';
-- Older JPA generation could replace the assigned row ID. Preserve the immutable envelope ID,
-- including any event already ACKed by Kafka whose publication flag was not committed.
UPDATE outbox_events SET id=(payload->>'eventId')::uuid
WHERE status<>'PUBLISHED' AND payload ? 'eventId' AND id<>(payload->>'eventId')::uuid;
