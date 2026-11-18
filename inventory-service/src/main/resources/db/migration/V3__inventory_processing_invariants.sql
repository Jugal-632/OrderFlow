-- Preserve V1/V2 checksums; interpret baseline timestamp-without-zone values as UTC.
ALTER TABLE inventory ALTER COLUMN updated_at TYPE timestamptz USING updated_at AT TIME ZONE 'UTC';
UPDATE inventory SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
ALTER TABLE inventory ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE inventory_reservations ALTER COLUMN reserved_at TYPE timestamptz USING reserved_at AT TIME ZONE 'UTC';
ALTER TABLE inventory_reservations ADD CONSTRAINT fk_inventory_reservation_product FOREIGN KEY (product_id) REFERENCES inventory(product_id);
UPDATE outbox_events SET status='PENDING' WHERE status='FAILED';
ALTER TABLE outbox_events ALTER COLUMN status SET NOT NULL;
ALTER TABLE outbox_events ADD CONSTRAINT chk_inventory_outbox_status CHECK (status IN ('PENDING','PUBLISHED','BLOCKED'));
