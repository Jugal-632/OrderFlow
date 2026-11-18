-- Decisions retain the immutable initial outcome; reservation version tracks reserve/release.
ALTER TABLE inventory_reservations ADD COLUMN version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0);
UPDATE inventory_reservations SET version=2 WHERE status='RELEASED';
