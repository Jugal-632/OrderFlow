ALTER TABLE notifications ADD COLUMN payload_hash varchar(64);
ALTER TABLE notifications ALTER COLUMN recorded_at TYPE timestamptz USING recorded_at AT TIME ZONE 'UTC';
UPDATE notifications SET recorded_at=CURRENT_TIMESTAMP WHERE recorded_at IS NULL;
ALTER TABLE notifications ALTER COLUMN recorded_at SET NOT NULL;
