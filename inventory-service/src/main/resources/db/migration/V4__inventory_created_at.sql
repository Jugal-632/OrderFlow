ALTER TABLE inventory ADD COLUMN created_at TIMESTAMPTZ;
UPDATE inventory SET created_at=updated_at;
ALTER TABLE inventory ALTER COLUMN created_at SET NOT NULL;
ALTER TABLE inventory ALTER COLUMN created_at SET DEFAULT now();
