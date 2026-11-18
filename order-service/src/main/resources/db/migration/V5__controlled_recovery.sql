ALTER TABLE dead_letters ADD COLUMN original_dead_letter_id UUID REFERENCES dead_letters(id);
ALTER TABLE dead_letters ADD COLUMN replay_request_id UUID REFERENCES replay_requests(id);
CREATE UNIQUE INDEX one_active_replay ON replay_requests(dead_letter_id) WHERE result='QUEUED';
CREATE TABLE ops_audit(id UUID PRIMARY KEY DEFAULT gen_random_uuid(),actor VARCHAR(100) NOT NULL,dead_letter_id UUID,http_status INT NOT NULL,outcome VARCHAR(160) NOT NULL,recorded_at TIMESTAMPTZ NOT NULL DEFAULT now());
