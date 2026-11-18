-- ─── Notification Schema Extensions (Phase 4A) ───────────────────────────────────

-- 1. Extend notifications table
ALTER TABLE notifications
    RENAME COLUMN sent_at TO recorded_at;

ALTER TABLE notifications
    ADD COLUMN IF NOT EXISTS customer_id UUID,
    ADD COLUMN IF NOT EXISTS source_event_id UUID,
    ADD COLUMN IF NOT EXISTS order_version BIGINT,
    ADD COLUMN IF NOT EXISTS delivery_status VARCHAR(50) NOT NULL DEFAULT 'RECORDED';

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS uq_notifications_source_event,
    ADD CONSTRAINT uq_notifications_source_event UNIQUE (source_event_id);

CREATE INDEX IF NOT EXISTS idx_notifications_customer ON notifications(customer_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_order_version ON notifications(order_id, order_version);

-- 2. Create consumer_inbox table
CREATE TABLE IF NOT EXISTS consumer_inbox (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

-- 3. Operational outbox_events table (for operational replay commands)
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    event_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',
    aggregate_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    next_attempt_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    published_at TIMESTAMPTZ,
    retry_count INT DEFAULT 0,
    last_error TEXT,
    CONSTRAINT uq_notif_outbox_agg_ver_type UNIQUE (aggregate_id, aggregate_version, event_type)
);

CREATE INDEX IF NOT EXISTS idx_notif_outbox_pending_attempt ON outbox_events(next_attempt_at, created_at)
    WHERE status = 'PENDING';

-- 4. Operational dead_letters table
CREATE TABLE IF NOT EXISTS dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_topic VARCHAR(100) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    dlt_topic VARCHAR(100) NOT NULL,
    dlt_partition INT NOT NULL,
    dlt_offset BIGINT NOT NULL,
    event_id UUID,
    event_type VARCHAR(100),
    order_id UUID,
    raw_payload TEXT NOT NULL,
    raw_headers JSONB,
    error_code VARCHAR(100),
    sanitized_error TEXT,
    first_seen_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    last_seen_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    status VARCHAR(50) DEFAULT 'OPEN' NOT NULL,
    replay_attempts INT DEFAULT 0 NOT NULL,
    CONSTRAINT uq_notif_dead_letters_dlt_offset UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);
CREATE INDEX IF NOT EXISTS idx_notif_dead_letters_status_seen ON dead_letters(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_notif_dead_letters_order_id ON dead_letters(order_id);

-- 5. Operational replay_requests table
CREATE TABLE IF NOT EXISTS replay_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dead_letter_id UUID NOT NULL REFERENCES dead_letters(id),
    idempotency_key VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    requested_by VARCHAR(100) NOT NULL,
    reason TEXT,
    requested_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    completed_at TIMESTAMPTZ,
    result VARCHAR(50) NOT NULL,
    CONSTRAINT uq_notif_replay_requests_user_key UNIQUE (requested_by, idempotency_key)
);
