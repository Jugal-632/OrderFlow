-- ─── Order Schema Extensions (Phase 4A) ──────────────────────────────────────────

-- 1. Extend orders table
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reservation_id UUID,
    ADD COLUMN IF NOT EXISTS cancellation_id UUID,
    ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;

-- Constraints on orders
ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS chk_orders_quantity,
    ADD CONSTRAINT chk_orders_quantity CHECK (quantity > 0);

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS chk_orders_status,
    ADD CONSTRAINT chk_orders_status CHECK (status IN ('PENDING', 'CONFIRMED', 'FAILED', 'CANCEL_PENDING', 'CANCELLED'));

-- Indexes on orders
CREATE INDEX IF NOT EXISTS idx_orders_customer_created ON orders(customer_id, created_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at);

-- 2. Create request_idempotency table
CREATE TABLE IF NOT EXISTS request_idempotency (
    customer_id UUID NOT NULL,
    operation VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    resource_id UUID,
    response_status INT NOT NULL,
    response_body JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    PRIMARY KEY (customer_id, operation, idempotency_key)
);

-- 3. Create order_history table
CREATE TABLE IF NOT EXISTS order_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(id),
    order_version BIGINT NOT NULL,
    previous_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    source_event_id UUID,
    correlation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    reason VARCHAR(255),
    CONSTRAINT uq_order_history_version UNIQUE (order_id, order_version)
);
CREATE INDEX IF NOT EXISTS idx_order_history_version ON order_history(order_id, order_version);

-- 4. Extend outbox_events table
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS topic VARCHAR(100),
    ADD COLUMN IF NOT EXISTS event_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS aggregate_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Backfill topic and event_key for legacy rows
UPDATE outbox_events
SET topic = 'order.placed',
    event_key = aggregate_id::text
WHERE topic IS NULL;

-- Convert unpublished legacy outbox rows to Envelope Version 1
-- Envelope: eventId, eventType, schemaVersion=1, occurredAt, producer, aggregateId, aggregateVersion, correlationId, causationId, data
UPDATE outbox_events
SET payload = jsonb_build_object(
    'eventId', id,
    'eventType', 'order.placed',
    'schemaVersion', 1,
    'occurredAt', to_char(created_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'producer', 'order-service',
    'aggregateId', aggregate_id,
    'aggregateVersion', aggregate_version,
    'correlationId', aggregate_id,
    'causationId', id,
    'data', payload
)
WHERE NOT (payload ? 'schemaVersion');

ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS uq_outbox_aggregate_version_type,
    ADD CONSTRAINT uq_outbox_aggregate_version_type UNIQUE (aggregate_id, aggregate_version, event_type);

CREATE INDEX IF NOT EXISTS idx_outbox_pending_attempt ON outbox_events(next_attempt_at, created_at)
    WHERE status = 'PENDING';

-- 5. Create consumer_inbox table
CREATE TABLE IF NOT EXISTS consumer_inbox (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

-- 6. Operational dead_letters table
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
    CONSTRAINT uq_dead_letters_dlt_offset UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_status_seen ON dead_letters(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_dead_letters_order_id ON dead_letters(order_id);

-- 7. Operational replay_requests table
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
    CONSTRAINT uq_replay_requests_user_key UNIQUE (requested_by, idempotency_key)
);
