-- ─── Inventory Schema Extensions (Phase 4A) ──────────────────────────────────────

-- 1. Extend inventory table
ALTER TABLE inventory
    DROP CONSTRAINT IF EXISTS chk_inventory_qty,
    ADD CONSTRAINT chk_inventory_qty CHECK (quantity_available >= 0);

-- 2. Extend inventory_reservations table
ALTER TABLE inventory_reservations
    ADD COLUMN IF NOT EXISTS status VARCHAR(50) NOT NULL DEFAULT 'RESERVED',
    ADD COLUMN IF NOT EXISTS cancellation_id UUID,
    ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ;

ALTER TABLE inventory_reservations
    DROP CONSTRAINT IF EXISTS chk_reservations_status,
    ADD CONSTRAINT chk_reservations_status CHECK (status IN ('RESERVED', 'RELEASED'));

ALTER TABLE inventory_reservations
    DROP CONSTRAINT IF EXISTS chk_reservations_qty,
    ADD CONSTRAINT chk_reservations_qty CHECK (quantity > 0);

-- 3. Create inventory_decisions table (immutable decision log)
CREATE TABLE IF NOT EXISTS inventory_decisions (
    order_id UUID PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    product_id UUID NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    outcome VARCHAR(50) NOT NULL CHECK (outcome IN ('RESERVED', 'REJECTED')),
    reason VARCHAR(255),
    available_at_decision INT NOT NULL,
    reservation_id UUID,
    version BIGINT NOT NULL DEFAULT 1,
    decided_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- 4. Create inventory outbox_events table
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
    CONSTRAINT uq_inv_outbox_agg_ver_type UNIQUE (aggregate_id, aggregate_version, event_type)
);

CREATE INDEX IF NOT EXISTS idx_inv_outbox_pending_attempt ON outbox_events(next_attempt_at, created_at)
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
    CONSTRAINT uq_inv_dead_letters_dlt_offset UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);
CREATE INDEX IF NOT EXISTS idx_inv_dead_letters_status_seen ON dead_letters(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_inv_dead_letters_order_id ON dead_letters(order_id);

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
    CONSTRAINT uq_inv_replay_requests_user_key UNIQUE (requested_by, idempotency_key)
);
