-- Create separate schemas per service (database-per-service pattern)
CREATE SCHEMA IF NOT EXISTS auth_schema;
CREATE SCHEMA IF NOT EXISTS order_schema;
CREATE SCHEMA IF NOT EXISTS inventory_schema;
CREATE SCHEMA IF NOT EXISTS notification_schema;

-- ─── Auth Service ──────────────────────────────────────────────
CREATE TABLE auth_schema.users (
                                   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                   email VARCHAR(255) UNIQUE NOT NULL,
                                   password_hash VARCHAR(255) NOT NULL,
                                   created_at TIMESTAMP DEFAULT NOW()
);

-- Accounts are bootstrapped through environment-provided Auth credentials.

-- ─── Order Service ─────────────────────────────────────────────
CREATE TABLE order_schema.orders (
                                     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                     customer_id UUID NOT NULL,
                                     product_id UUID NOT NULL,
                                     quantity INT NOT NULL,
                                     status VARCHAR(50) DEFAULT 'PENDING',
                                     failure_reason VARCHAR(255),
                                     created_at TIMESTAMP DEFAULT NOW(),
                                     updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE order_schema.outbox_events (
                                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                            aggregate_id UUID NOT NULL,
                                            aggregate_type VARCHAR(100) NOT NULL,
                                            event_type VARCHAR(100) NOT NULL,
                                            payload JSONB NOT NULL,
                                            status VARCHAR(50) DEFAULT 'PENDING',
                                            created_at TIMESTAMP DEFAULT NOW(),
                                            published_at TIMESTAMP,
                                            retry_count INT DEFAULT 0
);

CREATE INDEX idx_outbox_status ON order_schema.outbox_events(status)
    WHERE status = 'PENDING';

-- ─── Inventory Service ─────────────────────────────────────────
CREATE TABLE inventory_schema.inventory (
                                            product_id UUID PRIMARY KEY,
                                            product_name VARCHAR(255) NOT NULL,
                                            quantity_available INT NOT NULL DEFAULT 0,
                                            updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE inventory_schema.inventory_reservations (
                                                         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                                         order_id UUID NOT NULL UNIQUE,
                                                         product_id UUID NOT NULL,
                                                         quantity INT NOT NULL,
                                                         reserved_at TIMESTAMP DEFAULT NOW()
);

-- Seed some products for testing
INSERT INTO inventory_schema.inventory (product_id, product_name, quantity_available)
VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Laptop Pro X1', 50),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Wireless Mouse', 200),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', 'USB Hub 7-Port', 100);

-- ─── Notification Service ──────────────────────────────────────
CREATE TABLE notification_schema.notifications (
                                                   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                                   order_id UUID NOT NULL,
                                                   event_type VARCHAR(100) NOT NULL,
                                                   message TEXT,
                                                   sent_at TIMESTAMP DEFAULT NOW(),
                                                   UNIQUE(order_id, event_type)
);