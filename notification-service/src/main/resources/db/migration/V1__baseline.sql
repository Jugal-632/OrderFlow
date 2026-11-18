CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    message TEXT,
    sent_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(order_id, event_type)
);
