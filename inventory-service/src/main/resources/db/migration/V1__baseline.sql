CREATE TABLE IF NOT EXISTS inventory (
    product_id UUID PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    quantity_available INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL UNIQUE,
    product_id UUID NOT NULL,
    quantity INT NOT NULL,
    reserved_at TIMESTAMP DEFAULT NOW()
);
