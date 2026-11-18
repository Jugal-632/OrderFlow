package com.orderflow.order.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {

    private UUID orderId;
    private UUID customerId;
    private UUID productId;
    private int quantity;
    private Instant createdAt = Instant.now();

    // Constructor used in OrderService
    public OrderEvent(UUID orderId, UUID customerId,
                      UUID productId, int quantity) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.createdAt = Instant.now();
    }
}
