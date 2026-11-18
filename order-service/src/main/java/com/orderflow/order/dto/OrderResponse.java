package com.orderflow.order.dto;

import com.orderflow.order.model.Order;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
public class OrderResponse {

    private UUID id;
    private UUID customerId;
    private UUID productId;
    private int quantity;
    private String status;
    private String failureReason;
    private Instant createdAt;
    private long version;
    @Builder.Default
    private java.util.List<OrderTimelineEntry> timeline = java.util.List.of();

    public static OrderResponse from(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .status(order.getStatus().name())
                .failureReason(order.getFailureReason())
                .createdAt(order.getCreatedAt())
                .version(order.getVersion())
                .build();
    }
}
