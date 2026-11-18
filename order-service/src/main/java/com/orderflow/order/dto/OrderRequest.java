package com.orderflow.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.util.UUID;

@Data
public class OrderRequest {

    @NotNull(message = "customerId is required")
    private UUID customerId;

    @NotNull(message = "productId is required")
    private UUID productId;

    @Min(value = 1, message = "quantity must be at least 1")
    private int quantity;
}
