package com.orderflow.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

/** HTTP input deliberately excludes identity; customer comes from authenticated context. */
public record CreateOrderRequest(@NotNull UUID productId, @Min(1) int quantity) {}
