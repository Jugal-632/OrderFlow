package com.orderflow.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderCancelledEvent {

    public static final String EVENT_TYPE = "order.cancelled";

    @NotNull(message = "orderId is required")
    private UUID orderId;

    @NotNull(message = "customerId is required")
    private UUID customerId;

    @NotNull(message = "cancellationId is required")
    private UUID cancellationId;
}
