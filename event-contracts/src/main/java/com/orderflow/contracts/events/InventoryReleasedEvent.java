package com.orderflow.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Min;
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
public class InventoryReleasedEvent {

    public static final String EVENT_TYPE = "inventory.released";

    @NotNull(message = "orderId is required")
    private UUID orderId;

    @NotNull(message = "reservationId is required")
    private UUID reservationId;

    @NotNull(message = "cancellationId is required")
    private UUID cancellationId;

    @Min(value = 1, message = "quantity must be greater than 0")
    private int quantity;
}
