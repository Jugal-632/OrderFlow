package com.orderflow.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
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
public class InventoryRejectedEvent {

    public static final String EVENT_TYPE = "inventory.rejected";

    @NotNull(message = "orderId is required")
    private UUID orderId;

    @NotNull(message = "productId is required")
    private UUID productId;

    @Min(value = 1, message = "quantity must be greater than 0")
    private int quantity;

    @NotBlank(message = "reason is required")
    private String reason;

    @Min(value = 0, message = "availableQuantity must be >= 0")
    private int availableQuantity;
}
