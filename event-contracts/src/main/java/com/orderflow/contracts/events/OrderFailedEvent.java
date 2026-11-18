package com.orderflow.contracts.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
public class OrderFailedEvent {

    public static final String EVENT_TYPE = "order.failed";

    @NotNull(message = "orderId is required")
    private UUID orderId;

    @NotNull(message = "customerId is required")
    private UUID customerId;

    @NotBlank(message = "reason is required")
    private String reason;
}
