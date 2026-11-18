package com.orderflow.contracts.envelope;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Versioned JSON Event Envelope (Schema Version 1).
 * Standard envelope for all asynchronous business events published via Kafka.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventEnvelope<T> {

    @NotNull(message = "eventId is required")
    private UUID eventId;

    @NotBlank(message = "eventType is required")
    private String eventType;

    @Min(value = 1, message = "schemaVersion must be >= 1")
    @Builder.Default
    private int schemaVersion = 1;

    @NotNull(message = "occurredAt is required")
    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC")
    private Instant occurredAt;

    @NotBlank(message = "producer is required")
    private String producer;

    @NotNull(message = "aggregateId is required")
    private UUID aggregateId;

    @Min(value = 1, message = "aggregateVersion must be >= 1")
    @Builder.Default
    private long aggregateVersion = 1;

    @NotNull(message = "correlationId is required")
    private UUID correlationId;

    @NotNull(message = "causationId is required")
    private UUID causationId;

    @Valid
    @NotNull(message = "data payload is required")
    private T data;
}
