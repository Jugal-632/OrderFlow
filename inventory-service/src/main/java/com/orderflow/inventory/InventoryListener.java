package com.orderflow.inventory;

import com.orderflow.contracts.envelope.EventValidator;
import com.orderflow.contracts.events.OrderPlacedEvent;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryListener {
    private final InventoryReservationService reservations;
    public InventoryListener(InventoryReservationService reservations) { this.reservations = reservations; }

    @KafkaListener(id = "inventory-reservations", topics = "order.placed", groupId = "inventory-service")
    public void receive(String payload) {
        if (payload == null || payload.isBlank()) throw new com.orderflow.contracts.envelope.EventValidationException("Missing event payload");
        var envelope = EventValidator.deserializeAndValidate(payload, OrderPlacedEvent.class);
        try (var ignored = com.orderflow.telemetry.TraceContext.correlation( envelope.getCorrelationId().toString())) {
            reservations.reserve(envelope, InventoryReservationService.hash(payload));
        }
    }
}
