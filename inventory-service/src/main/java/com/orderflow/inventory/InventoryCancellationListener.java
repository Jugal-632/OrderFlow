package com.orderflow.inventory;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.OrderCancelRequestedEvent;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCancellationListener {
    private final InventoryReleaseService releases;
    public InventoryCancellationListener(InventoryReleaseService releases) { this.releases=releases; }
    @KafkaListener(id="inventory-releases",topics="order.cancel.requested",groupId="inventory-service")
    public void receive(String payload) {
        if(payload==null || payload.isBlank()) throw new EventValidationException("Missing cancellation payload");
        var event=EventValidator.deserializeAndValidate(payload,OrderCancelRequestedEvent.class);
        try(var ignored=com.orderflow.telemetry.TraceContext.correlation(event.getCorrelationId().toString())) {
            releases.release(event,InventoryReservationService.hash(payload));
        }
    }
}
