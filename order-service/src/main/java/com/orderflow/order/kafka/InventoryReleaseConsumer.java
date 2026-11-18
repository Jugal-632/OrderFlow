package com.orderflow.order.kafka;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.InventoryReleasedEvent;
import com.orderflow.order.service.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryReleaseConsumer {
    private final OrderCancellationService cancellations;
    public InventoryReleaseConsumer(OrderCancellationService cancellations) { this.cancellations=cancellations; }
    @KafkaListener(id="order-releases",topics="inventory.released",groupId="order-service",containerFactory="orderOutcomeFactory")
    public void receive(ConsumerRecord<String,String> record) {
        if(record.value()==null || record.value().isBlank()) throw new EventValidationException("Missing release payload");
        var event=EventValidator.deserializeAndValidate(record.value(),InventoryReleasedEvent.class);
        if(!record.topic().equals(event.getEventType())) throw new EventValidationException("Topic/event type mismatch");
        try(var ignored=com.orderflow.telemetry.TraceContext.correlation(event.getCorrelationId().toString())) {
            cancellations.complete(event,OrderOutcomeService.hash(record.value()));
        }
    }
}
