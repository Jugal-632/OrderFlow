package com.orderflow.order.kafka;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import com.orderflow.order.service.OrderOutcomeService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryOutcomeConsumer {
    private final OrderOutcomeService outcomes;
    public InventoryOutcomeConsumer(OrderOutcomeService outcomes) { this.outcomes = outcomes; }

    @KafkaListener(id="order-outcomes", topics={"inventory.reserved","inventory.rejected"}, groupId="order-service", containerFactory="orderOutcomeFactory")
    public void receive(ConsumerRecord<String,String> record) {
        String json = record.value();
        if (json == null || json.isBlank()) throw new EventValidationException("Missing inventory payload");
        OrderOutcomeService.Outcome outcome;
        if (record.topic().equals("inventory.reserved")) {
            var event = EventValidator.deserializeAndValidate(json, InventoryReservedEvent.class);
            var data = event.getData();
            outcome = new OrderOutcomeService.Outcome(event, data.getOrderId(), data.getProductId(), data.getQuantity(), data.getReservationId(), null, null);
        } else if (record.topic().equals("inventory.rejected")) {
            var event = EventValidator.deserializeAndValidate(json, InventoryRejectedEvent.class);
            var data = event.getData();
            outcome = new OrderOutcomeService.Outcome(event, data.getOrderId(), data.getProductId(), data.getQuantity(), null, data.getReason(), data.getAvailableQuantity());
        } else throw new EventValidationException("Unsupported inventory topic");
        if (!record.topic().equals(outcome.envelope().getEventType())) throw new EventValidationException("Topic/event type mismatch");
        try (var ignored = com.orderflow.telemetry.TraceContext.correlation( outcome.envelope().getCorrelationId().toString())) {
            outcomes.apply(outcome, OrderOutcomeService.hash(json));
        }
    }
}
