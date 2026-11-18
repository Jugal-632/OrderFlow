package com.orderflow.notification;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderLifecycleConsumer {
    private final NotificationService notifications;
    public OrderLifecycleConsumer(NotificationService notifications) { this.notifications = notifications; }

    @KafkaListener(id="order-notifications", topics={"order.placed","order.confirmed","order.failed","order.cancelled"},
            groupId="notification-service", containerFactory="notificationFactory")
    public void receive(ConsumerRecord<String,String> record) {
        String json = record.value();
        if (json == null || json.isBlank()) throw new EventValidationException("Missing lifecycle payload");
        NotificationService.Lifecycle lifecycle = switch (record.topic()) {
            case "order.placed" -> {
                var event = EventValidator.deserializeAndValidate(json, OrderPlacedEvent.class);
                yield new NotificationService.Lifecycle(event, event.getData().getOrderId(), event.getData().getCustomerId(),
                        "ORDER_RECEIVED", "Order received and awaiting stock verification.");
            }
            case "order.confirmed" -> {
                var event = EventValidator.deserializeAndValidate(json, OrderConfirmedEvent.class);
                yield new NotificationService.Lifecycle(event, event.getData().getOrderId(), event.getData().getCustomerId(),
                        "ORDER_CONFIRMED", "Stock reserved. Order confirmed.");
            }
            case "order.failed" -> {
                var event = EventValidator.deserializeAndValidate(json, OrderFailedEvent.class);
                yield new NotificationService.Lifecycle(event, event.getData().getOrderId(), event.getData().getCustomerId(),
                        "ORDER_FAILED", "Order failed: " + event.getData().getReason());
            }
            case "order.cancelled" -> {
                var event = EventValidator.deserializeAndValidate(json, OrderCancelledEvent.class);
                yield new NotificationService.Lifecycle(event, event.getData().getOrderId(), event.getData().getCustomerId(),
                        "ORDER_CANCELLED", "Order cancelled and reserved stock restored.");
            }
            default -> throw new EventValidationException("Unsupported lifecycle topic");
        };
        if (!record.topic().equals(lifecycle.event().getEventType())) throw new EventValidationException("Topic/event type mismatch");
        try (var ignored = com.orderflow.telemetry.TraceContext.correlation( lifecycle.event().getCorrelationId().toString())) {
            notifications.record(lifecycle, NotificationService.hash(json));
        }
    }
}
