package com.orderflow.contracts;

import com.orderflow.contracts.envelope.EventEnvelope;
import com.orderflow.contracts.envelope.EventValidationException;
import com.orderflow.contracts.envelope.EventValidator;
import com.orderflow.contracts.events.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventEnvelopeSerializationTest {

    private final UUID orderId = UUID.randomUUID();
    private final UUID customerId = UUID.randomUUID();
    private final UUID productId = UUID.randomUUID();
    private final UUID reservationId = UUID.randomUUID();
    private final UUID cancellationId = UUID.randomUUID();
    private final UUID correlationId = UUID.randomUUID();
    private final UUID causationId = UUID.randomUUID();

    @Test
    @DisplayName("Should serialize and validate OrderPlacedEvent envelope against JSON Schema")
    void testOrderPlacedEnvelope() {
        OrderPlacedEvent data = OrderPlacedEvent.builder()
                .orderId(orderId)
                .customerId(customerId)
                .productId(productId)
                .quantity(2)
                .build();

        EventEnvelope<OrderPlacedEvent> envelope = EventEnvelope.<OrderPlacedEvent>builder()
                .eventId(UUID.randomUUID())
                .eventType(OrderPlacedEvent.EVENT_TYPE)
                .schemaVersion(1)
                .occurredAt(Instant.now())
                .producer("order-service")
                .aggregateId(orderId)
                .aggregateVersion(1)
                .correlationId(correlationId)
                .causationId(causationId)
                .data(data)
                .build();

        String json = EventValidator.serializeAndValidate(envelope);
        assertThat(json).contains("\"eventType\":\"order.placed\"");
        assertThat(json).contains("\"schemaVersion\":1");

        EventEnvelope<OrderPlacedEvent> deserialized = EventValidator.deserializeAndValidate(json, OrderPlacedEvent.class);
        assertThat(deserialized.getEventId()).isEqualTo(envelope.getEventId());
        assertThat(deserialized.getData().getQuantity()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should validate all Phase 4 event types successfully")
    void testAllEventTypes() {
        Instant now = Instant.now();
        UUID eventId = UUID.randomUUID();

        // 1. InventoryReserved
        InventoryReservedEvent reserved = InventoryReservedEvent.builder()
                .orderId(orderId).productId(productId).quantity(3).reservationId(reservationId).build();
        String jsonReserved = EventValidator.serializeAndValidate(EventEnvelope.<InventoryReservedEvent>builder()
                .eventId(eventId).eventType(InventoryReservedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("inventory-service").aggregateId(orderId).aggregateVersion(1)
                .correlationId(correlationId).causationId(causationId).data(reserved).build());
        assertThat(jsonReserved).isNotEmpty();

        // 2. InventoryRejected
        InventoryRejectedEvent rejected = InventoryRejectedEvent.builder()
                .orderId(orderId).productId(productId).quantity(5).reason("INSUFFICIENT_STOCK").availableQuantity(2).build();
        String jsonRejected = EventValidator.serializeAndValidate(EventEnvelope.<InventoryRejectedEvent>builder()
                .eventId(eventId).eventType(InventoryRejectedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("inventory-service").aggregateId(orderId).aggregateVersion(1)
                .correlationId(correlationId).causationId(causationId).data(rejected).build());
        assertThat(jsonRejected).isNotEmpty();

        // 3. OrderConfirmed
        OrderConfirmedEvent confirmed = OrderConfirmedEvent.builder()
                .orderId(orderId).customerId(customerId).productId(productId).quantity(3).build();
        String jsonConfirmed = EventValidator.serializeAndValidate(EventEnvelope.<OrderConfirmedEvent>builder()
                .eventId(eventId).eventType(OrderConfirmedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("order-service").aggregateId(orderId).aggregateVersion(2)
                .correlationId(correlationId).causationId(causationId).data(confirmed).build());
        assertThat(jsonConfirmed).isNotEmpty();

        // 4. OrderFailed
        OrderFailedEvent failed = OrderFailedEvent.builder()
                .orderId(orderId).customerId(customerId).reason("OUT_OF_STOCK").build();
        String jsonFailed = EventValidator.serializeAndValidate(EventEnvelope.<OrderFailedEvent>builder()
                .eventId(eventId).eventType(OrderFailedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("order-service").aggregateId(orderId).aggregateVersion(2)
                .correlationId(correlationId).causationId(causationId).data(failed).build());
        assertThat(jsonFailed).isNotEmpty();

        // 5. OrderCancelRequested
        OrderCancelRequestedEvent cancelReq = OrderCancelRequestedEvent.builder()
                .orderId(orderId).customerId(customerId).cancellationId(cancellationId).reservationId(reservationId).build();
        String jsonCancelReq = EventValidator.serializeAndValidate(EventEnvelope.<OrderCancelRequestedEvent>builder()
                .eventId(eventId).eventType(OrderCancelRequestedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("order-service").aggregateId(orderId).aggregateVersion(3)
                .correlationId(correlationId).causationId(causationId).data(cancelReq).build());
        assertThat(jsonCancelReq).isNotEmpty();

        // 6. InventoryReleased
        InventoryReleasedEvent released = InventoryReleasedEvent.builder()
                .orderId(orderId).reservationId(reservationId).cancellationId(cancellationId).quantity(3).build();
        String jsonReleased = EventValidator.serializeAndValidate(EventEnvelope.<InventoryReleasedEvent>builder()
                .eventId(eventId).eventType(InventoryReleasedEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("inventory-service").aggregateId(orderId).aggregateVersion(2)
                .correlationId(correlationId).causationId(causationId).data(released).build());
        assertThat(jsonReleased).isNotEmpty();

        // 7. OrderCancelled
        OrderCancelledEvent cancelled = OrderCancelledEvent.builder()
                .orderId(orderId).customerId(customerId).cancellationId(cancellationId).build();
        String jsonCancelled = EventValidator.serializeAndValidate(EventEnvelope.<OrderCancelledEvent>builder()
                .eventId(eventId).eventType(OrderCancelledEvent.EVENT_TYPE).schemaVersion(1)
                .occurredAt(now).producer("order-service").aggregateId(orderId).aggregateVersion(4)
                .correlationId(correlationId).causationId(causationId).data(cancelled).build());
        assertThat(jsonCancelled).isNotEmpty();
    }

    @Test
    @DisplayName("Should reject envelope with unsupported schema version")
    void testUnsupportedSchemaVersion() {
        String invalidJson = """
            {
              "eventId": "4bf3cbf5-670b-44e7-9968-73e9d156744e",
              "eventType": "order.placed",
              "schemaVersion": 2,
              "occurredAt": "2026-10-23T04:00:00Z",
              "producer": "order-service",
              "aggregateId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
              "aggregateVersion": 1,
              "correlationId": "2ef8f994-560b-4377-b97d-0809ab6f6e74",
              "causationId": "65d5d1b7-5d2e-45f4-807f-e1aa65effc18",
              "data": {
                "orderId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
                "customerId": "decaef96-5e1f-4d5f-8616-5d42778901e9",
                "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "quantity": 2
              }
            }
            """;

        assertThatThrownBy(() -> EventValidator.validateJson(invalidJson))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("Envelope validation failed");
    }

    @Test
    @DisplayName("Should reject order.placed with non-positive quantity")
    void testNonPositiveQuantityRejected() {
        String invalidJson = """
            {
              "eventId": "4bf3cbf5-670b-44e7-9968-73e9d156744e",
              "eventType": "order.placed",
              "schemaVersion": 1,
              "occurredAt": "2026-10-23T04:00:00Z",
              "producer": "order-service",
              "aggregateId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
              "aggregateVersion": 1,
              "correlationId": "2ef8f994-560b-4377-b97d-0809ab6f6e74",
              "causationId": "65d5d1b7-5d2e-45f4-807f-e1aa65effc18",
              "data": {
                "orderId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
                "customerId": "decaef96-5e1f-4d5f-8616-5d42778901e9",
                "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "quantity": 0
              }
            }
            """;

        assertThatThrownBy(() -> EventValidator.validateJson(invalidJson))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("Payload validation failed for 'order.placed'");
    }

    @Test
    @DisplayName("Should reject envelope missing required UUID fields")
    void testMissingRequiredFields() {
        String missingCorrelation = """
            {
              "eventId": "4bf3cbf5-670b-44e7-9968-73e9d156744e",
              "eventType": "order.placed",
              "schemaVersion": 1,
              "occurredAt": "2026-10-23T04:00:00Z",
              "producer": "order-service",
              "aggregateId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
              "aggregateVersion": 1,
              "causationId": "65d5d1b7-5d2e-45f4-807f-e1aa65effc18",
              "data": {
                "orderId": "0a41dc66-3dbe-4d3d-bb02-6ef0dc3433cf",
                "customerId": "decaef96-5e1f-4d5f-8616-5d42778901e9",
                "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "quantity": 2
              }
            }
            """;

        assertThatThrownBy(() -> EventValidator.validateJson(missingCorrelation))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("correlationId");
    }
}
