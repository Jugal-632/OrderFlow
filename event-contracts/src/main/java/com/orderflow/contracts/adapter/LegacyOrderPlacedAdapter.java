package com.orderflow.contracts.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.envelope.EventEnvelope;
import com.orderflow.contracts.envelope.EventValidator;
import com.orderflow.contracts.events.OrderPlacedEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Migration adapter for transforming un-enveloped legacy 'order.placed' records
 * into Envelope Version 1 format.
 * Active under 'local-legacy-events' profile.
 */
public class LegacyOrderPlacedAdapter {

    private static final ObjectMapper MAPPER = EventValidator.getObjectMapper();
    private static final String PRODUCER = "order-service";

    /**
     * Converts a retained legacy Kafka record into an Envelope Version 1 event.
     * EventId is deterministically derived from topic, partition, and offset.
     * CorrelationId is deterministically derived from orderId.
     */
    public static EventEnvelope<OrderPlacedEvent> adaptKafkaRecord(
            String topic,
            int partition,
            long offset,
            String rawJson,
            Instant fallbackTimestamp) {

        try {
            JsonNode node = MAPPER.readTree(rawJson);

            // If already an envelope, deserialize directly
            if (node.has("schemaVersion") && node.has("data")) {
                return EventValidator.deserializeAndValidate(rawJson, OrderPlacedEvent.class);
            }

            // Extract flat legacy fields
            UUID orderId = UUID.fromString(node.get("orderId").asText());
            UUID customerId = UUID.fromString(node.get("customerId").asText());
            UUID productId = UUID.fromString(node.get("productId").asText());
            int quantity = node.get("quantity").asInt();

            Instant occurredAt = fallbackTimestamp != null ? fallbackTimestamp : Instant.now();
            if (node.has("createdAt") && !node.get("createdAt").isNull()) {
                try {
                    occurredAt = Instant.parse(node.get("createdAt").asText());
                } catch (Exception ignored) {
                }
            }

            // Deterministic UUIDs
            UUID eventId = UUID.nameUUIDFromBytes((topic + ":" + partition + ":" + offset).getBytes(StandardCharsets.UTF_8));
            UUID correlationId = UUID.nameUUIDFromBytes(("corr:" + orderId).getBytes(StandardCharsets.UTF_8));
            UUID causationId = eventId;

            OrderPlacedEvent data = OrderPlacedEvent.builder()
                    .orderId(orderId)
                    .customerId(customerId)
                    .productId(productId)
                    .quantity(quantity)
                    .build();

            EventEnvelope<OrderPlacedEvent> envelope = EventEnvelope.<OrderPlacedEvent>builder()
                    .eventId(eventId)
                    .eventType(OrderPlacedEvent.EVENT_TYPE)
                    .schemaVersion(1)
                    .occurredAt(occurredAt)
                    .producer(PRODUCER)
                    .aggregateId(orderId)
                    .aggregateVersion(1)
                    .correlationId(correlationId)
                    .causationId(causationId)
                    .data(data)
                    .build();

            EventValidator.validateJsonNode(MAPPER.valueToTree(envelope));
            return envelope;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to adapt legacy Kafka record: " + e.getMessage(), e);
        }
    }

    /**
     * Adapts an unpublished legacy outbox row to Envelope Version 1.
     * Matches the migration rule: event ID equals existing outbox ID,
     * aggregate ID equals order ID, occurrence time equals created time.
     */
    public static EventEnvelope<OrderPlacedEvent> adaptOutboxPayload(
            UUID outboxId,
            UUID orderId,
            Instant createdAt,
            String rawPayload) {

        try {
            JsonNode node = MAPPER.readTree(rawPayload);

            if (node.has("schemaVersion") && node.has("data")) {
                return EventValidator.deserializeAndValidate(rawPayload, OrderPlacedEvent.class);
            }

            UUID customerId = UUID.fromString(node.get("customerId").asText());
            UUID productId = UUID.fromString(node.get("productId").asText());
            int quantity = node.get("quantity").asInt();

            UUID correlationId = UUID.nameUUIDFromBytes(("corr:" + orderId).getBytes(StandardCharsets.UTF_8));

            OrderPlacedEvent data = OrderPlacedEvent.builder()
                    .orderId(orderId)
                    .customerId(customerId)
                    .productId(productId)
                    .quantity(quantity)
                    .build();

            EventEnvelope<OrderPlacedEvent> envelope = EventEnvelope.<OrderPlacedEvent>builder()
                    .eventId(outboxId)
                    .eventType(OrderPlacedEvent.EVENT_TYPE)
                    .schemaVersion(1)
                    .occurredAt(createdAt)
                    .producer(PRODUCER)
                    .aggregateId(orderId)
                    .aggregateVersion(1)
                    .correlationId(correlationId)
                    .causationId(outboxId)
                    .data(data)
                    .build();

            EventValidator.validateJsonNode(MAPPER.valueToTree(envelope));
            return envelope;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to adapt legacy outbox row: " + e.getMessage(), e);
        }
    }
}
