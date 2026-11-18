package com.orderflow.contracts;

import com.orderflow.contracts.adapter.LegacyOrderPlacedAdapter;
import com.orderflow.contracts.envelope.EventEnvelope;
import com.orderflow.contracts.events.OrderPlacedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LegacyOrderPlacedAdapterTest {

    @Test
    @DisplayName("Should successfully adapt flat legacy order.placed Kafka record into Envelope Version 1")
    void testAdaptLegacyKafkaRecord() {
        String legacyKafkaJson = """
            {
              "orderId": "e650f65f-04a7-4baa-935e-5e1d8c21d6c2",
              "quantity": 2,
              "createdAt": "2026-10-24T06:27:19.709758600Z",
              "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
              "customerId": "11111111-1111-1111-1111-111111111111"
            }
            """;

        String topic = "order.placed";
        int partition = 0;
        long offset = 42L;

        EventEnvelope<OrderPlacedEvent> envelope = LegacyOrderPlacedAdapter.adaptKafkaRecord(
                topic, partition, offset, legacyKafkaJson, Instant.now()
        );

        assertThat(envelope).isNotNull();
        assertThat(envelope.getSchemaVersion()).isEqualTo(1);
        assertThat(envelope.getEventType()).isEqualTo("order.placed");
        assertThat(envelope.getProducer()).isEqualTo("order-service");
        assertThat(envelope.getAggregateId()).isEqualTo(UUID.fromString("e650f65f-04a7-4baa-935e-5e1d8c21d6c2"));
        assertThat(envelope.getCorrelationId()).isNotNull();
        assertThat(envelope.getEventId()).isNotNull();

        // Deterministic derivation test: same topic/partition/offset must yield same eventId
        EventEnvelope<OrderPlacedEvent> envelope2 = LegacyOrderPlacedAdapter.adaptKafkaRecord(
                topic, partition, offset, legacyKafkaJson, Instant.now()
        );
        assertThat(envelope2.getEventId()).isEqualTo(envelope.getEventId());
        assertThat(envelope2.getCorrelationId()).isEqualTo(envelope.getCorrelationId());

        // Verify data payload
        OrderPlacedEvent data = envelope.getData();
        assertThat(data.getOrderId()).isEqualTo(UUID.fromString("e650f65f-04a7-4baa-935e-5e1d8c21d6c2"));
        assertThat(data.getQuantity()).isEqualTo(2);
        assertThat(data.getProductId()).isEqualTo(UUID.fromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890"));
        assertThat(data.getCustomerId()).isEqualTo(UUID.fromString("11111111-1111-1111-1111-111111111111"));
    }

    @Test
    @DisplayName("Should successfully adapt legacy outbox row to Envelope Version 1")
    void testAdaptLegacyOutboxPayload() {
        UUID outboxId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-10-24T06:00:00Z");

        String legacyPayload = """
            {
              "orderId": "%s",
              "customerId": "11111111-1111-1111-1111-111111111111",
              "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
              "quantity": 3
            }
            """.formatted(orderId);

        EventEnvelope<OrderPlacedEvent> envelope = LegacyOrderPlacedAdapter.adaptOutboxPayload(
                outboxId, orderId, createdAt, legacyPayload
        );

        assertThat(envelope.getEventId()).isEqualTo(outboxId);
        assertThat(envelope.getAggregateId()).isEqualTo(orderId);
        assertThat(envelope.getOccurredAt()).isEqualTo(createdAt);
        assertThat(envelope.getSchemaVersion()).isEqualTo(1);
        assertThat(envelope.getData().getQuantity()).isEqualTo(3);
    }
}
