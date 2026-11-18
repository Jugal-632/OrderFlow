package com.orderflow.order;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.InventoryReleasedEvent;
import com.orderflow.order.kafka.InventoryReleaseConsumer;
import com.orderflow.order.service.*;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class InventoryReleaseConsumerTest {
    @Test void validatesBeforeDelegatingAndPreservesCorrelation() {
        var service=mock(OrderCancellationService.class);var consumer=new InventoryReleaseConsumer(service);
        UUID order=UUID.randomUUID(),correlation=UUID.randomUUID();
        var event=EventEnvelope.builder().eventId(UUID.randomUUID()).eventType("inventory.released").schemaVersion(1)
                .occurredAt(Instant.now()).producer("inventory-service").aggregateId(order).aggregateVersion(2)
                .correlationId(correlation).causationId(UUID.randomUUID())
                .data(new InventoryReleasedEvent(order,UUID.randomUUID(),UUID.randomUUID(),2)).build();
        String payload=EventValidator.serializeAndValidate(event);
        doAnswer(call->{assertThat(org.slf4j.MDC.get("correlationId")).isEqualTo(correlation.toString());return null;})
                .when(service).complete(any(),eq(OrderOutcomeService.hash(payload)));
        consumer.receive(new ConsumerRecord<>("inventory.released",0,0,order.toString(),payload));
        verify(service).complete(any(),eq(OrderOutcomeService.hash(payload)));assertThat(org.slf4j.MDC.get("correlationId")).isNull();
        assertThatThrownBy(()->consumer.receive(new ConsumerRecord<>("inventory.released",0,0,"key","bad"))).isInstanceOf(EventValidationException.class);
        assertThatThrownBy(()->consumer.receive(new ConsumerRecord<>("inventory.reserved",0,0,"key",payload))).isInstanceOf(EventValidationException.class);
        verifyNoMoreInteractions(service);
    }
}
