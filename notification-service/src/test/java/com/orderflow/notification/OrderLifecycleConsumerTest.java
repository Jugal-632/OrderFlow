package com.orderflow.notification;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.OrderPlacedEvent;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderLifecycleConsumerTest {
    @Test void malformedAndMissingPayloadNeverReachTransaction() {
        var service=mock(NotificationService.class); var listener=new OrderLifecycleConsumer(service);
        for(String payload:new String[]{null,"","{broken"}) {
            assertThatThrownBy(()->listener.receive(new ConsumerRecord<>("order.placed",0,0,"key",payload))).isInstanceOf(EventValidationException.class);
        }
        verifyNoInteractions(service);
    }
    @Test void transactionFailurePropagatesToKafkaErrorHandler() {
        var service=mock(NotificationService.class); var listener=new OrderLifecycleConsumer(service);
        UUID order=UUID.randomUUID();
        var event=EventEnvelope.builder().eventId(UUID.randomUUID()).eventType("order.placed").occurredAt(Instant.now())
                .producer("order-service").aggregateId(order).correlationId(UUID.randomUUID()).causationId(UUID.randomUUID())
                .data(new OrderPlacedEvent(order,UUID.randomUUID(),UUID.randomUUID(),2)).build();
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("unavailable")).when(service).record(any(),any());
        assertThatThrownBy(()->listener.receive(new ConsumerRecord<>("order.placed",0,0,order.toString(),EventValidator.serializeAndValidate(event))))
                .isInstanceOf(org.springframework.dao.DataAccessResourceFailureException.class);
    }
}
