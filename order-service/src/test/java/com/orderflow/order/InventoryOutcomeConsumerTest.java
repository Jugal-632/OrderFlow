package com.orderflow.order;

import com.orderflow.order.kafka.InventoryOutcomeConsumer;
import com.orderflow.order.service.OrderOutcomeService;
import com.orderflow.contracts.envelope.EventValidationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class InventoryOutcomeConsumerTest {
    @Test void malformedAndMissingPayloadNeverReachTransaction() {
        var service=mock(OrderOutcomeService.class); var listener=new InventoryOutcomeConsumer(service);
        for(String payload:new String[]{null,"","{broken"}) {
            assertThatThrownBy(()->listener.receive(new ConsumerRecord<>("inventory.reserved",0,0,"key",payload))).isInstanceOf(EventValidationException.class);
        }
        verifyNoInteractions(service);
    }
}
