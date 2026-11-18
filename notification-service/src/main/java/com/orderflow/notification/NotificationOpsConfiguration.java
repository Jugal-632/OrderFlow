package com.orderflow.notification;
import com.orderflow.ops.*;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.*;
@Configuration
@Import(OpsConfiguration.class)
public class NotificationOpsConfiguration {
    @Bean ReplayHandler replayHandler(OrderLifecycleConsumer notifications) {
        return new ReplayHandler() {
            public Set<String> topics() {return Set.of("order.placed","order.confirmed","order.failed","order.cancelled");}
            public void dispatch(String topic,String payload) {notifications.receive(new ConsumerRecord<>(topic,0,0,"replay",payload));}
        };
    }
}
