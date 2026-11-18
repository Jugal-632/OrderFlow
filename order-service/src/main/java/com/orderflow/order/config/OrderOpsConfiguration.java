package com.orderflow.order.config;
import com.orderflow.ops.*;
import com.orderflow.order.kafka.*;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.*;
@Configuration
@Import(OpsConfiguration.class)
public class OrderOpsConfiguration {
    @Bean ReplayHandler replayHandler(InventoryOutcomeConsumer outcomes,InventoryReleaseConsumer releases) {
        return new ReplayHandler() {
            public Set<String> topics() {return Set.of("inventory.reserved","inventory.rejected","inventory.released");}
            public void dispatch(String topic,String payload) {
                var record=new ConsumerRecord<String,String>(topic,0,0,"replay",payload);
                if(topic.equals("inventory.released")) releases.receive(record);else outcomes.receive(record);
            }
        };
    }
}
