package com.orderflow.inventory;
import com.orderflow.ops.*;
import java.util.Set;
import org.springframework.context.annotation.*;
@Configuration
@Import(OpsConfiguration.class)
public class InventoryOpsConfiguration {
    @Bean ReplayHandler replayHandler(InventoryListener placements,InventoryCancellationListener cancellations) {
        return new ReplayHandler() {
            public Set<String> topics() {return Set.of("order.placed","order.cancel.requested");}
            public void dispatch(String topic,String payload) {if(topic.equals("order.placed"))placements.receive(payload);else cancellations.receive(payload);}
        };
    }
}
