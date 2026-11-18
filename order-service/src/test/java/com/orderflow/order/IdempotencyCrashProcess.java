package com.orderflow.order;

import com.orderflow.order.dto.CreateOrderRequest;
import com.orderflow.order.service.IdempotentOrderService;
import java.util.UUID;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Test-only worker: terminate after the transactional proxy commits, before delivering any response. */
public class IdempotencyCrashProcess {
    public static void main(String[] args) {
        var context=new SpringApplicationBuilder(OrderServiceApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url="+args[0],"--spring.datasource.username="+args[1],"--spring.datasource.password="+args[2],
                "--spring.flyway.locations=filesystem:src/main/resources/db/migration","--spring.kafka.admin.auto-create=false",
                "--spring.kafka.listener.auto-startup=false","--order.relay.enabled=false","--spring.jpa.show-sql=false");
        if(args.length>5 && args[5].equals("before-commit")) {
            new org.springframework.transaction.support.TransactionTemplate(context.getBean(org.springframework.transaction.PlatformTransactionManager.class)).executeWithoutResult(s->{
                context.getBean(IdempotentOrderService.class).create(UUID.fromString(args[3]),"crash",new CreateOrderRequest(UUID.fromString(args[4]),2));
                Runtime.getRuntime().halt(73);
            });
        }
        context.getBean(IdempotentOrderService.class).create(UUID.fromString(args[3]),"crash",new CreateOrderRequest(UUID.fromString(args[4]),2));
        Runtime.getRuntime().halt(73);
    }
}
