package com.orderflow.order;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.*;
import org.apache.kafka.clients.consumer.*;

/** Present only on the test classpath; terminate after the domain transaction, before RECORD acknowledgement. */
public class InventoryCommitCrashProcess {
    public static void main(String[] args){new SpringApplicationBuilder(com.orderflow.inventory.InventoryServiceApplication.class,Crash.class).run(
        "--spring.application.name=inventory-service","--spring.flyway.locations=filesystem:src/main/resources/db/migration",
        "--spring.flyway.schemas=inventory_schema","--spring.flyway.default-schema=inventory_schema","--spring.jpa.hibernate.ddl-auto=none");}
    @TestConfiguration(proxyBeanMethods=false)
    static class Crash {
        @Bean org.springframework.context.ApplicationListener<org.springframework.context.event.ContextRefreshedEvent> crashAfterCommit(KafkaListenerEndpointRegistry registry){return event->{
            if(event.getApplicationContext().getParent()!=null)return;
            var container=(AbstractMessageListenerContainer<String,String>)registry.getListenerContainer("inventory-reservations");
            container.stop();
            container.setRecordInterceptor(new RecordInterceptor<String,String>(){
                public ConsumerRecord<String,String> intercept(ConsumerRecord<String,String> record,Consumer<String,String> consumer){return record;}
                public void success(ConsumerRecord<String,String> record,Consumer<String,String> consumer){Runtime.getRuntime().halt(73);}
            });
            container.start();
        };}
    }
}
