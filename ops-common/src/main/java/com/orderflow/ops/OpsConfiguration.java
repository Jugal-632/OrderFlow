package com.orderflow.ops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.envelope.EventValidationException;
import com.orderflow.security.AccessAudit;
import javax.sql.DataSource;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods=false)
@EnableScheduling
@Import({OpsController.class,InternalOpsController.class})
public class OpsConfiguration {
    @Bean public OperationalMetrics operationalMetrics(OpsService ops,JdbcTemplate jdbc,io.micrometer.core.instrument.MeterRegistry meters) {return new OperationalMetrics(ops.schema,jdbc,meters);}
    @Bean public MessagingHealth messagingHealth(org.springframework.boot.kafka.autoconfigure.KafkaProperties properties,org.springframework.kafka.config.KafkaListenerEndpointRegistry listeners) {return new MessagingHealth(properties,listeners);}
    @Bean public org.springframework.boot.health.contributor.HealthIndicator schemaHealth(OpsService ops,JdbcTemplate jdbc) {
        return ()->{try{jdbc.queryForObject("SELECT count(*) FROM "+ops.schema+".outbox_events WHERE false",Long.class);return org.springframework.boot.health.contributor.Health.up().build();}catch(Exception e){return org.springframework.boot.health.contributor.Health.down().build();}};
    }
    @Bean public OpsService opsService(@Value("${spring.application.name}") String name,JdbcTemplate jdbc,ObjectMapper json,ReplayHandler handler,PlatformTransactionManager tx) {return new OpsService(name,jdbc,json,handler,tx);}
    @Bean public OpsListeners opsListeners(OpsService ops) {return new OpsListeners(ops);}
    @Bean public AccessAudit opsAccessAudit(OpsService ops) {return (status,correlation)->ops.audit("UNAUTHENTICATED_OR_FORBIDDEN",null,status,"ACCESS_DENIED:"+correlation);}
    @Bean @ConditionalOnProperty(name="ops.relay.enabled",havingValue="true",matchIfMissing=true)
    public OpsRelay opsRelay(DataSource data,JdbcTemplate jdbc,KafkaTemplate<String,String> kafka,OpsService ops,PlatformTransactionManager tx) {return new OpsRelay(data,jdbc,kafka,ops,tx);}
    @Bean public KafkaAdmin.NewTopics replayTopics(@Value("${spring.application.name}") String name) {
        var topics=new java.util.LinkedHashSet<String>();topics.add(name+".replay");topics.add(name+".replay.DLT");
        var sources=switch(name){
            case "order-service" -> java.util.List.of("inventory.reserved","inventory.rejected","inventory.released");
            case "inventory-service" -> java.util.List.of("order.placed","order.cancel.requested");
            case "notification-service" -> java.util.List.of("order.placed","order.confirmed","order.failed","order.cancelled");
            default -> throw new IllegalArgumentException("Unknown operations service");
        };
        for(String source:sources){topics.add(source);topics.add(source+"."+name+".DLT");}
        return new KafkaAdmin.NewTopics(topics.stream().map(topic->TopicBuilder.name(topic).partitions(3).replicas(1).build()).toArray(org.apache.kafka.clients.admin.NewTopic[]::new));
    }
    @Bean public ConcurrentKafkaListenerContainerFactory<String,String> opsIndexerFactory(ConsumerFactory<String,String> consumer,@Value("${spring.kafka.listener.auto-startup:true}") boolean start) {
        var factory=base(consumer,start);
        // Indexing failure never recurses into another DLT; offset stays pending until the DB recovers.
        var errors=new DefaultErrorHandler(new FixedBackOff(1000,FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.setClassifications(java.util.Map.of(),true);
        factory.setCommonErrorHandler(errors);return factory;
    }
    @Bean public ConcurrentKafkaListenerContainerFactory<String,String> opsReplayFactory(ConsumerFactory<String,String> consumer,KafkaTemplate<String,String> kafka,
            @Value("${spring.application.name}") String name,@Value("${spring.kafka.listener.auto-startup:true}") boolean start) {
        var factory=base(consumer,start);var recoverer=new DeadLetterPublishingRecoverer(kafka,(r,e)->new TopicPartition(name+".replay.DLT",r.partition()));
        recoverer.setFailIfSendResultIsError(true);recoverer.setWaitForSendResultTimeout(java.time.Duration.ofSeconds(10));
        var errors=new DefaultErrorHandler(recoverer,new FixedBackOff(1000,3));errors.addNotRetryableExceptions(EventValidationException.class);
        factory.setCommonErrorHandler(errors);return factory;
    }
    private static ConcurrentKafkaListenerContainerFactory<String,String> base(ConsumerFactory<String,String> consumer,boolean start) {
        var factory=new ConcurrentKafkaListenerContainerFactory<String,String>();factory.setConsumerFactory(consumer);factory.setConcurrency(3);factory.setAutoStartup(start);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);return factory;
    }
}
