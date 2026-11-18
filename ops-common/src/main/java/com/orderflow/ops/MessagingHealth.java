package com.orderflow.ops;
import org.springframework.boot.health.contributor.*;
import org.springframework.boot.health.actuate.endpoint.*;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.apache.kafka.clients.admin.Admin;
import java.util.*;
import java.util.concurrent.TimeUnit;
public class MessagingHealth implements HealthIndicator,AutoCloseable {
    private final Admin admin;private final KafkaListenerEndpointRegistry listeners;
    public MessagingHealth(KafkaProperties properties,KafkaListenerEndpointRegistry listeners){this.listeners=listeners;var settings=new HashMap<>(properties.buildAdminProperties());settings.put("request.timeout.ms",1000);settings.put("default.api.timeout.ms",1000);admin=Admin.create(settings);}
    public Health health(){try{admin.describeCluster().clusterId().get(1,TimeUnit.SECONDS);return listeners.getListenerContainers().stream().allMatch(c->c.isRunning()&&!c.isPauseRequested()&&c.getAssignedPartitions()!=null&&!c.getAssignedPartitions().isEmpty())?Health.up().build():Health.down().build();}catch(Exception e){if(e instanceof InterruptedException)Thread.currentThread().interrupt();return Health.down().build();}}
    @jakarta.annotation.PreDestroy public void close(){admin.close(java.time.Duration.ofSeconds(1));}
}
