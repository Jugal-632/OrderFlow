package com.orderflow.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaConfig {

    @Bean
    public org.springframework.kafka.core.KafkaAdmin.NewTopics cancellationTopics() {
        return new org.springframework.kafka.core.KafkaAdmin.NewTopics(
                TopicBuilder.name("order.cancel.requested").partitions(3).replicas(1).build(),
                TopicBuilder.name("order.cancelled").partitions(3).replicas(1).build(),
                TopicBuilder.name("inventory.released.order-service.DLT").partitions(3).replicas(1).build(),
                TopicBuilder.name("order.cancelled.notification-service.DLT").partitions(3).replicas(1).build());
    }

    @Bean
    public NewTopic orderPlacedTopic() {
        return TopicBuilder.name("order.placed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderFailedTopic() {
        return TopicBuilder.name("order.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name("inventory.reserved")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
