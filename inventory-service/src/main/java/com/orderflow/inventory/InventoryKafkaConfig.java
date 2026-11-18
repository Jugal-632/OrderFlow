package com.orderflow.inventory;

import com.orderflow.contracts.envelope.EventValidationException;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOff;

@Configuration
public class InventoryKafkaConfig {
    @Bean
    org.springframework.kafka.core.KafkaAdmin.NewTopics releaseTopics() {
        return new org.springframework.kafka.core.KafkaAdmin.NewTopics(
                org.springframework.kafka.config.TopicBuilder.name("order.placed.inventory-service.DLT").partitions(3).replicas(1).build(),
                org.springframework.kafka.config.TopicBuilder.name("inventory.released").partitions(3).replicas(1).build(),
                org.springframework.kafka.config.TopicBuilder.name("order.cancel.requested.inventory-service.DLT").partitions(3).replicas(1).build());
    }
    @Bean
    ThreadPoolTaskScheduler inventoryRetryScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("inventory-retry-");
        return scheduler;
    }

    @Bean
    DefaultErrorHandler inventoryErrorHandler(KafkaTemplate<String, String> kafka,
            KafkaListenerEndpointRegistry registry, ThreadPoolTaskScheduler inventoryRetryScheduler) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(record.topic() + ".inventory-service.DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
        var retries = new ExponentialBackOffWithMaxRetries(3);
        retries.setInitialInterval(1000);
        retries.setMultiplier(2);
        retries.setMaxInterval(4000);
        var handler = new DefaultErrorHandler(recoverer, retries,
                new ContainerPausingBackOffHandler(new ListenerContainerPauseService(registry, inventoryRetryScheduler)));
        handler.addNotRetryableExceptions(EventValidationException.class);
        handler.setBackOffFunction((record, error) -> infrastructureFailure(error) ? infrastructureBackOff() : retries);
        return handler;
    }

    static boolean infrastructureFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof CannotCreateTransactionException || cause instanceof DataAccessResourceFailureException) return true;
        }
        return false;
    }

    static BackOff infrastructureBackOff() {
        // Three bounded retries, then pause/poll at 60-second intervals until recovery.
        return () -> new org.springframework.util.backoff.BackOffExecution() {
            private int attempt;
            public long nextBackOff() { return switch (attempt++) { case 0 -> 1000; case 1 -> 2000; case 2 -> 4000; default -> 60000; }; }
        };
    }

    @Bean
    HealthIndicator inventoryConsumer(KafkaListenerEndpointRegistry registry) {
        return () -> registry.getListenerContainers().stream().allMatch(c -> c.isRunning() && !c.isPauseRequested())
                ? Health.up().build() : Health.down().withDetail("consumer", "stopped or waiting for dependency recovery").build();
    }
}
