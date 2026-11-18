package com.orderflow.notification;

import com.orderflow.contracts.envelope.EventValidationException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class NotificationKafkaConfig {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,String> notificationFactory(
            ConsumerFactory<String,String> consumers, KafkaTemplate<String,String> kafka,
            KafkaListenerEndpointRegistry registry,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(record.topic() + ".notification-service.DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(java.time.Duration.ofSeconds(10));
        var retry = new ExponentialBackOffWithMaxRetries(3);
        retry.setInitialInterval(1000); retry.setMultiplier(2); retry.setMaxInterval(4000);
        var errorHandler = new DefaultErrorHandler(recoverer, retry,
                new ContainerPausingBackOffHandler(new ListenerContainerPauseService(registry, notificationRetryScheduler())));
        errorHandler.addNotRetryableExceptions(EventValidationException.class);
        errorHandler.setBackOffFunction((record, error) -> {
            for (Throwable cause=error; cause!=null; cause=cause.getCause()) {
                if (cause instanceof org.springframework.transaction.CannotCreateTransactionException
                        || cause instanceof org.springframework.dao.DataAccessResourceFailureException) {
                    return () -> new org.springframework.util.backoff.BackOffExecution() {
                        private int attempt;
                        public long nextBackOff() { return switch (attempt++) { case 0 -> 1000; case 1 -> 2000; case 2 -> 4000; default -> 60000; }; }
                    };
                }
            }
            return retry;
        });
        var factory = new ConcurrentKafkaListenerContainerFactory<String,String>();
        factory.setConsumerFactory(consumers);
        factory.setConcurrency(3);
        factory.setAutoStartup(autoStartup);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setPollTimeout(1000);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
    @Bean
    public ThreadPoolTaskScheduler notificationRetryScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("notification-service-retry-");
        return scheduler;
    }
}