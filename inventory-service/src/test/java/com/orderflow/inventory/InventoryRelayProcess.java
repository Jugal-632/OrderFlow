package com.orderflow.inventory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/** Forked integration-test worker. No production fault-injection hooks. */
public class InventoryRelayProcess {
    public static void main(String[] args) throws Exception {
        var source = new DriverManagerDataSource(args[0], args[1], args[2]);
        var factory = new DefaultKafkaProducerFactory<String,String>(Map.of(
                "bootstrap.servers", args[3], "acks", "all", "enable.idempotence", true,
                "delivery.timeout.ms", 10000, "request.timeout.ms", 5000,
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
        KafkaTemplate<String,String> template = new KafkaTemplate<>(factory) {
            @Override public CompletableFuture<SendResult<String,String>> send(ProducerRecord<String,String> record) {
                return super.send(record).thenApply(result -> {
                    if (args[4].equals("crash")) Runtime.getRuntime().halt(73);
                    return result;
                });
            }
        };
        try {
            new InventoryOutboxRelay(source, new JdbcTemplate(source), template,
                    new DataSourceTransactionManager(source), Clock.systemUTC()).publishPending();
        } finally { factory.destroy(); }
    }
}
