package com.orderflow.inventory;

import com.orderflow.contracts.envelope.EventValidationException;
import com.orderflow.contracts.envelope.EventValidator;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(name = "inventory.relay.enabled", havingValue = "true", matchIfMissing = true)
public class InventoryOutboxRelay {
    static final long LOCK_ID = 0x494E564F5554424FL;
    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transaction;
    private final Clock clock;
    public InventoryOutboxRelay(DataSource dataSource, JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
            PlatformTransactionManager manager, Clock clock) {
        this.dataSource = dataSource; this.jdbc = jdbc; this.kafka = kafka; this.clock = clock;
        this.transaction = new TransactionTemplate(manager);
        this.transaction.setTimeout(5);
    }

    @Scheduled(fixedDelay = 1000)
    public void publishPending() throws Exception {
        // This connection is held exclusively for the session lock, never a business transaction.
        try (Connection session = dataSource.getConnection()) {
            boolean locked = false;
            try {
                try (var statement = session.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                    statement.setLong(1, LOCK_ID);
                    try (var result = statement.executeQuery()) { result.next(); locked = result.getBoolean(1); }
                }
                if (!locked) return;
                var rows = jdbc.queryForList("""
                        SELECT o.* FROM inventory_schema.outbox_events o
                        WHERE o.event_type<>'ops.replay' AND o.status='PENDING' AND o.next_attempt_at<=?
                        AND NOT EXISTS (SELECT 1 FROM inventory_schema.outbox_events p
                          WHERE p.aggregate_id=o.aggregate_id AND p.status<>'PUBLISHED'
                          AND (p.aggregate_version,p.created_at,p.id)<(o.aggregate_version,o.created_at,o.id))
                        ORDER BY o.created_at,o.id LIMIT 100
                        """, Timestamp.from(clock.instant()));
                for (var row : rows) {
                    UUID id = (UUID) row.get("id");
                    String payload = row.get("payload").toString();
                    try(var telemetry=com.orderflow.telemetry.Operation.event("outbox.publish",payload,(String)row.get("topic"))) {
                    ProducerRecord<String, String> record;
                    try {
                        EventValidator.validateJson(payload);
                        var root = EventValidator.getObjectMapper().readTree(payload);
                        String type = root.path("eventType").asText();
                        if ((!type.equals("inventory.reserved") && !type.equals("inventory.rejected") && !type.equals("inventory.released"))
                                || !id.toString().equals(root.path("eventId").asText())
                                || !row.get("topic").equals(type) || !row.get("event_type").equals(type)
                                || !row.get("aggregate_id").toString().equals(root.path("aggregateId").asText())
                                || !row.get("event_key").equals(root.path("aggregateId").asText())) {
                            throw new EventValidationException("Stored outbox metadata mismatch");
                        }
                        record = new ProducerRecord<>((String) row.get("topic"), (String) row.get("event_key"), payload);
                        for (String field : new String[]{"eventId", "eventType", "schemaVersion", "correlationId"}) {
                            String header = field.equals("correlationId") ? "X-Correlation-Id" : field;
                            record.headers().add(header, root.path(field).asText().getBytes(StandardCharsets.UTF_8));
                        }
                    } catch (EventValidationException | com.fasterxml.jackson.core.JsonProcessingException invalid) {
                        transaction.executeWithoutResult(s -> jdbc.update("UPDATE inventory_schema.outbox_events SET status='BLOCKED',last_error='INVALID_STORED_PAYLOAD' WHERE id=?", id));
                        LoggerFactory.getLogger(getClass()).error("Blocked invalid inventory outbox event {}", id);
                        continue;
                    }
                    try {
                        kafka.send(record).get(10, TimeUnit.SECONDS);
                        transaction.executeWithoutResult(s -> jdbc.update("UPDATE inventory_schema.outbox_events SET status='PUBLISHED',published_at=?,last_error=NULL WHERE id=? AND status='PENDING'", Timestamp.from(clock.instant()), id));
                        telemetry.success();
                    } catch (Exception failure) {
                        int attempts = ((Number) row.get("retry_count")).intValue();
                        long delay = attempts >= 6 ? 60 : 1L << attempts;
                        transaction.executeWithoutResult(s -> jdbc.update("UPDATE inventory_schema.outbox_events SET retry_count=retry_count+1,next_attempt_at=?,last_error=? WHERE id=? AND status='PENDING'",
                                Timestamp.from(clock.instant().plusSeconds(delay)), failure.getClass().getSimpleName(), id));
                        LoggerFactory.getLogger(getClass()).warn("Inventory outbox {} remains pending after {}", id, failure.getClass().getSimpleName());
                        if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); return; }
                    }
                    }
                }
            } finally {
                if (locked) {
                    try (var statement = session.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                        statement.setLong(1, LOCK_ID); statement.execute();
                    } catch (Exception unlockFailure) {
                        // A pooled connection must never return with a session lock still held.
                        session.abort(Runnable::run);
                        throw unlockFailure;
                    }
                }
            }
        }
    }
}
