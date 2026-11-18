package com.orderflow.notification;

import com.orderflow.contracts.envelope.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationService {
    private final JdbcTemplate jdbc;
    private final Clock clock;
    public NotificationService(JdbcTemplate jdbc, Clock clock) { this.jdbc = jdbc; this.clock = clock; }
    public static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    public record Lifecycle(EventEnvelope<?> event, UUID orderId, UUID customerId, String type, String message) {}
    public record Notification(UUID orderId, String eventType, long version, String message, Instant recordedAt) {}

    @Transactional(timeout=5)
    public void record(Lifecycle lifecycle, String payloadHash) {
        var event = lifecycle.event();
        if (lifecycle.orderId() == null || lifecycle.customerId() == null || !lifecycle.orderId().equals(event.getAggregateId())) {
            throw new EventValidationException("Lifecycle payload does not match aggregate");
        }
        var now = Timestamp.from(clock.instant());
        int added = jdbc.update("""
            INSERT INTO notification_schema.consumer_inbox(consumer_name,event_id,payload_hash,processed_at)
            VALUES ('notification-service',?,?,?) ON CONFLICT DO NOTHING
            """, event.getEventId(), payloadHash, now);
        if (added == 0) {
            String previous = jdbc.queryForObject("SELECT payload_hash FROM notification_schema.consumer_inbox WHERE consumer_name='notification-service' AND event_id=?", String.class, event.getEventId());
            if (!payloadHash.equals(previous)) throw new EventValidationException("Event ID reused with changed content");
            return;
        }
        // Typed DTOs omit unknown additive JSON fields and have deterministic field ordering.
        String businessHash = hash(event.getEventType() + ":" + event.getAggregateVersion() + ":" + event.getData());
        int inserted = jdbc.update("""
            INSERT INTO notification_schema.notifications(id,order_id,event_type,message,recorded_at,customer_id,
            source_event_id,order_version,delivery_status,payload_hash)
            VALUES (?,?,?,?,?,?,?,?,'RECORDED',?) ON CONFLICT (order_id,event_type) DO NOTHING
            """, UUID.randomUUID(), lifecycle.orderId(), lifecycle.type(), lifecycle.message(), now,
                lifecycle.customerId(), event.getEventId(), event.getAggregateVersion(), businessHash);
        if (inserted == 0) {
            String previous = jdbc.queryForObject("SELECT payload_hash FROM notification_schema.notifications WHERE order_id=? AND event_type=?", String.class, lifecycle.orderId(), lifecycle.type());
            if (!businessHash.equals(previous)) throw new EventValidationException("Conflicting lifecycle notification");
        }
    }

    @Transactional(readOnly=true)
    public List<Notification> timeline(UUID orderId) {
        return jdbc.query("SELECT * FROM notification_schema.notifications WHERE order_id=? ORDER BY order_version NULLS FIRST,recorded_at,id",
                (row, index) -> new Notification(row.getObject("order_id",UUID.class), row.getString("event_type"),
                        row.getLong("order_version"), row.getString("message"), row.getTimestamp("recorded_at").toInstant()), orderId);
    }

    public Optional<Notification> latest(UUID orderId) {
        var records = timeline(orderId);
        return records.isEmpty() ? Optional.empty() : Optional.of(records.getLast());
    }
}
