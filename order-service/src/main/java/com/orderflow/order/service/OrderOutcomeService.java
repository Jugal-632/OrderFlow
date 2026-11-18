package com.orderflow.order.service;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class OrderOutcomeService {
    private final JdbcTemplate jdbc;
    private final Clock clock;
    public OrderOutcomeService(JdbcTemplate jdbc, Clock clock) { this.jdbc = jdbc; this.clock = clock; }

    public static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    public record Outcome(EventEnvelope<?> envelope, UUID orderId, UUID productId, int quantity,
            UUID reservationId, String reason, Integer availableQuantity) {}

    @Transactional(isolation = Isolation.READ_COMMITTED, timeout = 5)
    public void apply(Outcome outcome, String payloadHash) {
        var event = outcome.envelope();
        boolean reserved = "inventory.reserved".equals(event.getEventType());
        if ((!reserved && !"inventory.rejected".equals(event.getEventType())) || event.getSchemaVersion() != 1
                || outcome.orderId() == null || outcome.productId() == null || outcome.quantity() <= 0
                || !outcome.orderId().equals(event.getAggregateId())
                || (reserved && outcome.reservationId() == null)
                || (!reserved && (outcome.reason() == null || outcome.reason().isBlank() || outcome.reason().length() > 255))) {
            throw new EventValidationException("Invalid inventory outcome");
        }
        var now = Timestamp.from(clock.instant());
        int added = jdbc.update("""
                INSERT INTO order_schema.consumer_inbox(consumer_name,event_id,payload_hash,processed_at)
                VALUES ('order-service',?,?,?) ON CONFLICT DO NOTHING
                """, event.getEventId(), payloadHash, now);
        if (added == 0) {
            String recorded = jdbc.queryForObject("SELECT payload_hash FROM order_schema.consumer_inbox WHERE consumer_name='order-service' AND event_id=?", String.class, event.getEventId());
            if (!payloadHash.equals(recorded)) throw new EventValidationException("Event ID reused with changed content");
            return;
        }
        var orders = jdbc.queryForList("SELECT * FROM order_schema.orders WHERE id=? FOR UPDATE", outcome.orderId());
        if (orders.isEmpty()) throw new EventValidationException("Unknown order");
        var order = orders.getFirst();
        if (!outcome.productId().equals(order.get("product_id")) || outcome.quantity() != ((Number) order.get("quantity")).intValue()) {
            throw new EventValidationException("Inventory outcome does not match order");
        }
        String decisionHash = hash(event.getEventType() + ":" + outcome.orderId() + ":" + outcome.productId() + ":"
                + outcome.quantity() + ":" + outcome.reservationId() + ":" + outcome.reason() + ":" + outcome.availableQuantity());
        var history = jdbc.queryForList("SELECT decision_hash FROM order_schema.order_history WHERE order_id=? AND new_status IN ('CONFIRMED','FAILED')", outcome.orderId());
        if (!history.isEmpty()) {
            if (!decisionHash.equals(history.getFirst().get("decision_hash"))) throw new EventValidationException("Contradictory inventory outcome");
            return; // Includes a matching reservation replay after subsequent cancellation states.
        }
        if (!"PENDING".equals(order.get("status"))) throw new EventValidationException("No matching original decision for terminal order");
        long version = ((Number) order.get("version")).longValue() + 1;
        String status = reserved ? "CONFIRMED" : "FAILED";
        jdbc.update("UPDATE order_schema.orders SET status=?,version=?,reservation_id=?,failure_reason=?,updated_at=? WHERE id=?",
                status, version, outcome.reservationId(), outcome.reason(), now, outcome.orderId());
        jdbc.update("""
                INSERT INTO order_schema.order_history(id,order_id,order_version,previous_status,new_status,
                source_event_id,correlation_id,occurred_at,recorded_at,reason,decision_hash)
                VALUES (?,?,?,'PENDING',?,?,?,?,?,?,?)
                """, UUID.randomUUID(), outcome.orderId(), version, status, event.getEventId(), event.getCorrelationId(),
                Timestamp.from(event.getOccurredAt()), now, outcome.reason(), decisionHash);
        Object data = reserved
                ? new OrderConfirmedEvent(outcome.orderId(), (UUID) order.get("customer_id"), outcome.productId(), outcome.quantity())
                : new OrderFailedEvent(outcome.orderId(), (UUID) order.get("customer_id"), outcome.reason());
        String type = reserved ? "order.confirmed" : "order.failed";
        UUID eventId = UUID.randomUUID();
        String payload = EventValidator.serializeAndValidate(EventEnvelope.builder().eventId(eventId).eventType(type)
                .schemaVersion(1).occurredAt(clock.instant()).producer("order-service").aggregateId(outcome.orderId())
                .aggregateVersion(version).correlationId(event.getCorrelationId()).causationId(event.getEventId()).data(data).build());
        jdbc.update("""
                INSERT INTO order_schema.outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,payload,
                status,aggregate_version,created_at,next_attempt_at,retry_count)
                VALUES (?,?,'ORDER',?,?,?,CAST(? AS jsonb),'PENDING',?,?,?,0)
                """, eventId, outcome.orderId(), type, type, outcome.orderId().toString(), payload, version, now, now);
        if(reserved) {
            long elapsed=Math.max(0,now.getTime()-((Timestamp)order.get("created_at")).getTime());
            org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(new org.springframework.transaction.support.TransactionSynchronization(){
                public void afterCommit(){io.micrometer.core.instrument.Timer.builder("orderflow.confirmation.duration").publishPercentileHistogram().register(io.micrometer.core.instrument.Metrics.globalRegistry).record(elapsed,java.util.concurrent.TimeUnit.MILLISECONDS);}
            });
        }
    }
}
