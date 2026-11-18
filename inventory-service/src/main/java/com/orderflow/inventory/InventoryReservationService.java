package com.orderflow.inventory;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryReservationService {
    private final JdbcTemplate jdbc;
    private final Clock clock;
    public InventoryReservationService(JdbcTemplate jdbc, Clock clock) { this.jdbc = jdbc; this.clock = clock; }

    public static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, timeout = 5)
    public String reserve(EventEnvelope<OrderPlacedEvent> event, String payloadHash) {
        var data = event.getData();
        if (!"order.placed".equals(event.getEventType()) || event.getSchemaVersion() != 1 || data == null
                || data.getOrderId() == null || data.getProductId() == null || data.getCustomerId() == null
                || data.getQuantity() <= 0 || !data.getOrderId().equals(event.getAggregateId())) {
            throw new EventValidationException("Invalid order placement");
        }
        var now = Timestamp.from(clock.instant());
        int inserted = jdbc.update("""
                INSERT INTO inventory_schema.consumer_inbox(consumer_name,event_id,payload_hash,processed_at)
                VALUES ('inventory-service',?,?,?) ON CONFLICT DO NOTHING
                """, event.getEventId(), payloadHash, now);
        if (inserted == 0) {
            String recorded = jdbc.queryForObject("SELECT payload_hash FROM inventory_schema.consumer_inbox WHERE consumer_name='inventory-service' AND event_id=?", String.class, event.getEventId());
            if (!payloadHash.equals(recorded)) throw new EventValidationException("Event ID reused with changed content");
        }
        UUID orderId = data.getOrderId();
        long lock = orderId.getMostSignificantBits() ^ orderId.getLeastSignificantBits();
        jdbc.queryForList("SELECT pg_advisory_xact_lock(?)", lock);
        String requestHash = hash(orderId + ":" + data.getCustomerId() + ":" + data.getProductId() + ":" + data.getQuantity());
        var decisions = jdbc.queryForList("SELECT request_hash,outcome FROM inventory_schema.inventory_decisions WHERE order_id=?", orderId);
        if (!decisions.isEmpty()) {
            if (!requestHash.equals(decisions.getFirst().get("request_hash"))) throw new EventValidationException("Order ID reused with changed request");
            return (String) decisions.getFirst().get("outcome");
        }
        var stock = jdbc.queryForList("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?", data.getProductId());
        int available = stock.isEmpty() ? 0 : (Integer) stock.getFirst().get("quantity_available");
        boolean reserved = !stock.isEmpty() && jdbc.update("""
                UPDATE inventory_schema.inventory SET quantity_available=quantity_available-?, updated_at=?
                WHERE product_id=? AND quantity_available>=?
                """, data.getQuantity(), now, data.getProductId(), data.getQuantity()) == 1;
        String outcome = reserved ? "RESERVED" : "REJECTED";
        String reason = reserved ? null : stock.isEmpty() ? "PRODUCT_NOT_FOUND" : "INSUFFICIENT_STOCK";
        UUID reservationId = reserved ? UUID.randomUUID() : null;
        if (reserved) jdbc.update("""
                INSERT INTO inventory_schema.inventory_reservations(id,order_id,product_id,quantity,reserved_at,status)
                VALUES (?,?,?,?,?,'RESERVED')
                """, reservationId, orderId, data.getProductId(), data.getQuantity(), now);
        jdbc.update("""
                INSERT INTO inventory_schema.inventory_decisions(order_id,request_hash,product_id,quantity,outcome,
                reason,available_at_decision,reservation_id,version,decided_at) VALUES (?,?,?,?,?,?,?,?,1,?)
                """, orderId, requestHash, data.getProductId(), data.getQuantity(), outcome, reason, available, reservationId, now);
        Object result = reserved
                ? new InventoryReservedEvent(orderId, data.getProductId(), data.getQuantity(), reservationId)
                : new InventoryRejectedEvent(orderId, data.getProductId(), data.getQuantity(), reason, available);
        String type = reserved ? "inventory.reserved" : "inventory.rejected";
        UUID eventId = UUID.randomUUID();
        String payload = EventValidator.serializeAndValidate(EventEnvelope.builder().eventId(eventId).eventType(type)
                .schemaVersion(1).occurredAt(clock.instant()).producer("inventory-service").aggregateId(orderId)
                .aggregateVersion(1).correlationId(event.getCorrelationId()).causationId(event.getEventId()).data(result).build());
        jdbc.update("""
                INSERT INTO inventory_schema.outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,
                payload,status,aggregate_version,created_at,next_attempt_at,retry_count)
                VALUES (?,?,'INVENTORY',?,?,?,CAST(? AS jsonb),'PENDING',1,?,?,0)
                """, eventId, orderId, type, type, orderId.toString(), payload, now, now);
        return outcome;
    }
}
