package com.orderflow.inventory;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class InventoryReleaseService {
    private final JdbcTemplate jdbc;
    private final Clock clock;
    public InventoryReleaseService(JdbcTemplate jdbc,Clock clock) { this.jdbc=jdbc;this.clock=clock; }
    @Transactional(isolation=Isolation.READ_COMMITTED,timeout=5)
    public void release(EventEnvelope<OrderCancelRequestedEvent> event,String payloadHash) {
        var data=event.getData();
        if(!"order.cancel.requested".equals(event.getEventType()) || event.getSchemaVersion()!=1 || data==null
                || data.getOrderId()==null || !data.getOrderId().equals(event.getAggregateId()) || data.getCustomerId()==null
                || data.getReservationId()==null || data.getCancellationId()==null)
            throw new EventValidationException("Invalid cancellation request");
        var now=Timestamp.from(clock.instant());
        int inserted=jdbc.update("INSERT INTO inventory_schema.consumer_inbox(consumer_name,event_id,payload_hash,processed_at) VALUES ('inventory-service',?,?,?) ON CONFLICT DO NOTHING",event.getEventId(),payloadHash,now);
        if(inserted==0) {
            String previous=jdbc.queryForObject("SELECT payload_hash FROM inventory_schema.consumer_inbox WHERE consumer_name='inventory-service' AND event_id=?",String.class,event.getEventId());
            if(!payloadHash.equals(previous)) throw new EventValidationException("Event ID reused with changed content");
            return;
        }
        var rows=jdbc.queryForList("SELECT * FROM inventory_schema.inventory_reservations WHERE order_id=? AND id=? FOR UPDATE",data.getOrderId(),data.getReservationId());
        if(rows.isEmpty()) throw new EventValidationException("Missing matching reservation");
        var reservation=rows.getFirst();
        UUID product=(UUID)reservation.get("product_id");
        int quantity=((Number)reservation.get("quantity")).intValue();
        String expected=InventoryReservationService.hash(data.getOrderId()+":"+data.getCustomerId()+":"+product+":"+quantity);
        var decisions=jdbc.queryForList("SELECT request_hash FROM inventory_schema.inventory_decisions WHERE order_id=? AND reservation_id=? AND outcome='RESERVED'",data.getOrderId(),data.getReservationId());
        if(decisions.isEmpty() || !expected.equals(decisions.getFirst().get("request_hash"))) throw new EventValidationException("Cancellation does not match original reservation owner");
        if("RELEASED".equals(reservation.get("status"))) {
            if(!data.getCancellationId().equals(reservation.get("cancellation_id"))) throw new EventValidationException("Conflicting cancellation ID");
            return;
        }
        if(!"RESERVED".equals(reservation.get("status"))) throw new EventValidationException("Invalid reservation state");
        if(jdbc.update("UPDATE inventory_schema.inventory SET quantity_available=quantity_available+?,updated_at=? WHERE product_id=?",quantity,now,product)!=1)
            throw new EventValidationException("Missing stock for reservation");
        long version=((Number)reservation.get("version")).longValue()+1;
        jdbc.update("UPDATE inventory_schema.inventory_reservations SET status='RELEASED',cancellation_id=?,released_at=?,version=? WHERE id=?",data.getCancellationId(),now,version,data.getReservationId());
        UUID id=UUID.randomUUID();
        String payload=EventValidator.serializeAndValidate(EventEnvelope.builder().eventId(id).eventType("inventory.released").schemaVersion(1)
                .occurredAt(now.toInstant()).producer("inventory-service").aggregateId(data.getOrderId()).aggregateVersion(version)
                .correlationId(event.getCorrelationId()).causationId(event.getEventId())
                .data(new InventoryReleasedEvent(data.getOrderId(),data.getReservationId(),data.getCancellationId(),quantity)).build());
        jdbc.update("""
            INSERT INTO inventory_schema.outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,payload,status,aggregate_version,created_at,next_attempt_at,retry_count)
            VALUES (?,?,'INVENTORY','inventory.released','inventory.released',?,CAST(? AS jsonb),'PENDING',?,?,?,0)
            """,id,data.getOrderId(),data.getOrderId().toString(),payload,version,now,now);
    }
}
