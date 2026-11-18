package com.orderflow.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import com.orderflow.order.service.IdempotencyService.RequestFailure;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class OrderCancellationService {
    private final JdbcTemplate jdbc;
    private final IdempotencyService keys;
    private final ObjectMapper json;
    private final Clock clock;
    public OrderCancellationService(JdbcTemplate jdbc,IdempotencyService keys,ObjectMapper json,Clock clock) {
        this.jdbc=jdbc;this.keys=keys;this.json=json;this.clock=clock;
    }
    public record Cancellation(UUID id,String status,long version,UUID cancellationId) {}

    public IdempotencyService.Result cancel(UUID customer,UUID orderId,String key,UUID correlation) {
        String hash=OrderOutcomeService.hash("CANCEL_ORDER\n"+customer+"\n"+orderId);
        // The idempotency transaction encloses the lock, state/history/outbox and saved HTTP response.
        return keys.execute(customer,IdempotencyService.Operation.CANCEL_ORDER,key,hash,()->{
            var rows=jdbc.queryForList("SELECT * FROM order_schema.orders WHERE id=? AND customer_id=? FOR UPDATE",orderId,customer);
            if(rows.isEmpty()) throw new RequestFailure(404,"ORDER_NOT_FOUND");
            var order=rows.getFirst();
            if(!"CONFIRMED".equals(order.get("status"))) throw new RequestFailure(409,"INVALID_ORDER_STATE");
            UUID reservation=(UUID)order.get("reservation_id");
            if(reservation==null) throw new IllegalStateException("Confirmed order has no reservation");
            UUID cancellation=UUID.randomUUID(),requestId=UUID.randomUUID();
            long version=((Number)order.get("version")).longValue()+1;
            var now=Timestamp.from(clock.instant());
            jdbc.update("UPDATE order_schema.orders SET status='CANCEL_PENDING',cancellation_id=?,version=?,updated_at=? WHERE id=?",cancellation,version,now,orderId);
            history(orderId,version,"CONFIRMED","CANCEL_PENDING",requestId,correlation,now,now);
            publish(orderId,version,"order.cancel.requested",new OrderCancelRequestedEvent(orderId,customer,cancellation,reservation),correlation,requestId,now);
            try { return new IdempotencyService.Result(202,json.writeValueAsString(new Cancellation(orderId,"CANCEL_PENDING",version,cancellation)),orderId,false); }
            catch(JsonProcessingException e) { throw new IllegalStateException(e); }
        });
    }

    @Transactional(isolation=Isolation.READ_COMMITTED,timeout=5)
    public void complete(EventEnvelope<InventoryReleasedEvent> event,String payloadHash) {
        var data=event.getData();
        if(!"inventory.released".equals(event.getEventType()) || event.getSchemaVersion()!=1 || data==null
                || data.getOrderId()==null || !data.getOrderId().equals(event.getAggregateId())
                || data.getReservationId()==null || data.getCancellationId()==null || data.getQuantity()<1)
            throw new EventValidationException("Invalid release event");
        var now=Timestamp.from(clock.instant());
        int inserted=jdbc.update("INSERT INTO order_schema.consumer_inbox(consumer_name,event_id,payload_hash,processed_at) VALUES ('order-service',?,?,?) ON CONFLICT DO NOTHING",event.getEventId(),payloadHash,now);
        if(inserted==0) {
            String previous=jdbc.queryForObject("SELECT payload_hash FROM order_schema.consumer_inbox WHERE consumer_name='order-service' AND event_id=?",String.class,event.getEventId());
            if(!payloadHash.equals(previous)) throw new EventValidationException("Event ID reused with changed content");
            return;
        }
        var rows=jdbc.queryForList("SELECT * FROM order_schema.orders WHERE id=? FOR UPDATE",data.getOrderId());
        if(rows.isEmpty()) throw new EventValidationException("Unknown order for release");
        var order=rows.getFirst();
        if(!data.getReservationId().equals(order.get("reservation_id")) || !data.getCancellationId().equals(order.get("cancellation_id"))
                || data.getQuantity()!=((Number)order.get("quantity")).intValue())
            throw new EventValidationException("Release does not match cancellation");
        if("CANCELLED".equals(order.get("status"))) return;
        if(!"CANCEL_PENDING".equals(order.get("status"))) throw new EventValidationException("Order is not awaiting release");
        long version=((Number)order.get("version")).longValue()+1;
        jdbc.update("UPDATE order_schema.orders SET status='CANCELLED',version=?,updated_at=? WHERE id=?",version,now,data.getOrderId());
        history(data.getOrderId(),version,"CANCEL_PENDING","CANCELLED",event.getEventId(),event.getCorrelationId(),Timestamp.from(event.getOccurredAt()),now);
        publish(data.getOrderId(),version,"order.cancelled",new OrderCancelledEvent(data.getOrderId(),(UUID)order.get("customer_id"),data.getCancellationId()),event.getCorrelationId(),event.getEventId(),now);
    }
    private void history(UUID order,long version,String previous,String status,UUID source,UUID correlation,Timestamp occurred,Timestamp recorded) {
        jdbc.update("""
            INSERT INTO order_schema.order_history(id,order_id,order_version,previous_status,new_status,source_event_id,correlation_id,occurred_at,recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,UUID.randomUUID(),order,version,previous,status,source,correlation,occurred,recorded);
    }
    private void publish(UUID order,long version,String type,Object data,UUID correlation,UUID cause,Timestamp now) {
        UUID id=UUID.randomUUID();
        String payload=EventValidator.serializeAndValidate(EventEnvelope.builder().eventId(id).eventType(type).schemaVersion(1)
                .occurredAt(now.toInstant()).producer("order-service").aggregateId(order).aggregateVersion(version)
                .correlationId(correlation).causationId(cause).data(data).build());
        jdbc.update("""
            INSERT INTO order_schema.outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,payload,status,aggregate_version,created_at,next_attempt_at,retry_count)
            VALUES (?,?,'ORDER',?,?,?,CAST(? AS jsonb),'PENDING',?,?,?,0)
            """,id,order,type,type,order.toString(),payload,version,now,now);
    }
}
