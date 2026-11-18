package com.orderflow.order.service;

import com.orderflow.contracts.envelope.EventEnvelope;
import com.orderflow.contracts.envelope.EventValidator;
import com.orderflow.contracts.events.OrderPlacedEvent;
import com.orderflow.order.config.CorrelationIdFilter;
import com.orderflow.order.dto.OrderRequest;
import com.orderflow.order.dto.OrderResponse;
import com.orderflow.order.model.Order;
import com.orderflow.order.model.OutboxEvent;
import com.orderflow.order.repository.OrderRepository;
import com.orderflow.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final org.springframework.jdbc.core.JdbcTemplate jdbc;
    private final java.time.Clock clock;

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // Write 1: save the order
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .status(Order.OrderStatus.PENDING)
                .version(1L)
                .createdAt(clock.instant())
                .updatedAt(clock.instant())
                .build();
        order = orderRepository.saveAndFlush(order);

        UUID orderId = order.getId();
        String mdcCorrelation = MDC.get(CorrelationIdFilter.MDC_KEY);
        UUID correlationId;
        try {
            correlationId = (mdcCorrelation != null && !mdcCorrelation.isBlank())
                    ? UUID.fromString(mdcCorrelation)
                    : UUID.randomUUID();
        } catch (IllegalArgumentException e) {
            correlationId = UUID.nameUUIDFromBytes(mdcCorrelation.getBytes());
        }

        UUID eventId = UUID.randomUUID();
        UUID causationId = UUID.randomUUID(); // Request/action identifier

        OrderPlacedEvent eventData = OrderPlacedEvent.builder()
                .orderId(orderId)
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .build();

        EventEnvelope<OrderPlacedEvent> envelope = EventEnvelope.<OrderPlacedEvent>builder()
                .eventId(eventId)
                .eventType(OrderPlacedEvent.EVENT_TYPE)
                .schemaVersion(1)
                .occurredAt(order.getCreatedAt())
                .producer("order-service")
                .aggregateId(orderId)
                .aggregateVersion(1L)
                .correlationId(correlationId)
                .causationId(causationId)
                .data(eventData)
                .build();

        String payload = EventValidator.serializeAndValidate(envelope);

        // Write 2: save outbox entry in same local transaction
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(eventId)
                .aggregateId(orderId)
                .aggregateType("ORDER")
                .eventType(OrderPlacedEvent.EVENT_TYPE)
                .topic("order.placed")
                .eventKey(orderId.toString())
                .aggregateVersion(1L)
                .payload(payload)
                .status(OutboxEvent.OutboxStatus.PENDING)
                .createdAt(clock.instant())
                .nextAttemptAt(clock.instant())
                .build();
        outboxEventRepository.save(outboxEvent);
        jdbc.update("""
                INSERT INTO order_schema.order_history(id,order_id,order_version,previous_status,new_status,
                source_event_id,correlation_id,occurred_at,recorded_at) VALUES (?,?,1,NULL,'PENDING',?,?,?,?)
                """, UUID.randomUUID(), orderId, causationId, correlationId,
                java.sql.Timestamp.from(order.getCreatedAt()), java.sql.Timestamp.from(clock.instant()));

        try(var scope=com.orderflow.telemetry.TraceContext.open(java.util.Map.of("orderId",orderId.toString(),"eventId",eventId.toString(),"operation","order.create","outcome","PREPARED"))) {
            log.info("Order created: {} with outbox event: {} correlationId: {}",order.getId(),eventId,correlationId);
        }

        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true, isolation = org.springframework.transaction.annotation.Isolation.REPEATABLE_READ)
    public Optional<OrderResponse> findById(UUID id) {
        return details(orderRepository.findById(id));
    }

    @Transactional(readOnly=true,isolation=org.springframework.transaction.annotation.Isolation.REPEATABLE_READ)
    public Optional<OrderResponse> findVisible(UUID id,com.orderflow.security.Caller caller) {
        return details(caller.admin()?orderRepository.findById(id):orderRepository.findByIdAndCustomerId(id,caller.userId()));
    }

    @Transactional(readOnly=true)
    public java.util.List<OrderResponse> list(com.orderflow.security.Caller caller,int page,int size) {
        return list(caller,page,size,null,null);
    }

    @Transactional(readOnly=true)
    public java.util.List<OrderResponse> list(com.orderflow.security.Caller caller,int page,int size,UUID id,Order.OrderStatus status) {
        com.orderflow.security.ApiErrors.page(page,size);
        var pageable=org.springframework.data.domain.PageRequest.of(page,size,
                org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC,"createdAt","id"));
        var orders=orderRepository.findAll((root,query,builder)->{
            var filters=new java.util.ArrayList<jakarta.persistence.criteria.Predicate>();
            if(!caller.admin())filters.add(builder.equal(root.get("customerId"),caller.userId()));
            if(id!=null)filters.add(builder.equal(root.get("id"),id));
            if(status!=null)filters.add(builder.equal(root.get("status"),status));
            return builder.and(filters.toArray(jakarta.persistence.criteria.Predicate[]::new));
        },pageable);
        return orders.stream().map(OrderResponse::from).toList();
    }

    private Optional<OrderResponse> details(Optional<Order> found) {
        return found
                .map(order -> {
                    var response = OrderResponse.from(order);
                    response.setTimeline(jdbc.query("SELECT * FROM order_schema.order_history WHERE order_id=? ORDER BY order_version",
                            (row, index) -> new com.orderflow.order.dto.OrderTimelineEntry(row.getLong("order_version"),
                                    row.getString("previous_status"), row.getString("new_status"),
                                    row.getObject("source_event_id", UUID.class), row.getObject("correlation_id", UUID.class),
                                    row.getTimestamp("occurred_at").toInstant(), row.getTimestamp("recorded_at").toInstant(),
                                    row.getString("reason")), order.getId()));
                    return response;
                });
    }
}
