package com.orderflow.order;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import com.orderflow.order.dto.*;
import com.orderflow.order.service.OrderService;
import com.orderflow.order.kafka.InventoryOutcomeConsumer;
import com.orderflow.order.outbox.OutboxRelay;
import com.orderflow.notification.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","order.relay.enabled=false","spring.kafka.listener.auto-startup=false",
        "spring.flyway.locations=filesystem:src/main/resources/db/migration","spring.jpa.show-sql=false"})
@AutoConfigureMockMvc
@Import({NotificationService.class, OrderLifecycleConsumer.class, NotificationKafkaConfig.class})
class Phase4CIT {
    @Container static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15.6");
    @Container static KafkaContainer broker = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @DynamicPropertySource static void properties(DynamicPropertyRegistry properties) {
        Flyway.configure().dataSource(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword())
                .schemas("notification_schema").defaultSchema("notification_schema")
                .locations("filesystem:../notification-service/src/main/resources/db/migration").load().migrate();
        properties.add("spring.datasource.url",postgres::getJdbcUrl);
        properties.add("spring.datasource.username",postgres::getUsername);
        properties.add("spring.datasource.password",postgres::getPassword);
        properties.add("spring.kafka.bootstrap-servers",broker::getBootstrapServers);
    }
    @Autowired OrderService orders;
    @Autowired InventoryOutcomeConsumer outcomes;
    @Autowired OrderLifecycleConsumer notificationConsumer;
    @Autowired NotificationService notifications;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired PlatformTransactionManager transactions;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired MockMvc http;
    @org.springframework.test.context.bean.override.mockito.MockitoBean com.orderflow.security.IdentityVerifier identity;

    @BeforeAll static void topics() throws Exception {
        var topics = new ArrayList<String>(List.of("inventory.reserved","inventory.rejected","order.placed","order.confirmed","order.failed","order.cancelled"));
        for (var topic : List.copyOf(topics)) topics.add(topic + (topic.startsWith("inventory") ? ".order-service.DLT" : ".notification-service.DLT"));
        try (var admin = Admin.create(Map.of("bootstrap.servers",broker.getBootstrapServers()))) {
            admin.createTopics(topics.stream().map(name -> new NewTopic(name,3,(short)1)).toList()).all().get(20,TimeUnit.SECONDS);
        }
    }
    @BeforeEach void reset() {
        org.mockito.Mockito.when(identity.verify(org.mockito.ArgumentMatchers.any())).thenReturn(new com.orderflow.security.Caller(UUID.randomUUID(),Set.of("ADMIN"),"test",Instant.now().plusSeconds(1800)));
        jdbc.execute("TRUNCATE order_schema.orders,order_schema.order_history,order_schema.outbox_events,order_schema.consumer_inbox,notification_schema.notifications,notification_schema.consumer_inbox CASCADE");
    }
    OrderResponse create() {
        var request = new OrderRequest(); request.setCustomerId(UUID.randomUUID()); request.setProductId(UUID.randomUUID()); request.setQuantity(2);
        return orders.createOrder(request);
    }
    EventEnvelope<?> outcome(OrderResponse order, boolean reserved, String reason) {
        Object data = reserved ? new InventoryReservedEvent(order.getId(),order.getProductId(),order.getQuantity(),UUID.randomUUID())
                : new InventoryRejectedEvent(order.getId(),order.getProductId(),order.getQuantity(),reason,0);
        return EventEnvelope.builder().eventId(UUID.randomUUID()).eventType(reserved ? "inventory.reserved" : "inventory.rejected")
                .schemaVersion(1).occurredAt(Instant.now()).producer("inventory-service").aggregateId(order.getId()).aggregateVersion(1)
                .correlationId(UUID.randomUUID()).causationId(UUID.randomUUID()).data(data).build();
    }
    void deliver(EventEnvelope<?> event) {
        outcomes.receive(new ConsumerRecord<>(event.getEventType(),0,0,event.getAggregateId().toString(),EventValidator.serializeAndValidate(event)));
    }
    String outgoing(UUID orderId, String type) {
        return jdbc.queryForObject("SELECT payload::text FROM order_schema.outbox_events WHERE aggregate_id=? AND event_type=?",String.class,orderId,type);
    }
    void notify(UUID orderId,String type) {
        notificationConsumer.receive(new ConsumerRecord<>(type,0,0,orderId.toString(),outgoing(orderId,type)));
    }
    int count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table,Integer.class); }

    @Test void happyPathPersistsVersionTimelineAndNotifications() throws Exception {
        var order=create(); var event=outcome(order,true,null); deliver(event);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE id<>(payload->>'eventId')::uuid",Integer.class)).isZero();
        notify(order.getId(),"order.placed"); notify(order.getId(),"order.confirmed");
        var result=orders.findById(order.getId()).orElseThrow();
        assertThat(result.getStatus()).isEqualTo("CONFIRMED");
        assertThat(result.getVersion()).isEqualTo(2);
        assertThat(result.getTimeline()).extracting(OrderTimelineEntry::status).containsExactly("PENDING","CONFIRMED");
        assertThat(notifications.timeline(order.getId())).extracting(NotificationService.Notification::eventType).containsExactly("ORDER_RECEIVED","ORDER_CONFIRMED");
        var emitted=EventValidator.deserializeAndValidate(outgoing(order.getId(),"order.confirmed"),OrderConfirmedEvent.class);
        assertThat(emitted.getCausationId()).isEqualTo(event.getEventId());
        assertThat(emitted.getCorrelationId()).isEqualTo(event.getCorrelationId());
        assertThat(jdbc.queryForObject("SELECT reservation_id FROM order_schema.orders WHERE id=?",UUID.class,order.getId()))
                .isEqualTo(((InventoryReservedEvent)event.getData()).getReservationId());
        http.perform(get("/api/orders/"+order.getId())).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CONFIRMED")).andExpect(jsonPath("$.version").value(2))
                .andExpect(jsonPath("$.timeline[1].status").value("CONFIRMED"));
        http.perform(get("/api/orders/"+UUID.randomUUID())).andExpect(status().isNotFound());
    }
    @Test void insufficientStockAndMissingProductRemainFailed() {
        for (String reason:List.of("INSUFFICIENT_STOCK","PRODUCT_NOT_FOUND")) {
            var order=create(); deliver(outcome(order,false,reason));
            notify(order.getId(),"order.failed"); notify(order.getId(),"order.placed");
            var result=orders.findById(order.getId()).orElseThrow();
            assertThat(result.getStatus()).isEqualTo("FAILED"); assertThat(result.getFailureReason()).isEqualTo(reason);
            assertThat(result.getTimeline().getLast().reason()).isEqualTo(reason);
            assertThat(notifications.timeline(order.getId())).extracting(NotificationService.Notification::eventType).containsExactly("ORDER_RECEIVED","ORDER_FAILED");
            assertThat(notifications.latest(order.getId()).orElseThrow().message()).contains(reason);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE aggregate_id=? AND event_type='order.confirmed'",Integer.class,order.getId())).isZero();
        }
    }
    @Test void duplicateAndStaleDeliveriesPreserveHistoryAndTerminalState() {
        var order=create(); var event=outcome(order,true,null);
        for (int i=0;i<11;i++) deliver(event);
        event.setEventId(UUID.randomUUID()); deliver(event);
        notify(order.getId(),"order.confirmed"); notify(order.getId(),"order.placed");
        for (int i=0;i<10;i++) { notify(order.getId(),"order.placed"); notify(order.getId(),"order.confirmed"); }
        assertThat(count("order_schema.order_history")).isEqualTo(2);
        assertThat(count("order_schema.outbox_events")).isEqualTo(2);
        assertThat(count("notification_schema.notifications")).isEqualTo(2);
        assertThat(notifications.latest(order.getId()).orElseThrow().eventType()).isEqualTo("ORDER_CONFIRMED");
        // Future cancellation state is a fixture, not cancellation implementation.
        jdbc.update("UPDATE order_schema.orders SET status='CANCELLED',version=4 WHERE id=?",order.getId());
        event.setEventId(UUID.randomUUID()); deliver(event);
        assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo("CANCELLED");
        assertThat(count("order_schema.order_history")).isEqualTo(2);
    }
    @Test void contradictoryUnknownAndMismatchedOutcomesAreNotAcknowledged() {
        var order=create(); var rejected=outcome(order,false,"INSUFFICIENT_STOCK"); deliver(rejected);
        assertThatThrownBy(()->deliver(outcome(order,true,null))).isInstanceOf(EventValidationException.class);
        var unknown=create(); jdbc.update("DELETE FROM order_schema.order_history WHERE order_id=?",unknown.getId());
        jdbc.update("DELETE FROM order_schema.orders WHERE id=?",unknown.getId());
        assertThatThrownBy(()->deliver(outcome(unknown,true,null))).isInstanceOf(EventValidationException.class);
        var mismatch=outcome(order,false,"INSUFFICIENT_STOCK"); ((InventoryRejectedEvent)mismatch.getData()).setProductId(UUID.randomUUID());
        assertThatThrownBy(()->deliver(mismatch)).isInstanceOf(EventValidationException.class);
        rejected.setCorrelationId(UUID.randomUUID());
        assertThatThrownBy(()->deliver(rejected)).isInstanceOf(EventValidationException.class);
        assertThat(count("order_schema.consumer_inbox")).isEqualTo(1);
        assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo("FAILED");
    }
    @Test void concurrentOutcomesCreateOneTransition() throws Exception {
        var order=create(); var event=outcome(order,true,null);
        try(var executor=Executors.newFixedThreadPool(10)) {
            List<Callable<Void>> work=new ArrayList<>();
            for(int i=0;i<10;i++) {
                var copy=EventValidator.deserializeAndValidate(EventValidator.serializeAndValidate(event),InventoryReservedEvent.class);
                copy.setEventId(UUID.randomUUID()); work.add(()->{deliver(copy);return null;});
            }
            for(var result:executor.invokeAll(work)) result.get(20,TimeUnit.SECONDS);
        }
        assertThat(orders.findById(order.getId()).orElseThrow().getVersion()).isEqualTo(2);
        assertThat(count("order_schema.order_history")).isEqualTo(2);
        assertThat(count("order_schema.outbox_events")).isEqualTo(2);
    }
    @Test void outcomeFailureRollsBackStateInboxHistoryAndOutbox() {
        var order=create();
        jdbc.execute("ALTER TABLE order_schema.outbox_events ADD CONSTRAINT test_fail CHECK (event_type='order.placed')");
        try { assertThatThrownBy(()->deliver(outcome(order,true,null))).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class); }
        finally { jdbc.execute("ALTER TABLE order_schema.outbox_events DROP CONSTRAINT test_fail"); }
        assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo("PENDING");
        assertThat(count("order_schema.consumer_inbox")).isZero();
        assertThat(count("order_schema.order_history")).isEqualTo(1);
        assertThat(count("order_schema.outbox_events")).isEqualTo(1);
    }
    @Test void notificationFailureRollsBackInboxAndConflictingDuplicateIsRejected() {
        var order=create();
        jdbc.execute("ALTER TABLE notification_schema.notifications ADD CONSTRAINT test_fail CHECK (false)");
        try { assertThatThrownBy(()->notify(order.getId(),"order.placed")).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class); }
        finally { jdbc.execute("ALTER TABLE notification_schema.notifications DROP CONSTRAINT test_fail"); }
        assertThat(count("notification_schema.consumer_inbox")).isZero();
        notify(order.getId(),"order.placed");
        var duplicate=EventValidator.deserializeAndValidate(outgoing(order.getId(),"order.placed"),OrderPlacedEvent.class);
        duplicate.setEventId(UUID.randomUUID());
        notificationConsumer.receive(new ConsumerRecord<>("order.placed",0,0,order.getId().toString(),EventValidator.serializeAndValidate(duplicate)));
        duplicate.setEventId(UUID.randomUUID()); duplicate.getData().setQuantity(99);
        assertThatThrownBy(()->notificationConsumer.receive(new ConsumerRecord<>("order.placed",0,0,order.getId().toString(),EventValidator.serializeAndValidate(duplicate)))).isInstanceOf(EventValidationException.class);
        assertThat(count("notification_schema.notifications")).isEqualTo(1);
        assertThat(count("notification_schema.consumer_inbox")).isEqualTo(2);
    }
    @Test void cancelledNotificationCanArriveBeforeEarlierLifecycle() {
        var order=create();
        var cancelled=EventEnvelope.builder().eventId(UUID.randomUUID()).eventType("order.cancelled").schemaVersion(1)
                .occurredAt(Instant.now()).producer("order-service").aggregateId(order.getId()).aggregateVersion(4)
                .correlationId(UUID.randomUUID()).causationId(UUID.randomUUID())
                .data(new OrderCancelledEvent(order.getId(),order.getCustomerId(),UUID.randomUUID())).build();
        notificationConsumer.receive(new ConsumerRecord<>("order.cancelled",0,0,order.getId().toString(),EventValidator.serializeAndValidate(cancelled)));
        notify(order.getId(),"order.placed");
        assertThat(notifications.timeline(order.getId())).extracting(NotificationService.Notification::version).containsExactly(1L,4L);
        assertThat(notifications.latest(order.getId()).orElseThrow().eventType()).isEqualTo("ORDER_CANCELLED");
        assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo("PENDING");
    }
    @Test void realKafkaOutcomesRelayAndNotificationsCompleteAllPaths() throws Exception {
        registry.start();
        var relay=new OutboxRelay(dataSource,jdbc,kafka,transactions,Clock.systemUTC());
        try {
            for(String reason:List.of("RESERVED","INSUFFICIENT_STOCK","PRODUCT_NOT_FOUND")) {
                var order=create(); var event=outcome(order,reason.equals("RESERVED"),reason.equals("RESERVED")?null:reason);
                relay.publishPending();
                kafka.send(event.getEventType(),order.getId().toString(),EventValidator.serializeAndValidate(event)).get(10,TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(30)).until(()->orders.findById(order.getId()).orElseThrow().getVersion()==2);
                relay.publishPending();
                await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertThat(notifications.timeline(order.getId())).hasSize(2));
                assertThat(notifications.latest(order.getId()).orElseThrow().eventType()).isEqualTo(reason.equals("RESERVED")?"ORDER_CONFIRMED":"ORDER_FAILED");
                assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(reason.equals("RESERVED")?"CONFIRMED":"FAILED");
            }
        } finally { registry.stop(); }
    }
}

