package com.orderflow.order;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import com.orderflow.inventory.*;
import com.orderflow.notification.*;
import com.orderflow.order.dto.*;
import com.orderflow.order.kafka.*;
import com.orderflow.order.outbox.OutboxRelay;
import com.orderflow.order.service.*;
import com.orderflow.security.*;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","order.relay.enabled=false","inventory.relay.enabled=false","spring.kafka.listener.auto-startup=false",
        "spring.kafka.listener.ack-mode=record","spring.kafka.listener.concurrency=3",
        "spring.flyway.locations=filesystem:src/main/resources/db/migration","spring.jpa.show-sql=false"})
@AutoConfigureMockMvc
@Import({InventoryReservationService.class,InventoryReleaseService.class,InventoryListener.class,InventoryCancellationListener.class,
        InventoryKafkaConfig.class,NotificationService.class,OrderLifecycleConsumer.class,NotificationKafkaConfig.class})
class Phase7IT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static KafkaContainer broker=new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        for(String service:List.of("inventory","notification")) Flyway.configure().dataSource(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword())
                .schemas(service+"_schema").defaultSchema(service+"_schema").locations("filesystem:../"+service+"-service/src/main/resources/db/migration").load().migrate();
        p.add("spring.datasource.url",postgres::getJdbcUrl);p.add("spring.datasource.username",postgres::getUsername);p.add("spring.datasource.password",postgres::getPassword);
        p.add("spring.kafka.bootstrap-servers",broker::getBootstrapServers);
    }
    @Autowired OrderService orders;
    @Autowired OrderCancellationService cancellations;
    @Autowired InventoryOutcomeConsumer outcomes;
    @Autowired InventoryReleaseConsumer completions;
    @Autowired InventoryReservationService reservations;
    @Autowired InventoryReleaseService releases;
    @Autowired OrderLifecycleConsumer notifications;
    @Autowired NotificationService notificationService;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc http;
    @Autowired DataSource dataSource;
    @Autowired PlatformTransactionManager transactions;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @MockitoBean IdentityVerifier identity;
    UUID owner,product;
    @BeforeAll static void topics() throws Exception {
        var topics=new ArrayList<String>(List.of("order.placed","order.confirmed","order.failed","order.cancel.requested","order.cancelled","inventory.reserved","inventory.rejected","inventory.released"));
        for(String topic:List.copyOf(topics)) topics.add(topic+(topic.startsWith("inventory.")?".order-service.DLT":
                topic.equals("order.placed") || topic.equals("order.cancel.requested")?".inventory-service.DLT":".notification-service.DLT"));
        topics.add("order.placed.notification-service.DLT");
        try(var admin=Admin.create(Map.of("bootstrap.servers",broker.getBootstrapServers()))) {
            admin.createTopics(topics.stream().map(t->new NewTopic(t,3,(short)1)).toList()).all().get(20,TimeUnit.SECONDS);
        }
    }
    @BeforeEach void seed() {
        jdbc.execute("TRUNCATE order_schema.orders,order_schema.order_history,order_schema.outbox_events,order_schema.consumer_inbox,order_schema.request_idempotency,inventory_schema.inventory,inventory_schema.inventory_reservations,inventory_schema.inventory_decisions,inventory_schema.outbox_events,inventory_schema.consumer_inbox,notification_schema.notifications,notification_schema.consumer_inbox CASCADE");
        owner=UUID.randomUUID();product=UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES (?,'Phase7',50)",product);
        when(identity.verify(any())).thenReturn(new Caller(owner,Set.of("CUSTOMER"),"test",Instant.now().plusSeconds(1800)));
    }
    OrderResponse create() {
        var request=new OrderRequest();request.setCustomerId(owner);request.setProductId(product);request.setQuantity(2);
        return orders.createOrder(request);
    }
    OrderResponse confirmed() {
        var order=create();String placed=payload("order",order.getId(),"order.placed");
        reservations.reserve(EventValidator.deserializeAndValidate(placed,OrderPlacedEvent.class),OrderOutcomeService.hash(placed));
        String reserved=payload("inventory",order.getId(),"inventory.reserved");
        outcomes.receive(record("inventory.reserved",order.getId(),reserved));
        return orders.findById(order.getId()).orElseThrow();
    }
    String payload(String schema,UUID order,String type) {
        return jdbc.queryForObject("SELECT payload::text FROM "+schema+"_schema.outbox_events WHERE aggregate_id=? AND event_type=?",String.class,order,type);
    }
    ConsumerRecord<String,String> record(String topic,UUID order,String payload) { return new ConsumerRecord<>(topic,0,0,order.toString(),payload); }
    EventEnvelope<OrderCancelRequestedEvent> cancellation(UUID order) { return EventValidator.deserializeAndValidate(payload("order",order,"order.cancel.requested"),OrderCancelRequestedEvent.class); }
    void release(EventEnvelope<OrderCancelRequestedEvent> event) { String body=EventValidator.serializeAndValidate(event);releases.release(event,OrderOutcomeService.hash(body)); }
    void complete(UUID order) { completions.receive(record("inventory.released",order,payload("inventory",order,"inventory.released"))); }
    int stock() { return jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?",Integer.class,product); }
    int count(String schema,String type) { return jdbc.queryForObject("SELECT count(*) FROM "+schema+"_schema.outbox_events WHERE event_type=?",Integer.class,type); }
    int cancelledNotifications() { return jdbc.queryForObject("SELECT count(*) FROM notification_schema.notifications WHERE event_type='ORDER_CANCELLED'",Integer.class); }
    String status(UUID order) { return jdbc.queryForObject("SELECT status FROM order_schema.orders WHERE id=?",String.class,order); }
    MockHttpServletResponse cancel(UUID id,String key) throws Exception {
        var request=post("/api/orders/"+id+"/cancel").header("Authorization","Bearer test");
        if(key!=null) request.header("Idempotency-Key",key);
        return http.perform(request).andReturn().getResponse();
    }
    @Test void fullLifecycleTenDuplicatesAndLateReservationsRestoreOnlyOnce() throws Exception {
        var order=confirmed();assertThat(stock()).isEqualTo(48);
        var first=cancel(order.getId(),"cancel");assertThat(first.getStatus()).isEqualTo(202);
        assertThat(status(order.getId())).isEqualTo("CANCEL_PENDING");assertThat(cancelledNotifications()).isZero();
        var event=cancellation(order.getId());
        for(int i=0;i<10;i++) { release(event);release(event);event.setEventId(UUID.randomUUID()); }
        assertThat(stock()).isEqualTo(50);assertThat(count("inventory","inventory.released")).isEqualTo(1);
        String released=payload("inventory",order.getId(),"inventory.released");
        var releaseEvent=EventValidator.deserializeAndValidate(released,InventoryReleasedEvent.class);
        for(int i=0;i<10;i++) {
            completions.receive(record("inventory.released",order.getId(),EventValidator.serializeAndValidate(releaseEvent)));
            completions.receive(record("inventory.released",order.getId(),EventValidator.serializeAndValidate(releaseEvent)));
            releaseEvent.setEventId(UUID.randomUUID());
        }
        assertThat(status(order.getId())).isEqualTo("CANCELLED");assertThat(count("order","order.cancelled")).isEqualTo(1);
        assertThat(orders.findById(order.getId()).orElseThrow().getVersion()).isEqualTo(4);
        assertThat(releaseEvent.getCorrelationId().toString()).isEqualTo(first.getHeader("X-Correlation-Id"));
        String cancelled=payload("order",order.getId(),"order.cancelled");
        for(int i=0;i<10;i++) notifications.receive(record("order.cancelled",order.getId(),cancelled));
        assertThat(cancelledNotifications()).isEqualTo(1);
        assertThat(orders.findById(order.getId()).orElseThrow().getTimeline()).extracting(OrderTimelineEntry::status)
                .containsExactly("PENDING","CONFIRMED","CANCEL_PENDING","CANCELLED");
        assertThat(jdbc.queryForObject("SELECT version FROM inventory_schema.inventory_reservations WHERE order_id=?",Long.class,order.getId())).isEqualTo(2);
        var replay=cancel(order.getId(),"cancel");assertThat(replay.getStatus()).isEqualTo(202);
        assertThat(replay.getHeader("Idempotent-Replayed")).isEqualTo("true");assertThat(replay.getContentAsString()).isEqualTo(first.getContentAsString());
        assertThat(cancel(order.getId(),"new-key").getStatus()).isEqualTo(409);
        String placed=payload("order",order.getId(),"order.placed");var late=EventValidator.deserializeAndValidate(placed,OrderPlacedEvent.class);late.setEventId(UUID.randomUUID());
        reservations.reserve(late,OrderOutcomeService.hash(EventValidator.serializeAndValidate(late)));
        var lateReserved=EventValidator.deserializeAndValidate(payload("inventory",order.getId(),"inventory.reserved"),InventoryReservedEvent.class);
        lateReserved.setEventId(UUID.randomUUID());
        outcomes.receive(record("inventory.reserved",order.getId(),EventValidator.serializeAndValidate(lateReserved)));
        assertThat(stock()).isEqualTo(50);assertThat(status(order.getId())).isEqualTo("CANCELLED");
    }
    @Test void concurrentDifferentKeysConflictAndSameKeyReplays() throws Exception {
        for(boolean same:List.of(false,true)) {
            var order=confirmed();var start=new CountDownLatch(1);
            try(var pool=Executors.newFixedThreadPool(2)) {
                var a=pool.submit(()->{start.await();return cancel(order.getId(),"a-"+order.getId());});
                var b=pool.submit(()->{start.await();return cancel(order.getId(),(same?"a-":"b-")+order.getId());});start.countDown();
                var first=a.get(15,TimeUnit.SECONDS);var second=b.get(15,TimeUnit.SECONDS);
                assertThat(List.of(first.getStatus(),second.getStatus())).containsExactlyInAnyOrder(202,same?202:409);
                if(same) { assertThat(first.getContentAsString()).isEqualTo(second.getContentAsString());
                    assertThat(Arrays.asList(first.getHeader("Idempotent-Replayed"),second.getHeader("Idempotent-Replayed"))).containsExactlyInAnyOrder(null,"true"); }
            }
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE aggregate_id=? AND event_type='order.cancel.requested'",Integer.class,order.getId())).isEqualTo(1);
        }
    }
    @Test void invalidStateKeyOwnershipAndCrossOrderReuseFailWithoutMutation() throws Exception {
        var pending=create();assertThat(cancel(pending.getId(),null).getStatus()).isEqualTo(400);
        assertThat(cancel(pending.getId(),"pending").getStatus()).isEqualTo(409);
        jdbc.update("UPDATE order_schema.orders SET status='FAILED' WHERE id=?",pending.getId());
        assertThat(cancel(pending.getId(),"failed").getStatus()).isEqualTo(409);
        var order=confirmed();when(identity.verify(any())).thenReturn(new Caller(UUID.randomUUID(),Set.of("ADMIN"),"test",Instant.now().plusSeconds(1800)));
        assertThat(cancel(order.getId(),"not-owner").getStatus()).isEqualTo(404);
        when(identity.verify(any())).thenReturn(new Caller(owner,Set.of("CUSTOMER"),"test",Instant.now().plusSeconds(1800)));
        assertThat(cancel(order.getId(),"scope").getStatus()).isEqualTo(202);
        assertThat(cancel(pending.getId(),"scope").getStatus()).isEqualTo(409);
        assertThat(count("order","order.cancel.requested")).isEqualTo(1);
    }
    @Test void contradictoryAndMissingReservationsAndReleasesAreProtocolErrors() throws Exception {
        var order=confirmed();cancel(order.getId(),"cancel");var event=cancellation(order.getId());
        UUID correct=event.getData().getReservationId();event.getData().setReservationId(UUID.randomUUID());
        assertThatThrownBy(()->release(event)).isInstanceOf(EventValidationException.class);assertThat(stock()).isEqualTo(48);
        event.getData().setReservationId(correct);event.getData().setCustomerId(UUID.randomUUID());
        assertThatThrownBy(()->release(event)).isInstanceOf(EventValidationException.class);
        event.getData().setCustomerId(owner);release(event);
        event.setEventId(UUID.randomUUID());event.getData().setCancellationId(UUID.randomUUID());
        assertThatThrownBy(()->release(event)).isInstanceOf(EventValidationException.class);assertThat(stock()).isEqualTo(50);
        var released=EventValidator.deserializeAndValidate(payload("inventory",order.getId(),"inventory.released"),InventoryReleasedEvent.class);
        released.getData().setQuantity(3);
        assertThatThrownBy(()->cancellations.complete(released,OrderOutcomeService.hash(EventValidator.serializeAndValidate(released)))).isInstanceOf(EventValidationException.class);
        assertThat(status(order.getId())).isEqualTo("CANCEL_PENDING");complete(order.getId());
    }
    @Test void failureAtEachOutboxWriteRollsBackAllLocalEffects() throws Exception {
        var order=confirmed();
        jdbc.execute("ALTER TABLE order_schema.outbox_events ADD CONSTRAINT phase7_fail CHECK(event_type<>'order.cancel.requested')");
        try { assertThat(cancel(order.getId(),"rollback").getStatus()).isEqualTo(500); }
        finally { jdbc.execute("ALTER TABLE order_schema.outbox_events DROP CONSTRAINT phase7_fail"); }
        assertThat(status(order.getId())).isEqualTo("CONFIRMED");assertThat(cancel(order.getId(),"rollback").getStatus()).isEqualTo(202);
        var event=cancellation(order.getId());
        jdbc.execute("ALTER TABLE inventory_schema.outbox_events ADD CONSTRAINT phase7_fail CHECK(event_type<>'inventory.released')");
        try { assertThatThrownBy(()->release(event)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class); }
        finally { jdbc.execute("ALTER TABLE inventory_schema.outbox_events DROP CONSTRAINT phase7_fail"); }
        assertThat(stock()).isEqualTo(48);assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.inventory_reservations WHERE order_id=?",String.class,order.getId())).isEqualTo("RESERVED");
        assertThat(cancelledNotifications()).isZero();release(event);assertThat(stock()).isEqualTo(50);
        jdbc.execute("ALTER TABLE order_schema.outbox_events ADD CONSTRAINT phase7_fail CHECK(event_type<>'order.cancelled')");
        try { assertThatThrownBy(()->complete(order.getId())).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class); }
        finally { jdbc.execute("ALTER TABLE order_schema.outbox_events DROP CONSTRAINT phase7_fail"); }
        assertThat(status(order.getId())).isEqualTo("CANCEL_PENDING");complete(order.getId());assertThat(status(order.getId())).isEqualTo("CANCELLED");
    }
    @Test void kafkaLifecycleWaitsForInventoryRecoveryAndNotifiesOnlyAfterRelease() throws Exception {
        var order=create();var orderRelay=new OutboxRelay(dataSource,jdbc,kafka,transactions,Clock.systemUTC());
        var inventoryRelay=new InventoryOutboxRelay(dataSource,jdbc,kafka,transactions,Clock.systemUTC());
        registry.start();
        try {
            orderRelay.publishPending();
            await().atMost(Duration.ofSeconds(30)).until(()->stock()==48);inventoryRelay.publishPending();
            await().atMost(Duration.ofSeconds(30)).until(()->status(order.getId()).equals("CONFIRMED"));orderRelay.publishPending();
            await().atMost(Duration.ofSeconds(30)).until(()->notificationService.timeline(order.getId()).size()==2);
            var inventory=registry.getListenerContainer("inventory-releases");inventory.stop();
            assertThat(cancel(order.getId(),"outage").getStatus()).isEqualTo(202);orderRelay.publishPending();
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(()->{
                assertThat(status(order.getId())).isEqualTo("CANCEL_PENDING");assertThat(stock()).isEqualTo(48);
                assertThat(cancelledNotifications()).isZero();assertThat(count("order","order.cancelled")).isZero();
            });
            inventory.start();await().atMost(Duration.ofSeconds(30)).until(()->stock()==50);
            assertThat(status(order.getId())).isEqualTo("CANCEL_PENDING");assertThat(cancelledNotifications()).isZero();
            inventoryRelay.publishPending();await().atMost(Duration.ofSeconds(30)).until(()->status(order.getId()).equals("CANCELLED"));
            orderRelay.publishPending();await().atMost(Duration.ofSeconds(30)).until(()->cancelledNotifications()==1);
            assertThat(notificationService.latest(order.getId()).orElseThrow().eventType()).isEqualTo("ORDER_CANCELLED");
        } finally { registry.stop(); }
    }
}

