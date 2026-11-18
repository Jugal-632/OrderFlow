package com.orderflow.inventory;

import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.OrderPlacedEvent;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties = {"spring.flyway.enabled=true", "inventory.relay.enabled=false",
        "spring.kafka.listener.auto-startup=false", "spring.datasource.hikari.maximum-pool-size=20"})
class InventoryProcessingIT {
    @Container static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15.6");
    @Container static KafkaContainer broker = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", broker::getBootstrapServers);
    }
    @Autowired InventoryReservationService reservations;
    @Autowired InventoryListener listener;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired PlatformTransactionManager transactions;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    UUID product;

    @BeforeAll static void topics() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers()))) {
            admin.createTopics(List.of("order.placed", "inventory.reserved", "inventory.rejected", "order.placed.inventory-service.DLT")
                    .stream().map(name -> new NewTopic(name, 3, (short) 1)).toList()).all().get(20, TimeUnit.SECONDS);
        }
    }
    @BeforeEach void seed() {
        jdbc.execute("TRUNCATE inventory_schema.consumer_inbox,inventory_schema.outbox_events,inventory_schema.inventory_decisions,inventory_schema.inventory_reservations,inventory_schema.inventory CASCADE");
        product = UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available,updated_at) VALUES (?,'Test product',50,?)", product, Timestamp.from(Instant.now()));
    }
    EventEnvelope<OrderPlacedEvent> event(int quantity) {
        UUID order = UUID.randomUUID();
        return EventEnvelope.<OrderPlacedEvent>builder().eventId(UUID.randomUUID()).eventType("order.placed")
                .schemaVersion(1).occurredAt(Instant.now()).producer("order-service").aggregateId(order).aggregateVersion(1)
                .correlationId(UUID.randomUUID()).causationId(UUID.randomUUID())
                .data(new OrderPlacedEvent(order, UUID.randomUUID(), product, quantity)).build();
    }
    String process(EventEnvelope<OrderPlacedEvent> event) {
        return reservations.reserve(event, InventoryReservationService.hash(EventValidator.serializeAndValidate(event)));
    }
    int count(String table) { return jdbc.queryForObject("SELECT count(*) FROM inventory_schema." + table, Integer.class); }
    int stock() { return jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?", Integer.class, product); }
    InventoryOutboxRelay relay(KafkaTemplate<String,String> template) {
        return new InventoryOutboxRelay(dataSource, jdbc, template, transactions, Clock.systemUTC());
    }
    @SuppressWarnings("unchecked") KafkaTemplate<String,String> successfulKafka() {
        KafkaTemplate<String,String> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        return template;
    }

    @Test void reservationAndTenReplaysDeductOnlyOnce() {
        var event = event(2);
        String json = EventValidator.serializeAndValidate(event);
        for (int i = 0; i < 11; i++) listener.receive(json);
        assertThat(stock()).isEqualTo(48);
        assertThat(count("inventory_reservations")).isEqualTo(1);
        assertThat(count("inventory_decisions")).isEqualTo(1);
        assertThat(count("outbox_events")).isEqualTo(1);
        assertThat(count("consumer_inbox")).isEqualTo(1);
        event.setEventId(UUID.randomUUID());
        assertThat(process(event)).isEqualTo("RESERVED");
        assertThat(stock()).isEqualTo(48);
        assertThat(count("outbox_events")).isEqualTo(1);
    }
    @Test void rejectionStaysRejectedAfterRestocking() {
        var event = event(51);
        assertThat(process(event)).isEqualTo("REJECTED");
        jdbc.update("UPDATE inventory_schema.inventory SET quantity_available=100 WHERE product_id=?", product);
        event.setEventId(UUID.randomUUID());
        assertThat(process(event)).isEqualTo("REJECTED");
        assertThat(stock()).isEqualTo(100);
        assertThat(count("inventory_reservations")).isZero();
        assertThat(count("outbox_events")).isEqualTo(1);
    }
    @Test void hundredConcurrentOrdersCannotOversell() throws Exception {
        try (var executor = Executors.newFixedThreadPool(20)) {
            List<Callable<String>> work = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                var event = event(1);
                work.add(() -> {
                    var outcome = new java.util.concurrent.atomic.AtomicReference<String>();
                    // Direct invocation bypasses the listener's infrastructure retry policy.
                    // Retry the identical immutable event after a rolled-back transient failure.
                    await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).until(() -> {
                        try { outcome.set(process(event)); return true; }
                        catch (org.springframework.dao.DataAccessResourceFailureException | org.springframework.dao.TransientDataAccessException transientFailure) { return false; }
                    });
                    return outcome.get();
                });
            }
            var outcomes = new ArrayList<String>();
            for (var future : executor.invokeAll(work)) outcomes.add(future.get(30, TimeUnit.SECONDS));
            assertThat(outcomes.stream().filter("RESERVED"::equals).count()).isEqualTo(50);
            assertThat(outcomes.stream().filter("REJECTED"::equals).count()).isEqualTo(50);
        }
        assertThat(stock()).isZero();
        assertThat(count("inventory_reservations")).isEqualTo(50);
        assertThat(count("outbox_events")).isEqualTo(100);
    }
    @Test void concurrentDistinctEventsForSameOrderHaveOneOutcome() throws Exception {
        var original = event(2);
        try (var executor = Executors.newFixedThreadPool(10)) {
            List<Callable<String>> work = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                var duplicate = EventValidator.deserializeAndValidate(EventValidator.serializeAndValidate(original), OrderPlacedEvent.class);
                duplicate.setEventId(UUID.randomUUID()); work.add(() -> process(duplicate));
            }
            for (var future : executor.invokeAll(work)) assertThat(future.get()).isEqualTo("RESERVED");
        }
        assertThat(stock()).isEqualTo(48);
        assertThat(count("outbox_events")).isEqualTo(1);
    }
    @Test void protocolConflictsRollbackInboxAndNeverChangeStock() {
        var event = event(2); process(event);
        event.getData().setQuantity(3);
        assertThatThrownBy(() -> process(event)).isInstanceOf(EventValidationException.class);
        event.setEventId(UUID.randomUUID());
        assertThatThrownBy(() -> process(event)).isInstanceOf(EventValidationException.class);
        assertThat(count("consumer_inbox")).isEqualTo(1);
        assertThat(stock()).isEqualTo(48);
    }
    @Test void transactionRollsBackAllWritesWhenOutboxInsertFails() {
        jdbc.execute("ALTER TABLE inventory_schema.outbox_events ADD CONSTRAINT test_reject_insert CHECK (false)");
        try { assertThatThrownBy(() -> process(event(2))).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class); }
        finally { jdbc.execute("ALTER TABLE inventory_schema.outbox_events DROP CONSTRAINT test_reject_insert"); }
        assertThat(stock()).isEqualTo(50);
        assertThat(count("consumer_inbox")).isZero();
        assertThat(count("inventory_decisions")).isZero();
        assertThat(count("inventory_reservations")).isZero();
    }
    @Test void unknownProductAndMalformedInputHaveDistinctHandling() {
        var event = event(1); event.getData().setProductId(UUID.randomUUID());
        assertThat(process(event)).isEqualTo("REJECTED");
        assertThat(jdbc.queryForObject("SELECT reason FROM inventory_schema.inventory_decisions", String.class)).isEqualTo("PRODUCT_NOT_FOUND");
        assertThatThrownBy(() -> listener.receive("{broken")).isInstanceOf(EventValidationException.class);
        event.getData().setQuantity(0);
        assertThatThrownBy(() -> process(event)).isInstanceOf(EventValidationException.class);
        assertThat(count("consumer_inbox")).isEqualTo(1);
    }
    @Test void brokerFailureRetainsImmutablePendingEventThenRecovers() throws Exception {
        process(event(2));
        var template = successfulKafka();
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new TimeoutException()));
        String payload = jdbc.queryForObject("SELECT payload::text FROM inventory_schema.outbox_events", String.class);
        relay(template).publishPending();
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events", String.class)).isEqualTo("PENDING");
        assertThat(jdbc.queryForObject("SELECT retry_count FROM inventory_schema.outbox_events", Integer.class)).isEqualTo(1);
        jdbc.update("UPDATE inventory_schema.outbox_events SET next_attempt_at=?", Timestamp.from(Instant.EPOCH));
        relay(successfulKafka()).publishPending();
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events", String.class)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("SELECT payload::text FROM inventory_schema.outbox_events", String.class)).isEqualTo(payload);
    }
    @Test void processTerminationAfterBrokerAckRepublishesWithoutStockChange() throws Exception {
        var event = event(2); process(event);
        assertThat(runRelayProcess("crash")).isEqualTo(73);
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events", String.class)).isEqualTo("PENDING");
        assertThat(runRelayProcess("publish")).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events", String.class)).isEqualTo("PUBLISHED");
        process(event);
        assertThat(stock()).isEqualTo(48);
        assertThat(count("inventory_reservations")).isEqualTo(1);
        assertThat(count("outbox_events")).isEqualTo(1);
    }
    int runRelayProcess(String mode) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Duser.timezone=UTC", "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                InventoryRelayProcess.class.getName(), postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(),
                broker.getBootstrapServers(), mode)
                .redirectErrorStream(true).redirectOutput(Path.of("target", "relay-process-" + mode + ".log").toFile()).start();
        try {
            assertThat(process.waitFor(40, TimeUnit.SECONDS)).as("Forked relay completes within deadline").isTrue();
            return process.exitValue();
        } finally { if (process.isAlive()) process.destroyForcibly(); }
    }
    @Test void sessionLockAndBlockedPredecessorPreventPublishing() throws Exception {
        process(event(2));
        var template = successfulKafka();
        try (var session = dataSource.getConnection(); var statement = session.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(" + InventoryOutboxRelay.LOCK_ID + ")");
            try { relay(template).publishPending(); verifyNoInteractions(template); }
            finally { statement.execute("SELECT pg_advisory_unlock(" + InventoryOutboxRelay.LOCK_ID + ")"); }
        }
        jdbc.update("UPDATE inventory_schema.outbox_events SET payload='{}'::jsonb");
        relay(template).publishPending();
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events", String.class)).isEqualTo("BLOCKED");
        jdbc.execute("""
            INSERT INTO inventory_schema.outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,payload,status,aggregate_version)
            SELECT gen_random_uuid(),aggregate_id,aggregate_type,event_type,topic,event_key,payload,'PENDING',2 FROM inventory_schema.outbox_events
            """);
        relay(template).publishPending();
        verifyNoInteractions(template);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_schema.outbox_events WHERE status='PENDING'", Integer.class)).isEqualTo(1);
    }
    @Test void actualKafkaConsumerRoutesMalformedJsonToDltAndProcessesValidRecord() throws Exception {
        String group = "verify-dlt-" + UUID.randomUUID();
        try (var consumer = new KafkaConsumer<String,String>(Map.of("bootstrap.servers", broker.getBootstrapServers(),
                "group.id", group, "auto.offset.reset", "earliest", "enable.auto.commit", false), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("order.placed.inventory-service.DLT"));
            registry.start();
            try {
                kafka.send("order.placed", UUID.randomUUID().toString(), "{broken").get(10, TimeUnit.SECONDS);
                var event = event(2);
                kafka.send("order.placed", event.getAggregateId().toString(), EventValidator.serializeAndValidate(event)).get(10, TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> assertThat(count("inventory_reservations")).isEqualTo(1));
                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    for (var record : consumer.poll(Duration.ofMillis(500))) if (record.value().equals("{broken")) return true;
                    return false;
                });
                assertThat(stock()).isEqualTo(48);
            } finally { registry.stop(); }
        }
    }
    @Test void allServiceMigrationsWorkFreshAndOnExplicitV1Baseline() throws Exception {
        Path root = Path.of("..").toAbsolutePath().normalize();
        for (String service : List.of("auth", "order", "inventory", "notification")) {
            String location = "filesystem:" + root.resolve(service + "-service/src/main/resources/db/migration").toString().replace('\\','/');
            String fresh = service + "_audit_fresh";
            var flyway = Flyway.configure().dataSource(dataSource).schemas(fresh).defaultSchema(fresh).locations(location).load();
            flyway.migrate(); flyway.validate();
            String existing = service + "_audit_existing";
            jdbc.execute("CREATE SCHEMA " + existing);
            try (var connection = dataSource.getConnection()) {
                connection.setSchema(existing);
                org.springframework.jdbc.datasource.init.ScriptUtils.executeSqlScript(connection,
                        new org.springframework.core.io.FileSystemResource(root.resolve(service + "-service/src/main/resources/db/migration/V1__baseline.sql")));
            }
            UUID preserved=UUID.randomUUID();
            switch(service){
                case "auth" -> jdbc.update("INSERT INTO "+existing+".users(id,email,password_hash) VALUES (?,'migration@isolated.test','test-hash')",preserved);
                case "order" -> jdbc.update("INSERT INTO "+existing+".orders(id,customer_id,product_id,quantity,status) VALUES (?,?,?,2,'PENDING')",preserved,UUID.randomUUID(),UUID.randomUUID());
                case "inventory" -> {jdbc.update("INSERT INTO "+existing+".inventory(product_id,product_name,quantity_available) VALUES (?,'Preserved',17)",preserved);jdbc.update("INSERT INTO "+existing+".inventory_reservations(order_id,product_id,quantity) VALUES (?,?,3)",UUID.randomUUID(),preserved);}
                case "notification" -> jdbc.update("INSERT INTO "+existing+".notifications(id,order_id,event_type,message) VALUES (?,?,'ORDER_RECEIVED','Preserved')",preserved,UUID.randomUUID());
            }
            var upgrade = Flyway.configure().dataSource(dataSource).schemas(existing).defaultSchema(existing).locations(location).baselineVersion("1").load();
            upgrade.baseline(); upgrade.migrate(); upgrade.validate();
            String table=switch(service){case "auth"->"users";case "order"->"orders";case "inventory"->"inventory";default->"notifications";};
            assertThat(jdbc.queryForObject("SELECT count(*) FROM "+existing+"."+table,Long.class)).isEqualTo(1);
            if(service.equals("inventory"))assertThat(jdbc.queryForObject("SELECT quantity_available FROM "+existing+".inventory WHERE product_id=?",Integer.class,preserved)).isEqualTo(17);
        }
    }
}

