package com.orderflow.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.envelope.*;
import com.orderflow.contracts.events.*;
import com.orderflow.inventory.*;
import com.orderflow.notification.*;
import com.orderflow.ops.*;
import com.orderflow.order.dto.*;
import com.orderflow.order.service.*;
import com.orderflow.security.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;
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
import org.springframework.kafka.support.KafkaHeaders;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","order.relay.enabled=false","ops.relay.enabled=false","spring.kafka.listener.auto-startup=false",
    "spring.flyway.locations=filesystem:src/main/resources/db/migration","spring.jpa.show-sql=false",
    "ops.internal-token=phase8-internal-test-credential-32-characters","management.health.redis.enabled=false"})
@AutoConfigureMockMvc
@Import({InventoryReservationService.class,NotificationService.class,OrderLifecycleConsumer.class,NotificationKafkaConfig.class})
class Phase8IT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static KafkaContainer broker=new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        for(String service:List.of("inventory","notification")) Flyway.configure().dataSource(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword())
            .schemas(service+"_schema").defaultSchema(service+"_schema").locations("filesystem:../"+service+"-service/src/main/resources/db/migration").load().migrate();
        p.add("spring.datasource.url",postgres::getJdbcUrl);p.add("spring.datasource.username",postgres::getUsername);p.add("spring.datasource.password",postgres::getPassword);
        p.add("spring.kafka.bootstrap-servers",broker::getBootstrapServers);
    }
    @Autowired OpsService ops;
    @Autowired OrderService orders;
    @Autowired InventoryReservationService inventory;
    @Autowired OrderLifecycleConsumer notifications;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;
    @Autowired MockMvc http;
    @Autowired DataSource data;
    @Autowired PlatformTransactionManager transactions;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired KafkaListenerEndpointRegistry registry;
    @MockitoBean IdentityVerifier identity;
    UUID owner,product;Caller admin;
    @BeforeEach void seed() {
        jdbc.execute("TRUNCATE order_schema.orders,order_schema.outbox_events,order_schema.consumer_inbox,order_schema.dead_letters,order_schema.ops_audit,inventory_schema.inventory,inventory_schema.inventory_reservations,inventory_schema.inventory_decisions,inventory_schema.outbox_events,inventory_schema.consumer_inbox,inventory_schema.dead_letters,notification_schema.notifications,notification_schema.consumer_inbox,notification_schema.dead_letters CASCADE");
        owner=UUID.randomUUID();product=UUID.randomUUID();admin=new Caller(owner,Set.of("ADMIN"),"test",Instant.now().plusSeconds(1800));when(identity.verify(org.mockito.ArgumentMatchers.anyString())).thenReturn(admin);
        jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES (?,'Phase8',50)",product);
    }
    OrderResponse create() {var r=new OrderRequest();r.setCustomerId(owner);r.setProductId(product);r.setQuantity(2);return orders.createOrder(r);}
    String payload(String schema,UUID id,String type) {return jdbc.queryForObject("SELECT payload::text FROM "+schema+"_schema.outbox_events WHERE aggregate_id=? AND event_type=?",String.class,id,type);}
    String reserved(UUID id) {String placed=payload("order",id,"order.placed");inventory.reserve(EventValidator.deserializeAndValidate(placed,OrderPlacedEvent.class),OrderOutcomeService.hash(placed));return payload("inventory",id,"inventory.reserved");}
    ConsumerRecord<String,String> dlt(OpsService service,String topic,String raw,long offset) {
        var r=new ConsumerRecord<String,String>(topic+"."+service.service+".DLT",1,offset,"key",raw);
        r.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC,topic.getBytes(StandardCharsets.UTF_8));r.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION,ByteBuffer.allocate(4).putInt(2).array());r.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET,ByteBuffer.allocate(8).putLong(37).array());return r;
    }
    UUID index(OpsService service,String topic,String raw) {var r=dlt(service,topic,raw,0);service.index(r);service.index(r);return (UUID)service.list("OPEN",0,20).getFirst().get("id");}
    String queue(OpsService service,UUID id,String key) {var request=service.request(admin,id,key,json.createObjectNode().put("reason","Dependency repaired"));return jdbc.queryForObject("SELECT payload::text FROM "+service.schema+".outbox_events WHERE id=?",String.class,request.get("replayRequestId"));}
    int count(String table) {return jdbc.queryForObject("SELECT count(*) FROM "+table,Integer.class);}
    String orderStatus(UUID id) {return jdbc.queryForObject("SELECT status FROM order_schema.orders WHERE id=?",String.class,id);}
    @Test void replayIsAtomicAndDuplicateDeliveryHasNoBusinessEffects() {
        UUID order=create().getId();String source=reserved(order);UUID id=index(ops,"inventory.reserved",source);String wrapper=queue(ops,id,"one");
        assertThat(count("order_schema.dead_letters")).isEqualTo(1);
        assertThat(ops.detail(id).get("deadLetter").toString()).contains("source_offset=37","source_partition=2");
        jdbc.execute("ALTER TABLE order_schema.replay_requests ADD CONSTRAINT force_replay_rollback CHECK(result <> 'SUCCEEDED')");
        try {assertThatThrownBy(()->ops.replay(wrapper)).isInstanceOf(RuntimeException.class);assertThat(orderStatus(order)).isEqualTo("PENDING");assertThat(count("order_schema.consumer_inbox")).isZero();}
        finally {jdbc.execute("ALTER TABLE order_schema.replay_requests DROP CONSTRAINT force_replay_rollback");}
        for(int i=0;i<10;i++)ops.replay(wrapper);
        assertThat(orderStatus(order)).isEqualTo("CONFIRMED");assertThat(count("order_schema.consumer_inbox")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE event_type='order.confirmed'",Integer.class)).isEqualTo(1);
        assertThat(ops.list("RESOLVED",0,20)).hasSize(1);
        assertThat(ops.request(admin,id,"one",json.createObjectNode().put("reason","Dependency repaired")).get("replayed")).isEqualTo(true);
    }
    @Test void allServiceHandlersRetainOriginalEventIdentity() {
        UUID order=create().getId();String placed=payload("order",order,"order.placed");
        var inv=new OpsService("inventory-service",jdbc,json,new ReplayHandler(){public Set<String> topics(){return Set.of("order.placed");}public void dispatch(String t,String p){inventory.reserve(EventValidator.deserializeAndValidate(p,OrderPlacedEvent.class),OrderOutcomeService.hash(p));}},transactions);
        String invWrapper=queue(inv,index(inv,"order.placed",placed),"inventory");inv.replay(invWrapper);inv.replay(invWrapper);
        assertThat(jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?",Integer.class,product)).isEqualTo(48);
        var notify=new OpsService("notification-service",jdbc,json,new ReplayHandler(){public Set<String> topics(){return Set.of("order.placed");}public void dispatch(String t,String p){notifications.receive(new ConsumerRecord<>(t,0,0,"key",p));}},transactions);
        String notifyWrapper=queue(notify,index(notify,"order.placed",placed),"notification");notify.replay(notifyWrapper);notify.replay(notifyWrapper);
        assertThat(count("notification_schema.notifications")).isEqualTo(1);
        assertThat(inv.list("RESOLVED",0,20)).hasSize(1);assertThat(notify.list("RESOLVED",0,20)).hasSize(1);
    }
    @Test void concurrentRequestsQueueOnceAndRejectChangedPayload() throws Exception {
        UUID order=create().getId();UUID id=index(ops,"inventory.reserved",reserved(order));
        try(var pool=Executors.newFixedThreadPool(10)) {
            var jobs=new ArrayList<Callable<Map<String,Object>>>();for(int i=0;i<10;i++)jobs.add(()->ops.request(admin,id,"same",json.createObjectNode().put("reason","Dependency repaired")));
            var ids=new HashSet<Object>();for(var f:pool.invokeAll(jobs))ids.add(f.get().get("replayRequestId"));assertThat(ids).hasSize(1);
        }
        assertThat(count("order_schema.replay_requests")).isEqualTo(1);
        assertThatThrownBy(()->ops.request(admin,id,"same",json.createObjectNode().put("reason","Different reason"))).isInstanceOfSatisfying(IdentityFailure.class,e->assertThat(e.status).isEqualTo(409));
        assertThatThrownBy(()->queue(ops,id,"different-key")).isInstanceOfSatisfying(IdentityFailure.class,e->assertThat(e.status).isEqualTo(409));
    }
    @Test void replayHttpContractAndIndexerDatabaseFailuresRemainRetryable() throws Exception {
        UUID order=create().getId();UUID id=index(ops,"inventory.reserved",reserved(order));String path="/api/ops/dead-letters/"+id+"/replay";
        for(int i=0;i<2;i++) {
            var result=http.perform(post(path).header("Authorization","Bearer test").header("Idempotency-Key","http-replay").contentType("application/json").content("{\"reason\":\"Dependency repaired\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("QUEUED"));
            if(i==1)result.andExpect(header().string("Idempotent-Replayed","true"));
        }
        http.perform(get("/api/ops/dead-letters/"+id).header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.deadLetter.source_offset").value(37)).andExpect(jsonPath("$.replays.length()").value(1));
        String wrapper=jdbc.queryForObject("SELECT payload::text FROM order_schema.outbox_events WHERE event_type='ops.replay'",String.class);
        var failed=new ConsumerRecord<String,String>("order-service.replay.DLT",0,100,"key",wrapper);
        jdbc.execute("ALTER TABLE order_schema.dead_letters ADD CONSTRAINT simulate_index_failure CHECK(dlt_offset<>100)");
        try {assertThatThrownBy(()->ops.index(failed)).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(ops.list("REPLAY_QUEUED",0,20)).hasSize(1);}
        finally {jdbc.execute("ALTER TABLE order_schema.dead_letters DROP CONSTRAINT simulate_index_failure");}
        ops.index(failed);ops.index(failed);assertThat(ops.list("LINKED",0,20)).hasSize(1);assertThat(ops.list("OPEN",0,20)).hasSize(1);
    }
    @Test void httpRejectsAndAuditsUnauthorizedMalformedAndIneligibleRequests() throws Exception {
        UUID id=index(ops,"inventory.reserved","{\"schemaVersion\":99}");String path="/api/ops/dead-letters/"+id+"/replay";
        when(identity.verify(org.mockito.ArgumentMatchers.isNull())).thenThrow(new IdentityFailure(401,"UNAUTHORIZED"));
        http.perform(post(path).contentType("application/json").content("{}")).andExpect(status().isUnauthorized());
        when(identity.verify(org.mockito.ArgumentMatchers.anyString())).thenReturn(new Caller(owner,Set.of("CUSTOMER"),"test",Instant.now().plusSeconds(30)));
        http.perform(post(path).header("Authorization","Bearer test").contentType("application/json").content("{}")).andExpect(status().isForbidden());
        when(identity.verify(org.mockito.ArgumentMatchers.anyString())).thenReturn(admin);
        http.perform(post(path).header("Authorization","Bearer test").header("Idempotency-Key","valid").contentType("application/json").content("{")).andExpect(status().isBadRequest());
        http.perform(post(path).header("Authorization","Bearer test").header("Idempotency-Key","valid").contentType("application/json").content("{\"reason\":\"Dependency repaired\",\"payload\":\"edited\"}")).andExpect(status().isBadRequest());
        http.perform(post(path).header("Authorization","Bearer test").header("Idempotency-Key","valid").contentType("application/json").content("{\"reason\":\"Dependency repaired\"}")).andExpect(status().isConflict());
        assertThat(count("order_schema.ops_audit")).isEqualTo(5);assertThat(count("order_schema.replay_requests")).isZero();
    }
    @Test void dashboardUsesPersistedCountsTimelineEvidenceAndScopedFilters() throws Exception {
        UUID order=create().getId();ops.replay(queue(ops,index(ops,"inventory.reserved",reserved(order)),"dashboard"));
        http.perform(get("/api/dashboard/overview/stats").header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.counts.CONFIRMED").value(1));
        http.perform(get("/api/orders").param("id",order.toString()).param("status","CONFIRMED").header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(order.toString()));
        http.perform(get("/api/orders/"+order).header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.timeline.length()").value(2));
        http.perform(get("/api/dashboard/test-runs").header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.runs[0].sha256").exists());
        http.perform(get("/api/dashboard/services/health").header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.order.data.readiness").exists()).andExpect(jsonPath("$.order.data.dependencies.db").value("UP"));
        http.perform(get("/internal/ops/status")).andExpect(status().isUnauthorized());
        http.perform(get("/internal/ops/status").header("X-Service-Credential","phase8-internal-test-credential-32-characters")).andExpect(status().isOk());
        when(identity.verify(org.mockito.ArgumentMatchers.anyString())).thenReturn(new Caller(UUID.randomUUID(),Set.of("CUSTOMER"),"test",Instant.now().plusSeconds(30)));
        http.perform(get("/api/orders").param("id",order.toString()).header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
        http.perform(get("/api/dashboard/overview/stats").header("Authorization","Bearer test")).andExpect(status().isForbidden());
    }
    @Test void kafkaRelayAndReplayDltResolveSuccessAndReopenFailure() throws Exception {
        UUID order=create().getId();String source=reserved(order);UUID id=index(ops,"inventory.reserved",source);queue(ops,id,"kafka");
        registry.getListenerContainer("ops-replay").start();registry.getListenerContainer("ops-dlt-indexer").start();
        try {
            new OpsRelay(data,jdbc,kafka,ops,transactions).publish();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertThat(ops.list("RESOLVED",0,20)).hasSize(1));
            String bad=source.replace(order.toString(),UUID.randomUUID().toString()).replace(json.readTree(source).path("eventId").asText(),UUID.randomUUID().toString());
            var record=dlt(ops,"inventory.reserved",bad,1);ops.index(record);UUID failed=(UUID)ops.list("OPEN",0,20).getFirst().get("id");queue(ops,failed,"failure");
            new OpsRelay(data,jdbc,kafka,ops,transactions).publish();
            await().atMost(Duration.ofSeconds(35)).untilAsserted(()->assertThat(ops.list("LINKED",0,20)).hasSize(1));
            assertThat(ops.list("OPEN",0,20)).hasSize(1);
            assertThat(jdbc.queryForObject("SELECT result FROM order_schema.replay_requests WHERE dead_letter_id=?",String.class,failed)).isEqualTo("FAILED");
            http.perform(get("/api/dashboard/kafka/topics").header("Authorization","Bearer test")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("AVAILABLE"));
        } finally {registry.getListenerContainer("ops-replay").stop();registry.getListenerContainer("ops-dlt-indexer").stop();}
    }
}




