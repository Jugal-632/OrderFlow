package com.orderflow.order;

import com.orderflow.security.*;
import com.orderflow.order.service.IdempotencyService.RequestFailure;
import java.time.Duration;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","order.relay.enabled=false","spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false","spring.flyway.locations=filesystem:src/main/resources/db/migration",
        "spring.jpa.show-sql=false","order.idempotency.lock-timeout-ms=500","spring.datasource.hikari.maximum-pool-size=25"})
@AutoConfigureMockMvc
class Phase5IT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @DynamicPropertySource static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url",postgres::getJdbcUrl);
        properties.add("spring.datasource.username",postgres::getUsername);
        properties.add("spring.datasource.password",postgres::getPassword);
    }
    @MockitoBean IdentityVerifier authenticator;
    @Autowired MockMvc http;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    UUID customer=UUID.randomUUID(),other=UUID.randomUUID(),product=UUID.randomUUID();
    String body() { return "{\"productId\":\""+product+"\",\"quantity\":2}"; }
    @BeforeEach void reset() {
        jdbc.execute("TRUNCATE order_schema.orders,order_schema.order_history,order_schema.outbox_events,order_schema.consumer_inbox,order_schema.request_idempotency CASCADE");
        when(authenticator.verify(any())).thenAnswer(call->{
            String token=call.getArgument(0);
            if("Bearer A".equals(token)) return new Caller(customer,Set.of("CUSTOMER"),"test",java.time.Instant.now().plusSeconds(1800));
            if("Bearer B".equals(token)) return new Caller(other,Set.of("CUSTOMER"),"test",java.time.Instant.now().plusSeconds(1800));
            throw new IdentityFailure(401,"UNAUTHENTICATED");
        });
    }
    org.springframework.mock.web.MockHttpServletResponse request(String key,String payload,String token) throws Exception {
        var request=post("/api/orders").contentType("application/json").content(payload);
        if(key!=null) request.header("Idempotency-Key",key);
        if(token!=null) request.header("Authorization",token);
        return http.perform(request).andReturn().getResponse();
    }
    int count(String table) { return jdbc.queryForObject("SELECT count(*) FROM order_schema."+table,Integer.class); }

    @Test void twentyConcurrentHttpRequestsCreateOneOrderAndOneEvent() throws Exception {
        try(var executor=Executors.newFixedThreadPool(20)) {
            var ready=new CountDownLatch(20);var start=new CountDownLatch(1);
            List<Future<org.springframework.mock.web.MockHttpServletResponse>> results=new ArrayList<>();
            for(int i=0;i<20;i++) results.add(executor.submit(()->{ready.countDown();start.await();return request("same",body(),"Bearer A");}));
            assertThat(ready.await(5,TimeUnit.SECONDS)).isTrue();start.countDown();
            Set<String> bodies=new HashSet<>();Set<String> locations=new HashSet<>();int replays=0;
            for(var result:results) {
                var response=result.get(15,TimeUnit.SECONDS);assertThat(response.getStatus()).isEqualTo(201);
                bodies.add(response.getContentAsString());locations.add(response.getHeader("Location"));
                if("true".equals(response.getHeader("Idempotent-Replayed"))) replays++;
            }
            assertThat(bodies).hasSize(1);assertThat(locations).hasSize(1);assertThat(replays).isEqualTo(19);
        }
        assertThat(count("orders")).isEqualTo(1);assertThat(count("outbox_events")).isEqualTo(1);
        assertThat(count("order_history")).isEqualTo(1);assertThat(count("request_idempotency")).isEqualTo(1);
    }
    @Test void replayReturnsOriginalResponseEvenAfterConfirmation() throws Exception {
        var first=request("snapshot",body(),"Bearer A");assertThat(first.getStatus()).isEqualTo(201);
        jdbc.update("UPDATE order_schema.orders SET status='CONFIRMED',version=2");
        String reordered="{ \"quantity\" : 2, \"productId\" : \""+product.toString().toUpperCase()+"\" }";
        var replay=request("snapshot",reordered,"Bearer A");
        assertThat(replay.getStatus()).isEqualTo(201);assertThat(replay.getHeader("Idempotent-Replayed")).isEqualTo("true");
        assertThat(replay.getContentAsString()).isEqualTo(first.getContentAsString()).contains("PENDING");
        assertThat(replay.getHeader("Location")).isEqualTo(first.getHeader("Location"));
    }
    @Test void changedProductOrQuantityConflictsAndCustomersAreIndependent() throws Exception {
        assertThat(request("scope",body(),"Bearer A").getStatus()).isEqualTo(201);
        for(String changed:List.of(body().replace(":2",":3"),body().replace(product.toString(),UUID.randomUUID().toString()))) {
            var response=request("scope",changed,"Bearer A");assertThat(response.getStatus()).isEqualTo(409);
            assertThat(response.getContentAsString()).contains("IDEMPOTENCY_KEY_REUSED");
        }
        assertThat(request("scope",body(),"Bearer B").getStatus()).isEqualTo(201);
        assertThat(count("orders")).isEqualTo(2);assertThat(count("request_idempotency")).isEqualTo(2);
        assertThat(jdbc.queryForList("SELECT customer_id FROM order_schema.orders",UUID.class)).containsExactlyInAnyOrder(customer,other);
    }
    @Test void invalidKeysBodiesAndAuthenticationLeaveNoRecords() throws Exception {
        for(String key:new String[]{null,"","has space","é","x".repeat(129),"tab\tkey"})
            assertThat(request(key,body(),"Bearer A").getStatus()).as("invalid key %s",key).isEqualTo(400);
        for(String invalid:List.of("{}",body().replace(":2",":0"),body().replace(":2",":2.1"),body().replace(":2",":null"),
                body().replace("}",",\"customerId\":\""+other+"\"}"),body().replace("}",",\"extra\":true}")))
            assertThat(request("invalid",invalid,"Bearer A").getStatus()).isEqualTo(400);
        assertThat(request("unauth",body(),null).getStatus()).isEqualTo(401);
        assertThat(count("orders")).isZero();assertThat(count("request_idempotency")).isZero();
    }
    @Test void insertFailureRollsBackClaimAndRetryCanOwnIt() throws Exception {
        jdbc.execute("ALTER TABLE order_schema.outbox_events ADD CONSTRAINT test_fail CHECK(false)");
        try { assertThat(request("rollback",body(),"Bearer A").getStatus()).isEqualTo(500); }
        finally { jdbc.execute("ALTER TABLE order_schema.outbox_events DROP CONSTRAINT test_fail"); }
        assertThat(count("orders")).isZero();assertThat(count("order_history")).isZero();assertThat(count("request_idempotency")).isZero();
        assertThat(request("rollback",body(),"Bearer A").getStatus()).isEqualTo(201);
    }
    java.sql.Connection claim(String key) throws Exception {
        var connection=dataSource.getConnection();connection.setAutoCommit(false);
        try(var statement=connection.prepareStatement("INSERT INTO order_schema.request_idempotency(customer_id,operation,idempotency_key,request_hash,response_status) VALUES (?,'CREATE_ORDER',?,'test',0)")) {
            statement.setObject(1,customer);statement.setString(2,key);statement.executeUpdate();
        }
        return connection;
    }
    @Test void lockTimeoutIsRetryableAndRollbackReleasesClaim() throws Exception {
        try(var owner=claim("locked")) {
            var response=request("locked",body(),"Bearer A");assertThat(response.getStatus()).isEqualTo(503);
            assertThat(response.getHeader("Retry-After")).isEqualTo("1");owner.rollback();
        }
        assertThat(request("locked",body(),"Bearer A").getStatus()).isEqualTo(201);
    }
    @Test void waitingCallerTakesOwnershipAfterOriginalTransactionRollsBack() throws Exception {
        try(var owner=claim("takeover");var executor=Executors.newSingleThreadExecutor()) {
            var result=executor.submit(()->request("takeover",body(),"Bearer A"));
            Thread.sleep(100);assertThat(result.isDone()).isFalse();owner.rollback();
            assertThat(result.get(10,TimeUnit.SECONDS).getStatus()).isEqualTo(201);
        }
        assertThat(count("orders")).isEqualTo(1);
    }
    @Test void incompleteClaimCannotCommit() throws Exception {
        try(var connection=claim("incomplete")) {
            assertThatThrownBy(connection::commit).isInstanceOf(java.sql.SQLException.class);connection.rollback();
        }
        assertThat(count("request_idempotency")).isZero();
    }
    @Test void migrationsAndDeferredGuardRespectSelectedSchema() {
        org.flywaydb.core.Flyway.configure().dataSource(dataSource).schemas("idempotency_audit").defaultSchema("idempotency_audit")
                .locations("filesystem:src/main/resources/db/migration").load().migrate();
        assertThatThrownBy(()->jdbc.update("INSERT INTO idempotency_audit.request_idempotency(customer_id,operation,idempotency_key,request_hash,response_status) VALUES (?,'CREATE_ORDER','incomplete','test',0)",customer))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM idempotency_audit.request_idempotency",Integer.class)).isZero();
    }
    @Test void realProcessCrashAfterCommitRecoversSavedResponse() throws Exception {
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Duser.timezone=UTC",
                "-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                IdempotencyCrashProcess.class.getName(),postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword(),customer.toString(),product.toString())
                .redirectErrorStream(true).redirectOutput(Path.of("target","phase5-crash.log").toFile()).start();
        try { assertThat(child.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isEqualTo(73); }
        finally { if(child.isAlive()) child.destroyForcibly(); }
        var replay=request("crash",body(),"Bearer A");assertThat(replay.getStatus()).isEqualTo(201);
        assertThat(replay.getHeader("Idempotent-Replayed")).isEqualTo("true");
        assertThat(replay.getContentAsString()).isEqualTo(jdbc.queryForObject("SELECT response_body::text FROM order_schema.request_idempotency",String.class));
        assertThat(count("orders")).isEqualTo(1);assertThat(count("outbox_events")).isEqualTo(1);
    }
    @Test void realProcessCrashBeforeCommitLeavesNoRows() throws Exception {
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Duser.timezone=UTC","-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                IdempotencyCrashProcess.class.getName(),postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword(),customer.toString(),product.toString(),"before-commit")
                .redirectErrorStream(true).redirectOutput(Path.of("target","phase9-before-commit.log").toFile()).start();
        try {assertThat(child.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isEqualTo(73);}finally{if(child.isAlive())child.destroyForcibly();}
        assertThat(count("orders")).isZero();assertThat(count("outbox_events")).isZero();assertThat(count("request_idempotency")).isZero();
    }
}

