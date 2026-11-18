package com.orderflow.gateway;

import com.orderflow.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.*;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.http.MediaType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import static org.assertj.core.api.Assertions.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"jwt.secret=phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-"})
class GatewaySecurityIT {
    @Container static GenericContainer<?> redisContainer=new GenericContainer<>("redis:7.2.4-alpine").withExposedPorts(6379);
    static DisposableServer upstream=HttpServer.create().host("127.0.0.1").port(0).handle((request,response)->
            request.receive().aggregate().asString().defaultIfEmpty("").flatMap(body->response
                    .header("Content-Type","text/plain").header("X-Seen-Path",request.uri())
                    .header("X-Seen-Authorization",Objects.toString(request.requestHeaders().get("Authorization"),"none"))
                    .header("X-Seen-User",Objects.toString(request.requestHeaders().get("X-User-Id"),"none"))
                    .header("X-Seen-Correlation",Objects.toString(request.requestHeaders().get("X-Correlation-Id"),"none"))
                    .sendString(Mono.just("ok")).then())).bindNow();
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.data.redis.host",redisContainer::getHost);p.add("spring.data.redis.port",()->redisContainer.getMappedPort(6379));
        for(String service:List.of("auth","order","inventory","notification")) p.add("services."+service,()->"http://127.0.0.1:"+upstream.port());
    }
    @LocalServerPort int port;
    @Autowired JwtTokens tokens;
    @Autowired ReactiveStringRedisTemplate redis;
    WebTestClient client;
    @BeforeEach void setup() {
        client=WebTestClient.bindToServer().baseUrl("http://localhost:"+port).responseTimeout(Duration.ofSeconds(10)).build();
        redis.execute(connection->connection.serverCommands().flushAll()).blockLast();
    }
    @AfterAll static void close() { upstream.disposeNow(); }
    String token(String role) { return "Bearer "+tokens.issue(UUID.randomUUID(),Set.of(role)); }
    @Test void rejectedRecoveryAccessIsAuditedWithoutCredentials() {
        var logger=(ch.qos.logback.classic.Logger)org.slf4j.LoggerFactory.getLogger("orderflow.access.audit");
        var capture=new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();capture.start();logger.addAppender(capture);
        String correlation=UUID.randomUUID().toString(),bearer=token("CUSTOMER");
        try {
            client.post().uri("/api/ops/order/dead-letters/invalid/replay").header("X-Correlation-Id",correlation).exchange().expectStatus().isUnauthorized();
            client.post().uri("/api/ops/order/dead-letters/invalid/replay").header("Authorization",bearer).header("X-Correlation-Id",correlation).exchange().expectStatus().isForbidden();
            assertThat(capture.list).hasSize(2);assertThat(capture.list.getFirst().getFormattedMessage()).contains("status=401",correlation).doesNotContain(bearer);
            assertThat(capture.list.getLast().getFormattedMessage()).contains("status=403",correlation).doesNotContain(bearer);
        } finally {logger.detachAppender(capture);capture.stop();}
    }
    @Test void routesPublicAndProtectedApisAndStripsSpoofedIdentity() {
        client.post().uri("/auth/login").exchange().expectStatus().isOk();
        String bearer=token("ADMIN"),correlation=UUID.randomUUID().toString();
        for(String path:List.of("/auth/me","/api/orders","/api/dashboard/stats","/api/inventory","/api/notifications"))
            client.get().uri(path).header("Authorization",bearer).header("X-User-Id","spoof").header("X-Correlation-Id",correlation)
                    .exchange().expectStatus().isOk().expectHeader().valueEquals("X-Seen-Authorization",bearer)
                    .expectHeader().valueEquals("X-Seen-User","none").expectHeader().valueEquals("X-Seen-Correlation",correlation);
        for(String service:List.of("order","inventory","notification")) client.get().uri("/api/ops/"+service+"/dead-letters")
                .header("Authorization",bearer).exchange().expectStatus().isOk().expectHeader().valueEquals("X-Seen-Path","/api/ops/dead-letters");
        client.get().uri("/internal/test").header("Authorization",bearer).exchange().expectStatus().isNotFound();
    }
    @Test void rejectsMissingInvalidExpiredRevokedAndCustomerAdminAccess() {
        client.get().uri("/api/orders").exchange().expectStatus().isUnauthorized().expectBody().jsonPath("$.correlationId").exists();
        client.get().uri("/api/orders").header("Authorization","Bearer invalid").exchange().expectStatus().isUnauthorized();
        String expired=io.jsonwebtoken.Jwts.builder().subject(UUID.randomUUID().toString()).issuer("orderflow-auth")
                .audience().add("orderflow-api").and().claim("roles",List.of("CUSTOMER")).id(UUID.randomUUID().toString())
                .issuedAt(Date.from(Instant.now().minusSeconds(1900))).expiration(Date.from(Instant.now().minusSeconds(100)))
                .signWith(io.jsonwebtoken.security.Keys.hmacShaKeyFor(JwtTokensTestSecret.VALUE.getBytes()),io.jsonwebtoken.Jwts.SIG.HS512).compact();
        client.get().uri("/api/orders").header("Authorization","Bearer "+expired).exchange().expectStatus().isUnauthorized();
        String bearer=token("CUSTOMER");
        for(String path:List.of("/api/inventory","/api/dashboard/stats","/api/notifications","/api/ops/order/replay"))
            client.get().uri(path).header("Authorization",bearer).exchange().expectStatus().isForbidden();
        redis.opsForValue().set(IdentityVerifier.REVOKED+tokens.verify(bearer).tokenId(),"1",Duration.ofMinutes(30)).block();
        client.get().uri("/api/orders").header("Authorization",bearer).exchange().expectStatus().isUnauthorized();
    }
    @Test void rateLimitsLoginReadsAndMutationsWithRetryAfter() {
        for(String kind:List.of("login","read","write")) {
            String bearer=token("CUSTOMER");boolean limited=false;
            int max=kind.equals("read")?80:kind.equals("login")?15:30;
            for(int i=0;i<max;i++) {
                var request=kind.equals("read")?client.get().uri("/api/orders"):client.post().uri(kind.equals("login")?"/auth/login":"/api/orders");
                var response=request.header("Authorization",bearer).exchange().returnResult(String.class);
                if(response.getStatus().value()==429) {assertThat(Integer.parseInt(response.getResponseHeaders().getFirst("Retry-After"))).isPositive();limited=true;break;}
                assertThat(response.getStatus().value()).isEqualTo(200);
            }
            assertThat(limited).as(kind).isTrue();
        }
    }
    @Test void corsAndBothKnownAndChunkedBodyLimits() {
        client.options().uri("/api/orders").header("Origin","http://localhost:3000").header("Access-Control-Request-Method","POST")
                .header("Access-Control-Request-Headers","authorization,idempotency-key,x-correlation-id,content-type")
                .exchange().expectStatus().isOk().expectHeader().valueEquals("Access-Control-Allow-Origin","http://localhost:3000");
        client.options().uri("/api/orders").header("Origin","https://evil.example").header("Access-Control-Request-Method","POST")
                .exchange().expectStatus().isForbidden();
        String bearer=token("CUSTOMER");
        client.post().uri("/api/orders").header("Authorization",bearer).contentType(MediaType.APPLICATION_JSON)
                .bodyValue("x".repeat(65537)).exchange().expectStatus().isEqualTo(413);
        client.post().uri("/api/orders").header("Authorization",bearer).contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(reactor.core.publisher.Flux.just(new byte[40000],new byte[40000]),byte[].class)
                .exchange().expectStatus().isEqualTo(413);
    }
    @Test void redisOutageFailsClosedAndRecoversWithoutRestart() throws Exception {
        String bearer=token("CUSTOMER");
        redisContainer.getDockerClient().pauseContainerCmd(redisContainer.getContainerId()).exec();
        try { client.get().uri("/api/orders").header("Authorization",bearer).exchange().expectStatus().isEqualTo(503); }
        finally { redisContainer.getDockerClient().unpauseContainerCmd(redisContainer.getContainerId()).exec(); }
        client.get().uri("/api/orders").header("Authorization",bearer).exchange().expectStatus().isOk();
    }
    static class JwtTokensTestSecret { static final String VALUE="phase-six-test-secret-".repeat(4); }
}

