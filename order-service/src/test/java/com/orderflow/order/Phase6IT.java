package com.orderflow.order;

import com.orderflow.security.*;
import com.orderflow.order.dto.OrderRequest;
import com.orderflow.order.service.OrderService;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","order.relay.enabled=false","spring.kafka.listener.auto-startup=false","spring.kafka.admin.auto-create=false",
        "spring.flyway.locations=filesystem:src/main/resources/db/migration","jwt.secret=phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-"})
@AutoConfigureMockMvc
class Phase6IT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static GenericContainer<?> redis=new GenericContainer<>("redis:7.2.4-alpine").withExposedPorts(6379);
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url",postgres::getJdbcUrl);p.add("spring.datasource.username",postgres::getUsername);p.add("spring.datasource.password",postgres::getPassword);
        p.add("spring.data.redis.host",redis::getHost);p.add("spring.data.redis.port",()->redis.getMappedPort(6379));
    }
    @Autowired MockMvc http; @Autowired JdbcTemplate jdbc; @Autowired OrderService orders; @Autowired JwtTokens tokens;
    UUID owner,other,id;String customer,admin;
    @BeforeEach void seed() {
        jdbc.execute("TRUNCATE order_schema.orders,order_schema.order_history,order_schema.outbox_events,order_schema.request_idempotency CASCADE");
        owner=UUID.randomUUID();other=UUID.randomUUID();
        var request=new OrderRequest();request.setCustomerId(owner);request.setProductId(UUID.randomUUID());request.setQuantity(1);
        id=orders.createOrder(request).getId();
        customer="Bearer "+tokens.issue(owner,Set.of("CUSTOMER"));admin="Bearer "+tokens.issue(other,Set.of("ADMIN"));
    }
    @Test void ownerAndAdminCanReadButOtherCustomerGets404ForOrderAndTimeline() throws Exception {
        String stranger="Bearer "+tokens.issue(other,Set.of("CUSTOMER"));
        for(String suffix:List.of("","/timeline")) {
            http.perform(get("/api/orders/"+id+suffix).header("Authorization",customer)).andExpect(status().isOk());
            http.perform(get("/api/orders/"+id+suffix).header("Authorization",admin)).andExpect(status().isOk());
            http.perform(get("/api/orders/"+id+suffix).header("Authorization",stranger)).andExpect(status().isNotFound());
        }
        http.perform(get("/api/orders").header("Authorization",stranger)).andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
        http.perform(get("/api/orders").header("Authorization",customer)).andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(id.toString()));
        http.perform(get("/api/orders").header("Authorization",admin)).andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(1));
    }
    @Test void directServiceAccessRequiresJwtAndAdminEndpointsRejectCustomer() throws Exception {
        http.perform(get("/api/orders/"+id).header("X-User-Id",owner)).andExpect(status().isUnauthorized());
        http.perform(get("/api/orders/"+id).header("Authorization","Bearer invalid")).andExpect(status().isUnauthorized());
        for(String path:List.of("/api/dashboard/stats","/api/ops/dead-letters"))
            http.perform(get(path).header("Authorization",customer)).andExpect(status().isForbidden());
        http.perform(get("/api/orders?size=101").header("Authorization",customer)).andExpect(status().isBadRequest());
        http.perform(get("/api/orders?page=-1").header("Authorization",customer)).andExpect(status().isBadRequest());
    }
    @Test void createRejectsClientIdentityAndPreservesAuthenticatedScope() throws Exception {
        String payload="{\"productId\":\""+UUID.randomUUID()+"\",\"quantity\":1}";
        http.perform(post("/api/orders").header("Authorization",customer).header("Idempotency-Key","phase6")
                .contentType("application/json").content(payload)).andExpect(status().isCreated()).andExpect(jsonPath("$.customerId").value(owner.toString()));
        http.perform(post("/api/orders").header("Authorization",customer).header("Idempotency-Key","spoof")
                .contentType("application/json").content(payload.replace("}",",\"customerId\":\""+other+"\"}")))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }
}

