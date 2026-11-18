package com.orderflow.inventory;
import com.orderflow.security.*;
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
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","spring.flyway.enabled=true","inventory.relay.enabled=false","spring.kafka.listener.auto-startup=false","spring.kafka.admin.auto-create=false",
        "jwt.secret=phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-"})
@AutoConfigureMockMvc
class HttpSecurityIT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static GenericContainer<?> redis=new GenericContainer<>("redis:7.2.4-alpine").withExposedPorts(6379);
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url",postgres::getJdbcUrl);p.add("spring.datasource.username",postgres::getUsername);p.add("spring.datasource.password",postgres::getPassword);
        p.add("spring.data.redis.host",redis::getHost);p.add("spring.data.redis.port",()->redis.getMappedPort(6379));
    }
    @Autowired MockMvc http; @Autowired JdbcTemplate jdbc; @Autowired JwtTokens tokens;
    UUID id;String customer,admin;
    @BeforeEach void seed() {
        id=UUID.randomUUID();
        jdbc.execute("TRUNCATE inventory_schema.inventory CASCADE");
        jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES (?,'Phase6',10)",id);
        customer="Bearer "+tokens.issue(UUID.randomUUID(),Set.of("CUSTOMER"));
        admin="Bearer "+tokens.issue(UUID.randomUUID(),Set.of("ADMIN"));
    }
    @Test void requiresJwtAndAdminEvenWithoutGateway() throws Exception {
        http.perform(get("/api/inventory")).andExpect(status().isUnauthorized());
        http.perform(get("/api/inventory").header("Authorization","Bearer invalid")).andExpect(status().isUnauthorized());
        http.perform(get("/api/inventory").header("Authorization",customer)).andExpect(status().isForbidden());
        http.perform(get("/api/inventory").header("Authorization",admin)).andExpect(status().isOk()).andExpect(jsonPath("$[0].product_id").value(id.toString()));
    }
    @Test void boundedPaginationAndFilters() throws Exception {
        http.perform(get("/api/inventory?size=101").header("Authorization",admin)).andExpect(status().isBadRequest());
        http.perform(get("/api/inventory?page=-1").header("Authorization",admin)).andExpect(status().isBadRequest());
        http.perform(get("/api/inventory?page=1&size=1").header("Authorization",admin)).andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
        http.perform(get("/api/inventory/"+id+"/reservations").header("Authorization",admin)).andExpect(status().isOk());
    }
}

