package com.orderflow.auth;

import com.orderflow.security.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"observability.readiness.enforce=false","management.server.port=0","jwt.secret=phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-phase-six-test-secret-"})
@AutoConfigureMockMvc
class AuthSecurityIT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static GenericContainer<?> redisContainer=new GenericContainer<>("redis:7.2.4-alpine").withExposedPorts(6379);
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url",postgres::getJdbcUrl);p.add("spring.datasource.username",postgres::getUsername);p.add("spring.datasource.password",postgres::getPassword);
        p.add("spring.data.redis.host",redisContainer::getHost);p.add("spring.data.redis.port",()->redisContainer.getMappedPort(6379));
    }
    @Autowired MockMvc http; @Autowired JdbcTemplate jdbc; @Autowired PasswordEncoder encoder;
    @Autowired ObjectMapper json; @Autowired JwtTokens tokens; @Autowired StringRedisTemplate redis;
    UUID user;
    @BeforeEach void seed() {
        jdbc.execute("TRUNCATE auth_schema.users CASCADE");user=UUID.randomUUID();
        jdbc.update("INSERT INTO auth_schema.users(id,email,password_hash) VALUES (?,?,?)",user,"customer@example.com",encoder.encode("test-only-password"));
        jdbc.update("INSERT INTO auth_schema.user_roles(user_id,role) VALUES (?,'CUSTOMER')",user);
    }
    String login() throws Exception {
        var response=http.perform(post("/auth/login").contentType("application/json")
                .content("{\"email\":\"customer@example.com\",\"password\":\"test-only-password\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.expiresIn").value(1800))
                .andExpect(jsonPath("$.userId").value(user.toString())).andExpect(jsonPath("$.roles[0]").value("CUSTOMER")).andReturn();
        return "Bearer "+json.readTree(response.getResponse().getContentAsString()).get("accessToken").asText();
    }
    @Test void loginMeLogoutAndRemainingTtl() throws Exception {
        String bearer=login();var caller=tokens.verify(bearer);assertThat(caller.userId()).isEqualTo(user);
        http.perform(get("/auth/me").header("Authorization",bearer)).andExpect(status().isOk()).andExpect(jsonPath("$.userId").value(user.toString()));
        http.perform(post("/auth/logout").header("Authorization",bearer)).andExpect(status().isNoContent());
        assertThat(redis.getExpire(IdentityVerifier.REVOKED+caller.tokenId())).isBetween(1790L,1800L);
        http.perform(get("/auth/me").header("Authorization",bearer)).andExpect(status().isUnauthorized());
        assertThatThrownBy(()->jdbc.update("INSERT INTO auth_schema.user_roles(user_id,role) VALUES (?,'CUSTOMER')",user))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
    }
    @Test void invalidCredentialsAndMissingTokenUseSafeErrors() throws Exception {
        http.perform(post("/auth/login").contentType("application/json").content("{\"email\":\"missing@example.com\",\"password\":\"bad\"}"))
                .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.code").value("INVALID_CREDENTIALS"))
                .andExpect(jsonPath("$.timestamp").exists()).andExpect(jsonPath("$.fieldErrors").exists());
        http.perform(get("/auth/me")).andExpect(status().isUnauthorized());
    }
    @Test void logoutCannotSucceedDuringRedisOutageAndRecovers() throws Exception {
        String bearer=login();
        redisContainer.getDockerClient().pauseContainerCmd(redisContainer.getContainerId()).exec();
        try { http.perform(post("/auth/logout").header("Authorization",bearer)).andExpect(status().isServiceUnavailable()); }
        finally { redisContainer.getDockerClient().unpauseContainerCmd(redisContainer.getContainerId()).exec(); }
        http.perform(get("/auth/me").header("Authorization",bearer)).andExpect(status().isOk());
        http.perform(post("/auth/logout").header("Authorization",bearer)).andExpect(status().isNoContent());
    }
}

