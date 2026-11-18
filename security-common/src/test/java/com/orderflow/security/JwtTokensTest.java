package com.orderflow.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class JwtTokensTest {
    static final String SECRET="phase-six-test-secret-".repeat(4);
    JwtTokens tokens=new JwtTokens(SECRET,"orderflow-auth","orderflow-api");
    String token(String issuer,String audience,Instant issued,String subject) {
        return Jwts.builder().subject(subject).issuer(issuer).audience().add(audience).and()
                .claim("roles",List.of("CUSTOMER")).id(UUID.randomUUID().toString())
                .issuedAt(Date.from(issued)).expiration(Date.from(issued.plusSeconds(1800)))
                .signWith(Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8)),Jwts.SIG.HS512).compact();
    }
    @Test void issuesUuidRolesAndThirtyMinuteClaims() {
        UUID user=UUID.randomUUID();Instant before=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        String issued=tokens.issue(user,Set.of("CUSTOMER"));Instant after=Instant.now();var caller=tokens.verify("Bearer "+issued);
        assertThat(caller.userId()).isEqualTo(user);assertThat(caller.roles()).containsExactly("CUSTOMER");
        var claims=Jwts.parser().verifyWith(Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8))).build().parseSignedClaims(issued).getPayload();
        assertThat(claims.getIssuedAt().toInstant()).isBetween(before,after);
        assertThat(Duration.between(claims.getIssuedAt().toInstant(),caller.expiresAt())).isEqualTo(Duration.ofMinutes(30));
        assertThat(UUID.fromString(caller.tokenId())).isNotNull();
    }
    @Test void rejectsMissingInvalidExpiredIssuerAudienceAndLegacyEmail() {
        String user=UUID.randomUUID().toString();
        for(String header:Arrays.asList(null,"Bearer bad","Basic abc",
                "Bearer "+token("bad","orderflow-api",Instant.now(),user),
                "Bearer "+token("orderflow-auth","bad",Instant.now(),user),
                "Bearer "+token("orderflow-auth","orderflow-api",Instant.now().minusSeconds(1900),user),
                "Bearer "+token("orderflow-auth","orderflow-api",Instant.now(),"test@example.com"),
                "Bearer "+new JwtTokens("different-secret-".repeat(5),"orderflow-auth","orderflow-api").issue(UUID.randomUUID(),Set.of("CUSTOMER"))))
            assertThatThrownBy(()->tokens.verify(header)).isInstanceOfSatisfying(IdentityFailure.class,e->assertThat(e.status).isEqualTo(401));
    }
    @Test void revocationFailsClosedAndRecovers() {
        var redis=mock(org.springframework.data.redis.core.StringRedisTemplate.class);
        var verifier=new IdentityVerifier(tokens,redis);String bearer="Bearer "+tokens.issue(UUID.randomUUID(),Set.of("ADMIN"));
        String key=IdentityVerifier.REVOKED+tokens.verify(bearer).tokenId();
        when(redis.hasKey(key)).thenReturn(true).thenThrow(new org.springframework.data.redis.RedisConnectionFailureException("down")).thenReturn(false);
        assertThatThrownBy(()->verifier.verify(bearer)).isInstanceOfSatisfying(IdentityFailure.class,e->assertThat(e.status).isEqualTo(401));
        assertThatThrownBy(()->verifier.verify(bearer)).isInstanceOfSatisfying(IdentityFailure.class,e->assertThat(e.status).isEqualTo(503));
        assertThat(verifier.verify(bearer).admin()).isTrue();
    }
}
