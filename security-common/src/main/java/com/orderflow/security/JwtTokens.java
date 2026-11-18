package com.orderflow.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

/** The same signature and claims contract is used at every HTTP trust boundary. */
public class JwtTokens {
    private final String secret,issuer,audience;
    public JwtTokens(String secret,String issuer,String audience) {
        this.secret=secret; this.issuer=issuer; this.audience=audience;
    }
    private javax.crypto.SecretKey key() {
        if(secret.getBytes(StandardCharsets.UTF_8).length<64) throw new IdentityFailure(503,"IDENTITY_CONFIGURATION_REQUIRED");
        return Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }
    public String issue(UUID user,Set<String> roles) {
        Instant now=Instant.now();
        return Jwts.builder().subject(user.toString()).claim("roles",roles).issuer(issuer)
                .audience().add(audience).and().issuedAt(Date.from(now)).expiration(Date.from(now.plusSeconds(1800)))
                .id(UUID.randomUUID().toString()).signWith(key(),Jwts.SIG.HS512).compact();
    }
    public Caller verify(String authorization) {
        if(authorization==null || !authorization.startsWith("Bearer ")) throw new IdentityFailure(401,"UNAUTHENTICATED");
        var signingKey=key();
        try {
            var signed=Jwts.parser().verifyWith(signingKey).requireIssuer(issuer).requireAudience(audience)
                    .build().parseSignedClaims(authorization.substring(7));
            var claims=signed.getPayload();
            if(!"HS512".equals(signed.getHeader().getAlgorithm()) || claims.getIssuedAt()==null || claims.getExpiration()==null
                    || claims.getId()==null || claims.getIssuedAt().toInstant().isAfter(Instant.now())
                    || claims.getExpiration().getTime()-claims.getIssuedAt().getTime()!=1_800_000L)
                throw new IllegalArgumentException();
            UUID.fromString(claims.getId());
            Object raw=claims.get("roles");
            if(!(raw instanceof Collection<?> values) || values.isEmpty()
                    || values.stream().anyMatch(v->!Set.of("CUSTOMER","ADMIN").contains(v))) throw new IllegalArgumentException();
            Set<String> roles=new HashSet<>(); for(Object role:values) roles.add((String)role);
            return new Caller(UUID.fromString(claims.getSubject()),roles,claims.getId(),claims.getExpiration().toInstant());
        } catch(JwtException | IllegalArgumentException e) { throw new IdentityFailure(401,"UNAUTHENTICATED"); }
    }
}
