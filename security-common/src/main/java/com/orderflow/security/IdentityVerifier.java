package com.orderflow.security;

import org.springframework.data.redis.core.StringRedisTemplate;

public class IdentityVerifier {
    public static final String REVOKED="orderflow:revoked:";
    private final JwtTokens tokens;
    private final StringRedisTemplate redis;
    public IdentityVerifier(JwtTokens tokens,StringRedisTemplate redis) { this.tokens=tokens;this.redis=redis; }
    public Caller verify(String authorization) {
        Caller caller=tokens.verify(authorization);
        boolean revoked;
        try { revoked=Boolean.TRUE.equals(redis.hasKey(REVOKED+caller.tokenId())); }
        catch(org.springframework.dao.DataAccessException e) {
            throw new IdentityFailure(503,"IDENTITY_UNAVAILABLE");
        }
        if(revoked) throw new IdentityFailure(401,"UNAUTHENTICATED");
        return caller;
    }
}
