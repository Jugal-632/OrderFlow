package com.orderflow.security;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record Caller(UUID userId, Set<String> roles, String tokenId, Instant expiresAt) {
    public Caller { roles=Set.copyOf(roles); }
    public boolean admin() { return roles.contains("ADMIN"); }
}
