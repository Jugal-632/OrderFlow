package com.orderflow.auth.service;
import com.orderflow.auth.dto.*;
import com.orderflow.auth.repository.UserRepository;
import com.orderflow.security.*;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
@Service
@RequiredArgsConstructor
public class AuthService {
    private final UserRepository users;
    private final PasswordEncoder passwords;
    private final JwtTokens tokens;
    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    public LoginResponse login(LoginRequest request) {
        if(request.getEmail()==null || request.getPassword()==null) throw new IdentityFailure(400,"INVALID_REQUEST");
        var user=users.findByEmail(request.getEmail()).orElseThrow(()->new IdentityFailure(401,"INVALID_CREDENTIALS"));
        if(!passwords.matches(request.getPassword(),user.getPasswordHash())) throw new IdentityFailure(401,"INVALID_CREDENTIALS");
        Set<String> roles=new HashSet<>(jdbc.queryForList("SELECT role FROM auth_schema.user_roles WHERE user_id=?",String.class,user.getId()));
        if(roles.isEmpty()) throw new IdentityFailure(403,"NO_ROLE_ASSIGNED");
        return new LoginResponse(tokens.issue(user.getId(),roles),1800,user.getId(),roles);
    }
    public void logout(Caller caller) {
        Duration ttl=Duration.between(Instant.now(),caller.expiresAt());
        if(ttl.isNegative() || ttl.isZero()) throw new IdentityFailure(401,"UNAUTHENTICATED");
        try { redis.opsForValue().set(IdentityVerifier.REVOKED+caller.tokenId(),"1",ttl); }
        catch(org.springframework.dao.DataAccessException e) { throw new IdentityFailure(503,"IDENTITY_UNAVAILABLE"); }
    }
}
