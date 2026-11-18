package com.orderflow.security;

import org.springframework.context.annotation.*;
import org.springframework.beans.factory.annotation.Value;

@Configuration(proxyBeanMethods=false)
public class IdentityConfig {
    @Bean public JwtTokens jwtTokens(@Value("${jwt.secret:${JWT_SECRET:}}") String secret,
            @Value("${jwt.issuer:orderflow-auth}") String issuer,@Value("${jwt.audience:orderflow-api}") String audience) {
        return new JwtTokens(secret,issuer,audience);
    }
}
