package com.orderflow.gateway.config;

import com.orderflow.security.IdentityConfig;
import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.*;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.*;
import org.springframework.core.annotation.Order;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.*;

@Configuration
@Import(IdentityConfig.class)
public class GatewayConfiguration {
    @Bean RouteLocator routes(RouteLocatorBuilder builder,
            @Value("${services.auth:http://auth-service:8081}") String auth,
            @Value("${services.order:http://order-service:8082}") String order,
            @Value("${services.inventory:http://inventory-service:8083}") String inventory,
            @Value("${services.notification:http://notification-service:8084}") String notification) {
        return builder.routes()
                .route("login",r->r.path("/auth/login").and().method("POST").uri(auth))
                .route("logout",r->r.path("/auth/logout").and().method("POST").uri(auth))
                .route("me",r->r.path("/auth/me").and().method("GET").uri(auth))
                .route("orders",r->r.path("/api/orders","/api/orders/**","/api/dashboard/**").uri(order))
                .route("inventory",r->r.path("/api/inventory","/api/inventory/**").uri(inventory))
                .route("notifications",r->r.path("/api/notifications","/api/notifications/**").uri(notification))
                .route("order-ops",r->r.path("/api/ops/order/**").filters(f->f.rewritePath("/api/ops/order(?<tail>/?.*)","/api/ops${tail}")).uri(order))
                .route("inventory-ops",r->r.path("/api/ops/inventory/**").filters(f->f.rewritePath("/api/ops/inventory(?<tail>/?.*)","/api/ops${tail}")).uri(inventory))
                .route("notification-ops",r->r.path("/api/ops/notification/**").filters(f->f.rewritePath("/api/ops/notification(?<tail>/?.*)","/api/ops${tail}")).uri(notification))
                .build();
    }
    @Bean @Order(-400) CorsWebFilter cors(@Value("${gateway.cors.origins:http://localhost:3000,http://localhost:5173}") List<String> origins) {
        if(origins.contains("*")) throw new IllegalArgumentException("Explicit CORS origins required");
        var config=new CorsConfiguration();config.setAllowedOrigins(origins);
        config.setAllowedMethods(List.of("GET","POST","PUT","PATCH","DELETE","OPTIONS"));
        config.setAllowedHeaders(List.of("Authorization","Content-Type","Idempotency-Key","X-Correlation-Id"));
        config.setExposedHeaders(List.of("Location","X-Correlation-Id","Idempotent-Replayed","Retry-After"));
        config.setMaxAge(600L);
        var source=new UrlBasedCorsConfigurationSource();source.registerCorsConfiguration("/**",config);
        return new CorsWebFilter(source);
    }
}
