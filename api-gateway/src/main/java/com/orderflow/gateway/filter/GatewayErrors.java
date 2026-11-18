package com.orderflow.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.security.ApiErrors;
import org.springframework.core.Ordered;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.server.*;
import reactor.core.publisher.Mono;

@Component
public class GatewayErrors implements WebExceptionHandler,Ordered {
    private final ObjectMapper json;
    public GatewayErrors(ObjectMapper json) { this.json=json; }
    public int getOrder() { return -2; }
    public Mono<Void> handle(ServerWebExchange exchange,Throwable error) {
        if(exchange.getResponse().isCommitted()) return Mono.error(error);
        int status=error instanceof ResponseStatusException e?e.getStatusCode().value():502;
        String code=status==404?"NOT_FOUND":status==413?"REQUEST_TOO_LARGE":"GATEWAY_REQUEST_FAILED";
        exchange.getResponse().setStatusCode(HttpStatusCode.valueOf(status));exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        try { return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(json.writeValueAsBytes(
                ApiErrors.body(code,ApiErrors.correlation(exchange.getResponse().getHeaders().getFirst("X-Correlation-Id"))))))); }
        catch(java.io.IOException e) { return Mono.error(e); }
    }
}
