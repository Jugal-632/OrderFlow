package com.orderflow.gateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.security.*;
import java.time.Duration;
import java.util.*;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.*;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.*;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.*;
import reactor.core.publisher.*;

/** Nonblocking authentication and distributed per-principal token buckets. */
@Component
public class GatewayBoundary implements WebFilter,Ordered {
    private final JwtTokens tokens; private final ReactiveStringRedisTemplate redis; private final ObjectMapper json;
    private static final RedisScript<List> BUCKET=RedisScript.of("""
        local time=redis.call('TIME')
        local now=tonumber(time[1])+tonumber(time[2])/1000000
        local rate=tonumber(ARGV[1])/60
        local capacity=tonumber(ARGV[2])
        local old=redis.call('HMGET',KEYS[1],'tokens','at')
        local available=math.min(capacity,tonumber(old[1] or capacity)+math.max(0,now-tonumber(old[2] or now))*rate)
        local allowed=0
        local retry=0
        if available>=1 then available=available-1 allowed=1 else retry=math.ceil((1-available)/rate) end
        redis.call('HSET',KEYS[1],'tokens',available,'at',now)
        redis.call('EXPIRE',KEYS[1],120)
        return {allowed,retry}
        """,List.class);
    public GatewayBoundary(JwtTokens tokens,ReactiveStringRedisTemplate redis,ObjectMapper json) {
        this.tokens=tokens;this.redis=redis;this.json=json;
    }
    @Override public int getOrder() { return -300; }
    @Override public Mono<Void> filter(ServerWebExchange exchange,WebFilterChain chain) {
        String path=exchange.getRequest().getPath().value();
        if(path.startsWith("/actuator")) return chain.filter(exchange); // The outer correlation filter restricts this to the private management listener.
        boolean login=path.equals("/auth/login") && exchange.getRequest().getMethod()==HttpMethod.POST;
        Mono<Caller> identity;
        if(login) identity=Mono.empty();
        else {
            try { identity=Mono.just(tokens.verify(exchange.getRequest().getHeaders().getFirst("Authorization"))); }
            catch(IdentityFailure e) { return error(exchange,e.status,e.code); }
        }
        Mono<String> key=login?Mono.just("login:"+exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()):
                identity.flatMap(caller->redis.hasKey(IdentityVerifier.REVOKED+caller.tokenId()).timeout(Duration.ofSeconds(2))
                        .onErrorMap(e->new IdentityFailure(503,"IDENTITY_UNAVAILABLE"))
                        .map(revoked->{
                            if(revoked) throw new IdentityFailure(401,"UNAUTHENTICATED");
                            boolean order=path.equals("/api/orders") || path.startsWith("/api/orders/");
                            if(path.startsWith("/api/") && !order && !caller.admin()) throw new IdentityFailure(403,"FORBIDDEN");
                            return caller.userId().toString();
                        }));
        boolean read=exchange.getRequest().getMethod()==HttpMethod.GET || exchange.getRequest().getMethod()==HttpMethod.HEAD;
        int rate=login?10:read?300:60,burst=login?10:read?50:20;
        return key.flatMap(k->redis.execute(BUCKET,List.of("orderflow:rate:"+k+":"+(login?"login":read?"read":"write")),
                        List.of(String.valueOf(rate),String.valueOf(burst))).single().timeout(Duration.ofSeconds(2))
                        .onErrorMap(e->new IdentityFailure(503,"RATE_LIMIT_UNAVAILABLE")))
                .flatMap(result->{
                    if(((Number)result.get(0)).intValue()==0) {
                        exchange.getResponse().getHeaders().set("Retry-After",result.get(1).toString());
                        return error(exchange,429,"RATE_LIMIT_EXCEEDED");
                    }
                    if(exchange.getRequest().getHeaders().getContentLength()>65536) return error(exchange,413,"REQUEST_TOO_LARGE");
                    // Enforce the same limit for chunked requests, before forwarding any bytes.
                    return DataBufferUtils.join(exchange.getRequest().getBody(),65536)
                            .map(buffer->{byte[] bytes=new byte[buffer.readableByteCount()];buffer.read(bytes);DataBufferUtils.release(buffer);return bytes;})
                            .defaultIfEmpty(new byte[0]).flatMap(bytes->{
                                var request=new ServerHttpRequestDecorator(exchange.getRequest()) {
                                    @Override public Flux<DataBuffer> getBody() { return Flux.defer(()->Flux.just(exchange.getResponse().bufferFactory().wrap(bytes))); }
                                };
                                return chain.filter(exchange.mutate().request(request).build());
                            });
                }).onErrorResume(IdentityFailure.class,e->error(exchange,e.status,e.code))
                .onErrorResume(DataBufferLimitException.class,e->error(exchange,413,"REQUEST_TOO_LARGE"));
    }
    private Mono<Void> error(ServerWebExchange exchange,int status,String code) {
        try(var auditScope=com.orderflow.telemetry.TraceContext.open(java.util.Map.of("status",Integer.toString(status),"outcome",code,"operation","access.denied"))) {
        if(exchange.getRequest().getPath().value().startsWith("/api/ops/") && (status==401 || status==403))
            org.slf4j.LoggerFactory.getLogger("orderflow.access.audit").warn("Replay access rejected status={} correlationId={}",status,exchange.getResponse().getHeaders().getFirst("X-Correlation-Id"));
        }
        exchange.getResponse().setStatusCode(HttpStatusCode.valueOf(status));
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        if(status==503) exchange.getResponse().getHeaders().set("Retry-After","1");
        try { return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(
                json.writeValueAsBytes(ApiErrors.body(code,exchange.getResponse().getHeaders().getFirst("X-Correlation-Id")))))); }
        catch(java.io.IOException e) { return Mono.error(e); }
    }
}
