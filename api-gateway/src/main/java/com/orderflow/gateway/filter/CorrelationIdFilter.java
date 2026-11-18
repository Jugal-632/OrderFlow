package com.orderflow.gateway.filter;
import com.orderflow.security.ApiErrors;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.*;
import reactor.core.publisher.Mono;
@Component
public class CorrelationIdFilter implements WebFilter,Ordered {
    public int getOrder() { return -500; }
    public Mono<Void> filter(ServerWebExchange exchange,WebFilterChain chain) {
        String correlation=ApiErrors.correlation(exchange.getRequest().getHeaders().getFirst("X-Correlation-Id"));
        exchange.getResponse().getHeaders().set("X-Correlation-Id",correlation);
        String requestId=java.util.UUID.randomUUID().toString();long start=System.nanoTime();
        int management=exchange.getApplicationContext().getEnvironment().getProperty("local.management.port",Integer.class,-1);
        if(exchange.getRequest().getPath().value().startsWith("/actuator")) {
            if(exchange.getRequest().getLocalAddress().getPort()==management)return chain.filter(exchange);
            exchange.getResponse().setStatusCode(org.springframework.http.HttpStatus.NOT_FOUND);return exchange.getResponse().setComplete();
        }
        return Mono.deferContextual(context->chain.filter(exchange.mutate().request(request->request.headers(headers->{
            for(String name:new java.util.ArrayList<>(headers.headerNames()))if(name.equalsIgnoreCase("X-User-Id") || name.equalsIgnoreCase("X-Customer-Id")
                    || name.equalsIgnoreCase("X-Roles") || name.equalsIgnoreCase("X-User-Roles")
                    || name.equalsIgnoreCase("Forwarded") || name.toLowerCase(java.util.Locale.ROOT).startsWith("x-forwarded-"))headers.remove(name);
            headers.set("X-Correlation-Id",correlation);
        })).build()).doFinally(signal->{
            try(var scope=com.orderflow.telemetry.TraceContext.open(java.util.Map.of("correlationId",correlation,"requestId",requestId,"operation","http.request"))){
                int status=exchange.getResponse().getStatusCode()==null?200:exchange.getResponse().getStatusCode().value();
                org.slf4j.MDC.put("status",Integer.toString(status));org.slf4j.MDC.put("outcome",status>=500?"ERROR":status>=400?"REJECTED":"SUCCESS");org.slf4j.MDC.put("duration",Double.toString((System.nanoTime()-start)/1_000_000d));
                org.slf4j.LoggerFactory.getLogger("orderflow.http").info("request completed");
            }
        })).contextWrite(context->context.put("correlationId",correlation).put("requestId",requestId));
    }
}
