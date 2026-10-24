package com.orderflow.gateway.filter;

import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import reactor.core.publisher.Mono;
import java.util.UUID;

@Configuration
public class CorrelationIdFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Bean
    @Order(-1)
    public GlobalFilter correlationIdFilter() {
        return (exchange, chain) -> {
            String correlationId = exchange.getRequest()
                    .getHeaders()
                    .getFirst(CORRELATION_ID_HEADER);

            if (correlationId == null) {
                correlationId = UUID.randomUUID().toString();
            }

            final String finalCorrelationId = correlationId;

            return chain.filter(
                    exchange.mutate()
                            .request(r -> r.header(CORRELATION_ID_HEADER, finalCorrelationId))
                            .build()
            ).then(Mono.fromRunnable(() ->
                    exchange.getResponse()
                            .getHeaders()
                            .add(CORRELATION_ID_HEADER, finalCorrelationId)
            ));
        };
    }
}

