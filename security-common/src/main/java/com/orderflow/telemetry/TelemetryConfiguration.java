package com.orderflow.telemetry;

import io.micrometer.context.ContextRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Hooks;
import org.slf4j.MDC;

@AutoConfiguration
public class TelemetryConfiguration {
    @Bean public MeterFilter boundedMeters() {
        return MeterFilter.deny(id->id.getTags().stream().anyMatch(tag->java.util.Set.of("orderId","eventId","requestId","correlationId","customerId","idempotencyKey").contains(tag.getKey())));
    }
    @Bean public Object reactorContextBridge() {
        for(String key:java.util.List.of("correlationId","requestId")) ContextRegistry.getInstance().registerThreadLocalAccessor(key,()->MDC.get(key),value->MDC.put(key,value),()->MDC.remove(key));
        Hooks.enableAutomaticContextPropagation();return new Object();
    }
}
