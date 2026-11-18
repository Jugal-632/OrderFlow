package com.orderflow.telemetry;
import ch.qos.logback.classic.*;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import static org.assertj.core.api.Assertions.*;
class TelemetryTest {
    @Test void identityTagsAreRejectedWhileBoundedDimensionsRemainAvailable(){
        var meters=new io.micrometer.core.instrument.simple.SimpleMeterRegistry();meters.config().meterFilter(new TelemetryConfiguration().boundedMeters());
        var denied=meters.counter("unsafe","orderId",UUID.randomUUID().toString());denied.increment();assertThat(denied.count()).isZero();
        meters.counter("safe","outcome","ERROR").increment();assertThat(meters.get("safe").counter().count()).isEqualTo(1);meters.close();
    }
    @Test void nestedScopesAndReactorThreadsRestoreCallerContext(){
        String old=UUID.randomUUID().toString(),next=UUID.randomUUID().toString();MDC.put("correlationId",old);
        try{
            try(var scope=TraceContext.correlation(next)){assertThat(MDC.get("correlationId")).isEqualTo(next);}
            assertThat(MDC.get("correlationId")).isEqualTo(old);new TelemetryConfiguration().reactorContextBridge();
            String observed=Mono.delay(java.time.Duration.ofMillis(1)).publishOn(Schedulers.boundedElastic()).map(n->MDC.get("correlationId")).contextWrite(c->c.put("correlationId",next)).block();
            assertThat(observed).isEqualTo(next);assertThat(MDC.get("correlationId")).isEqualTo(old);
        }finally{MDC.clear();}
    }
    @Test void arbitraryMessagesThrowableAndUnknownMdcCannotLeakSecrets() throws Exception {
        var context=new LoggerContext();context.putProperty("service","test-service");context.putProperty("environment","test");
        var encoder=new SafeJsonEncoder();encoder.setContext(context);encoder.start();
        var event=new LoggingEvent("test",context.getLogger("test"),Level.ERROR,"password=secret jwt=token jdbc:postgresql://private",new RuntimeException("sensitive-password"),new Object[]{"secret"});
        event.setMDCPropertyMap(Map.of("password","secret","correlationId","token","eventId",UUID.randomUUID().toString(),"outcome","ERROR"));
        String raw=new String(encoder.encode(event),java.nio.charset.StandardCharsets.UTF_8);
        assertThat(raw).doesNotContain("secret","password","token","jdbc:postgresql","sensitive");var json=new ObjectMapper().readTree(raw);
        assertThat(json.path("service").asText()).isEqualTo("test-service");assertThat(json.path("correlationId").isNull()).isTrue();assertThat(json.path("eventId").asText()).hasSize(36);
    }
}

