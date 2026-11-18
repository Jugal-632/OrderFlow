package com.orderflow.order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.order.dashboard.ServiceStatusClient;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
class ServiceStatusClientTest {
    @Test void cachedObservationBecomesStaleAndNeverFabricatesZeroCounts() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var calls=new AtomicInteger();
        String secret="test-private-internal-credential-32";
        server.createContext("/internal/ops/status",exchange->{
            calls.incrementAndGet();assertThat(exchange.getRequestHeaders().getFirst("X-Service-Credential")).isEqualTo(secret);
            byte[] body="{\"outbox\":{\"pending\":7},\"readiness\":\"UP\"}".getBytes();exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);exchange.close();
        });server.start();String url="http://127.0.0.1:"+server.getAddress().getPort();
        try(var client=new ServiceStatusClient(new ObjectMapper(),url,url,secret)) {
            Map<?,?> good=(Map<?,?>)client.snapshot().get("inventory");assertThat(good.get("status")).isEqualTo("AVAILABLE");
            client.snapshot();assertThat(calls.get()).isEqualTo(2);server.stop(0);
            await().atMost(Duration.ofSeconds(9)).untilAsserted(()->assertThat(((Map<?,?>)client.snapshot().get("inventory")).get("status")).isEqualTo("STALE"));
            Map<?,?> stale=(Map<?,?>)client.snapshot().get("inventory");assertThat(stale.get("data")).isEqualTo(good.get("data"));assertThat(stale.get("lastSuccessfulAt")).isEqualTo(good.get("lastSuccessfulAt"));
        } finally {server.stop(0);}
        try(var client=new ServiceStatusClient(new ObjectMapper(),url,url,secret)) {Map<?,?> unavailable=(Map<?,?>)client.snapshot().get("inventory");assertThat(unavailable.get("status")).isEqualTo("UNAVAILABLE");assertThat(unavailable.get("data")).isNull();}
    }
}
