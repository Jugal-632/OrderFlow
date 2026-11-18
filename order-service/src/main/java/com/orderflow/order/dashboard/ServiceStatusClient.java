package com.orderflow.order.dashboard;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ServiceStatusClient implements AutoCloseable {
    private final Map<String,URI> targets;private final String credential;private final ObjectMapper json;
    private final ExecutorService workers=Executors.newFixedThreadPool(2);
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final Map<String,Map<String,Object>> previous=new HashMap<>();
    private Map<String,Object> cached=Map.of();private Instant expires=Instant.MIN;
    public ServiceStatusClient(ObjectMapper json,@Value("${services.inventory:http://inventory-service:8083}") String inventory,
            @Value("${services.notification:http://notification-service:8084}") String notification,
            @Value("${ops.internal-token:${INTERNAL_SERVICE_TOKEN:}}") String credential) {
        this.json=json;this.credential=credential;
        targets=Map.of("inventory",URI.create(inventory+"/internal/ops/status"),"notification",URI.create(notification+"/internal/ops/status"));
    }
    public synchronized Map<String,Object> snapshot() {
        if(Instant.now().isBefore(expires))return cached;
        var pending=new LinkedHashMap<String,CompletableFuture<Map<String,Object>>>();
        targets.forEach((name,url)->pending.put(name,CompletableFuture.supplyAsync(()->fetch(url),workers)));
        var result=new LinkedHashMap<String,Object>();
        pending.forEach((name,future)->{
            var component=new LinkedHashMap<String,Object>();component.put("observedAt",Instant.now().toString());
            try {var data=future.join();previous.put(name,Map.of("data",data,"lastSuccessfulAt",Instant.now().toString()));component.putAll(previous.get(name));component.put("status","AVAILABLE");}
            catch(CompletionException e) {component.put("status",previous.containsKey(name)?"STALE":"UNAVAILABLE");component.put("data",null);component.put("lastSuccessfulAt",null);if(previous.containsKey(name))component.putAll(previous.get(name));}
            result.put(name,component);
        });
        cached=result;expires=Instant.now().plusSeconds(5);return cached;
    }
    private Map<String,Object> fetch(URI target) {
        if(credential.length()<32)throw new IllegalStateException("Service identity unconfigured");
        try {
            var response=http.send(HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(2)).header("X-Service-Credential",credential).GET().build(),HttpResponse.BodyHandlers.ofString());
            if(response.statusCode()!=200 || response.body().length()>65536)throw new IllegalStateException("Dependency unavailable");
            return json.readValue(response.body(),new TypeReference<Map<String,Object>>(){});
        } catch(InterruptedException e) {Thread.currentThread().interrupt();throw new IllegalStateException(e);}
        catch(Exception e) {throw new IllegalStateException("Dependency unavailable",e);}
    }
    @jakarta.annotation.PreDestroy public void close() {workers.shutdownNow();}
}
