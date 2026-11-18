package com.orderflow.order.dashboard;
import com.orderflow.ops.OpsService;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {
    private final JdbcTemplate jdbc;private final OpsService ops;private final ServiceStatusClient services;private final KafkaTopics kafka;private final TestEvidence evidence;
    private final org.springframework.boot.health.actuate.endpoint.HealthEndpoint health;
    public DashboardController(JdbcTemplate jdbc,OpsService ops,ServiceStatusClient services,KafkaTopics kafka,TestEvidence evidence,org.springframework.boot.health.actuate.endpoint.HealthEndpoint health) {this.jdbc=jdbc;this.ops=ops;this.services=services;this.kafka=kafka;this.evidence=evidence;this.health=health;}
    @GetMapping("/overview/stats") public Object stats() {
        var counts=new LinkedHashMap<String,Long>();for(String state:List.of("PENDING","CONFIRMED","FAILED","CANCEL_PENDING","CANCELLED"))counts.put(state,0L);
        jdbc.queryForList("SELECT status,count(*) AS total FROM order_schema.orders GROUP BY status").forEach(row->counts.put(row.get("status").toString(),((Number)row.get("total")).longValue()));
        var result=new LinkedHashMap<String,Object>();result.put("observedAt",Instant.now().toString());result.put("counts",counts);
        result.put("confirmationLatency",jdbc.queryForMap("SELECT avg(EXTRACT(EPOCH FROM h.recorded_at-o.created_at)) AS mean_seconds,count(*) AS samples FROM order_schema.orders o JOIN order_schema.order_history h ON h.order_id=o.id AND h.new_status='CONFIRMED'"));
        result.put("order",ops.stats());result.put("services",services.snapshot());return result;
    }
    @GetMapping("/services/health") public Object health() {
        var data=new LinkedHashMap<>(ops.stats());var state=health.health();data.put("readiness",state.getStatus().getCode());
        var dependencies=new TreeMap<String,String>();if(state instanceof org.springframework.boot.health.actuate.endpoint.CompositeHealthDescriptor composite) composite.getComponents().forEach((k,v)->dependencies.put(k,v.getStatus().getCode()));data.put("dependencies",dependencies);
        var result=new LinkedHashMap<>(services.snapshot());result.put("order",Map.of("status","AVAILABLE","lastSuccessfulAt",Instant.now().toString(),"data",data));return result;
    }
    @GetMapping("/kafka/topics") public Object topics() {return kafka.snapshot();}
    @GetMapping("/test-runs") public Object evidence() {return evidence.reports();}
}
