package com.orderflow.ops;

import io.micrometer.core.instrument.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

/** Queries only the owning schema. Unavailable observations are NaN, never fabricated zeros. */
public class OperationalMetrics {
    private final String schema;private final JdbcTemplate jdbc;private final Map<String,AtomicReference<Double>> values=new LinkedHashMap<>();
    public OperationalMetrics(String schema,JdbcTemplate jdbc,MeterRegistry meters){
        this.schema=schema;this.jdbc=jdbc;
        for(String name:List.of("outbox.pending","outbox.blocked","outbox.oldest.seconds","outbox.retries","deadletters.open","replay.succeeded","replay.failed","business.rejections","orders.stuck.seconds")){
            var value=new AtomicReference<>(Double.NaN);values.put(name,value);Gauge.builder("orderflow."+name,value,AtomicReference::get).register(meters);
        }
    }
    private void set(String key,Number number){values.get(key).set(number.doubleValue());}
    @Scheduled(fixedDelay=5000) public void observe(){
        try{
            var outbox=jdbc.queryForMap("SELECT count(*) FILTER(WHERE status='PENDING') AS pending,count(*) FILTER(WHERE status='BLOCKED') AS blocked,COALESCE(EXTRACT(EPOCH FROM now()-min(created_at) FILTER(WHERE status='PENDING')),0) AS age,COALESCE(sum(retry_count),0) AS retries FROM "+schema+".outbox_events");
            set("outbox.pending",(Number)outbox.get("pending"));set("outbox.blocked",(Number)outbox.get("blocked"));set("outbox.oldest.seconds",(Number)outbox.get("age"));set("outbox.retries",(Number)outbox.get("retries"));
            set("deadletters.open",jdbc.queryForObject("SELECT count(*) FROM "+schema+".dead_letters WHERE status IN ('OPEN','REPLAY_QUEUED')",Long.class));
            for(String result:List.of("SUCCEEDED","FAILED"))set("replay."+result.toLowerCase(Locale.ROOT),jdbc.queryForObject("SELECT count(*) FROM "+schema+".replay_requests WHERE result=?",Long.class,result));
            if(schema.equals("order_schema")){
                set("orders.stuck.seconds",jdbc.queryForObject("SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(updated_at)),0) FROM order_schema.orders WHERE status IN ('PENDING','CANCEL_PENDING')",Double.class));
                set("business.rejections",jdbc.queryForObject("SELECT count(*) FROM order_schema.orders WHERE status='FAILED'",Long.class));
            }else if(schema.equals("inventory_schema"))set("business.rejections",jdbc.queryForObject("SELECT count(*) FROM inventory_schema.inventory_decisions WHERE outcome='REJECTED'",Long.class));
        }catch(Exception e){values.values().forEach(v->v.set(Double.NaN));}
    }
}
