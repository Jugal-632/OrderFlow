package com.orderflow.ops;

import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class OpsRelay {
    private final DataSource data;private final JdbcTemplate jdbc;private final KafkaTemplate<String,String> kafka;
    private final OpsService ops;private final TransactionTemplate tx;
    public OpsRelay(DataSource data,JdbcTemplate jdbc,KafkaTemplate<String,String> kafka,OpsService ops,PlatformTransactionManager manager) {
        this.data=data;this.jdbc=jdbc;this.kafka=kafka;this.ops=ops;tx=new TransactionTemplate(manager);tx.setTimeout(5);
    }
    @Scheduled(fixedDelay=1000) public void publish() throws Exception {
        long lock=0x4F50530000000000L+ops.service.hashCode();
        try(var session=data.getConnection()) {
            boolean locked=false;
            try {
                try(var statement=session.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                    statement.setLong(1,lock);try(var result=statement.executeQuery()) {result.next();locked=result.getBoolean(1);}
                }
                if(!locked)return;
                for(var row:jdbc.queryForList("SELECT * FROM "+ops.schema+".outbox_events WHERE event_type='ops.replay' AND status='PENDING' AND next_attempt_at<=now() ORDER BY created_at,id LIMIT 100")) {
                    try(var telemetry=com.orderflow.telemetry.Operation.event("outbox.replay",row.get("payload").toString(),ops.service+".replay")) {
                        var wrapper=ops.decode(row.get("payload").toString());
                        if(!wrapper.replayRequestId().equals(row.get("id")) || !wrapper.deadLetterId().equals(row.get("aggregate_id")) || !(ops.service+".replay").equals(row.get("topic"))) throw new IllegalStateException("Replay metadata mismatch");
                        kafka.send(ops.service+".replay",wrapper.deadLetterId().toString(),row.get("payload").toString()).get(10,TimeUnit.SECONDS);
                        tx.executeWithoutResult(s->jdbc.update("UPDATE "+ops.schema+".outbox_events SET status='PUBLISHED',published_at=now(),last_error=NULL WHERE id=?",row.get("id")));
                        telemetry.success();
                    } catch(Exception error) {
                        if(error instanceof InterruptedException) {Thread.currentThread().interrupt();return;}
                        tx.executeWithoutResult(s->jdbc.update("UPDATE "+ops.schema+".outbox_events SET retry_count=retry_count+1,next_attempt_at=now()+interval '5 seconds',last_error='Replay publication failed' WHERE id=?",row.get("id")));
                    }
                }
            } finally { if(locked) try(var statement=session.prepareStatement("SELECT pg_advisory_unlock(?)")) {statement.setLong(1,lock);statement.execute();} }
        }
    }
}
