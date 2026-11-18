package com.orderflow.ops;

import com.fasterxml.jackson.databind.*;
import com.orderflow.contracts.envelope.EventValidationException;
import com.orderflow.security.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.transaction.*;
import org.springframework.transaction.support.TransactionTemplate;

public class OpsService {
    public final String service,schema;
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final ReplayHandler handler;
    private final TransactionTemplate tx,auditTx;
    public OpsService(String service,JdbcTemplate jdbc,ObjectMapper json,ReplayHandler handler,PlatformTransactionManager manager) {
        if(!Set.of("order-service","inventory-service","notification-service").contains(service)) throw new IllegalArgumentException("Unknown operations owner");
        this.service=service;this.schema=service.replace("-service","_schema");this.jdbc=jdbc;this.handler=handler;
        this.json=json.copy().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        tx=new TransactionTemplate(manager);tx.setTimeout(5);
        auditTx=new TransactionTemplate(manager);auditTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);auditTx.setTimeout(5);
    }
    public String getDltPattern() { return ".*\\."+service+"\\.DLT|"+service+"\\.replay\\.DLT"; }
    public String encode(Object value) { try { return json.writeValueAsString(value); } catch(Exception e) { throw new IllegalStateException(e); } }
    public ReplayMessage decode(String value) {
        try { var message=json.readValue(value,ReplayMessage.class);
            if(message.schemaVersion()!=1 || !service.equals(message.service()) || message.replayRequestId()==null || message.deadLetterId()==null || message.payload()==null)
                throw new IllegalArgumentException();
            return message;
        } catch(Exception e) { throw new EventValidationException("Invalid replay wrapper"); }
    }
    static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch(NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    public void audit(String actor,UUID deadLetter,int status,String code) {
        auditTx.executeWithoutResult(s->jdbc.update("INSERT INTO "+schema+".ops_audit(actor,dead_letter_id,http_status,outcome) VALUES (?,?,?,?)",actor,deadLetter,status,code));
    }
    public List<Map<String,Object>> list(String status,int page,int size) {
        ApiErrors.page(page,size);
        if(!Set.of("OPEN","REPLAY_QUEUED","RESOLVED","LINKED","ALL").contains(status)) throw new IdentityFailure(400,"INVALID_STATUS");
        return jdbc.queryForList("SELECT id,source_topic,source_partition,source_offset,event_type,order_id,sanitized_error,status,replay_attempts,last_seen_at,original_dead_letter_id FROM "+schema+
                ".dead_letters WHERE (?='ALL' OR status=?) ORDER BY first_seen_at DESC,id DESC LIMIT ? OFFSET ?",status,status,size,page*size);
    }
    public Map<String,Object> detail(UUID id) {
        var rows=jdbc.queryForList("SELECT * FROM "+schema+".dead_letters WHERE id=?",id);
        if(rows.isEmpty()) throw new IdentityFailure(404,"DEAD_LETTER_NOT_FOUND");
        return Map.of("deadLetter",rows.getFirst(),"replays",jdbc.queryForList("SELECT * FROM "+schema+".replay_requests WHERE dead_letter_id=? ORDER BY requested_at DESC,id DESC LIMIT 100",id));
    }
    public Map<String,Object> request(Caller caller,UUID id,String key,JsonNode body) {
        String actor=caller.userId().toString();
        try {
            if(!caller.admin()) throw new IdentityFailure(403,"FORBIDDEN");
            if(key==null || key.isBlank() || key.length()>128 || key.chars().anyMatch(c->c<33 || c>126)) throw new IdentityFailure(400,"INVALID_IDEMPOTENCY_KEY");
            if(body==null || !body.isObject() || body.size()!=1 || !body.path("reason").isTextual()) throw new IdentityFailure(400,"INVALID_REPLAY_REQUEST");
            String reason=body.path("reason").asText().strip();
            if(reason.length()<5 || reason.length()>500 || reason.chars().anyMatch(c->c<32)) throw new IdentityFailure(400,"INVALID_REASON");
            String requestHash=hash(id+"\n"+reason);
            var result=tx.execute(s->{
                jdbc.queryForList("SELECT pg_advisory_xact_lock(?)",ByteBuffer.wrap(HexFormat.of().parseHex(hash(actor+":"+key))).getLong());
                var prior=jdbc.queryForList("SELECT id,request_hash FROM "+schema+".replay_requests WHERE requested_by=? AND idempotency_key=?",actor,key);
                if(!prior.isEmpty()) {
                    if(!requestHash.equals(prior.getFirst().get("request_hash"))) throw new IdentityFailure(409,"IDEMPOTENCY_KEY_REUSED");
                    return Map.<String,Object>of("replayRequestId",prior.getFirst().get("id"),"deadLetterId",id,"status","QUEUED","replayed",true);
                }
                var letters=jdbc.queryForList("SELECT * FROM "+schema+".dead_letters WHERE id=? FOR UPDATE",id);
                if(letters.isEmpty()) throw new IdentityFailure(404,"DEAD_LETTER_NOT_FOUND");
                var letter=letters.getFirst();
                if(!"OPEN".equals(letter.get("status"))) throw new IdentityFailure(409,"REPLAY_NOT_ELIGIBLE");
                String payload=(String)letter.get("raw_payload"),topic=(String)letter.get("source_topic");
                try { handler.validate(topic,payload); } catch(RuntimeException invalid) { throw new IdentityFailure(409,"INVALID_REPLAY_PAYLOAD"); }
                UUID replay=UUID.randomUUID();int attempt=((Number)letter.get("replay_attempts")).intValue()+1;
                var wrapper=new ReplayMessage(1,service,replay,id,topic,((Number)letter.get("source_partition")).intValue(),((Number)letter.get("source_offset")).longValue(),payload);
                jdbc.update("INSERT INTO "+schema+".replay_requests(id,dead_letter_id,idempotency_key,request_hash,requested_by,reason,result) VALUES (?,?,?,?,?,?,'QUEUED')",replay,id,key,requestHash,actor,reason);
                jdbc.update("INSERT INTO "+schema+".outbox_events(id,aggregate_id,aggregate_type,event_type,topic,event_key,payload,status,aggregate_version,created_at,next_attempt_at,retry_count) VALUES (?,?,'OPERATIONS','ops.replay',?,?,CAST(? AS jsonb),'PENDING',?,now(),now(),0)",replay,id,service+".replay",id.toString(),encode(wrapper),attempt);
                jdbc.update("UPDATE "+schema+".dead_letters SET status='REPLAY_QUEUED',replay_attempts=? WHERE id=?",attempt,id);
                jdbc.update("INSERT INTO "+schema+".ops_audit(actor,dead_letter_id,http_status,outcome) VALUES (?,?,202,'REPLAY_QUEUED')",actor,id);
                return Map.<String,Object>of("replayRequestId",replay,"deadLetterId",id,"status","QUEUED","replayed",false);
            });
            return result;
        } catch(IdentityFailure e) { audit(actor,id,e.status,e.code);throw e; }
    }
    public void replay(String raw) {
        ReplayMessage message=decode(raw);
        tx.executeWithoutResult(s->{
            var requests=jdbc.queryForList("SELECT * FROM "+schema+".replay_requests WHERE id=? AND dead_letter_id=? FOR UPDATE",message.replayRequestId(),message.deadLetterId());
            if(requests.isEmpty()) throw new EventValidationException("Unknown replay request");
            String expected=jdbc.queryForObject("SELECT payload::text FROM "+schema+".outbox_events WHERE id=? AND event_type='ops.replay'",String.class,message.replayRequestId());
            if(!decode(expected).equals(message)) throw new EventValidationException("Replay wrapper was modified");
            if(!"QUEUED".equals(requests.getFirst().get("result"))) return;
            jdbc.queryForList("SELECT id FROM "+schema+".dead_letters WHERE id=? FOR UPDATE",message.deadLetterId());
            handler.validate(message.sourceTopic(),message.payload());
            try(var ignored=com.orderflow.telemetry.TraceContext.correlation(json.readTree(message.payload()).path("correlationId").asText())) {
                handler.dispatch(message.sourceTopic(),message.payload());
            } catch(java.io.IOException e) { throw new EventValidationException("Invalid source event"); }
            jdbc.update("UPDATE "+schema+".replay_requests SET result='SUCCEEDED',completed_at=now() WHERE id=?",message.replayRequestId());
            jdbc.update("UPDATE "+schema+".dead_letters SET status='RESOLVED',last_seen_at=now() WHERE id=?",message.deadLetterId());
        });
    }
    public void index(ConsumerRecord<String,String> record) {
        tx.executeWithoutResult(s->{
            String source=textHeader(record,KafkaHeaders.DLT_ORIGINAL_TOPIC);
            int partition=(int)numberHeader(record,KafkaHeaders.DLT_ORIGINAL_PARTITION,4);
            long offset=numberHeader(record,KafkaHeaders.DLT_ORIGINAL_OFFSET,8);
            if(source==null) source="UNKNOWN";
            UUID id=UUID.randomUUID(),original=null,replay=null,eventId=null,order=null;String type=null;
            String raw=Objects.toString(record.value(),"");
            if(record.topic().equals(service+".replay.DLT")) {
                try {
                    var wrapper=decode(raw);
                    var rows=jdbc.queryForList("SELECT r.id FROM "+schema+".replay_requests r JOIN "+schema+".outbox_events o ON o.id=r.id WHERE r.id=? AND r.dead_letter_id=? AND o.payload=CAST(? AS jsonb)",wrapper.replayRequestId(),wrapper.deadLetterId(),raw);
                    if(!rows.isEmpty()) {original=wrapper.deadLetterId();replay=wrapper.replayRequestId();}
                } catch(EventValidationException invalid) { /* Invalid wrappers are indexed; database failures must retry without committing the offset. */ }
            } else {
                try { var tree=json.readTree(raw);eventId=UUID.fromString(tree.path("eventId").asText());order=UUID.fromString(tree.path("aggregateId").asText());type=tree.path("eventType").asText(); }
                catch(Exception ignored) { /* Malformed source records remain inspectable. */ }
            }
            int added=jdbc.update("INSERT INTO "+schema+".dead_letters(id,source_topic,source_partition,source_offset,dlt_topic,dlt_partition,dlt_offset,event_id,event_type,order_id,raw_payload,raw_headers,error_code,sanitized_error,status,original_dead_letter_id,replay_request_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb),'PROCESSING_FAILED','Processing failed; inspect source identity and payload.',?,?,?) ON CONFLICT (dlt_topic,dlt_partition,dlt_offset) DO NOTHING",
                    id,source,partition,offset,record.topic(),record.partition(),record.offset(),eventId,type,order,raw,
                    encode(Map.of("sourceTopic",source,"sourcePartition",partition,"sourceOffset",offset)),original==null?"OPEN":"LINKED",original,replay);
            if(added==1 && replay!=null) {
                int changed=jdbc.update("UPDATE "+schema+".replay_requests SET result='FAILED',completed_at=now() WHERE id=? AND result='QUEUED'",replay);
                if(changed==1) jdbc.update("UPDATE "+schema+".dead_letters SET status='OPEN',last_seen_at=now() WHERE id=?",original);
            }
        });
    }
    private static String textHeader(ConsumerRecord<?,?> r,String key) { var h=r.headers().lastHeader(key);return h==null?null:new String(h.value(),StandardCharsets.UTF_8); }
    private static long numberHeader(ConsumerRecord<?,?> r,String key,int length) { var h=r.headers().lastHeader(key);if(h==null || h.value().length!=length)return -1;var b=ByteBuffer.wrap(h.value());return length==4?b.getInt():b.getLong(); }
    public Map<String,Object> stats() {
        var result=new LinkedHashMap<String,Object>();result.put("service",service);result.put("observedAt",Instant.now().toString());
        result.put("outbox",jdbc.queryForMap("SELECT count(*) FILTER(WHERE status='PENDING') AS pending,count(*) FILTER(WHERE status='BLOCKED') AS blocked,COALESCE(EXTRACT(EPOCH FROM now()-min(created_at) FILTER(WHERE status='PENDING')),0) AS oldest_pending_seconds FROM "+schema+".outbox_events"));
        result.put("unresolvedDeadLetters",jdbc.queryForObject("SELECT count(*) FROM "+schema+".dead_letters WHERE status IN ('OPEN','REPLAY_QUEUED')",Long.class));return result;
    }
    public Map<String,Object> investigate(UUID orderId) {
        if(!schema.equals("inventory_schema"))throw new IdentityFailure(404,"NOT_FOUND");
        return Map.of("decisions",jdbc.queryForList("SELECT * FROM inventory_schema.inventory_decisions WHERE order_id=?",orderId),
            "reservations",jdbc.queryForList("SELECT * FROM inventory_schema.inventory_reservations WHERE order_id=?",orderId),
            "outbox",jdbc.queryForList("SELECT id,event_type,status,retry_count,next_attempt_at FROM inventory_schema.outbox_events WHERE aggregate_id=? ORDER BY aggregate_version",orderId));
    }
}
