package com.orderflow.order.service;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class IdempotencyService {
    public enum Operation { CREATE_ORDER, CANCEL_ORDER }
    public record Result(int status, String body, UUID resourceId, boolean replayed) {}
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final int lockTimeout;
    public IdempotencyService(JdbcTemplate jdbc, Clock clock, @Value("${order.idempotency.lock-timeout-ms:4000}") int lockTimeout) {
        this.jdbc=jdbc; this.clock=clock; this.lockTimeout=lockTimeout;
    }
    public static void validateKey(String key) {
        if (key==null || key.length()<1 || key.length()>128 || key.chars().anyMatch(c->c<33 || c>126))
            throw new RequestFailure(400,"INVALID_IDEMPOTENCY_KEY");
    }
    public static String createHash(UUID customer, UUID product, int quantity) {
        String canonical="CREATE_ORDER\n"+customer+"\n"+product+"\n"+quantity;
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8))); }
        catch(NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    @Transactional(isolation=Isolation.READ_COMMITTED, timeout=5)
    public Result execute(UUID customer, Operation operation, String key, String hash, Supplier<Result> action) {
        validateKey(key);
        Objects.requireNonNull(customer);
        jdbc.queryForObject("SELECT set_config('lock_timeout',?,true)",String.class,lockTimeout+"ms");
        int inserted=jdbc.update("""
            INSERT INTO order_schema.request_idempotency(customer_id,operation,idempotency_key,request_hash,response_status,created_at)
            VALUES (?,?,?,?,0,?) ON CONFLICT (customer_id,operation,idempotency_key) DO NOTHING
            """,customer,operation.name(),key,hash,Timestamp.from(clock.instant()));
        if(inserted==0) {
            var row=jdbc.queryForMap("SELECT * FROM order_schema.request_idempotency WHERE customer_id=? AND operation=? AND idempotency_key=?",customer,operation.name(),key);
            if(!hash.equals(row.get("request_hash"))) throw new RequestFailure(409,"IDEMPOTENCY_KEY_REUSED");
            return new Result(((Number)row.get("response_status")).intValue(),row.get("response_body").toString(),(UUID)row.get("resource_id"),true);
        }
        Result result=action.get();
        jdbc.update("""
            UPDATE order_schema.request_idempotency SET resource_id=?,response_status=?,response_body=CAST(? AS jsonb)
            WHERE customer_id=? AND operation=? AND idempotency_key=?
            """,result.resourceId(),result.status(),result.body(),customer,operation.name(),key);
        // Return PostgreSQL's JSON representation on both first response and replay for byte-for-byte equality.
        String stored=jdbc.queryForObject("SELECT response_body::text FROM order_schema.request_idempotency WHERE customer_id=? AND operation=? AND idempotency_key=?",String.class,customer,operation.name(),key);
        return new Result(result.status(),stored,result.resourceId(),false);
    }
    public static class RequestFailure extends RuntimeException {
        public final int status;
        public final String code;
        public RequestFailure(int status,String code) { super(code);this.status=status;this.code=code; }
    }
}
