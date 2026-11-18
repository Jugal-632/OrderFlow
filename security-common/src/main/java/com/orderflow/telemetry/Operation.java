package com.orderflow.telemetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Metrics;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
public final class Operation implements AutoCloseable {
    private final TraceContext scope;private final long start=System.nanoTime();private final String operation;private String outcome="ERROR";
    private Operation(String operation,String payload,String topic){
        this.operation=operation;var fields=new HashMap<String,String>();fields.put("operation",operation);fields.put("topic",topic);
        try{var json=new ObjectMapper().readTree(payload);if(json.has("payload")&&json.path("payload").isTextual())json=new ObjectMapper().readTree(json.path("payload").asText());
            fields.put("eventId",json.path("eventId").asText());fields.put("correlationId",json.path("correlationId").asText());fields.put("orderId",json.path("aggregateId").asText());
        }catch(Exception ignored){}scope=TraceContext.open(fields);
    }
    public static Operation event(String operation,String payload,String topic){return new Operation(operation,payload,topic);}
    public void success(){outcome="SUCCESS";}
    public void close(){try{long elapsed=System.nanoTime()-start;MDC.put("outcome",outcome);MDC.put("duration",Double.toString(elapsed/1_000_000d));Metrics.timer("orderflow.operation.duration","operation",operation,"outcome",outcome).record(elapsed,TimeUnit.NANOSECONDS);LoggerFactory.getLogger("orderflow.operation").info("operation completed");}finally{scope.close();}}
}
