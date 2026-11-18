package com.orderflow.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.listener.RecordInterceptor;

@AutoConfiguration
@ConditionalOnClass(AbstractKafkaListenerContainerFactory.class)
public class KafkaTelemetryConfiguration {
    @Bean public static BeanPostProcessor consumerTelemetry(org.springframework.beans.factory.ObjectProvider<MeterRegistry> meters) {
        return new BeanPostProcessor(){public Object postProcessAfterInitialization(Object bean,String name){
            if(bean instanceof AbstractKafkaListenerContainerFactory factory)factory.setRecordInterceptor(new RecordInterceptor<Object,Object>(){
                private final ThreadLocal<TraceContext> scope=new ThreadLocal<>();private final ThreadLocal<Long> start=new ThreadLocal<>();
                public ConsumerRecord<Object,Object> intercept(ConsumerRecord<Object,Object> record,Consumer<Object,Object> consumer){
                    var fields=new HashMap<String,String>();fields.put("operation","kafka.consume");fields.put("topic",record.topic());fields.put("partition",Integer.toString(record.partition()));fields.put("offset",Long.toString(record.offset()));
                    try{var root=new ObjectMapper().readTree(String.valueOf(record.value()));if(root.path("payload").isTextual())root=new ObjectMapper().readTree(root.path("payload").asText());for(String key:List.of("correlationId","eventId"))fields.put(key,root.path(key).asText());fields.put("orderId",root.path("aggregateId").asText());}catch(Exception ignored){}
                    scope.set(TraceContext.open(fields));start.set(System.nanoTime());return record;
                }
                public void success(ConsumerRecord<Object,Object> r,Consumer<Object,Object> c){complete("SUCCESS");}
                public void failure(ConsumerRecord<Object,Object> r,Exception e,Consumer<Object,Object> c){complete("ERROR");}
                private void complete(String result){
                    if(start.get()==null)return;long elapsed=System.nanoTime()-start.get();MDC.put("outcome",result);MDC.put("duration",Double.toString(elapsed/1_000_000d));
                    var registry=meters.getIfAvailable();if(registry!=null)io.micrometer.core.instrument.Timer.builder("orderflow.consumer.duration").tag("outcome",result).register(registry).record(elapsed,java.util.concurrent.TimeUnit.NANOSECONDS);
                    LoggerFactory.getLogger("orderflow.consumer").info("consumer completed");
                }
                public void afterRecord(ConsumerRecord<Object,Object> r,Consumer<Object,Object> c){try{if(scope.get()!=null)scope.get().close();}finally{scope.remove();start.remove();}}
            });return bean;
        }};
    }
}

