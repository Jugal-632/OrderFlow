package com.orderflow.order.dashboard;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopics implements AutoCloseable {
    private final Admin admin;private Instant expires=Instant.MIN,lastSuccess;
    private List<Map<String,Object>> previous;
    private Map<String,Object> cached=Map.of();
    public KafkaTopics(KafkaProperties properties) {
        var settings=new HashMap<>(properties.buildAdminProperties());settings.put("request.timeout.ms",1000);settings.put("default.api.timeout.ms",2000);admin=Admin.create(settings);
    }
    private static <T> T get(KafkaFuture<T> future,long deadline) throws Exception {return future.get(Math.max(1,deadline-System.nanoTime()),TimeUnit.NANOSECONDS);}
    public synchronized Map<String,Object> snapshot() {
        if(Instant.now().isBefore(expires))return cached;
        var result=new LinkedHashMap<String,Object>();result.put("observedAt",Instant.now().toString());
        try {
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
            var topics=get(admin.listTopics().names(),deadline).stream().filter(t->t.startsWith("order.")||t.startsWith("inventory.")||t.startsWith("order-service.")||t.startsWith("inventory-service.")||t.startsWith("notification-service.")).sorted().limit(100).toList();
            var descriptions=get(admin.describeTopics(topics).allTopicNames(),deadline);
            Map<TopicPartition,OffsetSpec> ends=new HashMap<>();descriptions.values().forEach(d->d.partitions().forEach(p->ends.put(new TopicPartition(d.name(),p.partition()),OffsetSpec.latest())));
            var endFuture=admin.listOffsets(ends).all();
            Map<String,KafkaFuture<Map<TopicPartition,OffsetAndMetadata>>> commits=new LinkedHashMap<>();
            for(String service:List.of("order-service","inventory-service","notification-service")) for(String suffix:List.of("","-replay","-dlt-indexer")) {
                String group=service+suffix;commits.put(group,admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata());
            }
            var latest=get(endFuture,deadline);List<Map<String,Object>> rows=new ArrayList<>();
            for(var group:commits.entrySet()) {
                var offsets=get(group.getValue(),deadline);
                for(var partition:latest.entrySet()) {
                    String topic=partition.getKey().topic();String name=group.getKey();
                    boolean relevant=name.endsWith("-replay")?topic.equals(name.substring(0,name.length()-7)+".replay"):
                            name.endsWith("-dlt-indexer")?topic.endsWith(name.substring(0,name.length()-12)+".DLT")||topic.equals(name.substring(0,name.length()-12)+".replay.DLT"):
                            name.equals("order-service")?Set.of("inventory.reserved","inventory.rejected","inventory.released").contains(topic):
                            name.equals("inventory-service")?Set.of("order.placed","order.cancel.requested").contains(topic):Set.of("order.placed","order.confirmed","order.failed","order.cancelled").contains(topic);
                    if(!relevant)continue;
                    var row=new LinkedHashMap<String,Object>();var committed=offsets.get(partition.getKey());
                    row.put("topic",topic);row.put("partition",partition.getKey().partition());row.put("group",name);row.put("logEndOffset",partition.getValue().offset());
                    row.put("committedOffset",committed==null?null:committed.offset());row.put("lag",committed==null?null:Math.max(0,partition.getValue().offset()-committed.offset()));
                    row.put("status",committed==null?"UNINITIALIZED":"AVAILABLE");
                    var info=descriptions.get(topic).partitions().stream().filter(p->p.partition()==partition.getKey().partition()).findFirst().orElseThrow();
                    row.put("leader",info.leader().id());row.put("replicas",info.replicas().size());rows.add(row);
                }
            }
            previous=rows;lastSuccess=Instant.now();result.put("status","AVAILABLE");
        } catch(Exception e) {result.put("status",previous==null?"UNAVAILABLE":"STALE");if(e instanceof InterruptedException)Thread.currentThread().interrupt();}
        result.put("data",previous);result.put("lastSuccessfulAt",lastSuccess==null?null:lastSuccess.toString());cached=result;expires=Instant.now().plusSeconds(5);return result;
    }
    @jakarta.annotation.PreDestroy public void close() {admin.close(Duration.ofSeconds(1));}
}
