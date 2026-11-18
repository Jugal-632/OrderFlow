package com.orderflow.ops;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public class OpsListeners {
    private final OpsService ops;
    public OpsListeners(OpsService ops) { this.ops=ops; }
    @KafkaListener(id="ops-dlt-indexer",groupId="${spring.application.name}-dlt-indexer",topicPattern="#{opsService.dltPattern}",containerFactory="opsIndexerFactory")
    public void index(ConsumerRecord<String,String> record) { ops.index(record); }
    @KafkaListener(id="ops-replay",groupId="${spring.application.name}-replay",topics="${spring.application.name}.replay",containerFactory="opsReplayFactory")
    public void replay(String value) { ops.replay(value); }
}
