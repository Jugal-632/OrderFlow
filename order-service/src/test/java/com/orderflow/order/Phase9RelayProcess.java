package com.orderflow.order;
import java.util.Map;
import java.time.Clock;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
/** Test worker publishing a real persisted Inventory event against the test broker. */
public class Phase9RelayProcess {
    public static void main(String[] args)throws Exception{
        var data=new DriverManagerDataSource(args[0],args[1],args[2]);
        var producer=new DefaultKafkaProducerFactory<String,String>(Map.of("bootstrap.servers",args[3],"acks","all","enable.idempotence",true,"delivery.timeout.ms",10000,"request.timeout.ms",5000,"max.block.ms",10000,"key.serializer",StringSerializer.class,"value.serializer",StringSerializer.class));
        try{new com.orderflow.inventory.InventoryOutboxRelay(data,new JdbcTemplate(data),new KafkaTemplate<>(producer),new DataSourceTransactionManager(data),Clock.systemUTC()).publishPending();}finally{producer.destroy();}
    }
}
