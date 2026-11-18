package com.orderflow.telemetry;
import io.micrometer.cloudwatch2.*;
import io.micrometer.core.instrument.Clock;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.*;
import java.util.concurrent.CompletableFuture;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;
class CloudWatchExportTest {
    @Test void publishesBoundedDimensionsToConfiguredNamespaceWithoutAwsAccess(){
        var client=mock(CloudWatchAsyncClient.class);when(client.putMetricData(any(PutMetricDataRequest.class))).thenReturn(CompletableFuture.completedFuture(PutMetricDataResponse.builder().build()));
        var testClock=new io.micrometer.core.instrument.MockClock();
        class Registry extends CloudWatchMeterRegistry {
            Registry(){super(new CloudWatchConfig(){public String get(String key){return null;}public String namespace(){return "OrderFlow";}},testClock,client);}
            void flush(){publish();}
        }
        var registry=new Registry();try{
            registry.counter("orderflow.consumer.errors","service","inventory-service","environment","test").increment();testClock.add(java.time.Duration.ofMinutes(1));registry.flush();
            var capture=org.mockito.ArgumentCaptor.forClass(PutMetricDataRequest.class);verify(client,atLeastOnce()).putMetricData(capture.capture());
            assertThat(capture.getValue().namespace()).isEqualTo("OrderFlow");assertThat(capture.getValue().metricData()).isNotEmpty();
            assertThat(capture.getValue().metricData().getFirst().dimensions()).allMatch(d->java.util.Set.of("service","environment").contains(d.name()));
        }finally{registry.close();}
    }
}

