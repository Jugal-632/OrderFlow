package com.orderflow.ops;

import com.orderflow.contracts.envelope.EventValidator;
import com.orderflow.contracts.envelope.EventValidationException;
import java.util.Set;

public interface ReplayHandler {
    Set<String> topics();
    void dispatch(String topic,String payload);
    default void validate(String topic,String payload) {
        EventValidator.validateJson(payload);
        try {
            var event=EventValidator.getObjectMapper().readTree(payload);
            if(!topics().contains(topic) || !topic.equals(event.path("eventType").asText())) throw new EventValidationException("Unsupported replay source");
        } catch(java.io.IOException e) { throw new EventValidationException("Invalid replay payload"); }
    }
}
