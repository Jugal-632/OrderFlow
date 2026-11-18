package com.orderflow.ops;
import java.util.UUID;
public record ReplayMessage(int schemaVersion,String service,UUID replayRequestId,UUID deadLetterId,
        String sourceTopic,int sourcePartition,long sourceOffset,String payload) {}
