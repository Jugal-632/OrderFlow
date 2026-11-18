package com.orderflow.telemetry;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.EncoderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/** Strict field allowlist: arbitrary library messages, arguments and exceptions never enter logs. */
public class SafeJsonEncoder extends EncoderBase<ILoggingEvent> {
    private final ObjectMapper json=new ObjectMapper();
    public byte[] headerBytes(){return null;}
    public byte[] footerBytes(){return null;}
    public byte[] encode(ILoggingEvent event) {
        var row=new LinkedHashMap<String,Object>();row.put("timestamp",Instant.ofEpochMilli(event.getTimeStamp()).toString());
        row.put("service",context.getProperty("service"));row.put("environment",context.getProperty("environment"));
        row.put("level",event.getLevel().toString());row.put("logger",event.getLoggerName());
        var mdc=event.getMDCPropertyMap();
        for(String key:List.of("correlationId","requestId","eventId","orderId","actorId"))row.put(key,uuid(mdc.get(key)));
        for(String key:List.of("topic","operation","outcome")){String value=mdc.get(key);row.put(key,value!=null&&value.matches("[A-Za-z0-9_.-]{1,100}")?value:null);}
        for(String key:List.of("partition","offset","duration","status")){String value=mdc.get(key);row.put(key,value!=null&&value.matches("[0-9.]{1,24}")?value:null);}
        // Exception class is useful diagnostically; message/stack may include SQL, credentials or payloads.
        row.put("errorType",event.getThrowableProxy()==null?null:event.getThrowableProxy().getClassName());
        var causes=new ArrayList<String>();var cause=event.getThrowableProxy();while(cause!=null&&causes.size()<8){causes.add(cause.getClassName());cause=cause.getCause();}row.put("errorCauses",causes);
        try{return (json.writeValueAsString(row)+"\n").getBytes(StandardCharsets.UTF_8);}catch(Exception e){return "{\"outcome\":\"ENCODING_FAILURE\"}\n".getBytes(StandardCharsets.UTF_8);}
    }
    private static String uuid(String value){try{return UUID.fromString(value).toString();}catch(Exception e){return null;}}
}
