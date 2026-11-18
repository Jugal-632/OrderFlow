package com.orderflow.telemetry;

import java.util.*;
import org.slf4j.MDC;

/** A nested scope always restores its caller's context, including on failure. */
public final class TraceContext implements AutoCloseable {
    private final Map<String,String> previous;
    private TraceContext(Map<String,String> fields) {
        previous=MDC.getCopyOfContextMap();fields.forEach((key,value)->{if(value!=null)MDC.put(key,value);});
    }
    public static TraceContext open(Map<String,String> fields) {return new TraceContext(fields);}
    public static TraceContext correlation(String correlation) {return open(Map.of("correlationId",correlation));}
    public void close() {if(previous==null)MDC.clear();else MDC.setContextMap(previous);}
}
