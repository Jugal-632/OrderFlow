package com.orderflow.security;

import java.time.Instant;
import java.util.*;

public final class ApiErrors {
    private ApiErrors() {}
    public static String correlation(String value) {
        try { return UUID.fromString(value).toString(); } catch(RuntimeException e) { return UUID.randomUUID().toString(); }
    }
    public static Map<String,Object> body(String code,String correlation) {
        return Map.of("code",code,"message",code.replace('_',' ').toLowerCase(Locale.ROOT),
                "correlationId",correlation,"timestamp",Instant.now().toString(),"fieldErrors",Map.of());
    }
    public static void page(int page,int size) {
        if(page<0 || page>10000 || size<1 || size>100) throw new IdentityFailure(400,"INVALID_PAGINATION");
    }
}
