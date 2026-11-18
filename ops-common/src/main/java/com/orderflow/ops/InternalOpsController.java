package com.orderflow.ops;

import com.orderflow.security.IdentityFailure;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.*;
import org.springframework.boot.health.actuate.endpoint.*;
import org.springframework.web.bind.annotation.*;

@RestController
public class InternalOpsController {
    private final OpsService ops;private final HealthEndpoint health;private final String credential;
    public InternalOpsController(OpsService ops,HealthEndpoint health,@Value("${ops.internal-token:${INTERNAL_SERVICE_TOKEN:}}") String credential) {this.ops=ops;this.health=health;this.credential=credential;}
    private void authorize(String supplied) {
        if(credential.length()<32) throw new IdentityFailure(503,"SERVICE_IDENTITY_UNCONFIGURED");
        if(supplied==null || !MessageDigest.isEqual(credential.getBytes(StandardCharsets.UTF_8),supplied.getBytes(StandardCharsets.UTF_8))) throw new IdentityFailure(401,"INVALID_SERVICE_IDENTITY");
    }
    @GetMapping("/internal/ops/orders/{orderId}") public Object order(@PathVariable UUID orderId,@RequestHeader(value="X-Service-Credential",required=false) String supplied) {authorize(supplied);return ops.investigate(orderId);}
    @GetMapping("/internal/ops/status") public Object status(@RequestHeader(value="X-Service-Credential",required=false) String supplied) {
        authorize(supplied);
        var result=new LinkedHashMap<>(ops.stats());var state=health.health();result.put("readiness",state.getStatus().getCode());
        var dependencies=new TreeMap<String,String>();if(state instanceof CompositeHealthDescriptor composite) composite.getComponents().forEach((k,v)->dependencies.put(k,v.getStatus().getCode()));
        result.put("dependencies",dependencies);return result;
    }
}
