package com.orderflow.ops;

import com.fasterxml.jackson.databind.JsonNode;
import com.orderflow.security.*;
import java.util.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

@RestController
@RequestMapping("/api/ops/dead-letters")
public class OpsController {
    private final OpsService ops;
    public OpsController(OpsService ops) { this.ops=ops; }
    @GetMapping public Object list(@RequestParam(defaultValue="OPEN") String status,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) { return ops.list(status,page,size); }
    @GetMapping("/{id}") public Object detail(@PathVariable UUID id) { return ops.detail(id); }
    @PostMapping("/{id}/replay") public ResponseEntity<?> replay(@PathVariable UUID id,@RequestAttribute("caller") Caller caller,
            @RequestHeader(value="Idempotency-Key",required=false) String key,@RequestBody(required=false) JsonNode body) {
        var result=ops.request(caller,id,key,body);var response=ResponseEntity.accepted();
        if(Boolean.TRUE.equals(result.get("replayed"))) response.header("Idempotent-Replayed","true");
        return response.body(result);
    }
}
