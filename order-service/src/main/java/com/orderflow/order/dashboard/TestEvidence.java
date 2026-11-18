package com.orderflow.order.dashboard;
import com.fasterxml.jackson.databind.*;
import com.orderflow.security.IdentityFailure;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
@Component
public class TestEvidence {
    private final ObjectMapper json;
    public TestEvidence(ObjectMapper json) {this.json=json;}
    public JsonNode reports() {
        try(var input=new ClassPathResource("evidence/test-runs.json").getInputStream()) {
            byte[] bytes=input.readNBytes(1048577);if(bytes.length>1048576)throw new IllegalArgumentException();
            JsonNode root=json.readTree(bytes);if(!root.path("runs").isArray() || root.path("runs").size()>100)throw new IllegalArgumentException();
            java.time.Instant.parse(root.path("generatedAt").asText());
            for(var run:root.path("runs")) {
                if(!run.path("source").asText().matches("phase[0-9]+-test-summary.json") || !run.path("sha256").asText().matches("[a-f0-9]{64}"))throw new IllegalArgumentException();
                int total=0;
                if(!run.path("suites").isArray())throw new IllegalArgumentException();
                for(var suite:run.path("suites")) {
                    for(String key:java.util.List.of("tests","failures","errors","skipped")) if(!suite.path(key).isIntegralNumber() || suite.path(key).asInt()<0)throw new IllegalArgumentException();
                    if(suite.path("failures").asInt()+suite.path("errors").asInt()+suite.path("skipped").asInt()>suite.path("tests").asInt())throw new IllegalArgumentException();
                    total+=suite.path("tests").asInt();
                }
                if(total==0)throw new IllegalArgumentException();
            }
            if(root.has("verification")) {
                var matrix=root.path("verification").path("matrix");var performance=root.path("verification").path("performance");
                if(!matrix.path("scenarios").isArray()||matrix.path("scenarios").size()!=22)throw new IllegalArgumentException();
                var ids=new java.util.HashSet<String>();for(var scenario:matrix.path("scenarios")) {
                    if(!scenario.path("scenarioId").asText().matches("T(0[1-9]|1[0-9]|2[0-2])")||!ids.add(scenario.path("scenarioId").asText())||!java.util.Set.of("PASS","FAIL").contains(scenario.path("result").asText()))throw new IllegalArgumentException();
                }
                if(performance.path("warmupSeconds").asInt()!=60||performance.path("measuredSeconds").asInt()!=300||performance.path("users").asInt()!=20||!performance.path("httpMilliseconds").path("p95").isNumber()||!performance.path("confirmationMilliseconds").path("p95").isNumber())throw new IllegalArgumentException();
            }
            return root;
        } catch(Exception e) {throw new IdentityFailure(503,"EVIDENCE_UNAVAILABLE");}
    }
}
