package com.orderflow.telemetry;

import com.orderflow.security.ApiErrors;
import io.micrometer.core.instrument.*;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.util.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.web.filter.OncePerRequestFilter;

@AutoConfiguration
@ConditionalOnWebApplication(type=ConditionalOnWebApplication.Type.SERVLET)
public class ServletTelemetryConfiguration {
    @Bean
    @ConditionalOnProperty(name="observability.readiness.enforce",havingValue="true")
    public FilterRegistrationBean<OncePerRequestFilter> readinessGate(org.springframework.beans.factory.ObjectProvider<org.springframework.boot.health.actuate.endpoint.HealthEndpoint> health) {
        var registration=new FilterRegistrationBean<OncePerRequestFilter>();registration.setOrder(-100);
        registration.setFilter(new OncePerRequestFilter(){
            private volatile long expires;private volatile boolean ready;
            private synchronized boolean ready(){if(System.nanoTime()<expires)return ready;try{ready=health.getObject().health().getStatus().equals(org.springframework.boot.health.contributor.Status.UP);}catch(Exception e){ready=false;}expires=System.nanoTime()+1_000_000_000L;return ready;}
            protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
                if(!java.util.Set.of("GET","HEAD","OPTIONS").contains(request.getMethod())&&(request.getRequestURI().startsWith("/api/")||request.getRequestURI().startsWith("/auth/"))&&!ready()){
                    response.setStatus(503);response.setHeader("Retry-After","1");response.setContentType("application/json");response.getWriter().write("{\"code\":\"DEPENDENCY_UNAVAILABLE\"}");return;
                }chain.doFilter(request,response);
            }
        });return registration;
    }
    @Bean public FilterRegistrationBean<OncePerRequestFilter> telemetryFilter(MeterRegistry meters,Environment environment) {
        var registration=new FilterRegistrationBean<OncePerRequestFilter>();registration.setOrder(Integer.MIN_VALUE);
        registration.setFilter(new OncePerRequestFilter(){
            protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
                String correlation=ApiErrors.correlation(request.getHeader("X-Correlation-Id")),requestId=UUID.randomUUID().toString();
                long start=System.nanoTime();boolean failed=false;
                try(var scope=TraceContext.open(Map.of("correlationId",correlation,"requestId",requestId,"operation","http.request"))){
                    response.setHeader("X-Correlation-Id",correlation);
                    var wrapped=new HttpServletRequestWrapper(request){public String getHeader(String key){return key.equalsIgnoreCase("X-Correlation-Id")?correlation:super.getHeader(key);}};
                    try {
                        if(request.getRequestURI().startsWith("/actuator") && request.getLocalPort()!=environment.getProperty("local.management.port",Integer.class,environment.getProperty("management.server.port",Integer.class,-1))) {response.setStatus(404);return;}
                        chain.doFilter(wrapped,response);
                    }catch(IOException|ServletException|RuntimeException e){failed=true;throw e;}
                    finally{
                        int status=failed?500:response.getStatus();String outcome=status>=500?"ERROR":status>=400?"REJECTED":"SUCCESS";
                        MDC.put("status",Integer.toString(status));MDC.put("outcome",outcome);MDC.put("duration",Double.toString((System.nanoTime()-start)/1_000_000d));
                        Object route=request.getAttribute("org.springframework.web.servlet.HandlerMapping.bestMatchingPattern");
                        String uri=route==null?"UNMATCHED":route.toString();
                        String method=Set.of("GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS").contains(request.getMethod())?request.getMethod():"OTHER";
                        io.micrometer.core.instrument.Timer.builder("orderflow.http.duration").tags("method",method,"route",uri,"outcome",outcome).publishPercentileHistogram().register(meters).record(System.nanoTime()-start,java.util.concurrent.TimeUnit.NANOSECONDS);
                        LoggerFactory.getLogger("orderflow.http").info("request completed");
                    }
                }
            }
        });return registration;
    }
}
