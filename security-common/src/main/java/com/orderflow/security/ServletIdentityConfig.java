package com.orderflow.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import org.springframework.context.annotation.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.web.filter.OncePerRequestFilter;

@Configuration(proxyBeanMethods=false)
@Import({IdentityConfig.class,ServletErrors.class})
public class ServletIdentityConfig {
    @Bean public IdentityVerifier identityVerifier(JwtTokens tokens,StringRedisTemplate redis) { return new IdentityVerifier(tokens,redis); }
    @Bean public FilterRegistrationBean<OncePerRequestFilter> identityFilter(IdentityVerifier verifier,ObjectMapper json,org.springframework.beans.factory.ObjectProvider<AccessAudit> audit) {
        var registration=new FilterRegistrationBean<OncePerRequestFilter>();
        registration.setOrder(-200);
        registration.setFilter(new OncePerRequestFilter() {
            @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain) throws ServletException,IOException {
                String path=request.getRequestURI().substring(request.getContextPath().length());
                String correlation=ApiErrors.correlation(request.getHeader("X-Correlation-Id"));
                response.setHeader("X-Correlation-Id",correlation);
                request.setAttribute("correlationId",correlation);
                HttpServletRequest bounded=request;
                try {
                    if(request.getContentLengthLong()>65536) throw new IdentityFailure(413,"REQUEST_TOO_LARGE");
                    if(path.startsWith("/api/") || path.startsWith("/auth/") && !path.equals("/auth/login")) {
                        Caller caller=verifier.verify(request.getHeader("Authorization"));
                        boolean order=path.equals("/api/orders") || path.startsWith("/api/orders/");
                        if(!order && path.startsWith("/api/") && !caller.admin())
                            throw new IdentityFailure(403,"FORBIDDEN");
                        request.setAttribute("caller",caller);
                        request.setAttribute("authenticatedCustomerId",caller.userId());
                    }
                    if(path.startsWith("/api/") || path.startsWith("/auth/")) {
                        byte[] body=request.getInputStream().readNBytes(65537);
                        if(body.length>65536) throw new IdentityFailure(413,"REQUEST_TOO_LARGE");
                        bounded=new HttpServletRequestWrapper(request) {
                            @Override public String getHeader(String name) { return name.equalsIgnoreCase("X-Correlation-Id")?correlation:super.getHeader(name); }
                            @Override public ServletInputStream getInputStream() {
                                var input=new java.io.ByteArrayInputStream(body);
                                return new ServletInputStream() {
                                    public int read() { return input.read(); }
                                    public boolean isFinished() { return input.available()==0; }
                                    public boolean isReady() { return true; }
                                    public void setReadListener(ReadListener listener) { throw new UnsupportedOperationException("Synchronous MVC request body"); }
                                };
                            }
                            @Override public java.io.BufferedReader getReader() { return new java.io.BufferedReader(new java.io.InputStreamReader(getInputStream(),java.nio.charset.StandardCharsets.UTF_8)); }
                        };
                    }
                } catch(IdentityFailure e) {
                    if(path.startsWith("/api/ops/") && (e.status==401 || e.status==403)) audit.ifAvailable(sink->sink.rejected(e.status,correlation));
                    response.setStatus(e.status);response.setContentType("application/json");
                    if(e.status==503) response.setHeader("Retry-After","1");
                    json.writeValue(response.getOutputStream(),ApiErrors.body(e.code,correlation));return;
                }
                chain.doFilter(bounded,response);
            }
        });
        return registration;
    }
}
