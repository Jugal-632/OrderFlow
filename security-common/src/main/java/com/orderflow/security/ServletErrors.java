package com.orderflow.security;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
@org.springframework.core.annotation.Order(org.springframework.core.Ordered.LOWEST_PRECEDENCE)
public class ServletErrors {
    @org.springframework.beans.factory.annotation.Autowired
    private org.springframework.beans.factory.ObjectProvider<AccessAudit> audits;
    @ExceptionHandler(IdentityFailure.class)
    public ResponseEntity<?> identity(IdentityFailure e,HttpServletRequest request) {
        var builder=ResponseEntity.status(e.status);
        if(e.status==503) builder.header("Retry-After","1");
        return builder.body(ApiErrors.body(e.code,String.valueOf(request.getAttribute("correlationId"))));
    }
    @ExceptionHandler({org.springframework.http.converter.HttpMessageNotReadableException.class,
            org.springframework.web.bind.MethodArgumentNotValidException.class,
            org.springframework.web.bind.MissingServletRequestParameterException.class,
            org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class})
    public ResponseEntity<?> invalid(Exception e,HttpServletRequest request) {
        if(request.getRequestURI().startsWith("/api/ops/")) audits.ifAvailable(a->a.rejected(400,String.valueOf(request.getAttribute("correlationId"))));
        return ResponseEntity.badRequest().body(ApiErrors.body("INVALID_REQUEST",String.valueOf(request.getAttribute("correlationId"))));
    }
    @ExceptionHandler(org.springframework.web.servlet.resource.NoResourceFoundException.class)
    public ResponseEntity<?> missing(Exception e,HttpServletRequest request) {
        return ResponseEntity.status(404).body(ApiErrors.body("NOT_FOUND",String.valueOf(request.getAttribute("correlationId"))));
    }
    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> unexpected(Exception e,HttpServletRequest request) {
        int status=e instanceof org.springframework.web.ErrorResponse error?error.getStatusCode().value():500;
        if(status>=500) org.slf4j.LoggerFactory.getLogger(ServletErrors.class).error("Request failed, correlationId={}",request.getAttribute("correlationId"),e);
        return ResponseEntity.status(status).body(ApiErrors.body(status>=500?"INTERNAL_ERROR":"INVALID_REQUEST",String.valueOf(request.getAttribute("correlationId"))));
    }
}
