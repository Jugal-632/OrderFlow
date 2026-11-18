package com.orderflow.order.controller;

import com.orderflow.order.service.IdempotencyService.RequestFailure;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.Map;
import com.orderflow.security.ApiErrors;
import jakarta.servlet.http.HttpServletRequest;

@RestControllerAdvice
@org.springframework.core.annotation.Order(0)
public class RequestErrorHandler {
    @ExceptionHandler(RequestFailure.class)
    ResponseEntity<?> request(RequestFailure e,HttpServletRequest request) { return ResponseEntity.status(e.status).body(ApiErrors.body(e.code,String.valueOf(request.getAttribute("correlationId")))); }
    @ExceptionHandler({org.springframework.dao.CannotAcquireLockException.class,org.springframework.dao.QueryTimeoutException.class,
            org.springframework.transaction.TransactionTimedOutException.class,org.springframework.transaction.CannotCreateTransactionException.class})
    ResponseEntity<?> busy(Exception e,HttpServletRequest request) { return ResponseEntity.status(503).header("Retry-After","1").body(ApiErrors.body("REQUEST_RETRY_REQUIRED",String.valueOf(request.getAttribute("correlationId")))); }
    @ExceptionHandler(org.springframework.jdbc.UncategorizedSQLException.class)
    ResponseEntity<?> postgresTimeout(org.springframework.jdbc.UncategorizedSQLException e,HttpServletRequest request) {
        String state=e.getSQLException().getSQLState();
        if("55P03".equals(state) || "57014".equals(state)) return busy(e,request);
        org.slf4j.LoggerFactory.getLogger(RequestErrorHandler.class).error("Database request failed",e);
        return ResponseEntity.internalServerError().body(ApiErrors.body("INTERNAL_ERROR",String.valueOf(request.getAttribute("correlationId"))));
    }
}
