package com.orderflow.order.controller;

import com.orderflow.order.dto.OrderRequest;
import com.orderflow.order.dto.OrderResponse;
import com.orderflow.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import com.orderflow.order.dto.CreateOrderRequest;
import com.orderflow.order.service.IdempotentOrderService;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;
    private final IdempotentOrderService idempotentOrders;
    private final com.orderflow.order.service.OrderCancellationService cancellations;

    @PostMapping("/{id}/cancel")
    public ResponseEntity<String> cancel(@PathVariable UUID id,
            @RequestHeader(value="Idempotency-Key",required=false) String key,
            @RequestAttribute("authenticatedCustomerId") UUID customer,
            @RequestAttribute("correlationId") String correlation,
            @RequestBody(required=false) String body) {
        if(body!=null && !body.isBlank()) throw new com.orderflow.order.service.IdempotencyService.RequestFailure(400,"UNEXPECTED_REQUEST_BODY");
        var result=cancellations.cancel(customer,id,key,UUID.fromString(correlation));
        var response=ResponseEntity.status(result.status()).contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .location(java.net.URI.create("/api/orders/"+id));
        if(result.replayed()) response.header("Idempotent-Replayed","true");
        return response.body(result.body());
    }

    @PostMapping
    public ResponseEntity<String> placeOrder(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(value="Idempotency-Key",required=false) String key,
            @RequestAttribute("authenticatedCustomerId") UUID customer) {
        var response=idempotentOrders.create(customer,key,request);
        var builder=ResponseEntity.status(response.status()).contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .location(java.net.URI.create("/api/orders/"+response.resourceId()));
        if(response.replayed()) builder.header("Idempotent-Replayed","true");
        return builder.body(response.body());
    }

    @GetMapping("/{id}")
    public OrderResponse getOrder(@PathVariable UUID id,@RequestAttribute("caller") com.orderflow.security.Caller caller) {
        return orderService.findVisible(id,caller).orElseThrow(()->new com.orderflow.security.IdentityFailure(404,"ORDER_NOT_FOUND"));
    }

    @GetMapping public java.util.List<OrderResponse> list(@RequestAttribute("caller") com.orderflow.security.Caller caller,
            @RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size,
            @RequestParam(required=false) UUID id,@RequestParam(required=false) com.orderflow.order.model.Order.OrderStatus status) {
        return orderService.list(caller,page,size,id,status);
    }

    @GetMapping("/{id}/timeline") public java.util.List<com.orderflow.order.dto.OrderTimelineEntry> timeline(
            @PathVariable UUID id,@RequestAttribute("caller") com.orderflow.security.Caller caller) {
        return getOrder(id,caller).getTimeline();
    }
}
