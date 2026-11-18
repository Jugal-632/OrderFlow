package com.orderflow.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.order.dto.*;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class IdempotentOrderService {
    private final IdempotencyService keys;
    private final OrderService orders;
    private final ObjectMapper json;
    public IdempotentOrderService(IdempotencyService keys,OrderService orders,ObjectMapper json) {
        this.keys=keys;this.orders=orders;this.json=json;
    }
    public IdempotencyService.Result create(UUID customer,String key,CreateOrderRequest input) {
        IdempotencyService.validateKey(key);
        if(input.productId()==null || input.quantity()<1) throw new IdempotencyService.RequestFailure(400,"INVALID_REQUEST");
        return keys.execute(customer,IdempotencyService.Operation.CREATE_ORDER,key,
                IdempotencyService.createHash(customer,input.productId(),input.quantity()),()->{
                    var request=new OrderRequest(); request.setCustomerId(customer);
                    request.setProductId(input.productId());request.setQuantity(input.quantity());
                    var result=orders.createOrder(request);
                    try { return new IdempotencyService.Result(201,json.writeValueAsString(result),result.getId(),false); }
                    catch(com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalStateException("Cannot serialize saved response",e); }
                });
    }
}
