package com.orderflow.order;

import com.orderflow.order.service.IdempotencyService;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class IdempotencyContractTest {
    @Test void keyBoundariesUsePrintableNonWhitespaceAscii() {
        IdempotencyService.validateKey("!");IdempotencyService.validateKey("~".repeat(128));
        for(String key:new String[]{null,"","x".repeat(129),"a b","a\nb","\u007f","é"})
            assertThatThrownBy(()->IdempotencyService.validateKey(key)).isInstanceOf(IdempotencyService.RequestFailure.class);
    }
    @Test void canonicalHashIsStableAndIncludesCustomerProductAndQuantity() {
        UUID customer=UUID.randomUUID(),product=UUID.randomUUID();
        String hash=IdempotencyService.createHash(customer,product,2);
        assertThat(hash).hasSize(64).isEqualTo(IdempotencyService.createHash(customer,UUID.fromString(product.toString().toUpperCase()),2));
        assertThat(hash).isNotEqualTo(IdempotencyService.createHash(UUID.randomUUID(),product,2))
                .isNotEqualTo(IdempotencyService.createHash(customer,UUID.randomUUID(),2))
                .isNotEqualTo(IdempotencyService.createHash(customer,product,3));
    }
}
