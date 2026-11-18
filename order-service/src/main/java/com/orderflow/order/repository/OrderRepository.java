package com.orderflow.order.repository;

import com.orderflow.order.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID>, org.springframework.data.jpa.repository.JpaSpecificationExecutor<Order> {
    java.util.Optional<Order> findByIdAndCustomerId(UUID id,UUID customerId);
    org.springframework.data.domain.Page<Order> findByCustomerId(UUID customerId,org.springframework.data.domain.Pageable pageable);
}
