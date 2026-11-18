package com.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@org.springframework.context.annotation.Import(com.orderflow.security.ServletIdentityConfig.class)
@SpringBootApplication
public class OrderServiceApplication {
    @org.springframework.context.annotation.Bean
    java.time.Clock orderClock() { return java.time.Clock.systemUTC(); }
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
