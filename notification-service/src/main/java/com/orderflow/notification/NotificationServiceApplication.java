package com.orderflow.notification;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Import(com.orderflow.security.ServletIdentityConfig.class)
@SpringBootApplication
public class NotificationServiceApplication {
    public static void main(String[] args) { SpringApplication.run(NotificationServiceApplication.class, args); }
    @Bean Clock notificationClock() { return Clock.systemUTC(); }
}
