package com.orderflow.order.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", schema = "order_schema")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String eventType;

    private String topic;

    private String eventKey;

    @Column(nullable = false)
    @Builder.Default
    private Long aggregateVersion = 1L;

    @Column(columnDefinition = "jsonb", nullable = false)
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(nullable = false)
    @Builder.Default
    private Instant nextAttemptAt = Instant.now();

    @Builder.Default
    private Instant createdAt = Instant.now();

    private Instant publishedAt;

    @Builder.Default
    private Integer retryCount = 0;

    private String lastError;

    public enum OutboxStatus {
        PENDING, PUBLISHED, FAILED, BLOCKED
    }
}
