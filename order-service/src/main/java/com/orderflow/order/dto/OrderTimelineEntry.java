package com.orderflow.order.dto;

import java.time.Instant;
import java.util.UUID;

public record OrderTimelineEntry(long version, String previousStatus, String status, UUID sourceEventId,
        UUID correlationId, Instant occurredAt, Instant recordedAt, String reason) {}
