# Investigating a stuck order

Trigger: oldest pending publication or PENDING/CANCEL_PENDING age exceeds 60 seconds. Re-alert/escalate at five minutes. Assign one operator and record the incident/correlation/order IDs, observation time, and current component readiness. Check `/api/dashboard/services/health`, `/api/dashboard/kafka/topics`, and the owning dead-letter API as an ADMIN. Lag measures consumer progress, not business failure. STALE/UNAVAILABLE observations are not zero backlog.

1. Read `GET /api/orders/{id}` and its persisted timeline. A delayed order remains PENDING or CANCEL_PENDING; never mark it FAILED because of a timeout.
2. On Order's database connection, query the order's history and outbox. Preserve the original event IDs and payloads:

```sql
SELECT id,status,version,reservation_id,cancellation_id,updated_at
FROM order_schema.orders WHERE id = :order_id;
SELECT order_version,new_status,source_event_id,correlation_id,recorded_at,reason
FROM order_schema.order_history WHERE order_id = :order_id ORDER BY order_version;
SELECT id,event_type,status,retry_count,next_attempt_at,last_error
FROM order_schema.outbox_events WHERE aggregate_id = :order_id ORDER BY aggregate_version;
```

3. On Inventory's connection, inspect its immutable decision, reservation/release history and outcome publication. The private `/internal/ops/orders/{orderId}` endpoint provides this owning-service view with `X-Service-Credential`; that credential must never enter browser code, tickets, logs or screenshots.
4. Identify the missing edge: unpublished request, stopped consumer, failed reservation/release publication, unprocessed outcome, or failed notification. Check the original inbox/event ID and the relevant service-specific DLT.
5. Restore the failed dependency first. Follow `broker-and-replay.md` for publication/replay. Observe the existing order until its persisted transition completes; verify stock and unique notifications. Do not create replacement orders or manually decrement/increment stock.
6. Record cause, changed configuration, replay request ID/actor/reason, recovered event IDs, final timeline and measured recovery duration. Escalate contradictory outcomes instead of overwriting terminal state.
