# Confirmed-order cancellation

Send `POST /api/orders/{id}/cancel` through the gateway, with a valid bearer token and `Idempotency-Key`, and no request body. Only the owning user can cancel, including when that user has ADMIN. Other owners' orders return 404.

The initial response is 202 with `id`, `status: CANCEL_PENDING`, `version`, and `cancellationId`, plus a Location header pointing to the order. Poll the order/timeline API for completion. PENDING, FAILED, CANCEL_PENDING, and CANCELLED orders reject a new cancellation key with 409. Replaying the original successful key returns the saved 202 body with `Idempotent-Replayed: true`, including after the order becomes CANCELLED. The key is scoped to user and CANCEL_ORDER; changing the order ID for that key returns `IDEMPOTENCY_KEY_REUSED`.

Order commits the cancellation state, history, immutable `order.cancel.requested` event, and saved response together. Inventory locks the original reservation, verifies its original owner and decision, restores its stored quantity, records release/cancellation and version, and writes `inventory.released` in one transaction. Order accepts only a matching release, commits CANCELLED and history, and writes `order.cancelled`. The existing Notification consumer then records ORDER_CANCELLED. Notification cannot be emitted by the cancellation request alone.

Inbox records guard exact event replays. Reservation and cancellation identifiers also guard semantic duplicates with new event IDs. Conflicting cancellation/release identifiers or a missing reservation are protocol errors sent through the existing DLT policy, without inventing a successful release. Transient infrastructure failure retains the existing retry/pause behavior. A stopped Inventory consumer leaves durable work in Kafka and Order in CANCEL_PENDING; it completes after consumption resumes. Relays publish the original payload and retain their ACK, retry, locking and per-aggregate ordering rules.

Apply Inventory Flyway V5 before running the new consumer. It adds a reservation version (reserve = 1, release = 2); the original inventory decision remains immutable. Existing migrations are unchanged. New cancellation/release topics and service-specific DLT topics are declared with three partitions, matching listener concurrency. Production topic provisioning must honor the same partition counts.

Inventory now retains a regular library JAR for the cross-service integration test dependency and packages its runnable application as `inventory-service-1.0.0-exec.jar`, matching Notification's packaging convention. Use the `-exec.jar` when launching Inventory with `java -jar`.

Phase 8 dashboard and recovery UI work is outside this implementation.
