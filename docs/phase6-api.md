# Phase 6: identity and gateway contract

The browser/API base URL is `http://localhost:8080`. `requests.http` uses this gateway for every request. This repository has no frontend application yet; the gateway integration tests exercise browser headers, preflight, routing, and limits over HTTP.

## Configuration

Generate one signing key for the local environment and supply the same value to Auth, Gateway, Order, Inventory, and Notification. Never commit the value:

```powershell
$env:JWT_SECRET = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(64))
$env:AUTH_SERVICE_URL = 'http://127.0.0.1:8081'
$env:ORDER_SERVICE_URL = 'http://127.0.0.1:8082'
$env:INVENTORY_SERVICE_URL = 'http://127.0.0.1:8083'
$env:NOTIFICATION_SERVICE_URL = 'http://127.0.0.1:8084'
$env:CORS_ALLOWED_ORIGINS = 'http://localhost:3000,http://localhost:5173'
$env:BOOTSTRAP_CUSTOMER_EMAIL = 'test@orderflow.com'
$env:BOOTSTRAP_ADMIN_EMAIL = 'admin@orderflow.com'
```

Supply `BOOTSTRAP_CUSTOMER_PASSWORD` and `BOOTSTRAP_ADMIN_PASSWORD` securely (at least 12 characters). Launch services from the same environment. Bootstrap creates missing users and roles; it does not reset existing passwords. Migration assigns CUSTOMER to existing users. No demo password or deployable signing key is embedded in configuration. JWTs issued before this phase are intentionally invalid; log in again.

For cloud deployment, inject `JWT_SECRET` from Secrets Manager, set the `cloud` gateway profile, provide the exact frontend domain in `CORS_ALLOWED_ORIGINS`, and set private service DNS URLs and `REDIS_HOST`. Downstream services bind to loopback locally. Set `SERVICE_BIND_ADDRESS=0.0.0.0` only behind private service networking; permit incoming service traffic only from the gateway's security group. Cloud infrastructure provisioning belongs to the later deployment phase and was not performed here. Login's IP bucket uses the immediate TCP peer, ignoring untrusted forwarded headers; a future load-balancer deployment must supply a trusted client-IP policy before enabling forwarded IPs.

## Identity and requests

- Login returns `accessToken`, `expiresIn: 1800`, `userId`, and `roles`. HS512 tokens carry UUID subject, CUSTOMER/ADMIN roles, issuer `orderflow-auth`, audience `orderflow-api`, issued/expiry times, and a UUID token ID. Override issuer/audience consistently with `JWT_ISSUER`/`JWT_AUDIENCE`.
- Send `Authorization: Bearer <accessToken>`. Services independently validate tokens and Redis revocation. `X-User-Id`, `X-Customer-Id`, and role headers never establish identity.
- Create-order accepts only `productId` and `quantity`, plus `Idempotency-Key`. Customer ID comes from the validated token. Explicit customer identity fields fail with 400.
- CUSTOMER sees only owned orders and timelines; another owner's ID returns 404. ADMIN can inspect all orders. Inventory, notifications, dashboard and operational routes require ADMIN, including direct downstream access.
- Logout returns 204 only after writing revocation with the remaining token lifetime. Redis failure returns 503 with `Retry-After`; recovery does not require a service restart.

## Limits and read APIs

Login has a distributed 10/minute token bucket with capacity 10 per peer IP. Each authenticated user has separate read and mutation buckets: 300/minute with capacity 50, and 60/minute with capacity 20. Buckets refill continuously using Redis time. A depleted bucket returns 429 and a computed `Retry-After` in seconds.

The gateway bounds request bodies to 64 KiB, including chunked bodies, uses a 2-second connection timeout and 10-second upstream response timeout, and does not retry mutations. Downstream HTTP services also bound bodies to 64 KiB. CORS allows explicit origins and Authorization, Content-Type, Idempotency-Key, and X-Correlation-Id; responses expose Location, X-Correlation-Id, Idempotent-Replayed, and Retry-After.

Lists accept `page` (zero-based, 0–10000) and `size` (default 20, maximum 100), and return bounded arrays. Orders sort by creation time then ID descending. Stock uses immutable `created_at` then product ID. Reservations and notifications use their creation timestamps (`reserved_at` / `recorded_at`) then ID. Timeline is version ordered. UUID filters are validated; Notification accepts optional `orderId`.

Operational route selectors are stripped (`/api/ops/order/...` → `/api/ops/...`). Internal paths are not routed. This phase establishes access and routing for later dashboard/operational implementations; it does not implement cancellation, compensation, replay, or dashboards.

Errors contain `code`, `message`, `correlationId`, `timestamp`, and `fieldErrors`. Correlation IDs are validated UUIDs or replaced with a newly generated UUID.
