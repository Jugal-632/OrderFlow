# OrderFlow operations dashboard

Run `npm ci` and `npm run dev`, then open `http://127.0.0.1:5173` and sign in with an existing ADMIN account. The development server forwards `/auth` and `/api` exclusively to Gateway (`GATEWAY_URL`, default `http://127.0.0.1:8080`). For cloud hosting, serve these static assets behind the same-origin Gateway routes; no service credentials belong in frontend configuration.

Set the same deployment-provided `INTERNAL_SERVICE_TOKEN` (at least 32 characters) on order, inventory, and notification services. Order uses `INVENTORY_SERVICE_URL` and `NOTIFICATION_SERVICE_URL` for its fixed internal aggregation targets. Their local defaults are ports 8083 and 8084. Keep these internal endpoints on the private network. Missing credentials or dependency failures appear as unavailable components.

Run `npm test` for memory-only authentication and polling checks. Run `npm run test:browser` with Google Chrome installed for the six-page contract, recovery, outage, and responsive-layout tests. Browser tests use explicit API fixtures; backend integration tests exercise PostgreSQL, Kafka, and actual HTTP handlers. Test screenshots are fixtures, not production metrics.

`python scripts/package-evidence.py` from the repository root packages generated `docs/evidence/phase*-test-summary.json` files into order-service resources before its release build. The dashboard exposes those validated reports read-only. Gateway access-denial audit logs persist to `logs/gateway.log` (override `GATEWAY_LOG_FILE`); replay attempts and results persist in each service's PostgreSQL schema.
