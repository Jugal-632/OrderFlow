# Local stack and CI

Use Docker Desktop with Linux containers and at least 8 GiB available to Docker. All builds use the repository root. Java builder/runtime image digests and GitHub Actions commits are pinned in their definitions. The five Java containers run as `10001:10001`, with writable `/tmp` and `/app/logs`, and the Asia/Kolkata JVM timezone. Only Gateway, frontend and local infrastructure ports are published, bound to loopback. Management ports stay inside each container.

From a clean checkout:

```sh
python scripts/local_env.py
docker compose --env-file .env --profile compose config -q
docker compose --env-file .env --profile compose build -q
docker compose --env-file .env --profile compose up -d --quiet-pull --wait --wait-timeout 600
```

Open `http://localhost:5173`; Kafka UI remains on `http://localhost:8090`. Run the generator to populate ignored `.env` with random local credentials. Existing values are preserved to avoid invalidating persistent database volumes. Read bootstrap login credentials locally from `.env`; never commit them. Infrastructure-only IDE mode omits `--profile compose`; set each service's datasource, Kafka and Redis variables for its published localhost ports.

Flyway creates service tables. The new database initializer creates only isolated schema owners; it does not execute the legacy `init-db.sql`. Each service owns its schema and has no grants on peer schemas. Local owners also run migrations; cloud migration/runtime separation remains deployment work. Existing database volumes are preserved: do not mount an old baseline volume into this fresh-stack setup without following the documented baseline/migration procedure. Use a distinct Compose project for testing rather than deleting existing volumes.

The default fixture has no inventory product. Insert a deliberate local product through the owning database, or run the isolated verification script below, which creates its own product. Startup never resets stock.

Verification uses a separate project and never deletes volumes:

```sh
docker compose --env-file .env --profile compose -p orderflow-phase10 build -q
docker compose --env-file .env --profile compose -p orderflow-phase10 up -d --quiet-pull --wait --wait-timeout 600
python scripts/verify-phase10.py
docker compose --env-file .env --profile compose -p orderflow-phase10 down
```

Stop a different stack using the same published ports before running this isolated project, or override the six port variables. Readiness is polled through Docker health status, with bounded deadlines; dependencies use `service_healthy`, never startup sleeps. `compose down` retains databases and Redis state. Do not use `down -v` against data you need.

The verification script checks every application UID/GID and writable directory, customer/admin access boundaries, confirmation and cancellation, then three recovery cases. A test-database trigger holds an Order write while the container is killed; the uncommitted idempotency claim must disappear and retry must succeed. Inventory is stopped with an unprocessed placement and must catch up after restart. A test session holds Inventory's relay advisory lock while its real decision commits; the broker and Inventory are stopped with a pending outcome, then restarted, and exactly one reservation must remain. These hooks exist only in the disposable test database/session and are removed in `finally`; no failpoint is compiled into release images. On interruption, inspect the isolated database for `phase10_hold` triggers before reusing it.

`.github/workflows/ci.yml` runs Java unit/contracts/integration tests (including fresh/populated Flyway fixtures), frontend syntax/unit/browser checks, image builds, Compose smoke/chaos checks, and pinned Trivy vulnerability/secret scans with CycloneDX inventories. HIGH/CRITICAL findings fail the gate; reports upload even on failure. Images are tagged with the commit SHA. Publishing is deliberately not configured until a repository/registry destination exists and the gates pass. Workflow configuration is not evidence of a successful hosted run; retain the eventual Actions run URL with the release evidence.

Build/test/scan results are stored under `docs/evidence/phase10/`. See the Phase 10 checkpoint there for actual outcomes and unresolved gates.

## Phase 10 dependency remediation

The authorized security remediation upgrades the parent to Spring Boot 4.0.9 and the Spring Cloud BOM to 2025.1.4. Jackson 2 compatibility preserves the existing HTTP/event contract while the Boot starters, health APIs and Gateway property prefix follow Boot 4. Kafka compression and Bouncy Castle dependencies have explicit security-fix pins in the root POM. The frontend runtime includes Node but removes unused package managers.

The original and remediated scans are retained separately in `docs/evidence/phase10/`. `remediation.md` records the changes and test-attempt history. The final summary checks that the scanned, inspected and live-tested Java image IDs match.

For the full system and performance regression, add `-Dphase9.system=true -Dphase9.benchmark=true -Dsystem.service.heapMiB=496 -Dsystem.evidence.directory=docs/evidence/phase10/system` to Maven `verify`. Run this workload without concurrent image builds or another application stack. The evidence directory override preserves the original Phase 9 reports. `scripts/collect-phase10-regression.py` archives completed JUnit and benchmark results; it fails if a required suite or performance gate is incomplete.


ZooKeeper data and transaction logs now use named volumes alongside Kafka data. The verification script includes a complete down/up cycle and checks that broker metadata and business data survive together. A stack created by the earlier Kafka-only persistence configuration can have mismatched cluster metadata after recreation; preserve its data and restore the matching ZooKeeper backup rather than editing Kafka's cluster ID. The final local verification uses `PHASE10_COMPOSE_PROJECT=orderflow-phase10-final` to preserve the earlier isolated project's volumes.

Order pre-sizes its JVM heap to the existing 65% allocation within the 768 MiB container limit. Its benchmark JVM mirrors that allocation with a 496 MiB initial/maximum heap; other service JVMs retain 64 MiB initial heaps and 496 MiB maximums. This setting was introduced to investigate the measured Order outbox warm-up backlog; subsequent failed benchmarks mean it is not a verified performance fix. It operates without changing stock processing, event ordering, acknowledgment rules, batch size, or polling delay. See the final performance report for the measured result.

### Accepted Phase 10 benchmark

The accepted report is `docs/evidence/phase10/compose-performance-retry/performance.json`: create p95 78.189 ms, confirmation p95 1,916.007 ms, zero errors and reconciled stock/events. Existing application images were used, with the optional isolated Kafka UI stopped during measurement. Host settings were unchanged; allow adequate host memory and avoid competing workloads. Earlier failed attempts show sensitivity to the local runtime and remain preserved.

To reproduce, start the isolated Compose project on the documented alternate ports, set `PHASE10_COMPOSE_PROJECT=orderflow-phase10-final`, `GATEWAY_PORT=18080`, and `PHASE10_PERFORMANCE_DIR` to a new directory under `docs/evidence/phase10/`, then run `python scripts/benchmark-phase10.py`. The driver refuses to overwrite existing results, verifies scanned image IDs, preserves durability, and includes a forced checkpoint during load. Repeat only when relevant code, images or runtime configuration changes.
