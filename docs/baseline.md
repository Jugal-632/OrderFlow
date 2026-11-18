# OrderFlow — Baseline Specification & Verification Audit

**Audit Date:** 24 October 2026  
**Auditor:** Antigravity Agent  
**Release Plan:** OrderFlow Implementation and Delivery Plan (Revision 2.0)  
**Execution Phase:** Phase 4A (Verify Baseline and Establish Contracts)  

---

## 1. Verified Environment & Dependency Versions

| Component | Verified Version / Identifier | Source / Location | Notes |
|---|---|---|---|
| **Java** | `21.0.12.1` (Oracle Corporation) | `C:\Program Files\Java\jdk-21.0.12.1` | Language Level 21 pinned in root POM `<java.version>21</java.version>`. |
| **Spring Boot** | `3.2.5` | Root `pom.xml` | Parent POM: `spring-boot-starter-parent:3.2.5`. |
| **Spring Cloud BOM** | `2023.0.1` | Root `pom.xml` | Release Train: **Richmond** (`spring-cloud-dependencies:2023.0.1`). Compatible with Spring Boot 3.2.x. |
| **Maven Wrapper** | `0.5.6` with Apache Maven `3.9.6` | `.mvn/wrapper/` | Distribution URL pinned in `.mvn/wrapper/maven-wrapper.properties`. |
| **Node.js** | `v24.14.1` | Host environment | Frontend runtime. |
| **Frontend Package Manager** | `npm 11.13.0` | Host environment | Pinned package manager for dashboard. |
| **PostgreSQL** | `postgres:15.6` | `docker-compose.yml` | Port `5432:5432`. Local database: `orderflow_db`. |
| **Kafka Broker** | `confluentinc/cp-kafka:7.4.0` | `docker-compose.yml` | Port `9092:9092` (broker 1). |
| **ZooKeeper** | `confluentinc/cp-zookeeper:7.4.0` | `docker-compose.yml` | Port `2181:2181`. |
| **Kafka UI** | `provectuslabs/kafka-ui:v0.7.2` | `docker-compose.yml` | Port `8090:8080`. Pinned explicit version (replaced floating `latest`). |
| **Redis** | `redis:7.2.4-alpine` | `docker-compose.yml` | Port `6379:6379`. Pinned explicit version. |

---

## 2. Module Inventory

| Module Directory | Artifact ID | Role | Status |
|---|---|---|---|
| Root | `orderflow` | Parent aggregator POM | Active |
| `api-gateway/` | `api-gateway` | Spring Cloud Gateway (Port 8080) | Baseline scaffold |
| `auth-service/` | `auth-service` | Authentication & JWT Service (Port 8081) | Verified working baseline |
| `order-service/` | `order-service` | Order Lifecycle & Outbox (Port 8082) | Verified working baseline |
| `inventory-service/` | `inventory-service` | Stock Reservation & Inventory Outbox (Port 8083) | Scaffolding (Phase 4B delivery) |
| `notification-service/` | `notification-service` | In-App Notification Store (Port 8084) | Scaffolding (Phase 4C delivery) |
| `event-contracts/` | `event-contracts` | Shared Event Envelopes & Schemas | Phase 4A deliverable |

---

## 3. Baseline Audit & Smoke Test Results

All smoke tests executed against running local infrastructure (PostgreSQL, Kafka, ZooKeeper, Redis):

### 3.1 Auth Service (`:8081`)
- **Action**: Sent HTTP POST to `http://localhost:8081/auth/login` with credentials `{"email":"test@orderflow.com","password":"<local-test-password>"}`.
- **Result**: `HTTP 200 OK`.
- **Response**: Valid HS512 JWT issued with subject `test@orderflow.com` and 12-hour expiry.
- **Verification**: Database lookup against `auth_schema.users` succeeded; BCrypt verification passed.

### 3.2 Order Service (`:8082`)
- **Action**: Sent HTTP POST to `http://localhost:8082/api/orders` with Bearer JWT token from Auth Service:
  ```json
  {
    "customerId": "11111111-1111-1111-1111-111111111111",
    "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "quantity": 2
  }
  ```
- **Result**: `HTTP 200 OK` (Order ID: `e650f65f-04a7-4baa-935e-5e1d8c21d6c2`, status: `PENDING`).
- **Database Verification**:
  - `order_schema.orders`: Record persisted with status `PENDING`.
  - `order_schema.outbox_events`: Outbox row created and transitioned from `PENDING` to `PUBLISHED`.
- **Kafka Verification**:
  - Message published to Kafka topic `order.placed`:
    ```json
    {
      "orderId": "e650f65f-04a7-4baa-935e-5e1d8c21d6c2",
      "quantity": 2,
      "createdAt": "2026-10-24T06:27:19.709758600Z",
      "productId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "customerId": "11111111-1111-1111-1111-111111111111"
    }
    ```

---

## 4. Platform & Runtime Fixes Documented

1. **PostgreSQL TimeZone Handshake**:
   - On Windows systems where the JVM default timezone resolves to legacy `"Asia/Calcutta"`, PostgreSQL rejects the startup parameter with `FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"`.
   - Resolution: Application must run with JVM option `"-Duser.timezone=Asia/Kolkata"` or UTC.
2. **Artifact ID Correction in `order-service/pom.xml`**:
   - `order-service/pom.xml` originally declared `<artifactId>api-gateway</artifactId>`, which caused Maven reactor duplication failures (`DuplicateProjectException`).
   - Resolution: Fixed to `<artifactId>order-service</artifactId>`.
3. **Container Image Pinning**:
   - `kafka-ui` was updated from `:latest` to `:v0.7.2`.
   - `postgres` was pinned to `:15.6`.
   - `redis` was pinned to `:7.2.4-alpine`.

## 5. Phase 4A re-audit — 25 October 2026 (Asia/Kolkata)

This entry supersedes the older smoke observations above. The workspace already contained broad uncommitted Phase 4A changes; this audit preserved them.

- PASS: Maven wrapper contract tests: 7 tests, 0 failures/errors/skips (serialization and legacy adapter fixtures).
- PASS: rebuilt Auth and Order; live login returned 200, order creation returned 201. HTTP correlation UUID matched persisted outbox JSON and the Kafka envelope/header. Evidence: `docs/evidence/phase4a-smoke.json`; reproduction: `scripts/verify-phase4a-smoke.ps1` with Auth/Order and Compose infrastructure running.
- PASS: PostgreSQL 15.6 integration tests migrated all four service schemas from fresh state and from an explicitly baselined V1 schema. This checks synthetic V1 schemas, not backup restoration of arbitrary production data. Existing local Auth/Order also started with Flyway validation enabled.
- PASS: dependency pins inspected in parent/module POMs and image pins in Compose. Additional recorded pins: Flyway 10.10.0; Testcontainers 1.19.7; Awaitility 4.2.1; JSON Schema validator 1.4.0; JJWT 0.12.3; Spring Kafka 3.1.4 and Kafka client 3.6.2 through Boot dependency management. Maven Surefire/Failsafe 3.1.2 through Boot plugin management.
- MDC audit: existing Order HTTP filter and relay carry correlation IDs; Inventory listener adds MDC scope and its log pattern includes correlationId. Full JSON logging/observability remains a later delivery item.
- Migration inventory: Auth V1; Order V1/V2; Notification V1/V2; Inventory V1/V2 plus additive V3 for timestamp/stock-reservation/outbox invariants. Existing migration checksums were preserved.

### Reproduction environment

`mvn` is absent from PATH and the inherited JAVA_HOME points at a removed JDK. Use the checked-in wrapper with a process-local JAVA_HOME:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21.0.12.1'
$env:DOCKER_HOST = 'npipe:////./pipe/dockerDesktopLinuxEngine'
.\mvnw.cmd test -pl event-contracts
.\mvnw.cmd verify -pl inventory-service -am '-Dapi.version=1.44'
```

The API override is needed by pinned Testcontainers 1.19.7/docker-java on this host's Docker Desktop 4.81.0 / Engine 29.6.1 (minimum API 1.40). No dependency upgrade or global environment change was made. `-am` builds the shared contract dependency in the same reactor. Integration tests require Docker access and do not silently skip when Docker is unavailable.

### Inventory runtime boundary

Inventory runs on port 8083. Supply `INVENTORY_DB_URL`, `INVENTORY_DB_USER`, `INVENTORY_DB_PASSWORD`, and `KAFKA_BOOTSTRAP_SERVERS`. Flyway is disabled during normal runtime; apply migrations with deployment credentials before startup. Configure a restricted inventory runtime database role separately. Provision three-partition `order.placed`, `inventory.reserved`, `inventory.rejected`, and `order.placed.inventory-service.DLT` topics before starting its listener. Integration tests provision disposable infrastructure and run Flyway explicitly. No Phase 4C consumers or Phase 5 HTTP idempotency implementation was added.
