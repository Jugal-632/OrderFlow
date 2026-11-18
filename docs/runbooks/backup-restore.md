# Backup and isolated restore

Before demonstration or incident repair, capture a consistent PostgreSQL custom-format backup using a deployment-approved service account and protected credential provider (`PGPASSFILE` or secret injection). Do not put passwords in shell arguments. Record database identity, timestamp, application commit and migration versions; restrict backup access and retention because it contains customer data.

```sh
pg_dump --format=custom --file=orderflow-backup.dump --dbname="$SOURCE_DATABASE"
pg_restore --list orderflow-backup.dump
createdb "$ISOLATED_RESTORE_DATABASE"
pg_restore --no-owner --exit-on-error --dbname="$ISOLATED_RESTORE_DATABASE" orderflow-backup.dump
```

Resolve both database endpoints before execution. The restore target must be a new isolated database, never the live database. Restore into the same PostgreSQL major version; restore roles/grants separately under the deployment's ownership policy. Never use `--clean` against a shared/live database. Keep each service scoped to its own schema.

Verify row counts and deterministic row hashes for all application tables, orders/history/idempotency/outboxes/inboxes, inventory/decisions/reservations, notifications, dead letters/replays/audit, and all Flyway histories. Validate migration checksums without rerunning destructive baseline operations. Reconcile available stock against reserved/released quantities; assert unique event effects and lifecycle versions. Do not start consumers against production Kafka while inspecting the restored database.

The Phase 9 T22 test uses `pg_dump`/`pg_restore` inside a disposable PostgreSQL Testcontainer, creates a distinct restore database, and compares every owned table including migration histories. Its machine-readable result is in `docs/evidence/phase9-system/scenarios.json`. This validates the procedure on the recorded test version, not a production recovery-time objective. Keep the isolated restore until reviewed, then remove only that explicitly identified test database/container.
