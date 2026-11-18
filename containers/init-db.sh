#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 -v app_password="$POSTGRES_PASSWORD" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE ROLE auth_runtime LOGIN PASSWORD :'app_password';
CREATE ROLE order_runtime LOGIN PASSWORD :'app_password';
CREATE ROLE inventory_runtime LOGIN PASSWORD :'app_password';
CREATE ROLE notification_runtime LOGIN PASSWORD :'app_password';
CREATE SCHEMA auth_schema AUTHORIZATION auth_runtime;
CREATE SCHEMA order_schema AUTHORIZATION order_runtime;
CREATE SCHEMA inventory_schema AUTHORIZATION inventory_runtime;
CREATE SCHEMA notification_schema AUTHORIZATION notification_runtime;
SQL
