#!/bin/sh
YELLOW="\033[33m"
WHITE="\033[0m"
ECHO_PREFIX="*** ${YELLOW}(init_postgres.sh)${WHITE} ***  "
echo "$ECHO_PREFIX === INIT SCRIPT STARTING ==="

# Exit immediately if any command fails.
# This ensures that if any step fails, the container stops instead of continuing
# with a partially initialized database.
set -e

# Connect to Postgres as the superuser specified by POSTGRES_USER.
# -v ON_ERROR_STOP=1 ensures that psql exits on any SQL error.
# -d postgres is required, psql will otherwise try to connect to db with same name as user.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres <<-EOSQL

-- Create the application user (role) if it doesn't already exist.
-- IF NOT EXISTS ensures idempotency so that restarting the container
-- does not cause an error.
DO
\$do\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$POSTGRES_USER') THEN
      CREATE ROLE "$POSTGRES_USER" WITH LOGIN PASSWORD '$POSTGRES_PASSWORD';
      -- LOGIN allows this role to connect to databases.
      -- Password is set to allow authentication.
   END IF;
END
\$do\$;

-- Grant the user permission to create databases
ALTER ROLE "$POSTGRES_USER" CREATEDB;

-- Create the working database if it doesn't already exist.
-- Assign ownership to the application user.
DO
\$do\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '$POSTGRES_DB') THEN
      CREATE DATABASE "$POSTGRES_DB" OWNER "$POSTGRES_USER";
      -- Ownership grants the user full privileges on this database
      -- (create tables, modify schema, insert/update/delete data, etc.)
   END IF;
END
\$do\$;

EOSQL
