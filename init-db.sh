#!/bin/bash
set -e

AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow}"
AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:-airflow}"
AIRFLOW_DB_NAME="${AIRFLOW_DB_NAME:-airflow}"
NY_TAXI_DB_NAME="${NY_TAXI_DB_NAME:-ny_taxi}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = '${AIRFLOW_DB_USER}'
    ) THEN
        EXECUTE format(
            'CREATE ROLE %I LOGIN PASSWORD %L',
            '${AIRFLOW_DB_USER}',
            '${AIRFLOW_DB_PASSWORD}'
        );
    ELSE
        EXECUTE format(
            'ALTER ROLE %I WITH LOGIN PASSWORD %L',
            '${AIRFLOW_DB_USER}',
            '${AIRFLOW_DB_PASSWORD}'
        );
    END IF;
END
\$\$;

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    '${AIRFLOW_DB_NAME}',
    '${AIRFLOW_DB_USER}'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = '${AIRFLOW_DB_NAME}'
)\gexec

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    '${NY_TAXI_DB_NAME}',
    '${AIRFLOW_DB_USER}'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = '${NY_TAXI_DB_NAME}'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB_NAME}" TO "${AIRFLOW_DB_USER}";
GRANT ALL PRIVILEGES ON DATABASE "${NY_TAXI_DB_NAME}" TO "${AIRFLOW_DB_USER}";
GRANT ALL PRIVILEGES ON DATABASE "${NY_TAXI_DB_NAME}" TO "${POSTGRES_USER}";
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$AIRFLOW_DB_NAME" <<-EOSQL
GRANT ALL ON SCHEMA public TO "${AIRFLOW_DB_USER}";
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$NY_TAXI_DB_NAME" -f /docker-entrypoint-initdb.d/init.sql

