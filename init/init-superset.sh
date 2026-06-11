#!/bin/bash
# init/init-superset.sh

set -e

echo "============================================================"
echo "Superset Initialization"
echo "============================================================"

# Ждем пока Superset запустится
sleep 15

echo "Creating admin user: ${SUPERSET_ADMIN_USERNAME:-admin}..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname F1 \
    --lastname Admin \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@f1.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin123}"

echo "Initializing database..."
superset db upgrade

echo "Setting up roles..."
superset init

echo "Adding PostgreSQL database connection..."
superset set-database-uri \
    --database-name "F1 DDS" \
    --uri "postgresql://${POSTGRES_USER:-f1_user}:${POSTGRES_PASSWORD:-f1_password}@${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-f1_dds}"

echo "Adding ClickHouse database connection..."
superset set-database-uri \
    --database-name "F1 Analytics" \
    --uri "clickhousedb+connect://${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-}@${CLICKHOUSE_HOST:-clickhouse}:${CLICKHOUSE_CONNECTION_PORT:-8123}/${CLICKHOUSE_DB:-f1_analytics}"

echo "============================================================"
echo "Superset initialization completed!"
echo "Login: ${SUPERSET_ADMIN_USERNAME:-admin} / ${SUPERSET_ADMIN_PASSWORD:-admin123}"
echo "URL: http://localhost:8088"
echo "============================================================"