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

# ============================================
# ИМПОРТ ДАШБОРДОВ
# ============================================
echo "============================================================"
echo "Importing dashboards..."
echo "============================================================"

DASHBOARD_DIR="/app/superset_home/dashboards"

if [ -d "$DASHBOARD_DIR" ] && [ "$(ls -A $DASHBOARD_DIR 2>/dev/null)" ]; then
    for dashboard in $DASHBOARD_DIR/*.zip; do
        if [ -f "$dashboard" ]; then
            echo "Importing dashboard: $(basename $dashboard)"
            superset import-dashboards --path "$dashboard" 2>/dev/null || echo "Failed to import $(basename $dashboard)"
        fi
    done
    echo "✅ All dashboards imported successfully!"
else
    echo "⚠️ No dashboards found in $DASHBOARD_DIR"
    echo "   Place your exported dashboard .zip files in ./superset/dashboards/"
fi

echo "============================================================"
echo "Superset initialization completed!"
echo "Login: ${SUPERSET_ADMIN_USERNAME:-admin} / ${SUPERSET_ADMIN_PASSWORD:-admin123}"
echo "URL: http://localhost:8088"
echo "============================================================"