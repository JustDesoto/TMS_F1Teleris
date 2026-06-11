#!/bin/bash
# init/init-clickhouse.sh

set -e

echo "Initializing ClickHouse..."

# Загружаем переменные из .env если файл существует
if [ -f /app/.env ]; then
    export $(cat /app/.env | grep -v '^#' | xargs)
fi

# Используем переменные из окружения или значения по умолчанию
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-f1_analytics}"

sleep 5

clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery << 'EOF'
-- ============================================
-- 1. Создание и переключение на базу данных
-- ============================================
CREATE DATABASE IF NOT EXISTS f1_analytics;

-- ВАЖНО: переключаемся на базу f1_analytics
USE f1_analytics;

-- ============================================
-- 2. Основные факт-таблицы
-- ============================================

-- 2.1 Телеметрия машин
CREATE TABLE IF NOT EXISTS fact_car_data (
    session_key UInt32,
    driver_number UInt8,
    timestamp DateTime64(3),
    date Date,
    hour UInt8,
    speed_kmh UInt16,
    rpm UInt16,
    brake_percent UInt8,
    throttle_percent UInt8,
    drs_open Bool,
    gear UInt8,
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    is_speed_high Bool,
    is_full_throttle Bool,
    is_braking Bool,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, timestamp);

-- 2.2 Позиции гонщиков
CREATE TABLE IF NOT EXISTS fact_position (
    session_key UInt32,
    driver_number UInt8,
    timestamp DateTime64(3),
    date Date,
    position UInt8,
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    is_leader Bool,
    is_podium Bool,
    points UInt8,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, timestamp);

-- 2.3 Данные о кругах (все Float32 поля - Nullable)
CREATE TABLE IF NOT EXISTS fact_lap (
    session_key UInt32,
    driver_number UInt8,
    lap_number UInt8,
    date_start DateTime64(3),
    lap_duration Nullable(Float32),
    sector1_duration Nullable(Float32),
    sector2_duration Nullable(Float32),
    sector3_duration Nullable(Float32),
    speed_trap Nullable(UInt16),
    speed_i1 Nullable(UInt16),
    speed_i2 Nullable(UInt16),
    is_pit_out_lap Bool,
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_end DateTime,
    lap_time_seconds Nullable(Float32),
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, lap_number);

-- 2.4 Погодные данные
CREATE TABLE IF NOT EXISTS fact_weather (
    session_key UInt32,
    timestamp DateTime64(3),
    date Date,
    air_temperature Float32,
    track_temperature Float32,
    humidity Float32,
    pressure Float32,
    rainfall Bool,
    wind_speed Float32,
    wind_direction UInt16,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    track_temp_air_diff Float32,
    is_wet Bool,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, timestamp);

-- 2.5 Пит-стопы
CREATE TABLE IF NOT EXISTS fact_pit_stop (
    session_key UInt32,
    driver_number UInt8,
    lap_number UInt8,
    timestamp DateTime64(3),
    pit_duration Nullable(Float32),
    stop_duration Nullable(Float32),
    lane_duration Nullable(Float32),
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    total_time_lost Float32,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, lap_number);

-- 2.6 Интервалы
CREATE TABLE IF NOT EXISTS fact_interval (
    session_key UInt32,
    driver_number UInt8,
    timestamp DateTime64(3),
    date Date,
    gap_to_leader Nullable(String),
    interval_to_prev Nullable(String),
    is_lapped Bool,
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, timestamp);

-- 2.7 Race Control события
CREATE TABLE IF NOT EXISTS fact_race_control (
    session_key UInt32,
    timestamp DateTime64(3),
    date Date,
    category String,
    flag Nullable(String),
    message String,
    lap_number Nullable(UInt16),
    driver_number Nullable(UInt8),
    sector Nullable(UInt8),
    scope Nullable(String),
    qualifying_phase Nullable(String),
    driver_full_name Nullable(String),
    driver_acronym Nullable(String),
    driver_team Nullable(String),
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    is_safety_car Bool,
    is_virtual_safety_car Bool,
    is_red_flag Bool,
    is_yellow_flag Bool,
    is_green_flag Bool,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, timestamp);

-- 2.8 Обгоны
CREATE TABLE IF NOT EXISTS fact_overtake (
    session_key UInt32,
    timestamp DateTime64(3),
    date Date,
    overtaking_driver UInt8,
    overtaken_driver UInt8,
    position UInt8,
    overtaking_driver_full_name String,
    overtaking_driver_acronym String,
    overtaking_driver_team String,
    overtaking_driver_team_colour String,
    overtaken_driver_full_name String,
    overtaken_driver_acronym String,
    overtaken_driver_team String,
    overtaken_driver_team_colour String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, timestamp);

-- 2.9 Стенты
CREATE TABLE IF NOT EXISTS fact_stint (
    session_key UInt32,
    driver_number UInt8,
    stint_number UInt8,
    compound Nullable(String),
    tyre_age_at_start UInt16,
    lap_start Nullable(UInt16),
    lap_end Nullable(UInt16),
    total_laps Nullable(UInt16),
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    is_wet_tyre Bool,
    is_soft Bool,
    is_medium Bool,
    is_hard Bool,
    is_intermediate Bool,
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, stint_number);

-- 2.10 GPS локация
CREATE TABLE IF NOT EXISTS fact_location (
    session_key UInt32,
    driver_number UInt8,
    timestamp DateTime64(3),
    date Date,
    x Int32,
    y Int32,
    z Int32,
    driver_full_name String,
    driver_first_name String,
    driver_last_name String,
    driver_acronym String,
    driver_team String,
    driver_team_colour String,
    driver_broadcast_name String,
    meeting_name String,
    meeting_key UInt32,
    circuit_name String,
    circuit_key UInt32,
    session_type String,
    session_name String,
    year UInt16,
    country String,
    date_start DateTime,
    date_end DateTime,
    track_position_percent Nullable(Float32),
    etl_hash String,
    transformed_at DateTime
) ENGINE = MergeTree()
PARTITION BY year
ORDER BY (session_key, driver_number, timestamp);


-- ============================================
-- 5. Проверка
-- ============================================
SELECT 'ClickHouse initialization completed!' as message;
SELECT count() as tables_created FROM system.tables WHERE database = 'f1_analytics';
EOF

echo "✅ ClickHouse initialized!"