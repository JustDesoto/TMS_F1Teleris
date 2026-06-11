-- init-postgres.sql
-- Создает таблицы для DDS слоя

-- Подключаемся к базе данных f1_dds
\c f1_dds;

-- ============================================
-- Dimension Tables (измерения)
-- ============================================

-- Сессии
CREATE TABLE IF NOT EXISTS dim_session (
    session_key INT PRIMARY KEY,
    meeting_key INT,
    session_name VARCHAR(100),
    session_type VARCHAR(50),
    date_start TIMESTAMP,
    date_end TIMESTAMP,
    circuit_key INT,
    circuit_name VARCHAR(100),
    country_name VARCHAR(50),
    country_code VARCHAR(3),
    location VARCHAR(100),
    year INT,
    is_cancelled BOOLEAN DEFAULT FALSE,
    gmt_offset VARCHAR(10),
    etl_hash VARCHAR(64),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Комментарии
COMMENT ON TABLE dim_session IS 'Информация о сессиях (гонки, квалификации, практики)';
COMMENT ON COLUMN dim_session.session_key IS 'Уникальный ключ сессии (Primary Key)';

-- Встречи (этапы чемпионата)
CREATE TABLE IF NOT EXISTS dim_meeting_session (
    meeting_key INT,
    session_key INT,
    meeting_name VARCHAR(200),
    meeting_official_name VARCHAR(200),
    location VARCHAR(100),
    country_name VARCHAR(50),
    country_code VARCHAR(3),
    date_start DATE,
    date_end DATE,
    year INT,
    circuit_key INT,
    circuit_short_name VARCHAR(100),
    circuit_type VARCHAR(50),
    gmt_offset VARCHAR(10),
    is_cancelled BOOLEAN DEFAULT FALSE,
    etl_hash VARCHAR(64),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (meeting_key, session_key)
);

-- Пилоты (снимок на сессию)
CREATE TABLE IF NOT EXISTS dim_driver_session (
    session_key INT,
    driver_number INT,
    full_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    team_name VARCHAR(100),
    team_colour VARCHAR(7),
    acronym CHAR(3),
    broadcast_name VARCHAR(50),
    headshot_url TEXT,
    meeting_key INT,
    etl_hash VARCHAR(64),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_key, driver_number)
);

-- ============================================
-- Fact Tables (факты)
-- ============================================

-- Стартовая решетка
CREATE TABLE IF NOT EXISTS fact_starting_grid (
    session_key INT,
    driver_number INT,
    position INT,
    lap_duration FLOAT,
    etl_hash VARCHAR(64),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_key, driver_number)
);

-- Результаты сессии
CREATE TABLE IF NOT EXISTS fact_session_result (
    session_key INT,
    driver_number INT,
    position INT,
    -- Квалификация (Q1, Q2, Q3)
    gap_to_leader_q1 VARCHAR(20),
    gap_to_leader_q2 VARCHAR(20),
    gap_to_leader_q3 VARCHAR(20),
    duration_q1 FLOAT,
    duration_q2 FLOAT,
    duration_q3 FLOAT,
    -- Гонка
    gap_to_leader_race VARCHAR(20),
    duration_race FLOAT,
    -- Общие поля
    number_of_laps INT,
    dnf BOOLEAN DEFAULT FALSE,
    dns BOOLEAN DEFAULT FALSE,
    dsq BOOLEAN DEFAULT FALSE,
    etl_hash VARCHAR(64),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_key, driver_number)
);


-- ============================================
-- 1. ВИТРИНА ДЛЯ ГОНОК (RACE)
-- ============================================

CREATE MATERIALIZED VIEW gold_race_analytics_mv AS
SELECT 
    ROW_NUMBER() OVER () as id,
    s.session_key,
    s.year,
    s.session_name,
    s.session_type,
    s.circuit_name,
    s.country_name as country,
    s.date_start,
    s.date_end,
    d.driver_number,
    d.full_name as driver_full_name,
    d.first_name,
    d.last_name,
    d.team_name as driver_team,
    d.acronym,
    d.team_colour,
    r.position,
    r.duration_race as race_time_seconds,
    TO_CHAR(INTERVAL '1 second' * r.duration_race, 'MI:SS.MS') as race_time_formatted,
    r.gap_to_leader_race,
    r.number_of_laps,
    r.dnf,
    r.dns,
    r.dsq,
    CASE 
        WHEN s.session_name = 'Race' AND r.position = 1 THEN 25
        WHEN s.session_name = 'Race' AND r.position = 2 THEN 18
        WHEN s.session_name = 'Race' AND r.position = 3 THEN 15
        WHEN s.session_name = 'Race' AND r.position = 4 THEN 12
        WHEN s.session_name = 'Race' AND r.position = 5 THEN 10
        WHEN s.session_name = 'Race' AND r.position = 6 THEN 8
        WHEN s.session_name = 'Race' AND r.position = 7 THEN 6
        WHEN s.session_name = 'Race' AND r.position = 8 THEN 4
        WHEN s.session_name = 'Race' AND r.position = 9 THEN 2
        WHEN s.session_name = 'Race' AND r.position = 10 THEN 1
        WHEN s.session_name = 'Sprint' AND r.position = 1 THEN 8
        WHEN s.session_name = 'Sprint' AND r.position = 2 THEN 7
        WHEN s.session_name = 'Sprint' AND r.position = 3 THEN 6
        WHEN s.session_name = 'Sprint' AND r.position = 4 THEN 5
        WHEN s.session_name = 'Sprint' AND r.position = 5 THEN 4
        WHEN s.session_name = 'Sprint' AND r.position = 6 THEN 3
        WHEN s.session_name = 'Sprint' AND r.position = 7 THEN 2
        WHEN s.session_name = 'Sprint' AND r.position = 8 THEN 1
        ELSE 0
    END as points,
    CASE WHEN r.position = 1 THEN true ELSE false END as is_winner,
    CASE WHEN r.position <= 3 THEN true ELSE false END as is_podium,
    CASE 
        WHEN s.session_name = 'Race' AND r.position <= 10 THEN true
        WHEN s.session_name = 'Sprint' AND r.position <= 8 THEN true
        ELSE false
    END as is_points_finish,
    r.loaded_at as data_loaded_at
FROM fact_session_result r
JOIN dim_driver_session d 
    ON r.session_key = d.session_key 
    AND r.driver_number = d.driver_number
JOIN dim_session s 
    ON r.session_key = s.session_key
WHERE s.session_type = 'Race'
  AND s.session_name IN ('Race', 'Sprint')
WITH DATA;

-- Индексы (исправлены названия колонок)
CREATE UNIQUE INDEX idx_gold_race_id ON gold_race_analytics_mv(id);
CREATE INDEX idx_gold_race_year ON gold_race_analytics_mv(year);
CREATE INDEX idx_gold_race_session_name ON gold_race_analytics_mv(session_name);
CREATE INDEX idx_gold_race_team ON gold_race_analytics_mv(driver_team);  -- ← исправлено
CREATE INDEX idx_gold_race_position ON gold_race_analytics_mv(position);
CREATE INDEX idx_gold_race_points ON gold_race_analytics_mv(points);
CREATE INDEX idx_gold_race_driver ON gold_race_analytics_mv(driver_number);
CREATE INDEX idx_gold_race_session ON gold_race_analytics_mv(session_key);

COMMENT ON MATERIALIZED VIEW gold_race_analytics_mv IS 'Объединенная витрина для Race и Sprint';


-- ============================================
-- 2. ВИТРИНА ДЛЯ КВАЛИФИКАЦИИ (QUALIFYING)
-- ============================================

CREATE MATERIALIZED VIEW gold_qualifying_analytics_mv AS
SELECT 
    ROW_NUMBER() OVER () as id,
    s.session_key,
    s.year,
    s.session_name,
    s.circuit_name,
    s.country_name as country,
    s.date_start,
    d.driver_number,
    d.full_name as driver_full_name,
    d.first_name,
    d.last_name,
    d.team_name as driver_team,
    d.acronym,
    d.team_colour,
    sg.position as starting_grid_position,
    r.duration_q1 as q1_time_seconds,
    r.duration_q2 as q2_time_seconds,
    r.duration_q3 as q3_time_seconds,
    TO_CHAR(INTERVAL '1 second' * r.duration_q1, 'MI:SS.MS') as q1_time_formatted,
    TO_CHAR(INTERVAL '1 second' * r.duration_q2, 'MI:SS.MS') as q2_time_formatted,
    TO_CHAR(INTERVAL '1 second' * r.duration_q3, 'MI:SS.MS') as q3_time_formatted,
    r.gap_to_leader_q1,
    r.gap_to_leader_q2,
    r.gap_to_leader_q3,
    CASE WHEN r.duration_q1 IS NOT NULL THEN true ELSE false END as participated_in_q1,
    CASE WHEN r.duration_q2 IS NOT NULL THEN true ELSE false END as participated_in_q2,
    CASE WHEN r.duration_q3 IS NOT NULL THEN true ELSE false END as participated_in_q3,
    LEAST(
        COALESCE(r.duration_q1, 999999),
        COALESCE(r.duration_q2, 999999),
        COALESCE(r.duration_q3, 999999)
    ) as best_qualifying_time,
    r.loaded_at as data_loaded_at
FROM fact_session_result r
JOIN dim_driver_session d 
    ON r.session_key = d.session_key 
    AND r.driver_number = d.driver_number
JOIN dim_session s 
    ON r.session_key = s.session_key
LEFT JOIN fact_starting_grid sg
    ON r.session_key = sg.session_key 
    AND r.driver_number = sg.driver_number
WHERE s.session_type = 'Qualifying'
WITH DATA;

-- Индексы для qualifying витрины (исправлены названия колонок)
CREATE UNIQUE INDEX idx_gold_qual_id ON gold_qualifying_analytics_mv(id);
CREATE INDEX idx_gold_qual_year ON gold_qualifying_analytics_mv(year);
CREATE INDEX idx_gold_qual_team ON gold_qualifying_analytics_mv(driver_team);  -- ← исправлено
CREATE INDEX idx_gold_qual_start_pos ON gold_qualifying_analytics_mv(starting_grid_position);
CREATE INDEX idx_gold_qual_driver ON gold_qualifying_analytics_mv(driver_number);
CREATE INDEX idx_gold_qual_session ON gold_qualifying_analytics_mv(session_key);

COMMENT ON MATERIALIZED VIEW gold_qualifying_analytics_mv IS 'Витрина для анализа квалификаций';


-- ============================================
-- 3. ВИТРИНА ДЛЯ ЧЕМПИОНАТА
-- ============================================

DROP MATERIALIZED VIEW IF EXISTS gold_driver_championship_mv CASCADE;

CREATE MATERIALIZED VIEW gold_driver_championship_mv AS
SELECT 
    ROW_NUMBER() OVER () as id,
    s.year,
    d.driver_number,
    d.full_name as driver_full_name,
    d.team_name as driver_team,
    d.acronym,
    d.team_colour,
    
    COUNT(DISTINCT CASE WHEN s.session_name = 'Race' THEN r.session_key END) AS races_participated,
    COUNT(DISTINCT CASE WHEN s.session_name = 'Sprint' THEN r.session_key END) AS sprints_participated,
    COUNT(DISTINCT r.session_key) AS total_events,
    
    SUM(CASE WHEN s.session_name = 'Race' AND r.position = 1 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN s.session_name = 'Sprint' AND r.position = 1 THEN 1 ELSE 0 END) AS sprint_wins,
    SUM(CASE WHEN r.position = 1 THEN 1 ELSE 0 END) AS total_wins,
    
    SUM(CASE WHEN s.session_name = 'Race' AND r.position <= 3 THEN 1 ELSE 0 END) AS podiums,
    SUM(CASE WHEN s.session_name = 'Sprint' AND r.position <= 3 THEN 1 ELSE 0 END) AS sprint_podiums,
    SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS total_podiums,
    
    SUM(CASE WHEN s.session_name = 'Race' AND r.position <= 10 THEN 1 ELSE 0 END) AS points_finishes,
    SUM(CASE WHEN s.session_name = 'Sprint' AND r.position <= 8 THEN 1 ELSE 0 END) AS sprint_points_finishes,
    
    SUM(
        CASE 
            WHEN s.session_name = 'Race' AND r.position = 1 THEN 25
            WHEN s.session_name = 'Race' AND r.position = 2 THEN 18
            WHEN s.session_name = 'Race' AND r.position = 3 THEN 15
            WHEN s.session_name = 'Race' AND r.position = 4 THEN 12
            WHEN s.session_name = 'Race' AND r.position = 5 THEN 10
            WHEN s.session_name = 'Race' AND r.position = 6 THEN 8
            WHEN s.session_name = 'Race' AND r.position = 7 THEN 6
            WHEN s.session_name = 'Race' AND r.position = 8 THEN 4
            WHEN s.session_name = 'Race' AND r.position = 9 THEN 2
            WHEN s.session_name = 'Race' AND r.position = 10 THEN 1
            WHEN s.session_name = 'Sprint' AND r.position = 1 THEN 8
            WHEN s.session_name = 'Sprint' AND r.position = 2 THEN 7
            WHEN s.session_name = 'Sprint' AND r.position = 3 THEN 6
            WHEN s.session_name = 'Sprint' AND r.position = 4 THEN 5
            WHEN s.session_name = 'Sprint' AND r.position = 5 THEN 4
            WHEN s.session_name = 'Sprint' AND r.position = 6 THEN 3
            WHEN s.session_name = 'Sprint' AND r.position = 7 THEN 2
            WHEN s.session_name = 'Sprint' AND r.position = 8 THEN 1
            ELSE 0
        END
    ) AS total_points,
    
    SUM(CASE WHEN s.session_name = 'Race' THEN 
        CASE 
            WHEN r.position = 1 THEN 25
            WHEN r.position = 2 THEN 18
            WHEN r.position = 3 THEN 15
            WHEN r.position = 4 THEN 12
            WHEN r.position = 5 THEN 10
            WHEN r.position = 6 THEN 8
            WHEN r.position = 7 THEN 6
            WHEN r.position = 8 THEN 4
            WHEN r.position = 9 THEN 2
            WHEN r.position = 10 THEN 1
            ELSE 0
        END
    ELSE 0 END) AS race_points,
    
    SUM(CASE WHEN s.session_name = 'Sprint' THEN 
        CASE 
            WHEN r.position = 1 THEN 8
            WHEN r.position = 2 THEN 7
            WHEN r.position = 3 THEN 6
            WHEN r.position = 4 THEN 5
            WHEN r.position = 5 THEN 4
            WHEN r.position = 6 THEN 3
            WHEN r.position = 7 THEN 2
            WHEN r.position = 8 THEN 1
            ELSE 0
        END
    ELSE 0 END) AS sprint_points,
    
    ROUND(AVG(r.position), 2) AS avg_finish_position,
    MIN(r.position) AS best_finish,
    MAX(r.position) AS worst_finish,
    
    SUM(CASE WHEN r.dnf = true THEN 1 ELSE 0 END) AS dnf_count,
    ROUND(100.0 * SUM(CASE WHEN r.dnf = true THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric, 2) AS dnf_percentage,
    
    NOW() AS view_refreshed_at
    
FROM fact_session_result r
JOIN dim_driver_session d 
    ON r.session_key = d.session_key 
    AND r.driver_number = d.driver_number
JOIN dim_session s 
    ON r.session_key = s.session_key
WHERE s.session_type = 'Race'
  AND s.session_name IN ('Race', 'Sprint')
GROUP BY s.year, d.driver_number, d.full_name, d.team_name, d.acronym, d.team_colour
WITH DATA;

-- Индексы для чемпионата (исправлены названия колонок)
CREATE UNIQUE INDEX idx_gold_champ_id ON gold_driver_championship_mv(id);
CREATE INDEX idx_gold_champ_year ON gold_driver_championship_mv(year);
CREATE INDEX idx_gold_champ_points ON gold_driver_championship_mv(total_points DESC);
CREATE INDEX idx_gold_champ_wins ON gold_driver_championship_mv(total_wins DESC);
CREATE INDEX idx_gold_champ_team ON gold_driver_championship_mv(driver_team);  -- ← исправлено

COMMENT ON MATERIALIZED VIEW gold_driver_championship_mv IS 'Агрегированная витрина чемпионата (учитывает Race и Sprint)';


-- ============================================
-- Функция для обновления (БЕЗ CONCURRENTLY или с CONCURRENTLY)
-- ============================================
CREATE OR REPLACE FUNCTION refresh_all_gold_views()
RETURNS TEXT AS $$
BEGIN
    -- Если нужно обновить без блокировок (CONCURRENTLY) - нужны уникальные индексы
    -- У нас они есть (id), поэтому можно использовать CONCURRENTLY
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_race_analytics_mv;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_qualifying_analytics_mv;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_driver_championship_mv;
    
    RETURN 'Gold views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- Альтернативная функция без CONCURRENTLY (блокирует чтение)
CREATE OR REPLACE FUNCTION refresh_all_gold_views_simple()
RETURNS TEXT AS $$
BEGIN
    REFRESH MATERIALIZED VIEW gold_race_analytics_mv;
    REFRESH MATERIALIZED VIEW gold_qualifying_analytics_mv;
    REFRESH MATERIALIZED VIEW gold_driver_championship_mv;
    
    RETURN 'Gold views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Индексы
-- ============================================

-- dim_session
CREATE INDEX IF NOT EXISTS idx_dim_session_year ON dim_session(year);
CREATE INDEX IF NOT EXISTS idx_dim_session_meeting ON dim_session(meeting_key);
CREATE INDEX IF NOT EXISTS idx_dim_session_date ON dim_session(date_start);

-- dim_driver_session
CREATE INDEX IF NOT EXISTS idx_dim_driver_session_number ON dim_driver_session(driver_number);
CREATE INDEX IF NOT EXISTS idx_dim_driver_team ON dim_driver_session(team_name);

-- fact_session_result
CREATE INDEX IF NOT EXISTS idx_fact_session_result_position ON fact_session_result(position);
CREATE INDEX IF NOT EXISTS idx_fact_session_result_dnf ON fact_session_result(dnf);

-- ============================================
-- Служебная таблица для метаданных ETL
-- ============================================
CREATE TABLE IF NOT EXISTS etl_metadata (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    last_loaded_session INT,
    last_loaded_at TIMESTAMP,
    records_loaded INT,
    status VARCHAR(20),
    error_message TEXT
);

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_etl_metadata_table ON etl_metadata(table_name);


CREATE TABLE IF NOT EXISTS etl_session_queue (
    session_key     INTEGER      PRIMARY KEY,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',
    session_name    TEXT,
    date_end        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Индекс для быстрой фильтрации по статусу
CREATE INDEX IF NOT EXISTS idx_etl_session_queue_status 
    ON etl_session_queue(status);

-- ============================================
-- Вывод информации о созданных таблицах
-- ============================================
SELECT 'PostgreSQL initialization completed!' as message;