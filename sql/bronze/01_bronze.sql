-- =============================================================================
-- BRONZE LAYER
-- Raw ingestion only. No cleaning, no deduplication, no type enforcement
-- beyond what's needed to land the files. Bronze is the immutable, append-
-- only record of exactly what arrived from source systems, warts and all
-- (duplicates, nulls, out-of-order arrival are all preserved here on purpose).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- -----------------------------------------------------------------------------
-- bronze.raw_events
-- Source: data/bronze_landing/events_*.csv  (daily telemetry export drops)
-- One row per raw client event exactly as delivered, including duplicates
-- from at-least-once delivery and events that arrived late (delivery lag).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.raw_events;
CREATE TABLE bronze.raw_events AS
SELECT
    *,
    current_timestamp                     AS _ingested_at,
    regexp_extract(filename, '([^/]+)$')  AS _source_file
FROM read_csv(
    'data/bronze_landing/events_*.csv',
    header = true,
    union_by_name = true,
    filename = true,
    all_varchar = true          -- land everything as text; typing happens in Silver
);

-- -----------------------------------------------------------------------------
-- bronze.raw_experiment_assignments
-- Source: data/raw/cookie_cats_real.csv  -- the REAL, untouched public
-- Cookie Cats A/B test export (90,189 real players). Loaded byte-for-byte
-- as its own raw source, exactly like an experimentation-platform export
-- would land in a real Bronze layer.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.raw_experiment_assignments;
CREATE TABLE bronze.raw_experiment_assignments AS
SELECT
    *,
    current_timestamp                     AS _ingested_at,
    regexp_extract(filename, '([^/]+)$')  AS _source_file
FROM read_csv(
    'data/raw/cookie_cats_real.csv',
    header = true,
    filename = true,
    all_varchar = true
);

-- Quick sanity counts (visible when run via duckdb CLI / python)
SELECT 'bronze.raw_events'                 AS table_name, count(*) AS row_count FROM bronze.raw_events
UNION ALL
SELECT 'bronze.raw_experiment_assignments' AS table_name, count(*) AS row_count FROM bronze.raw_experiment_assignments;
