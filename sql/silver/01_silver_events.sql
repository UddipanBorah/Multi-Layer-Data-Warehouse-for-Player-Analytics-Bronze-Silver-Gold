-- =============================================================================
-- SILVER LAYER (1/2) -- cleaned, deduplicated, typed events + sessionization
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- -----------------------------------------------------------------------------
-- silver.events_clean
-- CTE pipeline: dedupe raw (at-least-once) deliveries by event_id, cast types,
-- normalize missing metadata. This is the "one clean row per real event" table.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.events_clean;
CREATE TABLE silver.events_clean AS
WITH typed AS (
    SELECT
        event_id,
        event_name,
        CAST(client_event_time AS TIMESTAMP)   AS event_time,
        CAST(userid AS BIGINT)                 AS userid,
        COALESCE(platform, 'unknown')          AS platform,
        COALESCE(country, 'unknown')           AS country,
        app_version,
        device_id,
        TRY_CAST(rounds_completed AS INTEGER)  AS rounds_completed,
        TRY_CAST(revenue_usd AS DOUBLE)        AS revenue_usd,
        product_id,
        currency,
        _ingested_at
    FROM bronze.raw_events
),
deduped AS (
    -- Real telemetry pipelines deliver at-least-once: the same event_id can
    -- land more than once. Keep exactly one copy of each real event.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _ingested_at, event_time
        ) AS rn
    FROM typed
)
SELECT
    event_id,
    event_name,
    event_time,
    CAST(event_time AS DATE)   AS event_date,
    userid,
    platform,
    country,
    app_version,
    device_id,
    rounds_completed,
    revenue_usd,
    product_id,
    currency
FROM deduped
WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- silver.sessions
-- Classic gap-based sessionization using window functions:
--   1) LAG() to find the time since the user's previous event
--   2) a new-session flag when the gap exceeds the 30-minute inactivity rule
--      (industry-standard session cutoff) or it's the user's first event
--   3) a running SUM() of that flag to assign a monotonically increasing
--      session sequence number per user
-- Then aggregate raw events up into one row per real session.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.sessions;
CREATE TABLE silver.sessions AS
WITH ordered AS (
    SELECT
        *,
        LAG(event_time) OVER (PARTITION BY userid ORDER BY event_time) AS prev_event_time
    FROM silver.events_clean
),
flagged AS (
    SELECT
        *,
        CASE
            WHEN prev_event_time IS NULL THEN 1
            WHEN date_diff('minute', prev_event_time, event_time) > 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM ordered
),
numbered AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            PARTITION BY userid ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_seq
    FROM flagged
)
SELECT
    userid || '-' || session_seq                       AS session_id,
    userid,
    MIN(event_time)                                     AS session_start,
    MAX(event_time)                                     AS session_end,
    date_diff('second', MIN(event_time), MAX(event_time)) AS session_duration_seconds,
    CAST(MIN(event_time) AS DATE)                       AS session_date,
    COUNT(*)                                             AS event_count,
    COALESCE(SUM(rounds_completed), 0)                   AS rounds_played,
    COALESCE(SUM(revenue_usd), 0.0)                      AS revenue_usd,
    SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    ANY_VALUE(platform)                                  AS platform,
    ANY_VALUE(country)                                   AS country,
    ANY_VALUE(app_version)                               AS app_version
FROM numbered
GROUP BY userid, session_seq;

SELECT 'silver.events_clean' AS table_name, count(*) AS row_count FROM silver.events_clean
UNION ALL
SELECT 'silver.sessions'     AS table_name, count(*) AS row_count FROM silver.sessions;
