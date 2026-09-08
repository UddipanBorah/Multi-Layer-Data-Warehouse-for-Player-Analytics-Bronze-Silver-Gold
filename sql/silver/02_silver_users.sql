-- =============================================================================
-- SILVER LAYER (2/2) -- conformed user dimension
-- Joins the REAL experiment-assignment feed (ground truth: A/B version,
-- actual sum_gamerounds, actual retention_1/retention_7) with attributes
-- and an install_date derived from the cleaned event stream, and adds
-- event-derived metrics recomputed independently from raw events -- used
-- later purely as a data-quality check against the real published numbers.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.users_dim;
CREATE TABLE silver.users_dim AS
WITH real_assignments AS (
    -- the untouched, real Cookie Cats A/B test ground truth
    SELECT
        CAST(userid AS BIGINT)         AS userid,
        version                        AS ab_version,
        CAST(sum_gamerounds AS INTEGER) AS real_sum_gamerounds,
        CAST(retention_1 AS BOOLEAN)   AS real_retention_1,
        CAST(retention_7 AS BOOLEAN)   AS real_retention_7
    FROM bronze.raw_experiment_assignments
),
user_attrs AS (
    -- one representative row per user for slowly-changing attributes
    SELECT
        userid,
        MIN(event_date)                                                AS install_date,
        MODE(platform)                                                 AS platform,
        MODE(country)                                                  AS country,
        MAX(app_version)                                               AS latest_app_version
    FROM silver.events_clean
    GROUP BY userid
),
recomputed AS (
    -- independently recompute the same business metrics from raw session
    -- events, so Gold can verify they line up with the real published data.
    -- Uses calendar-DATE arithmetic (not timestamp arithmetic) so a day-1
    -- return is judged the same way a real analytics stack judges it: did
    -- the user open the app on the calendar day after install.
    SELECT
        s.userid,
        SUM(s.rounds_played) AS event_derived_rounds,
        MAX(CASE WHEN date_diff('day', a.install_date, s.session_date) = 1 THEN 1 ELSE 0 END) AS event_derived_retention_1,
        MAX(CASE WHEN date_diff('day', a.install_date, s.session_date) = 7 THEN 1 ELSE 0 END) AS event_derived_retention_7
    FROM silver.sessions s
    JOIN user_attrs a ON a.userid = s.userid
    GROUP BY s.userid
)
SELECT
    r.userid,
    r.ab_version,
    r.real_sum_gamerounds,
    r.real_retention_1,
    r.real_retention_7,
    a.install_date,
    a.platform,
    a.country,
    a.latest_app_version,
    COALESCE(rc.event_derived_rounds, 0)              AS event_derived_rounds,
    COALESCE(rc.event_derived_retention_1, 0) = 1     AS event_derived_retention_1,
    COALESCE(rc.event_derived_retention_7, 0) = 1     AS event_derived_retention_7
FROM real_assignments r
LEFT JOIN user_attrs a  ON a.userid = r.userid
LEFT JOIN recomputed rc ON rc.userid = r.userid;

SELECT 'silver.users_dim' AS table_name, count(*) AS row_count FROM silver.users_dim;
