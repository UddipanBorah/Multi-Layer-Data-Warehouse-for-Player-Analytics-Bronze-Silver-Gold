-- =============================================================================
-- GOLD LAYER -- Retention marts
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- gold.mart_retention_curve
-- Day-by-day (day-since-install) retention curve per A/B arm, computed from
-- event-derived sessions, with a 3-day trailing moving average (window fn).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_retention_curve;
CREATE TABLE gold.mart_retention_curve AS
WITH cohort_size AS (
    SELECT ab_version, COUNT(*) AS cohort_users
    FROM silver.users_dim
    GROUP BY ab_version
),
activity AS (
    SELECT
        s.userid,
        u.ab_version,
        date_diff('day', u.install_date, s.session_date) AS day_since_install
    FROM silver.sessions s
    JOIN silver.users_dim u ON u.userid = s.userid
),
daily_active AS (
    SELECT
        ab_version,
        day_since_install,
        COUNT(DISTINCT userid) AS active_users
    FROM activity
    WHERE day_since_install BETWEEN 0 AND 13
    GROUP BY ab_version, day_since_install
)
SELECT
    d.ab_version,
    d.day_since_install,
    d.active_users,
    c.cohort_users,
    ROUND(d.active_users * 1.0 / c.cohort_users, 4) AS retention_rate,
    ROUND(
        AVG(d.active_users * 1.0 / c.cohort_users) OVER (
            PARTITION BY d.ab_version ORDER BY d.day_since_install
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 4
    ) AS retention_rate_3day_trailing_avg
FROM daily_active d
JOIN cohort_size c ON c.ab_version = d.ab_version
ORDER BY d.ab_version, d.day_since_install;

-- -----------------------------------------------------------------------------
-- gold.mart_retention_validation
-- Data-quality check: does the retention we RECOMPUTED from raw events match
-- the REAL published retention_1 / retention_7 figures? Should match closely
-- (small deltas only come from the 30-day acquisition-window date arithmetic).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_retention_validation;
CREATE TABLE gold.mart_retention_validation AS
SELECT
    ab_version,
    COUNT(*)                                              AS users,
    ROUND(AVG(CASE WHEN real_retention_1 THEN 1.0 ELSE 0 END), 4)          AS real_retention_1_rate,
    ROUND(AVG(CASE WHEN event_derived_retention_1 THEN 1.0 ELSE 0 END), 4) AS event_derived_retention_1_rate,
    ROUND(AVG(CASE WHEN real_retention_7 THEN 1.0 ELSE 0 END), 4)          AS real_retention_7_rate,
    ROUND(AVG(CASE WHEN event_derived_retention_7 THEN 1.0 ELSE 0 END), 4) AS event_derived_retention_7_rate
FROM silver.users_dim
GROUP BY ab_version
ORDER BY ab_version;

SELECT * FROM gold.mart_retention_validation;
