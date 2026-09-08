-- =============================================================================
-- GOLD LAYER -- Engagement segmentation & A/B test readout
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- gold.mart_engagement_segments
-- NTILE()-based quartile segmentation of players by rounds played, plus a
-- power-user RANK() so the top players (whales/superfans) can be identified.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_engagement_segments;
CREATE TABLE gold.mart_engagement_segments AS
WITH ranked AS (
    SELECT
        userid,
        ab_version,
        platform,
        country,
        event_derived_rounds,
        NTILE(4) OVER (ORDER BY event_derived_rounds)          AS engagement_quartile,
        RANK() OVER (ORDER BY event_derived_rounds DESC)       AS engagement_rank
    FROM silver.users_dim
)
SELECT
    userid,
    ab_version,
    platform,
    country,
    event_derived_rounds,
    engagement_rank,
    CASE
        WHEN event_derived_rounds = 0        THEN 'non_activated'
        WHEN engagement_quartile = 1         THEN 'casual'
        WHEN engagement_quartile = 2         THEN 'regular'
        WHEN engagement_quartile = 3         THEN 'core'
        WHEN engagement_quartile = 4         THEN 'power_user'
    END AS engagement_segment
FROM ranked;

DROP TABLE IF EXISTS gold.mart_engagement_segment_summary;
CREATE TABLE gold.mart_engagement_segment_summary AS
SELECT
    engagement_segment,
    COUNT(*)                                   AS users,
    ROUND(AVG(event_derived_rounds), 1)        AS avg_rounds
FROM gold.mart_engagement_segments
GROUP BY engagement_segment
ORDER BY avg_rounds;

-- -----------------------------------------------------------------------------
-- gold.mart_ab_test_readout
-- Reproduces the original real-world Cookie Cats experiment analysis
-- (gate_30 vs gate_40) end-to-end through the warehouse: retention AND
-- monetization impact of moving the level-30 progress gate to level 40.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_ab_test_readout;
CREATE TABLE gold.mart_ab_test_readout AS
SELECT
    v.ab_version,
    v.users,
    v.real_retention_1_rate,
    v.real_retention_7_rate,
    m.conversion_rate,
    m.arpu_usd,
    m.arppu_usd
FROM (
    SELECT
        ab_version,
        COUNT(*) AS users,
        ROUND(AVG(CASE WHEN real_retention_1 THEN 1.0 ELSE 0 END), 4) AS real_retention_1_rate,
        ROUND(AVG(CASE WHEN real_retention_7 THEN 1.0 ELSE 0 END), 4) AS real_retention_7_rate
    FROM silver.users_dim
    GROUP BY ab_version
) v
JOIN (
    SELECT ab_version,
           ROUND(SUM(users * conversion_rate) / SUM(users), 4) AS conversion_rate,
           ROUND(SUM(total_revenue_usd) / SUM(users), 4)       AS arpu_usd,
           ROUND(SUM(total_revenue_usd) / NULLIF(SUM(paying_users), 0), 4) AS arppu_usd
    FROM gold.mart_monetization_summary
    GROUP BY ab_version
) m ON m.ab_version = v.ab_version
ORDER BY v.ab_version;

SELECT * FROM gold.mart_ab_test_readout;
SELECT * FROM gold.mart_engagement_segment_summary;
