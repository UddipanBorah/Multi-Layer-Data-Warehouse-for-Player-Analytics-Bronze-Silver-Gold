-- =============================================================================
-- GOLD LAYER -- Monetization marts
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- gold.mart_monetization_summary
-- ARPU, ARPPU, conversion rate and total revenue, sliced by A/B arm,
-- platform and country.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_monetization_summary;
CREATE TABLE gold.mart_monetization_summary AS
WITH user_revenue AS (
    SELECT
        u.userid,
        u.ab_version,
        u.platform,
        u.country,
        COALESCE(SUM(s.revenue_usd), 0.0) AS total_revenue,
        COALESCE(SUM(s.purchase_count), 0) AS total_purchases
    FROM silver.users_dim u
    LEFT JOIN silver.sessions s ON s.userid = u.userid
    GROUP BY u.userid, u.ab_version, u.platform, u.country
)
SELECT
    ab_version,
    platform,
    country,
    COUNT(*)                                                        AS users,
    SUM(CASE WHEN total_purchases > 0 THEN 1 ELSE 0 END)            AS paying_users,
    ROUND(SUM(CASE WHEN total_purchases > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS conversion_rate,
    ROUND(SUM(total_revenue), 2)                                    AS total_revenue_usd,
    ROUND(SUM(total_revenue) / COUNT(*), 4)                         AS arpu_usd,
    ROUND(
        SUM(total_revenue) / NULLIF(SUM(CASE WHEN total_purchases > 0 THEN 1 ELSE 0 END), 0), 4
    )                                                                AS arppu_usd
FROM user_revenue
GROUP BY ab_version, platform, country
ORDER BY ab_version, platform, country;

-- -----------------------------------------------------------------------------
-- gold.mart_ltv_curve
-- Cumulative revenue-per-user (LTV curve) by day-since-install and A/B arm,
-- using a running-total window function -- the classic LTV-curve pattern.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.mart_ltv_curve;
CREATE TABLE gold.mart_ltv_curve AS
WITH cohort_size AS (
    SELECT ab_version, COUNT(*) AS cohort_users
    FROM silver.users_dim
    GROUP BY ab_version
),
daily_revenue AS (
    SELECT
        u.ab_version,
        date_diff('day', u.install_date, s.session_date) AS day_since_install,
        SUM(s.revenue_usd) AS revenue_that_day
    FROM silver.sessions s
    JOIN silver.users_dim u ON u.userid = s.userid
    WHERE date_diff('day', u.install_date, s.session_date) BETWEEN 0 AND 13
    GROUP BY u.ab_version, date_diff('day', u.install_date, s.session_date)
)
SELECT
    d.ab_version,
    d.day_since_install,
    ROUND(d.revenue_that_day, 2) AS revenue_that_day,
    ROUND(
        SUM(d.revenue_that_day) OVER (
            PARTITION BY d.ab_version ORDER BY d.day_since_install
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    ) AS cumulative_revenue,
    ROUND(
        SUM(d.revenue_that_day) OVER (
            PARTITION BY d.ab_version ORDER BY d.day_since_install
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / c.cohort_users, 4
    ) AS cumulative_ltv_per_user
FROM daily_revenue d
JOIN cohort_size c ON c.ab_version = d.ab_version
ORDER BY d.ab_version, d.day_since_install;

SELECT * FROM gold.mart_monetization_summary LIMIT 10;
