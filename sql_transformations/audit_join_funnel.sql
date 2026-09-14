-- Row counts at each step from the raw NSE feed to the mart, by segment.

CREATE OR REPLACE TABLE audit_join_funnel AS
WITH raw AS (
    SELECT CASE WHEN TRIM(securitytype) = 'SME' THEN 'SME' ELSE 'MAINBOARD' END AS segment, securitytype
    FROM raw_past_ipo
),
steps AS (
    SELECT 1 AS step, 'raw_nse_issues' AS stage, 'All rows in the NSE past-issues feed' AS description,
           NULL::VARCHAR AS segment, COUNT(*) AS n
    FROM raw
    UNION ALL
    SELECT 2, 'equity_issues', 'Security type EQ, BE or SME', segment, COUNT(*)
    FROM stg_past_ipo GROUP BY segment
    UNION ALL
    SELECT 3, 'ipos', 'Excluding FPOs, withdrawn, partly-paid and follow-on issues', segment, COUNT(*)
    FROM stg_past_ipo WHERE exclusion_reason IS NULL GROUP BY segment
    UNION ALL
    SELECT 4, 'listed_ipos', 'Traded on NSE after issue close, or has a listing date in the feed', segment, COUNT(*)
    FROM int_ipo_listing WHERE is_listed GROUP BY segment
    UNION ALL
    SELECT 5, 'mart_rows', 'Rows in mart_ipo_analytics', segment, COUNT(*)
    FROM mart_ipo_analytics GROUP BY segment
    UNION ALL
    SELECT 6, 'with_subscription_data', 'Matched to a Chittorgarh subscription record', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE has_subscription_data GROUP BY segment
    UNION ALL
    SELECT 7, 'with_market_data', 'Matched to a Tickertape stock', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE has_market_data GROUP BY segment
    UNION ALL
    SELECT 8, 'with_both', 'Subscription and market data', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE has_subscription_data AND has_market_data GROUP BY segment
    UNION ALL
    SELECT 9, 'with_listing_price', 'Listing-day open and close from the NSE bhavcopy', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE has_listing_price GROUP BY segment
    UNION ALL
    SELECT 10, 'with_return_1y', 'One year since listing has passed and the stock still traded', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE return_1y_pct IS NOT NULL GROUP BY segment
    UNION ALL
    SELECT 11, 'with_listing_price_and_subscription', 'Listing price and subscription data', segment, COUNT(*)
    FROM mart_ipo_analytics WHERE has_listing_price AND has_subscription_data GROUP BY segment
)
SELECT
    step,
    stage,
    description,
    COALESCE(segment, 'ALL') AS segment,
    n
FROM (
    SELECT step, stage, description, segment, n FROM steps WHERE segment IS NOT NULL
    UNION ALL
    SELECT step, stage, description, 'ALL', SUM(n)::BIGINT FROM steps GROUP BY step, stage, description
)
ORDER BY step, segment;
