-- Links each listed IPO to at most one subscription record, and each subscription record to at most one IPO.
-- Candidates: issue close dates within 3 days and Jaro-Winkler similarity of normalised names above the
-- threshold. Manual seed rows take priority over fuzzy candidates.
-- Requires variable: subscription_match_min_score

CREATE OR REPLACE TABLE int_ipo_subscription_match AS
WITH ipo AS (
    SELECT i.ticker, i.ipo_end_dt, i.segment, i.company_name_norm
    FROM stg_past_ipo i
    JOIN int_ipo_listing l USING (ticker, ipo_end_dt)
    WHERE l.is_listed AND i.exclusion_reason IS NULL
),

manual AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        s.subscription_key,
        jaro_winkler_similarity(i.company_name_norm, s.company_name_norm) AS match_score,
        ABS(s.ipo_end_dt - i.ipo_end_dt)                                  AS close_date_diff_days,
        'manual'                                                          AS match_method,
        0                                                                 AS priority
    FROM seed_manual_subscription_matches m
    JOIN ipo i
        ON i.ticker = m.ticker AND i.ipo_end_dt = m.ipo_end_dt
    JOIN stg_ipo_subscription s
        ON s.company_name = m.subscription_company_name AND s.ipo_end_dt = m.subscription_ipo_end_dt
),

fuzzy AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        s.subscription_key,
        jaro_winkler_similarity(i.company_name_norm, s.company_name_norm) AS match_score,
        ABS(s.ipo_end_dt - i.ipo_end_dt)                                  AS close_date_diff_days,
        CASE
            WHEN i.company_name_norm = s.company_name_norm AND s.ipo_end_dt = i.ipo_end_dt THEN 'exact'
            ELSE 'fuzzy'
        END                                                               AS match_method,
        CASE WHEN i.segment = s.segment THEN 1 ELSE 2 END                 AS priority
    FROM ipo i
    JOIN stg_ipo_subscription s
        ON s.ipo_end_dt BETWEEN i.ipo_end_dt - 3 AND i.ipo_end_dt + 3
    WHERE jaro_winkler_similarity(i.company_name_norm, s.company_name_norm)
          >= getvariable('subscription_match_min_score')
),

candidates AS (
    SELECT * FROM manual
    UNION ALL
    SELECT f.* FROM fuzzy f
    WHERE NOT EXISTS (SELECT 1 FROM manual m WHERE m.ticker = f.ticker AND m.ipo_end_dt = f.ipo_end_dt)
      AND NOT EXISTS (SELECT 1 FROM manual m WHERE m.subscription_key = f.subscription_key)
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, ipo_end_dt
            ORDER BY priority, match_score DESC, close_date_diff_days, subscription_key
        ) AS rank_for_ipo,
        ROW_NUMBER() OVER (
            PARTITION BY subscription_key
            ORDER BY priority, match_score DESC, close_date_diff_days, ticker
        ) AS rank_for_subscription
    FROM candidates
)

SELECT
    ticker,
    ipo_end_dt,
    subscription_key,
    ROUND(match_score, 4) AS match_score,
    close_date_diff_days,
    match_method
FROM ranked
WHERE rank_for_ipo = 1 AND rank_for_subscription = 1;
