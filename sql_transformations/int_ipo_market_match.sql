-- Links each listed IPO to at most one Tickertape stock, and each stock to at most one IPO.
-- Candidates come from the same ticker (scored by name similarity, which resolves collisions such as
-- GSTL = Globesecure and Globalspace) or, for renamed symbols, an identical normalised name.
-- Seed rows accept reviewed renames below the threshold and reject known wrong pairs.
-- Requires variable: market_match_min_score

CREATE OR REPLACE TABLE int_ipo_market_match AS
WITH ipo AS (
    SELECT ticker, ipo_end_dt, company_name_norm
    FROM stg_past_ipo
    WHERE is_listed_ipo
),

seed AS (
    SELECT UPPER(TRIM(ticker)) AS ticker, TRIM(tickertape_sid) AS tickertape_sid, LOWER(TRIM(decision)) AS decision
    FROM seed_manual_ticker_matches
),

by_ticker AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        t.tickertape_sid,
        jaro_winkler_similarity(i.company_name_norm, t.company_name_norm) AS match_score,
        'ticker' AS match_method,
        1        AS priority
    FROM ipo i
    JOIN stg_tickertape t ON t.ticker = UPPER(i.ticker)
),

by_name AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        t.tickertape_sid,
        1.0    AS match_score,
        'name' AS match_method,
        2      AS priority
    FROM ipo i
    JOIN stg_tickertape t
        ON t.company_name_norm = i.company_name_norm
       AND t.ticker <> UPPER(i.ticker)
),

manual AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        t.tickertape_sid,
        jaro_winkler_similarity(i.company_name_norm, t.company_name_norm) AS match_score,
        'manual' AS match_method,
        0        AS priority
    FROM seed s
    JOIN ipo i ON UPPER(i.ticker) = s.ticker
    JOIN stg_tickertape t ON t.tickertape_sid = s.tickertape_sid
    WHERE s.decision = 'accept'
),

candidates AS (
    SELECT * FROM manual
    UNION ALL
    SELECT c.*
    FROM (SELECT * FROM by_ticker UNION ALL SELECT * FROM by_name) c
    WHERE c.match_score >= getvariable('market_match_min_score')
      AND NOT EXISTS (
          SELECT 1 FROM seed s
          WHERE s.ticker = UPPER(c.ticker) AND s.tickertape_sid = c.tickertape_sid AND s.decision = 'reject'
      )
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, ipo_end_dt
            ORDER BY priority, match_score DESC, tickertape_sid
        ) AS rank_for_ipo,
        ROW_NUMBER() OVER (
            PARTITION BY tickertape_sid
            ORDER BY priority, match_score DESC, ipo_end_dt DESC, ticker
        ) AS rank_for_stock
    FROM candidates
)

SELECT
    ticker,
    ipo_end_dt,
    tickertape_sid,
    ROUND(match_score, 4) AS match_score,
    match_method
FROM ranked
WHERE rank_for_ipo = 1 AND rank_for_stock = 1;
