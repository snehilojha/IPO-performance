-- Links each listed IPO to at most one Tickertape stock, and each stock to at most one IPO.
-- Candidates, in order of priority:
--   manual      seed rows accepting reviewed renames (data/seeds/manual_ticker_matches.csv)
--   nse_symbol  the Tickertape ticker is the IPO's current NSE symbol, found by following its ISIN chain in
--               the bhavcopy (int_ipo_price_history) — resolves renames such as Infibeam -> CCAVENUE; only
--               for stocks still trading at the snapshot, and accepted whatever the name similarity
--   ticker      same ticker as the NSE feed, scored by name similarity (resolves collisions such as
--               GSTL = Globesecure and Globalspace)
--   name        identical normalised name under a different ticker
-- Seed rows also reject known wrong pairs.
-- Requires variables: market_match_min_score, snapshot_date, price_max_stale_days

CREATE OR REPLACE TABLE int_ipo_market_match AS
WITH ipo AS (
    SELECT i.ticker, i.ipo_end_dt, i.company_name_norm
    FROM stg_past_ipo i
    JOIN int_ipo_listing l USING (ticker, ipo_end_dt)
    WHERE l.is_listed AND i.exclusion_reason IS NULL
),

seed AS (
    SELECT UPPER(TRIM(ticker)) AS ticker, TRIM(tickertape_sid) AS tickertape_sid, LOWER(TRIM(decision)) AS decision
    FROM seed_manual_ticker_matches
),

current_symbol AS (
    SELECT ticker, ipo_end_dt, arg_max(symbol, trade_dt) AS symbol
    FROM int_ipo_price_history
    GROUP BY ticker, ipo_end_dt
    HAVING MAX(trade_dt) >= getvariable('snapshot_date')::DATE - getvariable('price_max_stale_days')::INTEGER
),

by_nse_symbol AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        t.tickertape_sid,
        jaro_winkler_similarity(i.company_name_norm, t.company_name_norm) AS match_score,
        'nse_symbol' AS match_method,
        1            AS priority
    FROM ipo i
    JOIN current_symbol c ON c.ticker = i.ticker AND c.ipo_end_dt = i.ipo_end_dt
    JOIN stg_tickertape t ON t.ticker = c.symbol
),

by_ticker AS (
    SELECT
        i.ticker,
        i.ipo_end_dt,
        t.tickertape_sid,
        jaro_winkler_similarity(i.company_name_norm, t.company_name_norm) AS match_score,
        'ticker' AS match_method,
        2        AS priority
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
        3      AS priority
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
    FROM (
        SELECT * FROM by_nse_symbol
        UNION ALL
        SELECT * FROM by_ticker WHERE match_score >= getvariable('market_match_min_score')
        UNION ALL
        SELECT * FROM by_name
    ) c
    WHERE NOT EXISTS (
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
