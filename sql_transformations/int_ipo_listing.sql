-- One row per IPO (excluded issues aside): whether and when it listed on NSE, and its listing-day prices.
-- The listing is the IPO's first trading day in the NSE bhavcopy within 30 days of issue close, found by:
--   manual       reviewed rows in data/seeds/manual_listings.csv (accept a symbol, or reject a match)
--   feed_symbol  the symbol from the NSE past-issues feed starts trading (and wasn't trading just before)
--   new_listing  another symbol's first ever trade, at a base price equal to the issue price — the feed
--                sometimes carries a later symbol (DCX listed as DCXINDIA); accepted only when unambiguous
-- On the listing day the bhavcopy's previous close is the exchange's base price, normally the issue price.
-- When that base price differs from stg_past_ipo.issue_price but is one of the prices the feed gives for the
-- issue (inside the band, or the feed's issueprice), the base price is taken as the issue price.
-- Requires variable: snapshot_date

CREATE OR REPLACE TABLE int_ipo_listing AS
WITH ipo AS (
    SELECT ticker, ipo_end_dt, segment, issue_price, issue_price_source, issue_price_band_low,
           issue_price_band_high, issueprice_reported, listing_dt, listing_dt_reported
    FROM stg_past_ipo
    WHERE exclusion_reason IS NULL
      AND ipo_end_dt <= getvariable('snapshot_date')::DATE
),

panel_start AS (
    SELECT MIN(trade_dt) AS first_panel_dt FROM stg_nse_prices
),

first_trades AS (
    SELECT symbol, MIN(trade_dt) AS first_dt FROM stg_nse_prices GROUP BY symbol
),

seed AS (
    SELECT ticker, ipo_end_dt, UPPER(TRIM(trading_symbol)) AS trading_symbol, LOWER(TRIM(decision)) AS decision
    FROM seed_manual_listings
),

manual AS (
    SELECT i.ticker, i.ipo_end_dt, p.symbol, MIN(p.trade_dt) AS listing_dt, 'manual' AS listing_method, 0 AS priority
    FROM seed s
    JOIN ipo i ON i.ticker = s.ticker AND i.ipo_end_dt = s.ipo_end_dt
    JOIN stg_nse_prices p
        ON p.symbol = s.trading_symbol
       AND p.trade_dt > i.ipo_end_dt AND p.trade_dt <= i.ipo_end_dt + 30
    WHERE s.decision = 'accept'
    GROUP BY ALL
),

feed_symbol AS (
    SELECT i.ticker, i.ipo_end_dt, p.symbol, MIN(p.trade_dt) AS listing_dt, 'feed_symbol' AS listing_method, 1 AS priority
    FROM ipo i
    JOIN stg_nse_prices p
        ON p.symbol = UPPER(i.ticker)
       AND p.trade_dt > i.ipo_end_dt AND p.trade_dt <= i.ipo_end_dt + 30
    WHERE NOT EXISTS (
        SELECT 1 FROM stg_nse_prices q
        WHERE q.symbol = p.symbol AND q.trade_dt > i.ipo_end_dt - 90 AND q.trade_dt <= i.ipo_end_dt
    )
    GROUP BY ALL
),

new_listing AS (
    SELECT i.ticker, i.ipo_end_dt, f.symbol, f.first_dt AS listing_dt, 'new_listing' AS listing_method, 2 AS priority
    FROM ipo i
    JOIN first_trades f
        ON f.first_dt > i.ipo_end_dt AND f.first_dt <= i.ipo_end_dt + 30
    JOIN stg_nse_prices p
        ON p.symbol = f.symbol AND p.trade_dt = f.first_dt
       AND ABS(p.prev_close - i.issue_price) < 0.005
    -- a symbol trading on the panel's first days may have listed long before
    WHERE f.first_dt > (SELECT first_panel_dt FROM panel_start) + 7
    QUALIFY COUNT(*) OVER (PARTITION BY i.ticker, i.ipo_end_dt) = 1
),

candidates AS (
    SELECT * FROM manual
    UNION ALL
    SELECT c.*
    FROM (SELECT * FROM feed_symbol UNION ALL SELECT * FROM new_listing) c
    WHERE NOT EXISTS (
        SELECT 1 FROM seed s
        WHERE s.ticker = c.ticker AND s.ipo_end_dt = c.ipo_end_dt AND s.decision = 'reject'
          AND (s.trading_symbol IS NULL OR s.trading_symbol = c.symbol)
    )
),

matched AS (
    SELECT *
    FROM candidates
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, ipo_end_dt ORDER BY priority, listing_dt, symbol) = 1
        AND ROW_NUMBER() OVER (PARTITION BY symbol, listing_dt ORDER BY priority, ipo_end_dt DESC, ticker) = 1
)

SELECT
    i.ticker,
    i.ipo_end_dt,
    i.segment,
    CASE
        WHEN ABS(p.prev_close - i.issue_price) >= 0.005
         AND (p.prev_close BETWEEN i.issue_price_band_low AND i.issue_price_band_high
              OR ABS(p.prev_close - i.issueprice_reported) < 0.005)
        THEN p.prev_close::DECIMAL(12, 2)
        ELSE i.issue_price
    END                                                         AS issue_price,
    CASE
        WHEN ABS(p.prev_close - i.issue_price) >= 0.005
         AND (p.prev_close BETWEEN i.issue_price_band_low AND i.issue_price_band_high
              OR ABS(p.prev_close - i.issueprice_reported) < 0.005)
        THEN 'listing_base_price'
        ELSE i.issue_price_source
    END                                                         AS issue_price_source,
    m.symbol IS NOT NULL                                        AS has_nse_listing,
    m.symbol IS NOT NULL OR i.listing_dt_reported IS NOT NULL   AS is_listed,
    COALESCE(m.listing_dt, i.listing_dt)                        AS listing_dt,
    CASE
        WHEN m.symbol IS NOT NULL    THEN 'nse_bhavcopy'
        WHEN i.listing_dt IS NOT NULL THEN 'nse_feed'
    END                                                         AS listing_dt_source,
    m.listing_method,
    m.symbol                                                    AS listing_symbol,
    p.isin                                                      AS listing_isin,
    p.series                                                    AS listing_series,
    p.prev_close                                                AS listing_base_price,
    p.open                                                      AS listing_open,
    p.close                                                     AS listing_close,
    p.volume                                                    AS listing_volume
FROM ipo i
LEFT JOIN matched m USING (ticker, ipo_end_dt)
LEFT JOIN stg_nse_prices p ON p.symbol = m.symbol AND p.trade_dt = m.listing_dt;
