-- Returns at fixed horizons after listing, one row per IPO per horizon, against the NIFTY 50.
--   return_pct                    issue price -> adjusted close at the horizon (what an allottee made)
--   nifty_return_pct              NIFTY 50 over the same money-at-risk period: issue close -> horizon
--   excess_return_pct             return_pct - nifty_return_pct (percentage points)
--   aftermarket_return_pct        listing-day close -> horizon (buying on the listing day instead)
--   aftermarket_excess_return_pct against NIFTY from the listing day
-- The horizon price is the last close on or before the horizon date. Returns are NULL when the horizon is
-- after the snapshot date, when the stock had not traded for price_max_stale_days before it
-- (suspended, delisted or moved to BSE), or when an unexplained price gap (int_ipo_price_gaps) lies
-- between listing and the horizon price.
-- Requires variables: snapshot_date, price_max_stale_days

CREATE OR REPLACE TABLE int_ipo_horizon_returns AS
WITH horizons (horizon, horizon_months) AS (
    VALUES ('1m', 1), ('6m', 6), ('1y', 12), ('3y', 36)
),

first_gap AS (
    SELECT ticker, ipo_end_dt, MIN(trade_dt) AS first_gap_dt FROM int_ipo_price_gaps GROUP BY ALL
),

ipo AS (
    SELECT l.ticker, l.ipo_end_dt, l.listing_dt, l.listing_close, l.issue_price, g.first_gap_dt
    FROM int_ipo_listing l
    LEFT JOIN first_gap g USING (ticker, ipo_end_dt)
    WHERE l.has_nse_listing
),

targets AS (
    SELECT
        ipo.*,
        h.horizon,
        h.horizon_months,
        (ipo.listing_dt + TO_MONTHS(h.horizon_months))::DATE AS horizon_dt
    FROM ipo CROSS JOIN horizons h
),

priced AS (
    SELECT t.*, ph.trade_dt AS price_dt, ph.adj_close
    FROM targets t
    ASOF LEFT JOIN int_ipo_price_history ph
        ON ph.ticker = t.ticker AND ph.ipo_end_dt = t.ipo_end_dt AND ph.trade_dt <= t.horizon_dt
),

with_nifty AS (
    SELECT p.*, n.close AS nifty_at_price_dt
    FROM priced p
    ASOF LEFT JOIN stg_nifty50 n ON n.trade_dt <= p.price_dt
),

with_nifty_start AS (
    SELECT w.*, n.close AS nifty_at_issue_close
    FROM with_nifty w
    ASOF LEFT JOIN stg_nifty50 n ON n.trade_dt <= w.ipo_end_dt
),

with_nifty_listing AS (
    SELECT w.*, n.close AS nifty_at_listing
    FROM with_nifty_start w
    ASOF LEFT JOIN stg_nifty50 n ON n.trade_dt <= w.listing_dt
),

valid AS (
    SELECT
        *,
        horizon_dt <= getvariable('snapshot_date')::DATE AS is_elapsed,
        price_dt >= horizon_dt - getvariable('price_max_stale_days')::INTEGER AS has_recent_price,
        first_gap_dt IS NOT NULL AND price_dt >= first_gap_dt AS is_after_price_gap
    FROM with_nifty_listing
),

computed AS (
    SELECT
        *,
        CASE WHEN is_elapsed AND has_recent_price AND NOT is_after_price_gap THEN (adj_close / issue_price - 1) * 100 END           AS r,
        CASE WHEN is_elapsed AND has_recent_price AND NOT is_after_price_gap THEN (nifty_at_price_dt / nifty_at_issue_close - 1) * 100 END AS nr,
        CASE WHEN is_elapsed AND has_recent_price AND NOT is_after_price_gap THEN (adj_close / listing_close - 1) * 100 END         AS ar,
        CASE WHEN is_elapsed AND has_recent_price AND NOT is_after_price_gap THEN (nifty_at_price_dt / nifty_at_listing - 1) * 100 END AS anr
    FROM valid
)

SELECT
    ticker,
    ipo_end_dt,
    horizon,
    horizon_months,
    horizon_dt,
    is_elapsed,
    COALESCE(has_recent_price, FALSE)  AS has_recent_price,
    COALESCE(is_after_price_gap, FALSE) AS is_after_price_gap,
    price_dt,
    adj_close,
    ROUND(r, 2)                        AS return_pct,
    ROUND(nr, 2)                       AS nifty_return_pct,
    ROUND(r - nr, 2)                   AS excess_return_pct,
    ROUND(ar, 2)                       AS aftermarket_return_pct,
    ROUND(ar - anr, 2)                 AS aftermarket_excess_return_pct
FROM computed;
