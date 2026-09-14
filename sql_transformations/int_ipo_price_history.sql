-- Daily NSE prices for each listed IPO from its listing day, following the security through renames and
-- splits, plus the bonus / split factors that apply to it.
--
-- The security is followed as a chain of (symbol, ISIN) periods starting from the listing-day row:
--   same ISIN                                   -> a rename (ZOMATO -> ETERNAL)
--   same symbol or same ISIN issuer prefix,     -> a new ISIN on a split, or a new symbol and ISIN
--   starting within 30 days of the last period     together on an SME -> mainboard migration
-- share_multiplier = shares held per share bought at the IPO; adj_close = close * share_multiplier is the
-- value of one IPO share, comparable with the issue price.
-- Prices are not adjusted for dividends (neither is the NIFTY 50 price index they are compared with).

CREATE OR REPLACE TABLE int_ipo_security_periods AS
WITH RECURSIVE periods AS (
    SELECT symbol, isin, MIN(trade_dt) AS first_dt, MAX(trade_dt) AS last_dt
    FROM stg_nse_prices
    GROUP BY symbol, isin
),

chain AS (
    SELECT l.ticker, l.ipo_end_dt, l.listing_dt, p.symbol, p.isin, p.first_dt, p.last_dt
    FROM int_ipo_listing l
    JOIN periods p ON p.symbol = l.listing_symbol AND p.isin = l.listing_isin
    WHERE l.has_nse_listing

    UNION

    SELECT c.ticker, c.ipo_end_dt, c.listing_dt, p.symbol, p.isin, p.first_dt, p.last_dt
    FROM chain c
    JOIN periods p
        ON p.last_dt >= c.listing_dt
       AND (
            p.isin = c.isin
            OR ((p.symbol = c.symbol OR LEFT(p.isin, 9) = LEFT(c.isin, 9))
                AND p.first_dt BETWEEN c.last_dt - 10 AND c.last_dt + 30)
       )
)

SELECT DISTINCT ticker, ipo_end_dt, listing_dt, symbol, isin, first_dt, last_dt
FROM chain;


-- Bonus, split and consolidation factors for the IPO's security. NSE files past actions under the
-- company's current symbol (KARDA's 2021 split is listed under DHARAN), so any symbol of the chain counts,
-- for ex-dates between listing and the chain's last trade.
CREATE OR REPLACE TABLE int_ipo_corporate_actions AS
WITH chain AS (
    SELECT ticker, ipo_end_dt, listing_dt, LIST(DISTINCT symbol) AS symbols, MAX(last_dt) AS last_dt
    FROM int_ipo_security_periods
    GROUP BY ALL
),

applied AS (
    SELECT DISTINCT
        c.ticker,
        c.ipo_end_dt,
        a.ex_dt,
        a.symbol,
        a.action_type,
        a.share_factor,
        a.subject
    FROM chain c
    JOIN stg_corporate_actions a
        ON list_contains(c.symbols, a.symbol)
       AND a.ex_dt > c.listing_dt
       AND a.ex_dt <= c.last_dt + 10
)
SELECT *
FROM applied
-- the same action announced under two symbols of one chain counts once
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker, ipo_end_dt, ex_dt, action_type, share_factor ORDER BY symbol
) = 1;


CREATE OR REPLACE TABLE int_ipo_price_history AS
WITH rows AS (
    SELECT s.ticker, s.ipo_end_dt, p.*
    FROM int_ipo_security_periods s
    JOIN stg_nse_prices p
        ON p.symbol = s.symbol AND p.isin = s.isin AND p.trade_dt >= s.listing_dt
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.ticker, s.ipo_end_dt, p.trade_dt ORDER BY p.volume DESC NULLS LAST, p.symbol
    ) = 1
),

multipliers AS (
    SELECT
        ticker,
        ipo_end_dt,
        ex_dt,
        EXP(SUM(LN(share_factor)) OVER (
            PARTITION BY ticker, ipo_end_dt ORDER BY ex_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS share_multiplier
    FROM (
        SELECT ticker, ipo_end_dt, ex_dt, EXP(SUM(LN(share_factor))) AS share_factor
        FROM int_ipo_corporate_actions
        GROUP BY ALL
    )
)

SELECT
    r.ticker,
    r.ipo_end_dt,
    r.trade_dt,
    r.symbol,
    r.series,
    r.isin,
    r.open,
    r.close,
    r.prev_close,
    r.volume,
    COALESCE(m.share_multiplier, 1)            AS share_multiplier,
    r.close * COALESCE(m.share_multiplier, 1)  AS adj_close
FROM rows r
ASOF LEFT JOIN multipliers m
    ON m.ticker = r.ticker AND m.ipo_end_dt = r.ipo_end_dt AND r.trade_dt >= m.ex_dt;


-- Overnight moves the bonus / split factors don't explain: the share-adjusted open is below 60% or above
-- 167% of the previous close. Daily price bands make such moves practically impossible without a corporate
-- action, so these are usually demergers or actions missing from the NSE feed. Returns measured across
-- such a gap are not trusted (see int_ipo_horizon_returns).
CREATE OR REPLACE TABLE int_ipo_price_gaps AS
WITH moves AS (
    SELECT
        ticker,
        ipo_end_dt,
        trade_dt,
        symbol,
        isin,
        open,
        LAG(close) OVER w                               AS previous_close,
        LAG(trade_dt) OVER w                            AS previous_trade_dt,
        share_multiplier / LAG(share_multiplier) OVER w AS applied_factor
    FROM int_ipo_price_history
    WINDOW w AS (PARTITION BY ticker, ipo_end_dt ORDER BY trade_dt)
)
SELECT
    *,
    ROUND(open * applied_factor / previous_close, 4) AS adjusted_open_to_previous_close
FROM moves
WHERE previous_close > 0
  AND open > 0
  AND NOT (open * applied_factor / previous_close BETWEEN 0.6 AND 1.0 / 0.6);
