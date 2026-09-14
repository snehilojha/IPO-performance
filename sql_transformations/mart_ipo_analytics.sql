-- One row per listed equity IPO on NSE (mainboard and SME). Nothing is dropped for missing data:
-- has_subscription_data / has_market_data / has_listing_price say what is available.
-- has_subscription_figures: the matched subscription record has numbers (most 2016-17 SME records have none).
-- All returns are price returns in percent, adjusted for bonuses and splits, not for dividends.
-- Requires variables: snapshot_date, price_max_stale_days

CREATE OR REPLACE TABLE mart_ipo_analytics AS
WITH horizons AS (
    SELECT
        ticker,
        ipo_end_dt,
        MAX(return_pct) FILTER (WHERE horizon = '1m')                  AS return_1m_pct,
        MAX(nifty_return_pct) FILTER (WHERE horizon = '1m')            AS nifty_return_1m_pct,
        MAX(excess_return_pct) FILTER (WHERE horizon = '1m')           AS excess_return_1m_pct,
        MAX(aftermarket_excess_return_pct) FILTER (WHERE horizon = '1m') AS aftermarket_excess_return_1m_pct,
        MAX(return_pct) FILTER (WHERE horizon = '6m')                  AS return_6m_pct,
        MAX(nifty_return_pct) FILTER (WHERE horizon = '6m')            AS nifty_return_6m_pct,
        MAX(excess_return_pct) FILTER (WHERE horizon = '6m')           AS excess_return_6m_pct,
        MAX(aftermarket_excess_return_pct) FILTER (WHERE horizon = '6m') AS aftermarket_excess_return_6m_pct,
        MAX(return_pct) FILTER (WHERE horizon = '1y')                  AS return_1y_pct,
        MAX(nifty_return_pct) FILTER (WHERE horizon = '1y')            AS nifty_return_1y_pct,
        MAX(excess_return_pct) FILTER (WHERE horizon = '1y')           AS excess_return_1y_pct,
        MAX(aftermarket_excess_return_pct) FILTER (WHERE horizon = '1y') AS aftermarket_excess_return_1y_pct,
        MAX(return_pct) FILTER (WHERE horizon = '3y')                  AS return_3y_pct,
        MAX(nifty_return_pct) FILTER (WHERE horizon = '3y')            AS nifty_return_3y_pct,
        MAX(excess_return_pct) FILTER (WHERE horizon = '3y')           AS excess_return_3y_pct,
        MAX(aftermarket_excess_return_pct) FILTER (WHERE horizon = '3y') AS aftermarket_excess_return_3y_pct
    FROM int_ipo_horizon_returns
    GROUP BY ticker, ipo_end_dt
),

last_trade AS (
    SELECT ticker, ipo_end_dt, trade_dt AS last_trade_dt, adj_close AS last_adj_close, share_multiplier
    FROM int_ipo_price_history
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, ipo_end_dt ORDER BY trade_dt DESC) = 1
),

first_gap AS (
    SELECT ticker, ipo_end_dt, MIN(trade_dt) AS price_gap_dt FROM int_ipo_price_gaps GROUP BY ALL
),

action_counts AS (
    SELECT ticker, ipo_end_dt, COUNT(*) AS corporate_action_count
    FROM int_ipo_corporate_actions
    WHERE ex_dt <= getvariable('snapshot_date')::DATE
    GROUP BY ALL
),

base AS (
    SELECT
        ipo.ticker,
        ipo.company_name,
        ipo.segment,
        ipo.security_type,
        ipo.ipo_start_dt,
        ipo.ipo_end_dt,
        YEAR(ipo.ipo_end_dt)                         AS issue_year,
        l.listing_dt,
        l.listing_dt_source,
        ipo.listing_dt_reported,
        ipo.listing_dt_flag,
        l.issue_price,
        ipo.issue_price_band_low,
        ipo.issue_price_band_high,
        l.issue_price_source,

        -- Listing day (NSE bhavcopy)
        l.has_nse_listing                            AS has_listing_price,
        l.listing_method,
        l.listing_symbol,
        l.listing_base_price,
        ABS(l.listing_base_price - l.issue_price) < 0.005 AS listing_base_matches_issue_price,
        l.listing_open,
        l.listing_close,
        l.listing_volume,
        -- NSE caps an SME IPO's opening price at 90% over the issue price from 2024-07-04
        -- (special pre-open session for SME IPOs); mainboard IPOs have no such cap.
        CASE WHEN l.has_nse_listing AND ipo.segment = 'SME' THEN l.listing_dt >= DATE '2024-07-04' END
                                                     AS listing_cap_applies,
        CASE
            WHEN l.has_nse_listing AND ipo.segment = 'SME' AND l.listing_dt >= DATE '2024-07-04'
            THEN l.listing_open >= ROUND(l.issue_price * 1.9, 2) - 0.05
        END                                          AS listing_capped,

        sm.subscription_key IS NOT NULL              AS has_subscription_data,
        COALESCE(sub.total_x IS NOT NULL, FALSE)     AS has_subscription_figures,
        sm.match_method                              AS subscription_match_method,
        sm.match_score                               AS subscription_match_score,
        sub.issue_size_cr,
        sub.qib_x                                    AS subscription_qib_x,
        sub.nii_x                                    AS subscription_nii_x,
        sub.snii_x                                   AS subscription_snii_x,
        sub.bnii_x                                   AS subscription_bnii_x,
        sub.retail_x                                 AS subscription_retail_x,
        sub.employee_x                               AS subscription_employee_x,
        sub.total_x                                  AS subscription_total_x,
        sub.applications,
        -- Many SME issues have no QIB category at all; that is a level, not a zero.
        -- Unknown (NULL) when there are no subscription figures to tell.
        CASE WHEN sub.total_x IS NOT NULL THEN sub.qib_reported END AS has_qib_category,

        mm.tickertape_sid IS NOT NULL                AS has_market_data,
        mm.match_method                              AS market_match_method,
        mm.match_score                               AS market_match_score,
        tt.tickertape_sid,
        tt.ticker                                    AS current_ticker,
        tt.company_name                              AS current_company_name,
        tt.sector,
        tt.subindustry,
        tt.market_cap_cr,
        tt.last_price                                AS current_price,
        tt.pe_ratio,
        tt.roe_pct,
        tt.pb_ratio,
        tt.is_lossmaking,
        tt.return_4w_pct,
        tt.return_1d_pct,

        -- Price history (NSE bhavcopy, adjusted for bonuses and splits)
        lt.last_trade_dt,
        lt.last_trade_dt >= getvariable('snapshot_date')::DATE - getvariable('price_max_stale_days')::INTEGER
                                                     AS is_trading_at_snapshot,
        lt.share_multiplier,
        COALESCE(ac.corporate_action_count, 0)       AS corporate_action_count,
        -- first overnight move the corporate actions don't explain (demerger or missing action)
        g.price_gap_dt,
        lt.last_adj_close,

        h.* EXCLUDE (ticker, ipo_end_dt),

        getvariable('snapshot_date')::DATE           AS snapshot_date
    FROM int_ipo_listing l
    -- (ticker, ipo_end_dt) is unique among IPOs; an excluded duplicate row of the feed can share it
    JOIN stg_past_ipo ipo ON ipo.ticker = l.ticker AND ipo.ipo_end_dt = l.ipo_end_dt AND ipo.exclusion_reason IS NULL
    LEFT JOIN int_ipo_subscription_match sm
        ON sm.ticker = ipo.ticker AND sm.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN stg_ipo_subscription sub
        ON sub.subscription_key = sm.subscription_key
    LEFT JOIN int_ipo_market_match mm
        ON mm.ticker = ipo.ticker AND mm.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN stg_tickertape tt
        ON tt.tickertape_sid = mm.tickertape_sid
    LEFT JOIN last_trade lt
        ON lt.ticker = ipo.ticker AND lt.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN first_gap g
        ON g.ticker = ipo.ticker AND g.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN action_counts ac
        ON ac.ticker = ipo.ticker AND ac.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN horizons h
        ON h.ticker = ipo.ticker AND h.ipo_end_dt = ipo.ipo_end_dt
    WHERE l.is_listed
)

SELECT
    * EXCLUDE (last_adj_close),
    ROUND((listing_open / issue_price - 1) * 100, 2)          AS listing_gain_pct,
    ROUND((listing_close / issue_price - 1) * 100, 2)         AS listing_day_close_gain_pct,
    -- Issue close (when the issue price is paid) to the last NSE trade up to the snapshot date
    last_trade_dt - ipo_end_dt                                AS holding_days,
    ROUND((last_trade_dt - ipo_end_dt) / 365.25, 2)           AS holding_years,
    CASE WHEN price_gap_dt IS NULL THEN ROUND((last_adj_close / issue_price - 1) * 100, 2) END
                                                              AS return_since_issue_pct,
    CASE
        WHEN last_trade_dt - ipo_end_dt >= 182 AND price_gap_dt IS NULL
        THEN ROUND((POWER(last_adj_close / issue_price, 365.25 / (last_trade_dt - ipo_end_dt)) - 1) * 100, 2)
    END                                                       AS annualized_return_pct
FROM base
ORDER BY ipo_end_dt DESC, ticker;
