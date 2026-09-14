-- One row per listed equity IPO on NSE (mainboard and SME). Nothing is dropped for missing data:
-- has_subscription_data / has_market_data say what is available.
-- Requires variable: snapshot_date

CREATE OR REPLACE TABLE mart_ipo_analytics AS
WITH base AS (
    SELECT
        ipo.ticker,
        ipo.company_name,
        ipo.segment,
        ipo.security_type,
        ipo.ipo_start_dt,
        ipo.ipo_end_dt,
        YEAR(ipo.ipo_end_dt)                         AS issue_year,
        ipo.listing_dt,
        ipo.listing_dt_reported,
        ipo.listing_dt_flag,
        ipo.issue_price,
        ipo.issue_price_band_low,
        ipo.issue_price_band_high,
        ipo.issue_price_source,

        sm.subscription_key IS NOT NULL              AS has_subscription_data,
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
        CASE WHEN sm.subscription_key IS NOT NULL THEN sub.qib_x IS NOT NULL END AS has_qib_category,

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

        getvariable('snapshot_date')::DATE           AS snapshot_date
    FROM stg_past_ipo ipo
    LEFT JOIN int_ipo_subscription_match sm
        ON sm.ticker = ipo.ticker AND sm.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN stg_ipo_subscription sub
        ON sub.subscription_key = sm.subscription_key
    LEFT JOIN int_ipo_market_match mm
        ON mm.ticker = ipo.ticker AND mm.ipo_end_dt = ipo.ipo_end_dt
    LEFT JOIN stg_tickertape tt
        ON tt.tickertape_sid = mm.tickertape_sid
    WHERE ipo.is_listed_ipo
      AND ipo.ipo_end_dt <= getvariable('snapshot_date')::DATE
),

returns AS (
    SELECT
        *,
        -- Measured from issue close (when the issue price is paid) to the snapshot date.
        snapshot_date - ipo_end_dt                   AS holding_days,
        -- Price-to-price only: not adjusted for splits, bonuses or dividends.
        CASE
            WHEN issue_price > 0 AND current_price IS NOT NULL
            THEN (current_price - issue_price) / issue_price * 100
        END                                          AS return_since_issue_raw
    FROM base
)

SELECT
    * EXCLUDE (return_since_issue_raw),
    ROUND(holding_days / 365.25, 2)                  AS holding_years,
    ROUND(return_since_issue_raw, 2)                 AS return_since_issue_pct,
    CASE
        WHEN holding_days >= 182 AND return_since_issue_raw IS NOT NULL
        THEN ROUND((POWER(1 + return_since_issue_raw / 100, 365.25 / holding_days) - 1) * 100, 2)
    END                                              AS annualized_return_pct
FROM returns
ORDER BY ipo_end_dt DESC, ticker;
