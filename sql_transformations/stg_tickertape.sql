-- One row per stock in the Tickertape screener (current snapshot of listed companies).
-- Missing ratios stay NULL: an unknown PE is not a PE of zero.

CREATE OR REPLACE TABLE stg_tickertape AS
SELECT
    TRIM(sid)                         AS tickertape_sid,
    UPPER(TRIM(ticker))               AS ticker,
    TRIM(name)                        AS company_name,
    norm_company_name(name)           AS company_name_norm,
    sector,
    subindustry,
    TRY_CAST(market_cap AS DOUBLE)    AS market_cap_cr,
    TRY_CAST(last_price AS DOUBLE)    AS last_price,
    TRY_CAST(pe AS DOUBLE)            AS pe_ratio,
    TRY_CAST(roe AS DOUBLE)           AS roe_pct,
    TRY_CAST(pb AS DOUBLE)            AS pb_ratio,
    CASE
        WHEN TRY_CAST(pe AS DOUBLE) < 0 OR TRY_CAST(roe AS DOUBLE) < 0 THEN TRUE
        WHEN TRY_CAST(pe AS DOUBLE) IS NULL AND TRY_CAST(roe AS DOUBLE) IS NULL THEN NULL
        ELSE FALSE
    END                               AS is_lossmaking,
    TRY_CAST("4w_return_pct" AS DOUBLE) AS return_4w_pct,
    TRY_CAST("1d_return_pct" AS DOUBLE) AS return_1d_pct
FROM raw_tickertape
WHERE sid IS NOT NULL;
