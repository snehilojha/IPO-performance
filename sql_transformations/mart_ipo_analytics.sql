DROP TABLE IF EXISTS mart_ipo_analytics_1;

CREATE TABLE mart_ipo_analytics_1 AS
SELECT 
    ipo.ticker,
    ipo.company_name,
    ipo.ipo_start_dt,
    ipo.ipo_end_dt,
    ipo.listing_dt,
    ipo.security_type,
    ipo.issue_price,
    
    sub.total_issued_in_cr,
    sub.qib_x                   AS subscription_qib_times,
    sub.nii_x                   AS subscription_nii_times,
    sub.retail_x                AS subscription_retail_times,
    sub.total_x                 AS subscription_total_times,
    sub.ipo_type,
    sub.year                    AS ipo_year,

    tt.sector,
    tt.subindustry,
    tt.current_market_cap,
    tt.last_price               AS current_price,
    tt.current_pe_ratio,
    tt.current_roe_ratio,
    tt.current_pb_ratio,
    tt.return_last_4w_pct       AS return_4week_pct,
    tt.return_last_1d_pct       AS return_1day_pct,
    

    CASE 
        WHEN ipo.issue_price > 0 AND tt.last_price IS NOT NULL 
        THEN ROUND(((tt.last_price - ipo.issue_price) / ipo.issue_price * 100), 2)
        ELSE NULL 
    END AS listing_gains_pct,


    CURRENT_TIMESTAMP AS etl_processed_timestamp

FROM stg_past_ipo_table_1 ipo

-- Join with subscription table on company name and IPO dates
LEFT JOIN stg_pastipo_subscription_table_1 sub
    ON TRIM(UPPER(ipo.company_name)) = TRIM(UPPER(sub.company_name))
    AND ipo.ipo_start_dt = sub.ipo_start_dt
    AND ipo.ipo_end_dt = sub.ipo_end_dt

-- Join with tickertape data on ticker symbol
LEFT JOIN stg_tickertape_data_table_1 tt
    ON TRIM(UPPER(ipo.ticker)) = TRIM(UPPER(tt.ticker))

WHERE ipo.listing_dt IS NOT NULL
    AND sub.company_name IS NOT NULL
    AND tt.ticker IS NOT NULL
-- Order by most recent IPOs first
ORDER BY ipo.listing_dt DESC NULLS LAST, ipo.ipo_end_dt DESC;