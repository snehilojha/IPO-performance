-- NIFTY 50 daily index values (price index, no dividends — comparable with the unadjusted-for-dividends
-- stock returns in the mart).

CREATE OR REPLACE TABLE stg_nifty50 AS
SELECT
    TRY_CAST(trade_dt AS DATE)    AS trade_dt,
    TRY_CAST(open AS DOUBLE)      AS open,
    TRY_CAST(high AS DOUBLE)      AS high,
    TRY_CAST(low AS DOUBLE)       AS low,
    TRY_CAST(close AS DOUBLE)     AS close
FROM raw_nifty50
WHERE TRY_CAST(close AS DOUBLE) > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(trade_dt AS DATE) ORDER BY close) = 1;
