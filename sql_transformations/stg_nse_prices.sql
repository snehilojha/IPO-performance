-- Daily unadjusted prices from the NSE CM bhavcopy, one row per symbol per trading day.
-- Only securities linked to an IPO symbol are in the raw file (see scrapers/nse_price_scraper.py).
-- The bhavcopy has high, low and trade counts too; the snapshot file keeps only what the mart uses.

CREATE OR REPLACE TABLE stg_nse_prices AS
SELECT
    trade_dt::DATE              AS trade_dt,
    UPPER(TRIM(symbol))         AS symbol,
    TRIM(series)                AS series,
    TRIM(isin)                  AS isin,
    open::DOUBLE                AS open,
    close::DOUBLE               AS close,
    prev_close::DOUBLE          AS prev_close,
    volume::BIGINT              AS volume
FROM raw_nse_prices
WHERE close > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trade_dt, UPPER(TRIM(symbol))
    ORDER BY volume DESC NULLS LAST, series
) = 1;
