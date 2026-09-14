-- The NIFTY 50 series has a value for (almost) every trading day in the price history.
SELECT COUNT(*) AS trading_days_without_nifty
FROM (SELECT DISTINCT trade_dt FROM stg_nse_prices) d
ANTI JOIN stg_nifty50 n USING (trade_dt)
WHERE d.trade_dt BETWEEN (SELECT MIN(trade_dt) FROM stg_nifty50) AND (SELECT MAX(trade_dt) FROM stg_nifty50)
HAVING COUNT(*) > 5;
