-- The price history has at most one row per IPO per trading day.
SELECT ticker, ipo_end_dt, trade_dt, COUNT(*) AS n
FROM int_ipo_price_history
GROUP BY ALL
HAVING COUNT(*) > 1;
