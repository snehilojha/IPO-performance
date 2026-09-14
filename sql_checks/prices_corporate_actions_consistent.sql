-- Every bonus / split factor applied to an IPO's prices matches the actual move on its ex-date: the previous
-- close divided by the ex-date open is within 0.6x-1.67x of the factor (checked where both days traded
-- within 10 days of each other).
WITH factors AS (
    SELECT ticker, ipo_end_dt, ex_dt, EXP(SUM(LN(share_factor))) AS share_factor
    FROM int_ipo_corporate_actions
    GROUP BY ALL
),
moves AS (
    SELECT
        ticker, ipo_end_dt, trade_dt, open,
        LAG(close) OVER w    AS previous_close,
        LAG(trade_dt) OVER w AS previous_trade_dt
    FROM int_ipo_price_history
    WINDOW w AS (PARTITION BY ticker, ipo_end_dt ORDER BY trade_dt)
)
SELECT f.ticker, f.ipo_end_dt, f.ex_dt, f.share_factor, m.previous_close, m.open,
       ROUND(m.previous_close / m.open, 3) AS observed_ratio
FROM factors f
JOIN moves m ON m.ticker = f.ticker AND m.ipo_end_dt = f.ipo_end_dt AND m.trade_dt = f.ex_dt
WHERE m.trade_dt - m.previous_trade_dt <= 10
  AND NOT (m.previous_close / m.open / f.share_factor BETWEEN 0.6 AND 1.0 / 0.6);
