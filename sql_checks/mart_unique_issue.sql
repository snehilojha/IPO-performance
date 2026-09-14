-- Each IPO appears once in the mart.
SELECT ticker, ipo_end_dt, COUNT(*) AS n
FROM mart_ipo_analytics
GROUP BY ticker, ipo_end_dt
HAVING COUNT(*) > 1;
