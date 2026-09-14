-- A ticker cannot list twice on the same day.
SELECT ticker, listing_dt, COUNT(*) AS n
FROM mart_ipo_analytics
WHERE listing_dt IS NOT NULL
GROUP BY ticker, listing_dt
HAVING COUNT(*) > 1;
