-- The mart is not unexpectedly small.
SELECT COUNT(*) AS mart_rows, getvariable('min_mart_rows') AS required
FROM mart_ipo_analytics
HAVING COUNT(*) < getvariable('min_mart_rows');
