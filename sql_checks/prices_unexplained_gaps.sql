-- Only a handful of overnight moves are left unexplained by corporate actions; many more would mean the
-- corporate actions feed or its parsing broke.
SELECT COUNT(*) AS unexplained_gaps, getvariable('max_unexplained_price_gaps') AS allowed
FROM int_ipo_price_gaps
HAVING COUNT(*) > getvariable('max_unexplained_price_gaps');
