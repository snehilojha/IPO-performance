-- No IPO, subscription record or Tickertape stock is used by more than one match.
SELECT 'subscription: ipo' AS duplicated, ticker || ' ' || ipo_end_dt AS key, COUNT(*) AS n
FROM int_ipo_subscription_match GROUP BY ticker, ipo_end_dt HAVING COUNT(*) > 1
UNION ALL
SELECT 'subscription: record', subscription_key, COUNT(*)
FROM int_ipo_subscription_match GROUP BY subscription_key HAVING COUNT(*) > 1
UNION ALL
SELECT 'market: ipo', ticker || ' ' || ipo_end_dt, COUNT(*)
FROM int_ipo_market_match GROUP BY ticker, ipo_end_dt HAVING COUNT(*) > 1
UNION ALL
SELECT 'market: stock', tickertape_sid, COUNT(*)
FROM int_ipo_market_match GROUP BY tickertape_sid HAVING COUNT(*) > 1;
