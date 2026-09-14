-- The mart keeps every listed IPO up to the snapshot date; nothing is silently dropped.
SELECT i.ticker, i.ipo_end_dt
FROM int_ipo_listing i
LEFT JOIN mart_ipo_analytics m USING (ticker, ipo_end_dt)
WHERE i.is_listed
  AND m.ticker IS NULL;
