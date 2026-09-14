-- The mart keeps every listed IPO up to the snapshot date; nothing is silently dropped.
SELECT i.ticker, i.ipo_end_dt
FROM stg_past_ipo i
LEFT JOIN mart_ipo_analytics m USING (ticker, ipo_end_dt)
WHERE i.is_listed_ipo
  AND i.ipo_end_dt <= getvariable('snapshot_date')::DATE
  AND m.ticker IS NULL;
