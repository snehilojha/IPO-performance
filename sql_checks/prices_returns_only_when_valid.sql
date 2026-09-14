-- A horizon return exists only when the horizon has passed, the price is recent, it is not after an
-- unexplained gap, and the price date is on or before the horizon date.
SELECT ticker, ipo_end_dt, horizon, horizon_dt, price_dt, is_elapsed, has_recent_price, is_after_price_gap
FROM int_ipo_horizon_returns
WHERE return_pct IS NOT NULL
  AND (NOT is_elapsed OR NOT has_recent_price OR is_after_price_gap OR price_dt > horizon_dt);
