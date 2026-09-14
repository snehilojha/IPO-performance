-- Listing happens on or after issue close (dates flagged in stg_past_ipo are already NULL).
SELECT ticker, ipo_end_dt, listing_dt
FROM mart_ipo_analytics
WHERE listing_dt < ipo_end_dt;
