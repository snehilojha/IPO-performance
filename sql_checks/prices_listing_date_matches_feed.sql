-- Where the NSE feed's listing date looked valid, the first trading day in the bhavcopy agrees with it.
SELECT ticker, ipo_end_dt, listing_dt_reported, listing_dt, listing_symbol
FROM mart_ipo_analytics
WHERE listing_dt_flag = 'ok'
  AND listing_dt_source = 'nse_bhavcopy'
  AND listing_dt <> listing_dt_reported;
