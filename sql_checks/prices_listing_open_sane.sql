-- Listing-day open and close lie within 0.2x-10x of the issue price.
SELECT ticker, ipo_end_dt, issue_price, listing_open, listing_close
FROM mart_ipo_analytics
WHERE has_listing_price
  AND NOT (listing_open BETWEEN issue_price * 0.2 AND issue_price * 10
           AND listing_close BETWEEN issue_price * 0.2 AND issue_price * 10);
