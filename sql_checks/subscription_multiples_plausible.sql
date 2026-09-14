-- Subscription multiples stay below 5000x (the largest real one is about 2,550x); anything above is a
-- placeholder or a parsing problem. Chittorgarh's 9999 placeholder is nulled in stg_ipo_subscription.
SELECT ticker, ipo_end_dt, subscription_qib_x, subscription_nii_x, subscription_snii_x, subscription_bnii_x,
       subscription_retail_x, subscription_employee_x, subscription_total_x
FROM mart_ipo_analytics
WHERE GREATEST(subscription_qib_x, subscription_nii_x, subscription_snii_x, subscription_bnii_x,
               subscription_retail_x, subscription_employee_x, subscription_total_x) >= 5000;
