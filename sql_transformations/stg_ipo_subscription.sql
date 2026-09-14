-- One row per issue in the Chittorgarh subscription reports (mainboard and SME, all exchanges).
-- The two report layouts are merged: mainboard rows use Company / Closing Date,
-- SME rows use Company Name / Open Date / Close Date.

CREATE OR REPLACE TABLE stg_ipo_subscription AS
WITH unified AS (
    SELECT
        TRY_CAST("~id" AS BIGINT)                                         AS subscription_id,
        TRIM(COALESCE(Company, "Company Name"))                           AS company_name,
        norm_company_name(COALESCE(Company, "Company Name"))              AS company_name_norm,
        UPPER(TRIM(ipo_type))                                             AS segment,
        TRY_CAST(year AS INTEGER)                                         AS report_year,
        COALESCE(
            TRY_CAST("~Issue_Open_Date" AS DATE),
            TRY_STRPTIME(TRIM("Open Date"), '%b %d, %Y')::DATE
        )                                                                 AS ipo_start_dt,
        COALESCE(
            TRY_CAST("~Issue_Close_Date" AS DATE),
            TRY_STRPTIME(TRIM(COALESCE("Closing Date", "Close Date")), '%b %d, %Y')::DATE
        )                                                                 AS ipo_end_dt,
        TRY_CAST("Total Issue Amount (Incl.Firm reservations) (Rs.cr.)" AS DOUBLE) AS issue_size_cr,
        TRY_CAST("QIB (x)" AS DOUBLE)                                     AS qib_x,
        TRY_CAST("NII (x)" AS DOUBLE)                                     AS nii_x,
        TRY_CAST("sNII (x)" AS DOUBLE)                                    AS snii_x,
        TRY_CAST("bNII (x)" AS DOUBLE)                                    AS bnii_x,
        TRY_CAST("Retail (x)" AS DOUBLE)                                  AS retail_x,
        TRY_CAST("Employee (x)" AS DOUBLE)                                AS employee_x,
        TRY_CAST("Total (x)" AS DOUBLE)                                   AS total_x,
        CAST(parse_number(Applications) AS BIGINT)                        AS applications
    FROM raw_ipo_subscription
)

SELECT
    segment || '|' || company_name_norm || '|' || ipo_end_dt AS subscription_key,
    * EXCLUDE (rn)
FROM (
    SELECT
        *,
        -- The same issue can appear in two report years; keep one row, deterministically.
        ROW_NUMBER() OVER (
            PARTITION BY company_name_norm, ipo_end_dt, segment
            ORDER BY (total_x IS NULL), report_year DESC, subscription_id DESC
        ) AS rn
    FROM unified
    WHERE company_name IS NOT NULL
)
WHERE rn = 1;
