DROP TABLE IF EXISTS stg_pastipo_subscription_table_1;

CREATE TABLE stg_pastipo_subscription_table_1 (
    company_name VARCHAR,
    ipo_start_dt DATE,
    ipo_end_dt DATE,
    total_issued_in_cr DOUBLE,
    qib_x DOUBLE,
    nii_x DOUBLE,
    retail_x DOUBLE,
    total_x DOUBLE,
    ipo_type VARCHAR,
    year INTEGER,
    UNIQUE (company_name, ipo_start_dt, ipo_end_dt)
);

INSERT INTO stg_pastipo_subscription_table_1
SELECT
    company_name,
    ipo_start_dt,
    ipo_end_dt,
    total_issued_in_cr,
    qib_x,
    nii_x,
    retail_x,
    total_x,
    ipo_type,
    year
FROM (
    SELECT
        base.company_name,
        base.ipo_start_dt,
        base.ipo_end_dt,
        base.total_issued_in_cr,
        base.qib_x,
        base.nii_x,
        base.retail_x,
        base.total_x,
        base.ipo_type,
        base.year,
        ROW_NUMBER() OVER (
            PARTITION BY base.company_name, base.ipo_start_dt, base.ipo_end_dt
            ORDER BY base.year DESC
        ) AS rn
    FROM (
        SELECT
            CASE
                WHEN Company IS NULL THEN "Company Name"
                ELSE Company
            END AS company_name,
            "~Issue_Open_Date" AS ipo_start_dt,
            "~Issue_Close_Date" AS ipo_end_dt,
            "Total Issue Amount (Incl.Firm reservations) (Rs.cr.)" AS total_issued_in_cr,
            "QIB (x)" AS qib_x,
            "NII (x)" AS nii_x,
            "Retail (x)" AS retail_x,
            "Total (x)" AS total_x,
            ipo_type AS ipo_type,
            year AS year
        FROM raw_pastipo_subscription_table_1
    ) AS base
) 
WHERE rn = 1;
