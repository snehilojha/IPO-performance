DROP TABLE IF EXISTS stg_past_ipo_table_1;

CREATE TABLE stg_past_ipo_table_1 (
    ticker VARCHAR ,
    company_name VARCHAR,
    ipo_start_dt DATE,
    ipo_end_dt DATE,
    listing_dt DATE,
    security_type VARCHAR,
    issue_price INTEGER
);

INSERT INTO stg_past_ipo_table_1
SELECT 
    ticker,
    company_name,
    ipo_start_dt,
    ipo_end_dt,
    listing_dt,
    security_type,
    issue_price
FROM (
    SELECT
        symbol as ticker,
        company as company_name,
        STRPTIME(ipostartdate, '%d-%b-%Y') AS ipo_start_dt,
        STRPTIME(ipoenddate, '%d-%b-%Y') AS ipo_end_dt,
        STRPTIME(NULLIF(listingdate, '-'), '%d-%b-%Y') AS listing_dt,
        securitytype as security_type,
        CAST(NULLIF(TRIM(REGEXP_EXTRACT(pricerange, '(\d+)\s*$')),'') AS INTEGER) AS issue_price,

        ROW_NUMBER() OVER (
            PARTITION BY symbol,
            STRPTIME(NULLIF(listingdate, '-'), '%d-%b-%Y')
            ORDER BY STRPTIME(ipoenddate, '%d-%b-%Y') DESC
        ) AS rn
    FROM raw_past_ipo_table_1
)
WHERE rn = 1;