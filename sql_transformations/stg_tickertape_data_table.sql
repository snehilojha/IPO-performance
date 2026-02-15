DROP TABLE IF EXISTS stg_tickertape_data_table_1;

CREATE TABLE stg_tickertape_data_table_1 (
    ticker VARCHAR,
    company VARCHAR,
    sector VARCHAR,
    subindustry VARCHAR,
    current_market_cap DOUBLE,
    last_price DOUBLE,
    current_pe_ratio DOUBLE,
    current_roe_ratio DOUBLE,
    current_pb_ratio DOUBLE,
    return_last_4w_pct DOUBLE,
    return_last_1d_pct DOUBLE,
    UNIQUE (ticker, company)
);


INSERT INTO stg_tickertape_data_table_1
SELECT
      ticker AS ticker,
      name AS company,
      sector AS sector,
      subindustry AS subindustry,
      market_cap AS current_market_cap,
      last_price AS last_price,
      COALESCE(pe,0) AS current_pe_ratio,
      COALESCE(roe,0) AS current_roe_ration,
      COALESCE(pb,0) AS current_pb_ratio,
      "4w_return_pct" AS return_last_4w_pct,
      "1d_return_pct" AS return_last_1d_pct
FROM raw_tickertape_data_1