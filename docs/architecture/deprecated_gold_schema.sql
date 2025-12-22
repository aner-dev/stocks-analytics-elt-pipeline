-- gold_schema.sql 
-- date dimension 
CREATE TABLE dim_date (
  date_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  week_ending DATE NOT NULL UNIQUE,
  year INTEGER NOT NULL CHECK (year BETWEEN 2000 AND 2030), 
  week_number INTEGER NOT NULL CHECK (week_number BETWEEN 1 AND 53), 
  quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4), 
  -- metadata 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

  
COMMENT ON TABLE dim_date IS 'Time dimension for weekly analysis of stocks';
COMMENT ON COLUMN dim_date.week_ending IS 'date weekend (usually friday)';
  

CREATE TABLE dim_stock (
  stock_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  symbol VARCHAR (10) NOT NULL UNIQUE,
  company_name VARCHAR (150) NOT NULL,
  sector VARCHAR(30),
  industry VARCHAR(30),
  exchange_code VARCHAR (10),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  country VARCHAR (30), 
  -- metadata 
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);



COMMENT ON TABLE dim_stock IS 'dimension of stocks/enterprises';
COMMENT ON COLUMN dim_stock.symbol IS 'ticker symbol (IBM, AAPL, etc.)';

CREATE TABLE fact_stock_prices (
    -- foreign keys 
    stock_id BIGINT NOT NULL REFERENCES dim_stock (stock_id),
    date_id BIGINT NOT NULL REFERENCES dim_date (date_id),
    -- facts 
    open_price NUMERIC NOT NULL,
    high_price NUMERIC NOT NULL,
    low_price NUMERIC NOT NULL,
    close_price NUMERIC NOT NULL,
    adjusted_close NUMERIC NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    weekly_return_abs NUMERIC,
    weekly_return_pct NUMERIC,
    trading_range_abs NUMERIC,
    volume_usd NUMERIC,

    -- compose primary key (for physical implementation of grain)
    PRIMARY KEY (stock_id, date_id),
    -- metadata
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
);

