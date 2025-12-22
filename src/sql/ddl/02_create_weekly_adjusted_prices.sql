CREATE TABLE IF NOT EXISTS stocks.weekly_adjusted_prices (
    price_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,

    open NUMERIC(15, 4),
    high NUMERIC(15, 4),
    low NUMERIC(15, 4),
    close NUMERIC(15, 4),
    adjusted_close NUMERIC(15, 4),
    volume BIGINT,

    _ingestion_timestamp TIMESTAMPTZ,
    _data_source VARCHAR(50) DEFAULT 'alpha_vantage',
    _processing_date DATE DEFAULT CURRENT_DATE,

    -- PK compose by the key of time series and symbol 
    PRIMARY KEY (price_date, symbol)
);
