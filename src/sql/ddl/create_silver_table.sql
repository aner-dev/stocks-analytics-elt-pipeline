CREATE TABLE IF NOT EXISTS stocks.weekly_adjusted_stocks (
    timestamp DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjusted_close DOUBLE PRECISION,
    volume BIGINT,
    _ingestion_timestamp TIMESTAMP WITH TIME ZONE,
    _data_source VARCHAR(50),
    _processing_date DATE,

    -- PK compose by the key of time series and symbol 
    PRIMARY KEY (timestamp, symbol)
);
