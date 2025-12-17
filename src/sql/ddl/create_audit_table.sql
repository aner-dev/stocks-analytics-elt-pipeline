-- sql/ddl/02_create_table_pipeline_audit.sql 

CREATE TABLE IF NOT EXISTS stocks.pipeline_audit (
    audit_id SERIAL PRIMARY KEY,
    -- Added: The name of the pipeline task, e.g., 'transform_load_silver'
    pipeline_step VARCHAR(50) NOT NULL,
    stock_symbol VARCHAR(10) NOT NULL,

    -- Volume Metrics (Directly from Polars)
    -- Corresponds to: 'initial_records' (records read before transformation)
    rows_processed INT,
    -- Corresponds to: 'final_records' (records successfully loaded)
    rows_inserted INT,
    -- Records filtered out due to quality checks or issues
    rows_rejected INT,

    -- Time Metrics
    duration_seconds DOUBLE PRECISION,
    -- When the audit record was registered
    run_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
