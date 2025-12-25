# 🏛️ Financial Data Warehouse: Gold Layer Analytics

Technical oversight of the curated data layer. Metrics are derived from the **Star Schema** materialized by dbt, ensuring referential integrity between fact tables and dimensions.

```sql market_metrics
SELECT 
    s.symbol,
    s.company_name,
    d.week_ending,
    f.adjusted_close_price,
    f.weekly_return_pct,
    f.trading_range_abs,
    f.volume_usd,
    f.load_timestamp
FROM stocks.fact_adjusted_prices f
JOIN stocks.dim_stock s ON f.stock_id = s.stock_id
JOIN stocks.dim_date d ON f.date_id = d.date_id
ORDER BY d.week_ending DESC
```

<Grid cols={3}>
    <BigValue 
        data={market_metrics} 
        value=symbol 
        agg=count_distinct
        title="Tracked Symbols"
    />
    <BigValue 
        data={market_metrics} 
        value=volume_usd 
        agg=sum 
        title="Total Traded Volume (USD)"
        fmt=usd
    />
    <BigValue 
        data={market_metrics} 
        value=load_timestamp 
        agg=max 
        title="Last Ingestion (UTC)"
    />
</Grid>

## 📈 Market Dynamics

Visualization of business logic calculated during the transformation layer (Gold).

<LineChart
data={market_metrics}
x=week_ending
y=weekly_return_pct
series=symbol
title="Calculated Weekly Returns (%)"
yAxisTitle="Return %"
/>

## 🔍 Analytical Audit Trail

Direct access to the `fact_adjusted_prices` granularity. Used for auditing incremental load results and surrogate key mapping.

<DataTable data={market_metrics} search=true sort=true>
  <Column id="symbol" title="Ticker"/>
  <Column id="week_ending" title="Week Ending"/>
  <Column id="adjusted_close_price" title="Adj. Close" fmt=usd/>
  <Column id="weekly_return_pct" title="Return" fmt=pct2/>
  <Column id="volume_usd" title="Volume USD" fmt=usd0/>
</DataTable>

## 📊 Volatility Analysis

Trading range (High-Low) as an indicator of market volatility.

<BarChart
data={market_metrics}
x=week_ending
y=trading_range_abs
series=symbol
title="Market Volatility (Trading Range)"
/>
