import streamlit as st
import polars as pl
import os
import plotly.express as px
import time

# Professional page configuration
st.set_page_config(
    page_title="Stocks Gold Layer Monitor", layout="wide", page_icon="📈"
)


@st.cache_resource
def get_db_uri():
    """Retrieves the database connection URI from environment variables."""
    return os.getenv(
        "DB_URL", "postgresql://postgres:postgres@localhost:5432/stocks_dwh"
    )


@st.cache_data(ttl=60)
def load_gold_data():
    """Loads Gold layer data using the ConnectorX engine for maximum speed."""
    uri = get_db_uri()
    query = """
    SELECT 
        trade_date as date,
        symbol,
        company_name,
        adjusted_close_price,
        weekly_return_pct,
        volatility_pct,
        trade_volume
    FROM public.fact_adjusted_prices
    ORDER BY trade_date ASC
    """
    return pl.read_database_uri(query, uri, engine="connectorx")


def main():
    st.title("📈 Stocks Analytics: Gold Layer")
    st.markdown("---")

    try:
        # --- 1. PERFORMANCE MEASUREMENT (DATA ENGINEERING OPS) ---
        t_start = time.perf_counter()

        df = load_gold_data()

        # --- 2. SIDEBAR FILTERS ---
        st.sidebar.header("🎛️ Pipeline Controls")

        # Get unique list of symbols
        all_symbols = df["symbol"].unique().sort().to_list()

        selected_symbols = st.sidebar.multiselect(
            "Select Tickers",
            options=all_symbols,
            default=all_symbols[:3] if len(all_symbols) >= 3 else all_symbols,
        )

        # --- 3. ANALYTICAL PROCESSING (POLARS ENGINE) ---
        # Ultra-fast filtering
        filtered_df = df.filter(pl.col("symbol").is_in(selected_symbols))

        # Statistical Anomaly Detection (Z-Score)
        # Calculate price deviation relative to its mean per symbol
        filtered_df = filtered_df.with_columns(
            [
                (
                    (
                        pl.col("adjusted_close_price")
                        - pl.col("adjusted_close_price").mean().over("symbol")
                    )
                    / pl.col("adjusted_close_price").std().over("symbol")
                ).alias("z_score")
            ]
        )

        # Count rows considered anomalies (outside +/- 2 standard deviations)
        anomaly_count = filtered_df.filter(pl.col("z_score").abs() > 2).height

        # End of latency measurement
        t_end = time.perf_counter()
        proc_time = t_end - t_start

        # --- 4. BUSINESS & OPERATIONAL KPIs ---
        st.subheader("🚀 Operational Metrics & Portfolio Health")
        last_date = df["date"].max()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Active Tickers", df["symbol"].n_unique())
        with col2:
            st.metric("Processing Latency", f"{proc_time:.4f}s")
        with col3:
            avg_return = filtered_df["weekly_return_pct"].mean()
            # Validate data presence to avoid formatting errors
            ret_val = avg_return if avg_return is not None else 0
            st.metric("Avg Weekly Return", f"{ret_val:.2%}")
        with col4:
            st.metric(
                "Detected Anomalies",
                anomaly_count,
                delta_color="inverse",
                help="Records with Z-Score > 2 (atypical price variation)",
            )

        st.caption(f"📅 Gold Layer data updated until: **{last_date}**")

        # --- 4.5 DYNAMIC INSIGHTS (Data Driven) ---
        highest_stock = filtered_df.sort("adjusted_close_price", descending=True).limit(
            1
        )
        if not highest_stock.is_empty():
            ticker = highest_stock["symbol"][0]
            price = highest_stock["adjusted_close_price"][0]
            st.info(
                f"💡 **Top Performer Ingested:** {ticker} is leading the current batch at ${price:.2f}. Pipeline validated high-variance movement via Z-Score."
            )

        # --- 5. VISUALIZATIONS (POLARS -> PANDAS BRIDGE FOR PLOTLY) ---

        # Adjusted Price Line Chart
        st.subheader("Market Trend Analysis")
        fig = px.line(
            filtered_df.to_pandas(),
            x="date",
            y="adjusted_close_price",
            color="symbol",
            hover_data=["company_name", "z_score"],
            template="plotly_dark",
            labels={"adjusted_close_price": "Adjusted Price (USD)", "date": "Week"},
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Volatility Chart
        st.subheader("Risk Analysis: Weekly Volatility (Z-Score Integrated)")
        fig_vol = px.box(
            filtered_df.to_pandas(),
            x="symbol",
            y="volatility_pct",
            color="symbol",
            template="plotly_dark",
            labels={"volatility_pct": "Volatility %", "symbol": "Ticker"},
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        # --- 6. DATA AUDIT TABLE ---
        st.subheader(" 󰆼  Gold Layer: Fact Adjusted Prices (Final Model)")
        st.dataframe(
            filtered_df.to_pandas().sort_values("date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

        # Sidebar Footer
        st.sidebar.markdown("---")
        st.sidebar.caption("🚀 **Backend:** Polars + ConnectorX")
        st.sidebar.caption(f"⚙️ **Rows Ingested:** {len(df)}")
        st.sidebar.caption(f"⏱️ **Total Latency:** {proc_time * 1000:.2f} ms")

    except Exception as e:
        st.error(f"❌ Error loading Star Schema: {e}")
        st.info(
            "Check the database connection and ensure Gold tables exist in Postgres."
        )


if __name__ == "__main__":
    main()
