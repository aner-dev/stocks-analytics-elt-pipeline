import os
import time
import plotly.express as px
import polars as pl
import streamlit as st

st.set_page_config(page_title="Stocks Gold Layer Monitor", layout="wide", page_icon="📈")


@st.cache_resource
def get_db_uri():
    return os.getenv("DB_URL", "postgresql://postgres:postgres@localhost:5432/stocks_dwh")


@st.cache_data(ttl=60)
def load_gold_data():
    uri = get_db_uri()
    query = """
    SELECT 
        d.week_ending as date,
        s.symbol,
        s.company_name,
        s.sector_group,
        f.adjusted_close_price,
        f.weekly_return_pct,
        f.volatility_pct,
        f.trade_volume
    FROM public.fact_adjusted_prices f
    JOIN public.dim_stock s ON f.stock_id = s.stock_id
    JOIN public.dim_date d ON f.date_id = d.date_id
    ORDER BY d.week_ending ASC
    """
    return pl.read_database_uri(query, uri, engine="connectorx")


def main():
    st.title("📈 Stocks Analytics: Gold Layer")
    st.markdown("---")

    try:
        t_start = time.perf_counter()
        df = load_gold_data()

        # --- FILTROS ---
        st.sidebar.header("🎛️ Pipeline Controls")
        all_sectors = df["sector_group"].unique().sort().to_list()
        selected_sectors = st.sidebar.multiselect(
            "Filter by Sector", options=all_sectors, default=all_sectors
        )

        df_sector = df.filter(pl.col("sector_group").is_in(selected_sectors))
        available_tickers = df_sector["symbol"].unique().sort().to_list()
        selected_symbols = st.sidebar.multiselect(
            "Select Tickers", options=available_tickers, default=available_tickers[:3]
        )

        filtered_df = df_sector.filter(pl.col("symbol").is_in(selected_symbols))

        # --- PROCESAMIENTO ---
        if not filtered_df.is_empty():
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
            anomaly_count = filtered_df.filter(pl.col("z_score").abs() > 2).height
        else:
            anomaly_count = 0

        proc_time = time.perf_counter() - t_start

        # --- SECCIÓN 1: KPIs ---
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Tickers", filtered_df["symbol"].n_unique())
        with col2:
            v = filtered_df["volatility_pct"].mean()
            st.metric("Avg Volatility", f"{(v * 100 if v else 0):.2f}%")
        with col3:
            r = filtered_df["weekly_return_pct"].mean()
            st.metric("Avg Return", f"{(r if r else 0):.2%}")
        with col4:
            st.metric("Total Volume", f"{filtered_df['trade_volume'].sum() / 1e6:.1f}M")
        with col5:
            st.metric("Anomalies", anomaly_count, delta_color="inverse")

        # --- SECCIÓN 2: GRÁFICOS PRINCIPALES ---
        tab1, tab2 = st.tabs(["📈 Market Trends", "⚖️ Risk & Sectors"])

        with tab1:
            fig_line = px.line(
                filtered_df.to_pandas(),
                x="date",
                y="adjusted_close_price",
                color="symbol",
                hover_data=["company_name"],
                template="plotly_dark",
                title="Price Trends",
            )
            st.plotly_chart(fig_line, use_container_width=True)

        with tab2:
            c1, c2 = st.columns(2)
            with c1:
                # RECUPERADO: Gráfico de Riesgo (Boxplot)
                fig_risk = px.box(
                    filtered_df.to_pandas(),
                    x="symbol",
                    y="volatility_pct",
                    color="symbol",
                    template="plotly_dark",
                    title="Volatility Risk Distribution",
                )
                st.plotly_chart(fig_risk, use_container_width=True)
            with c2:
                fig_pie = px.pie(
                    filtered_df.to_pandas(),
                    values="trade_volume",
                    names="sector_group",
                    template="plotly_dark",
                    title="Volume by Sector",
                    hole=0.4,
                )
                st.plotly_chart(fig_pie, use_container_width=True)

        # --- SECCIÓN 3: DATA AUDIT ---
        with st.expander("🔍 View Raw Gold Layer Data"):
            st.dataframe(
                filtered_df.to_pandas().sort_values("date", ascending=False),
                use_container_width=True,
            )

    except Exception as e:
        st.error(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
