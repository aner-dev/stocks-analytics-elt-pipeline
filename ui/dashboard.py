import streamlit as st
import polars as pl
import os
import plotly.express as px

st.set_page_config(page_title="Gold Layer Monitor", layout="wide")


@st.cache_resource
def get_db_uri():
    return os.getenv("DB_URL")


@st.cache_data(ttl=60)
def load_gold_data():
    uri = get_db_uri()

    # query profesional: Unimos la FACT con DIM_STOCK y DIM_DATE
    # Esto demuestra que tu Star Schema funciona perfectamente
    query = """
    SELECT 
        d.week_ending as date,
        s.symbol,
        f.adjusted_close_price,
        f.weekly_return_pct,
        f.trade_volume,
        f.volume_usd
    FROM public.fact_adjusted_prices f
    INNER JOIN public.dim_stock s ON f.stock_id = s.stock_id
    INNER JOIN public.dim_date d ON f.date_id = d.date_id
    ORDER BY d.week_ending ASC
    """
    return pl.read_database(query, connection=uri)


def main():
    st.title("⚡ Polars + dbt Star Schema Dashboard")
    st.markdown("---")

    try:
        df = load_gold_data()

        # --- KPIs USANDO POLARS ---
        # Aprovechamos que ya tenemos las métricas calculadas en dbt (Gold)
        last_date = df["date"].max()
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Symbols", df["symbol"].n_unique())
        with col2:
            st.metric("Last Week ending", str(last_date))
        with col3:
            avg_return = df["weekly_return_pct"].mean()
            st.metric("Avg Weekly Return", f"{avg_return:.2%}")

        # --- GRÁFICO DE PRECIOS ---
        st.subheader("Market Trends (From Fact Table)")
        fig = px.line(
            df.to_pandas(),
            x="date",
            y="adjusted_close_price",
            color="symbol",
            template="plotly_dark",
            labels={"adjusted_close_price": "Price (USD)", "date": "Week Ending"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- TABLA DE AUDITORÍA ---
        st.subheader("🔍 Gold Layer Inspection")
        st.dataframe(df.to_pandas().tail(10), use_container_width=True)

    except Exception as e:
        st.error(f"Error cargando el Star Schema: {e}")
        st.info("Asegúrate de que 'dbt run' haya creado las tablas dim_ y fact_.")


if __name__ == "__main__":
    main()

