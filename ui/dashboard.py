# dashboard.py
import streamlit as st
import polars as pl
import os
import plotly.express as px

st.set_page_config(page_title="Stocks Gold Layer Monitor", layout="wide")


@st.cache_resource
def get_db_uri():
    # Asegúrate de que esta variable esté en tu .env o entorno
    return os.getenv(
        "DB_URL", "postgresql://postgres:postgres@localhost:5432/stocks_dwh"
    )


@st.cache_data(ttl=60)
def load_gold_data():
    uri = get_db_uri()

    # QUERY OPTIMIZADA:
    # Usamos las columnas denormalizadas que añadimos en el ADR-003.
    # Ya no necesitamos JOINs pesados porque la Fact Table tiene lo necesario para el BI.
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
        df = load_gold_data()

        # --- FILTROS EN SIDEBAR ---
        st.sidebar.header("Filtros")
        selected_symbols = st.sidebar.multiselect(
            "Selecciona Tickers",
            options=df["symbol"].unique().sort(),
            default=df["symbol"].unique().sort()[:3],
        )

        # Filtrado con Polars (Ultra rápido)
        filtered_df = df.filter(pl.col("symbol").is_in(selected_symbols))

        # --- KPIs ---
        last_date = df["date"].max()
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Tickers Activos", df["symbol"].n_unique())
        with col2:
            st.metric("Última Actualización", str(last_date))
        with col3:
            avg_return = filtered_df["weekly_return_pct"].mean()
            st.metric("Avg Weekly Return", f"{avg_return:.2%}")
        with col4:
            # Nueva métrica gracias al refactor
            avg_vol = filtered_df["volatility_pct"].mean()
            st.metric("Avg Volatility", f"{avg_vol:.2%}")

        # --- GRÁFICO DE PRECIOS ---
        st.subheader("Evolución de Precios Ajustados")
        fig = px.line(
            filtered_df.to_pandas(),
            x="date",
            y="adjusted_close_price",
            color="symbol",
            hover_data=["company_name"],  # ¡Gracias al Seed de dbt!
            template="plotly_dark",
            labels={"adjusted_close_price": "Precio Ajustado (USD)", "date": "Semana"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- ANÁLISIS DE VOLATILIDAD ---
        st.subheader("Volatilidad Semanal por Símbolo")
        fig_vol = px.box(
            filtered_df.to_pandas(),
            x="symbol",
            y="volatility_pct",
            color="symbol",
            template="plotly_dark",
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        # --- TABLA DE AUDITORÍA ---
        st.subheader("🔍 Inspección de Datos (Gold Layer)")
        st.dataframe(
            filtered_df.to_pandas().sort_values("date", ascending=False),
            use_container_width=True,
        )

    except Exception as e:
        st.error(f"Error cargando el Star Schema: {e}")
        st.info("Revisa la conexión a la base de datos y que las tablas Gold existan.")


if __name__ == "__main__":
    main()
