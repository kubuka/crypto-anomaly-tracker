import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import os

# 1. Ustawienia strony Streamlit
st.set_page_config(layout="wide")
st.title("Crypto Dashboard 📈")

DB_PATH = "../dbt_crypto/crypto_lakehouse.db"


def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)

    query = """
        SELECT * FROM gold.mart_crypto_anomalies 
    """

    df = conn.execute(query).df()
    conn.close()
    return df


try:
    df = load_data()

    coin_list = df["coin_name"].unique()
    selected_coin = st.selectbox("Wybierz kryptowalutę:", coin_list)

    filtered_data = df[df["coin_name"] == selected_coin].sort_values("price_timestamp")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=filtered_data["price_timestamp"],
            y=filtered_data["current_price"],
            mode="lines",
            name="Cena",
            line=dict(color="#1f77b4", width=2),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=filtered_data["price_timestamp"],
            y=filtered_data["rolling_avg"],
            mode="lines",
            name="Średnia krocząca (10 dni)",
            line=dict(color="orange", width=2, dash="dash"),
        )
    )

    df_anom = filtered_data[filtered_data["is_anomaly"] == 1]
    fig.add_trace(
        go.Scatter(
            x=df_anom["price_timestamp"],
            y=df_anom["current_price"],
            mode="markers",
            name="Anomalia",
            marker=dict(
                color="red", size=12, symbol="circle", line=dict(width=2, color="white")
            ),
        )
    )

    fig.update_layout(
        title=f"Analiza kryptowaluty: {selected_coin}",
        xaxis_title="Data",
        yaxis_title="Cena (USD)",
        yaxis=dict(tickformat="~f"),
        hovermode="x unified",
        template="plotly_dark",
    )

    st.plotly_chart(fig, use_container_width=True)

    if not df_anom.empty:
        st.warning(f"Wykryto {len(df_anom)} anomalii w wybranym okresie!")
    else:
        st.success("Brak anomalii. Rynek stabilny.")

    st.write("Tabela z danymi:")
    st.dataframe(
        filtered_data[["price_timestamp", "current_price"]].sort_values(
            "price_timestamp", ascending=False
        )
    )

except Exception as e:
    st.error("Nie udało się połączyć z bazą danych!")
    st.code(e)
