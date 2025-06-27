import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Dashboard Boursier", layout="wide")

# Rafraîchir toutes les 5 secondes (5000 ms)
st_autorefresh(interval=5000, limit=None, key="refresh")

# Connexion PostgreSQL
def connect():
    return psycopg2.connect(
        dbname="postgres",
        user="spark",
        password="spark123",
        host="postgres",  # nom du service Docker
        port="5432"
    )

# Charger les données agrégées
def load_data():
    conn = connect()
    df = pd.read_sql(
        "SELECT * FROM public.stock_data_agg ORDER BY date_calc DESC LIMIT 500",
        conn
    )
    conn.close()
    return df

# Interface
st.title("📊 Tableau de bord Boursier")

df = load_data()
tickers = df["ticker"].unique().tolist()

# Sécurisation du ticker sélectionné
if "selected_ticker" not in st.session_state or st.session_state.selected_ticker not in tickers:
    st.session_state.selected_ticker = tickers[0]

selected = st.selectbox(
    "🎯 Choisissez un Ticker",
    tickers,
    index=list(tickers).index(st.session_state.selected_ticker)
)

if selected != st.session_state.selected_ticker:
    st.session_state.selected_ticker = selected

# Filtrage du ticker
sub = df[df["ticker"] == selected].sort_values("date_calc", ascending=False).head(1)

# Extraire valeurs
if not sub.empty:
    vwap = sub["vwap"].values[0]
    close = sub["plus_haut"].values[0]  # ou à adapter avec vraie valeur close
    low = sub["plus_bas"].values[0]
    high = sub["plus_haut"].values[0]
    open_price = sub["volume_moyen"].values[0]  # valeur fictive pour le test
    vol = ((high - low) / open_price) * 100 if open_price != 0 else 0
    drawdown = ((close - max(close, vwap)) / max(close, vwap)) * 100
    roi = ((close - open_price) / open_price) * 100
    nb_tx = sub["nb_enregistrements"].values[0]

    # KPI layout
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("VWAP", f"{vwap:.2f} USD")
    col2.metric("Volatilité (%)", f"{vol:.2f}")
    col3.metric("Max Drawdown (%)", f"{drawdown:.2f}")
    col4.metric("ROI Simulé (%)", f"{roi:.2f}")
    col5.metric("Transactions", f"{int(nb_tx)}")

    # Message interprétation
    if close > vwap:
        st.success(f"🟢 Le prix actuel de **{selected}** est **au-dessus** du VWAP ({close:.2f} > {vwap:.2f}) → Tendance haussière possible.")
    elif close < vwap:
        st.error(f"🔴 Le prix actuel de **{selected}** est **en-dessous** du VWAP ({close:.2f} < {vwap:.2f}) → Tendance baissière possible.")
    else:
        st.info(f"➖ Le prix actuel de **{selected}** est **égal** au VWAP (**{close:.2f} = {vwap:.2f}**)")
else:
    st.warning("Aucune donnée disponible pour ce ticker.")