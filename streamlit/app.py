import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

st.set_page_config(page_title="📈 Portfolio Tracker", layout="wide")
st_autorefresh(interval=5000, key="refresh")

# ======================== Connexions ========================
def connect():
    return psycopg2.connect(
               dbname="postgres", user="spark", password="spark123", host="postgres", port="5432"
           )


# ttl=5 pour rafraîchir toutes les 5s
def load_live():
    conn = connect()
    df = pd.read_sql("SELECT * FROM public.stock_data_agg ORDER BY date_calc", conn)
    conn.close()
    return df

@st.cache_data
def load_portfolio():
    df = pd.read_csv("Portfolio.csv")
    df.rename(columns={
        'Ticker': 'ticker',
        'Nom complet de l’entreprise': 'company',
        'Action': 'action',
        'Quantité': 'quantite',
        'Prix ($)': 'prix',
        'Date/Heure': 'date_heure'
    }, inplace=True)
    df['date_heure'] = pd.to_datetime(df['date_heure'])
    df['prix'] = df['prix'].astype(str).str.replace(",", "").astype(float)
    df['quantite'] = pd.to_numeric(df['quantite'], errors="coerce")
    return df

# ======================== Données ========================
df_live = load_live()
df_portfolio = load_portfolio()

latest_batch = df_live['date_calc'].max()
df_last = df_live.groupby("ticker").tail(1).copy()
df_last["ferm"] = pd.to_numeric(df_last["ferm"], errors="coerce")

# portefeuille filtré aux transactions <= dernier batch
df_portfolio = df_portfolio[df_portfolio['date_heure'] <= latest_batch]
df_portfolio["sens"] = df_portfolio["action"].map({"Achat": 1, "Vente": -1})
df_portfolio["quantite_nette"] = df_portfolio["quantite"] * df_portfolio["sens"]

positions = df_portfolio.groupby("ticker").agg(
    quantite_nette=("quantite_nette", "sum"),
    prix_moyen=("prix", "mean"),
    company=("company", "first")
).reset_index()

merged = positions.merge(df_last, on="ticker", how="left")
merged["PnL"] = (merged["ferm"] - merged["prix_moyen"]) * merged["quantite_nette"]
merged["ROI"] = ((merged["ferm"] - merged["prix_moyen"]) / merged["prix_moyen"]) * 100

# ======================== Sidebar ========================
st.sidebar.title("Filtres")
tickers = st.sidebar.multiselect("Sélectionnez les tickers :", merged["ticker"].unique(), default=merged["ticker"].unique())
merged = merged[merged["ticker"].isin(tickers)]

# ======================== KPI Résumé ========================
st.title(f"📈 Portfolio Tracker — {latest_batch:%Y-%m-%d %H:%M:%S}")

k1, k2, k3 = st.columns(3)
k1.metric("💰 Valeur Totale ($)", f"{(merged['ferm'] * merged['quantite_nette']).sum():.2f}")
k2.metric("📈 PnL ($)", f"{merged['PnL'].sum():.2f}")
k3.metric("📊 ROI (%)", f"{merged['ROI'].mean():.2f}")

# ======================== Graphiques ========================
st.subheader("📈 Évolution intraday des prix")
for ticker in tickers:
    sub = df_live[df_live["ticker"] == ticker].copy()
    sub.sort_values("date_calc", inplace=True)

    if sub.empty:
        continue

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sub["date_calc"], y=sub["ferm"],
        mode='lines', name='Close', line=dict(color='blue')
    ))
    fig.add_trace(go.Scatter(
        x=sub["date_calc"], y=sub["vwap"],
        mode='lines', name='VWAP', line=dict(color='purple')
    ))

    fig.update_layout(
        title=f"{ticker}",
        xaxis_title="Date",
        yaxis_title="Prix ($)",
        template="plotly_white",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)

# ======================== PnL par ticker ========================
st.subheader("📊 PnL par ticker")
fig = go.Figure()
fig.add_trace(go.Bar(
    x=merged["ticker"], y=merged["PnL"],
    marker_color="seagreen"
))
fig.update_layout(
    xaxis_title="Ticker",
    yaxis_title="PnL ($)",
    template="plotly_white",
    height=400
)
st.plotly_chart(fig, use_container_width=True)

# ======================== Données brutes ========================
with st.expander("📋 Voir données live"):
    st.dataframe(df_live)

st.info("Rafraîchissement automatique toutes les 5 secondes. Données en dollars ($).")
