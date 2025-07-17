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

# ======================== Données ========================
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

# ======================== Chargement ========================
df_live = load_live()
df_portfolio = load_portfolio()

# Filtre horaire (09:30–16:00) pour la bourse
start_time = pd.to_datetime("09:30:00").time()
end_time = pd.to_datetime("16:00:00").time()
df_live = df_live[df_live['date_calc'].dt.time.between(start_time, end_time)]

latest_batch = df_live['date_calc'].max()
df_live_latest = df_live.groupby("ticker").tail(1).copy()
df_portfolio = df_portfolio[df_portfolio['date_heure'] <= latest_batch]

# Positions actuelles
df_portfolio["sens"] = df_portfolio["action"].map({"Achat": 1, "Vente": -1})
df_portfolio["quantite_nette"] = df_portfolio["quantite"] * df_portfolio["sens"]

positions = df_portfolio.groupby("ticker").agg(
    quantite_nette=("quantite_nette", "sum"),
    prix_moyen=("prix", "mean"),
    company=("company", "first")
).reset_index()

merged = positions.merge(df_live_latest, on="ticker", how="left")
merged["PnL"] = (merged["ferm"] - merged["prix_moyen"]) * merged["quantite_nette"]
merged["ROI (%)"] = ((merged["ferm"] - merged["prix_moyen"]) / merged["prix_moyen"]) * 100
merged["Volatilité (%)"] = merged["volatibilite_pct"]
merged["Drawdown (%)"] = merged["drawdown"]

# ======================== Sidebar ========================
st.sidebar.title("Filtres")
tickers = st.sidebar.multiselect("Sélectionnez les tickers :", merged["ticker"].unique(), default=list(merged["ticker"].unique()))
merged = merged[merged["ticker"].isin(tickers)]

# ======================== KPI Résumé ========================
st.title(f"📈 Portfolio Tracker — {latest_batch:%Y-%m-%d %H:%M:%S}")

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("VWAP", f"{merged['vwap'].mean():.2f}")
k2.metric("Volatilité (%)", f"{merged['Volatilité (%)'].mean():.2f}")
k3.metric("Drawdown (%)", f"{merged['Drawdown (%)'].mean():.2f}")
k4.metric("PnL ($)", f"{merged['PnL'].sum():.2f}")
k5.metric("ROI moyen (%)", f"{merged['ROI (%)'].mean():.2f}")

# ======================== Graphique Prix + Tendances ========================
st.subheader("📈 Prix réel + tendances")
for ticker in tickers:
    sub = df_live[df_live["ticker"] == ticker].copy()
    sub.sort_values("date_calc", inplace=True)

    if sub.empty:
        continue

    sub["mm5_close"] = sub["ferm"].rolling(5).mean()
    sub["typical_price"] = (sub["plus_haut"] + sub["plus_bas"] + sub["ferm"]) / 3

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sub["date_calc"], y=sub["ferm"], mode='lines', name='Close'))
    fig.add_trace(go.Scatter(x=sub["date_calc"], y=sub["mm5_close"], mode='lines', name='MM5 Close'))
    fig.add_trace(go.Scatter(x=sub["date_calc"], y=sub["typical_price"], mode='lines', name='Typical Price'))

    fig.update_layout(title=f"{ticker}", xaxis_title="Date", yaxis_title="Prix ($)", template="plotly_white", height=400)
    st.plotly_chart(fig, use_container_width=True)

# ======================== Graphique Activité ========================
st.subheader("📊 Volume & Variation")
for ticker in tickers:
    sub = df_live[df_live["ticker"] == ticker].copy()
    sub.sort_values("date_calc", inplace=True)

    couleur = sub["roi_simule"].apply(lambda x: "green" if x >= 0 else "red")

    fig = go.Figure()
    fig.add_trace(go.Bar(x=sub["date_calc"], y=sub["volume_moyen"], marker_color=couleur))

    fig.update_layout(title=f"Volume & Variation — {ticker}", xaxis_title="Date", yaxis_title="Volume", template="plotly_white", height=300)
    st.plotly_chart(fig, use_container_width=True)

# ======================== Graphique Performance & Risque ========================
st.subheader("📉 PnL simulé & Drawdown")
for ticker in tickers:
    sub = df_live[df_live["ticker"] == ticker].copy()
    sub.sort_values("date_calc", inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sub["date_calc"], y=sub["roi_simule"], mode='lines', name='ROI Simulé'))
    fig.add_trace(go.Scatter(x=sub["date_calc"], y=sub["drawdown"], mode='lines', name='Drawdown'))

    fig.update_layout(title=f"Performance & Risque — {ticker}", xaxis_title="Date", yaxis_title="%", template="plotly_white", height=300)
    st.plotly_chart(fig, use_container_width=True)

# ======================== PnL par ticker ========================
st.subheader("📊 PnL par ticker")
fig = go.Figure()
fig.add_trace(go.Bar(x=merged["ticker"], y=merged["PnL"], marker_color="seagreen"))
fig.update_layout(xaxis_title="Ticker", yaxis_title="PnL ($)", template="plotly_white", height=400)
st.plotly_chart(fig, use_container_width=True)

# ======================== Tableau dynamique ========================
with st.expander("📋 Voir données live"):
    st.dataframe(df_live.sort_values("date_calc", ascending=False))

# ======================== Alerte intelligente ========================
if merged["Drawdown (%)"].min() < -5:
    st.error("⚠ Grosse perte détectée : drawdown élevé !")

st.info("Rafraîchissement automatique toutes les 5 secondes. Données en dollars ($).")