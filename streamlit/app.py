import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="📈 Portfolio Tracker", layout="wide")
st_autorefresh(interval=5000, key="refresh")

# ======================== Connexions ========================
def connect():
    return psycopg2.connect(
        dbname="postgres", user="spark", password="spark123", host="postgres", port="5432"
    )

@st.cache_data
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

df_live = load_live()
df_live['date_calc'] = pd.to_datetime(df_live['date_calc'], errors='coerce')
df_portfolio = load_portfolio()

# ======================== Préparation données live ========================
cols = ['vwap', 'volatibilite_pct', 'drawdown', 'transactions_totales', 'roi_simule']
for col in cols:
    df_live[col] = pd.to_numeric(df_live[col], errors='coerce')

# ======================== Sidebar ========================
st.sidebar.title("🎛️ Filtres")

# On récupère tous les tickers possibles (live + portfolio)
tickers_live = df_live['ticker'].dropna().unique()
tickers_portfolio = df_portfolio['ticker'].dropna().unique()
all_tickers = sorted(set(tickers_live) | set(tickers_portfolio))

selected_tickers = st.sidebar.multiselect(
    "Sélectionnez le(s) ticker(s) :",
    options=all_tickers,
    default=all_tickers
)

df_filtered = df_live[df_live['ticker'].isin(selected_tickers)]

# ======================== KPI Avancés — stock_data_agg ========================
st.title("📊 KPI Avancés — Sélection (stock_data_agg)")

if df_filtered.empty:
    st.warning("Aucun ticker sélectionné ou pas de données pour la sélection.")
else:
    vwap_global         = df_filtered['vwap'].mean()
    volatilite_pct      = df_filtered['volatibilite_pct'].mean()
    max_drawdown        = df_filtered['drawdown'].min()
    transactions_total  = df_filtered['transactions_totales'].sum()
    roi_simule_moyen    = df_filtered['roi_simule'].mean()

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric("VWAP (moyen)", f"{vwap_global:.2f}")
    k2.metric("Volatilité (%)", f"{volatilite_pct:.2f}")
    k3.metric("Max Drawdown (%)", f"{max_drawdown:.2f}")
    k4.metric("Transactions", f"{transactions_total:,}")
    k5.metric("ROI Simulé (%)", f"{roi_simule_moyen:.2f}")

    with st.expander("📋 Voir données filtrées (stock_data_agg)"):
        st.dataframe(df_filtered)

# ======================== Performance Portfolio ========================
st.title("📈 Performance du Portfolio")

latest_batch = df_live['date_calc'].max()
latest_batch = pd.to_datetime(latest_batch)

df_last = df_live.groupby("ticker").tail(1).copy()
df_last["ferm"] = pd.to_numeric(df_last["ferm"], errors="coerce")

# Portfolio filtré aux transactions <= dernier batch
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

capital_initial = (merged["prix_moyen"] * merged["quantite_nette"]).sum()
pnl_global = merged["PnL"].sum()
roi_total = (pnl_global / capital_initial) * 100 if capital_initial != 0 else 0
drawdown_global = df_live['drawdown'].min()

if not merged.empty and merged['ROI'].notna().any():
    best_row = merged.loc[merged['ROI'].idxmax()]
    worst_row = merged.loc[merged['ROI'].idxmin()]
else:
    best_row = pd.Series({'ticker': 'N/A', 'ROI': 0})
    worst_row = pd.Series({'ticker': 'N/A', 'ROI': 0})

k1, k2 = st.columns(2)
with k1:
    st.metric("💵 Capital Initial", f"{capital_initial:.0f} $")
with k2:
    st.metric("📈 PnL Global", f"{pnl_global:.0f} $")

k3, k4 = st.columns(2)
with k3:
    st.metric(f"🏆 Meilleur Ticker\n{best_row['ticker']}", f"{best_row['ROI']:.1f} %")
with k4:
    st.metric(f"🔻 Pire Ticker\n{worst_row['ticker']}", f"{worst_row['ROI']:.1f} %")

k5, k6 = st.columns(2)
with k5:
    st.metric("📉 Drawdown Global", f"{drawdown_global:.1f} %")
with k6:
    st.metric(f"📊 ROI Total", f"{roi_total:.1f} %")

st.subheader("Répartition par Ticker du portefeuille")

fig = px.pie(
    merged,
    names='ticker',
    values='quantite_nette',
    title='Répartition des positions',
    hole=0.3
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("📋 Voir détails des positions"):
    st.dataframe(merged)

# ======================== Graphiques — Performance, Risques, Marché & Timing ========================
st.title("📊 Graphiques — Performance, Risques, Marché & Timing")

# Évolution du portefeuille — PnL cumulé dans le temps
st.subheader("Évolution du portefeuille - PnL cumulé")
pnl_cumule = df_live.copy()
pnl_cumule['pnl_cumule'] = pnl_cumule.groupby('ticker')['roi_simule'].cumsum()

fig1 = go.Figure()
for ticker in selected_tickers:
    sub = pnl_cumule[pnl_cumule['ticker'] == ticker]
    fig1.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['pnl_cumule'],
        mode='lines', name=ticker
    ))
fig1.update_layout(
    xaxis_title="Date", yaxis_title="PnL cumulé",
    template="plotly_white"
)
st.plotly_chart(fig1, use_container_width=True)

# Drawdown
st.subheader("Drawdown")
fig2 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig2.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['drawdown'],
        mode='lines', name=ticker
    ))
fig2.update_layout(
    xaxis_title="Date", yaxis_title="Drawdown (%)",
    template="plotly_white"
)
st.plotly_chart(fig2, use_container_width=True)

# Prix + VWAP
st.subheader("Prix & VWAP")
fig3 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig3.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['ferm'],
        mode='lines', name=f"{ticker} - Prix"
    ))
    fig3.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['vwap'],
        mode='lines', name=f"{ticker} - VWAP", line=dict(dash='dot')
    ))
fig3.update_layout(
    xaxis_title="Date", yaxis_title="Prix",
    template="plotly_white"
)
st.plotly_chart(fig3, use_container_width=True)

# PnL par actif (bar chart)
st.subheader("PnL par actif")
fig4 = go.Figure()
fig4.add_trace(go.Bar(
    x=merged['ticker'], y=merged['PnL'],
    marker_color=['green' if x >=0 else 'red' for x in merged['PnL']]
))
fig4.update_layout(
    xaxis_title="Ticker", yaxis_title="PnL",
    template="plotly_white"
)
st.plotly_chart(fig4, use_container_width=True)

# Volatilité
st.subheader("Volatilité")
fig5 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig5.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['volatibilite_pct'],
        mode='lines', name=ticker
    ))
fig5.update_layout(
    xaxis_title="Date", yaxis_title="Volatilité (%)",
    template="plotly_white"
)
st.plotly_chart(fig5, use_container_width=True)

# Volumes
st.subheader("Volumes")
fig6 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig6.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['transactions_totales'],
        mode='lines', name=ticker
    ))
fig6.update_layout(
    xaxis_title="Date", yaxis_title="Volumes",
    template="plotly_white"
)
st.plotly_chart(fig6, use_container_width=True)

st.info("Rafraîchissement automatique toutes les 5 secondes. Données en dollars ($).")
