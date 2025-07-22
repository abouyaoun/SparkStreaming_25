import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="📈 Suivi de portefeuille", layout="wide")
st_autorefresh(interval=5000, key="refresh")

st.title("📈 Suivi de portefeuille et analyseur de risques en temps réel")


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
cols = ['ferm', 'volatibilite_pct', 'drawdown', 'transactions_totales', 'roi_simule']
for col in cols:
    df_live[col] = pd.to_numeric(df_live[col], errors='coerce')

# ======================== Sidebar ========================
st.sidebar.title("🎛️ Filtres")

tickers_live = df_live['ticker'].dropna().unique()
tickers_portfolio = df_portfolio['ticker'].dropna().unique()
all_tickers = sorted(set(tickers_live) | set(tickers_portfolio))

selected_tickers = st.sidebar.multiselect(
    "Sélectionnez le(s) ticker(s) :",
    options=all_tickers,
    default=tickers_portfolio
)

df_filtered = df_live[df_live['ticker'].isin(selected_tickers)]

# ======================== KPI Avancés — stock_data_agg ========================
st.title("📊 KPI Avancés — Sélection (stock_data_agg)")

if df_filtered.empty:
    st.warning("Aucun ticker sélectionné ou pas de données pour la sélection.")
else:
    prix_moyen        = df_filtered['ferm'].mean()
    volatilite_pct     = df_filtered['volatibilite_pct'].mean()
    max_drawdown       = df_filtered['drawdown'].min()
    transactions_total = df_filtered['transactions_totales'].sum()
    roi_simule_moyen   = df_filtered['roi_simule'].mean()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Prix moyen actuel", f"{prix_moyen:.2f}")
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

capital_initial_csv = (df_portfolio['quantite'] * df_portfolio['prix']).sum()
capital_actuel = (merged['ferm'] * merged['quantite_nette']).sum()
pnl_global = capital_actuel - capital_initial_csv
roi_total = (pnl_global / capital_initial_csv) * 100 if capital_initial_csv != 0 else 0
drawdown_global = df_live['drawdown'].min()

if not merged.empty and merged['ROI'].notna().any():
    best_row = merged.loc[merged['ROI'].idxmax()]
    worst_row = merged.loc[merged['ROI'].idxmin()]
else:
    best_row = pd.Series({'ticker': 'N/A', 'ROI': 0})
    worst_row = pd.Series({'ticker': 'N/A', 'ROI': 0})

k1, k2 = st.columns(2)
with k1:
    st.metric("💵 Capital Actuel", f"{capital_actuel:,.0f} $")
with k2:
    st.metric("📈 PnL Global", f"{pnl_global:.0f} $")
st.markdown("&nbsp;")

k3, k4 = st.columns(2)
with k3:
    st.metric(f"🏆 Meilleur Ticker\n{best_row['ticker']}", f"{best_row['ROI']:.1f} %")
with k4:
    st.metric(f"🔻 Pire Ticker\n{worst_row['ticker']}", f"{worst_row['ROI']:.1f} %")
st.markdown("&nbsp;")

k5, k6 = st.columns(2)
with k5:
    st.metric("📉 Drawdown Global", f"{drawdown_global:.1f} %")
with k6:
    st.metric(f"📊 ROI Total", f"{roi_total:.1f} %")
st.markdown("&nbsp;")

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

def update_fig_xaxis(fig):
    fig.update_layout(
        xaxis_title="Date",
        xaxis_tickformat="%H:%M",
        template="plotly_white"
    )
    return fig

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
st.plotly_chart(update_fig_xaxis(fig1), use_container_width=True)

# Drawdown
st.subheader("Drawdown")
fig2 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig2.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['drawdown'],
        mode='lines', name=ticker
    ))
st.plotly_chart(update_fig_xaxis(fig2), use_container_width=True)

# Prix
st.subheader("Prix")
fig3 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig3.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['ferm'],
        mode='lines', name=f"{ticker} - Prix"
    ))
st.plotly_chart(update_fig_xaxis(fig3), use_container_width=True)

# PnL par actif
st.subheader("PnL par actif")
fig4 = go.Figure()
fig4.add_trace(go.Bar(
    x=merged['ticker'], y=merged['PnL'],
    marker_color=['green' if x >= 0 else 'red' for x in merged['PnL']]
))
st.plotly_chart(update_fig_xaxis(fig4), use_container_width=True)

# Volatilité
st.subheader("Volatilité")
fig5 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig5.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['volatibilite_pct'],
        mode='lines', name=ticker
    ))
st.plotly_chart(update_fig_xaxis(fig5), use_container_width=True)

# Volumes
st.subheader("Volumes")
fig6 = go.Figure()
for ticker in selected_tickers:
    sub = df_live[df_live['ticker'] == ticker]
    fig6.add_trace(go.Scatter(
        x=sub['date_calc'], y=sub['transactions_totales'],
        mode='lines', name=ticker
    ))
st.plotly_chart(update_fig_xaxis(fig6), use_container_width=True)

st.info("Rafraîchissement automatique toutes les 5 secondes. Données en dollars ($).")
