import psycopg2
import pandas as pd
import streamlit as st

def connect():
    return psycopg2.connect(
        dbname="postgres",
        user="spark",
        password="spark123",
        host="postgres",  # Pas localhost ici !
        port="5432"
    )

def load_data():
    conn = connect()
    df = pd.read_sql(
        "SELECT * FROM public.stock_data_agg ORDER BY date_calc DESC LIMIT 100",
        conn
    )
    conn.close()
    return df

# Streamlit UI
st.title("📊 Indicateur VWAP : Écart prix/vwap")

df = load_data()

# Pour chaque ligne du DataFrame
for _, row in df.iterrows():
    ticker = row['ticker']
    close = row['plus_haut']  # ou 'close' si tu veux le prix de clôture exact
    vwap = row['vwap']

    if close > vwap:
        st.success(f"✅ Le prix actuel de **{ticker}** est **au-dessus** du VWAP (**{close:.2f} > {vwap:.2f}**)")
    elif close < vwap:
        st.error(f"❌ Le prix actuel de **{ticker}** est **en dessous** du VWAP (**{close:.2f} < {vwap:.2f}**)")
    else:
        st.info(f"➖ Le prix actuel de **{ticker}** est **égal** au VWAP (**{close:.2f} = {vwap:.2f}**)")