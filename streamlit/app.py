import psycopg2
import pandas as pd
import streamlit as st

def connect():
    return psycopg2.connect(
        dbname="postgres",
        user="spark",
        password="spark123",
        host="postgres",  # 👈 PAS localhost ici !
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
st.title("📈 Agrégations Boursières")
df = load_data()
st.dataframe(df)