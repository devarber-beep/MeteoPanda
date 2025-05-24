import streamlit as st
import duckdb
import pandas as pd

from src.utils.db import get_connection

con = get_connection()

st.title("🌦️ MeteoPanda Dashboard")

df = con.execute("SELECT * FROM gold.city_yearly_summary").df()

city = st.selectbox("Ciudad", df['city'].unique())
st.line_chart(df[df['city'] == city].set_index('year')[['avg_temp', 'total_precip']])
