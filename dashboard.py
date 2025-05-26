import streamlit as st
import duckdb
import pandas as pd

from src.utils.db import get_connection

con = get_connection()

st.set_page_config(
    page_title="MeteoPanda Dashboard",
    page_icon="🌦️",

    layout="wide"
)

st.title("🐼 MeteoPanda Dashboard")

df = con.execute("SELECT * FROM gold.city_yearly_summary").df()


with st.sidebar:
    #Filtro de año
    year = st.selectbox("Año", options=df['year'].unique(), index=0)
    #Filtro de Mes
    month = st.selectbox("Mes", options=sorted(df['month'].unique()), index=0)
    #Filtro de Region
    region = st.selectbox("Region", options=df['region'].unique(), index=0)
    #Filtro de Ciudades
    city = st.multiselect("Ciudad", options=df['city'].unique())


#Aplicamos los filtros en cascada
if year:
    df_filtered = df[df['year'] == year]
    if month:
        df_filtered = df_filtered[df_filtered['month'] <= month]
    if region:
        df_filtered = df_filtered[df_filtered['region'] == region]
    if city:
        df_filtered = df_filtered[df_filtered['city'].isin(city)]



st.header(f"Resumen de {month}/{year}")

#Metricas
cols = st.columns(len(df_filtered['city'].unique()))

# Calculate metrics
metrics = {
    "Temperatura Promedio": "avg_temp",
    "Temperatura Máxima": "max_temp", 
    "Temperatura Mínima": "min_temp",
    "Precipitación Total": "total_precip",
    "Tiempo de Sol": "total_sunshine"
}

# Display metrics for each city in columns
for i, city_name in enumerate(df_filtered['city'].unique()):
    city_data = df_filtered[df_filtered['city'] == city_name]
    
    with cols[i]:
        st.subheader(city_name.capitalize())
        for metric_name, metric_col in metrics.items():
            value = city_data[metric_col].iloc[0]
            if pd.isna(value):
                st.metric(label=metric_name, value="Sin datos")
                if metric_name == "Precipitación Total":
                    st.progress(0)
            else:
                if metric_name == "Precipitación Total":
                  
                    # Mostrar el valor 
                    st.metric(label=f"{metric_name}", value=f"{round(value, 2)} mm")
                    
                    # Mostrar barra de progreso
                    max_precip = 100 
                    st.progress(min(value/max_precip, 1.0))
                else:
                    st.metric(label=metric_name, value=round(value, 2))


st.header(f"Dias extremos de {month}/{year}")

# Crear nuevas columnas para días extremos
num_cities = len(df_filtered['city'].unique())
    
df_extreme = con.execute("SELECT * FROM gold.city_extreme_days").df()
extreme_cols = st.columns(num_cities)

if year:
    df_filtered = df_extreme[df_extreme['year'] == year]
    if month:
        df_filtered = df_filtered[df_filtered['month'] <= month]
    if region:
        df_filtered = df_filtered[df_filtered['region'] == region]
    if city:
        df_filtered = df_filtered[df_filtered['city'].isin(city)]

extreme_metrics = {
    "Fecha": "date",
    "Temperatura Máxima": "temp_max_c", 
    "Temperatura Mínima": "temp_min_c",
    "Precipitación Total": "precip_mm"
}

# Display metrics for each city in columns
for i, city_name in enumerate(df_filtered['city'].unique()):
    city_data = df_filtered[df_filtered['city'] == city_name]
    
    with extreme_cols[i]:
        st.subheader(city_name.capitalize())
        for metric_name, metric_col in extreme_metrics.items():
            value = city_data[metric_col].iloc[0]
            if pd.isna(value):
                st.metric(label=metric_name, value="Sin datos")
            else:
                if metric_name == "Fecha":
                    # Convertir la fecha a formato string
                    value = value.strftime('%d/%m/%Y')
                if metric_name == "Precipitación Total":
                    # Mostrar el valor 
                    st.metric(label=f"{metric_name}", value=f"{round(value, 2)} mm")
                    
                    # Mostrar barra de progreso
                    max_precip = 100 
                    st.progress(min(value/max_precip, 1.0))
                st.metric(label=metric_name, value=value)