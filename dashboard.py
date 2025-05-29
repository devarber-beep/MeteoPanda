import streamlit as st
import duckdb
import pandas as pd
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go

from src.utils.db import get_connection

con = get_connection()

st.set_page_config(
    page_title="MeteoPanda Dashboard",
    page_icon="🌦️",
    layout="wide"
)

st.title("🐼 MeteoPanda Dashboard")

# Cargar datos
df = con.execute("SELECT * FROM gold.city_yearly_summary").df()
df_extreme = con.execute("SELECT * FROM gold.city_extreme_days").df()
df_trends = con.execute("SELECT * FROM gold.weather_trends").df()
df_climate = con.execute("SELECT * FROM gold.climate_profiles").df()
# Cargar coordenadas de las ciudades
df_coords = con.execute("""
    SELECT DISTINCT city, lat, lon 
    FROM silver.weather_cleaned
""").df()

# Sidebar con filtros comunes
with st.sidebar:
    # Selector de páginas
    st.header("Navegación")
    page = st.radio("Selecciona una página", ["Inicio", "Resumen", "Tendencias"])
    
    #Filtro de año
    year = st.selectbox("Año", options=df['year'].unique(), index=0)
    #Filtro de Mes
    month = st.selectbox("Mes", options=sorted(df['month'].unique()), index=0)
    #Filtro de Region
    region = st.selectbox("Region", options=df['region'].unique(), index=0)
    #Filtro de Ciudades
    city = st.multiselect("Ciudad", options=df['city'].unique())

# Función para aplicar filtros según las columnas disponibles
def apply_filters(df):
    if year and 'year' in df.columns:
        df_filtered = df[df['year'] == year]
        if month and 'month' in df.columns:
            df_filtered = df_filtered[df_filtered['month'] <= month]
        if region and 'region' in df.columns:
            df_filtered = df_filtered[df_filtered['region'] == region]
        if city and 'city' in df.columns:
            df_filtered = df_filtered[df_filtered['city'].isin(city)]
        return df_filtered
    return df

# Aplicar filtros
df_filtered = apply_filters(df)
df_extreme_filtered = apply_filters(df_extreme)
df_trends_filtered = apply_filters(df_trends)

# Página de Inicio
if page == "Inicio":
    st.header("🌍 Mapa de España")
    
    # Crear mapa centrado en España
    m = folium.Map(location=[40.4168, -3.7038], zoom_start=6)
    
    # Añadir marcadores para cada ciudad
    for city_name in df_filtered['city'].unique():
        city_coords = df_coords[df_coords['city'] == city_name]
        if not city_coords.empty:
            lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
            city_data = df_filtered[df_filtered['city'] == city_name]
            temp = city_data['avg_temp'].iloc[0] if not city_data.empty else "N/A"
            
            folium.Marker(
                location=[lat, lon],
                popup=f"{city_name}: {temp}°C",
                tooltip=city_name
            ).add_to(m)
    
    # Mostrar el mapa
    folium_static(m, width=1200, height=600)
    
    # Mostrar KPIs generales
    st.header("📊 KPIs Generales")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Ciudades Monitoreadas", len(df['city'].unique()))
    with col2:
        st.metric("Regiones", len(df['region'].unique()))
    with col3:
        st.metric("Años de Datos", len(df['year'].unique()))

# Página de Resumen
elif page == "Resumen":
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
                        st.metric(label=f"{metric_name}", value=f"{round(value, 2)} mm")
                        max_precip = 100 
                        st.progress(min(value/max_precip, 1.0))
                    else:
                        st.metric(label=metric_name, value=round(value, 2))

    st.header(f"Dias extremos de {month}/{year}")
    
    # Crear nuevas columnas para días extremos
    num_cities = len(df_extreme_filtered['city'].unique())
    extreme_cols = st.columns(num_cities)

    extreme_metrics = {
        "Fecha": "date",
        "Temperatura Máxima": "temp_max_c", 
        "Temperatura Mínima": "temp_min_c",
        "Precipitación Total": "precip_mm"
    }

    # Display metrics for each city in columns
    for i, city_name in enumerate(df_extreme_filtered['city'].unique()):
        city_data = df_extreme_filtered[df_extreme_filtered['city'] == city_name]
        
        with extreme_cols[i]:
            st.subheader(city_name.capitalize())
            for metric_name, metric_col in extreme_metrics.items():
                value = city_data[metric_col].iloc[0]
                if pd.isna(value):
                    st.metric(label=metric_name, value="Sin datos")
                else:
                    if metric_name == "Fecha":
                        value = value.strftime('%d/%m/%Y')
                    if metric_name == "Precipitación Total":
                        st.metric(label=f"{metric_name}", value=f"{round(value, 2)} mm")
                        max_precip = 100 
                        st.progress(min(value/max_precip, 1.0))
                    st.metric(label=metric_name, value=value)

# Página de Tendencias
else:
    st.header("📈 Tendencias Climáticas")
    
    # Gráfico de tendencias de temperatura
    st.subheader("Evolución de Temperatura por Ciudad")
    fig_temp = px.line(df_trends_filtered, 
                      x='year', 
                      y='avg_temp', 
                      color='city',
                      title='Tendencia de Temperatura Promedio por Ciudad')
    st.plotly_chart(fig_temp, use_container_width=True)
    
    # Gráfico de precipitación
    st.subheader("Evolución de Precipitación por Ciudad")
    fig_precip = px.bar(df_trends_filtered, 
                       x='year', 
                       y='total_precip', 
                       color='city',
                       title='Precipitación Total por Ciudad')
    st.plotly_chart(fig_precip, use_container_width=True)
    
    # Perfiles climáticos
    st.subheader("Perfiles Climáticos")
    col1, col2 = st.columns(2)
    
    with col1:
        # Temperaturas extremas
        fig_extremes = px.bar(df_climate, 
                            x='city', 
                            y=['record_high', 'record_low'],
                            title='Temperaturas Extremas por Ciudad',
                            barmode='group')
        st.plotly_chart(fig_extremes, use_container_width=True)
    
    with col2:
        # Precipitación anual
        fig_precip_annual = px.bar(df_climate, 
                                 x='city', 
                                 y='yearly_precip',
                                 title='Precipitación Anual por Ciudad')
        st.plotly_chart(fig_precip_annual, use_container_width=True)