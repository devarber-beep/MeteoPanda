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
# Cargar coordenadas reales desde la base de datos
df_coords = con.execute("""
    SELECT DISTINCT city, lat, lon 
    FROM silver.weather_cleaned
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").df()

# Si no hay coordenadas en la BD, usar coordenadas hardcodeadas como fallback
if df_coords.empty:
    city_coordinates = {
        'Sevilla': {'lat': 37.3891, 'lon': -5.9845},
        'Malaga': {'lat': 36.7213, 'lon': -4.4217},
        'Cordoba': {'lat': 37.8882, 'lon': -4.7794},
        'Granada': {'lat': 37.1765, 'lon': -3.5976},
        'Almeria': {'lat': 36.8402, 'lon': -2.4679},
        'Cadiz': {'lat': 36.5298, 'lon': -6.2924},
        'Huelva': {'lat': 37.2578, 'lon': -6.9497},
        'Jaen': {'lat': 37.7796, 'lon': -3.7849}
    }
    
    df_coords = pd.DataFrame([
        {'city': city, 'lat': coords['lat'], 'lon': coords['lon']}
        for city, coords in city_coordinates.items()
    ])

# Asegurar que df_extreme y df_trends tengan la columna 'region' para un filtrado consistente
city_region_map = df[['city', 'region']].drop_duplicates()
df_extreme = pd.merge(df_extreme, city_region_map, on='city', how='left')
df_trends = pd.merge(df_trends, city_region_map, on='city', how='left')

# Sidebar con filtros comunes
with st.sidebar:
    # Selector de páginas
    st.header("Navegación")
    page = st.radio("Selecciona una página", ["Inicio", "Tendencias", "Resumen"])
    
    #Filtros
    year = st.selectbox("Año", options=df['year'].unique(), index=0)
    month = st.selectbox("Mes", options=sorted(df['month'].unique()), index=0)
    region = st.selectbox("Region", options=df['region'].unique(), index=0)
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
    m = folium.Map(location=[40.4168, -3.7038], zoom_start=6, tiles='CartoDB positron')
    
    # Añadir marcadores para cada ciudad
    for city_name in df_filtered['city'].unique():
        city_coords = df_coords[df_coords['city'] == city_name]
        if not city_coords.empty:
            lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
            city_data = df_filtered[df_filtered['city'] == city_name]
            temp = city_data['avg_temp'].iloc[0] if not city_data.empty else "N/A"
            
            folium.Marker(
                location=[lat, lon],
                popup=f"{city_name.capitalize()}: {temp}°C",
                tooltip=city_name.capitalize()
            ).add_to(m)
    
    # Mostrar el mapa
    folium_static(m, width=1200, height=600)
    
    # Mostrar KPIs generales
    st.header("📊 Informacion General")
    col1, col2, col3, col4 = st.columns(4)
    
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

    # Mostrar Metricas
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
    
    # Crear nuevas columnas para días extremos pero manteniendo la proporcion de las columnas
    num_cities = len(df_filtered['city'].unique())
    extreme_cols = st.columns(num_cities)

    extreme_metrics = {
        "Fecha": "date",
        "Temperatura Máxima": "temp_max_c", 
        "Temperatura Mínima": "temp_min_c",
        "Precipitación Total": "precip_mm"
    }

    # Mostrar metricas
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
    
    # Aplicar filtros solo de región y ciudad para las tendencias
    df_trends_filtered = df_trends.copy()
    if region and 'region' in df_trends_filtered.columns:
        df_trends_filtered = df_trends_filtered[df_trends_filtered['region'] == region]
    if city and 'city' in df_trends_filtered.columns:
        df_trends_filtered = df_trends_filtered[df_trends_filtered['city'].isin(city)]
    
    # Ordenar por año para ver la evolución correctamente
    df_trends_filtered = df_trends_filtered.sort_values('year')
    
    # Gráfico de tendencias de temperatura
    st.subheader("Evolución de Temperatura por Ciudad")
    
    # Capitalizar los nombres de las ciudades para la leyenda
    df_trends_filtered['city_capitalized'] = df_trends_filtered['city'].apply(lambda x: x.capitalize())
    
    fig_temp = px.line(df_trends_filtered, 
                      x='year', 
                      y='avg_temp', 
                      color='city_capitalized',
                      title='Tendencia de Temperatura Promedio por Ciudad')
    fig_temp.update_xaxes(type='category', title_text='Año') # Asegura que los años se muestren en orden y establece el título del eje X
    fig_temp.update_yaxes(title_text='Media de Temperatura') # Establece el título del eje Y
    fig_temp.update_layout(legend_title_text='Ciudad') # Establece el título de la leyenda
    st.plotly_chart(fig_temp, use_container_width=True)
    
    # Gráfico de precipitación
    st.subheader("Evolución de Precipitación por Ciudad")
    
    fig_precip = go.Figure()
    
    # Añadir una barra para cada ciudad
    for city_name in df_trends_filtered['city'].unique():
        city_data = df_trends_filtered[df_trends_filtered['city'] == city_name]
        fig_precip.add_trace(go.Bar(
            x=city_data['year'],
            y=city_data['total_precip'],
            name=city_name.capitalize()
        ))

    # Actualizar layout para agrupar barras y añadir título
    fig_precip.update_layout(
        barmode='group',
        title_text='Precipitación Total por Ciudad',
        xaxis_title_text='Año',
        yaxis_title_text='Precipitación Total (mm)'
    )

    # Asegura que los años se muestren en orden
    fig_precip.update_xaxes(type='category')
    
    st.plotly_chart(fig_precip, use_container_width=True)
    
    st.subheader("Perfiles Climáticos")
    col1, col2 = st.columns(2)
    
    with col1:
        # Temperaturas extremas
        # Determinar las ciudades a mostrar en el eje X
        cities_to_plot = df['city'].unique()
        if region:
            cities_in_selected_region = df[df['region'] == region]['city'].unique()
            cities_to_plot = [c for c in cities_to_plot if c in cities_in_selected_region]
        if city: # Si el usuario seleccionó ciudades específicas
            cities_to_plot = [c for c in cities_to_plot if c in city]
        cities_to_plot = sorted(cities_to_plot)

        df_temp_extremes_base = df_extreme.copy()

        # Aplicar filtros de año, mes y las ciudades a mostrar
        if year:
            df_temp_extremes_base = df_temp_extremes_base[df_temp_extremes_base['year'] == year]
        if month:
            df_temp_extremes_base = df_temp_extremes_base[df_temp_extremes_base['month'] == month]
        # Asegurarse de que solo se procesen las ciudades relevantes y existan en el DataFrame base
        df_temp_extremes_base = df_temp_extremes_base[df_temp_extremes_base['city'].isin(cities_to_plot)].copy()


        df_extremes_filtered_agg = df_temp_extremes_base.groupby('city').agg(
            min_temp=('min_temp_month', 'min'),
            max_temp=('max_temp_month', 'max')
        ).reset_index()

        # Reindexar para asegurar que todas las ciudades estén presentes, incluso si no tienen datos
        df_extremes_filtered_agg = df_extremes_filtered_agg.set_index('city').reindex(cities_to_plot).reset_index()
        
        # Preparar datos para el gráfico
        df_extremes_melted = df_extremes_filtered_agg.melt(
            id_vars='city',
            value_vars=['min_temp', 'max_temp'],
            var_name='extreme_type',
            value_name='temperature'
        )
        
        # Renombrar los tipos de extremos para mejor visualización
        df_extremes_melted['extreme_type'] = df_extremes_melted['extreme_type'].map({
            'max_temp': 'Máxima',
            'min_temp': 'Mínima'
        })
        
        # Capitalizar los nombres de las ciudades
        df_extremes_melted['city'] = df_extremes_melted['city'].str.capitalize()
        
        # Renombrar las columnas para el gráfico
        df_extremes_melted = df_extremes_melted.rename(columns={
            'city': 'Ciudad',
            'temperature': 'Temperatura'
        })
        
        # Asegurar que el orden de las ciudades en el eje X sea el deseado (categorías explícitas)
        df_extremes_melted['Ciudad'] = pd.Categorical(df_extremes_melted['Ciudad'], categories=[c.capitalize() for c in cities_to_plot], ordered=True)
        df_extremes_melted = df_extremes_melted.sort_values('Ciudad')

        fig_extremes = px.bar(df_extremes_melted, 
                            x='Ciudad', 
                            y='Temperatura',
                            color='extreme_type',
                            barmode='group',
                            title='Temperaturas Extremas por Ciudad',
                            color_discrete_map={'Máxima': '#ff7f0e', 'Mínima': '#1f77b4'})
        
        # Ajustar el rango del eje Y para mostrar temperaturas negativas
        y_min_val = df_extremes_melted['Temperatura'].min()
        y_max_val = df_extremes_melted['Temperatura'].max()
        fig_extremes.update_yaxes(range=[min(y_min_val - 5, 0) if pd.notna(y_min_val) else -5, 
                                       (y_max_val + 5) if pd.notna(y_max_val) else 5])
        
        st.plotly_chart(fig_extremes, use_container_width=True)
    
    with col2:
        # Precipitación mensual
        # Determinar las ciudades a mostrar en el eje X (misma lógica que para Temperaturas Extremas)
        cities_to_plot_precip = df['city'].unique()
        if region:
            cities_in_selected_region_precip = df[df['region'] == region]['city'].unique()
            cities_to_plot_precip = [c for c in cities_to_plot_precip if c in cities_in_selected_region_precip]
        if city:
            cities_to_plot_precip = [c for c in cities_to_plot_precip if c in city]
        cities_to_plot_precip = sorted(cities_to_plot_precip)

        df_precip_monthly_base = df_extreme.copy()

        # Aplicar filtros de año, mes y las ciudades a mostrar
        if year:
            df_precip_monthly_base = df_precip_monthly_base[df_precip_monthly_base['year'] == year]
        if month:
            df_precip_monthly_base = df_precip_monthly_base[df_precip_monthly_base['month'] == month]
        # Asegurarse de que solo se procesen las ciudades relevantes
        df_precip_monthly_base = df_precip_monthly_base[df_precip_monthly_base['city'].isin(cities_to_plot_precip)].copy()


        df_precip_monthly_agg = df_precip_monthly_base.groupby('city').agg(
            total_precip=('total_precip_month', 'sum')
        ).reset_index()

        # Reindexar para asegurar que todas las ciudades estén presentes y rellenar con 0
        df_precip_monthly_agg = df_precip_monthly_agg.set_index('city').reindex(cities_to_plot_precip).reset_index().fillna(0)

        # Encontrar el valor máximo para destacarlo
        max_precip = df_precip_monthly_agg['total_precip'].max()
        df_precip_monthly_agg['is_max'] = df_precip_monthly_agg['total_precip'] == max_precip

        # Capitalizar los nombres de las ciudades
        df_precip_monthly_agg['city'] = df_precip_monthly_agg['city'].str.capitalize()

        # Renombrar las columnas para el gráfico
        df_precip_monthly_agg = df_precip_monthly_agg.rename(columns={
            'city': 'Ciudad',
            'total_precip': 'Precipitación'
        })

        fig_precip_monthly = px.bar(df_precip_monthly_agg, 
                                  x='Ciudad', 
                                  y='Precipitación',
                                  color='is_max',
                                  color_discrete_sequence=['#1f77b4', '#ff7f0e'],
                                  title='Precipitación Mensual por Ciudad')
        fig_precip_monthly.update_layout(showlegend=False)
        st.plotly_chart(fig_precip_monthly, use_container_width=True)