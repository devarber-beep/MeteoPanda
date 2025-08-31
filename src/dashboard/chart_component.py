"""
Componente de gráficos mejorado con Plotly y funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional, Any
import numpy as np

class AdvancedChartComponent:
    """Componente de gráficos avanzado con Plotly"""
    
    def __init__(self):
        self.color_palette = px.colors.qualitative.Set3
        self.template = "plotly_white"
    
    def render_temperature_trends(self, data: pd.DataFrame, title: str = "Tendencias de Temperatura"):
        """Renderizar gráfico de tendencias de temperatura"""
        if data.empty:
            st.warning("No hay datos para mostrar las tendencias de temperatura.")
            return
        
        st.subheader(title)
        
        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Temperatura Promedio por Año', 'Temperatura por Mes', 
                          'Distribución de Temperaturas', 'Evolución Temporal'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Temperatura promedio por año
        if 'year' in data.columns and 'avg_temp' in data.columns:
            yearly_temp = data.groupby('year')['avg_temp'].mean().reset_index()
            fig.add_trace(
                go.Scatter(
                    x=yearly_temp['year'],
                    y=yearly_temp['avg_temp'],
                    mode='lines+markers',
                    name='Temp. Promedio',
                    line=dict(color='red', width=3),
                    marker=dict(size=8)
                ),
                row=1, col=1
            )
        
        # Gráfico 2: Temperatura por mes
        if 'month' in data.columns and 'avg_temp' in data.columns:
            monthly_temp = data.groupby('month')['avg_temp'].mean().reset_index()
            fig.add_trace(
                go.Bar(
                    x=monthly_temp['month'],
                    y=monthly_temp['avg_temp'],
                    name='Temp. por Mes',
                    marker_color='orange'
                ),
                row=1, col=2
            )
        
        # Gráfico 3: Distribución de temperaturas
        if 'avg_temp' in data.columns:
            fig.add_trace(
                go.Histogram(
                    x=data['avg_temp'],
                    nbinsx=20,
                    name='Distribución',
                    marker_color='green',
                    opacity=0.7
                ),
                row=2, col=1
            )
        
        # Gráfico 4: Evolución temporal por ciudad
        if all(col in data.columns for col in ['year', 'avg_temp', 'city']):
            for city in data['city'].unique()[:5]:  # Mostrar solo las primeras 5 ciudades
                city_data = data[data['city'] == city]
                city_yearly = city_data.groupby('year')['avg_temp'].mean().reset_index()
                fig.add_trace(
                    go.Scatter(
                        x=city_yearly['year'],
                        y=city_yearly['avg_temp'],
                        mode='lines+markers',
                        name=city,
                        line=dict(width=2)
                    ),
                    row=2, col=2
                )
        
        # Actualizar layout
        fig.update_layout(
            height=600,
            showlegend=True,
            template=self.template,
            title_text=title
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_precipitation_analysis(self, data: pd.DataFrame, title: str = "Análisis de Precipitación"):
        """Renderizar análisis de precipitación"""
        if data.empty:
            st.warning("No hay datos para mostrar el análisis de precipitación.")
            return
        
        st.subheader(title)
        
        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Precipitación Total por Año', 'Precipitación por Mes',
                          'Días de Lluvia por Ciudad', 'Distribución de Precipitación'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Precipitación total por año
        if 'year' in data.columns and 'total_precip' in data.columns:
            yearly_precip = data.groupby('year')['total_precip'].sum().reset_index()
            fig.add_trace(
                go.Bar(
                    x=yearly_precip['year'],
                    y=yearly_precip['total_precip'],
                    name='Precipitación Total',
                    marker_color='blue'
                ),
                row=1, col=1
            )
        
        # Gráfico 2: Precipitación por mes
        if 'month' in data.columns and 'total_precip' in data.columns:
            monthly_precip = data.groupby('month')['total_precip'].mean().reset_index()
            fig.add_trace(
                go.Bar(
                    x=monthly_precip['month'],
                    y=monthly_precip['total_precip'],
                    name='Precipitación Mensual',
                    marker_color='lightblue'
                ),
                row=1, col=2
            )
        
        # Gráfico 3: Días de lluvia por ciudad
        if all(col in data.columns for col in ['city', 'total_precip']):
            rainy_days = data[data['total_precip'] > 0].groupby('city').size().reset_index(name='dias_lluvia')
            rainy_days = rainy_days.sort_values('dias_lluvia', ascending=True)
            fig.add_trace(
                go.Bar(
                    x=rainy_days['dias_lluvia'],
                    y=rainy_days['city'],
                    orientation='h',
                    name='Días de Lluvia',
                    marker_color='cyan'
                ),
                row=2, col=1
            )
        
        # Gráfico 4: Distribución de precipitación
        if 'total_precip' in data.columns:
            fig.add_trace(
                go.Histogram(
                    x=data['total_precip'],
                    nbinsx=20,
                    name='Distribución',
                    marker_color='navy',
                    opacity=0.7
                ),
                row=2, col=2
            )
        
        # Actualizar layout
        fig.update_layout(
            height=600,
            showlegend=True,
            template=self.template,
            title_text=title
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_seasonal_analysis(self, data: pd.DataFrame, title: str = "Análisis Estacional"):
        """Renderizar análisis estacional"""
        if data.empty:
            st.warning("No hay datos para mostrar el análisis estacional.")
            return
        
        st.subheader(title)
        
        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Temperatura por Estación', 'Precipitación por Estación',
                          'Humedad por Estación', 'Comparación Estacional'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Temperatura por estación
        if 'season' in data.columns and 'avg_temp_season' in data.columns:
            season_temp = data.groupby('season')['avg_temp_season'].mean().reset_index()
            fig.add_trace(
                go.Bar(
                    x=season_temp['season'],
                    y=season_temp['avg_temp_season'],
                    name='Temp. Promedio',
                    marker_color='red'
                ),
                row=1, col=1
            )
        
        # Gráfico 2: Precipitación por estación
        if 'season' in data.columns and 'total_precip_season' in data.columns:
            season_precip = data.groupby('season')['total_precip_season'].mean().reset_index()
            fig.add_trace(
                go.Bar(
                    x=season_precip['season'],
                    y=season_precip['total_precip_season'],
                    name='Precipitación Total',
                    marker_color='blue'
                ),
                row=1, col=2
            )
        
        # Gráfico 3: Humedad por estación
        if 'season' in data.columns and 'avg_humidity_season' in data.columns:
            season_humidity = data.groupby('season')['avg_humidity_season'].mean().reset_index()
            fig.add_trace(
                go.Bar(
                    x=season_humidity['season'],
                    y=season_humidity['avg_humidity_season'],
                    name='Humedad Promedio',
                    marker_color='green'
                ),
                row=2, col=1
            )
        
        # Gráfico 4: Comparación estacional (radar chart)
        if all(col in data.columns for col in ['season', 'avg_temp_season', 'total_precip_season', 'avg_humidity_season']):
            season_avg = data.groupby('season').agg({
                'avg_temp_season': 'mean',
                'total_precip_season': 'mean',
                'avg_humidity_season': 'mean'
            }).reset_index()
            
            # Normalizar datos para el radar chart
            for col in ['avg_temp_season', 'total_precip_season', 'avg_humidity_season']:
                season_avg[col] = (season_avg[col] - season_avg[col].min()) / (season_avg[col].max() - season_avg[col].min()) * 100
            
            for _, row in season_avg.iterrows():
                fig.add_trace(
                    go.Scatterpolar(
                        r=[row['avg_temp_season'], row['total_precip_season'], row['avg_humidity_season']],
                        theta=['Temperatura', 'Precipitación', 'Humedad'],
                        fill='toself',
                        name=row['season']
                    ),
                    row=2, col=2
                )
        
        # Actualizar layout
        fig.update_layout(
            height=600,
            showlegend=True,
            template=self.template,
            title_text=title
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_alert_analysis(self, data: pd.DataFrame, title: str = "Análisis de Alertas"):
        """Renderizar análisis de alertas meteorológicas"""
        if data.empty:
            st.warning("No hay datos de alertas para mostrar.")
            return
        
        st.subheader(title)
        
        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Alertas por Tipo', 'Severidad de Alertas',
                          'Alertas por Ciudad', 'Evolución Temporal de Alertas'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Alertas por tipo
        if 'overall_alert' in data.columns:
            alert_counts = data['overall_alert'].value_counts().reset_index()
            alert_counts.columns = ['Alerta', 'Cantidad']
            fig.add_trace(
                go.Pie(
                    labels=alert_counts['Alerta'],
                    values=alert_counts['Cantidad'],
                    name='Tipos de Alerta'
                ),
                row=1, col=1
            )
        
        # Gráfico 2: Severidad de alertas
        if 'alert_severity' in data.columns:
            severity_counts = data['alert_severity'].value_counts().sort_index().reset_index()
            severity_counts.columns = ['Severidad', 'Cantidad']
            fig.add_trace(
                go.Bar(
                    x=severity_counts['Severidad'],
                    y=severity_counts['Cantidad'],
                    name='Severidad',
                    marker_color='orange'
                ),
                row=1, col=2
            )
        
        # Gráfico 3: Alertas por ciudad
        if 'city' in data.columns:
            city_alerts = data['city'].value_counts().reset_index()
            city_alerts.columns = ['Ciudad', 'Alertas']
            city_alerts = city_alerts.head(10)  # Top 10 ciudades
            fig.add_trace(
                go.Bar(
                    x=city_alerts['Ciudad'],
                    y=city_alerts['Alertas'],
                    name='Alertas por Ciudad',
                    marker_color='red'
                ),
                row=2, col=1
            )
        
        # Gráfico 4: Evolución temporal
        if 'date' in data.columns:
            data['date'] = pd.to_datetime(data['date'])
            data['month_year'] = data['date'].dt.to_period('M')
            monthly_alerts = data.groupby('month_year').size().reset_index(name='alertas')
            monthly_alerts['month_year'] = monthly_alerts['month_year'].astype(str)
            fig.add_trace(
                go.Scatter(
                    x=monthly_alerts['month_year'],
                    y=monthly_alerts['alertas'],
                    mode='lines+markers',
                    name='Evolución Temporal',
                    line=dict(color='purple', width=3)
                ),
                row=2, col=2
            )
        
        # Actualizar layout
        fig.update_layout(
            height=600,
            showlegend=True,
            template=self.template,
            title_text=title
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_climate_comparison(self, data: pd.DataFrame, title: str = "Comparación Climática"):
        """Renderizar comparación climática entre ciudades"""
        if data.empty:
            st.warning("No hay datos para mostrar la comparación climática.")
            return
        
        st.subheader(title)
        
        # Crear subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Temperatura por Ciudad', 'Precipitación por Ciudad',
                          'Clasificación Climática', 'Ranking de Ciudades'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Temperatura por ciudad
        if 'city' in data.columns and 'avg_temp_city' in data.columns:
            city_temp = data.sort_values('avg_temp_city', ascending=True)
            fig.add_trace(
                go.Bar(
                    x=city_temp['avg_temp_city'],
                    y=city_temp['city'],
                    orientation='h',
                    name='Temperatura Promedio',
                    marker_color='red'
                ),
                row=1, col=1
            )
        
        # Gráfico 2: Precipitación por ciudad
        if 'city' in data.columns and 'total_precip_city' in data.columns:
            city_precip = data.sort_values('total_precip_city', ascending=True)
            fig.add_trace(
                go.Bar(
                    x=city_precip['total_precip_city'],
                    y=city_precip['city'],
                    orientation='h',
                    name='Precipitación Total',
                    marker_color='blue'
                ),
                row=1, col=2
            )
        
        # Gráfico 3: Clasificación climática
        if 'climate_classification' in data.columns:
            climate_counts = data['climate_classification'].value_counts().reset_index()
            climate_counts.columns = ['Clasificación', 'Cantidad']
            fig.add_trace(
                go.Pie(
                    labels=climate_counts['Clasificación'],
                    values=climate_counts['Cantidad'],
                    name='Clasificación Climática'
                ),
                row=2, col=1
            )
        
        # Gráfico 4: Ranking de ciudades (scatter plot)
        if all(col in data.columns for col in ['avg_temp_city', 'total_precip_city', 'city']):
            fig.add_trace(
                go.Scatter(
                    x=data['avg_temp_city'],
                    y=data['total_precip_city'],
                    mode='markers+text',
                    text=data['city'],
                    textposition="top center",
                    name='Ciudades',
                    marker=dict(
                        size=10,
                        color=data['avg_temp_city'],
                        colorscale='Viridis',
                        showscale=True
                    )
                ),
                row=2, col=2
            )
        
        # Actualizar layout
        fig.update_layout(
            height=600,
            showlegend=True,
            template=self.template,
            title_text=title
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_kpi_dashboard(self, data: pd.DataFrame, title: str = "Dashboard de KPIs"):
        """Renderizar dashboard de KPIs"""
        if data.empty:
            st.warning("No hay datos para mostrar los KPIs.")
            return
        
        st.subheader(title)
        
        # Calcular KPIs
        kpis = self._calculate_kpis(data)
        
        # Mostrar KPIs en columnas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "🌡️ Temp. Promedio",
                f"{kpis['avg_temp']:.1f}°C",
                f"{kpis['temp_change']:+.1f}°C"
            )
        
        with col2:
            st.metric(
                "🌧️ Precipitación Total",
                f"{kpis['total_precip']:.0f} mm",
                f"{kpis['precip_change']:+.0f} mm"
            )
        
        with col3:
            st.metric(
                "💧 Humedad Promedio",
                f"{kpis['avg_humidity']:.1f}%",
                f"{kpis['humidity_change']:+.1f}%"
            )
        
        with col4:
            st.metric(
                "⚠️ Alertas Activas",
                kpis['active_alerts'],
                f"{kpis['alert_change']:+.0f}"
            )
        
        # Gráfico de KPIs
        fig = go.Figure()
        
        # Añadir indicadores de KPIs
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=kpis['avg_temp'],
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Temperatura Promedio (°C)"},
            delta={'reference': kpis['avg_temp'] - kpis['temp_change']},
            gauge={
                'axis': {'range': [None, 40]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 10], 'color': "lightgray"},
                    {'range': [10, 20], 'color': "yellow"},
                    {'range': [20, 30], 'color': "orange"},
                    {'range': [30, 40], 'color': "red"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 35
                }
            }
        ))
        
        fig.update_layout(height=300, template=self.template)
        st.plotly_chart(fig, use_container_width=True)
    
    def _calculate_kpis(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calcular KPIs principales"""
        kpis = {
            'avg_temp': 0.0,
            'temp_change': 0.0,
            'total_precip': 0.0,
            'precip_change': 0.0,
            'avg_humidity': 0.0,
            'humidity_change': 0.0,
            'active_alerts': 0,
            'alert_change': 0
        }
        
        # Temperatura
        if 'avg_temp' in data.columns:
            kpis['avg_temp'] = data['avg_temp'].mean()
            if 'year' in data.columns:
                years = sorted(data['year'].unique())
                if len(years) >= 2:
                    current_year = years[-1]
                    previous_year = years[-2]
                    current_temp = data[data['year'] == current_year]['avg_temp'].mean()
                    previous_temp = data[data['year'] == previous_year]['avg_temp'].mean()
                    kpis['temp_change'] = current_temp - previous_temp
        
        # Precipitación
        if 'total_precip' in data.columns:
            kpis['total_precip'] = data['total_precip'].sum()
            if 'year' in data.columns:
                years = sorted(data['year'].unique())
                if len(years) >= 2:
                    current_year = years[-1]
                    previous_year = years[-2]
                    current_precip = data[data['year'] == current_year]['total_precip'].sum()
                    previous_precip = data[data['year'] == previous_year]['total_precip'].sum()
                    kpis['precip_change'] = current_precip - previous_precip
        
        # Humedad
        if 'avg_humidity' in data.columns:
            kpis['avg_humidity'] = data['avg_humidity'].mean()
            if 'year' in data.columns:
                years = sorted(data['year'].unique())
                if len(years) >= 2:
                    current_year = years[-1]
                    previous_year = years[-2]
                    current_humidity = data[data['year'] == current_year]['avg_humidity'].mean()
                    previous_humidity = data[data['year'] == previous_year]['avg_humidity'].mean()
                    kpis['humidity_change'] = current_humidity - previous_humidity
        
        # Alertas
        if 'overall_alert' in data.columns:
            kpis['active_alerts'] = len(data[data['overall_alert'] != 'Normal'])
        
        return kpis
