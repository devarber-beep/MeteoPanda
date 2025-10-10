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
        
        # Crear gráficos separados para mejor legibilidad de leyendas
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico 1: Temperatura promedio por año
            if 'year' in data.columns and 'avg_temp' in data.columns:
                yearly_temp = data.groupby('year')['avg_temp'].mean().reset_index()
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Scatter(
                        x=yearly_temp['year'],
                        y=yearly_temp['avg_temp'],
                        mode='lines+markers',
                        name='Temperatura Promedio',
                        line=dict(color='red', width=3),
                        marker=dict(size=8)
                    )
                )
                fig1.update_layout(
                    title="Temperatura Promedio por Año",
                    xaxis_title="Año",
                    yaxis_title="Temperatura (°C)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Gráfico 3: Distribución de temperaturas
            if 'avg_temp' in data.columns:
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Histogram(
                        x=data['avg_temp'],
                        nbinsx=20,
                        name='Distribución de Temperaturas',
                        marker_color='green',
                        opacity=0.7
                    )
                )
                fig3.update_layout(
                    title="Distribución de Temperaturas",
                    xaxis_title="Temperatura (°C)",
                    yaxis_title="Frecuencia",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Gráfico 2: Temperatura por mes
            if 'month' in data.columns and 'avg_temp' in data.columns:
                monthly_temp = data.groupby('month')['avg_temp'].mean().reset_index()
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Bar(
                        x=monthly_temp['month'],
                        y=monthly_temp['avg_temp'],
                        name='Temperatura por Mes',
                        marker_color='orange'
                    )
                )
                fig2.update_layout(
                    title="Temperatura Promedio por Mes",
                    xaxis_title="Mes",
                    yaxis_title="Temperatura (°C)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            # Gráfico 4: Evolución temporal por ciudad
            if all(col in data.columns for col in ['year', 'avg_temp', 'city']):
                fig4 = go.Figure()
                cities = data['city'].unique()[:5]  # Mostrar solo las primeras 5 ciudades
                colors = px.colors.qualitative.Set3
                
                for i, city in enumerate(cities):
                    city_data = data[data['city'] == city]
                    city_yearly = city_data.groupby('year')['avg_temp'].mean().reset_index()
                    fig4.add_trace(
                        go.Scatter(
                            x=city_yearly['year'],
                            y=city_yearly['avg_temp'],
                            mode='lines+markers',
                            name=city,
                            line=dict(width=2, color=colors[i % len(colors)]),
                            marker=dict(size=6)
                        )
                    )
                
                fig4.update_layout(
                    title="Evolución Temporal por Ciudad",
                    xaxis_title="Año",
                    yaxis_title="Temperatura (°C)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig4, use_container_width=True)
    
    def render_precipitation_analysis(self, data: pd.DataFrame, title: str = "Análisis de Precipitación"):
        """Renderizar análisis de precipitación"""
        if data.empty:
            st.warning("No hay datos para mostrar el análisis de precipitación.")
            return
        
        st.subheader(title)
        
        # Crear gráficos separados para mejor legibilidad de leyendas
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico 1: Precipitación total por año
            if 'year' in data.columns and 'total_precip' in data.columns:
                yearly_precip = data.groupby('year')['total_precip'].sum().reset_index()
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Bar(
                        x=yearly_precip['year'],
                        y=yearly_precip['total_precip'],
                        name='Precipitación Total',
                        marker_color='blue'
                    )
                )
                fig1.update_layout(
                    title="Precipitación Total por Año",
                    xaxis_title="Año",
                    yaxis_title="Precipitación (mm)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Gráfico 3: Días de lluvia por ciudad
            if all(col in data.columns for col in ['city', 'total_precip']):
                rainy_days = data[data['total_precip'] > 0].groupby('city').size().reset_index(name='dias_lluvia')
                rainy_days = rainy_days.sort_values('dias_lluvia', ascending=True)
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Bar(
                        x=rainy_days['dias_lluvia'],
                        y=rainy_days['city'],
                        orientation='h',
                        name='Días de Lluvia',
                        marker_color='cyan'
                    )
                )
                fig3.update_layout(
                    title="Días de Lluvia por Ciudad",
                    xaxis_title="Días de Lluvia",
                    yaxis_title="Ciudad",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Gráfico 2: Precipitación por mes
            if 'month' in data.columns and 'total_precip' in data.columns:
                monthly_precip = data.groupby('month')['total_precip'].mean().reset_index()
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Bar(
                        x=monthly_precip['month'],
                        y=monthly_precip['total_precip'],
                        name='Precipitación Mensual',
                        marker_color='lightblue'
                    )
                )
                fig2.update_layout(
                    title="Precipitación Promedio por Mes",
                    xaxis_title="Mes",
                    yaxis_title="Precipitación (mm)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            # Gráfico 4: Distribución de precipitación
            if 'total_precip' in data.columns:
                fig4 = go.Figure()
                fig4.add_trace(
                    go.Histogram(
                        x=data['total_precip'],
                        nbinsx=20,
                        name='Distribución de Precipitación',
                        marker_color='navy',
                        opacity=0.7
                    )
                )
                fig4.update_layout(
                    title="Distribución de Precipitación",
                    xaxis_title="Precipitación (mm)",
                    yaxis_title="Frecuencia",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig4, use_container_width=True)
    
    def render_seasonal_analysis(self, data: pd.DataFrame, title: str = "Análisis Estacional"):
        """Renderizar análisis estacional"""
        if data.empty:
            st.warning("No hay datos para mostrar el análisis estacional.")
            return
        
        st.subheader(title)
        
        # Crear gráficos separados para mejor legibilidad de leyendas
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico 1: Temperatura por estación
            if 'season' in data.columns and 'avg_temp_season' in data.columns:
                season_temp = data.groupby('season')['avg_temp_season'].mean().reset_index()
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Bar(
                        x=season_temp['season'],
                        y=season_temp['avg_temp_season'],
                        name='Temperatura Promedio',
                        marker_color='red'
                    )
                )
                fig1.update_layout(
                    title="Temperatura Promedio por Estación",
                    xaxis_title="Estación",
                    yaxis_title="Temperatura (°C)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Gráfico 3: Humedad por estación
            if 'season' in data.columns and 'avg_humidity_season' in data.columns:
                season_humidity = data.groupby('season')['avg_humidity_season'].mean().reset_index()
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Bar(
                        x=season_humidity['season'],
                        y=season_humidity['avg_humidity_season'],
                        name='Humedad Promedio',
                        marker_color='green'
                    )
                )
                fig3.update_layout(
                    title="Humedad Promedio por Estación",
                    xaxis_title="Estación",
                    yaxis_title="Humedad (%)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Gráfico 2: Precipitación por estación
            if 'season' in data.columns and 'total_precip_season' in data.columns:
                season_precip = data.groupby('season')['total_precip_season'].mean().reset_index()
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Bar(
                        x=season_precip['season'],
                        y=season_precip['total_precip_season'],
                        name='Precipitación Total',
                        marker_color='blue'
                    )
                )
                fig2.update_layout(
                    title="Precipitación Total por Estación",
                    xaxis_title="Estación",
                    yaxis_title="Precipitación (mm)",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig2, use_container_width=True)
            
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
                
                fig4 = go.Figure()
                colors = px.colors.qualitative.Set3
                
                for i, (_, row) in enumerate(season_avg.iterrows()):
                    fig4.add_trace(
                        go.Scatterpolar(
                            r=[row['avg_temp_season'], row['total_precip_season'], row['avg_humidity_season']],
                            theta=['Temperatura', 'Precipitación', 'Humedad'],
                            fill='toself',
                            name=row['season'],
                            line_color=colors[i % len(colors)]
                        )
                    )
                
                fig4.update_layout(
                    title="Comparación Estacional",
                    template=self.template,
                    height=300,
                    showlegend=True,
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 100]
                        )
                    )
                )
                st.plotly_chart(fig4, use_container_width=True)
    
    def render_alert_analysis(self, data: pd.DataFrame, title: str = "Análisis de Alertas"):
        """Renderizar análisis de alertas meteorológicas"""
        if data.empty:
            st.warning("No hay datos de alertas para mostrar.")
            return
        
        st.subheader(title)
        
        # Crear gráficos separados para mejor legibilidad de leyendas
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico 1: Alertas por tipo
            if 'overall_alert' in data.columns:
                alert_counts = data['overall_alert'].value_counts().reset_index()
                alert_counts.columns = ['Alerta', 'Cantidad']
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Pie(
                        labels=alert_counts['Alerta'],
                        values=alert_counts['Cantidad'],
                        name='Tipos de Alerta'
                    )
                )
                fig1.update_layout(
                    title="Distribución de Alertas por Tipo",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Gráfico 3: Alertas por ciudad
            if 'city' in data.columns:
                city_alerts = data['city'].value_counts().reset_index()
                city_alerts.columns = ['Ciudad', 'Alertas']
                city_alerts = city_alerts.head(10)  # Top 10 ciudades
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Bar(
                        x=city_alerts['Ciudad'],
                        y=city_alerts['Alertas'],
                        name='Alertas por Ciudad',
                        marker_color='red'
                    )
                )
                fig3.update_layout(
                    title="Alertas por Ciudad (Top 10)",
                    xaxis_title="Ciudad",
                    yaxis_title="Número de Alertas",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Gráfico 2: Severidad de alertas
            if 'alert_severity' in data.columns:
                severity_counts = data['alert_severity'].value_counts().sort_index().reset_index()
                severity_counts.columns = ['Severidad', 'Cantidad']
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Bar(
                        x=severity_counts['Severidad'],
                        y=severity_counts['Cantidad'],
                        name='Severidad de Alertas',
                        marker_color='orange'
                    )
                )
                fig2.update_layout(
                    title="Distribución por Severidad",
                    xaxis_title="Nivel de Severidad",
                    yaxis_title="Número de Alertas",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            # Gráfico 4: Evolución temporal
            if 'date' in data.columns:
                data_copy = data.copy()
                data_copy['date'] = pd.to_datetime(data_copy['date'])
                data_copy['month_year'] = data_copy['date'].dt.to_period('M')
                monthly_alerts = data_copy.groupby('month_year').size().reset_index(name='alertas')
                monthly_alerts['month_year'] = monthly_alerts['month_year'].astype(str)
                fig4 = go.Figure()
                fig4.add_trace(
                    go.Scatter(
                        x=monthly_alerts['month_year'],
                        y=monthly_alerts['alertas'],
                        mode='lines+markers',
                        name='Evolución Temporal',
                        line=dict(color='purple', width=3),
                        marker=dict(size=8)
                    )
                )
                fig4.update_layout(
                    title="Evolución Temporal de Alertas",
                    xaxis_title="Período",
                    yaxis_title="Número de Alertas",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig4, use_container_width=True)
    
    def render_climate_comparison(self, data: pd.DataFrame, title: str = "Comparación Climática"):
        """Renderizar comparación climática entre ciudades"""
        if data.empty:
            st.warning("No hay datos para mostrar la comparación climática.")
            return
        
        st.subheader(title)
        
        # Crear gráficos separados para mejor legibilidad de leyendas
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico 1: Temperatura por ciudad
            if 'city' in data.columns and 'avg_temp_city' in data.columns:
                city_temp = data.sort_values('avg_temp_city', ascending=True)
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Bar(
                        x=city_temp['avg_temp_city'],
                        y=city_temp['city'],
                        orientation='h',
                        name='Temperatura Promedio',
                        marker_color='red'
                    )
                )
                fig1.update_layout(
                    title="Temperatura Promedio por Ciudad",
                    xaxis_title="Temperatura (°C)",
                    yaxis_title="Ciudad",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Gráfico 3: Clasificación climática
            if 'climate_classification' in data.columns:
                climate_counts = data['climate_classification'].value_counts().reset_index()
                climate_counts.columns = ['Clasificación', 'Cantidad']
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Pie(
                        labels=climate_counts['Clasificación'],
                        values=climate_counts['Cantidad'],
                        name='Clasificación Climática'
                    )
                )
                fig3.update_layout(
                    title="Distribución de Clasificaciones Climáticas",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Gráfico 2: Precipitación por ciudad
            if 'city' in data.columns and 'total_precip_city' in data.columns:
                city_precip = data.sort_values('total_precip_city', ascending=True)
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Bar(
                        x=city_precip['total_precip_city'],
                        y=city_precip['city'],
                        orientation='h',
                        name='Precipitación Total',
                        marker_color='blue'
                    )
                )
                fig2.update_layout(
                    title="Precipitación Total por Ciudad",
                    xaxis_title="Precipitación (mm)",
                    yaxis_title="Ciudad",
                    template=self.template,
                    height=300,
                    showlegend=True
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            # Gráfico 4: Ranking de ciudades (scatter plot)
            if all(col in data.columns for col in ['avg_temp_city', 'total_precip_city', 'city']):
                fig4 = go.Figure()
                fig4.add_trace(
                    go.Scatter(
                        x=data['avg_temp_city'],
                        y=data['total_precip_city'],
                        mode='markers+text',
                        text=data['city'],
                        textposition="top center",
                        name='Comparación de Ciudades',
                        marker=dict(
                            size=12,
                            color=data['avg_temp_city'],
                            colorscale='Viridis',
                            showscale=True,
                            colorbar=dict(
                                title="Temperatura (°C)",
                                y=0.3,
                                yanchor='middle'
                            )
                        )
                    )
                )
                fig4.update_layout(
                    title="Comparación Climática: Temperatura vs Precipitación",
                    xaxis_title="Temperatura Promedio (°C)",
                    yaxis_title="Precipitación Total (mm)",
                    template=self.template,
                    height=300,
                    showlegend=True,
                    legend=dict(
                        x=1.05,
                        y=1,
                        xanchor='left',
                        yanchor='top',
                        bgcolor='rgba(255,255,255,0.9)',
                        bordercolor='rgba(0,0,0,0.3)',
                        borderwidth=1,
                        itemwidth=30,
                        itemsizing='constant',
                        traceorder='normal',
                        itemclick='toggleothers',
                        font=dict(size=10),
                        orientation='v'
                    ),
                    margin=dict(r=120)  # Margen derecho ligeramente menor para la leyenda
                )
                st.plotly_chart(fig4, use_container_width=True)

    def render_similarity_heatmap(self, similarity_df: pd.DataFrame, title: str = "Matriz de Similitud Climática"):
        """Renderiza un heatmap de similitud ciudad-ciudad (matriz simétrica y ordenada)."""
        if similarity_df.empty:
            st.warning("No hay datos de similitud para mostrar.")
            return
        st.subheader(title)
        # Construir matriz simétrica (city x city). Partimos de top-k direccional y simetrizamos con el máximo
        try:
            df = similarity_df[['city','similar_city','similarity_score']].copy()
            df_rev = df.rename(columns={'city':'similar_city','similar_city':'city'})
            df_sym = pd.concat([df, df_rev], ignore_index=True)
            matrix = df_sym.pivot_table(index='city', columns='similar_city', values='similarity_score', aggfunc='max').fillna(0)
            # asegurar diagonal 1.0 para legibilidad relativa
            for c in matrix.index:
                if c in matrix.columns:
                    matrix.loc[c, c] = 1.0
            # Ordenar por afinidad media (sum of row)
            order = matrix.mean(axis=1).sort_values(ascending=False).index
            matrix = matrix.loc[order, order]
        except Exception:
            st.warning("No se pudo construir la matriz de similitud.")
            return
        fig = go.Figure(data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns,
            y=matrix.index,
            colorscale='Viridis',
            colorbar=dict(title='Similitud'),
            zmin=0,
            zmax=max(1.0, float(matrix.values.max()))
        ))
        fig.update_layout(template=self.template, height=600, xaxis_title="Ciudad", yaxis_title="Ciudad")
        st.plotly_chart(fig, use_container_width=True)

    def render_top_similars(self, similarity_df: pd.DataFrame, title: str = "Ciudades Más Similares"):
        """Renderiza un gráfico de barras de top similares por ciudad seleccionada."""
        if similarity_df.empty:
            st.warning("No hay datos de similitud.")
            return
        st.subheader(title)
        cities = sorted(similarity_df['city'].unique().tolist())
        selected_city = st.selectbox("Ciudad base", options=cities)
        k = st.slider("Top K", min_value=3, max_value=10, value=5, step=1)
        topk = similarity_df[similarity_df['city'] == selected_city].sort_values('rank').head(k)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=topk['similar_city'],
            y=topk['similarity_score'],
            marker_color='teal',
            name='Score'
        ))
        fig.update_layout(template=self.template, height=350, xaxis_title="Ciudad similar", yaxis_title="Similitud")
        st.plotly_chart(fig, use_container_width=True)

    def render_outliers_overview(self, outliers_df: pd.DataFrame, title: str = "Outliers Climáticos por Ciudad"):
        """Visualización de outliers: scatter score por ciudad y breakdown por variable dominante."""
        if outliers_df.empty:
            st.warning("No hay datos de outliers.")
            return
        st.subheader(title)
        # Top N outliers
        topn = outliers_df.sort_values('outlier_score', ascending=False).head(15)
        col1, col2 = st.columns(2)
        with col1:
            fig1 = go.Figure()
            fig1.add_trace(go.Bar(
                x=topn['city'],
                y=topn['outlier_score'],
                marker_color='crimson'
            ))
            fig1.update_layout(template=self.template, height=350, xaxis_title="Ciudad", yaxis_title="Outlier score")
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            by_var = topn['dominant_variable'].value_counts().reset_index()
            by_var.columns = ['Variable', 'Ciudades']
            fig2 = go.Figure(go.Pie(labels=by_var['Variable'], values=by_var['Ciudades']))
            fig2.update_layout(template=self.template, height=350)
            st.plotly_chart(fig2, use_container_width=True)

    def render_similarity_pairs_table(self, similarity_df: pd.DataFrame, title: str = "Tabla de Pares Similares"):
        """Tabla profesional con pares ciudad-ciudad, filtros y ordenación."""
        if similarity_df.empty:
            st.warning("No hay datos de similitud.")
            return
        st.subheader(title)
        # Filtros simples en la propia tabla
        col1, col2 = st.columns([1,1])
        with col1:
            base_city = st.selectbox("Filtrar por ciudad base (opcional)", options=["Todas"] + sorted(similarity_df['city'].unique().tolist()))
        with col2:
            min_score = st.slider("Score mínimo", min_value=float(similarity_df['similarity_score'].min()), max_value=float(similarity_df['similarity_score'].max()), value=float(similarity_df['similarity_score'].quantile(0.5)))
        df = similarity_df.copy()
        if base_city != "Todas":
            df = df[df['city'] == base_city]
        df = df[df['similarity_score'] >= min_score]
        df_view = df[['city','similar_city','similarity_distance','similarity_score','rank']].sort_values(['city','rank'])
        df_view = df_view.rename(columns={'city':'Ciudad','similar_city':'Similar a','similarity_distance':'Distancia','similarity_score':'Similitud','rank':'Rank'})
        st.dataframe(df_view, use_container_width=True, hide_index=True)
    
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
