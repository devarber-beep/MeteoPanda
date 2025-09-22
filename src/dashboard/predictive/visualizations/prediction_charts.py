"""
Visualizaciones avanzadas para predicciones meteorológicas
Sistema robusto de gráficos interactivos con Plotly
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
    from plotly.colors import qualitative, sequential
except ImportError:
    go = None
    make_subplots = None
    px = None
    qualitative = None
    sequential = None
    # Crear clases dummy para las anotaciones de tipo
    class DummyFigure:
        pass
    go = type('PlotlyModule', (), {'Figure': DummyFigure})()

logger = logging.getLogger(__name__)

class ChartTheme(Enum):
    """Temas de gráficos disponibles"""
    DEFAULT = "default"
    DARK = "dark"
    LIGHT = "light"
    WEATHER = "weather"

class WeatherPredictionCharts:
    """
    Generador de gráficos especializado en predicciones meteorológicas
    Sistema robusto con visualizaciones interactivas y profesionales
    """
    
    def __init__(self, theme: ChartTheme = ChartTheme.WEATHER):
        """
        Inicializar generador de gráficos
        
        Args:
            theme: Tema de los gráficos
        """
        if go is None:
            raise ImportError("Plotly no está instalado. Instala con: pip install plotly")
        
        self.theme = theme
        self.colors = self._get_theme_colors()
        
    def create_forecast_chart(self, 
                            historical_data: pd.DataFrame,
                            forecast_data: pd.DataFrame,
                            target_variable: str = 'y',
                            title: str = "Predicción Meteorológica",
                            show_confidence: bool = True,
                            show_components: bool = False) -> go.Figure:
        """
        Crear gráfico principal de predicción
        
        Args:
            historical_data: Datos históricos
            forecast_data: Datos de predicción
            target_variable: Variable objetivo
            title: Título del gráfico
            show_confidence: Mostrar intervalos de confianza
            show_components: Mostrar componentes de Prophet
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info(f"Creando gráfico de predicción para {target_variable}")
            
            # Crear figura
            fig = go.Figure()
            
            # Agregar datos históricos
            self._add_historical_data(fig, historical_data, target_variable)
            
            # Agregar predicciones
            self._add_forecast_data(fig, forecast_data, target_variable, show_confidence)
            
            # Agregar línea de separación entre histórico y predicción (deshabilitado temporalmente)
            # if not historical_data.empty and not forecast_data.empty:
            #     # Usar 'ds' si existe, sino usar 'date'
            #     date_column = 'ds' if 'ds' in historical_data.columns else 'date'
            #     if date_column in historical_data.columns:
            #         last_historical_date = historical_data[date_column].max()
            #         self._add_separation_line(fig, last_historical_date, target_variable)
            
            # Configurar layout
            self._configure_forecast_layout(fig, title, target_variable)
            
            logger.info("Gráfico de predicción creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de predicción: {str(e)}")
            raise
    
    def create_multi_city_comparison(self, 
                                   city_forecasts: Dict[str, pd.DataFrame],
                                   target_variable: str = 'y',
                                   title: str = "Comparación de Predicciones por Ciudad") -> go.Figure:
        """
        Crear gráfico de comparación entre ciudades
        
        Args:
            city_forecasts: Diccionario con predicciones por ciudad
            target_variable: Variable objetivo
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info(f"Creando comparación multi-ciudad para {target_variable}")
            
            fig = go.Figure()
            
            # Colores para cada ciudad
            city_colors = self._get_city_colors(list(city_forecasts.keys()))
            
            for i, (city, forecast) in enumerate(city_forecasts.items()):
                color = city_colors[i % len(city_colors)]
                
                # Línea de predicción
                fig.add_trace(go.Scatter(
                    x=forecast['ds'],
                    y=forecast['yhat'],
                    mode='lines',
                    name=f'{city} - Predicción',
                    line=dict(color=color, width=2),
                    hovertemplate=f'<b>{city}</b><br>' +
                                'Fecha: %{x}<br>' +
                                f'{target_variable}: %{{y:.2f}}<extra></extra>'
                ))
                
                # Intervalo de confianza
                if 'yhat_lower' in forecast.columns and 'yhat_upper' in forecast.columns:
                    fig.add_trace(go.Scatter(
                        x=forecast['ds'],
                        y=forecast['yhat_upper'],
                        mode='lines',
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo='skip'
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=forecast['ds'],
                        y=forecast['yhat_lower'],
                        mode='lines',
                        line=dict(width=0),
                        fill='tonexty',
                        fillcolor=f'rgba({self._hex_to_rgb(color)[0]}, {self._hex_to_rgb(color)[1]}, {self._hex_to_rgb(color)[2]}, 0.2)',
                        name=f'{city} - Intervalo de Confianza',
                        hovertemplate=f'<b>{city}</b><br>' +
                                    'Fecha: %{x}<br>' +
                                    f'{target_variable}: %{{y:.2f}}<extra></extra>'
                    ))
            
            # Configurar layout
            self._configure_multi_city_layout(fig, title, target_variable)
            
            logger.info("Gráfico multi-ciudad creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico multi-ciudad: {str(e)}")
            raise
    
    def create_components_chart(self, 
                              forecast_data: pd.DataFrame,
                              title: str = "Componentes de la Predicción") -> go.Figure:
        """
        Crear gráfico de componentes de Prophet
        
        Args:
            forecast_data: Datos de predicción con componentes
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de componentes")
            
            # Identificar componentes disponibles
            components = self._identify_components(forecast_data)
            
            if not components:
                logger.warning("No se encontraron componentes para mostrar")
                return go.Figure()
            
            # Crear subplots
            n_components = len(components)
            fig = make_subplots(
                rows=n_components,
                cols=1,
                subplot_titles=list(components.keys()),
                vertical_spacing=0.05
            )
            
            # Agregar cada componente
            for i, (component_name, component_data) in enumerate(components.items(), 1):
                fig.add_trace(
                    go.Scatter(
                        x=component_data['ds'],
                        y=component_data[component_name],
                        mode='lines',
                        name=component_name,
                        line=dict(color=self.colors['primary'], width=2),
                        hovertemplate=f'<b>{component_name}</b><br>' +
                                    'Fecha: %{x}<br>' +
                                    'Valor: %{y:.2f}<extra></extra>'
                    ),
                    row=i,
                    col=1
                )
            
            # Configurar layout
            self._configure_components_layout(fig, title)
            
            logger.info("Gráfico de componentes creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de componentes: {str(e)}")
            raise
    
    def create_accuracy_metrics_chart(self, 
                                    metrics_data: pd.DataFrame,
                                    title: str = "Métricas de Precisión del Modelo") -> go.Figure:
        """
        Crear gráfico de métricas de precisión
        
        Args:
            metrics_data: DataFrame con métricas
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de métricas de precisión")
            
            # Crear gráfico de barras
            fig = go.Figure()
            
            # Métricas principales
            main_metrics = ['rmse', 'mae', 'mape', 'r2']
            available_metrics = [m for m in main_metrics if m in metrics_data.columns]
            
            if not available_metrics:
                logger.warning("No se encontraron métricas para mostrar")
                return go.Figure()
            
            # Agregar barras para cada métrica
            for metric in available_metrics:
                fig.add_trace(go.Bar(
                    name=metric.upper(),
                    x=[metric.upper()],
                    y=[metrics_data[metric].iloc[0] if len(metrics_data) > 0 else 0],
                    marker_color=self.colors['secondary'],
                    hovertemplate=f'<b>{metric.upper()}</b><br>' +
                                'Valor: %{y:.4f}<extra></extra>'
                ))
            
            # Configurar layout
            self._configure_metrics_layout(fig, title)
            
            logger.info("Gráfico de métricas creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de métricas: {str(e)}")
            raise
    
    def create_weather_alert_chart(self, 
                                 forecast_data: pd.DataFrame,
                                 alert_thresholds: Dict[str, float],
                                 title: str = "Alertas Meteorológicas Predichas") -> go.Figure:
        """
        Crear gráfico de alertas meteorológicas
        
        Args:
            forecast_data: Datos de predicción
            alert_thresholds: Umbrales de alerta
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de alertas meteorológicas")
            
            fig = go.Figure()
            
            # Agregar predicción base
            fig.add_trace(go.Scatter(
                x=forecast_data['ds'],
                y=forecast_data['yhat'],
                mode='lines',
                name='Predicción',
                line=dict(color=self.colors['primary'], width=2),
                hovertemplate='<b>Predicción</b><br>' +
                            'Fecha: %{x}<br>' +
                            'Valor: %{y:.2f}<extra></extra>'
            ))
            
            # Agregar umbrales de alerta
            for alert_name, threshold in alert_thresholds.items():
                fig.add_hline(
                    y=threshold,
                    line_dash="dash",
                    line_color="red",
                    annotation_text=f"Umbral {alert_name}: {threshold}",
                    annotation_position="top right"
                )
            
            # Resaltar días con alertas
            alert_days = forecast_data[forecast_data['yhat'] > max(alert_thresholds.values())]
            if not alert_days.empty:
                fig.add_trace(go.Scatter(
                    x=alert_days['ds'],
                    y=alert_days['yhat'],
                    mode='markers',
                    name='Días de Alerta',
                    marker=dict(
                        color='red',
                        size=8,
                        symbol='triangle-up'
                    ),
                    hovertemplate='<b>¡ALERTA!</b><br>' +
                                'Fecha: %{x}<br>' +
                                'Valor: %{y:.2f}<extra></extra>'
                ))
            
            # Configurar layout
            self._configure_alert_layout(fig, title)
            
            logger.info("Gráfico de alertas creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de alertas: {str(e)}")
            raise
    
    def _add_historical_data(self, fig: go.Figure, data: pd.DataFrame, target_variable: str) -> None:
        """Agregar datos históricos al gráfico"""
        if data.empty:
            return
        
        # Usar 'ds' si existe, sino usar 'date'
        date_column = 'ds' if 'ds' in data.columns else 'date'
        
        # Mapear variable objetivo a columna real en los datos
        if target_variable == 'y' and 'temp_avg_c' in data.columns:
            actual_variable = 'temp_avg_c'
        elif target_variable in data.columns:
            actual_variable = target_variable
        else:
            # Buscar la primera columna numérica disponible
            numeric_cols = data.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                actual_variable = numeric_cols[0]
            else:
                return
        
        fig.add_trace(go.Scatter(
            x=data[date_column],
            y=data[actual_variable],
            mode='lines',
            name='Datos Históricos',
            line=dict(color=self.colors['historical'], width=2),
            hovertemplate='<b>Histórico</b><br>' +
                        'Fecha: %{x}<br>' +
                        f'{target_variable}: %{{y:.2f}}<extra></extra>'
        ))
    
    def _add_forecast_data(self, fig: go.Figure, data: pd.DataFrame, target_variable: str, show_confidence: bool) -> None:
        """Agregar datos de predicción al gráfico"""
        if data.empty:
            return
        
        # Línea de predicción
        fig.add_trace(go.Scatter(
            x=data['ds'],
            y=data['yhat'],
            mode='lines',
            name='Predicción',
            line=dict(color=self.colors['forecast'], width=3),
            hovertemplate='<b>Predicción</b><br>' +
                        'Fecha: %{x}<br>' +
                        f'{target_variable}: %{{y:.2f}}<extra></extra>'
        ))
        
        # Intervalo de confianza
        if show_confidence and 'yhat_lower' in data.columns and 'yhat_upper' in data.columns:
            fig.add_trace(go.Scatter(
                x=data['ds'],
                y=data['yhat_upper'],
                mode='lines',
                line=dict(width=0),
                showlegend=False,
                hoverinfo='skip'
            ))
            
            fig.add_trace(go.Scatter(
                x=data['ds'],
                y=data['yhat_lower'],
                mode='lines',
                line=dict(width=0),
                fill='tonexty',
                fillcolor=f'rgba({self._hex_to_rgb(self.colors["confidence"])[0]}, {self._hex_to_rgb(self.colors["confidence"])[1]}, {self._hex_to_rgb(self.colors["confidence"])[2]}, 0.3)',
                name='Intervalo de Confianza',
                hovertemplate='<b>Confianza</b><br>' +
                            'Fecha: %{x}<br>' +
                            f'{target_variable}: %{{y:.2f}}<extra></extra>'
            ))
    
    def _add_separation_line(self, fig: go.Figure, date: datetime, target_variable: str) -> None:
        """Agregar línea de separación entre histórico y predicción"""
        # Obtener rango de valores para la línea vertical
        y_values = []
        for trace in fig.data:
            if hasattr(trace, 'y') and trace.y is not None:
                y_values.extend(trace.y)
        
        if y_values:
            y_min, y_max = min(y_values), max(y_values)
            fig.add_vline(
                x=date,
                line_dash="dash",
                line_color="gray",
                annotation_text="Inicio de Predicción",
                annotation_position="top"
            )
    
    def _configure_forecast_layout(self, fig: go.Figure, title: str, target_variable: str) -> None:
        """Configurar layout del gráfico de predicción"""
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=20, color=self.colors['text'])
            ),
            xaxis_title="Fecha",
            yaxis_title=target_variable.replace('_', ' ').title(),
            hovermode='x unified',
            template=self._get_plotly_template(),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )
    
    def _configure_multi_city_layout(self, fig: go.Figure, title: str, target_variable: str) -> None:
        """Configurar layout del gráfico multi-ciudad"""
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=20, color=self.colors['text'])
            ),
            xaxis_title="Fecha",
            yaxis_title=target_variable.replace('_', ' ').title(),
            hovermode='x unified',
            template=self._get_plotly_template(),
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            ),
            margin=dict(l=50, r=150, t=80, b=50)
        )
    
    def _configure_components_layout(self, fig: go.Figure, title: str) -> None:
        """Configurar layout del gráfico de componentes"""
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=20, color=self.colors['text'])
            ),
            template=self._get_plotly_template(),
            showlegend=False,
            height=300 * len(fig.data),
            margin=dict(l=50, r=50, t=80, b=50)
        )
    
    def _configure_metrics_layout(self, fig: go.Figure, title: str) -> None:
        """Configurar layout del gráfico de métricas"""
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=20, color=self.colors['text'])
            ),
            xaxis_title="Métricas",
            yaxis_title="Valor",
            template=self._get_plotly_template(),
            showlegend=False,
            margin=dict(l=50, r=50, t=80, b=50)
        )
    
    def _configure_alert_layout(self, fig: go.Figure, title: str) -> None:
        """Configurar layout del gráfico de alertas"""
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=20, color=self.colors['text'])
            ),
            xaxis_title="Fecha",
            yaxis_title="Valor",
            hovermode='x unified',
            template=self._get_plotly_template(),
            showlegend=True,
            margin=dict(l=50, r=50, t=80, b=50)
        )
    
    def _identify_components(self, forecast_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Identificar componentes disponibles en los datos de predicción"""
        components = {}
        component_names = ['trend', 'yearly', 'weekly', 'daily', 'holidays']
        
        for component in component_names:
            if component in forecast_data.columns:
                components[component] = forecast_data[['ds', component]]
        
        return components
    
    def _get_theme_colors(self) -> Dict[str, str]:
        """Obtener colores del tema"""
        if self.theme == ChartTheme.WEATHER:
            return {
                'primary': '#1f77b4',      # Azul
                'secondary': '#ff7f0e',    # Naranja
                'historical': '#2ca02c',   # Verde
                'forecast': '#d62728',     # Rojo
                'confidence': '#1f77b4',   # Azul claro
                'text': '#2c3e50'          # Gris oscuro
            }
        elif self.theme == ChartTheme.DARK:
            return {
                'primary': '#00d4ff',
                'secondary': '#ff6b6b',
                'historical': '#51cf66',
                'forecast': '#ffd43b',
                'confidence': '#00d4ff',
                'text': '#ffffff'
            }
        else:  # DEFAULT
            return {
                'primary': '#1f77b4',
                'secondary': '#ff7f0e',
                'historical': '#2ca02c',
                'forecast': '#d62728',
                'confidence': '#1f77b4',
                'text': '#000000'
            }
    
    def _get_city_colors(self, cities: List[str]) -> List[str]:
        """Obtener colores para ciudades"""
        if qualitative:
            return qualitative.Plotly[:len(cities)]
        else:
            return ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b'][:len(cities)]
    
    def _hex_to_rgb(self, hex_color: str) -> Tuple[int, int, int]:
        """Convertir color hex a RGB"""
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    
    def _get_plotly_template(self) -> str:
        """Obtener template de Plotly según el tema"""
        if self.theme == ChartTheme.DARK:
            return "plotly_dark"
        elif self.theme == ChartTheme.LIGHT:
            return "plotly_white"
        else:
            return "plotly"
