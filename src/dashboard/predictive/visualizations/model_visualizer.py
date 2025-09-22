"""
Visualizador de modelos de Machine Learning
Sistema para visualizar rendimiento y comparar modelos
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
import logging
import warnings
warnings.filterwarnings('ignore')

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
except ImportError:
    go = None
    make_subplots = None
    px = None
    # Crear clases dummy para las anotaciones de tipo
    class DummyFigure:
        pass
    go = type('PlotlyModule', (), {'Figure': DummyFigure})()

logger = logging.getLogger(__name__)

class ModelVisualizer:
    """
    Visualizador de modelos de Machine Learning
    Crea gráficos para análisis de rendimiento y comparación de modelos
    """
    
    def __init__(self):
        """Inicializar visualizador de modelos"""
        if go is None:
            raise ImportError("Plotly no está instalado. Instala con: pip install plotly")
    
    def create_model_comparison_chart(self, 
                                    comparison_data: pd.DataFrame,
                                    title: str = "Comparación de Modelos") -> go.Figure:
        """
        Crear gráfico de comparación de modelos
        
        Args:
            comparison_data: DataFrame con métricas de modelos
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de comparación de modelos")
            
            # Crear subplots para diferentes métricas
            metrics = ['rmse', 'mae', 'mape', 'r2']
            available_metrics = [m for m in metrics if m in comparison_data.columns]
            
            if not available_metrics:
                logger.warning("No hay métricas disponibles para comparación")
                return go.Figure()
            
            # Crear subplots
            fig = make_subplots(
                rows=2,
                cols=2,
                subplot_titles=[m.upper() for m in available_metrics[:4]],
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "bar"}, {"type": "bar"}]]
            )
            
            # Agregar barras para cada métrica
            for i, metric in enumerate(available_metrics[:4]):
                row = (i // 2) + 1
                col = (i % 2) + 1
                
                fig.add_trace(
                    go.Bar(
                        x=comparison_data['Model'],
                        y=comparison_data[metric],
                        name=metric.upper(),
                        marker_color=self._get_metric_color(metric),
                        hovertemplate=f'<b>%{{x}}</b><br>{metric.upper()}: %{{y:.4f}}<extra></extra>'
                    ),
                    row=row,
                    col=col
                )
            
            # Configurar layout
            fig.update_layout(
                title=dict(
                    text=title,
                    x=0.5,
                    font=dict(size=20)
                ),
                showlegend=False,
                height=600,
                template="plotly_white"
            )
            
            logger.info("Gráfico de comparación creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de comparación: {str(e)}")
            raise
    
    def create_performance_trend_chart(self, 
                                     performance_history: List[Dict[str, Any]],
                                     title: str = "Evolución del Rendimiento") -> go.Figure:
        """
        Crear gráfico de evolución del rendimiento
        
        Args:
            performance_history: Lista con historial de rendimiento
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de evolución del rendimiento")
            
            if not performance_history:
                return go.Figure()
            
            # Convertir a DataFrame
            df = pd.DataFrame(performance_history)
            
            # Crear gráfico de líneas
            fig = go.Figure()
            
            # Métricas a mostrar
            metrics = ['rmse', 'mae', 'r2']
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
            
            for i, metric in enumerate(metrics):
                if metric in df.columns:
                    fig.add_trace(go.Scatter(
                        x=df.index,
                        y=df[metric],
                        mode='lines+markers',
                        name=metric.upper(),
                        line=dict(color=colors[i], width=2),
                        marker=dict(size=6),
                        hovertemplate=f'<b>{metric.upper()}</b><br>' +
                                    'Evaluación: %{x}<br>' +
                                    'Valor: %{y:.4f}<extra></extra>'
                    ))
            
            # Configurar layout
            fig.update_layout(
                title=dict(
                    text=title,
                    x=0.5,
                    font=dict(size=20)
                ),
                xaxis_title="Evaluación",
                yaxis_title="Valor de Métrica",
                hovermode='x unified',
                template="plotly_white",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            logger.info("Gráfico de evolución creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de evolución: {str(e)}")
            raise
    
    def create_feature_importance_chart(self, 
                                      feature_importance: Dict[str, float],
                                      title: str = "Importancia de Características",
                                      top_n: int = 20) -> go.Figure:
        """
        Crear gráfico de importancia de características
        
        Args:
            feature_importance: Diccionario con importancia de características
            title: Título del gráfico
            top_n: Número de características a mostrar
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de importancia de características")
            
            if not feature_importance:
                return go.Figure()
            
            # Ordenar por importancia
            sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
            top_features = sorted_features[:top_n]
            
            if not top_features:
                return go.Figure()
            
            # Preparar datos
            features = [item[0] for item in top_features]
            importance_values = [item[1] for item in top_features]
            
            # Crear gráfico de barras horizontales
            fig = go.Figure(go.Bar(
                x=importance_values,
                y=features,
                orientation='h',
                marker=dict(
                    color=importance_values,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Importancia")
                ),
                hovertemplate='<b>%{y}</b><br>Importancia: %{x:.4f}<extra></extra>'
            ))
            
            # Configurar layout
            fig.update_layout(
                title=dict(
                    text=title,
                    x=0.5,
                    font=dict(size=20)
                ),
                xaxis_title="Importancia",
                yaxis_title="Características",
                template="plotly_white",
                height=max(400, len(features) * 25)
            )
            
            logger.info("Gráfico de importancia creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de importancia: {str(e)}")
            raise
    
    def create_residuals_chart(self, 
                             actual: pd.Series, 
                             predicted: pd.Series,
                             title: str = "Análisis de Residuales") -> go.Figure:
        """
        Crear gráfico de análisis de residuales
        
        Args:
            actual: Valores reales
            predicted: Valores predichos
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de análisis de residuales")
            
            # Calcular residuales
            residuals = actual - predicted
            
            # Crear subplots
            fig = make_subplots(
                rows=2,
                cols=2,
                subplot_titles=[
                    "Residuales vs Predicciones",
                    "Histograma de Residuales",
                    "Q-Q Plot de Residuales",
                    "Residuales vs Tiempo"
                ],
                specs=[[{"type": "scatter"}, {"type": "histogram"}],
                       [{"type": "scatter"}, {"type": "scatter"}]]
            )
            
            # 1. Residuales vs Predicciones
            fig.add_trace(
                go.Scatter(
                    x=predicted,
                    y=residuals,
                    mode='markers',
                    name='Residuales',
                    marker=dict(color='blue', size=4, opacity=0.6),
                    hovertemplate='Predicción: %{x:.2f}<br>Residual: %{y:.2f}<extra></extra>'
                ),
                row=1, col=1
            )
            
            # Línea de referencia en y=0
            fig.add_hline(y=0, line_dash="dash", line_color="red", row=1, col=1)
            
            # 2. Histograma de residuales
            fig.add_trace(
                go.Histogram(
                    x=residuals,
                    name='Distribución',
                    marker=dict(color='lightblue'),
                    nbinsx=30
                ),
                row=1, col=2
            )
            
            # 3. Q-Q Plot (simplificado)
            sorted_residuals = np.sort(residuals)
            n = len(sorted_residuals)
            theoretical_quantiles = np.linspace(0, 1, n)
            
            fig.add_trace(
                go.Scatter(
                    x=theoretical_quantiles,
                    y=sorted_residuals,
                    mode='markers',
                    name='Q-Q Plot',
                    marker=dict(color='green', size=4),
                    hovertemplate='Teórico: %{x:.3f}<br>Observado: %{y:.2f}<extra></extra>'
                ),
                row=2, col=1
            )
            
            # 4. Residuales vs Tiempo
            time_index = range(len(residuals))
            fig.add_trace(
                go.Scatter(
                    x=time_index,
                    y=residuals,
                    mode='lines+markers',
                    name='Serie Temporal',
                    line=dict(color='purple', width=1),
                    marker=dict(size=3)
                ),
                row=2, col=2
            )
            
            # Configurar layout
            fig.update_layout(
                title=dict(
                    text=title,
                    x=0.5,
                    font=dict(size=20)
                ),
                showlegend=False,
                height=600,
                template="plotly_white"
            )
            
            logger.info("Gráfico de residuales creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de residuales: {str(e)}")
            raise
    
    def create_model_ranking_chart(self, 
                                 ranking_data: pd.DataFrame,
                                 title: str = "Ranking de Modelos") -> go.Figure:
        """
        Crear gráfico de ranking de modelos
        
        Args:
            ranking_data: DataFrame con ranking de modelos
            title: Título del gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            logger.info("Creando gráfico de ranking de modelos")
            
            # Crear gráfico de barras con ranking
            fig = go.Figure(go.Bar(
                x=ranking_data['Metric_Value'],
                y=ranking_data['Model'],
                orientation='h',
                marker=dict(
                    color=ranking_data['Rank'],
                    colorscale='RdYlGn_r',
                    showscale=True,
                    colorbar=dict(title="Ranking")
                ),
                text=ranking_data['Rank'],
                textposition='inside',
                hovertemplate='<b>%{y}</b><br>' +
                            'Ranking: %{text}<br>' +
                            'Valor: %{x:.4f}<extra></extra>'
            ))
            
            # Configurar layout
            fig.update_layout(
                title=dict(
                    text=title,
                    x=0.5,
                    font=dict(size=20)
                ),
                xaxis_title="Valor de Métrica",
                yaxis_title="Modelos",
                template="plotly_white",
                height=max(400, len(ranking_data) * 40)
            )
            
            logger.info("Gráfico de ranking creado exitosamente")
            return fig
            
        except Exception as e:
            logger.error(f"Error creando gráfico de ranking: {str(e)}")
            raise
    
    def _get_metric_color(self, metric: str) -> str:
        """Obtener color para métrica"""
        color_map = {
            'rmse': '#ff7f0e',
            'mae': '#2ca02c',
            'mape': '#d62728',
            'r2': '#1f77b4'
        }
        return color_map.get(metric, '#9467bd')
